from __future__ import annotations

from collections.abc import Callable, Mapping
import json
from typing import Any, Protocol

import httpx

from app.tool_models import ToolCall, ToolId, ToolPlan


class ToolPlanner(Protocol):
    name: str

    def plan(self, question: str) -> ToolPlan: ...


class RuleBasedToolPlanner:
    name = "rule_based"

    def plan(self, question: str) -> ToolPlan:
        normalized = question.casefold()
        if any(word in normalized for word in ("综合", "整体", "全部", "overall")):
            selected = [ToolId.REALTIME, ToolId.HISTORICAL, ToolId.DATA_QUALITY]
        elif any(word in normalized for word in ("质量", "checkpoint", "flink", "异常数据")):
            selected = [ToolId.DATA_QUALITY]
        elif any(word in normalized for word in ("历史", "构成", "事件类型", "historical")):
            selected = [ToolId.HISTORICAL]
        else:
            selected = [ToolId.REALTIME]
        return ToolPlan(calls=[ToolCall(tool_id=tool_id) for tool_id in selected])


class OpenAICompatibleToolPlanner:
    name = "openai_compatible"

    def __init__(
        self,
        api_key: str,
        base_url: str,
        model: str,
        timeout_seconds: float = 15,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        if not api_key or not base_url or not model:
            raise ValueError("AI_API_KEY, AI_BASE_URL and AI_MODEL are required")
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._model = model
        self._timeout_seconds = timeout_seconds
        self._client_factory = client_factory or (lambda: httpx.Client())

    def plan(self, question: str) -> ToolPlan:
        payload = {
            "model": self._model,
            "messages": [
                {
                    "role": "system",
                    "content": "Choose the required functions needed to answer the question.",
                },
                {"role": "user", "content": question},
            ],
            "tools": [self._tool_definition(tool_id) for tool_id in ToolId],
            "tool_choice": "required",
            "temperature": 0,
        }
        with self._client_factory() as client:
            response = client.post(
                f"{self._base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self._api_key}"},
                json=payload,
                timeout=self._timeout_seconds,
            )
        response.raise_for_status()
        tool_calls = response.json()["choices"][0]["message"]["tool_calls"]
        if not isinstance(tool_calls, list):
            raise ValueError("model tool_calls must be a list")

        # Build no partial plan: every model call must validate before ToolPlan is created.
        calls = [self._parse_tool_call(tool_call) for tool_call in tool_calls]
        return ToolPlan(calls=calls)

    @staticmethod
    def _tool_definition(tool_id: ToolId) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": tool_id.value,
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
            },
        }

    @staticmethod
    def _parse_tool_call(value: object) -> ToolCall:
        if not isinstance(value, Mapping) or value.get("type") != "function":
            raise ValueError("model tool call must be a function")
        function = value.get("function")
        if not isinstance(function, Mapping):
            raise ValueError("model tool call must include a function")

        name = function.get("name")
        arguments = function.get("arguments")
        if not isinstance(name, str) or not isinstance(arguments, str):
            raise ValueError("model tool call must include string name and arguments")
        try:
            tool_id = ToolId(name)
            parsed_arguments = json.loads(arguments)
        except ValueError:
            raise ValueError("model tool call is not allowed") from None
        if parsed_arguments != {}:
            raise ValueError("model tool calls must not include arguments")
        return ToolCall(tool_id=tool_id)
