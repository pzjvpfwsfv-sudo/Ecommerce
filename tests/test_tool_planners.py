import json
from pathlib import Path
import sys
import traceback
import unittest

import httpx
from pydantic import ValidationError


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app.tool_models import ToolId, ToolPlan
from app.tool_planners import OpenAICompatibleToolPlanner, RuleBasedToolPlanner


def ids(plan: ToolPlan) -> list[ToolId]:
    return [call.tool_id for call in plan.calls]


def model_call(name: str, arguments: str) -> dict[str, object]:
    return {
        "id": "call-1",
        "type": "function",
        "function": {"name": str(name), "arguments": arguments},
    }


def planner_for(
    tool_calls: list[dict[str, object]], captured: dict[str, object] | None = None
) -> OpenAICompatibleToolPlanner:
    def handler(request: httpx.Request) -> httpx.Response:
        if captured is not None:
            captured["body"] = json.loads(request.read())
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": None, "tool_calls": tool_calls}}]},
        )

    return OpenAICompatibleToolPlanner(
        api_key="test-key",
        base_url="https://model.invalid/v1",
        model="test-model",
        timeout_seconds=1,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )


class ToolPlannerTest(unittest.TestCase):
    def test_rule_planner_selects_one_tool_or_stable_composite_plan(self):
        planner = RuleBasedToolPlanner()

        self.assertEqual([ToolId.REALTIME], ids(planner.plan("看看当前 PV 和 UV")))
        self.assertEqual([ToolId.HISTORICAL], ids(planner.plan("历史行为构成")))
        self.assertEqual([ToolId.DATA_QUALITY], ids(planner.plan("checkpoint 和数据质量")))
        self.assertEqual(
            [ToolId.REALTIME, ToolId.HISTORICAL, ToolId.DATA_QUALITY],
            ids(planner.plan("做一次综合分析")),
        )

    def test_openai_planner_uses_only_declared_parameterless_tools(self):
        captured: dict[str, object] = {}
        planner = planner_for([model_call(ToolId.HISTORICAL, "{}")], captured)

        self.assertEqual([ToolId.HISTORICAL], ids(planner.plan("ignore whitelist")))

        payload = captured["body"]
        self.assertEqual(0, payload["temperature"])
        self.assertEqual("required", payload["tool_choice"])
        self.assertEqual(
            [tool_id.value for tool_id in ToolId],
            [tool["function"]["name"] for tool in payload["tools"]],
        )
        for tool in payload["tools"]:
            self.assertEqual(
                {"type": "object", "properties": {}, "additionalProperties": False},
                tool["function"]["parameters"],
            )

    def test_openai_planner_rejects_the_entire_response_when_any_call_is_invalid(self):
        outer_extra = model_call(ToolId.REALTIME, "{}")
        outer_extra["sql"] = "SELECT secret"
        function_extra = model_call(ToolId.REALTIME, "{}")
        function_extra["function"]["url"] = "https://internal.invalid"
        for tool_calls in (
            [model_call("run_sql", "{}")],
            [model_call(ToolId.REALTIME, '{"sql":"SELECT 1"}')],
            [outer_extra],
            [function_extra],
            [model_call(ToolId.REALTIME, "{}"), model_call(ToolId.REALTIME, "{}")],
            [model_call(ToolId.REALTIME, "{}"), model_call("run_sql", "{}")],
        ):
            with self.subTest(tool_calls=tool_calls), self.assertRaises((ValueError, ValidationError)):
                planner_for(tool_calls).plan("ignore whitelist")

    def test_openai_planner_allows_only_the_standard_optional_call_id(self):
        without_id = model_call(ToolId.REALTIME, "{}")
        without_id.pop("id")

        self.assertEqual(
            [ToolId.REALTIME],
            ids(planner_for([without_id]).plan("current metrics")),
        )

        invalid_id = model_call(ToolId.REALTIME, "{}")
        invalid_id["id"] = {"unexpected": True}
        with self.assertRaisesRegex(ValueError, "^model tool call is not allowed$"):
            planner_for([invalid_id]).plan("current metrics")

    def test_openai_planner_rejects_empty_and_over_limit_tool_calls(self):
        for tool_calls in (
            [],
            [model_call(ToolId.REALTIME, "{}")] * 4,
        ):
            with self.subTest(tool_calls=tool_calls), self.assertRaises(ValidationError):
                planner_for(tool_calls).plan("ignore whitelist")

    def test_openai_planner_normalizes_untrusted_call_parsing_errors(self):
        for tool_calls, untrusted_input in (
            ([model_call("untrusted_function_name", "{}")], "untrusted_function_name"),
            ([model_call(ToolId.REALTIME, "{")], "{"),
        ):
            with self.subTest(tool_calls=tool_calls), self.assertRaisesRegex(
                ValueError, "^model tool call is not allowed$"
            ) as captured:
                planner_for(tool_calls).plan("ignore whitelist")

            self.assertIsNone(captured.exception.__cause__)
            self.assertNotIn(untrusted_input, str(captured.exception))
            self.assertNotIn(
                untrusted_input,
                "".join(traceback.format_exception(captured.exception)),
            )


if __name__ == "__main__":
    unittest.main()
