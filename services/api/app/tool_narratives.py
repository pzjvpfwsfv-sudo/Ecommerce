from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
import json
from typing import Protocol

import httpx

from app.analysis_models import AnalysisNarrative
from app.tool_models import ToolAnalysisContext, ToolAnalysisSelection
from app.tool_deadline import remaining_timeout


class ToolNarrativeAnalyzer(Protocol):
    name: str

    def select(self, context: ToolAnalysisContext) -> ToolAnalysisSelection: ...

    def analyze(self, context: ToolAnalysisContext) -> AnalysisNarrative: ...


class RuleBasedToolNarrativeAnalyzer:
    name = "rule_based"

    def select(self, context: ToolAnalysisContext) -> ToolAnalysisSelection:
        evidence = context.evidence
        realtime = evidence.realtime
        historical = evidence.historical
        quality = evidence.data_quality
        partitions = sum(item is not None for item in (realtime, historical, quality))

        if partitions > 1:
            summary = "combined"
        elif realtime is not None:
            summary = "realtime_only"
        elif historical is not None:
            summary = "historical_only"
        else:
            summary = "quality_only"

        insights: list[str] = []
        risks: list[str] = []
        actions: list[str] = []
        if realtime is not None:
            risks.append("cumulative_metric_limit")
            actions.append("add_time_window_metrics")
            if realtime.pv is not None and realtime.uv is not None and realtime.uv > 0:
                insights.append("visits_per_user")
            elif realtime.uv == 0:
                risks.append("zero_uv")
        if historical is not None:
            if historical.event_count is not None:
                insights.append("historical_event_count")
            if historical.event_count and historical.event_type_counts:
                insights.append("top_event_type_share")
        if quality is not None:
            insights.append("checkpoint_status")
            rejected = _quality_rejections(context)
            if rejected is not None:
                insights.append("quality_event_counts")
                if rejected > 0:
                    risks.append("quality_rejections")
                    actions.append("inspect_quality_rejections")
            if quality.failed_checkpoints > 0:
                risks.append("failed_checkpoints")
                actions.append("inspect_failed_checkpoints")
        if partitions < 3:
            risks.append("partial_evidence")

        return ToolAnalysisSelection(
            summary=summary,
            insights=insights,
            risks=risks,
            actions=actions,
        )

    def analyze(self, context: ToolAnalysisContext) -> AnalysisNarrative:
        return render_tool_selection(self.select(context), context)


def render_tool_selection(
    selection: ToolAnalysisSelection,
    context: ToolAnalysisContext,
) -> AnalysisNarrative:
    # Revalidate serialized data so a caller cannot bypass Pydantic by mutating list fields.
    selection = ToolAnalysisSelection.model_validate(selection.model_dump())
    realtime = context.evidence.realtime
    historical = context.evidence.historical
    quality = context.evidence.data_quality
    partition_count = sum(item is not None for item in (realtime, historical, quality))

    if selection.summary == "realtime_only":
        if realtime is None:
            raise ValueError("realtime_only requires realtime evidence")
        summary = _realtime_summary(context)
    elif selection.summary == "historical_only":
        if historical is None:
            raise ValueError("historical_only requires historical evidence")
        summary = _historical_summary(context)
    elif selection.summary == "quality_only":
        if quality is None:
            raise ValueError("quality_only requires data quality evidence")
        summary = _quality_summary(context)
    else:
        if partition_count < 2:
            raise ValueError("combined requires multiple evidence partitions")
        summary = _combined_summary(context)

    insights = [_render_insight(claim, context) for claim in selection.insights]
    risks = [_render_risk(claim, context) for claim in selection.risks]
    actions = [_render_action(claim, context) for claim in selection.actions]
    return AnalysisNarrative(summary=summary, insights=insights, risks=risks, actions=actions)


def allowed_tool_narrative_numbers(context: ToolAnalysisContext) -> set[Decimal]:
    allowed: set[Decimal] = set()
    for value in context.evidence.model_dump().values():
        _collect_numbers(value, allowed)

    realtime = context.evidence.realtime
    if realtime is not None and realtime.pv is not None and realtime.uv not in (None, 0):
        allowed.add(Decimal(f"{realtime.pv / realtime.uv:.1f}"))
    historical = context.evidence.historical
    if historical is not None and historical.event_count and historical.event_type_counts:
        _, top_count = max(historical.event_type_counts.items(), key=lambda item: item[1])
        allowed.add(Decimal(f"{top_count / historical.event_count:.1%}".removesuffix("%")))
    rejected = _quality_rejections(context)
    if rejected is not None:
        allowed.add(Decimal(rejected))
    return allowed


def _collect_numbers(value: object, allowed: set[Decimal]) -> None:
    if isinstance(value, dict):
        for item in value.values():
            _collect_numbers(item, allowed)
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            _collect_numbers(item, allowed)
    elif isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        allowed.add(Decimal(str(value)))


def _realtime_summary(context: ToolAnalysisContext) -> str:
    realtime = context.evidence.realtime
    if realtime is None:
        raise ValueError("realtime summary requires realtime evidence")
    if realtime.pv is None or realtime.uv is None:
        return "实时 PV/UV 指标不完整，当前只能提供有限分析。"
    return f"当前累计访问 {realtime.pv} 次，覆盖 {realtime.uv} 名用户。"


def _historical_summary(context: ToolAnalysisContext) -> str:
    historical = context.evidence.historical
    if historical is None:
        raise ValueError("historical summary requires historical evidence")
    if historical.event_count is None:
        return "历史行为证据已获取，但事件总量暂不可用。"
    return f"历史明细共包含 {historical.event_count} 条行为事件。"


def _quality_summary(context: ToolAnalysisContext) -> str:
    quality = context.evidence.data_quality
    if quality is None:
        raise ValueError("quality summary requires data quality evidence")
    return f"数据质量作业已完成 {quality.completed_checkpoints} 个 checkpoint。"


def _combined_summary(context: ToolAnalysisContext) -> str:
    quality = context.evidence.data_quality
    if quality is not None:
        return f"综合证据显示，数据质量作业已完成 {quality.completed_checkpoints} 个 checkpoint。"
    return "已获取多类证据，可进行交叉核验。"


def _render_insight(claim: str, context: ToolAnalysisContext) -> str:
    realtime = context.evidence.realtime
    historical = context.evidence.historical
    quality = context.evidence.data_quality
    if claim == "visits_per_user":
        if realtime is None or realtime.pv is None or realtime.uv is None or realtime.uv <= 0:
            raise ValueError("visits_per_user requires positive UV evidence")
        return f"人均访问次数约为 {realtime.pv / realtime.uv:.1f} 次。"
    if claim == "historical_event_count":
        if historical is None or historical.event_count is None:
            raise ValueError("historical_event_count requires historical evidence")
        return f"历史明细共包含 {historical.event_count} 条行为事件。"
    if claim == "top_event_type_share":
        if historical is None or not historical.event_type_counts or not historical.event_count:
            raise ValueError("top_event_type_share requires non-empty historical evidence")
        top_type, top_count = max(historical.event_type_counts.items(), key=lambda item: item[1])
        return f"历史行为以 {top_type} 为主，占比约 {top_count / historical.event_count:.1%}。"
    if claim == "quality_event_counts":
        rejected = _quality_rejections(context)
        if rejected is None:
            raise ValueError("quality_event_counts requires data quality evidence")
        return f"数据质量计数中累计异常或拒绝事件为 {rejected} 条。"
    if claim == "checkpoint_status":
        if quality is None:
            raise ValueError("checkpoint_status requires data quality evidence")
        return (
            f"作业状态为 {quality.job_state}，已完成 {quality.completed_checkpoints} 个 checkpoint，"
            f"失败 {quality.failed_checkpoints} 个。"
        )
    raise ValueError("unsupported insight claim")


def _render_risk(claim: str, context: ToolAnalysisContext) -> str:
    realtime = context.evidence.realtime
    quality = context.evidence.data_quality
    if claim == "cumulative_metric_limit":
        if realtime is None:
            raise ValueError("cumulative_metric_limit requires realtime evidence")
        return "当前实时指标为累计值，缺少时间窗口对照时不能判断上涨或下降。"
    if claim == "zero_uv":
        if realtime is None or realtime.uv != 0:
            raise ValueError("zero_uv requires zero UV evidence")
        return "UV 为 0，当前证据不足以计算人均访问次数。"
    if claim == "failed_checkpoints":
        if quality is None or quality.failed_checkpoints <= 0:
            raise ValueError("failed_checkpoints requires failed checkpoint evidence")
        return "存在失败 checkpoint，数据质量链路需要进一步核验。"
    if claim == "quality_rejections":
        rejected = _quality_rejections(context)
        if rejected is None or rejected <= 0:
            raise ValueError("quality_rejections requires rejected event evidence")
        return "存在异常或拒绝事件，相关数据应在使用前复核。"
    if claim == "partial_evidence":
        if sum(item is not None for item in context.evidence.model_dump().values()) >= 3:
            raise ValueError("partial_evidence requires incomplete evidence")
        return "当前仅取得部分证据，结论不应外推到缺失的数据范围。"
    raise ValueError("unsupported risk claim")


def _render_action(claim: str, context: ToolAnalysisContext) -> str:
    quality = context.evidence.data_quality
    if claim == "add_time_window_metrics":
        if context.evidence.realtime is None:
            raise ValueError("add_time_window_metrics requires realtime evidence")
        return "补充分级时间窗口指标后，再判断变化趋势。"
    if claim == "inspect_quality_rejections":
        rejected = _quality_rejections(context)
        if rejected is None or rejected <= 0:
            raise ValueError("inspect_quality_rejections requires rejected event evidence")
        return "检查异常或拒绝事件的来源与处理结果。"
    if claim == "inspect_failed_checkpoints":
        if quality is None or quality.failed_checkpoints <= 0:
            raise ValueError("inspect_failed_checkpoints requires failed checkpoint evidence")
        return "检查失败 checkpoint 的原因并确认恢复状态。"
    raise ValueError("unsupported action claim")


def _quality_rejections(context: ToolAnalysisContext) -> int | None:
    quality = context.evidence.data_quality
    if quality is None:
        return None
    return (
        quality.counters["dlq_events_total"]
        + quality.counters["late_events_total"]
    )


class OpenAICompatibleToolNarrativeAnalyzer:
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

    def select(self, context: ToolAnalysisContext) -> ToolAnalysisSelection:
        payload = {
            "model": self._model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Return only a JSON ToolAnalysisSelection. Select only IDs supported by the "
                        "provided evidence. Do not return prose, numbers, SQL, code, URLs, or explanations."
                    ),
                },
                {"role": "user", "content": context.model_dump_json()},
            ],
            "temperature": 0,
        }
        with self._client_factory() as client:
            response = client.post(
                f"{self._base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self._api_key}"},
                json=payload,
                timeout=remaining_timeout(self._timeout_seconds),
            )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        return ToolAnalysisSelection.model_validate(json.loads(content))

    def analyze(self, context: ToolAnalysisContext) -> AnalysisNarrative:
        return render_tool_selection(self.select(context), context)
