from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock
from uuid import UUID


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app.analysis_models import HistoricalEvidence, RealtimeEvidence
from app.tool_analysis_service import ToolAnalysisService, ToolAnalysisUnavailableError
from app.tool_executor import ToolExecutor
from app.tool_models import (
    ToolCall,
    ToolCallSummary,
    ToolAnalysisSelection,
    ToolEvidence,
    ToolExecutionResult,
    ToolId,
    ToolPlan,
)
from app.tool_narratives import RuleBasedToolNarrativeAnalyzer, ToolNarrativeAnalyzer
from app.tool_planners import ToolPlanner


def realtime_plan() -> ToolPlan:
    return ToolPlan(calls=[ToolCall(tool_id=ToolId.REALTIME)])


def successful_execution() -> ToolExecutionResult:
    return ToolExecutionResult(
        evidence=ToolEvidence(realtime=RealtimeEvidence(pv=2, uv=1)),
        tool_calls=[
            ToolCallSummary(
                tool_id=ToolId.REALTIME,
                status="success",
                duration_ms=1,
            )
        ],
    )


def historical_execution(event_type: str) -> ToolExecutionResult:
    return ToolExecutionResult(
        evidence=ToolEvidence(
            historical=HistoricalEvidence(event_count=2, event_type_counts={event_type: 2})
        ),
        tool_calls=[
            ToolCallSummary(
                tool_id=ToolId.HISTORICAL,
                status="success",
                duration_ms=1,
            )
        ],
    )


def service_with(
    primary_planner: Mock | None = None,
    fallback_planner: Mock | None = None,
    executor: Mock | None = None,
    primary_analyzer: Mock | None = None,
    fallback_analyzer: ToolNarrativeAnalyzer | None = None,
    audit_ids: Iterator[UUID] | None = None,
) -> tuple[ToolAnalysisService, Mock]:
    primary = primary_planner if primary_planner is not None else Mock(spec=ToolPlanner)
    primary.name = "openai_compatible"
    if primary_planner is None:
        primary.plan.return_value = realtime_plan()
    fallback = fallback_planner if fallback_planner is not None else Mock(spec=ToolPlanner)
    fallback.name = "rule_based"
    if fallback_planner is None:
        fallback.plan.return_value = realtime_plan()
    tool_executor = executor if executor is not None else Mock(spec=ToolExecutor)
    if executor is None:
        tool_executor.execute.return_value = successful_execution()
    analyzer = primary_analyzer if primary_analyzer is not None else Mock(spec=ToolNarrativeAnalyzer)
    if primary_analyzer is None:
        analyzer.name = "openai_compatible"
        analyzer.select.return_value = ToolAnalysisSelection(
            summary="realtime_only", insights=[], risks=[], actions=[]
        )
    return (
        ToolAnalysisService(
            primary,
            fallback,
            tool_executor,
            analyzer,
            fallback_analyzer if fallback_analyzer is not None else RuleBasedToolNarrativeAnalyzer(),
            clock=lambda: datetime(2026, 8, 3, tzinfo=UTC),
            audit_id_factory=(lambda: next(audit_ids)) if audit_ids else None,
        ),
        tool_executor,
    )


class ToolAnalysisServiceTest(unittest.TestCase):
    def test_service_rejects_primary_plan_before_execution_and_uses_rule_plan(self):
        primary = Mock(spec=ToolPlanner)
        primary.name = "openai_compatible"
        primary.plan.side_effect = ValueError("sensitive model output")
        service, executor = service_with(primary_planner=primary)

        with self.assertLogs("app.tool_analysis_service", level="INFO"):
            response = service.analyze("综合分析")

        self.assertEqual("rule_based", response.planner)
        self.assertTrue(response.degraded)
        executor.execute.assert_called_once()

    def test_invalid_analyzer_selection_falls_back_without_returning_model_prose(self):
        primary_analyzer = Mock(spec=ToolNarrativeAnalyzer)
        primary_analyzer.name = "openai_compatible"
        primary_analyzer.select.side_effect = ValueError("SELECT secret FROM https://internal.invalid")
        service, _ = service_with(primary_analyzer=primary_analyzer)

        with self.assertLogs("app.tool_analysis_service", level="WARNING") as captured:
            response = service.analyze("当前指标")

        rendered = response.model_dump_json() + "\n".join(captured.output)
        self.assertEqual("rule_based", response.analyzer)
        self.assertTrue(response.degraded)
        for forbidden in ("SELECT", "secret", "https://", "internal.invalid"):
            self.assertNotIn(forbidden, rendered)

    def test_prohibited_template_output_from_evidence_falls_back_to_safe_claims(self):
        executor = Mock(spec=ToolExecutor)
        executor.execute.return_value = historical_execution("INSERT INTO secret")
        primary_analyzer = Mock(spec=ToolNarrativeAnalyzer)
        primary_analyzer.name = "openai_compatible"
        primary_analyzer.select.return_value = ToolAnalysisSelection(
            summary="historical_only", insights=["top_event_type_share"], risks=[], actions=[]
        )
        fallback_analyzer = Mock(spec=ToolNarrativeAnalyzer)
        fallback_analyzer.name = "rule_based"
        fallback_analyzer.select.return_value = ToolAnalysisSelection(
            summary="historical_only", insights=["historical_event_count"], risks=[], actions=[]
        )
        service, _ = service_with(
            executor=executor,
            primary_analyzer=primary_analyzer,
            fallback_analyzer=fallback_analyzer,
        )

        with self.assertLogs("app.tool_analysis_service", level="WARNING"):
            response = service.analyze("历史行为")

        self.assertEqual("rule_based", response.analyzer)
        self.assertTrue(response.degraded)
        self.assertEqual(["历史明细共包含 2 条行为事件。"], response.insights)
        fallback_analyzer.select.assert_called_once()

    def test_prohibited_template_output_from_rule_fallback_returns_safe_error(self):
        executor = Mock(spec=ToolExecutor)
        executor.execute.return_value = historical_execution("INSERT INTO secret")
        selection = ToolAnalysisSelection(
            summary="historical_only", insights=["top_event_type_share"], risks=[], actions=[]
        )
        primary_analyzer = Mock(spec=ToolNarrativeAnalyzer)
        primary_analyzer.name = "openai_compatible"
        primary_analyzer.select.return_value = selection
        fallback_analyzer = Mock(spec=ToolNarrativeAnalyzer)
        fallback_analyzer.name = "rule_based"
        fallback_analyzer.select.return_value = selection
        service, _ = service_with(
            executor=executor,
            primary_analyzer=primary_analyzer,
            fallback_analyzer=fallback_analyzer,
        )

        with self.assertLogs("app.tool_analysis_service", level="ERROR"):
            with self.assertRaisesRegex(ToolAnalysisUnavailableError, "^tool analysis is unavailable$") as error:
                service.analyze("历史行为")

        self.assertIsNone(error.exception.__cause__)

    def test_service_runs_plan_validate_execute_claims_then_narrative_once(self):
        trace: list[str] = []
        primary = Mock(spec=ToolPlanner)
        primary.name = "openai_compatible"
        primary.plan.side_effect = lambda question: trace.append("plan") or realtime_plan()
        executor = Mock(spec=ToolExecutor)
        executor.execute.side_effect = lambda plan, audit_id: trace.append("execute") or successful_execution()
        analyzer = Mock(spec=ToolNarrativeAnalyzer)
        analyzer.name = "openai_compatible"
        analyzer.select.side_effect = lambda context: trace.append("claims") or ToolAnalysisSelection(
            summary="realtime_only", insights=[], risks=[], actions=[]
        )
        service, _ = service_with(primary_planner=primary, executor=executor, primary_analyzer=analyzer)

        service.analyze("当前指标")

        self.assertEqual(["plan", "execute", "claims"], trace)
        executor.execute.assert_called_once()

    def test_service_returns_unique_audit_and_safe_logs_without_exception_message(self):
        service, _ = service_with(
            audit_ids=iter(
                (
                    UUID("00000000-0000-0000-0000-000000000001"),
                    UUID("00000000-0000-0000-0000-000000000002"),
                )
            )
        )

        with self.assertLogs("app.tool_analysis_service", level="INFO") as captured:
            first = service.analyze("当前指标")
            second = service.analyze("当前指标")

        self.assertNotEqual(first.audit_id, second.audit_id)
        self.assertNotIn("secret", "\n".join(captured.output))

    def test_service_returns_safe_unavailable_error_when_execution_or_rule_narrative_fails(self):
        executor = Mock(spec=ToolExecutor)
        executor.execute.side_effect = RuntimeError("https://internal.invalid secret")
        service, _ = service_with(executor=executor)

        with self.assertLogs("app.tool_analysis_service", level="ERROR"):
            with self.assertRaisesRegex(ToolAnalysisUnavailableError, "^tool analysis is unavailable$") as error:
                service.analyze("当前指标")

        self.assertIsNone(error.exception.__cause__)


if __name__ == "__main__":
    unittest.main()
