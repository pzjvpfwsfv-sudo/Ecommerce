from collections.abc import Iterator
from datetime import UTC, datetime
import logging
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock
from uuid import UUID


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app.analysis_models import HistoricalEvidence, RealtimeEvidence
from app.tool_executor import ToolExecutor
from app.tool_analysis_service import ToolAnalysisService, ToolAnalysisUnavailableError
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


class CapturingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def real_executor(
    historical_failure: bool = False,
    all_fail: bool = False,
) -> ToolExecutor:
    realtime = Mock()
    realtime.fetch_all_metrics.return_value = {
        "pv": 2,
        "uv": 1,
        "updated_at": "2026-08-03T00:00:00Z",
    }
    historical = Mock()
    historical.fetch_summary.return_value = HistoricalEvidence(
        event_count=2,
        event_type_counts={"view": 2},
        latest_event_time="2026-08-03T00:00:00Z",
    )
    quality = Mock()
    if historical_failure:
        historical.fetch_summary.side_effect = RuntimeError(
            "SELECT secret FROM https://internal.invalid"
        )
    if all_fail:
        realtime.fetch_all_metrics.side_effect = RuntimeError("password=secret")
        historical.fetch_summary.side_effect = RuntimeError("SELECT secret")
        quality.fetch_health.side_effect = RuntimeError("https://internal.invalid")
    return ToolExecutor(realtime, historical, quality, 3, 1, 20)


def capture_app_logs(action):
    logger = logging.getLogger("app")
    handler = CapturingHandler()
    old_level = logger.level
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        action()
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)
    return handler.records


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
    def test_close_delegates_to_the_tool_executor(self):
        service, executor = service_with()

        service.close()

        executor.close.assert_called_once_with()

    def test_audit_events_are_stable_for_normal_and_planner_degraded_requests(self):
        normal_service, _ = service_with(executor=real_executor())
        normal_records = capture_app_logs(lambda: normal_service.analyze("password=secret"))
        self.assertEqual(
            [
                "tool_analysis_started",
                "tool_plan_selected",
                "tool_call_completed",
                "tool_analysis_completed",
            ],
            [record.getMessage() for record in normal_records],
        )

        primary = Mock(spec=ToolPlanner)
        primary.name = "openai_compatible"
        primary.plan.side_effect = ValueError("SELECT secret https://internal.invalid")
        degraded_service, _ = service_with(
            primary_planner=primary,
            executor=real_executor(),
        )
        degraded_records = capture_app_logs(
            lambda: degraded_service.analyze("password=secret")
        )
        self.assertEqual(
            [
                "tool_analysis_started",
                "tool_planner_degraded",
                "tool_plan_selected",
                "tool_call_completed",
                "tool_analysis_completed",
            ],
            [record.getMessage() for record in degraded_records],
        )
        selected = next(
            record for record in degraded_records
            if record.getMessage() == "tool_plan_selected"
        )
        self.assertEqual([ToolId.REALTIME.value], selected.selected_tools)
        rendered = "\n".join(
            f"{record.getMessage()} {record.__dict__}" for record in degraded_records
        )
        for forbidden in ("password=secret", "SELECT secret", "https://internal.invalid"):
            self.assertNotIn(forbidden, rendered)

    def test_audit_events_cover_partial_and_total_tool_failure(self):
        plan = ToolPlan(
            calls=[
                ToolCall(tool_id=ToolId.REALTIME),
                ToolCall(tool_id=ToolId.HISTORICAL),
            ]
        )
        primary = Mock(spec=ToolPlanner)
        primary.name = "openai_compatible"
        primary.plan.return_value = plan
        partial_service, _ = service_with(
            primary_planner=primary,
            executor=real_executor(historical_failure=True),
        )
        partial_records = capture_app_logs(
            lambda: partial_service.analyze("综合 password=secret")
        )
        partial_selected = next(
            record for record in partial_records
            if record.getMessage() == "tool_plan_selected"
        )
        self.assertEqual(
            [ToolId.REALTIME.value, ToolId.HISTORICAL.value],
            partial_selected.selected_tools,
        )
        self.assertEqual(
            [
                "tool_analysis_started",
                "tool_plan_selected",
                "tool_call_completed",
                "tool_call_failed",
                "tool_analysis_completed",
            ],
            [record.getMessage() for record in partial_records],
        )

        all_failed_service, _ = service_with(
            primary_planner=primary,
            executor=real_executor(all_fail=True),
        )

        def fail_analysis() -> None:
            with self.assertRaises(ToolAnalysisUnavailableError):
                all_failed_service.analyze("综合 password=secret")

        failed_records = capture_app_logs(fail_analysis)
        self.assertEqual(
            [
                "tool_analysis_started",
                "tool_plan_selected",
                "tool_call_failed",
                "tool_call_failed",
                "tool_analysis_failed",
            ],
            [record.getMessage() for record in failed_records],
        )

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

        responses = []
        records = capture_app_logs(lambda: responses.append(service.analyze("当前指标")))
        response = responses[0]

        rendered = response.model_dump_json() + "\n".join(
            f"{record.getMessage()} {record.__dict__}" for record in records
        )
        self.assertEqual("rule_based", response.analyzer)
        self.assertTrue(response.degraded)
        self.assertEqual(
            [
                "tool_analysis_started",
                "tool_plan_selected",
                "tool_analysis_completed",
            ],
            [record.getMessage() for record in records],
        )
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
