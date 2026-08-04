import logging
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
import sys
import traceback
import unittest
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app.analysis_models import HistoricalEvidence
from app.tool_executor import ToolExecutionPlanError, ToolExecutionUnavailableError, ToolExecutor
from app.tool_models import DataQualityEvidence, ToolCall, ToolId, ToolPlan


COUNTERS = {
    "valid_events_total": 2,
    "dlq_events_total": 0,
    "late_events_total": 0,
    "duplicate_events_total": 0,
    "parse_errors_total": 0,
    "validation_errors_total": 0,
}


def valid_quality_evidence() -> DataQualityEvidence:
    return DataQualityEvidence(
        job_id="a" * 32,
        job_state="RUNNING",
        completed_checkpoints=1,
        failed_checkpoints=0,
        latest_completed_at=datetime(2026, 8, 3, tzinfo=UTC),
        counters=COUNTERS,
    )


def realtime_plan() -> ToolPlan:
    return ToolPlan(calls=[ToolCall(tool_id=ToolId.REALTIME)])


def historical_plan() -> ToolPlan:
    return ToolPlan(calls=[ToolCall(tool_id=ToolId.HISTORICAL)])


def composite_plan() -> ToolPlan:
    return ToolPlan(
        calls=[
            ToolCall(tool_id=ToolId.REALTIME),
            ToolCall(tool_id=ToolId.HISTORICAL),
            ToolCall(tool_id=ToolId.DATA_QUALITY),
        ]
    )


def timer_for(*values: float) -> Callable[[], float]:
    ticks = iter(values)
    return lambda: next(ticks)


def make_executor(
    realtime_result: object | None = None,
    historical_result: object | None = None,
    quality_result: object | None = None,
    max_calls: int = 3,
    timer: Callable[[], float] = lambda: 0.0,
) -> tuple[ToolExecutor, Mock, Mock, Mock]:
    realtime = Mock()
    realtime.fetch_all_metrics.return_value = realtime_result or {
        "pv": 2,
        "uv": 2,
        "updated_at": "2026-08-03T00:00:00Z",
    }
    historical = Mock()
    historical.fetch_summary.return_value = historical_result or HistoricalEvidence(
        event_count=2,
        event_type_counts={"view": 2},
        latest_event_time="2026-08-03T00:00:00Z",
    )
    quality = Mock()
    quality.fetch_health.return_value = quality_result or valid_quality_evidence()
    return ToolExecutor(realtime, historical, quality, max_calls, 20, 20, timer=timer), realtime, historical, quality


class CapturingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class ToolExecutorTest(unittest.TestCase):
    def test_executor_runs_fixed_registry_in_plan_order_and_builds_evidence(self):
        executor, realtime, historical, quality = make_executor()
        call_order: list[ToolId] = []
        realtime.fetch_all_metrics.side_effect = lambda: call_order.append(ToolId.REALTIME) or {
            "pv": 2,
            "uv": 2,
            "updated_at": "2026-08-03T00:00:00Z",
        }
        historical.fetch_summary.side_effect = lambda: call_order.append(ToolId.HISTORICAL) or HistoricalEvidence(
            event_count=2,
            event_type_counts={"view": 2},
            latest_event_time="2026-08-03T00:00:00Z",
        )
        quality.fetch_health.side_effect = lambda: call_order.append(ToolId.DATA_QUALITY) or valid_quality_evidence()

        result = executor.execute(composite_plan(), audit_id="audit-1")

        self.assertEqual(
            [ToolId.REALTIME, ToolId.HISTORICAL, ToolId.DATA_QUALITY],
            call_order,
        )
        self.assertEqual(
            [ToolId.REALTIME, ToolId.HISTORICAL, ToolId.DATA_QUALITY],
            [summary.tool_id for summary in result.tool_calls],
        )
        self.assertEqual(3, len(result.tool_calls))
        self.assertIsNotNone(result.evidence.realtime)
        self.assertIsNotNone(result.evidence.historical)
        self.assertIsNotNone(result.evidence.data_quality)

    def test_executor_keeps_successful_evidence_when_one_tool_fails_without_logging_details(self):
        executor, _, historical, _ = make_executor()
        historical.fetch_summary.side_effect = RuntimeError("SELECT secret FROM credentials https://internal.invalid")
        logger = logging.getLogger("app.tool_executor")
        handler = CapturingHandler()
        logger.addHandler(handler)
        try:
            result = executor.execute(composite_plan(), "audit-2")
        finally:
            logger.removeHandler(handler)

        self.assertIsNotNone(result.evidence.realtime)
        self.assertIsNone(result.evidence.historical)
        self.assertIsNotNone(result.evidence.data_quality)
        self.assertTrue(result.degraded)
        self.assertEqual("failed", result.tool_calls[1].status)
        self.assertEqual("tool_failure", result.tool_calls[1].error_type)
        rendered = result.model_dump_json() + "\n".join(
            f"{record.getMessage()} {record.__dict__}" for record in handler.records
        )
        for forbidden in ("SELECT", "secret", "https://", "credentials", "traceback"):
            self.assertNotIn(forbidden, rendered)
        self.assertTrue(all(record.exc_info is None for record in handler.records))

    def test_executor_raises_safe_error_when_all_tools_fail_or_budget_expires(self):
        failing_executor, failing_realtime, failing_historical, failing_quality = make_executor()
        failing_realtime.fetch_all_metrics.side_effect = RuntimeError("secret")
        failing_historical.fetch_summary.side_effect = RuntimeError("secret")
        failing_quality.fetch_health.side_effect = RuntimeError("secret")

        with self.assertRaisesRegex(ToolExecutionUnavailableError, "^tool execution unavailable$") as all_failed:
            failing_executor.execute(composite_plan(), "audit-3")
        self.assertIsNone(all_failed.exception.__cause__)
        self.assertNotIn("secret", "".join(traceback.format_exception(all_failed.exception)))

        expired_executor, expired_realtime, _, _ = make_executor(timer=timer_for(0.0, 21.0))
        with self.assertRaisesRegex(ToolExecutionUnavailableError, "^tool execution unavailable$"):
            expired_executor.execute(realtime_plan(), "audit-4")
        expired_realtime.fetch_all_metrics.assert_not_called()

    def test_executor_discards_evidence_when_a_completed_call_exceeds_the_total_budget(self):
        executor, realtime, _, _ = make_executor(timer=timer_for(0.0, 0.0, 21.0))

        with self.assertRaisesRegex(ToolExecutionUnavailableError, "^tool execution unavailable$"):
            executor.execute(realtime_plan(), "audit-5")

        realtime.fetch_all_metrics.assert_called_once_with()

    def test_executor_rejects_the_entire_mutated_or_over_limit_plan_before_any_call(self):
        mutated = realtime_plan()
        mutated.calls.append(ToolCall(tool_id=ToolId.REALTIME))
        executor, realtime, historical, quality = make_executor()

        with self.assertRaisesRegex(ToolExecutionPlanError, "^tool plan is not executable$") as invalid:
            executor.execute(mutated, "audit-6")
        self.assertIsNone(invalid.exception.__cause__)
        realtime.fetch_all_metrics.assert_not_called()
        historical.fetch_summary.assert_not_called()
        quality.fetch_health.assert_not_called()

        limited_executor, limited_realtime, limited_historical, limited_quality = make_executor(max_calls=1)
        with self.assertRaisesRegex(ToolExecutionPlanError, "^tool plan is not executable$"):
            limited_executor.execute(composite_plan(), "audit-7")
        limited_realtime.fetch_all_metrics.assert_not_called()
        limited_historical.fetch_summary.assert_not_called()
        limited_quality.fetch_health.assert_not_called()

    def test_executor_never_returns_untrusted_historical_labels_in_evidence(self):
        unsafe_historical = HistoricalEvidence(
            event_count=2,
            event_type_counts={"SELECT https://internal.invalid": 2},
            latest_event_time="2026-08-03T00:00:00Z",
        )
        executor, _, _, _ = make_executor(historical_result=unsafe_historical)

        with self.assertRaisesRegex(ToolExecutionUnavailableError, "^tool execution unavailable$") as error:
            executor.execute(historical_plan(), "audit-8")

        rendered = "".join(traceback.format_exception(error.exception))
        self.assertNotIn("SELECT", rendered)
        self.assertNotIn("https://", rendered)

    def test_executor_never_returns_an_untrusted_quality_job_identifier_in_evidence(self):
        unsafe_quality = valid_quality_evidence().model_copy(
            update={"job_id": "https://internal.invalid/credentials"}
        )
        executor, _, _, _ = make_executor(quality_result=unsafe_quality)

        with self.assertRaisesRegex(ToolExecutionUnavailableError, "^tool execution unavailable$") as error:
            executor.execute(ToolPlan(calls=[ToolCall(tool_id=ToolId.DATA_QUALITY)]), "audit-9")

        rendered = "".join(traceback.format_exception(error.exception))
        self.assertNotIn("https://", rendered)
        self.assertNotIn("credentials", rendered)


if __name__ == "__main__":
    unittest.main()
