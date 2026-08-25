import logging
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
import sys
import threading
import time
import traceback
import unittest
from unittest.mock import Mock

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app.analysis_models import HistoricalEvidence
from app.flink_quality_repository import FlinkQualityRepository
from app.tool_executor import ToolExecutionPlanError, ToolExecutionUnavailableError, ToolExecutor
from app.tool_deadline import remaining_timeout, use_tool_deadline
from app.tool_models import DataQualityEvidence, ToolCall, ToolId, ToolPlan
from app.tool_runner import BoundedToolRunner, ToolRunnerUnavailableError


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


def timer_for(*values: object) -> Callable[[], float]:
    ticks = iter(values)

    def timer() -> float:
        value = next(ticks)
        if isinstance(value, BaseException):
            raise value
        return value  # type: ignore[return-value]

    return timer


def make_executor(
    realtime_result: object | None = None,
    historical_result: object | None = None,
    quality_result: object | None = None,
    max_calls: int = 3,
    total_timeout_seconds: float = 20,
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
    return ToolExecutor(
        realtime,
        historical,
        quality,
        max_calls,
        total_timeout_seconds,
        20,
        timer=timer,
    ), realtime, historical, quality


class CapturingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class ToolExecutorTest(unittest.TestCase):
    def test_worker_repository_deadline_includes_time_since_runner_submission(self):
        class ControlledTimer:
            def __init__(self) -> None:
                self._value = 0.0
                self._lock = threading.Lock()

            def __call__(self) -> float:
                with self._lock:
                    return self._value

            def set(self, value: float) -> None:
                with self._lock:
                    self._value = value

        timer = ControlledTimer()
        repository_entered = threading.Event()
        release_repository = threading.Event()
        observed_timeouts: list[float] = []

        class GatedRealtimeRepository:
            def fetch_all_metrics(self):
                repository_entered.set()
                if not release_repository.wait(timeout=1):
                    raise RuntimeError("repository gate timed out")
                observed_timeouts.append(remaining_timeout(20))
                return {"pv": 2, "uv": 2}

        runner = BoundedToolRunner(max_workers=1)
        executor = ToolExecutor(
            GatedRealtimeRepository(),
            Mock(),
            Mock(),
            3,
            20,
            20,
            timer=timer,
            runner=runner,
        )

        def advance_while_worker_is_gated() -> None:
            if repository_entered.wait(timeout=1):
                timer.set(18.0)
                release_repository.set()

        controller = threading.Thread(target=advance_while_worker_is_gated)
        controller.start()
        try:
            with use_tool_deadline(20, timer=timer):
                executor.execute(realtime_plan(), "audit-worker-delay")
        finally:
            release_repository.set()
            controller.join(timeout=1)
            runner.close()

        self.assertFalse(controller.is_alive())
        self.assertEqual([2.0], observed_timeouts)

    def test_executor_passes_the_request_remaining_budget_to_the_runner(self):
        class CapturingRunner:
            def __init__(self):
                self.timeouts: list[float] = []

            def run(self, operation, timeout_seconds):
                self.timeouts.append(timeout_seconds)
                return operation()

            def close(self):
                pass

        realtime = Mock()
        realtime.fetch_all_metrics.return_value = {"pv": 2, "uv": 2}
        runner = CapturingRunner()
        executor = ToolExecutor(realtime, Mock(), Mock(), 3, 20, 20, timer=lambda: 0.0, runner=runner)
        timer = iter((0.0, 18.0))

        with use_tool_deadline(20, timer=lambda: next(timer)):
            executor.execute(realtime_plan(), "audit-remaining-budget")

        self.assertEqual([2.0], runner.timeouts)

    def test_executor_maps_runner_unavailability_to_the_existing_safe_error(self):
        class UnavailableRunner:
            def __init__(self):
                self.call_count = 0

            def run(self, operation, timeout_seconds):
                self.call_count += 1
                if self.call_count == 1:
                    return operation()
                raise ToolRunnerUnavailableError("password=secret")

            def close(self):
                pass

        realtime = Mock()
        realtime.fetch_all_metrics.return_value = {"pv": 2, "uv": 2}
        runner = UnavailableRunner()
        executor = ToolExecutor(realtime, Mock(), Mock(), 3, 1, 20, runner=runner)
        plan = ToolPlan(
            calls=[
                ToolCall(tool_id=ToolId.REALTIME),
                ToolCall(tool_id=ToolId.HISTORICAL),
            ]
        )
        logger = logging.getLogger("app.tool_executor")
        handler = CapturingHandler()
        old_level = logger.level
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

        try:
            with self.assertRaisesRegex(ToolExecutionUnavailableError, "^tool execution unavailable$") as error:
                executor.execute(plan, "audit-runner-unavailable")
        finally:
            logger.removeHandler(handler)
            logger.setLevel(old_level)

        self.assertIsNone(error.exception.__cause__)
        self.assertNotIn("secret", "".join(traceback.format_exception(error.exception)))
        self.assertEqual([None, "budget_exceeded"], [record.error_type for record in handler.records])

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

    def test_executor_discards_prior_evidence_when_a_failed_call_reaches_the_total_budget(self):
        executor, realtime, historical, _ = make_executor(timer=timer_for(0.0, 0.0, 0.0, 0.0, 21.0))
        historical.fetch_summary.side_effect = RuntimeError("secret failure detail")
        plan = ToolPlan(
            calls=[
                ToolCall(tool_id=ToolId.REALTIME),
                ToolCall(tool_id=ToolId.HISTORICAL),
            ]
        )

        with self.assertRaisesRegex(ToolExecutionUnavailableError, "^tool execution unavailable$") as error:
            executor.execute(plan, "audit-5a")

        self.assertIsNone(error.exception.__cause__)
        self.assertNotIn("secret", "".join(traceback.format_exception(error.exception)))
        realtime.fetch_all_metrics.assert_called_once_with()
        historical.fetch_summary.assert_called_once_with()

    def test_executor_enforces_a_wall_clock_deadline_on_blocking_tools(self):
        executor, realtime, historical, quality = make_executor(total_timeout_seconds=0.05)
        blocked = threading.Event()
        realtime.fetch_all_metrics.side_effect = lambda **_: blocked.wait(0.5)

        started_at = time.perf_counter()
        with self.assertRaisesRegex(ToolExecutionUnavailableError, "^tool execution unavailable$"):
            executor.execute(composite_plan(), "audit-blocked")
        elapsed = time.perf_counter() - started_at

        self.assertLess(elapsed, 0.2)
        historical.fetch_summary.assert_not_called()
        quality.fetch_health.assert_not_called()

    def test_executor_deadline_interrupts_a_blocking_http_transport(self):
        def blocking_handler(request: httpx.Request) -> httpx.Response:
            time.sleep(0.5)
            return httpx.Response(500)

        quality = FlinkQualityRepository(
            base_url="http://flink:8081",
            production_job_name="chapter-9-datastream-quality-production",
            timeout_seconds=1,
            checkpoint_max_age_seconds=120,
            clock=lambda: datetime.now(UTC),
            client_factory=lambda: httpx.Client(
                transport=httpx.MockTransport(blocking_handler)
            ),
        )
        executor = ToolExecutor(Mock(), Mock(), quality, 3, 0.05, 20)

        started_at = time.perf_counter()
        with self.assertRaisesRegex(ToolExecutionUnavailableError, "^tool execution unavailable$"):
            executor.execute(
                ToolPlan(calls=[ToolCall(tool_id=ToolId.DATA_QUALITY)]),
                "audit-blocked-transport",
            )

        self.assertLess(time.perf_counter() - started_at, 0.2)

    def test_executor_scrubs_a_failure_from_the_first_timer_call(self):
        executor, realtime, _, _ = make_executor(timer=timer_for(RuntimeError("timer secret")))

        with self.assertRaisesRegex(ToolExecutionUnavailableError, "^tool execution unavailable$") as error:
            executor.execute(realtime_plan(), "audit-5b")

        self.assertIsNone(error.exception.__cause__)
        self.assertNotIn("timer secret", "".join(traceback.format_exception(error.exception)))
        realtime.fetch_all_metrics.assert_not_called()

    def test_executor_scrubs_a_timer_failure_after_a_repository_call(self):
        executor, realtime, _, _ = make_executor(timer=timer_for(0.0, 0.0, RuntimeError("timer secret")))

        with self.assertRaisesRegex(ToolExecutionUnavailableError, "^tool execution unavailable$") as error:
            executor.execute(realtime_plan(), "audit-5c")

        self.assertIsNone(error.exception.__cause__)
        self.assertNotIn("timer secret", "".join(traceback.format_exception(error.exception)))
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
