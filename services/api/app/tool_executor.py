from __future__ import annotations

from collections.abc import Callable, Mapping
import logging
import math
from queue import Empty, Queue
import re
from threading import Thread
from time import perf_counter

from pydantic import ValidationError

from app.analysis_models import HistoricalEvidence, RealtimeEvidence
from app.flink_quality_repository import FlinkQualityRepository
from app.repository import RealtimeMetricsRepository
from app.tool_models import (
    DataQualityEvidence,
    ToolCall,
    ToolCallSummary,
    ToolEvidence,
    ToolExecutionResult,
    ToolId,
    ToolPlan,
)
from app.trino_repository import TrinoAnalyticsRepository
from app.tool_deadline import use_tool_deadline


LOGGER = logging.getLogger(__name__)
_SAFE_AUDIT_ID = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
_SAFE_EVENT_TYPE = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,63}\Z")
_SAFE_FLINK_JOB_ID = re.compile(r"[0-9a-f]{32}\Z")


class ToolExecutionPlanError(ValueError):
    """Raised when a request cannot execute its complete tool plan."""


class ToolExecutionUnavailableError(RuntimeError):
    """Raised when no safe, in-budget evidence can be returned."""


EvidencePartition = RealtimeEvidence | HistoricalEvidence | DataQualityEvidence


class ToolExecutor:
    def __init__(
        self,
        realtime_repository: RealtimeMetricsRepository,
        historical_repository: TrinoAnalyticsRepository,
        quality_repository: FlinkQualityRepository,
        max_calls: int,
        total_timeout_seconds: float,
        max_event_types: int,
        timer: Callable[[], float] = perf_counter,
    ) -> None:
        if type(max_calls) is not int or not 1 <= max_calls <= len(ToolId):
            raise ValueError("tool max calls must be between 1 and 3")
        if (
            not isinstance(total_timeout_seconds, (int, float))
            or isinstance(total_timeout_seconds, bool)
            or not math.isfinite(total_timeout_seconds)
            or not 0 < total_timeout_seconds <= 60
        ):
            raise ValueError("tool total timeout must be between 0 and 60 seconds")
        if type(max_event_types) is not int or not 1 <= max_event_types <= 100:
            raise ValueError("tool max event types must be between 1 and 100")

        self._max_calls = max_calls
        self._total_timeout_seconds = float(total_timeout_seconds)
        self._max_event_types = max_event_types
        self._timer = timer
        self._realtime_repository = realtime_repository
        self._historical_repository = historical_repository
        self._quality_repository = quality_repository
        # This registry is intentionally fixed to private, read-only adapters.
        self._registry: dict[ToolId, Callable[[], EvidencePartition]] = {
            ToolId.REALTIME: self._fetch_realtime,
            ToolId.HISTORICAL: self._fetch_historical,
            ToolId.DATA_QUALITY: self._fetch_quality,
        }

    def execute(self, plan: ToolPlan, audit_id: str) -> ToolExecutionResult:
        calls = self._validate_plan(plan)
        safe_audit_id = self._safe_audit_id(audit_id)
        started_at = self._now()
        evidence: dict[str, EvidencePartition] = {}
        summaries: list[ToolCallSummary] = []

        for call in calls:
            call_started_at = self._now()
            if self._elapsed(started_at, call_started_at) >= self._total_timeout_seconds:
                self._log_call(safe_audit_id, call.tool_id, "failed", 0.0, "budget_exceeded")
                raise ToolExecutionUnavailableError("tool execution unavailable") from None

            try:
                remaining = self._remaining(started_at, call_started_at)
                partition = self._run_with_timeout(
                    self._registry[call.tool_id],
                    remaining,
                )
            except ToolExecutionUnavailableError:
                completed_at = self._now()
                duration_ms = self._duration_ms(call_started_at, completed_at)
                self._log_call(
                    safe_audit_id,
                    call.tool_id,
                    "failed",
                    duration_ms,
                    "budget_exceeded",
                )
                raise ToolExecutionUnavailableError("tool execution unavailable") from None
            except Exception:
                completed_at = self._now()
                duration_ms = self._duration_ms(call_started_at, completed_at)
                if self._elapsed(started_at, completed_at) >= self._total_timeout_seconds:
                    self._log_call(safe_audit_id, call.tool_id, "failed", duration_ms, "budget_exceeded")
                    raise ToolExecutionUnavailableError("tool execution unavailable") from None
                summaries.append(
                    ToolCallSummary(
                        tool_id=call.tool_id,
                        status="failed",
                        duration_ms=duration_ms,
                        error_type="tool_failure",
                    )
                )
                self._log_call(safe_audit_id, call.tool_id, "failed", duration_ms, "tool_failure")
                continue

            completed_at = self._now()
            duration_ms = self._duration_ms(call_started_at, completed_at)
            if self._elapsed(started_at, completed_at) >= self._total_timeout_seconds:
                self._log_call(safe_audit_id, call.tool_id, "failed", duration_ms, "budget_exceeded")
                raise ToolExecutionUnavailableError("tool execution unavailable") from None

            evidence[self._evidence_key(call.tool_id)] = partition
            summaries.append(
                ToolCallSummary(
                    tool_id=call.tool_id,
                    status="success",
                    duration_ms=duration_ms,
                )
            )
            self._log_call(safe_audit_id, call.tool_id, "success", duration_ms, None)

        if not evidence:
            raise ToolExecutionUnavailableError("tool execution unavailable") from None

        return ToolExecutionResult(
            evidence=ToolEvidence(**evidence),
            tool_calls=summaries,
            warnings=["Some requested evidence was unavailable."] if any(
                summary.status == "failed" for summary in summaries
            ) else [],
            degraded=any(summary.status == "failed" for summary in summaries),
        )

    def _validate_plan(self, plan: ToolPlan) -> list[ToolCall]:
        try:
            if not isinstance(plan, ToolPlan):
                raise TypeError
            validated = ToolPlan.model_validate(plan.model_dump())
        except (TypeError, ValidationError, ValueError):
            raise ToolExecutionPlanError("tool plan is not executable") from None
        if len(validated.calls) > self._max_calls:
            raise ToolExecutionPlanError("tool plan is not executable")
        return validated.calls

    def _fetch_realtime(self) -> RealtimeEvidence:
        metrics = self._realtime_repository.fetch_all_metrics()
        if not isinstance(metrics, Mapping):
            raise ValueError("realtime metrics were malformed")
        # Copy only typed aggregate fields, never arbitrary backend payload keys.
        return RealtimeEvidence.model_validate(
            {
                "pv": metrics.get("pv"),
                "uv": metrics.get("uv"),
                "updated_at": metrics.get("updated_at"),
            }
        )

    def _fetch_historical(self) -> HistoricalEvidence:
        evidence = HistoricalEvidence.model_validate(self._historical_repository.fetch_summary())
        counts = evidence.event_type_counts
        if len(counts) > self._max_event_types:
            raise ValueError("historical evidence has too many event types")
        if any(not _SAFE_EVENT_TYPE.fullmatch(event_type) for event_type in counts):
            raise ValueError("historical evidence contains an unsafe event type")
        if evidence.event_count is not None and sum(counts.values()) != evidence.event_count:
            raise ValueError("historical evidence counts were inconsistent")
        return evidence

    def _fetch_quality(self) -> DataQualityEvidence:
        evidence = DataQualityEvidence.model_validate(self._quality_repository.fetch_health())
        if not _SAFE_FLINK_JOB_ID.fullmatch(evidence.job_id):
            raise ValueError("quality evidence contains an unsafe job identifier")
        return evidence

    @staticmethod
    def _evidence_key(tool_id: ToolId) -> str:
        if tool_id is ToolId.REALTIME:
            return "realtime"
        if tool_id is ToolId.HISTORICAL:
            return "historical"
        return "data_quality"

    def _log_call(
        self,
        audit_id: str,
        tool_id: ToolId,
        status: str,
        duration_ms: float,
        error_type: str | None,
    ) -> None:
        LOGGER.info(
            "tool_call_completed" if status == "success" else "tool_call_failed",
            extra={
                "audit_id": audit_id,
                "tool_id": tool_id.value,
                "status": status,
                "duration_ms": duration_ms,
                "error_type": error_type,
            },
        )

    @staticmethod
    def _safe_audit_id(audit_id: str) -> str:
        return audit_id if isinstance(audit_id, str) and _SAFE_AUDIT_ID.fullmatch(audit_id) else "redacted"

    def _now(self) -> float:
        try:
            return self._timer()
        except Exception:
            raise ToolExecutionUnavailableError("tool execution unavailable") from None

    def _remaining(self, started_at: float, current_at: float) -> float:
        return self._total_timeout_seconds - self._elapsed(started_at, current_at)

    @staticmethod
    def _run_with_timeout(
        operation: Callable[[], EvidencePartition],
        timeout_seconds: float,
    ) -> EvidencePartition:
        outcomes: Queue[tuple[bool, object]] = Queue(maxsize=1)

        def run() -> None:
            try:
                with use_tool_deadline(timeout_seconds):
                    outcomes.put((True, operation()))
            except Exception as error:
                outcomes.put((False, error))

        worker = Thread(target=run, name="tool-execution", daemon=True)
        worker.start()
        worker.join(timeout_seconds)
        if worker.is_alive():
            raise ToolExecutionUnavailableError("tool execution unavailable") from None
        try:
            succeeded, value = outcomes.get_nowait()
        except Empty:
            raise ToolExecutionUnavailableError("tool execution unavailable") from None
        if not succeeded:
            raise value  # type: ignore[misc]
        return value  # type: ignore[return-value]

    @staticmethod
    def _elapsed(started_at: float, current_at: float) -> float:
        return max(0.0, current_at - started_at)

    @staticmethod
    def _duration_ms(started_at: float, completed_at: float) -> float:
        return ToolExecutor._elapsed(started_at, completed_at) * 1000
