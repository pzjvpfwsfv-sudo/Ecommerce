from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
import logging
from time import perf_counter
from typing import Any, Protocol
from uuid import uuid4

from app.analysis_models import (
    AnalysisContext,
    AnalysisEvidence,
    AnalysisResponse,
    HistoricalEvidence,
    RealtimeEvidence,
)
from app.analyzers import MetricAnalyzer


logger = logging.getLogger(__name__)


class RealtimeRepository(Protocol):
    def fetch_all_metrics(self) -> dict[str, Any]: ...


class HistoricalRepository(Protocol):
    def fetch_summary(self) -> HistoricalEvidence: ...


class RealtimeDataUnavailableError(RuntimeError):
    pass


class AnalysisService:
    def __init__(
        self,
        realtime_repository: RealtimeRepository,
        historical_repository: HistoricalRepository,
        primary_analyzer: MetricAnalyzer,
        fallback_analyzer: MetricAnalyzer,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._realtime = realtime_repository
        self._historical = historical_repository
        self._primary = primary_analyzer
        self._fallback = fallback_analyzer
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def analyze(self, question: str) -> AnalysisResponse:
        request_id = str(uuid4())
        warnings: list[str] = []
        started_at = perf_counter()
        try:
            raw_realtime = self._realtime.fetch_all_metrics()
        except Exception as exc:
            logger.exception(
                "analysis_doris_failed request_id=%s doris_ms=%.2f error_type=%s",
                request_id,
                (perf_counter() - started_at) * 1000,
                type(exc).__name__,
            )
            raise RealtimeDataUnavailableError("Doris realtime metrics are unavailable") from exc
        doris_ms = (perf_counter() - started_at) * 1000

        realtime = RealtimeEvidence(
            pv=self._optional_int(raw_realtime.get("pv")),
            uv=self._optional_int(raw_realtime.get("uv")),
            updated_at=raw_realtime.get("updated_at"),
        )

        historical = None
        trino_started_at = perf_counter()
        try:
            historical = self._historical.fetch_summary()
        except Exception as exc:
            warnings.append("\u5386\u53f2\u6570\u636e\u6682\u4e0d\u53ef\u7528\uff0c\u672c\u6b21\u4ec5\u57fa\u4e8e Doris \u5b9e\u65f6\u6307\u6807\u5206\u6790\u3002")
            logger.warning(
                "analysis_trino_degraded request_id=%s trino_ms=%.2f error_type=%s",
                request_id,
                (perf_counter() - trino_started_at) * 1000,
                type(exc).__name__,
            )
        trino_ms = (perf_counter() - trino_started_at) * 1000

        generated_at = self._clock()
        context = AnalysisContext(
            question=question,
            generated_at=generated_at,
            evidence=AnalysisEvidence(realtime=realtime, historical=historical),
            warnings=warnings,
        )

        analyzer = self._primary
        analyzer_started_at = perf_counter()
        try:
            narrative = analyzer.analyze(context)
        except Exception as exc:
            warnings.append("\u6a21\u578b\u5206\u6790\u6682\u4e0d\u53ef\u7528\uff0c\u5df2\u964d\u7ea7\u4e3a\u89c4\u5219\u5206\u6790\u3002")
            logger.warning(
                "analysis_model_degraded request_id=%s analyzer=%s error_type=%s",
                request_id,
                analyzer.name,
                type(exc).__name__,
            )
            analyzer = self._fallback
            narrative = analyzer.analyze(context.model_copy(update={"warnings": warnings}))
        analyzer_ms = (perf_counter() - analyzer_started_at) * 1000

        logger.info(
            "analysis_complete request_id=%s analyzer=%s doris_ms=%.2f trino_ms=%.2f analyzer_ms=%.2f degraded=%s",
            request_id,
            analyzer.name,
            doris_ms,
            trino_ms,
            analyzer_ms,
            bool(warnings),
        )

        return AnalysisResponse(
            **narrative.model_dump(),
            evidence=context.evidence,
            warnings=warnings,
            analyzer=analyzer.name,
            generated_at=generated_at,
        )

    @staticmethod
    def _optional_int(value: Any) -> int | None:
        return None if value is None else int(value)
