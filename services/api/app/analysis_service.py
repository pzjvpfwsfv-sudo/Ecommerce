from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal
import logging
import re
from time import perf_counter
from typing import Any, Protocol
import unicodedata
from uuid import uuid4

from app.analysis_models import (
    AnalysisContext,
    AnalysisEvidence,
    AnalysisNarrative,
    AnalysisResponse,
    HistoricalEvidence,
    RealtimeEvidence,
)
from app.analyzers import MetricAnalyzer


logger = logging.getLogger(__name__)
_NUMBER_PATTERN = re.compile(
    r"(?<![\d.,])[-+]?(?:(?:(?:\d{1,3}(?:,\d{3})+)|\d+)(?:\.\d*)?|\.\d+)"
    r"(?:[eE][-+]?\d+)?%?(?![\d.,])"
)
_SPLIT_NUMBER_PATTERN = re.compile(r"\d(?:[\s_'\u2019\u02bc]+\d)")
_CHINESE_NUMBER_CHARACTERS = frozenset(
    "\u96f6\u3007\u4e00\u4e8c\u4e24\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d"
    "\u5341\u767e\u5343\u4e07\u4ebf\u5146\u58f9\u8d30\u53c1\u8086\u4f0d\u9646"
    "\u67d2\u634c\u7396\u62fe\u4f70\u4edf\u842c\u5104\u534a"
)


class NarrativeProvenanceError(RuntimeError):
    pass


def _numbers_in(value: Any, *, fail_closed: bool = False) -> set[Decimal]:
    if isinstance(value, dict):
        return (
            set().union(*(_numbers_in(item, fail_closed=fail_closed) for item in value.values()))
            if value
            else set()
        )
    if isinstance(value, (list, tuple, set)):
        return (
            set().union(*(_numbers_in(item, fail_closed=fail_closed) for item in value))
            if value
            else set()
        )
    if isinstance(value, bool) or value is None:
        return set()
    if isinstance(value, (int, float, Decimal)):
        return {Decimal(str(value))}

    text = value.isoformat() if isinstance(value, datetime) else str(value)
    normalized = unicodedata.normalize("NFKC", text)
    if fail_closed:
        if any(
            character in _CHINESE_NUMBER_CHARACTERS
            or (character.isnumeric() and not character.isdecimal())
            for character in text
        ):
            raise NarrativeProvenanceError("Narrative contains an unsupported numeric form")
        if _SPLIT_NUMBER_PATTERN.search(normalized):
            raise NarrativeProvenanceError("Narrative contains a split number")

    matches = list(_NUMBER_PATTERN.finditer(normalized))
    if fail_closed:
        residual = list(normalized)
        for match in matches:
            residual[match.start() : match.end()] = " " * (match.end() - match.start())
        if any(
            character.isnumeric() or character in _CHINESE_NUMBER_CHARACTERS
            for character in residual
        ):
            raise NarrativeProvenanceError("Narrative contains an unsupported numeric form")

    return {
        Decimal(match.group().removesuffix("%").replace(",", ""))
        for match in matches
    }


def _allowed_narrative_numbers(context: AnalysisContext) -> set[Decimal]:
    allowed = _numbers_in(context.evidence.model_dump())
    realtime = context.evidence.realtime
    if realtime.pv is not None and realtime.uv not in (None, 0):
        allowed.add(Decimal(f"{realtime.pv / realtime.uv:.1f}"))

    historical = context.evidence.historical
    if historical and historical.event_count and historical.event_count > 0:
        for count in historical.event_type_counts.values():
            allowed.add(Decimal(f"{count / historical.event_count:.1%}".removesuffix("%")))
    return allowed


def _validate_narrative_numbers(narrative: AnalysisNarrative, context: AnalysisContext) -> None:
    allowed = _allowed_narrative_numbers(context)
    narrative_numbers = _numbers_in(
        [narrative.summary, *narrative.insights, *narrative.risks, *narrative.actions],
        fail_closed=True,
    )
    if not narrative_numbers.issubset(allowed):
        raise NarrativeProvenanceError("Narrative contains an ungrounded number")


def _log_event(
    level: int,
    event: str,
    request_id: str,
    analyzer: str,
    degraded: bool,
    error_type: str | None,
    **timings: float,
) -> None:
    logger.log(
        level,
        event,
        extra={
            "event": event,
            "request_id": request_id,
            "analyzer": analyzer,
            "degraded": degraded,
            "error_type": error_type,
            **timings,
        },
    )


class RealtimeRepository(Protocol):
    def fetch_all_metrics(self) -> dict[str, Any]: ...


class HistoricalRepository(Protocol):
    def fetch_summary(self) -> HistoricalEvidence: ...


class RealtimeDataUnavailableError(RuntimeError):
    pass


class AnalysisUnavailableError(RuntimeError):
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
            _log_event(
                logging.ERROR,
                "analysis_doris_failed",
                request_id,
                self._primary.name,
                False,
                type(exc).__name__,
                doris_ms=(perf_counter() - started_at) * 1000,
            )
            raise RealtimeDataUnavailableError("Doris realtime metrics are unavailable") from None
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
            _log_event(
                logging.WARNING,
                "analysis_trino_degraded",
                request_id,
                self._primary.name,
                True,
                type(exc).__name__,
                trino_ms=(perf_counter() - trino_started_at) * 1000,
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
            _validate_narrative_numbers(narrative, context)
        except Exception as exc:
            warnings.append("\u6a21\u578b\u5206\u6790\u6682\u4e0d\u53ef\u7528\uff0c\u5df2\u964d\u7ea7\u4e3a\u89c4\u5219\u5206\u6790\u3002")
            _log_event(
                logging.WARNING,
                "analysis_model_degraded",
                request_id,
                analyzer.name,
                True,
                type(exc).__name__,
                analyzer_ms=(perf_counter() - analyzer_started_at) * 1000,
            )
            analyzer = self._fallback
            fallback_started_at = perf_counter()
            try:
                fallback_context = context.model_copy(update={"warnings": warnings})
                narrative = analyzer.analyze(fallback_context)
                _validate_narrative_numbers(narrative, fallback_context)
            except Exception as fallback_exc:
                _log_event(
                    logging.ERROR,
                    "analysis_fallback_failed",
                    request_id,
                    analyzer.name,
                    True,
                    type(fallback_exc).__name__,
                    analyzer_ms=(perf_counter() - fallback_started_at) * 1000,
                )
                raise AnalysisUnavailableError("Metric analysis is unavailable") from None
        analyzer_ms = (perf_counter() - analyzer_started_at) * 1000

        _log_event(
            logging.INFO,
            "analysis_complete",
            request_id,
            analyzer.name,
            bool(warnings),
            None,
            doris_ms=doris_ms,
            trino_ms=trino_ms,
            analyzer_ms=analyzer_ms,
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
