from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from app.behavior_models import (
    BehaviorMetricMeta,
    DimensionRanking,
    FunnelPoint,
    FunnelResponse,
    MetricDefinitionsResponse,
    OverviewPoint,
    OverviewResponse,
    PublicationData,
    PublicationResponse,
    QualityMetrics,
    QualityResponse,
    RankingsResponse,
)
from app.behavior_repository import BehaviorMetricsRepository
from app.metric_definitions import MetricDefinitionCatalog


_DATASET_ID = "rees46-multicategory"
_METRIC_VERSION = "behavior-v1"
_SCOPE_COUNTS = {
    "g2c-correctness-subset": 1002,
    "stable-user-2pct-full": 2_199_938,
}
_SUBSET_WARNING = "correctness subset; not the full 2% user sample"
_RATE_QUANTUM = Decimal("0.000001")


class BehaviorMetricsUnavailableError(RuntimeError):
    pass


class BehaviorMetricsService:
    def __init__(
        self,
        repository: BehaviorMetricsRepository,
        catalog: MetricDefinitionCatalog,
    ) -> None:
        self._repository = repository
        self._catalog = catalog

    def get_publication(self) -> PublicationResponse:
        try:
            publication, meta = self._load_publication()
            return PublicationResponse(
                meta=meta,
                data=PublicationData(
                    published_at=_as_utc(publication["published_at"]),
                    overview_row_count=_as_int(publication["overview_row_count"]),
                    overview_sha256=str(publication["overview_sha256"]),
                    funnel_row_count=_as_int(publication["funnel_row_count"]),
                    funnel_sha256=str(publication["funnel_sha256"]),
                    dimension_row_count=_as_int(publication["dimension_row_count"]),
                    dimension_sha256=str(publication["dimension_sha256"]),
                    quality_row_count=_as_int(publication["quality_row_count"]),
                    quality_sha256=str(publication["quality_sha256"]),
                    status=publication["status"],
                ),
            )
        except BehaviorMetricsUnavailableError:
            raise
        except Exception:
            raise _unavailable() from None

    def get_overview(self, window: str) -> OverviewResponse:
        try:
            publication, meta = self._load_publication()
            rows = self._repository.fetch_overview(str(publication["metric_run_id"]), window)
            points = []
            for row in rows:
                self._validate_data_identity(row, publication)
                points.append(
                    OverviewPoint(
                        window_type=row["window_type"],
                        window_start=row["window_start"],
                        window_end=row["window_end"],
                        event_count=_as_int(row["event_count"]),
                        view_count=_as_int(row["view_count"]),
                        cart_count=_as_int(row["cart_count"]),
                        purchase_count=_as_int(row["purchase_count"]),
                        unique_user_count=_as_int(row["unique_user_count"]),
                        session_count=_as_int(row["session_count"]),
                        product_count=_as_int(row["product_count"]),
                        purchase_amount_proxy=_amount(row["purchase_amount_proxy"]),
                    )
                )
            points.sort(key=lambda point: (point.window_start, point.window_end))
            return OverviewResponse(meta=meta, data=points)
        except BehaviorMetricsUnavailableError:
            raise
        except Exception:
            raise _unavailable() from None

    def get_funnel(self, window: str) -> FunnelResponse:
        try:
            publication, meta = self._load_publication()
            rows = self._repository.fetch_funnel(str(publication["metric_run_id"]), window)
            points = []
            for row in rows:
                self._validate_data_identity(row, publication)
                view_sessions = _as_int(row["view_sessions"])
                view_to_cart_sessions = _as_int(row["view_to_cart_sessions"])
                completed_sessions = _as_int(row["completed_sessions"])
                points.append(
                    FunnelPoint(
                        window_type=row["window_type"],
                        window_start=row["window_start"],
                        window_end=row["window_end"],
                        missing_session_event_count=_as_int(
                            row["missing_session_event_count"]
                        ),
                        view_sessions=view_sessions,
                        view_to_cart_sessions=view_to_cart_sessions,
                        completed_sessions=completed_sessions,
                        view_to_cart_rate=_ratio(view_to_cart_sessions, view_sessions),
                        cart_to_purchase_rate=_ratio(
                            completed_sessions, view_to_cart_sessions
                        ),
                        full_conversion_rate=_ratio(completed_sessions, view_sessions),
                    )
                )
            points.sort(key=lambda point: (point.window_start, point.window_end))
            return FunnelResponse(meta=meta, data=points)
        except BehaviorMetricsUnavailableError:
            raise
        except Exception:
            raise _unavailable() from None

    def get_rankings(
        self,
        dimension: str,
        window: str,
        sort_by: str,
        limit: int,
    ) -> RankingsResponse:
        try:
            publication, meta = self._load_publication()
            rows = self._repository.fetch_rankings(
                str(publication["metric_run_id"]), dimension, window, sort_by, limit
            )
            rankings = []
            for row in rows:
                self._validate_data_identity(row, publication)
                rankings.append(
                    DimensionRanking(
                        window_type=row["window_type"],
                        window_start=row["window_start"],
                        window_end=row["window_end"],
                        dimension_type=row["dimension_type"],
                        dimension_id=str(row["dimension_id"]),
                        dimension_name=(
                            None
                            if row["dimension_name"] is None
                            else str(row["dimension_name"])
                        ),
                        is_unknown=_as_bool(row["is_unknown"]),
                        view_count=_as_int(row["view_count"]),
                        cart_count=_as_int(row["cart_count"]),
                        purchase_count=_as_int(row["purchase_count"]),
                        unique_user_count=_as_int(row["unique_user_count"]),
                        purchase_amount_proxy=_amount(row["purchase_amount_proxy"]),
                    )
                )
            rankings.sort(key=lambda ranking: (ranking.window_start, ranking.window_end))
            return RankingsResponse(meta=meta, data=rankings)
        except BehaviorMetricsUnavailableError:
            raise
        except Exception:
            raise _unavailable() from None

    def get_quality(self) -> QualityResponse:
        try:
            publication, meta = self._load_publication()
            row = self._repository.fetch_quality(str(publication["metric_run_id"]))
            if row is None:
                raise _unavailable()
            self._validate_data_identity(row, publication)
            source_event_count = _as_int(row["source_event_count"])
            if (
                source_event_count != meta.source_event_count
                or _as_int(row["overview_event_count"]) != meta.source_event_count
            ):
                raise _unavailable()
            return QualityResponse(
                meta=meta,
                data=QualityMetrics(
                    window_type=row["window_type"],
                    window_start=row["window_start"],
                    window_end=row["window_end"],
                    source_event_count=source_event_count,
                    clean_event_count=_as_int(row["clean_event_count"]),
                    late_event_count=_as_int(row["late_event_count"]),
                    clean_event_rate=_rate(row["clean_event_rate"]),
                    late_event_rate=_rate(row["late_event_rate"]),
                    distinct_event_count=_as_int(row["distinct_event_count"]),
                    duplicate_event_count=_as_int(row["duplicate_event_count"]),
                    missing_session_count=_as_int(row["missing_session_count"]),
                    unknown_category_count=_as_int(row["unknown_category_count"]),
                    unknown_brand_count=_as_int(row["unknown_brand_count"]),
                    invalid_event_type_count=_as_int(row["invalid_event_type_count"]),
                    empty_key_id_count=_as_int(row["empty_key_id_count"]),
                    invalid_price_count=_as_int(row["invalid_price_count"]),
                    invalid_derived_date_count=_as_int(
                        row["invalid_derived_date_count"]
                    ),
                    overview_event_count=_as_int(row["overview_event_count"]),
                    reconciliation_status=row["reconciliation_status"],
                ),
            )
        except BehaviorMetricsUnavailableError:
            raise
        except Exception:
            raise _unavailable() from None

    def get_definitions(self) -> MetricDefinitionsResponse:
        return MetricDefinitionsResponse(
            domain=self._catalog.domain,
            dataset_id=self._catalog.dataset_id,
            metric_version=self._catalog.metric_version,
            definitions=self._catalog.all(),
        )

    def _load_publication(self) -> tuple[Mapping[str, Any], BehaviorMetricMeta]:
        publication = self._repository.fetch_latest_publication()
        if publication is None:
            raise _unavailable()
        snapshot_id = _as_int(publication["source_snapshot_id"])
        source_event_count = _as_int(publication["source_event_count"])
        data_scope = publication["data_scope"]
        if (
            publication["status"] != "PUBLISHED"
            or publication["dataset_id"] != _DATASET_ID
            or publication["metric_version"] != _METRIC_VERSION
            or snapshot_id <= 0
            or publication["metric_run_id"] != f"{_METRIC_VERSION}-s{snapshot_id}"
            or data_scope not in _SCOPE_COUNTS
            or source_event_count != _SCOPE_COUNTS[data_scope]
            or publication["window_start"] > publication["window_end"]
        ):
            raise _unavailable()
        warnings = [_SUBSET_WARNING] if data_scope == "g2c-correctness-subset" else []
        meta = BehaviorMetricMeta(
            dataset_id=publication["dataset_id"],
            metric_version=publication["metric_version"],
            metric_run_id=publication["metric_run_id"],
            source_snapshot_id=str(snapshot_id),
            window_start=publication["window_start"],
            window_end=publication["window_end"],
            calculated_at=_as_utc(publication["calculated_at"]),
            data_scope=data_scope,
            source_event_count=source_event_count,
            warnings=warnings,
        )
        return publication, meta

    @staticmethod
    def _validate_data_identity(
        row: Mapping[str, Any], publication: Mapping[str, Any]
    ) -> None:
        expected = {
            "metric_run_id": publication["metric_run_id"],
            "dataset_id": _DATASET_ID,
            "metric_version": _METRIC_VERSION,
        }
        for field, value in expected.items():
            if field in row and row[field] != value:
                raise _unavailable()


def _unavailable() -> BehaviorMetricsUnavailableError:
    return BehaviorMetricsUnavailableError("behavior metrics are unavailable")


def _as_int(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("boolean is not an integer metric")
    return int(value)


def _as_bool(value: Any) -> bool:
    if value in (False, 0, "0"):
        return False
    if value in (True, 1, "1"):
        return True
    raise ValueError("invalid boolean metric")


def _as_utc(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("invalid Doris datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _amount(value: Any) -> str | None:
    if value is None:
        return None
    return format(Decimal(str(value)), ".2f")


def _rate(value: Any) -> str | None:
    if value is None:
        return None
    rate = Decimal(str(value)).quantize(_RATE_QUANTUM, rounding=ROUND_HALF_UP)
    return format(rate, "f")


def _ratio(numerator: int, denominator: int) -> str | None:
    if denominator == 0:
        return None
    return format(
        (Decimal(numerator) / Decimal(denominator)).quantize(
            _RATE_QUANTUM, rounding=ROUND_HALF_UP
        ),
        "f",
    )
