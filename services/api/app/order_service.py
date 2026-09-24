from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
import json
import re
from typing import Any, TypeVar

from app.order_metric_definitions import (
    OrderMetricDefinitionCatalog,
    OrderMetricDefinitionsResponse,
)
from app.order_models import (
    CURATED_SNAPSHOT_TABLES,
    SOURCE_SNAPSHOT_TABLES,
    OrderDeliveryPoint,
    OrderDeliveryResponse,
    OrderMetricMeta,
    OrderOverviewPoint,
    OrderOverviewResponse,
    OrderPaymentPoint,
    OrderPaymentsResponse,
    OrderPublicationData,
    OrderPublicationResponse,
    OrderQualityMetrics,
    OrderQualityResponse,
    OrderRankingPoint,
    OrderRankingsResponse,
    OrderReviewPoint,
    OrderReviewsResponse,
)
from app.order_repository import (
    DIMENSIONS,
    RANK_SORT_COLUMNS,
    WINDOWS,
    OrderFamily,
    OrderMetricsRepository,
)


_DATASET_ID = "olist-brazilian-ecommerce-v2"
_METRIC_VERSION = "orders-v1"
_CURRENCY_WARNING = "源数据币种尚未核验，不展示货币符号或执行汇率换算。"
_FAMILIES: tuple[OrderFamily, ...] = (
    "overview",
    "delivery",
    "payment",
    "ranking",
    "review",
    "quality",
)
_SOURCE_ENTITIES = frozenset(
    {
        "orders",
        "order_items",
        "order_payments",
        "order_reviews",
        "customers",
        "products",
        "sellers",
        "geolocation",
        "category_translation",
    }
)
_CURATED_STAGE_PREFIXES = (
    "customer_dim",
    "category_dim",
    "product_dim",
    "seller_dim",
    "geolocation_dim",
    "order_fact",
    "order_item_fact",
    "payment_fact",
    "review_fact",
)
_FACT_RECONCILIATION_KEYS = frozenset(
    f"{prefix}_{suffix}"
    for prefix in _CURATED_STAGE_PREFIXES
    for suffix in ("expected_count", "row_count", "distinct_key_count")
)
_REPORTABLE_QUALITY_KEYS = frozenset(
    {
        "duplicate_review_id_count",
        "unknown_order_status_count",
        "unknown_payment_type_count",
        "missing_product_category_count",
        "missing_category_translation_count",
        "multi_review_order_count",
        "missing_optional_time_count",
        "lifecycle_order_anomaly_count",
        "payment_item_total_mismatch_count",
    }
)
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_HEX_40 = re.compile(r"^[0-9a-f]{40}$")
_SNAPSHOT_ID = re.compile(r"^[1-9][0-9]*$")
_COUNT_STRING = re.compile(r"^(?:0|[1-9][0-9]*)$")
_MONEY_QUANTUM = Decimal("0.01")
_SIX_QUANTUM = Decimal("0.000001")
_T = TypeVar("_T")


class OrderMetricsUnavailableError(RuntimeError):
    pass


class OrderMetricsRequestError(ValueError):
    pass


class OrderMetricsService:
    def __init__(
        self,
        repository: OrderMetricsRepository,
        catalog: OrderMetricDefinitionCatalog,
    ) -> None:
        self._repository = repository
        self._catalog = catalog

    def get_publication(self) -> OrderPublicationResponse:
        def load() -> OrderPublicationResponse:
            publication, meta = self._load_publication()
            for family in _FAMILIES:
                self._validate_family_count(publication, family)
            return OrderPublicationResponse(
                meta=meta,
                data=OrderPublicationData(
                    published_at=_as_utc(publication["published_at"]),
                    overview_row_count=_positive_int(publication["overview_row_count"]),
                    overview_sha256=_sha256(publication["overview_sha256"]),
                    delivery_row_count=_positive_int(publication["delivery_row_count"]),
                    delivery_sha256=_sha256(publication["delivery_sha256"]),
                    payment_row_count=_positive_int(publication["payment_row_count"]),
                    payment_sha256=_sha256(publication["payment_sha256"]),
                    ranking_row_count=_positive_int(publication["ranking_row_count"]),
                    ranking_sha256=_sha256(publication["ranking_sha256"]),
                    review_row_count=_positive_int(publication["review_row_count"]),
                    review_sha256=_sha256(publication["review_sha256"]),
                    quality_row_count=_positive_int(publication["quality_row_count"]),
                    quality_sha256=_sha256(publication["quality_sha256"]),
                    status=publication["status"],
                ),
            )

        return self._available(load)

    def get_overview(
        self,
        window: str,
        start_date: date | None,
        end_date: date | None,
    ) -> OrderOverviewResponse:
        _validate_query_shape(window, start_date, end_date)

        def load() -> OrderOverviewResponse:
            publication, meta = self._load_publication()
            _validate_range(start_date, end_date, meta.window_start, meta.window_end)
            self._validate_family_count(publication, "overview")
            rows = self._repository.fetch_overview(
                meta.metric_run_id, window, start_date, end_date
            )
            points = [
                self._overview_point(row, publication, meta, window, start_date, end_date)
                for row in rows
            ]
            _require_stable_order(points, lambda point: (point.window_start, point.window_end))
            if window == "full" and len(points) != 1:
                raise ValueError("full overview must contain exactly one row")
            return OrderOverviewResponse(meta=meta, data=points)

        return self._available(load)

    def get_delivery(
        self,
        window: str,
        start_date: date | None,
        end_date: date | None,
    ) -> OrderDeliveryResponse:
        _validate_query_shape(window, start_date, end_date)

        def load() -> OrderDeliveryResponse:
            publication, meta = self._load_publication()
            _validate_range(start_date, end_date, meta.window_start, meta.window_end)
            self._validate_family_count(publication, "delivery")
            rows = self._repository.fetch_delivery(
                meta.metric_run_id, window, start_date, end_date
            )
            points = [
                self._delivery_point(row, publication, window, start_date, end_date)
                for row in rows
            ]
            _require_stable_order(points, lambda point: (point.window_start, point.window_end))
            if window == "full" and len(points) != 1:
                raise ValueError("full delivery must contain exactly one row")
            return OrderDeliveryResponse(meta=meta, data=points)

        return self._available(load)

    def get_payments(
        self,
        window: str,
        start_date: date | None,
        end_date: date | None,
    ) -> OrderPaymentsResponse:
        _validate_query_shape(window, start_date, end_date)

        def load() -> OrderPaymentsResponse:
            publication, meta = self._load_publication()
            _validate_range(start_date, end_date, meta.window_start, meta.window_end)
            self._validate_family_count(publication, "payment")
            rows = self._repository.fetch_payments(
                meta.metric_run_id, window, start_date, end_date
            )
            points = [
                self._payment_point(row, publication, window, start_date, end_date)
                for row in rows
            ]
            _require_stable_order(
                points,
                lambda point: (
                    point.window_start,
                    point.window_end,
                    0 if point.is_all else 1,
                    point.payment_type,
                ),
            )
            self._validate_payment_windows(points)
            return OrderPaymentsResponse(meta=meta, data=points)

        return self._available(load)

    def get_rankings(
        self,
        dimension: str,
        window: str,
        start_date: date | None,
        end_date: date | None,
        sort_by: str,
        limit: int,
    ) -> OrderRankingsResponse:
        _validate_query_shape(window, start_date, end_date)
        if dimension not in DIMENSIONS:
            raise OrderMetricsRequestError("unsupported order metric dimension")
        if sort_by not in RANK_SORT_COLUMNS[dimension]:
            raise OrderMetricsRequestError("unsupported order metric sort for dimension")
        if type(limit) is not int or not 1 <= limit <= 100:
            raise OrderMetricsRequestError("order metric limit must be between 1 and 100")

        def load() -> OrderRankingsResponse:
            publication, meta = self._load_publication()
            _validate_range(start_date, end_date, meta.window_start, meta.window_end)
            self._validate_family_count(publication, "ranking")
            rows = self._repository.fetch_rankings(
                meta.metric_run_id,
                dimension,
                window,
                start_date,
                end_date,
                sort_by,
                limit,
            )
            points = [
                self._ranking_point(
                    row, publication, dimension, window, start_date, end_date
                )
                for row in rows
            ]
            _require_stable_order(
                points,
                lambda point: (
                    -_ranking_sort_value(point, sort_by),
                    point.dimension_id,
                    point.window_start,
                ),
            )
            return OrderRankingsResponse(meta=meta, data=points)

        return self._available(load)

    def get_reviews(
        self,
        window: str,
        start_date: date | None,
        end_date: date | None,
    ) -> OrderReviewsResponse:
        _validate_query_shape(window, start_date, end_date)

        def load() -> OrderReviewsResponse:
            publication, meta = self._load_publication()
            _validate_range(start_date, end_date, meta.window_start, meta.window_end)
            self._validate_family_count(publication, "review")
            rows = self._repository.fetch_reviews(
                meta.metric_run_id, window, start_date, end_date
            )
            points = [
                self._review_point(row, publication, window, start_date, end_date)
                for row in rows
            ]
            _require_stable_order(points, lambda point: (point.window_start, point.window_end))
            if window == "full" and len(points) != 1:
                raise ValueError("full reviews must contain exactly one row")
            return OrderReviewsResponse(meta=meta, data=points)

        return self._available(load)

    def get_quality(self) -> OrderQualityResponse:
        def load() -> OrderQualityResponse:
            publication, meta = self._load_publication()
            self._validate_family_count(publication, "quality")
            row = self._repository.fetch_quality(meta.metric_run_id)
            if row is None:
                raise ValueError("quality row is missing")
            self._validate_row(row, publication, "full", None, None)
            if row["reconciliation_status"] != "PASS":
                raise ValueError("quality reconciliation did not pass")

            raw_counts = _parse_integer_map(row["raw_row_counts_json"], _SOURCE_ENTITIES)
            normalized_counts = _parse_integer_map(
                row["normalized_row_counts_json"], _SOURCE_ENTITIES
            )
            iceberg_counts = _parse_integer_map(
                row["iceberg_row_counts_json"], SOURCE_SNAPSHOT_TABLES
            )
            normalized_hashes = _parse_hash_map(
                row["normalized_sha256_json"], _SOURCE_ENTITIES
            )
            source_snapshots = _parse_snapshot_map(
                row["source_snapshots_json"], SOURCE_SNAPSHOT_TABLES
            )
            curated_snapshots = _parse_snapshot_map(
                row["curated_snapshots_json"], CURATED_SNAPSHOT_TABLES
            )
            fact_reconciliations = _parse_integer_map(
                row["fact_reconciliations_json"],
                _FACT_RECONCILIATION_KEYS,
                allow_decimal_strings=True,
            )
            reportable_quality = _parse_integer_map(
                row["reportable_quality_json"],
                _REPORTABLE_QUALITY_KEYS,
                allow_decimal_strings=True,
            )
            if source_snapshots != meta.source_snapshots or curated_snapshots != meta.curated_snapshots:
                raise ValueError("quality snapshots differ from publication")
            source_count = _nonnegative_int(row["source_row_count"])
            iceberg_count = _nonnegative_int(row["iceberg_row_count"])
            if (
                source_count != meta.source_order_count
                or iceberg_count != meta.source_order_count
                or raw_counts["orders"] != meta.source_order_count
                or normalized_counts["orders"] != meta.source_order_count
                or iceberg_counts["orders_src_v1"] != meta.source_order_count
            ):
                raise ValueError("quality order counts differ from publication")
            for prefix in _CURATED_STAGE_PREFIXES:
                expected_count = fact_reconciliations[f"{prefix}_expected_count"]
                row_count = fact_reconciliations[f"{prefix}_row_count"]
                distinct_count = fact_reconciliations[f"{prefix}_distinct_key_count"]
                if expected_count != row_count or distinct_count != row_count:
                    raise ValueError("quality fact reconciliation failed")
            comparable = _nonnegative_int(row["amount_comparable_order_count"])
            reconciled = _nonnegative_int(row["amount_reconciled_order_count"])
            mismatch = _nonnegative_int(row["amount_mismatch_order_count"])
            if reconciled + mismatch != comparable:
                raise ValueError("quality amount counts do not reconcile")
            amount_rate = _rate(row["amount_reconciliation_rate"])
            _require_ratio(amount_rate, reconciled, comparable)
            return OrderQualityResponse(
                meta=meta,
                data=OrderQualityMetrics(
                    window_type=row["window_type"],
                    window_start=row["window_start"],
                    window_end=row["window_end"],
                    source_row_count=source_count,
                    iceberg_row_count=iceberg_count,
                    duplicate_key_count=_nonnegative_int(row["duplicate_key_count"]),
                    orphan_key_count=_nonnegative_int(row["orphan_key_count"]),
                    invalid_value_count=_nonnegative_int(row["invalid_value_count"]),
                    temporal_anomaly_count=_nonnegative_int(row["temporal_anomaly_count"]),
                    amount_comparable_order_count=comparable,
                    amount_reconciled_order_count=reconciled,
                    amount_mismatch_order_count=mismatch,
                    amount_reconciliation_rate=amount_rate,
                    payment_item_freight_abs_difference_avg=_six(
                        row["payment_item_freight_abs_difference_avg"]
                    ),
                    payment_item_freight_abs_difference_p50=_six(
                        row["payment_item_freight_abs_difference_p50"]
                    ),
                    payment_item_freight_abs_difference_p90=_six(
                        row["payment_item_freight_abs_difference_p90"]
                    ),
                    raw_row_counts=raw_counts,
                    normalized_row_counts=normalized_counts,
                    iceberg_row_counts=iceberg_counts,
                    normalized_sha256=normalized_hashes,
                    source_snapshots=source_snapshots,
                    curated_snapshots=curated_snapshots,
                    fact_reconciliations=fact_reconciliations,
                    reportable_quality=reportable_quality,
                    reconciliation_status=row["reconciliation_status"],
                ),
            )

        return self._available(load)

    def get_definitions(self) -> OrderMetricDefinitionsResponse:
        return self._available(
            lambda: OrderMetricDefinitionsResponse(
                domain=self._catalog.domain,
                dataset_id=self._catalog.dataset_id,
                metric_version=self._catalog.metric_version,
                definitions=self._catalog.all(),
            )
        )

    def _load_publication(self) -> tuple[Mapping[str, Any], OrderMetricMeta]:
        publication = self._repository.fetch_latest_publication()
        if publication is None:
            raise ValueError("publication is missing")
        if (
            publication["status"] != "PUBLISHED"
            or publication["dataset_id"] != _DATASET_ID
            or publication["metric_version"] != _METRIC_VERSION
        ):
            raise ValueError("publication identity is invalid")
        bundle = _sha256(publication["source_bundle_sha256"])
        if publication["metric_run_id"] != f"{_METRIC_VERSION}-b{bundle}":
            raise ValueError("publication run identity is invalid")
        revision = _hex_string(publication["implementation_revision"], _HEX_40)
        source_count = _positive_int(publication["source_order_count"])
        window_start = _date_value(publication["window_start"])
        window_end = _date_value(publication["window_end"])
        if window_start > window_end:
            raise ValueError("publication window is invalid")
        source_snapshots = _parse_snapshot_map(
            publication["source_snapshots_json"], SOURCE_SNAPSHOT_TABLES
        )
        curated_snapshots = _parse_snapshot_map(
            publication["curated_snapshots_json"], CURATED_SNAPSHOT_TABLES
        )
        for family in _FAMILIES:
            _positive_int(publication[f"{family}_row_count"])
            _sha256(publication[f"{family}_sha256"])
        meta = OrderMetricMeta(
            dataset_id=publication["dataset_id"],
            metric_version=publication["metric_version"],
            metric_run_id=publication["metric_run_id"],
            source_bundle_sha256=bundle,
            source_snapshots=source_snapshots,
            curated_snapshots=curated_snapshots,
            window_start=window_start,
            window_end=window_end,
            source_timezone="unspecified",
            source_currency=None,
            source_order_count=source_count,
            calculated_at=_as_utc(publication["calculated_at"]),
            implementation_revision=revision,
            warnings=[_CURRENCY_WARNING],
        )
        return publication, meta

    def _validate_family_count(
        self, publication: Mapping[str, Any], family: OrderFamily
    ) -> None:
        expected = _positive_int(publication[f"{family}_row_count"])
        actual = self._repository.fetch_family_row_count(
            str(publication["metric_run_id"]), family
        )
        if _nonnegative_int(actual) != expected:
            raise ValueError("published family row count differs from storage")

    def _validate_row(
        self,
        row: Mapping[str, Any],
        publication: Mapping[str, Any],
        window: str,
        start_date: date | None,
        end_date: date | None,
    ) -> tuple[date, date]:
        expected_identity = {
            "metric_run_id": publication["metric_run_id"],
            "dataset_id": _DATASET_ID,
            "metric_version": _METRIC_VERSION,
            "window_type": WINDOWS[window],
        }
        for field, expected in expected_identity.items():
            if row[field] != expected:
                raise ValueError("metric row identity is invalid")
        row_start = _date_value(row["window_start"])
        row_end = _date_value(row["window_end"])
        publication_start = _date_value(publication["window_start"])
        publication_end = _date_value(publication["window_end"])
        if not publication_start <= row_start <= row_end <= publication_end:
            raise ValueError("metric row window is outside publication")
        if window == "full" and (row_start != publication_start or row_end != publication_end):
            raise ValueError("full metric row window differs from publication")
        if start_date is not None and not (start_date <= row_start <= row_end <= end_date):
            raise ValueError("metric row is outside requested range")
        return row_start, row_end

    def _overview_point(
        self,
        row: Mapping[str, Any],
        publication: Mapping[str, Any],
        meta: OrderMetricMeta,
        window: str,
        start_date: date | None,
        end_date: date | None,
    ) -> OrderOverviewPoint:
        row_start, row_end = self._validate_row(
            row, publication, window, start_date, end_date
        )
        order_count = _nonnegative_int(row["order_count"])
        eligible = _nonnegative_int(row["status_eligible_order_count"])
        excluded = _nonnegative_int(row["status_excluded_order_count"])
        delivered = _nonnegative_int(row["delivered_order_count"])
        canceled = _nonnegative_int(row["canceled_order_count"])
        unavailable = _nonnegative_int(row["unavailable_order_count"])
        unique_customers = _nonnegative_int(row["unique_customer_count"])
        repeat_customers = _nonnegative_int(row["repeat_customer_count"])
        item_rows = _nonnegative_int(row["item_row_count"])
        if (
            eligible + excluded != order_count
            or max(delivered, canceled, unavailable) > eligible
            or repeat_customers > unique_customers
            or (window == "full" and order_count != meta.source_order_count)
        ):
            raise ValueError("overview counts do not reconcile")
        delivered_rate = _rate(row["delivered_rate"])
        canceled_rate = _rate(row["canceled_rate"])
        repeat_rate = _rate(row["repeat_customer_rate"])
        items_average = _six(row["items_per_order_avg"])
        _require_ratio(delivered_rate, delivered, eligible)
        _require_ratio(canceled_rate, canceled, eligible)
        _require_ratio(repeat_rate, repeat_customers, unique_customers)
        _require_ratio(items_average, item_rows, order_count)
        return OrderOverviewPoint(
            window_type=row["window_type"],
            window_start=row_start,
            window_end=row_end,
            order_count=order_count,
            delivered_order_count=delivered,
            canceled_order_count=canceled,
            unavailable_order_count=unavailable,
            status_eligible_order_count=eligible,
            status_excluded_order_count=excluded,
            delivered_rate=delivered_rate,
            canceled_rate=canceled_rate,
            unique_customer_count=unique_customers,
            repeat_customer_count=repeat_customers,
            repeat_customer_rate=repeat_rate,
            item_row_count=item_rows,
            item_value_sum=_money(row["item_value_sum"]),
            freight_value_sum=_money(row["freight_value_sum"]),
            payment_value_sum=_money(row["payment_value_sum"]),
            items_per_order_avg=items_average,
        )

    def _delivery_point(
        self,
        row: Mapping[str, Any],
        publication: Mapping[str, Any],
        window: str,
        start_date: date | None,
        end_date: date | None,
    ) -> OrderDeliveryPoint:
        row_start, row_end = self._validate_row(
            row, publication, window, start_date, end_date
        )
        late = _nonnegative_int(row["late_delivery_order_count"])
        late_eligible = _nonnegative_int(row["late_delivery_eligible_order_count"])
        if late > late_eligible:
            raise ValueError("late delivery numerator exceeds denominator")
        late_rate = _rate(row["late_delivery_rate"])
        _require_ratio(late_rate, late, late_eligible)
        return OrderDeliveryPoint(
            window_type=row["window_type"],
            window_start=row_start,
            window_end=row_end,
            delivery_eligible_order_count=_nonnegative_int(
                row["delivery_eligible_order_count"]
            ),
            delivery_excluded_order_count=_nonnegative_int(
                row["delivery_excluded_order_count"]
            ),
            delivery_days_avg=_six(row["delivery_days_avg"]),
            delivery_days_p50=_six(row["delivery_days_p50"]),
            delivery_days_p90=_six(row["delivery_days_p90"]),
            late_delivery_order_count=late,
            late_delivery_eligible_order_count=late_eligible,
            late_delivery_excluded_order_count=_nonnegative_int(
                row["late_delivery_excluded_order_count"]
            ),
            late_delivery_rate=late_rate,
        )

    def _payment_point(
        self,
        row: Mapping[str, Any],
        publication: Mapping[str, Any],
        window: str,
        start_date: date | None,
        end_date: date | None,
    ) -> OrderPaymentPoint:
        row_start, row_end = self._validate_row(
            row, publication, window, start_date, end_date
        )
        payment_type = _nonblank(row["payment_type"])
        is_all = _strict_bool(row["is_all"], allow_none=False)
        if (is_all and payment_type != "__ALL__") or (not is_all and payment_type == "__ALL__"):
            raise ValueError("payment all-row identity is invalid")
        global_orders = _nonnegative_int(row["global_order_count"])
        payment_orders = _nonnegative_int(row["payment_order_count"])
        type_orders = _nonnegative_int(row["payment_type_order_count"])
        installment_orders = _nonnegative_int(row["installment_order_count"])
        if (
            payment_orders != type_orders
            or payment_orders > global_orders
            or installment_orders > payment_orders
        ):
            raise ValueError("payment counts do not reconcile")
        return OrderPaymentPoint(
            window_type=row["window_type"],
            window_start=row_start,
            window_end=row_end,
            payment_type=payment_type,
            is_all=is_all,
            global_order_count=global_orders,
            payment_order_count=payment_orders,
            payment_row_count=_nonnegative_int(row["payment_row_count"]),
            installment_order_count=installment_orders,
            payment_value_sum=_money(row["payment_type_value_sum"]),
        )

    def _ranking_point(
        self,
        row: Mapping[str, Any],
        publication: Mapping[str, Any],
        dimension: str,
        window: str,
        start_date: date | None,
        end_date: date | None,
    ) -> OrderRankingPoint:
        row_start, row_end = self._validate_row(
            row, publication, window, start_date, end_date
        )
        if row["dimension_type"] != dimension:
            raise ValueError("ranking dimension differs from request")
        return OrderRankingPoint(
            window_type=row["window_type"],
            window_start=row_start,
            window_end=row_end,
            dimension_type=row["dimension_type"],
            dimension_id=_nonblank(row["dimension_id"]),
            dimension_name=_nonblank(row["dimension_name"]),
            is_unknown=_strict_bool(row["is_unknown"], allow_none=False),
            ranking_order_count=_nonnegative_int(row["ranking_order_count"]),
            ranking_item_row_count=_optional_int(row["ranking_item_row_count"]),
            ranking_customer_count=_nonnegative_int(row["ranking_customer_count"]),
            ranking_item_value_sum=_optional_money(row["ranking_item_value_sum"]),
            ranking_freight_value_sum=_optional_money(row["ranking_freight_value_sum"]),
            ranking_payment_value_sum=_optional_money(row["ranking_payment_value_sum"]),
            ranking_late_delivery_order_count=_optional_int(
                row["ranking_late_delivery_order_count"]
            ),
            ranking_late_delivery_eligible_order_count=_optional_int(
                row["ranking_late_delivery_eligible_order_count"]
            ),
            ranking_late_delivery_rate=_rate(row["ranking_late_delivery_rate"]),
            payment_value_is_additive=_strict_bool(
                row["payment_value_is_additive"], allow_none=True
            ),
        )

    def _review_point(
        self,
        row: Mapping[str, Any],
        publication: Mapping[str, Any],
        window: str,
        start_date: date | None,
        end_date: date | None,
    ) -> OrderReviewPoint:
        row_start, row_end = self._validate_row(
            row, publication, window, start_date, end_date
        )
        reviewed = _nonnegative_int(row["reviewed_order_count"])
        all_orders = _nonnegative_int(row["all_order_count"])
        low_score = _nonnegative_int(row["low_score_order_count"])
        multi_review = _nonnegative_int(row["multi_review_order_count"])
        if reviewed > all_orders or low_score > reviewed or multi_review > reviewed:
            raise ValueError("review counts do not reconcile")
        coverage = _rate(row["review_coverage_rate"])
        low_rate = _rate(row["low_score_rate"])
        _require_ratio(coverage, reviewed, all_orders)
        _require_ratio(low_rate, low_score, reviewed)
        score = _six(row["review_score_avg"])
        if score is not None and not Decimal("1") <= Decimal(score) <= Decimal("5"):
            raise ValueError("review score average is outside the supported range")
        return OrderReviewPoint(
            window_type=row["window_type"],
            window_start=row_start,
            window_end=row_end,
            review_row_count=_nonnegative_int(row["review_row_count"]),
            reviewed_order_count=reviewed,
            all_order_count=all_orders,
            review_coverage_rate=coverage,
            review_score_avg=score,
            low_score_order_count=low_score,
            low_score_rate=low_rate,
            multi_review_order_count=multi_review,
        )

    @staticmethod
    def _validate_payment_windows(points: list[OrderPaymentPoint]) -> None:
        grouped: dict[tuple[date, date], list[OrderPaymentPoint]] = {}
        for point in points:
            grouped.setdefault((point.window_start, point.window_end), []).append(point)
        for rows in grouped.values():
            all_rows = [row for row in rows if row.is_all]
            if len(all_rows) != 1:
                raise ValueError("each payment window must contain one all row")

    @staticmethod
    def _available(operation: Callable[[], _T]) -> _T:
        try:
            return operation()
        except OrderMetricsRequestError:
            raise
        except OrderMetricsUnavailableError:
            raise
        except Exception:
            raise _unavailable() from None


def _unavailable() -> OrderMetricsUnavailableError:
    return OrderMetricsUnavailableError("order metrics are unavailable")


def _validate_query_shape(
    window: str, start_date: date | None, end_date: date | None
) -> None:
    if window not in WINDOWS:
        raise OrderMetricsRequestError("unsupported order metric window")
    if (start_date is None) != (end_date is None):
        raise OrderMetricsRequestError("start_date and end_date must be provided together")
    if start_date is not None:
        if type(start_date) is not date or type(end_date) is not date:
            raise OrderMetricsRequestError("order metric date range must contain dates")
        if window == "full":
            raise OrderMetricsRequestError("full window does not accept a date range")
        if start_date > end_date:
            raise OrderMetricsRequestError("start_date must not be after end_date")


def _validate_range(
    start_date: date | None,
    end_date: date | None,
    publication_start: date,
    publication_end: date,
) -> None:
    if start_date is not None and not (
        publication_start <= start_date <= end_date <= publication_end
    ):
        raise OrderMetricsRequestError("date range is outside the publication window")


def _nonblank(value: Any) -> str:
    if type(value) is not str or not value.strip() or value != value.strip():
        raise ValueError("invalid nonblank string")
    return value


def _nonnegative_int(value: Any) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("invalid nonnegative integer")
    return value


def _positive_int(value: Any) -> int:
    result = _nonnegative_int(value)
    if result == 0:
        raise ValueError("invalid positive integer")
    return result


def _optional_int(value: Any) -> int | None:
    return None if value is None else _nonnegative_int(value)


def _strict_bool(value: Any, *, allow_none: bool) -> bool | None:
    if value is None and allow_none:
        return None
    if type(value) is bool:
        return value
    if type(value) is int and value in (0, 1):
        return bool(value)
    raise ValueError("invalid boolean")


def _hex_string(value: Any, pattern: re.Pattern[str]) -> str:
    if type(value) is not str or pattern.fullmatch(value) is None:
        raise ValueError("invalid hexadecimal identity")
    return value


def _sha256(value: Any) -> str:
    return _hex_string(value, _HEX_64)


def _date_value(value: Any) -> date:
    if type(value) is not date:
        raise ValueError("invalid Doris date")
    return value


def _as_utc(value: Any) -> datetime:
    if type(value) is not datetime:
        raise ValueError("invalid Doris datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _decimal(value: Any, quantum: Decimal) -> str:
    if type(value) is not Decimal or not value.is_finite() or value < 0:
        raise ValueError("invalid Doris decimal")
    quantized = value.quantize(quantum)
    if value != quantized:
        raise ValueError("Doris decimal exceeds the fixed scale")
    return format(quantized, "f")


def _money(value: Any) -> str:
    return _decimal(value, _MONEY_QUANTUM)


def _optional_money(value: Any) -> str | None:
    return None if value is None else _money(value)


def _six(value: Any) -> str | None:
    return None if value is None else _decimal(value, _SIX_QUANTUM)


def _rate(value: Any) -> str | None:
    result = _six(value)
    if result is not None and Decimal(result) > 1:
        raise ValueError("rate is outside zero through one")
    return result


def _require_ratio(value: str | None, numerator: int, denominator: int) -> None:
    expected = None
    if denominator != 0:
        expected = format(
            (Decimal(numerator) / Decimal(denominator)).quantize(_SIX_QUANTUM),
            "f",
        )
    if value != expected:
        raise ValueError("ratio does not match numerator and denominator")


def _parse_canonical_object(raw: Any) -> dict[str, Any]:
    if type(raw) is not str:
        raise ValueError("JSON evidence must be a string")
    parsed = json.loads(raw)
    if type(parsed) is not dict:
        raise ValueError("JSON evidence must be an object")
    canonical = json.dumps(parsed, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if raw != canonical:
        raise ValueError("JSON evidence is not canonical")
    return parsed


def _parse_snapshot_map(raw: Any, expected_keys: frozenset[str]) -> dict[str, str]:
    parsed = _parse_canonical_object(raw)
    if set(parsed) != expected_keys:
        raise ValueError("snapshot map has an unexpected key set")
    if any(type(value) is not str or _SNAPSHOT_ID.fullmatch(value) is None for value in parsed.values()):
        raise ValueError("snapshot map contains an invalid ID")
    return parsed


def _parse_integer_map(
    raw: Any,
    expected_keys: frozenset[str],
    *,
    allow_decimal_strings: bool = False,
) -> dict[str, int]:
    parsed = _parse_canonical_object(raw)
    if set(parsed) != expected_keys:
        raise ValueError("integer evidence map has an unexpected key set")

    def parse_value(value: Any) -> int:
        if (
            allow_decimal_strings
            and type(value) is str
            and _COUNT_STRING.fullmatch(value) is not None
        ):
            return int(value)
        return _nonnegative_int(value)

    return {key: parse_value(value) for key, value in parsed.items()}


def _parse_hash_map(raw: Any, expected_keys: frozenset[str]) -> dict[str, str]:
    parsed = _parse_canonical_object(raw)
    if set(parsed) != expected_keys:
        raise ValueError("hash evidence map has an unexpected key set")
    return {key: _sha256(value) for key, value in parsed.items()}


def _require_stable_order(values: list[_T], key: Callable[[_T], Any]) -> None:
    if values != sorted(values, key=key):
        raise ValueError("metric rows are not in stable order")


def _ranking_sort_value(point: OrderRankingPoint, sort_by: str) -> Decimal:
    fields = {
        "order_count": point.ranking_order_count,
        "item_value": point.ranking_item_value_sum,
        "freight_value": point.ranking_freight_value_sum,
        "payment_value": point.ranking_payment_value_sum,
        "late_rate": point.ranking_late_delivery_rate,
    }
    value = fields[sort_by]
    if value is None:
        raise ValueError("ranking sort value is null")
    return Decimal(str(value))
