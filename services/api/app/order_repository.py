from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
import math
from typing import Any, Literal

from app.config import ApiSettings
from app.tool_deadline import remaining_timeout


ConnectionFactory = Callable[[], Any]
OrderFamily = Literal["overview", "delivery", "payment", "ranking", "review", "quality"]

WINDOWS = {"day": "DAY", "month": "MONTH", "full": "FULL"}
ITEM_SORTS = frozenset({"order_count", "item_value", "freight_value"})
STATE_SORTS = frozenset({"order_count", "payment_value", "late_rate"})
DIMENSIONS = frozenset({"product", "category", "seller", "customer_state", "seller_state"})
RANK_SORT_COLUMNS = {
    "product": {
        "order_count": "ranking_order_count",
        "item_value": "ranking_item_value_sum",
        "freight_value": "ranking_freight_value_sum",
    },
    "category": {
        "order_count": "ranking_order_count",
        "item_value": "ranking_item_value_sum",
        "freight_value": "ranking_freight_value_sum",
    },
    "seller": {
        "order_count": "ranking_order_count",
        "item_value": "ranking_item_value_sum",
        "freight_value": "ranking_freight_value_sum",
    },
    "customer_state": {
        "order_count": "ranking_order_count",
        "payment_value": "ranking_payment_value_sum",
        "late_rate": "ranking_late_delivery_rate",
    },
    "seller_state": {
        "order_count": "ranking_order_count",
        "payment_value": "ranking_payment_value_sum",
        "late_rate": "ranking_late_delivery_rate",
    },
}

_FAMILY_COUNT_QUERIES = {
    "overview": "SELECT COUNT(*) AS row_count FROM order_metric_overview WHERE metric_run_id = %s",
    "delivery": "SELECT COUNT(*) AS row_count FROM order_metric_delivery WHERE metric_run_id = %s",
    "payment": "SELECT COUNT(*) AS row_count FROM order_metric_payment WHERE metric_run_id = %s",
    "ranking": "SELECT COUNT(*) AS row_count FROM order_metric_ranking WHERE metric_run_id = %s",
    "review": "SELECT COUNT(*) AS row_count FROM order_metric_review WHERE metric_run_id = %s",
    "quality": "SELECT COUNT(*) AS row_count FROM order_metric_quality WHERE metric_run_id = %s",
}


class OrderMetricsRepository:
    def __init__(self, connect: ConnectionFactory) -> None:
        self._connect = connect

    @classmethod
    def from_settings(cls, settings: ApiSettings) -> OrderMetricsRepository:
        def connect() -> Any:
            import pymysql

            timeout_seconds = math.ceil(
                remaining_timeout(settings.ai_tool_total_timeout_seconds)
            )
            return pymysql.connect(
                host=settings.doris_host,
                port=settings.doris_port,
                user=settings.doris_username,
                password=settings.doris_password,
                database=settings.doris_database,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=timeout_seconds,
                read_timeout=timeout_seconds,
                write_timeout=timeout_seconds,
            )

        return cls(connect)

    def fetch_latest_publication(self) -> dict[str, Any] | None:
        query = (
            "SELECT metric_run_id, dataset_id, metric_version, source_bundle_sha256, "
            "source_snapshots_json, curated_snapshots_json, implementation_revision, "
            "source_order_count, window_start, window_end, calculated_at, published_at, "
            "overview_row_count, overview_sha256, delivery_row_count, delivery_sha256, "
            "payment_row_count, payment_sha256, ranking_row_count, ranking_sha256, "
            "review_row_count, review_sha256, quality_row_count, quality_sha256, status "
            "FROM order_metric_publications WHERE status = %s "
            "ORDER BY published_at DESC, metric_run_id DESC LIMIT 1"
        )
        return self._fetch_one(query, ("PUBLISHED",))

    def fetch_overview(
        self,
        metric_run_id: str,
        window: str,
        start_date: date | None,
        end_date: date | None,
    ) -> list[dict[str, Any]]:
        window_type, range_sql, range_params = _window_and_range(window, start_date, end_date)
        query = (
            "SELECT metric_run_id, dataset_id, metric_version, window_type, window_start, "
            "window_end, order_count, delivered_order_count, canceled_order_count, "
            "unavailable_order_count, status_eligible_order_count, status_excluded_order_count, "
            "delivered_rate, canceled_rate, unique_customer_count, repeat_customer_count, "
            "repeat_customer_rate, item_row_count, item_value_sum, freight_value_sum, "
            "payment_value_sum, items_per_order_avg FROM order_metric_overview "
            f"WHERE metric_run_id = %s AND window_type = %s{range_sql} "
            "ORDER BY window_start ASC, window_end ASC"
        )
        return self._fetch_all(query, (metric_run_id, window_type, *range_params))

    def fetch_delivery(
        self,
        metric_run_id: str,
        window: str,
        start_date: date | None,
        end_date: date | None,
    ) -> list[dict[str, Any]]:
        window_type, range_sql, range_params = _window_and_range(window, start_date, end_date)
        query = (
            "SELECT metric_run_id, dataset_id, metric_version, window_type, window_start, "
            "window_end, delivery_eligible_order_count, delivery_excluded_order_count, "
            "delivery_days_avg, delivery_days_p50, delivery_days_p90, "
            "late_delivery_order_count, late_delivery_eligible_order_count, "
            "late_delivery_excluded_order_count, late_delivery_rate "
            "FROM order_metric_delivery "
            f"WHERE metric_run_id = %s AND window_type = %s{range_sql} "
            "ORDER BY window_start ASC, window_end ASC"
        )
        return self._fetch_all(query, (metric_run_id, window_type, *range_params))

    def fetch_payments(
        self,
        metric_run_id: str,
        window: str,
        start_date: date | None,
        end_date: date | None,
    ) -> list[dict[str, Any]]:
        window_type, range_sql, range_params = _window_and_range(window, start_date, end_date)
        query = (
            "SELECT metric_run_id, dataset_id, metric_version, window_type, window_start, "
            "window_end, payment_type, is_all, global_order_count, payment_order_count, "
            "payment_row_count, installment_order_count, payment_type_order_count, "
            "payment_type_value_sum FROM order_metric_payment "
            f"WHERE metric_run_id = %s AND window_type = %s{range_sql} "
            "ORDER BY window_start ASC, window_end ASC, is_all DESC, payment_type ASC"
        )
        return self._fetch_all(query, (metric_run_id, window_type, *range_params))

    def fetch_rankings(
        self,
        metric_run_id: str,
        dimension: str,
        window: str,
        start_date: date | None,
        end_date: date | None,
        sort_by: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        if dimension not in DIMENSIONS:
            raise ValueError("unsupported order metric dimension")
        sort_columns = RANK_SORT_COLUMNS[dimension]
        if sort_by not in sort_columns:
            raise ValueError("unsupported order metric sort for dimension")
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("order metric limit must be between 1 and 100")
        window_type, range_sql, range_params = _window_and_range(window, start_date, end_date)
        order_column = sort_columns[sort_by]
        query = (
            "SELECT metric_run_id, dataset_id, metric_version, window_type, window_start, "
            "window_end, dimension_type, dimension_id, dimension_name, is_unknown, "
            "ranking_order_count, ranking_item_row_count, ranking_customer_count, "
            "ranking_item_value_sum, ranking_freight_value_sum, ranking_payment_value_sum, "
            "ranking_late_delivery_order_count, ranking_late_delivery_eligible_order_count, "
            "ranking_late_delivery_rate, payment_value_is_additive "
            "FROM order_metric_ranking WHERE metric_run_id = %s AND dimension_type = %s "
            f"AND window_type = %s{range_sql} ORDER BY {order_column} DESC, "
            "dimension_id ASC, window_start ASC LIMIT %s"
        )
        return self._fetch_all(
            query,
            (metric_run_id, dimension, window_type, *range_params, limit),
        )

    def fetch_reviews(
        self,
        metric_run_id: str,
        window: str,
        start_date: date | None,
        end_date: date | None,
    ) -> list[dict[str, Any]]:
        window_type, range_sql, range_params = _window_and_range(window, start_date, end_date)
        query = (
            "SELECT metric_run_id, dataset_id, metric_version, window_type, window_start, "
            "window_end, review_row_count, reviewed_order_count, all_order_count, "
            "review_coverage_rate, review_score_avg, low_score_order_count, low_score_rate, "
            "multi_review_order_count FROM order_metric_review "
            f"WHERE metric_run_id = %s AND window_type = %s{range_sql} "
            "ORDER BY window_start ASC, window_end ASC"
        )
        return self._fetch_all(query, (metric_run_id, window_type, *range_params))

    def fetch_quality(self, metric_run_id: str) -> dict[str, Any] | None:
        query = (
            "SELECT metric_run_id, dataset_id, metric_version, window_type, window_start, "
            "window_end, source_row_count, iceberg_row_count, duplicate_key_count, "
            "orphan_key_count, invalid_value_count, temporal_anomaly_count, "
            "amount_comparable_order_count, amount_reconciled_order_count, "
            "amount_mismatch_order_count, amount_reconciliation_rate, "
            "payment_item_freight_abs_difference_avg, "
            "payment_item_freight_abs_difference_p50, "
            "payment_item_freight_abs_difference_p90, raw_row_counts_json, "
            "normalized_row_counts_json, iceberg_row_counts_json, normalized_sha256_json, "
            "source_snapshots_json, curated_snapshots_json, fact_reconciliations_json, "
            "reportable_quality_json, reconciliation_status FROM order_metric_quality "
            "WHERE metric_run_id = %s AND window_type = %s LIMIT 1"
        )
        return self._fetch_one(query, (metric_run_id, "FULL"))

    def fetch_family_row_count(self, metric_run_id: str, family: OrderFamily) -> int:
        if family not in _FAMILY_COUNT_QUERIES:
            raise ValueError("unsupported order metric family")
        row = self._fetch_one(_FAMILY_COUNT_QUERIES[family], (metric_run_id,))
        if row is None or type(row.get("row_count")) is not int or row["row_count"] < 0:
            raise ValueError("invalid order metric family row count")
        return row["row_count"]

    def _fetch_all(self, query: str, parameters: tuple[Any, ...]) -> list[dict[str, Any]]:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, parameters)
                return list(cursor.fetchall())

    def _fetch_one(self, query: str, parameters: tuple[Any, ...]) -> dict[str, Any] | None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, parameters)
                return cursor.fetchone()


def _window_and_range(
    window: str,
    start_date: date | None,
    end_date: date | None,
) -> tuple[str, str, tuple[date, ...]]:
    if window not in WINDOWS:
        raise ValueError("unsupported order metric window")
    if (start_date is None) != (end_date is None):
        raise ValueError("start_date and end_date must be provided together")
    if start_date is not None:
        if type(start_date) is not date or type(end_date) is not date:
            raise ValueError("order metric date range must contain dates")
        if window == "full":
            raise ValueError("full window does not accept a date range")
        if start_date > end_date:
            raise ValueError("start_date must not be after end_date")
        return WINDOWS[window], " AND window_start >= %s AND window_end <= %s", (
            start_date,
            end_date,
        )
    return WINDOWS[window], "", ()
