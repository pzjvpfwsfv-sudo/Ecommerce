from __future__ import annotations

from collections.abc import Callable
import math
from typing import Any

from app.config import ApiSettings
from app.tool_deadline import remaining_timeout


ConnectionFactory = Callable[[], Any]

_SORT_COLUMNS = {
    "views": "view_count",
    "carts": "cart_count",
    "purchases": "purchase_count",
    "users": "unique_user_count",
    "amount": "purchase_amount_proxy",
}
_DIMENSIONS = frozenset({"product", "category", "brand"})
_WINDOWS = {"day": "DAY", "full": "FULL"}


class BehaviorMetricsRepository:
    def __init__(self, connect: ConnectionFactory) -> None:
        self._connect = connect

    @classmethod
    def from_settings(cls, settings: ApiSettings) -> BehaviorMetricsRepository:
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
            "SELECT metric_run_id, dataset_id, metric_version, data_scope, "
            "source_snapshot_id, source_event_count, window_start, window_end, "
            "calculated_at, published_at, overview_row_count, overview_sha256, "
            "funnel_row_count, funnel_sha256, dimension_row_count, dimension_sha256, "
            "quality_row_count, quality_sha256, status "
            "FROM behavior_metric_publications WHERE status = %s "
            "ORDER BY published_at DESC, metric_run_id DESC LIMIT 1"
        )
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, ("PUBLISHED",))
                return cursor.fetchone()

    def fetch_overview(self, metric_run_id: str, window: str) -> list[dict[str, Any]]:
        window_type = self._window_type(window)
        query = (
            "SELECT metric_run_id, dataset_id, metric_version, window_type, window_start, "
            "window_end, event_count, view_count, cart_count, purchase_count, "
            "unique_user_count, session_count, product_count, purchase_amount_proxy "
            "FROM behavior_overview_metrics "
            "WHERE metric_run_id = %s AND window_type = %s "
            "ORDER BY window_start ASC, window_end ASC"
        )
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (metric_run_id, window_type))
                return list(cursor.fetchall())

    def fetch_funnel(self, metric_run_id: str, window: str) -> list[dict[str, Any]]:
        window_type = self._window_type(window)
        query = (
            "SELECT metric_run_id, dataset_id, metric_version, window_type, window_start, "
            "window_end, missing_session_event_count, view_sessions, "
            "view_to_cart_sessions, completed_sessions "
            "FROM behavior_funnel_metrics "
            "WHERE metric_run_id = %s AND window_type = %s "
            "ORDER BY window_start ASC, window_end ASC"
        )
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (metric_run_id, window_type))
                return list(cursor.fetchall())

    def fetch_rankings(
        self,
        metric_run_id: str,
        dimension: str,
        window: str,
        sort_by: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        if dimension not in _DIMENSIONS:
            raise ValueError("unsupported behavior metric dimension")
        window_type = self._window_type(window)
        if sort_by not in _SORT_COLUMNS:
            raise ValueError("unsupported behavior metric sort")
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("behavior metric limit must be between 1 and 100")
        order_column = _SORT_COLUMNS[sort_by]
        query = (
            "SELECT metric_run_id, dataset_id, metric_version, window_type, window_start, "
            "window_end, dimension_type, dimension_id, dimension_name, is_unknown, "
            "view_count, cart_count, purchase_count, unique_user_count, purchase_amount_proxy "
            "FROM behavior_dimension_metrics "
            "WHERE metric_run_id = %s AND dimension_type = %s AND window_type = %s "
            f"ORDER BY {order_column} DESC, dimension_id ASC, window_start ASC LIMIT %s"
        )
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (metric_run_id, dimension, window_type, limit))
                return list(cursor.fetchall())

    def fetch_quality(self, metric_run_id: str) -> dict[str, Any] | None:
        query = (
            "SELECT metric_run_id, dataset_id, metric_version, window_type, window_start, "
            "window_end, source_event_count, clean_event_count, late_event_count, "
            "clean_event_rate, late_event_rate, distinct_event_count, duplicate_event_count, "
            "missing_session_count, unknown_category_count, unknown_brand_count, "
            "invalid_event_type_count, empty_key_id_count, invalid_price_count, "
            "invalid_derived_date_count, overview_event_count, reconciliation_status "
            "FROM behavior_quality_metrics "
            "WHERE metric_run_id = %s AND window_type = %s LIMIT 1"
        )
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (metric_run_id, "FULL"))
                return cursor.fetchone()

    @staticmethod
    def _window_type(window: str) -> str:
        if window not in _WINDOWS:
            raise ValueError("unsupported behavior metric window")
        return _WINDOWS[window]
