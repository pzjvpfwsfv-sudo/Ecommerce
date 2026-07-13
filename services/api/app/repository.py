from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

from app.config import ApiSettings


ConnectionFactory = Callable[[], Any]


class RealtimeMetricsRepository:
    def __init__(self, connect: ConnectionFactory) -> None:
        self._connect = connect

    @classmethod
    def from_settings(cls, settings: ApiSettings) -> "RealtimeMetricsRepository":
        def connect() -> Any:
            import pymysql

            return pymysql.connect(
                host=settings.doris_host,
                port=settings.doris_port,
                user=settings.doris_username,
                password=settings.doris_password,
                database=settings.doris_database,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
            )

        return cls(connect)

    def fetch_all_metrics(self) -> dict[str, Any]:
        query = (
            "SELECT metric_name, metric_value, updated_at "
            "FROM realtime_metrics ORDER BY metric_name"
        )
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

        metrics: dict[str, Any] = {"updated_at": None}
        latest: datetime | None = None
        for row in rows:
            metrics[row["metric_name"]] = int(row["metric_value"])
            row_updated_at = row["updated_at"]
            if latest is None or row_updated_at > latest:
                latest = row_updated_at

        metrics["updated_at"] = latest.isoformat() if latest else None
        return metrics

    def fetch_metric(self, metric_name: str) -> dict[str, Any] | None:
        query = (
            "SELECT metric_name, metric_value, updated_at "
            "FROM realtime_metrics WHERE metric_name = %s"
        )
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (metric_name,))
                row = cursor.fetchone()

        if row is None:
            return None

        return {
            "metric_name": row["metric_name"],
            "metric_value": int(row["metric_value"]),
            "updated_at": row["updated_at"].isoformat(),
        }
