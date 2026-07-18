from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx

from app.analysis_models import HistoricalEvidence


EVENT_COUNT_SQL = "SELECT COUNT(*) AS event_count FROM lakehouse.analytics.user_behavior_detail"
EVENT_TYPE_COUNTS_SQL = (
    "SELECT event_type, COUNT(*) AS event_count "
    "FROM lakehouse.analytics.user_behavior_detail "
    "GROUP BY event_type ORDER BY event_count DESC, event_type ASC"
)
LATEST_EVENT_TIME_SQL = (
    "SELECT MAX(event_time) AS latest_event_time "
    "FROM lakehouse.analytics.user_behavior_detail"
)
ClientFactory = Callable[[], httpx.Client]


class TrinoAnalyticsRepository:
    def __init__(
        self,
        base_url: str,
        user: str = "ecommerce-ai",
        catalog: str = "lakehouse",
        schema: str = "analytics",
        timeout_seconds: float = 10,
        client_factory: ClientFactory | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._headers = {
            "X-Trino-User": user,
            "X-Trino-Source": "chapter-8-ai-analysis",
            "X-Trino-Catalog": catalog,
            "X-Trino-Schema": schema,
        }
        self._timeout_seconds = timeout_seconds
        self._client_factory = client_factory or (lambda: httpx.Client())

    def fetch_summary(self) -> HistoricalEvidence:
        count_rows = self._execute(EVENT_COUNT_SQL)
        type_rows = self._execute(EVENT_TYPE_COUNTS_SQL)
        latest_rows = self._execute(LATEST_EVENT_TIME_SQL)
        event_count = int(count_rows[0][0]) if count_rows else 0
        counts = {str(row[0]): int(row[1]) for row in type_rows}
        latest_event_time = latest_rows[0][0] if latest_rows and latest_rows[0] else None
        return HistoricalEvidence(
            event_count=event_count,
            event_type_counts=counts,
            latest_event_time=latest_event_time,
        )

    def _execute(self, sql: str) -> list[list[Any]]:
        rows: list[list[Any]] = []
        with self._client_factory() as client:
            response = client.post(
                f"{self._base_url}/v1/statement",
                headers=self._headers,
                content=sql,
                timeout=self._timeout_seconds,
            )
            while True:
                response.raise_for_status()
                payload = response.json()
                if payload.get("error"):
                    raise RuntimeError(f"Trino query failed: {payload['error']['message']}")
                rows.extend(payload.get("data", []))
                next_uri = payload.get("nextUri")
                if not next_uri:
                    return rows
                response = client.get(next_uri, timeout=self._timeout_seconds)
