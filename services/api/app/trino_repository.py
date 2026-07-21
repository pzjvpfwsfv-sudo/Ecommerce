from __future__ import annotations

from collections.abc import Callable
import re
from typing import Any

import httpx

from app.analysis_models import HistoricalEvidence


_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _quote_identifier(value: str, label: str) -> str:
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Trino {label} must be a simple ASCII identifier")
    return f'"{value}"'


def _build_summary_sql(catalog: str, schema: str) -> str:
    table = ".".join(
        (
            _quote_identifier(catalog, "catalog"),
            _quote_identifier(schema, "schema"),
            _quote_identifier("user_behavior_detail", "table"),
        )
    )
    return f"""WITH parsed_events AS (
    SELECT event_type, try(from_iso8601_timestamp(event_time)) AS parsed_event_time
    FROM {table}
),
summary AS (
    SELECT COUNT(*) AS event_count, MAX(parsed_event_time) AS latest_event_time
    FROM parsed_events
),
event_type_counts AS (
    SELECT event_type, COUNT(*) AS event_count
    FROM parsed_events
    GROUP BY event_type
)
SELECT summary.event_count, event_type_counts.event_type,
       event_type_counts.event_count, to_iso8601(summary.latest_event_time)
FROM summary
LEFT JOIN event_type_counts ON TRUE
ORDER BY event_type_counts.event_count DESC, event_type_counts.event_type ASC"""


SUMMARY_SQL = _build_summary_sql("lakehouse", "analytics")
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
        self._summary_sql = _build_summary_sql(catalog, schema)
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
        rows = self._execute(self._summary_sql)
        if not rows:
            raise ValueError("Trino summary returned no rows")

        if any(len(row) != 4 for row in rows):
            raise ValueError("Trino summary returned malformed rows")

        if (
            len(rows) == 1
            and type(rows[0][0]) is int
            and rows[0][0] == 0
            and rows[0][1:] == [None, None, None]
        ):
            return HistoricalEvidence(
                event_count=0,
                event_type_counts={},
                latest_event_time=None,
            )

        event_count = self._strict_count(rows[0][0], "total")
        if event_count <= 0:
            raise ValueError("Trino non-empty summary must have a positive total")
        latest_event_time = rows[0][3]
        if latest_event_time is None:
            raise ValueError("Trino non-empty summary requires a latest event time")
        counts: dict[str, int] = {}
        for row in rows:
            if self._strict_count(row[0], "total") != event_count or row[3] != latest_event_time:
                raise ValueError("Trino summary rows are inconsistent")
            if type(row[1]) is not str or not row[1].strip():
                raise ValueError("Trino summary contains an invalid event type")
            event_type = row[1]
            if event_type in counts:
                raise ValueError("Trino summary contains a duplicate event type")
            counts[event_type] = self._strict_count(row[2], "event type")

        if event_count < 0 or any(count < 0 for count in counts.values()):
            raise ValueError("Trino summary contains negative counts")
        if sum(counts.values()) != event_count:
            raise ValueError("Trino summary counts do not match the total")
        return HistoricalEvidence(
            event_count=event_count,
            event_type_counts=counts,
            latest_event_time=latest_event_time,
        )

    @staticmethod
    def _strict_count(value: Any, label: str) -> int:
        if type(value) is not int:
            raise ValueError(f"Trino {label} count must be an integer")
        return value

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
