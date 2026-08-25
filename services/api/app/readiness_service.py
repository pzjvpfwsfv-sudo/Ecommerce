from __future__ import annotations

from typing import Any


class ReadinessService:
    def __init__(
        self,
        doris_repository: Any,
        trino_repository: Any,
        flink_repository: Any,
    ) -> None:
        self._doris_repository = doris_repository
        self._trino_repository = trino_repository
        self._flink_repository = flink_repository

    def check(self) -> dict[str, object]:
        self._doris_repository.fetch_all_metrics()
        self._trino_repository.fetch_summary()
        self._flink_repository.fetch_health()
        return {
            "status": "ready",
            "dependencies": {"doris": "ready", "trino": "ready", "flink": "ready"},
        }
