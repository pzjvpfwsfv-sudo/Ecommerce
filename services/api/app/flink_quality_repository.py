from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
import re
from typing import Any
from urllib.parse import quote

import httpx

from app.tool_models import DataQualityEvidence, QUALITY_COUNTERS


_JOB_ID_PATTERN = re.compile(r"[0-9a-f]{32}\Z")
_COUNT_PATTERN = re.compile(r"[0-9]+\Z")
ClientFactory = Callable[[], httpx.Client]


class FlinkQualityRepository:
    def __init__(
        self,
        base_url: str,
        production_job_name: str,
        timeout_seconds: float,
        client_factory: ClientFactory | None = None,
    ) -> None:
        if not base_url.strip():
            raise ValueError("Flink base URL must not be empty")
        if not production_job_name.strip():
            raise ValueError("Flink production job name must not be empty")
        if not isinstance(timeout_seconds, (int, float)) or isinstance(timeout_seconds, bool) or timeout_seconds <= 0:
            raise ValueError("Flink timeout must be positive")

        self._base_url = base_url.rstrip("/")
        self._production_job_name = production_job_name
        self._timeout_seconds = timeout_seconds
        self._client_factory = client_factory or (lambda: httpx.Client())

    def fetch_health(self) -> DataQualityEvidence:
        with self._client_factory() as client:
            overview = self._get_json(client, "/jobs/overview")
            job_id = self._find_running_job_id(overview)
            checkpoints = self._get_json(client, f"/jobs/{job_id}/checkpoints")
            completed, failed, latest_completed_at = self._parse_checkpoints(checkpoints)
            job_details = self._get_json(client, f"/jobs/{job_id}")
            counters = self._fetch_counters(client, job_id, job_details)

        return DataQualityEvidence(
            job_id=job_id,
            job_state="RUNNING",
            completed_checkpoints=completed,
            failed_checkpoints=failed,
            latest_completed_at=latest_completed_at,
            counters=counters,
        )

    def _get_json(
        self,
        client: httpx.Client,
        path: str,
        params: list[tuple[str, str]] | None = None,
    ) -> Any:
        try:
            response = client.get(
                f"{self._base_url}{path}",
                params=params,
                timeout=self._timeout_seconds,
            )
            response.raise_for_status()
        except httpx.TimeoutException:
            raise RuntimeError("Flink quality request timed out") from None
        except httpx.HTTPError:
            raise RuntimeError("Flink quality upstream request failed") from None

        try:
            return response.json()
        except ValueError:
            raise ValueError("Flink quality response was malformed") from None

    def _find_running_job_id(self, payload: Any) -> str:
        if not isinstance(payload, dict) or not isinstance(payload.get("jobs"), list):
            raise ValueError("Flink jobs overview was malformed")

        matches = [
            item
            for item in payload["jobs"]
            if isinstance(item, dict) and item.get("name") == self._production_job_name
        ]
        if len(matches) != 1:
            raise ValueError("Flink production job discovery was ambiguous")

        job = matches[0]
        if job.get("state") != "RUNNING":
            raise ValueError("Flink production job is not running")
        job_id = job.get("jid")
        if not isinstance(job_id, str) or not _JOB_ID_PATTERN.fullmatch(job_id):
            raise ValueError("Flink production job ID was invalid")
        return job_id

    @staticmethod
    def _parse_checkpoints(payload: Any) -> tuple[int, int, datetime | None]:
        if not isinstance(payload, dict) or not isinstance(payload.get("counts"), dict):
            raise ValueError("Flink checkpoint response was malformed")

        counts = payload["counts"]
        completed = FlinkQualityRepository._strict_int(counts.get("completed"), "checkpoint count")
        failed = FlinkQualityRepository._strict_int(counts.get("failed"), "checkpoint count")
        latest = payload.get("latest")
        if latest is None:
            return completed, failed, None
        if not isinstance(latest, dict):
            raise ValueError("Flink checkpoint response was malformed")
        latest_completed = latest.get("completed")
        if latest_completed is None:
            return completed, failed, None
        if not isinstance(latest_completed, dict):
            raise ValueError("Flink checkpoint response was malformed")
        timestamp = FlinkQualityRepository._strict_int(
            latest_completed.get("latest_ack_timestamp"), "checkpoint timestamp"
        )
        return completed, failed, datetime.fromtimestamp(timestamp / 1000, tz=UTC)

    def _fetch_counters(self, client: httpx.Client, job_id: str, payload: Any) -> dict[str, int]:
        if not isinstance(payload, dict) or not isinstance(payload.get("vertices"), list):
            raise ValueError("Flink job details were malformed")

        totals = {counter: 0 for counter in QUALITY_COUNTERS}
        observed: set[str] = set()
        for vertex in payload["vertices"]:
            if not isinstance(vertex, dict) or not isinstance(vertex.get("id"), str) or not vertex["id"]:
                raise ValueError("Flink job details were malformed")
            vertex_id = vertex["id"]
            vertex_path = quote(vertex_id, safe="")
            metric_path = f"/jobs/{job_id}/vertices/{vertex_path}/metrics"
            metrics = self._get_json(client, metric_path)
            metric_ids = self._quality_metric_ids(metrics)
            if not metric_ids:
                continue
            values = self._get_json(
                client,
                metric_path,
                [("get", metric_id) for metric_id in metric_ids] + [("agg", "sum")],
            )
            self._add_counter_values(values, metric_ids, totals, observed)

        if observed != QUALITY_COUNTERS:
            raise ValueError("Flink quality counters were incomplete")
        return totals

    @staticmethod
    def _quality_metric_ids(payload: Any) -> list[str]:
        if not isinstance(payload, list):
            raise ValueError("Flink vertex metrics were malformed")
        metric_ids: list[str] = []
        for metric in payload:
            if not isinstance(metric, dict) or not isinstance(metric.get("id"), str):
                raise ValueError("Flink vertex metrics were malformed")
            metric_id = metric["id"]
            if FlinkQualityRepository._counter_for_metric(metric_id) is not None:
                metric_ids.append(metric_id)
        return metric_ids

    @staticmethod
    def _add_counter_values(
        payload: Any,
        requested_ids: list[str],
        totals: dict[str, int],
        observed: set[str],
    ) -> None:
        if not isinstance(payload, list):
            raise ValueError("Flink counter values were malformed")
        requested = set(requested_ids)
        returned: set[str] = set()
        for metric in payload:
            if not isinstance(metric, dict) or not isinstance(metric.get("id"), str):
                raise ValueError("Flink counter values were malformed")
            metric_id = metric["id"]
            if metric_id not in requested or metric_id in returned:
                raise ValueError("Flink counter values were malformed")
            counter = FlinkQualityRepository._counter_for_metric(metric_id)
            if counter is None:
                raise ValueError("Flink counter values were malformed")
            totals[counter] += FlinkQualityRepository._strict_metric_count(metric.get("sum"))
            observed.add(counter)
            returned.add(metric_id)
        if returned != requested:
            raise ValueError("Flink counter values were incomplete")

    @staticmethod
    def _counter_for_metric(metric_id: str) -> str | None:
        for counter in QUALITY_COUNTERS:
            if metric_id == counter or metric_id.endswith(f".{counter}"):
                return counter
        return None

    @staticmethod
    def _strict_int(value: Any, label: str) -> int:
        if type(value) is not int or value < 0:
            raise ValueError(f"Flink {label} must be a non-negative integer")
        return value

    @staticmethod
    def _strict_metric_count(value: Any) -> int:
        if not isinstance(value, str) or not _COUNT_PATTERN.fullmatch(value):
            raise ValueError("Flink counter value must be a non-negative decimal integer")
        return int(value)
