from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException

from app.config import load_settings
from app.repository import RealtimeMetricsRepository


def create_app(repository: RealtimeMetricsRepository | Any | None = None) -> FastAPI:
    if repository is None:
        settings = load_settings()
        repository = RealtimeMetricsRepository.from_settings(settings)

    app = FastAPI(title="Realtime Metrics API", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "realtime-metrics-api"}

    @app.get("/metrics/realtime")
    def get_realtime_metrics() -> dict[str, object]:
        return repository.fetch_all_metrics()

    @app.get("/metrics/{metric_name}")
    def get_metric(metric_name: str) -> dict[str, object]:
        metric = repository.fetch_metric(metric_name)
        if metric is None:
            raise HTTPException(status_code=404, detail=f"metric '{metric_name}' not found")
        return metric

    return app


app = create_app()
