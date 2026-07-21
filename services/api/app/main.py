from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, HTTPException, status

from app.analysis_models import AnalysisRequest, AnalysisResponse
from app.analysis_service import (
    AnalysisService,
    AnalysisUnavailableError,
    RealtimeDataUnavailableError,
)
from app.config import ApiSettings, load_settings
from app.dependencies import build_analysis_service
from app.repository import RealtimeMetricsRepository


logger = logging.getLogger(__name__)


def create_app(
    repository: RealtimeMetricsRepository | Any | None = None,
    analysis_service: AnalysisService | Any | None = None,
    settings: ApiSettings | None = None,
) -> FastAPI:
    settings = settings or load_settings()
    if repository is None:
        repository = RealtimeMetricsRepository.from_settings(settings)
    if analysis_service is None:
        analysis_service = build_analysis_service(settings, repository)

    app = FastAPI(title="Realtime Metrics API", version="0.2.0")

    @app.post("/analysis/realtime", response_model=AnalysisResponse)
    def analyze_realtime(request: AnalysisRequest) -> AnalysisResponse:
        if len(request.question) > settings.ai_max_question_length:
            raise HTTPException(status_code=422, detail="question is too long")
        try:
            return analysis_service.analyze(request.question)
        except RealtimeDataUnavailableError:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="realtime metrics are temporarily unavailable",
            ) from None
        except AnalysisUnavailableError:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="analysis is temporarily unavailable",
            ) from None
        except Exception as exc:
            logger.error(
                "analysis_route_failed",
                extra={"stage": "analysis_route", "error_type": type(exc).__name__},
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="analysis is temporarily unavailable",
            ) from None

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
