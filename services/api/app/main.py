from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import date
import logging
from threading import Lock
from typing import Any, Callable, Literal

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.staticfiles import StaticFiles

from app.analysis_models import AnalysisRequest, AnalysisResponse, ToolAnalysisRequest
from app.analysis_service import (
    AnalysisService,
    AnalysisUnavailableError,
    RealtimeDataUnavailableError,
)
from app.agent_api import create_agent_router
from app.agent_provider import build_model
from app.agent_service import AgentService
from app.agent_store import AgentStore
from app.auth_api import router as auth_router
from app.auth_service import AuthService, require_analyst, require_csrf, require_principal
from app.auth_store import AuthStore
from app.behavior_models import (
    FunnelResponse,
    MetricDefinitionsResponse,
    OverviewResponse,
    PublicationResponse,
    QualityResponse,
    RankingsResponse,
)
from app.behavior_service import BehaviorMetricsService, BehaviorMetricsUnavailableError
from app.config import ApiSettings, load_settings
from app.knowledge_api import create_knowledge_router
from app.knowledge_ingest import Embedder
from app.knowledge_search import KnowledgeSearchService
from app.knowledge_store import KnowledgeStore
from app.dependencies import (
    build_analysis_service,
    build_behavior_metrics_service,
    build_order_metrics_service,
    build_readiness_service,
    build_tool_analysis_service,
)
from app.order_metric_definitions import OrderMetricDefinitionsResponse
from app.order_models import (
    OrderDeliveryResponse,
    OrderOverviewResponse,
    OrderPaymentsResponse,
    OrderPublicationResponse,
    OrderQualityResponse,
    OrderRankingsResponse,
    OrderReviewsResponse,
)
from app.order_service import (
    OrderMetricsRequestError,
    OrderMetricsService,
    OrderMetricsUnavailableError,
)
from app.readiness_service import ReadinessService
from app.repository import RealtimeMetricsRepository
from app.tool_analysis_service import ToolAnalysisService, ToolAnalysisUnavailableError
from app.tool_models import ToolAnalysisResponse


logger = logging.getLogger(__name__)


def create_app(
    repository: RealtimeMetricsRepository | Any | None = None,
    analysis_service: AnalysisService | Any | None = None,
    tool_analysis_service: ToolAnalysisService | Any | None = None,
    readiness_service: ReadinessService | Any | None = None,
    settings: ApiSettings | None = None,
    behavior_service: BehaviorMetricsService | Any | None = None,
    order_service: OrderMetricsService | Any | None = None,
    auth_service: AuthService | None = None,
    knowledge_store: KnowledgeStore | Any | None = None,
    knowledge_search: KnowledgeSearchService | Any | None = None,
    knowledge_embedder: Embedder | Any | None = None,
    agent_service: AgentService | Any | None = None,
    agent_store: AgentStore | Any | None = None,
) -> FastAPI:
    settings = settings or load_settings()
    if settings.web_dist_dir is not None and not (settings.web_dist_dir / "index.html").is_file():
        raise RuntimeError("WEB_DIST_DIR must contain index.html")
    if auth_service is None:
        auth_service = AuthService(AuthStore(settings))
    if repository is None:
        repository = RealtimeMetricsRepository.from_settings(settings)
    if analysis_service is None:
        analysis_service = build_analysis_service(settings, repository)
    owns_tool_analysis_service = tool_analysis_service is None
    if owns_tool_analysis_service:
        tool_analysis_service = build_tool_analysis_service(settings, repository)
    if readiness_service is None:
        readiness_service = build_readiness_service(settings)
    if behavior_service is None:
        behavior_service = build_behavior_metrics_service(settings)
    if order_service is None:
        order_service = build_order_metrics_service(settings)
    knowledge_store = knowledge_store or KnowledgeStore(settings)
    knowledge_embedder = knowledge_embedder or Embedder()
    knowledge_search = knowledge_search or KnowledgeSearchService(knowledge_store, knowledge_embedder)
    if agent_service is None:
        agent_service = AgentService(
            model=build_model(settings.g4_agent), knowledge=knowledge_search,
            behavior=behavior_service, orders=order_service,
            model_name=settings.g4_agent.model if settings.g4_agent.provider != "off" else None,
        )
    agent_store = agent_store or AgentStore(settings, knowledge_store, behavior_service, order_service)
    tool_analysis_service_closed = False
    tool_analysis_service_close_lock = Lock()

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        nonlocal tool_analysis_service_closed
        try:
            yield
        finally:
            with tool_analysis_service_close_lock:
                if owns_tool_analysis_service and not tool_analysis_service_closed:
                    tool_analysis_service_closed = True
                    tool_analysis_service.close()

    app = FastAPI(title="Realtime Metrics API", version="0.3.0", lifespan=lifespan)
    app.state.auth_service = auth_service
    app.include_router(auth_router)
    app.include_router(create_knowledge_router(settings, knowledge_store, knowledge_search, knowledge_embedder))
    app.include_router(create_agent_router(agent_service, agent_store))

    def behavior_response(stage: str, operation: Callable[[], Any]) -> Any:
        try:
            return operation()
        except BehaviorMetricsUnavailableError as exc:
            logger.error(
                "behavior_metrics_unavailable",
                extra={"stage": stage, "error_type": type(exc).__name__},
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="behavior metrics are temporarily unavailable",
            ) from None

    def order_response(stage: str, operation: Callable[[], Any]) -> Any:
        try:
            return operation()
        except OrderMetricsRequestError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="invalid order metric request",
            ) from None
        except OrderMetricsUnavailableError as exc:
            logger.error(
                "order_metrics_unavailable",
                extra={"stage": stage, "error_type": type(exc).__name__},
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="order metrics are temporarily unavailable",
            ) from None

    def validate_order_range(
        window: str, start_date: date | None, end_date: date | None
    ) -> None:
        if (start_date is None) != (end_date is None):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="start_date and end_date must be provided together",
            )
        if start_date is not None and window == "full":
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="full window does not accept a date range",
            )
        if start_date is not None and start_date > end_date:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="start_date must not be after end_date",
            )

    @app.get("/api/v1/behavior/publication", response_model=PublicationResponse, dependencies=[Depends(require_principal)])
    def get_behavior_publication() -> PublicationResponse:
        return behavior_response("behavior_publication", behavior_service.get_publication)

    @app.get("/api/v1/behavior/overview", response_model=OverviewResponse, dependencies=[Depends(require_principal)])
    def get_behavior_overview(
        window: Literal["day", "full"] = "full",
    ) -> OverviewResponse:
        return behavior_response(
            "behavior_overview", lambda: behavior_service.get_overview(window)
        )

    @app.get("/api/v1/behavior/funnel", response_model=FunnelResponse, dependencies=[Depends(require_principal)])
    def get_behavior_funnel(
        window: Literal["day", "full"] = "full",
    ) -> FunnelResponse:
        return behavior_response(
            "behavior_funnel", lambda: behavior_service.get_funnel(window)
        )

    @app.get("/api/v1/behavior/rankings", response_model=RankingsResponse, dependencies=[Depends(require_principal)])
    def get_behavior_rankings(
        dimension: Literal["product", "category", "brand"],
        window: Literal["day", "full"] = "full",
        sort_by: Literal["views", "carts", "purchases", "users", "amount"] = "purchases",
        limit: int = Query(20, ge=1, le=100),
    ) -> RankingsResponse:
        return behavior_response(
            "behavior_rankings",
            lambda: behavior_service.get_rankings(dimension, window, sort_by, limit),
        )

    @app.get("/api/v1/behavior/quality", response_model=QualityResponse, dependencies=[Depends(require_principal)])
    def get_behavior_quality() -> QualityResponse:
        return behavior_response("behavior_quality", behavior_service.get_quality)

    @app.get("/api/v1/orders/publication", response_model=OrderPublicationResponse, dependencies=[Depends(require_principal)])
    def get_order_publication() -> OrderPublicationResponse:
        return order_response("order_publication", order_service.get_publication)

    @app.get("/api/v1/orders/overview", response_model=OrderOverviewResponse, dependencies=[Depends(require_principal)])
    def get_order_overview(
        window: Literal["day", "month", "full"] = "full",
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> OrderOverviewResponse:
        validate_order_range(window, start_date, end_date)
        return order_response(
            "order_overview",
            lambda: order_service.get_overview(window, start_date, end_date),
        )

    @app.get("/api/v1/orders/delivery", response_model=OrderDeliveryResponse, dependencies=[Depends(require_principal)])
    def get_order_delivery(
        window: Literal["day", "month", "full"] = "full",
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> OrderDeliveryResponse:
        validate_order_range(window, start_date, end_date)
        return order_response(
            "order_delivery",
            lambda: order_service.get_delivery(window, start_date, end_date),
        )

    @app.get("/api/v1/orders/payments", response_model=OrderPaymentsResponse, dependencies=[Depends(require_principal)])
    def get_order_payments(
        window: Literal["day", "month", "full"] = "full",
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> OrderPaymentsResponse:
        validate_order_range(window, start_date, end_date)
        return order_response(
            "order_payments",
            lambda: order_service.get_payments(window, start_date, end_date),
        )

    @app.get("/api/v1/orders/rankings", response_model=OrderRankingsResponse, dependencies=[Depends(require_principal)])
    def get_order_rankings(
        dimension: Literal["product", "category", "seller", "customer_state", "seller_state"],
        window: Literal["day", "month", "full"] = "full",
        start_date: date | None = None,
        end_date: date | None = None,
        sort_by: Literal[
            "order_count", "item_value", "freight_value", "payment_value", "late_rate"
        ] = "order_count",
        limit: int = Query(20, ge=1, le=100),
    ) -> OrderRankingsResponse:
        validate_order_range(window, start_date, end_date)
        item_dimension = dimension in {"product", "category", "seller"}
        valid_sort = sort_by in (
            {"order_count", "item_value", "freight_value"}
            if item_dimension
            else {"order_count", "payment_value", "late_rate"}
        )
        if not valid_sort:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="unsupported sort for order ranking dimension",
            )
        return order_response(
            "order_rankings",
            lambda: order_service.get_rankings(
                dimension, window, start_date, end_date, sort_by, limit
            ),
        )

    @app.get("/api/v1/orders/reviews", response_model=OrderReviewsResponse, dependencies=[Depends(require_principal)])
    def get_order_reviews(
        window: Literal["day", "month", "full"] = "full",
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> OrderReviewsResponse:
        validate_order_range(window, start_date, end_date)
        return order_response(
            "order_reviews",
            lambda: order_service.get_reviews(window, start_date, end_date),
        )

    @app.get("/api/v1/orders/quality", response_model=OrderQualityResponse, dependencies=[Depends(require_principal)])
    def get_order_quality() -> OrderQualityResponse:
        return order_response("order_quality", order_service.get_quality)

    @app.get(
        "/api/v1/metrics/definitions",
        response_model=MetricDefinitionsResponse | OrderMetricDefinitionsResponse,
        dependencies=[Depends(require_principal)],
    )
    def get_metric_definitions(
        domain: Literal["behavior", "orders"],
        version: Literal["behavior-v1", "orders-v1"],
    ) -> MetricDefinitionsResponse | OrderMetricDefinitionsResponse:
        if domain == "behavior" and version == "behavior-v1":
            return behavior_response(
                "metric_definitions", behavior_service.get_definitions
            )
        if domain == "orders" and version == "orders-v1":
            return order_response(
                "order_metric_definitions", order_service.get_definitions
            )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="domain and version do not match",
        )

    @app.post("/analysis/realtime", response_model=AnalysisResponse, dependencies=[Depends(require_analyst), Depends(require_csrf)])
    def analyze_realtime(request: AnalysisRequest) -> AnalysisResponse:
        if len(request.question) > settings.ai_max_question_length:
            raise HTTPException(status_code=422, detail="question is too long")
        stage = "analysis_route"
        try:
            result = analysis_service.analyze(request.question)
            stage = "analysis_response"
            return AnalysisResponse.model_validate(result)
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
                extra={"stage": stage, "error_type": type(exc).__name__},
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="analysis is temporarily unavailable",
            ) from None

    @app.post("/analysis/tools", response_model=ToolAnalysisResponse, dependencies=[Depends(require_analyst), Depends(require_csrf)])
    def analyze_with_tools(request: ToolAnalysisRequest) -> ToolAnalysisResponse:
        if len(request.question) > settings.ai_max_question_length:
            raise HTTPException(status_code=422, detail="question is too long")
        stage = "tool_analysis_route"
        try:
            result = tool_analysis_service.analyze(request.question)
            stage = "tool_analysis_response"
            return ToolAnalysisResponse.model_validate(result)
        except ToolAnalysisUnavailableError:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="analysis tools are temporarily unavailable",
            ) from None
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(
                "tool_analysis_route_failed",
                extra={"stage": stage, "error_type": type(exc).__name__},
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="analysis tools are temporarily unavailable",
            ) from None

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "realtime-metrics-api"}

    @app.get("/ready")
    def ready() -> dict[str, object]:
        try:
            readiness_service.check()
            return {
                "status": "ready",
                "dependencies": {
                    "doris": "ready",
                    "trino": "ready",
                    "flink": "ready",
                },
            }
        except Exception as exc:
            logger.error("readiness_check_failed", extra={"error_type": type(exc).__name__})
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="service is not ready",
            ) from None

    @app.get("/metrics/realtime", dependencies=[Depends(require_principal)])
    def get_realtime_metrics() -> dict[str, object]:
        return repository.fetch_all_metrics()

    @app.get("/metrics/{metric_name}", dependencies=[Depends(require_principal)])
    def get_metric(metric_name: str) -> dict[str, object]:
        metric = repository.fetch_metric(metric_name)
        if metric is None:
            raise HTTPException(status_code=404, detail=f"metric '{metric_name}' not found")
        return metric

    if settings.web_dist_dir is not None:
        app.mount("/app", StaticFiles(directory=settings.web_dist_dir, html=True), name="web")

    return app


app = create_app()
