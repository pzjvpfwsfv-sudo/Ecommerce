from datetime import UTC, datetime

from app.analysis_service import AnalysisService
from app.analyzers import OpenAICompatibleAnalyzer, RuleBasedAnalyzer
from app.behavior_repository import BehaviorMetricsRepository
from app.behavior_service import BehaviorMetricsService
from app.config import ApiSettings
from app.flink_quality_repository import FlinkQualityRepository
from app.metric_definitions import MetricDefinitionCatalog
from app.readiness_service import ReadinessService
from app.repository import RealtimeMetricsRepository
from app.tool_analysis_service import ToolAnalysisService
from app.tool_executor import ToolExecutor
from app.tool_narratives import OpenAICompatibleToolNarrativeAnalyzer, RuleBasedToolNarrativeAnalyzer
from app.tool_planners import OpenAICompatibleToolPlanner, RuleBasedToolPlanner
from app.tool_runner import BoundedToolRunner
from app.trino_repository import TrinoAnalyticsRepository


def build_analysis_service(
    settings: ApiSettings,
    realtime_repository: RealtimeMetricsRepository,
) -> AnalysisService:
    fallback = RuleBasedAnalyzer()
    primary = fallback
    if settings.ai_analyzer_mode == "openai_compatible":
        primary = OpenAICompatibleAnalyzer(
            api_key=settings.ai_api_key,
            base_url=settings.ai_base_url,
            model=settings.ai_model,
            timeout_seconds=settings.ai_request_timeout_seconds,
        )
    elif settings.ai_analyzer_mode != "rule_based":
        raise ValueError(f"unsupported AI_ANALYZER_MODE: {settings.ai_analyzer_mode}")

    historical = TrinoAnalyticsRepository(
        base_url=settings.trino_base_url,
        user=settings.trino_user,
        catalog=settings.trino_catalog,
        schema=settings.trino_schema,
        timeout_seconds=settings.trino_request_timeout_seconds,
    )
    return AnalysisService(realtime_repository, historical, primary, fallback)


def build_behavior_metrics_service(settings: ApiSettings) -> BehaviorMetricsService:
    catalog = MetricDefinitionCatalog.load(settings.behavior_metric_definitions_path)
    repository = BehaviorMetricsRepository.from_settings(settings)
    return BehaviorMetricsService(repository, catalog)


def build_tool_analysis_service(
    settings: ApiSettings,
    realtime_repository: RealtimeMetricsRepository,
) -> ToolAnalysisService:
    fallback_planner = RuleBasedToolPlanner()
    primary_planner = fallback_planner
    if settings.ai_tool_planner_mode == "openai_compatible":
        primary_planner = OpenAICompatibleToolPlanner(
            api_key=settings.ai_api_key,
            base_url=settings.ai_base_url,
            model=settings.ai_model,
            timeout_seconds=settings.ai_request_timeout_seconds,
        )

    fallback_analyzer = RuleBasedToolNarrativeAnalyzer()
    primary_analyzer = fallback_analyzer
    if settings.ai_analyzer_mode == "openai_compatible":
        primary_analyzer = OpenAICompatibleToolNarrativeAnalyzer(
            api_key=settings.ai_api_key,
            base_url=settings.ai_base_url,
            model=settings.ai_model,
            timeout_seconds=settings.ai_request_timeout_seconds,
        )

    historical_repository = TrinoAnalyticsRepository(
        base_url=settings.trino_base_url,
        user=settings.trino_user,
        catalog=settings.trino_catalog,
        schema=settings.trino_schema,
        timeout_seconds=settings.trino_request_timeout_seconds,
    )
    quality_repository = FlinkQualityRepository(
        base_url=settings.flink_rest_url,
        production_job_name=settings.chapter9_production_job_name,
        timeout_seconds=settings.ai_tool_total_timeout_seconds,
        checkpoint_max_age_seconds=settings.flink_checkpoint_max_age_seconds,
        clock=lambda: datetime.now(UTC),
    )
    executor = ToolExecutor(
        realtime_repository=realtime_repository,
        historical_repository=historical_repository,
        quality_repository=quality_repository,
        max_calls=settings.ai_tool_max_calls,
        total_timeout_seconds=settings.ai_tool_total_timeout_seconds,
        max_event_types=settings.ai_tool_max_event_types,
        runner=BoundedToolRunner(max_workers=settings.ai_tool_executor_max_workers),
    )
    return ToolAnalysisService(
        primary_planner=primary_planner,
        fallback_planner=fallback_planner,
        executor=executor,
        primary_analyzer=primary_analyzer,
        fallback_analyzer=fallback_analyzer,
        total_timeout_seconds=settings.ai_tool_total_timeout_seconds,
    )


def build_readiness_service(settings: ApiSettings) -> ReadinessService:
    doris_repository = RealtimeMetricsRepository.from_settings(settings)
    trino_repository = TrinoAnalyticsRepository(
        base_url=settings.trino_base_url,
        user=settings.trino_user,
        catalog=settings.trino_catalog,
        schema=settings.trino_schema,
        timeout_seconds=settings.trino_request_timeout_seconds,
    )
    flink_repository = FlinkQualityRepository(
        base_url=settings.flink_rest_url,
        production_job_name=settings.chapter9_production_job_name,
        timeout_seconds=settings.ai_tool_total_timeout_seconds,
        checkpoint_max_age_seconds=settings.flink_checkpoint_max_age_seconds,
        clock=lambda: datetime.now(UTC),
    )
    return ReadinessService(doris_repository, trino_repository, flink_repository)
