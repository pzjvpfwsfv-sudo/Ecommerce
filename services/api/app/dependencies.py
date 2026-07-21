from app.analysis_service import AnalysisService
from app.analyzers import OpenAICompatibleAnalyzer, RuleBasedAnalyzer
from app.config import ApiSettings
from app.repository import RealtimeMetricsRepository
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
