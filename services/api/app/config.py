from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from os import environ as os_environ


@dataclass(frozen=True)
class ApiSettings:
    doris_host: str = "127.0.0.1"
    doris_port: int = 9030
    doris_database: str = "analytics"
    doris_username: str = "root"
    doris_password: str = ""
    trino_base_url: str = "http://localhost:8088"
    trino_user: str = "ecommerce-ai"
    trino_catalog: str = "lakehouse"
    trino_schema: str = "analytics"
    trino_request_timeout_seconds: float = 10
    ai_analyzer_mode: str = "rule_based"
    ai_api_key: str = ""
    ai_base_url: str = ""
    ai_model: str = ""
    ai_request_timeout_seconds: float = 15
    ai_max_question_length: int = 500
    flink_rest_url: str = "http://flink-jobmanager:8081"
    chapter9_production_job_name: str = "chapter-9-datastream-quality-production"
    flink_checkpoint_max_age_seconds: int = 120
    ai_tool_planner_mode: str = "rule_based"
    ai_tool_max_calls: int = 3
    ai_tool_total_timeout_seconds: float = 20
    ai_tool_max_event_types: int = 20

    def __post_init__(self) -> None:
        if self.ai_tool_planner_mode not in {"rule_based", "openai_compatible"}:
            raise ValueError(f"unsupported AI_TOOL_PLANNER_MODE: {self.ai_tool_planner_mode}")
        if not 1 <= self.ai_tool_max_calls <= 3:
            raise ValueError("AI_TOOL_MAX_CALLS must be between 1 and 3")
        if not 0 < self.ai_tool_total_timeout_seconds <= 60:
            raise ValueError("AI_TOOL_TOTAL_TIMEOUT_SECONDS must be between 0 and 60")
        if not 1 <= self.ai_tool_max_event_types <= 100:
            raise ValueError("AI_TOOL_MAX_EVENT_TYPES must be between 1 and 100")
        if not self.flink_rest_url:
            raise ValueError("FLINK_REST_URL must not be empty")
        if not self.chapter9_production_job_name:
            raise ValueError("CHAPTER9_PRODUCTION_JOB_NAME must not be empty")
        if not 1 <= self.flink_checkpoint_max_age_seconds <= 3600:
            raise ValueError("FLINK_CHECKPOINT_MAX_AGE_SECONDS must be between 1 and 3600")


def load_settings(environ: Mapping[str, str] | None = None) -> ApiSettings:
    values = os_environ if environ is None else environ
    return ApiSettings(
        doris_host=values.get("DORIS_HOST", "127.0.0.1"),
        doris_port=int(values.get("DORIS_PORT", "9030")),
        doris_database=values.get("DORIS_DATABASE", "analytics"),
        doris_username=values.get("DORIS_USERNAME", "root"),
        doris_password=values.get("DORIS_PASSWORD", ""),
        trino_base_url=values.get("TRINO_BASE_URL", "http://localhost:8088"),
        trino_user=values.get("TRINO_USER", "ecommerce-ai"),
        trino_catalog=values.get("TRINO_CATALOG", "lakehouse"),
        trino_schema=values.get("TRINO_SCHEMA", "analytics"),
        trino_request_timeout_seconds=float(values.get("TRINO_REQUEST_TIMEOUT_SECONDS", "10")),
        ai_analyzer_mode=values.get("AI_ANALYZER_MODE", "rule_based"),
        ai_api_key=values.get("AI_API_KEY", ""),
        ai_base_url=values.get("AI_BASE_URL", ""),
        ai_model=values.get("AI_MODEL", ""),
        ai_request_timeout_seconds=float(values.get("AI_REQUEST_TIMEOUT_SECONDS", "15")),
        ai_max_question_length=int(values.get("AI_MAX_QUESTION_LENGTH", "500")),
        flink_rest_url=values.get("FLINK_REST_URL", "http://flink-jobmanager:8081"),
        chapter9_production_job_name=values.get(
            "CHAPTER9_PRODUCTION_JOB_NAME", "chapter-9-datastream-quality-production"
        ),
        flink_checkpoint_max_age_seconds=int(values.get("FLINK_CHECKPOINT_MAX_AGE_SECONDS", "120")),
        ai_tool_planner_mode=values.get("AI_TOOL_PLANNER_MODE", "rule_based"),
        ai_tool_max_calls=int(values.get("AI_TOOL_MAX_CALLS", "3")),
        ai_tool_total_timeout_seconds=float(values.get("AI_TOOL_TOTAL_TIMEOUT_SECONDS", "20")),
        ai_tool_max_event_types=int(values.get("AI_TOOL_MAX_EVENT_TYPES", "20")),
    )
