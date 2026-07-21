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
    )
