from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from app.config import AgentProviderSettings

if TYPE_CHECKING:
    from langchain_core.language_models.chat_models import BaseChatModel


_REMOTE_HOSTS = frozenset({"dashscope.aliyuncs.com", "api.deepseek.com"})
_LOCAL_HOSTS = frozenset({"127.0.0.1", "localhost"})
_MAX_TIMEOUT_SECONDS = 45
_MAX_OUTPUT_TOKENS = 800


def _validate_base_url(raw: str, *, local: bool) -> None:
    if not raw or raw != raw.strip() or "\\" in raw or any(ord(char) < 32 for char in raw):
        raise ValueError("invalid G4_AGENT_BASE_URL")
    try:
        parsed = urlsplit(raw)
        host = parsed.hostname
        port = parsed.port
    except ValueError:
        raise ValueError("invalid G4_AGENT_BASE_URL") from None
    if parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
        raise ValueError("invalid G4_AGENT_BASE_URL")
    if local:
        if parsed.scheme not in {"http", "https"} or host not in _LOCAL_HOSTS or parsed.path not in {"", "/"}:
            raise ValueError("G4 Ollama URL must use localhost or 127.0.0.1")
    elif parsed.scheme != "https" or host not in _REMOTE_HOSTS or port not in {None, 443}:
        raise ValueError("G4 remote URL must use an allowed HTTPS endpoint")


def build_model(settings: AgentProviderSettings) -> BaseChatModel | None:
    if settings.provider not in {"off", "openai_compatible", "ollama"}:
        raise ValueError("unsupported G4_AGENT_PROVIDER")
    if settings.provider == "off":
        return None
    if not 0 < settings.timeout_seconds:
        raise ValueError("G4_AGENT_TIMEOUT_SECONDS must be positive")
    if not settings.model.strip():
        raise ValueError("G4_AGENT_MODEL must not be empty")
    timeout = min(settings.timeout_seconds, _MAX_TIMEOUT_SECONDS)

    if settings.provider == "openai_compatible":
        if not settings.api_key.strip():
            raise ValueError("G4_AGENT_API_KEY is required for remote models")
        _validate_base_url(settings.base_url, local=False)
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=settings.model,
            api_key=settings.api_key,
            base_url=settings.base_url,
            timeout=timeout,
            max_retries=0,
            max_tokens=_MAX_OUTPUT_TOKENS,
            use_responses_api=False,
        )

    _validate_base_url(settings.base_url, local=True)
    from langchain_ollama import ChatOllama

    return ChatOllama(
        model=settings.model,
        base_url=settings.base_url,
        num_predict=_MAX_OUTPUT_TOKENS,
        client_kwargs={"timeout": timeout},
        validate_model_on_init=False,
    )
