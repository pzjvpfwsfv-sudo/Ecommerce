import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "api"))

from app.agent_provider import build_model  # noqa: E402
from app.config import AgentProviderSettings, load_agent_provider_settings, load_settings  # noqa: E402


class G4AgentProviderTest(unittest.TestCase):
    def test_default_is_off_and_does_not_construct_a_model(self):
        settings = load_agent_provider_settings({})
        self.assertEqual(settings.provider, "off")
        self.assertIsNone(build_model(settings))
        self.assertEqual(load_settings({}).g4_agent, settings)

    def test_existing_ai_configuration_does_not_enable_g4(self):
        settings = load_settings({
            "AI_ANALYZER_MODE": "openai_compatible",
            "AI_BASE_URL": "https://api.deepseek.com/v1",
            "AI_API_KEY": "legacy-secret",
            "AI_MODEL": "legacy-model",
        })
        self.assertIsNone(build_model(settings.g4_agent))

    def test_remote_requires_explicit_key_and_model(self):
        base = "https://dashscope.aliyuncs.com/compatible-mode/v1"
        for model, key in (("", "secret"), ("qwen-plus", "")):
            with self.subTest(model=model, has_key=bool(key)):
                with self.assertRaises(ValueError):
                    build_model(AgentProviderSettings(
                        provider="openai_compatible", base_url=base,
                        model=model, api_key=key,
                    ))

    def test_remote_rejects_unsafe_endpoints(self):
        rejected = (
            "http://api.deepseek.com/v1",
            "https://example.com/v1",
            "https://api.deepseek.com.evil.example/v1",
            "https://user:pass@api.deepseek.com/v1",
            "https://api.deepseek.com:444/v1",
            "https://api.deepseek.com/v1?token=secret",
            " https://api.deepseek.com/v1",
        )
        for endpoint in rejected:
            with self.subTest(endpoint=endpoint):
                with self.assertRaises(ValueError):
                    build_model(AgentProviderSettings(
                        provider="openai_compatible", base_url=endpoint,
                        model="deepseek-chat", api_key="secret",
                    ))

    def test_ollama_requires_loopback_and_model(self):
        for endpoint in (
            "http://0.0.0.0:11434",
            "http://192.168.1.2:11434",
            "http://localhost.evil.example:11434",
            "http://user:pass@localhost:11434",
            "http://localhost:11434/api?x=1",
        ):
            with self.subTest(endpoint=endpoint):
                with self.assertRaises(ValueError):
                    build_model(AgentProviderSettings(
                        provider="ollama", base_url=endpoint, model="qwen3:4b",
                    ))
        with self.assertRaises(ValueError):
            build_model(AgentProviderSettings(
                provider="ollama", base_url="http://localhost:11434", model=" ",
            ))

    def test_remote_client_has_fixed_output_and_bounded_timeout(self):
        with patch("langchain_openai.ChatOpenAI") as client:
            client.return_value = object()
            model = build_model(AgentProviderSettings(
                provider="openai_compatible",
                base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
                model="qwen-plus", api_key="unit-secret", timeout_seconds=90,
            ))
        self.assertIs(model, client.return_value)
        self.assertEqual(client.call_args.kwargs["max_tokens"], 800)
        self.assertEqual(client.call_args.kwargs["timeout"], 45)
        self.assertEqual(client.call_args.kwargs["max_retries"], 0)
        self.assertFalse(client.call_args.kwargs["use_responses_api"])

    def test_local_client_has_fixed_output_and_bounded_timeout(self):
        with patch("langchain_ollama.ChatOllama") as client:
            client.return_value = object()
            model = build_model(AgentProviderSettings(
                provider="ollama", base_url="http://127.0.0.1:11434",
                model="qwen3:4b", timeout_seconds=60,
            ))
        self.assertIs(model, client.return_value)
        self.assertEqual(client.call_args.kwargs["num_predict"], 800)
        self.assertEqual(client.call_args.kwargs["client_kwargs"]["timeout"], 45)
        self.assertFalse(client.call_args.kwargs["validate_model_on_init"])

    def test_secret_is_not_exposed_in_repr_or_validation_error(self):
        settings = AgentProviderSettings(
            provider="openai_compatible", base_url="https://example.com/v1",
            model="qwen-plus", api_key="unit-secret",
        )
        self.assertNotIn("unit-secret", repr(settings))
        with self.assertRaises(ValueError) as raised:
            build_model(settings)
        self.assertNotIn("unit-secret", str(raised.exception))

    def test_invalid_provider_and_timeout_fail_without_client(self):
        for settings in (
            AgentProviderSettings(provider="other"),
            AgentProviderSettings(provider="ollama", base_url="http://localhost:11434", model="qwen3:4b", timeout_seconds=0),
        ):
            with self.subTest(settings=settings):
                with self.assertRaises(ValueError):
                    build_model(settings)


if __name__ == "__main__":
    unittest.main()
