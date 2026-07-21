from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import unittest

import httpx


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app import analyzers
from app.analysis_models import AnalysisContext, AnalysisEvidence, RealtimeEvidence
from app.config import ApiSettings, load_settings


class OpenAICompatibleAnalyzerTest(unittest.TestCase):
    def test_adapter_sends_evidence_and_parses_structured_narrative(self):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.read())
            captured["authorization"] = request.headers["Authorization"]
            return httpx.Response(
                200,
                json={
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "summary": "Grounded conclusion",
                                        "insights": [],
                                        "risks": [],
                                        "actions": [],
                                    }
                                )
                            }
                        }
                    ]
                },
            )

        analyzer = getattr(analyzers, "OpenAICompatibleAnalyzer")(
            api_key="secret",
            base_url="http://model.local/v1",
            model="demo-model",
            client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
        )
        context = AnalysisContext(
            question="Analyze activity",
            generated_at=datetime(2026, 7, 18, tzinfo=timezone.utc),
            evidence=AnalysisEvidence(realtime=RealtimeEvidence(pv=12, uv=5)),
        )

        result = analyzer.analyze(context)

        self.assertEqual("Grounded conclusion", result.summary)
        self.assertEqual("Bearer secret", captured["authorization"])
        self.assertEqual(12, json.loads(captured["body"]["messages"][1]["content"])["evidence"]["realtime"]["pv"])
        system_prompt = captured["body"]["messages"][0]["content"]
        for requirement in ("evidence", "SQL", "outside", "insufficient", "Chinese or English"):
            self.assertIn(requirement, system_prompt)

    def test_load_settings_defaults_to_rule_mode(self):
        settings = load_settings(environ={})

        self.assertEqual("rule_based", settings.ai_analyzer_mode)
        self.assertEqual(500, settings.ai_max_question_length)

    def test_build_analysis_service_uses_configured_openai_analyzer(self):
        from app.dependencies import build_analysis_service

        realtime_repository = object()
        service = build_analysis_service(
            ApiSettings(
                ai_analyzer_mode="openai_compatible",
                ai_api_key="secret",
                ai_base_url="http://model.local/v1",
                ai_model="demo-model",
                ai_request_timeout_seconds=7,
                trino_base_url="http://trino.local:8088",
                trino_user="test-user",
                trino_catalog="test-catalog",
                trino_schema="test-schema",
                trino_request_timeout_seconds=3,
            ),
            realtime_repository,
        )

        self.assertIs(service._realtime, realtime_repository)
        self.assertIsInstance(service._primary, getattr(analyzers, "OpenAICompatibleAnalyzer"))
        self.assertIsInstance(service._fallback, analyzers.RuleBasedAnalyzer)
        self.assertEqual("http://trino.local:8088", service._historical._base_url)
        self.assertEqual(3, service._historical._timeout_seconds)

    def test_build_analysis_service_rejects_unknown_analyzer_mode(self):
        from app.dependencies import build_analysis_service

        with self.assertRaisesRegex(ValueError, "unsupported AI_ANALYZER_MODE: unknown"):
            build_analysis_service(ApiSettings(ai_analyzer_mode="unknown"), object())


if __name__ == "__main__":
    unittest.main()
