from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock

import httpx


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app import analyzers
from app.analysis_models import AnalysisContext, AnalysisEvidence, HistoricalEvidence, RealtimeEvidence
from app.analysis_service import AnalysisService
from app.config import ApiSettings, load_settings


class OpenAICompatibleAnalyzerTest(unittest.TestCase):
    def test_adapter_sends_evidence_and_renders_strict_claim_selection(self):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.read())
            captured["authorization"] = request.headers["Authorization"]
            captured["url"] = str(request.url)
            captured["timeout"] = request.extensions["timeout"]
            return httpx.Response(
                200,
                json={
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "summary": "realtime_overview",
                                        "insights": ["visits_per_user"],
                                        "risks": ["cumulative_metric_limit"],
                                        "actions": ["add_time_window_metrics"],
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
            timeout_seconds=7,
            client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
        )
        context = AnalysisContext(
            question="Analyze activity",
            generated_at=datetime(2026, 7, 18, tzinfo=timezone.utc),
            evidence=AnalysisEvidence(realtime=RealtimeEvidence(pv=12, uv=5)),
        )

        result = analyzer.analyze(context)

        self.assertEqual("当前累计访问 12 次，覆盖 5 名用户。", result.summary)
        self.assertEqual(["人均访问次数约为 2.4 次。"], result.insights)
        self.assertNotIn("realtime_overview", result.model_dump_json())
        self.assertEqual("Bearer secret", captured["authorization"])
        self.assertEqual("http://model.local/v1/chat/completions", captured["url"])
        self.assertEqual({"connect": 7, "read": 7, "write": 7, "pool": 7}, captured["timeout"])
        self.assertEqual("demo-model", captured["body"]["model"])
        self.assertEqual(12, json.loads(captured["body"]["messages"][1]["content"])["evidence"]["realtime"]["pv"])
        system_prompt = captured["body"]["messages"][0]["content"]
        for requirement in ("evidence", "SQL", "outside", "insufficient", "claim IDs"):
            self.assertIn(requirement, system_prompt)

    def test_free_text_unknown_claims_and_extra_fields_degrade_without_entering_response(self):
        complete_selection = {
            "summary": "realtime_overview",
            "insights": [],
            "risks": [],
            "actions": [],
        }
        malicious_outputs = (
            {**complete_selection, "summary": "SHOW TABLES"},
            {**complete_selection, "summary": "DESCRIBE user_behavior_detail"},
            {**complete_selection, "summary": "CALL system.runtime.kill_query('x')"},
            {**complete_selection, "summary": "EXPLAIN SELECT NULL"},
            {**complete_selection, "summary": "VALUES (secret)"},
            {**complete_selection, "summary": "WITH leaked AS (SELECT NULL) SELECT * FROM leaked"},
            {**complete_selection, "summary": "USE secret_catalog"},
            {**complete_selection, "summary": "SET SESSION hidden = true"},
            {**complete_selection, "summary": "import os"},
            {**complete_selection, "summary": "unknown_claim"},
            {**complete_selection, "extra": "SHOW TABLES"},
        )
        for model_output in malicious_outputs:
            with self.subTest(model_output=model_output):
                analyzer = analyzers.OpenAICompatibleAnalyzer(
                    api_key="test-key",
                    base_url="http://model.local/v1",
                    model="demo-model",
                    client_factory=lambda output=model_output: httpx.Client(
                        transport=httpx.MockTransport(
                            lambda request: httpx.Response(
                                200,
                                json={
                                    "choices": [
                                        {"message": {"content": json.dumps(output)}}
                                    ]
                                },
                            )
                        )
                    ),
                )
                realtime = Mock()
                realtime.fetch_all_metrics.return_value = {"pv": 12, "uv": 5}
                historical = Mock()
                historical.fetch_summary.return_value = HistoricalEvidence(event_count=0)
                service = AnalysisService(
                    realtime, historical, analyzer, analyzers.RuleBasedAnalyzer()
                )

                with self.assertLogs("app.analysis_service", level="WARNING"):
                    result = service.analyze("Analyze activity")

                self.assertEqual("rule_based", result.analyzer)
                serialized = result.model_dump_json()
                self.assertNotIn(str(model_output.get("summary")), serialized)
                if "extra" in model_output:
                    self.assertNotIn(str(model_output["extra"]), serialized)

    def test_adapter_failures_degrade_analysis_service_to_rule_based(self):
        complete_narrative = {
            "summary": "realtime_overview",
            "insights": [],
            "risks": [],
            "actions": [],
        }
        cases = {
            "non_2xx": lambda: httpx.Response(503, json={"error": "unavailable"}),
            "invalid_outer_json": lambda: httpx.Response(200, content=b"not-json"),
            "invalid_choices_structure": lambda: httpx.Response(200, json={"choices": []}),
            "invalid_message_structure": lambda: httpx.Response(200, json={"choices": [{}]}),
            "invalid_content_structure": lambda: httpx.Response(
                200,
                json={"choices": [{"message": {}}]},
            ),
            "invalid_inner_json": lambda: httpx.Response(
                200,
                json={"choices": [{"message": {"content": "not-json"}}]},
            ),
            **{
                f"missing_{missing_key}": (
                    lambda key=missing_key: httpx.Response(
                        200,
                        json={
                            "choices": [
                                {
                                    "message": {
                                        "content": json.dumps(
                                            {name: value for name, value in complete_narrative.items() if name != key}
                                        )
                                    }
                                }
                            ]
                        },
                    )
                )
                for missing_key in complete_narrative
            },
            "invalid_narrative_type": lambda: httpx.Response(
                200,
                json={
                    "choices": [
                        {
                            "message": {"content": json.dumps({**complete_narrative, "insights": "not-a-list"})}
                        }
                    ]
                },
            ),
        }

        for case_name, response_factory in cases.items():
            with self.subTest(case=case_name):
                analyzer = analyzers.OpenAICompatibleAnalyzer(
                    api_key="test-key",
                    base_url="http://model.local/v1",
                    model="demo-model",
                    client_factory=lambda factory=response_factory: httpx.Client(
                        transport=httpx.MockTransport(lambda request: factory())
                    ),
                )
                realtime = Mock()
                realtime.fetch_all_metrics.return_value = {"pv": 12, "uv": 5}
                historical = Mock()
                historical.fetch_summary.return_value = HistoricalEvidence(event_count=0)
                service = AnalysisService(realtime, historical, analyzer, analyzers.RuleBasedAnalyzer())

                with self.assertLogs("app.analysis_service", level="WARNING"):
                    result = service.analyze("Analyze activity")

                self.assertEqual("rule_based", result.analyzer)
                self.assertTrue(any("\u6a21\u578b\u5206\u6790" in warning for warning in result.warnings))

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
                trino_catalog="test_catalog",
                trino_schema="test_schema",
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
