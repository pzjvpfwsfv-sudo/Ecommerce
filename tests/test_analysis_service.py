from datetime import datetime, timezone
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app.analysis_models import AnalysisNarrative, HistoricalEvidence
from app.analysis_service import AnalysisService, RealtimeDataUnavailableError
from app.analyzers import RuleBasedAnalyzer


class AnalysisServiceTest(unittest.TestCase):
    def setUp(self):
        self.realtime = Mock()
        self.realtime.fetch_all_metrics.return_value = {
            "pv": 12,
            "uv": 5,
            "updated_at": "2026-07-18T10:00:00",
        }
        self.trino = Mock()
        self.trino.fetch_summary.return_value = HistoricalEvidence(
            event_count=20,
            event_type_counts={"view": 15, "click": 5},
            latest_event_time="2026-07-18T09:59:00Z",
        )
        self.clock = lambda: datetime(2026, 7, 18, tzinfo=timezone.utc)

    def test_service_returns_evidence_and_primary_analyzer_name(self):
        analyzer = Mock(name="primary")
        analyzer.name = "model"
        analyzer.analyze.return_value = AnalysisNarrative(summary="\u7ed3\u8bba")
        service = AnalysisService(self.realtime, self.trino, analyzer, RuleBasedAnalyzer(), self.clock)

        response = service.analyze("\u5206\u6790\u6d3b\u8dc3\u5ea6")

        self.assertEqual(12, response.evidence.realtime.pv)
        self.assertEqual(20, response.evidence.historical.event_count)
        self.assertEqual("model", response.analyzer)

    def test_trino_failure_adds_warning_and_keeps_realtime_analysis(self):
        self.trino.fetch_summary.side_effect = RuntimeError("trino down")
        service = AnalysisService(self.realtime, self.trino, RuleBasedAnalyzer(), RuleBasedAnalyzer(), self.clock)

        with self.assertLogs("app.analysis_service", level="WARNING") as captured:
            response = service.analyze("\u5206\u6790\u6d3b\u8dc3\u5ea6")

        self.assertIsNone(response.evidence.historical)
        self.assertTrue(any("\u5386\u53f2\u6570\u636e" in warning for warning in response.warnings))
        self.assertIn("analysis_trino_degraded", " ".join(captured.output))

    def test_primary_failure_falls_back_to_rule_analyzer(self):
        primary = Mock()
        primary.name = "openai_compatible"
        primary.analyze.side_effect = TimeoutError("timeout")
        service = AnalysisService(self.realtime, self.trino, primary, RuleBasedAnalyzer(), self.clock)

        with self.assertLogs("app.analysis_service", level="WARNING") as captured:
            response = service.analyze("\u5206\u6790\u6d3b\u8dc3\u5ea6")

        self.assertEqual("rule_based", response.analyzer)
        self.assertTrue(any("\u6a21\u578b\u5206\u6790" in warning for warning in response.warnings))
        self.assertIn("analysis_model_degraded", " ".join(captured.output))

    def test_service_logs_request_id_timings_and_analyzer(self):
        service = AnalysisService(self.realtime, self.trino, RuleBasedAnalyzer(), RuleBasedAnalyzer(), self.clock)

        with self.assertLogs("app.analysis_service", level="INFO") as captured:
            service.analyze("\u5206\u6790\u6d3b\u8dc3\u5ea6")

        joined = " ".join(captured.output)
        self.assertIn("request_id=", joined)
        self.assertIn("doris_ms=", joined)
        self.assertIn("trino_ms=", joined)
        self.assertIn("analyzer_ms=", joined)
        self.assertIn("analyzer=rule_based", joined)

    def test_realtime_failure_is_fatal(self):
        self.realtime.fetch_all_metrics.side_effect = RuntimeError("doris down")
        service = AnalysisService(self.realtime, self.trino, RuleBasedAnalyzer(), RuleBasedAnalyzer(), self.clock)

        with self.assertLogs("app.analysis_service", level="ERROR") as captured:
            with self.assertRaises(RealtimeDataUnavailableError):
                service.analyze("\u5206\u6790\u6d3b\u8dc3\u5ea6")

        self.assertIn("analysis_doris_failed", " ".join(captured.output))


if __name__ == "__main__":
    unittest.main()
