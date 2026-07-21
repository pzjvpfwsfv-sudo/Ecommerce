from datetime import datetime, timezone
from pathlib import Path
import sys
import traceback
import unittest
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app.analysis_models import AnalysisNarrative, HistoricalEvidence
from app.analysis_service import AnalysisService, AnalysisUnavailableError, RealtimeDataUnavailableError
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
        self.trino.fetch_summary.side_effect = RuntimeError("sensitive trino detail")
        service = AnalysisService(self.realtime, self.trino, RuleBasedAnalyzer(), RuleBasedAnalyzer(), self.clock)

        with self.assertLogs("app.analysis_service", level="WARNING") as captured:
            response = service.analyze("\u5206\u6790\u6d3b\u8dc3\u5ea6")

        self.assertIsNone(response.evidence.historical)
        self.assertTrue(any("\u5386\u53f2\u6570\u636e" in warning for warning in response.warnings))
        record = captured.records[0]
        self.assertEqual("analysis_trino_degraded", getattr(record, "event", None))
        self.assertIsInstance(record.request_id, str)
        self.assertIsInstance(record.trino_ms, float)
        self.assertEqual("rule_based", record.analyzer)
        self.assertIs(record.degraded, True)
        self.assertEqual("RuntimeError", record.error_type)
        self.assertIsNone(record.exc_info)
        self.assertNotIn("sensitive trino detail", record.getMessage())

    def test_primary_failure_falls_back_to_rule_analyzer(self):
        primary = Mock()
        primary.name = "openai_compatible"
        primary.analyze.side_effect = TimeoutError("timeout")
        service = AnalysisService(self.realtime, self.trino, primary, RuleBasedAnalyzer(), self.clock)

        with self.assertLogs("app.analysis_service", level="WARNING") as captured:
            response = service.analyze("\u5206\u6790\u6d3b\u8dc3\u5ea6")

        self.assertEqual("rule_based", response.analyzer)
        self.assertTrue(any("\u6a21\u578b\u5206\u6790" in warning for warning in response.warnings))
        record = captured.records[0]
        self.assertEqual("analysis_model_degraded", getattr(record, "event", None))
        self.assertIsInstance(record.request_id, str)
        self.assertIsInstance(record.analyzer_ms, float)
        self.assertEqual("openai_compatible", record.analyzer)
        self.assertIs(record.degraded, True)
        self.assertEqual("TimeoutError", record.error_type)
        self.assertIsNone(record.exc_info)
        self.assertNotIn("timeout", record.getMessage())

    def test_untrusted_primary_number_falls_back_to_rule_analyzer(self):
        primary = Mock()
        primary.name = "openai_compatible"
        primary.analyze.return_value = AnalysisNarrative(summary="There are 999 active users.")
        service = AnalysisService(self.realtime, self.trino, primary, RuleBasedAnalyzer(), self.clock)

        with self.assertLogs("app.analysis_service", level="WARNING") as captured:
            response = service.analyze("\u5206\u6790\u6d3b\u8dc3\u5ea6")

        self.assertEqual("rule_based", response.analyzer)
        self.assertNotIn("999", response.summary)
        record = captured.records[0]
        self.assertEqual("analysis_model_degraded", getattr(record, "event", None))
        self.assertEqual("NarrativeProvenanceError", record.error_type)

    def test_number_guard_rejects_common_untrusted_number_formats(self):
        for untrusted_number in (".999", "\uff19\uff19\uff19", "1,000", "9.99e2"):
            with self.subTest(untrusted_number=untrusted_number):
                primary = Mock()
                primary.name = "openai_compatible"
                primary.analyze.return_value = AnalysisNarrative(summary=f"value {untrusted_number}")
                service = AnalysisService(self.realtime, self.trino, primary, RuleBasedAnalyzer(), self.clock)

                with self.assertLogs("app.analysis_service", level="WARNING"):
                    response = service.analyze("\u5206\u6790\u6d3b\u8dc3\u5ea6")

                self.assertEqual("rule_based", response.analyzer)

    def test_primary_number_guard_fails_closed_for_numeric_bypasses(self):
        untrusted_numbers = (
            "\u4e00\u4e07",
            "\uff11\uff12\uff0c\uff10\uff10\uff15",
            "12 005",
            "12_005",
            "12'005",
            "12\u2019005",
            "\u216b",
        )
        for untrusted_number in untrusted_numbers:
            with self.subTest(untrusted_number=untrusted_number):
                primary = Mock()
                primary.name = "openai_compatible"
                primary.analyze.return_value = AnalysisNarrative(summary=f"value {untrusted_number}")
                service = AnalysisService(self.realtime, self.trino, primary, RuleBasedAnalyzer(), self.clock)

                with self.assertLogs("app.analysis_service", level="WARNING"):
                    response = service.analyze("\u5206\u6790\u6d3b\u8dc3\u5ea6")

                self.assertEqual("rule_based", response.analyzer)

    def test_fallback_number_guard_fails_closed_for_numeric_bypasses(self):
        untrusted_numbers = (
            "\u4e00\u4e07",
            "\uff11\uff12\uff0c\uff10\uff10\uff15",
            "12 005",
            "12_005",
            "12'005",
            "12\u2019005",
            "\u216b",
        )
        for untrusted_number in untrusted_numbers:
            with self.subTest(untrusted_number=untrusted_number):
                primary = Mock()
                primary.name = "openai_compatible"
                primary.analyze.side_effect = TimeoutError("sensitive primary detail")
                fallback = Mock()
                fallback.name = "rule_based"
                fallback.analyze.return_value = AnalysisNarrative(summary=f"value {untrusted_number}")
                service = AnalysisService(self.realtime, self.trino, primary, fallback, self.clock)

                with self.assertLogs("app.analysis_service", level="WARNING") as captured:
                    with self.assertRaises(AnalysisUnavailableError):
                        service.analyze("\u5206\u6790\u6d3b\u8dc3\u5ea6")

                fallback_record = next(
                    record
                    for record in captured.records
                    if getattr(record, "event", None) == "analysis_fallback_failed"
                )
                self.assertEqual("NarrativeProvenanceError", fallback_record.error_type)

    def test_untrusted_fallback_number_raises_safe_domain_error(self):
        primary = Mock()
        primary.name = "openai_compatible"
        primary.analyze.side_effect = TimeoutError("sensitive primary detail")
        fallback = Mock()
        fallback.name = "rule_based"
        fallback.analyze.return_value = AnalysisNarrative(summary="There are 999 active users.")
        service = AnalysisService(self.realtime, self.trino, primary, fallback, self.clock)

        with self.assertLogs("app.analysis_service", level="WARNING") as captured:
            with self.assertRaises(AnalysisUnavailableError) as raised:
                service.analyze("\u5206\u6790\u6d3b\u8dc3\u5ea6")

        self.assertEqual("Metric analysis is unavailable", str(raised.exception))
        fallback_record = next(
            (record for record in captured.records if getattr(record, "event", None) == "analysis_fallback_failed"),
            None,
        )
        self.assertIsNotNone(fallback_record)
        self.assertEqual("NarrativeProvenanceError", fallback_record.error_type)

    def test_primary_and_fallback_failures_log_fallback_and_raise_safe_error(self):
        primary = Mock()
        primary.name = "openai_compatible"
        primary.analyze.side_effect = TimeoutError("sensitive primary detail")
        fallback = Mock()
        fallback.name = "rule_based"
        fallback.analyze.side_effect = RuntimeError("sensitive fallback detail")
        service = AnalysisService(self.realtime, self.trino, primary, fallback, self.clock)

        with self.assertLogs("app.analysis_service", level="WARNING") as captured:
            with self.assertRaises(AnalysisUnavailableError) as raised:
                service.analyze("\u5206\u6790\u6d3b\u8dc3\u5ea6")

        self.assertEqual("Metric analysis is unavailable", str(raised.exception))
        formatted = "".join(traceback.format_exception(raised.exception))
        self.assertNotIn("sensitive primary detail", formatted)
        self.assertNotIn("sensitive fallback detail", formatted)
        self.assertIsNone(raised.exception.__cause__)
        fallback_record = next(
            (record for record in captured.records if getattr(record, "event", None) == "analysis_fallback_failed"),
            None,
        )
        self.assertIsNotNone(fallback_record)
        self.assertIsInstance(fallback_record.request_id, str)
        self.assertIsInstance(fallback_record.analyzer_ms, float)
        self.assertEqual("rule_based", fallback_record.analyzer)
        self.assertIs(fallback_record.degraded, True)
        self.assertEqual("RuntimeError", fallback_record.error_type)
        self.assertIsNone(fallback_record.exc_info)
        self.assertNotIn("sensitive primary detail", fallback_record.getMessage())
        self.assertNotIn("sensitive fallback detail", fallback_record.getMessage())

    def test_service_logs_request_id_timings_and_analyzer(self):
        service = AnalysisService(self.realtime, self.trino, RuleBasedAnalyzer(), RuleBasedAnalyzer(), self.clock)

        with self.assertLogs("app.analysis_service", level="INFO") as captured:
            service.analyze("\u5206\u6790\u6d3b\u8dc3\u5ea6")

        record = captured.records[0]
        self.assertEqual("analysis_complete", getattr(record, "event", None))
        self.assertIsInstance(record.request_id, str)
        self.assertIsInstance(record.doris_ms, float)
        self.assertIsInstance(record.trino_ms, float)
        self.assertIsInstance(record.analyzer_ms, float)
        self.assertEqual("rule_based", record.analyzer)
        self.assertIs(record.degraded, False)
        self.assertIsNone(record.error_type)

    def test_realtime_failure_is_fatal(self):
        self.realtime.fetch_all_metrics.side_effect = RuntimeError("sensitive doris detail")
        service = AnalysisService(self.realtime, self.trino, RuleBasedAnalyzer(), RuleBasedAnalyzer(), self.clock)

        with self.assertLogs("app.analysis_service", level="ERROR") as captured:
            with self.assertRaises(RealtimeDataUnavailableError) as raised:
                service.analyze("\u5206\u6790\u6d3b\u8dc3\u5ea6")

        formatted = "".join(traceback.format_exception(raised.exception))
        self.assertNotIn("sensitive doris detail", formatted)
        self.assertIsNone(raised.exception.__cause__)
        record = captured.records[0]
        self.assertEqual("analysis_doris_failed", getattr(record, "event", None))
        self.assertIsInstance(record.request_id, str)
        self.assertIsInstance(record.doris_ms, float)
        self.assertEqual("rule_based", record.analyzer)
        self.assertIs(record.degraded, False)
        self.assertEqual("RuntimeError", record.error_type)
        self.assertIsNone(record.exc_info)
        self.assertNotIn("sensitive doris detail", record.getMessage())


if __name__ == "__main__":
    unittest.main()
