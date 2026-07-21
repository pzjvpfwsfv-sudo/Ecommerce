from datetime import datetime, timezone
import unittest

from pydantic import ValidationError

from app.analysis_models import (
    AnalysisContext,
    AnalysisEvidence,
    AnalysisRequest,
    HistoricalEvidence,
    RealtimeEvidence,
)
from app.analyzers import RuleBasedAnalyzer


class AnalysisModelsAndRulesTest(unittest.TestCase):
    def test_request_strips_question_and_rejects_blank(self):
        self.assertEqual("\u6d3b\u8dc3\u60c5\u51b5", AnalysisRequest(question="  \u6d3b\u8dc3\u60c5\u51b5  ").question)
        with self.assertRaises(ValidationError):
            AnalysisRequest(question="   ")

    def test_request_normalizes_nfkc_and_rejects_invisible_controls(self):
        unsafe_questions = (
            "\u200b",
            "\u2060",
            "\u202e",
            "\x00",
            "activity\u200b?",
            "分析\u2060活跃度",
        )
        for question in unsafe_questions:
            with self.subTest(question=repr(question)):
                with self.assertRaises(ValidationError):
                    AnalysisRequest(question=question)

        self.assertEqual("Activity?", AnalysisRequest(question="  Ａｃｔｉｖｉｔｙ？  ").question)
        self.assertEqual("分析活跃度?", AnalysisRequest(question="  分析活跃度？  ").question)

    def test_rule_analyzer_uses_only_evidence(self):
        context = AnalysisContext(
            question="\u5f53\u524d\u7528\u6237\u6d3b\u8dc3\u60c5\u51b5\u5982\u4f55\uff1f",
            generated_at=datetime(2026, 7, 18, tzinfo=timezone.utc),
            evidence=AnalysisEvidence(
                realtime=RealtimeEvidence(pv=120, uv=80),
                historical=HistoricalEvidence(
                    event_count=1000,
                    event_type_counts={"view": 700, "click": 200, "cart": 70, "purchase": 30},
                ),
            ),
        )

        result = RuleBasedAnalyzer().analyze(context)

        self.assertIn("120", result.summary)
        self.assertTrue(any("1.5" in item for item in result.insights))
        self.assertTrue(any("\u7d2f\u8ba1" in item for item in result.risks))

    def test_rule_analyzer_handles_zero_uv_without_division(self):
        context = AnalysisContext(
            question="\u5206\u6790\u6d3b\u8dc3\u5ea6",
            generated_at=datetime(2026, 7, 18, tzinfo=timezone.utc),
            evidence=AnalysisEvidence(realtime=RealtimeEvidence(pv=10, uv=0)),
        )

        result = RuleBasedAnalyzer().analyze(context)

        self.assertTrue(any("UV" in item and "\u4e0d\u8db3" in item for item in result.risks))
        self.assertFalse(any("\u4eba\u5747" in item for item in result.insights))


if __name__ == "__main__":
    unittest.main()
