from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
import sys
import unittest

from pydantic import ValidationError


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app.analysis_models import AnalysisNarrative
from app.analysis_service import (
    NarrativeProvenanceError,
    validate_narrative_numbers,
    validate_narrative_output_safety,
)
from app.tool_models import (
    DataQualityEvidence,
    HistoricalEvidence,
    RealtimeEvidence,
    ToolAnalysisContext,
    ToolAnalysisSelection,
    ToolEvidence,
)
from app.tool_narratives import RuleBasedToolNarrativeAnalyzer, render_tool_selection
from app.tool_deadline import use_tool_deadline


COUNTERS = {
    "valid_events_total": 2,
    "dlq_events_total": 1,
    "late_events_total": 0,
    "duplicate_events_total": 0,
    "parse_errors_total": 0,
    "validation_errors_total": 0,
}


def complete_context() -> ToolAnalysisContext:
    return ToolAnalysisContext(
        question="综合分析",
        generated_at=datetime(2026, 8, 3, tzinfo=UTC),
        evidence=ToolEvidence(
            realtime=RealtimeEvidence(pv=4, uv=2),
            historical=HistoricalEvidence(event_count=2, event_type_counts={"view": 2}),
            data_quality=DataQualityEvidence(
                job_id="a" * 32,
                job_state="RUNNING",
                completed_checkpoints=2,
                failed_checkpoints=0,
                latest_completed_at=datetime(2026, 8, 3, tzinfo=UTC),
                counters=COUNTERS,
            ),
        ),
    )


def realtime_only_context() -> ToolAnalysisContext:
    return ToolAnalysisContext(
        question="当前指标",
        generated_at=datetime(2026, 8, 3, tzinfo=UTC),
        evidence=ToolEvidence(realtime=RealtimeEvidence(pv=2, uv=1)),
    )


def quality_claim_selection() -> ToolAnalysisSelection:
    return ToolAnalysisSelection(
        summary="quality_only",
        insights=["checkpoint_status"],
        risks=[],
        actions=[],
    )


class ToolNarrativesTest(unittest.TestCase):
    def test_model_selection_uses_remaining_deadline_budget_for_transport_timeout(self):
        from app.tool_narratives import OpenAICompatibleToolNarrativeAnalyzer
        import httpx
        import json

        captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["timeout"] = request.extensions["timeout"]
            return httpx.Response(
                200,
                json={"choices": [{"message": {"content": json.dumps({"summary": "realtime_only", "insights": [], "risks": [], "actions": []})}}]},
            )

        analyzer = OpenAICompatibleToolNarrativeAnalyzer(
            api_key="test-key",
            base_url="https://model.invalid/v1",
            model="test-model",
            timeout_seconds=7,
            client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
        )
        timer = iter((0.0, 18.0))

        with use_tool_deadline(20, timer=lambda: next(timer)):
            analyzer.select(realtime_only_context())

        self.assertEqual({"connect": 2, "read": 2, "write": 2, "pool": 2}, captured["timeout"])
    def test_rule_narrative_uses_only_available_tool_evidence(self):
        narrative = RuleBasedToolNarrativeAnalyzer().analyze(complete_context())

        self.assertIn("2", narrative.summary)
        self.assertTrue(any("checkpoint" in item.lower() for item in narrative.insights))

    def test_quality_event_count_does_not_double_count_overlapping_counters(self):
        context = complete_context()
        context.evidence.data_quality.counters.update(
            {
                "dlq_events_total": 6,
                "late_events_total": 2,
                "duplicate_events_total": 3,
                "parse_errors_total": 1,
                "validation_errors_total": 2,
            }
        )
        selection = ToolAnalysisSelection(
            summary="quality_only",
            insights=["quality_event_counts"],
            risks=[],
            actions=[],
        )

        narrative = render_tool_selection(selection, context)

        self.assertEqual(["数据质量计数中累计异常或拒绝事件为 8 条。"], narrative.insights)

    def test_model_selection_cannot_claim_missing_evidence_or_invent_numbers(self):
        with self.assertRaises(ValueError):
            render_tool_selection(quality_claim_selection(), realtime_only_context())
        with self.assertRaises(NarrativeProvenanceError):
            validate_narrative_numbers(
                AnalysisNarrative(summary="当前有 999 个异常事件。"),
                allowed_numbers={Decimal(2)},
            )

    def test_selection_rejects_unknown_or_duplicate_claim_ids_as_a_whole(self):
        for payload in (
            {
                "summary": "unknown",
                "insights": [],
                "risks": [],
                "actions": [],
            },
            {
                "summary": "realtime_only",
                "insights": ["visits_per_user", "visits_per_user"],
                "risks": [],
                "actions": [],
            },
        ):
            with self.subTest(payload=payload), self.assertRaises(ValidationError):
                ToolAnalysisSelection.model_validate(payload)

    def test_output_safety_allows_ordinary_keyword_substrings(self):
        for summary in (
            "Select the strongest channel for the next campaign.",
            "Update the plan after reviewing the evidence.",
            "Grant discounts on weekends only after approval.",
        ):
            with self.subTest(summary=summary):
                validate_narrative_output_safety(AnalysisNarrative(summary=summary))


if __name__ == "__main__":
    unittest.main()
