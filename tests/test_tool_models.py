from datetime import datetime, timezone
from pathlib import Path
import sys
import unittest
from uuid import UUID

from pydantic import ValidationError


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app.config import ApiSettings, load_settings
from app.tool_models import (
    DataQualityEvidence,
    ToolAnalysisResponse,
    ToolAnalysisSelection,
    ToolCall,
    ToolEvidence,
    ToolId,
    ToolPlan,
)


COUNTERS = {
    "valid_events_total": 2,
    "dlq_events_total": 5,
    "late_events_total": 1,
    "duplicate_events_total": 1,
    "parse_errors_total": 1,
    "validation_errors_total": 4,
}


class ToolModelsTest(unittest.TestCase):
    def test_tool_plan_rejects_duplicates_unknown_tools_and_extra_fields(self):
        with self.assertRaises(ValidationError):
            ToolPlan(calls=[ToolCall(tool_id=ToolId.REALTIME)] * 2)
        with self.assertRaises(ValidationError):
            ToolCall.model_validate({"tool_id": "run_sql"})
        with self.assertRaises(ValidationError):
            ToolCall.model_validate({"tool_id": ToolId.REALTIME, "arguments": {}})

    def test_tool_evidence_requires_at_least_one_partition(self):
        with self.assertRaises(ValidationError):
            ToolEvidence()

    def test_data_quality_evidence_requires_exact_non_boolean_counters(self):
        evidence = DataQualityEvidence(
            job_id="job-1",
            job_state="RUNNING",
            completed_checkpoints=3,
            failed_checkpoints=0,
            latest_completed_at=datetime(2026, 8, 3, tzinfo=timezone.utc),
            counters=COUNTERS,
        )

        self.assertEqual(COUNTERS, evidence.counters)
        for invalid_counters in (
            {key: value for key, value in COUNTERS.items() if key != "valid_events_total"},
            {**COUNTERS, "unknown": 1},
            {**COUNTERS, "valid_events_total": -1},
            {**COUNTERS, "valid_events_total": True},
        ):
            with self.subTest(counters=invalid_counters):
                with self.assertRaises(ValidationError):
                    DataQualityEvidence(
                        job_id="job-1",
                        job_state="RUNNING",
                        completed_checkpoints=0,
                        failed_checkpoints=0,
                        latest_completed_at=None,
                        counters=invalid_counters,
                    )

    def test_selection_rejects_duplicate_claims(self):
        for field, values in (
            ("insights", ["visits_per_user", "visits_per_user"]),
            ("risks", ["zero_uv", "zero_uv"]),
            ("actions", ["add_time_window_metrics", "add_time_window_metrics"]),
        ):
            with self.subTest(field=field):
                payload = {
                    "summary": "realtime_only",
                    "insights": [],
                    "risks": [],
                    "actions": [],
                }
                payload[field] = values
                with self.assertRaises(ValidationError):
                    ToolAnalysisSelection(**payload)

    def test_response_requires_uuid_audit_id(self):
        evidence = ToolEvidence(
            data_quality=DataQualityEvidence(
                job_id="job-1",
                job_state="RUNNING",
                completed_checkpoints=0,
                failed_checkpoints=0,
                latest_completed_at=None,
                counters=COUNTERS,
            )
        )
        response = ToolAnalysisResponse(
            summary="healthy",
            evidence=evidence,
            tool_calls=[{"tool_id": "get_data_quality_health", "status": "success", "duration_ms": 1}],
            planner="rule_based",
            analyzer="rule_based",
            degraded=False,
            audit_id=UUID("00000000-0000-0000-0000-000000000001"),
            generated_at=datetime(2026, 8, 3, tzinfo=timezone.utc),
        )

        self.assertIsInstance(response.audit_id, UUID)
        with self.assertRaises(ValidationError):
            ToolAnalysisResponse.model_validate({**response.model_dump(), "audit_id": "not-a-uuid"})

    def test_tool_settings_have_safe_defaults_ranges_and_environment_parsing(self):
        settings = load_settings({})

        self.assertEqual("http://flink-jobmanager:8081", settings.flink_rest_url)
        self.assertEqual("chapter-9-datastream-quality-production", settings.chapter9_production_job_name)
        self.assertEqual(3, settings.ai_tool_max_calls)
        self.assertEqual(3, settings.ai_tool_executor_max_workers)
        configured = load_settings(
            {
                "FLINK_REST_URL": "http://flink.local:8081",
                "CHAPTER9_PRODUCTION_JOB_NAME": "production-job",
                "AI_TOOL_PLANNER_MODE": "openai_compatible",
                "AI_TOOL_MAX_CALLS": "2",
                "AI_TOOL_EXECUTOR_MAX_WORKERS": "2",
                "AI_TOOL_TOTAL_TIMEOUT_SECONDS": "30.5",
                "AI_TOOL_MAX_EVENT_TYPES": "99",
            }
        )
        self.assertEqual("http://flink.local:8081", configured.flink_rest_url)
        self.assertEqual("production-job", configured.chapter9_production_job_name)
        self.assertEqual("openai_compatible", configured.ai_tool_planner_mode)
        self.assertEqual(2, configured.ai_tool_max_calls)
        self.assertEqual(2, configured.ai_tool_executor_max_workers)
        self.assertEqual(30.5, configured.ai_tool_total_timeout_seconds)
        self.assertEqual(99, configured.ai_tool_max_event_types)
        for field, value, message in (
            ("ai_tool_planner_mode", "unknown", "AI_TOOL_PLANNER_MODE"),
            ("ai_tool_max_calls", 4, "AI_TOOL_MAX_CALLS"),
            ("ai_tool_executor_max_workers", 4, "AI_TOOL_EXECUTOR_MAX_WORKERS"),
            ("ai_tool_total_timeout_seconds", 0, "AI_TOOL_TOTAL_TIMEOUT_SECONDS"),
            ("ai_tool_max_event_types", 101, "AI_TOOL_MAX_EVENT_TYPES"),
            ("flink_rest_url", "", "FLINK_REST_URL"),
            ("chapter9_production_job_name", "", "CHAPTER9_PRODUCTION_JOB_NAME"),
        ):
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, message):
                    ApiSettings(**{field: value})


if __name__ == "__main__":
    unittest.main()
