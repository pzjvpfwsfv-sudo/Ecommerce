from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent


class Chapter9PhaseBArtifactsTest(unittest.TestCase):
    def test_env_enables_four_slots_and_production_namespace(self):
        text = (ROOT / "infra/.env.example").read_text(encoding="utf-8")
        for marker in (
            "FLINK_TASKMANAGER_SLOTS=4",
            "CHAPTER9_CLEAN_TOPIC=user_behavior_clean",
            "CHAPTER9_PRODUCTION_CONSUMER_GROUP=chapter9-quality-production",
            "CHAPTER9_PRODUCTION_TRANSACTION_PREFIX=chapter9-production",
        ):
            self.assertIn(marker, text)

    def test_clean_sources_use_distinct_consumer_groups(self):
        doris = (ROOT / "jobs/sql/13_source_user_behavior_clean_doris.sql").read_text(encoding="utf-8")
        iceberg = (ROOT / "jobs/sql/14_source_user_behavior_clean_iceberg.sql").read_text(encoding="utf-8")
        self.assertIn("'topic' = 'user_behavior_clean'", doris)
        self.assertIn("'topic' = 'user_behavior_clean'", iceberg)
        self.assertIn("'properties.group.id' = 'chapter9-doris-clean-v1'", doris)
        self.assertIn("'properties.group.id' = 'chapter9-iceberg-clean-v1'", iceberg)
        self.assertNotEqual(doris, iceberg)

    def test_rollback_source_requires_recorded_offsets(self):
        text = (ROOT / "jobs/sql/15_source_user_behavior_raw_rollback.sql.template").read_text(encoding="utf-8")
        self.assertIn("'topic' = 'user_behavior_events'", text)
        self.assertIn("'scan.startup.mode' = 'specific-offsets'", text)
        self.assertIn("__ROLLBACK_GROUP_ID__", text)
        self.assertIn("__SPECIFIC_OFFSETS__", text)

    def test_resize_script_recreates_only_taskmanager_and_checks_recovery(self):
        text = (ROOT / "scripts/resize_chapter_9_flink_slots.ps1").read_text(encoding="utf-8")
        for marker in (
            "Get-WorkspaceMountSource",
            "Assert-FlinkCapacity",
            "--no-deps",
            "--force-recreate",
            "flink-taskmanager",
            '"slots-total" -ne 4',
            "/checkpoints",
        ):
            self.assertIn(marker, text)
        self.assertNotIn("docker compose down", text)
