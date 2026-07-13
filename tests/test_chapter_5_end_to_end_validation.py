from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
CHECKPOINT_SQL = ROOT / "jobs" / "sql" / "00_enable_iceberg_checkpointing.sql"
VALIDATION_SCRIPT = ROOT / "scripts" / "verify_chapter_5_end_to_end.ps1"
README_FILE = ROOT / "README.md"
JOBS_README = ROOT / "jobs" / "README.md"


class Chapter5EndToEndValidationArtifactsTest(unittest.TestCase):
    def test_checkpoint_sql_enables_streaming_commits(self):
        text = CHECKPOINT_SQL.read_text(encoding="utf-8")

        self.assertIn("SET 'execution.checkpointing.interval' = '10 s';", text)
        self.assertIn("SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';", text)

    def test_validation_script_replays_events_and_waits_for_iceberg_data_files(self):
        text = VALIDATION_SCRIPT.read_text(encoding="utf-8")

        self.assertIn("function Wait-ForIcebergDataCommit", text)
        self.assertIn("function Get-IcebergObjectNames", text)
        self.assertIn("function New-ValidationEventsFile", text)
        self.assertIn("run_chapter_5_iceberg_pipeline.ps1", text)
        self.assertIn("kafka-console-producer", text)
        self.assertIn("user_behavior_events", text)
        self.assertIn(".parquet", text)
        self.assertIn("BaselineMetadataNames", text)
        self.assertIn("BaselineDataNames", text)
        self.assertIn("mc ls --recursive local/warehouse/iceberg/analytics.db/user_behavior_detail", text)
        self.assertIn("Timed out waiting for new Iceberg metadata and data files in MinIO.", text)

    def test_docs_mention_end_to_end_validation(self):
        readme_text = README_FILE.read_text(encoding="utf-8")
        jobs_text = JOBS_README.read_text(encoding="utf-8")

        self.assertIn("verify_chapter_5_end_to_end.ps1", readme_text)
        self.assertIn("端到端收尾验证", jobs_text)
        self.assertIn("00_enable_iceberg_checkpointing.sql", jobs_text)
        self.assertIn("verify_chapter_5_end_to_end.ps1", jobs_text)


if __name__ == "__main__":
    unittest.main()
