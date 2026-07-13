from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
READBACK_SQL = ROOT / "jobs" / "sql" / "10_readback_iceberg_user_behavior.sql"
READBACK_SCRIPT = ROOT / "scripts" / "verify_chapter_5_readback.ps1"
README_FILE = ROOT / "README.md"
JOBS_README = ROOT / "jobs" / "README.md"


class Chapter5ReadbackValidationArtifactsTest(unittest.TestCase):
    def test_readback_sql_uses_batch_mode_and_queries_iceberg_table(self):
        text = READBACK_SQL.read_text(encoding="utf-8")

        self.assertIn("SET 'execution.runtime-mode' = 'batch';", text)
        self.assertIn("SET 'sql-client.execution.result-mode' = 'TABLEAU';", text)
        self.assertIn("SELECT COUNT(*) AS event_count FROM lakehouse.analytics.user_behavior_detail;", text)

    def test_readback_script_runs_end_to_end_validation_then_queries_count(self):
        text = READBACK_SCRIPT.read_text(encoding="utf-8")

        self.assertIn("verify_chapter_5_end_to_end.ps1", text)
        self.assertIn("10_readback_iceberg_user_behavior.sql", text)
        self.assertIn("sql-client.sh -f", text)
        self.assertIn("event_count", text)
        self.assertIn("Received a total of", text)

    def test_docs_mention_readback_validation(self):
        readme_text = README_FILE.read_text(encoding="utf-8")
        jobs_text = JOBS_README.read_text(encoding="utf-8")

        self.assertIn("verify_chapter_5_readback.ps1", readme_text)
        self.assertIn("查询回读验证", jobs_text)
        self.assertIn("10_readback_iceberg_user_behavior.sql", jobs_text)
        self.assertIn("verify_chapter_5_readback.ps1", jobs_text)


if __name__ == "__main__":
    unittest.main()
