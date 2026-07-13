from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
LOCAL_CATALOG_SQL = ROOT / "jobs" / "sql" / "08_create_iceberg_catalog_local.sql"
LOCAL_ICEBERG_SINK_SQL = ROOT / "jobs" / "sql" / "09_sink_user_behavior_to_iceberg_local.sql"
LOCAL_RUNNER = ROOT / "scripts" / "run_chapter_5_local_iceberg_validation.ps1"
README_FILE = ROOT / "README.md"
JOBS_README = ROOT / "jobs" / "README.md"


class Chapter5LocalValidationArtifactsTest(unittest.TestCase):
    def test_local_catalog_sql_defines_filesystem_warehouse(self):
        catalog_text = LOCAL_CATALOG_SQL.read_text(encoding="utf-8")

        self.assertIn("CREATE CATALOG lakehouse_local", catalog_text)
        self.assertIn("'type' = 'iceberg'", catalog_text)
        self.assertIn("'catalog-type' = 'hadoop'", catalog_text)
        self.assertIn("'warehouse' = 'file:///workspace/tmp/iceberg-warehouse'", catalog_text)
        self.assertIn("CREATE DATABASE IF NOT EXISTS lakehouse_local.analytics", catalog_text)
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse_local.analytics.user_behavior_detail", catalog_text)

    def test_local_sink_sql_targets_local_catalog_table(self):
        sink_text = LOCAL_ICEBERG_SINK_SQL.read_text(encoding="utf-8")

        self.assertIn("INSERT INTO lakehouse_local.analytics.user_behavior_detail", sink_text)
        self.assertIn("FROM user_behavior_source", sink_text)

    def test_local_runner_replays_flink_sql_against_filesystem_warehouse(self):
        text = LOCAL_RUNNER.read_text(encoding="utf-8")

        self.assertIn("function Invoke-CheckedCommand", text)
        self.assertIn("function Assert-DockerAvailable", text)
        self.assertIn("docker version", text)
        self.assertIn("08_create_iceberg_catalog_local.sql", text)
        self.assertIn("09_sink_user_behavior_to_iceberg_local.sql", text)
        self.assertIn("--profile flink up -d --force-recreate --quiet-pull", text)
        self.assertIn("kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists", text)
        self.assertIn("--topic user_behavior_events --partitions 1 --replication-factor 1", text)
        self.assertIn('$flinkOverviewUrl = "http://localhost:8081/overview"', text)
        self.assertIn("Invoke-WebRequest -Uri $flinkOverviewUrl", text)
        self.assertIn('$combinedSqlFile = "tmp/chapter_5_local_validation.sql"', text)
        self.assertIn('docker exec $containerName /opt/flink/bin/sql-client.sh -f $containerSqlPath', text)

        stack_start = text.index("--profile flink up -d --force-recreate --quiet-pull")
        topic_create = text.index("kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists")
        flink_probe = text.index("Invoke-WebRequest -Uri $flinkOverviewUrl")
        sql_submit = text.index("docker exec $containerName /opt/flink/bin/sql-client.sh -f $containerSqlPath")
        self.assertLess(stack_start, topic_create)
        self.assertLess(topic_create, flink_probe)
        self.assertLess(flink_probe, sql_submit)

    def test_docs_mention_local_filesystem_validation(self):
        readme_text = README_FILE.read_text(encoding="utf-8")
        jobs_text = JOBS_README.read_text(encoding="utf-8")

        self.assertIn("run_chapter_5_local_iceberg_validation.ps1", readme_text)
        self.assertIn("filesystem warehouse", readme_text)
        self.assertIn("08_create_iceberg_catalog_local.sql", jobs_text)
        self.assertIn("09_sink_user_behavior_to_iceberg_local.sql", jobs_text)
        self.assertIn("run_chapter_5_local_iceberg_validation.ps1", jobs_text)


if __name__ == "__main__":
    unittest.main()
