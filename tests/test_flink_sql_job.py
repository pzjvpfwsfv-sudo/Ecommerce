from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
SOURCE_SQL = ROOT / "jobs" / "sql" / "01_source_user_behavior.sql"
SINK_SQL = ROOT / "jobs" / "sql" / "02_sink_print_metrics.sql"
METRIC_SQL = ROOT / "jobs" / "sql" / "03_pv_uv_metrics.sql"
RUN_SCRIPT = ROOT / "scripts" / "run_flink_sql_job.ps1"
CHAPTER5_RUN_SCRIPT = ROOT / "scripts" / "run_chapter_5_iceberg_pipeline.ps1"
COMPOSE_FILE = ROOT / "infra" / "docker-compose.yml"


class FlinkSqlArtifactsTest(unittest.TestCase):
    def test_source_sql_defines_kafka_source_table(self):
        text = SOURCE_SQL.read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE user_behavior_source", text)
        self.assertIn("'connector' = 'kafka'", text)
        self.assertIn("'topic' = 'user_behavior_events'", text)
        self.assertIn("'format' = 'json'", text)

    def test_sink_sql_defines_print_sink(self):
        text = SINK_SQL.read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE metrics_print_sink", text)
        self.assertIn("'connector' = 'print'", text)

    def test_metric_sql_computes_pv_and_uv(self):
        text = METRIC_SQL.read_text(encoding="utf-8")

        self.assertIn("INSERT INTO metrics_print_sink", text)
        self.assertIn("COUNT(*) AS pv", text)
        self.assertIn("COUNT(DISTINCT user_id) AS uv", text)
        self.assertIn("FROM user_behavior_source", text)

    def test_runner_script_references_all_sql_files(self):
        text = RUN_SCRIPT.read_text(encoding="utf-8")

        self.assertIn("01_source_user_behavior.sql", text)
        self.assertIn("02_sink_print_metrics.sql", text)
        self.assertIn("03_pv_uv_metrics.sql", text)
        self.assertIn("docker exec", text)
        self.assertIn("sql-client.sh", text)

    def test_runner_script_prepares_connector_waits_for_kafka_and_flink_and_combines_sql(self):
        text = RUN_SCRIPT.read_text(encoding="utf-8")

        self.assertIn("docker compose", text)
        self.assertIn("--profile flink", text)
        self.assertIn('throw "Flink 最小运行环境启动失败。"', text)
        self.assertIn('$connectorJar = "infra/compose/flink/lib/flink-sql-connector-kafka-3.3.0-1.19.jar"', text)
        self.assertIn('$connectorUrl = "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar"', text)
        self.assertIn("Invoke-WebRequest -Uri $connectorUrl -OutFile $connectorJar", text)
        self.assertIn('$kafkaContainerName = "ecom-kafka"', text)
        self.assertIn("docker exec $kafkaContainerName kafka-topics --bootstrap-server kafka:29092 --list", text)
        self.assertIn('$flinkOverviewUrl = "http://localhost:8081/overview"', text)
        self.assertIn("for ($attempt = 1; $attempt -le 30; $attempt++)", text)
        self.assertIn("Start-Sleep -Seconds 2", text)
        self.assertIn('$combinedSqlFile = "tmp/chapter_3_flink_job.sql"', text)
        self.assertIn('$utf8NoBom = New-Object System.Text.UTF8Encoding($false)', text)
        self.assertIn("[System.IO.File]::WriteAllText($combinedSqlFile, \"\", $utf8NoBom)", text)
        self.assertIn("[System.IO.File]::AppendAllText($combinedSqlFile, (Get-Content -Raw $sqlFile), $utf8NoBom)", text)
        self.assertIn('[System.IO.File]::AppendAllText($combinedSqlFile, "`r`n`r`n", $utf8NoBom)', text)
        self.assertIn('$containerSqlPath = "/workspace/tmp/chapter_3_flink_job.sql"', text)
        self.assertIn("docker exec $containerName /opt/flink/bin/sql-client.sh -f $containerSqlPath", text)
        self.assertIn('throw "Flink SQL 提交失败。"', text)
        self.assertNotIn("zookeeper", text.lower())

    def test_chapter5_runner_creates_tmp_before_writing_combined_sql(self):
        text = CHAPTER5_RUN_SCRIPT.read_text(encoding="utf-8")

        self.assertIn('New-Item -ItemType Directory -Force -Path "tmp" | Out-Null', text)
        self.assertIn('[System.IO.File]::WriteAllText($combinedSqlFile, "", $utf8NoBom)', text)

    def test_compose_defines_minimal_flink_runtime(self):
        text = COMPOSE_FILE.read_text(encoding="utf-8")

        self.assertIn("flink-jobmanager:", text)
        self.assertIn('profiles: ["flink"]', text)
        self.assertIn("flink-taskmanager:", text)
        self.assertIn("flink-sql-client:", text)
        self.assertIn("FLINK_PROPERTIES", text)
        self.assertIn("rest.address: flink-jobmanager", text)
        self.assertIn("/workspace", text)
        self.assertIn("flink-sql-connector-kafka-3.3.0-1.19.jar", text)


if __name__ == "__main__":
    unittest.main()
