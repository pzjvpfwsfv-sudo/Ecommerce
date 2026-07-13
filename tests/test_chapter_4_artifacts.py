from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = ROOT / "infra" / ".env.example"
COMPOSE_FILE = ROOT / "infra" / "docker-compose.yml"
DORIS_INIT_SQL = ROOT / "infra" / "compose" / "doris" / "init" / "01_create_realtime_metrics.sql"
DORIS_RUNNER = ROOT / "scripts" / "init_doris_realtime_metrics.ps1"
DORIS_SINK_SQL = ROOT / "jobs" / "sql" / "04_sink_doris_metrics.sql"
DORIS_INSERT_SQL = ROOT / "jobs" / "sql" / "05_pv_uv_to_doris.sql"
CHAPTER4_RUNNER = ROOT / "scripts" / "run_chapter_4_pipeline.ps1"
README_FILE = ROOT / "README.md"
JOBS_README = ROOT / "jobs" / "README.md"


class Chapter4ArtifactsTest(unittest.TestCase):
    def test_env_file_includes_doris_and_api_variables(self):
        text = ENV_FILE.read_text(encoding="utf-8")

        self.assertIn("DORIS_FE_CONTAINER_NAME=", text)
        self.assertIn("DORIS_BE_CONTAINER_NAME=", text)
        self.assertIn("DORIS_VERSION=", text)
        self.assertIn("DORIS_FE_HTTP_PORT=", text)
        self.assertIn("DORIS_FE_QUERY_PORT=", text)
        self.assertIn("DORIS_BE_HEARTBEAT_PORT=", text)
        self.assertIn("API_PORT=", text)

    def test_compose_defines_minimal_doris_runtime_and_python_api(self):
        text = COMPOSE_FILE.read_text(encoding="utf-8")

        self.assertIn("custom_network:", text)
        self.assertIn("ip_range: 172.21.80.128/25", text)
        self.assertIn("ipv4_address: 172.21.80.2", text)
        self.assertIn("ipv4_address: 172.21.80.3", text)
        self.assertIn("doris-fe:", text)
        self.assertIn("doris-be:", text)
        self.assertIn('profiles: ["serving"]', text)
        self.assertIn("apache/doris:fe-${DORIS_VERSION}", text)
        self.assertIn("apache/doris:be-${DORIS_VERSION}", text)
        self.assertIn("SKIP_CHECK_ULIMIT", text)
        self.assertIn("services/api:/app", text)
        self.assertIn("uvicorn app.main:app", text)
        self.assertIn("flink-doris-connector-1.19-25.1.0.jar", text)

    def test_doris_init_sql_creates_realtime_metrics_table(self):
        text = DORIS_INIT_SQL.read_text(encoding="utf-8")

        self.assertIn("CREATE DATABASE IF NOT EXISTS analytics", text)
        self.assertIn("CREATE TABLE IF NOT EXISTS realtime_metrics", text)
        self.assertIn("metric_name VARCHAR(32)", text)
        self.assertIn("metric_value BIGINT", text)
        self.assertIn("updated_at DATETIME", text)

    def test_init_doris_script_executes_sql_inside_fe(self):
        text = DORIS_RUNNER.read_text(encoding="utf-8")

        self.assertIn("function Invoke-CheckedCommand", text)
        self.assertIn("function Assert-DockerAvailable", text)
        self.assertIn("docker version", text)
        self.assertIn('$feContainerName = "ecom-doris-fe"', text)
        self.assertIn("docker compose", text)
        self.assertIn("--profile serving up -d --quiet-pull doris-fe doris-be", text)
        self.assertIn("docker exec $feContainerName sh -lc", text)
        self.assertIn("mysql -uroot -h127.0.0.1 -P9030", text)
        self.assertIn("SHOW BACKENDS", text)
        self.assertIn('$backendStatus -join "`n"', text)
        self.assertIn('Doris BE did not become alive in time.', text)
        self.assertIn("01_create_realtime_metrics.sql", text)

    def test_doris_sink_sql_targets_realtime_metrics(self):
        text = DORIS_SINK_SQL.read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE doris_metrics_sink", text)
        self.assertIn("'connector' = 'doris'", text)
        self.assertIn("'fenodes' = 'doris-fe:8030'", text)
        self.assertIn("'table.identifier' = 'analytics.realtime_metrics'", text)
        self.assertIn("'sink.enable-2pc' = 'false'", text)

    def test_doris_insert_sql_writes_pv_and_uv(self):
        text = DORIS_INSERT_SQL.read_text(encoding="utf-8")

        self.assertIn("INSERT INTO doris_metrics_sink", text)
        self.assertIn("COUNT(*) AS metric_value", text)
        self.assertIn("COUNT(DISTINCT user_id) AS metric_value", text)
        self.assertIn("UNION ALL", text)
        self.assertIn("CURRENT_TIMESTAMP", text)

    def test_chapter_4_runner_prepares_doris_connector_and_sql(self):
        text = CHAPTER4_RUNNER.read_text(encoding="utf-8")

        self.assertIn("function Invoke-CheckedCommand", text)
        self.assertIn("function Assert-DockerAvailable", text)
        self.assertIn("docker version", text)
        self.assertIn('$connectorJar = "infra/compose/flink/lib/flink-doris-connector-1.19-25.1.0.jar"', text)
        self.assertIn('$connectorUrl = "https://repo1.maven.org/maven2/org/apache/doris/flink-doris-connector-1.19/25.1.0/flink-doris-connector-1.19-25.1.0.jar"', text)
        self.assertIn("./scripts/init_doris_realtime_metrics.ps1", text)
        self.assertIn("04_sink_doris_metrics.sql", text)
        self.assertIn("05_pv_uv_to_doris.sql", text)
        self.assertIn('$combinedSqlFile = "tmp/chapter_4_flink_job.sql"', text)
        self.assertIn("--profile flink --profile serving up -d --force-recreate --quiet-pull", text)
        self.assertIn("kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists", text)
        self.assertIn("--topic user_behavior_events --partitions 1 --replication-factor 1", text)
        self.assertIn('New-Item -ItemType Directory -Force -Path "tmp" | Out-Null', text)
        self.assertIn("docker exec $containerName /opt/flink/bin/sql-client.sh -f $containerSqlPath", text)

        stack_start = text.index("--profile flink --profile serving up -d --force-recreate --quiet-pull")
        doris_init = text.index("./scripts/init_doris_realtime_metrics.ps1")
        topic_create = text.index("kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists")
        sql_write = text.index("[System.IO.File]::WriteAllText")
        self.assertLess(stack_start, doris_init)
        self.assertLess(doris_init, topic_create)
        self.assertLess(topic_create, sql_write)
        self.assertLess(doris_init, sql_write)

    def test_docs_mention_chapter_4_commands(self):
        readme_text = README_FILE.read_text(encoding="utf-8")
        jobs_text = JOBS_README.read_text(encoding="utf-8")

        self.assertIn("run_chapter_4_pipeline.ps1", readme_text)
        self.assertIn("GET /metrics/realtime", readme_text)
        self.assertIn("04_sink_doris_metrics.sql", jobs_text)
        self.assertIn("05_pv_uv_to_doris.sql", jobs_text)


if __name__ == "__main__":
    unittest.main()
