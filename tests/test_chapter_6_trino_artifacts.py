from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]


class Chapter6TrinoArtifactsTest(unittest.TestCase):
    def test_env_and_compose_define_trino_service(self) -> None:
        env_text = (REPO_ROOT / "infra" / ".env.example").read_text(encoding="utf-8")
        compose_text = (REPO_ROOT / "infra" / "docker-compose.yml").read_text(encoding="utf-8")

        self.assertIn("TRINO_PORT", env_text)
        self.assertIn("TRINO_CONTAINER_NAME", env_text)
        self.assertIn("trino:", compose_text)
        self.assertIn("trinodb/trino", compose_text)

    def test_compose_mounts_trino_catalog_and_port(self) -> None:
        compose_text = (REPO_ROOT / "infra" / "docker-compose.yml").read_text(encoding="utf-8")
        lines = compose_text.splitlines()
        start_index = next(i for i, line in enumerate(lines) if line == "  trino:")
        trino_block_lines = [lines[start_index]]

        for line in lines[start_index + 1 :]:
            if line.startswith("  ") and not line.startswith("    "):
                break
            trino_block_lines.append(line)

        trino_block = "\n".join(trino_block_lines)

        self.assertIn('profiles: ["lakehouse"]', trino_block)
        self.assertIn('- "${TRINO_PORT}:8080"', trino_block)
        self.assertIn("TRINO_CONTAINER_NAME", trino_block)
        self.assertIn("./compose/trino/catalog:/etc/trino/catalog:ro", trino_block)

    def test_trino_catalog_points_to_minio_iceberg(self) -> None:
        catalog_text = (
            REPO_ROOT
            / "infra"
            / "compose"
            / "trino"
            / "catalog"
            / "lakehouse.properties"
        ).read_text(encoding="utf-8")

        self.assertIn("connector.name=iceberg", catalog_text)
        self.assertIn("iceberg.catalog.type=hadoop", catalog_text)
        self.assertIn("iceberg.hadoop.warehouse=s3a://warehouse/iceberg", catalog_text)
        self.assertIn("fs.s3.enabled=true", catalog_text)
        self.assertIn("s3.endpoint=http://minio:9000", catalog_text)
        self.assertIn("s3.region=us-east-1", catalog_text)
        self.assertIn("s3.path-style-access=true", catalog_text)
        self.assertIn("s3.aws-access-key=minioadmin", catalog_text)
        self.assertIn("s3.aws-secret-key=minioadmin123", catalog_text)
        self.assertNotIn("hive.s3.endpoint=http://minio:9000", catalog_text)
        self.assertNotIn("hive.s3.path-style-access=true", catalog_text)
        self.assertNotIn("hive.s3.ssl.enabled=false", catalog_text)

    def test_query_sql_covers_count_and_group_by(self) -> None:
        sql_text = (
            REPO_ROOT / "jobs" / "sql" / "11_trino_read_iceberg_user_behavior.sql"
        ).read_text(encoding="utf-8")

        self.assertIn("SELECT COUNT(*) AS event_count", sql_text)
        self.assertIn("GROUP BY event_type", sql_text)
        self.assertIn("lakehouse.analytics.user_behavior_detail", sql_text)

    def test_verification_script_runs_chapter5_then_queries_trino(self) -> None:
        script_text = (
            REPO_ROOT / "scripts" / "verify_chapter_6_trino_queries.ps1"
        ).read_text(encoding="utf-8")

        self.assertIn("verify_chapter_5_end_to_end.ps1", script_text)
        self.assertIn("/v1/info", script_text)
        self.assertIn("/v1/statement", script_text)
        self.assertIn("event_count", script_text)
        self.assertIn("event_type", script_text)

    def test_verification_script_checks_nonzero_rows(self) -> None:
        script_text = (
            REPO_ROOT / "scripts" / "verify_chapter_6_trino_queries.ps1"
        ).read_text(encoding="utf-8")

        self.assertIn("event_count", script_text)
        self.assertIn("Invoke-RestMethod", script_text)
        self.assertIn("Invoke-CheckedCommand", script_text)
        self.assertIn("Failed to start Trino with docker compose.", script_text)
        self.assertIn("nextUri", script_text)
        self.assertIn("Expected exactly 2 non-empty SQL statements", script_text)
        self.assertIn("throw", script_text)

    def test_docs_mention_chapter6_trino_validation(self) -> None:
        readme_text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
        jobs_text = (REPO_ROOT / "jobs" / "README.md").read_text(encoding="utf-8")

        self.assertIn("第 6 章：Trino + Iceberg 湖表查询", readme_text)
        self.assertIn("Trino + Iceberg", readme_text)
        self.assertIn("./scripts/verify_chapter_6_trino_queries.ps1", readme_text)
        self.assertIn("verify_chapter_6_trino_queries.ps1", readme_text)
        self.assertIn("第 6 章：Trino + Iceberg 湖表查询", jobs_text)
        self.assertIn("Trino + Iceberg", jobs_text)
        self.assertIn("./scripts/verify_chapter_6_trino_queries.ps1", jobs_text)
        self.assertIn("11_trino_read_iceberg_user_behavior.sql", jobs_text)


if __name__ == "__main__":
    unittest.main()
