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
        self.assertIn("hive.s3.endpoint=http://minio:9000", catalog_text)

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

    def test_docs_mention_chapter6_trino_validation(self) -> None:
        readme_text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
        jobs_text = (REPO_ROOT / "jobs" / "README.md").read_text(encoding="utf-8")

        self.assertIn("绗?6 绔?", readme_text)
        self.assertIn("Trino", readme_text)
        self.assertIn("verify_chapter_6_trino_queries.ps1", readme_text)
        self.assertIn("绗?6 绔?", jobs_text)
        self.assertIn("Trino", jobs_text)
        self.assertIn("11_trino_read_iceberg_user_behavior.sql", jobs_text)


if __name__ == "__main__":
    unittest.main()
