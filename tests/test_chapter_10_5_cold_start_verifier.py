from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = ROOT / "infra" / "docker-compose.yml"
ENV_FILE = ROOT / "infra" / ".env.example"
BOOTSTRAP = ROOT / "scripts" / "bootstrap_chapter_10_5.ps1"
VERIFIER = ROOT / "scripts" / "verify_chapter_10_5_cold_start.ps1"


class Chapter105ColdStartVerifierTest(unittest.TestCase):
    def test_verifier_requires_a_unique_isolated_project_and_reports_all_acceptance_gates(self):
        self.assertTrue(VERIFIER.is_file(), VERIFIER)
        text = VERIFIER.read_text(encoding="utf-8")

        self.assertIn("KeepOnFailure", text)
        self.assertIn("tmp/chapter-10-5/acceptance", text)
        self.assertIn("'compose'", text)
        self.assertIn("--project-name", text)
        self.assertIn("PROJECT_NAME", text)
        self.assertIn("Compose project identity is unsafe", text)
        for report_key in (
            "cold_start",
            "idempotent_second_run",
            "restart_recovery",
            "data_continuity",
            "readiness",
            "tool_analysis",
        ):
            self.assertIn(report_key, text)

    def test_verifier_cleanup_is_owned_and_confined_to_its_acceptance_directory(self):
        self.assertTrue(VERIFIER.is_file(), VERIFIER)
        text = VERIFIER.read_text(encoding="utf-8")

        self.assertIn("com.docker.compose.project", text)
        self.assertIn("GetFullPath", text)
        self.assertIn("StartsWith", text)
        self.assertIn("Remove-Item", text)
        for forbidden in ("docker system prune", "docker volume prune", "down -v"):
            self.assertNotIn(forbidden, text.lower())

    def test_verifier_snapshots_the_default_project_before_isolated_env_overrides(self):
        text = VERIFIER.read_text(encoding="utf-8")
        freeze = "$defaultProjectName = [string]$defaults['PROJECT_NAME']"
        overrides = "foreach ($key in $overrides.Keys)"
        snapshot = "-ProjectName $defaultProjectName"
        self.assertIn(freeze, text)
        self.assertIn(overrides, text)
        self.assertIn(snapshot, text)
        self.assertLess(text.index(freeze), text.index(overrides))

    def test_compose_exposes_defaults_for_every_isolation_sensitive_value(self):
        compose = COMPOSE_FILE.read_text(encoding="utf-8")
        env = ENV_FILE.read_text(encoding="utf-8")

        expected = {
            "MINIO_CONTAINER_NAME": "ecom-minio",
            "MINIO_INIT_CONTAINER_NAME": "ecom-minio-init",
            "METASTORE_POSTGRES_CONTAINER_NAME": "ecom-metastore-postgres",
            "DORIS_NETWORK_SUBNET": "172.21.80.0/24",
            "DORIS_NETWORK_IP_RANGE": "172.21.80.128/25",
            "DORIS_FE_STATIC_IP": "172.21.80.2",
            "DORIS_BE_STATIC_IP": "172.21.80.3",
            "DORIS_FE_EDIT_LOG_PORT": "9010",
            "DORIS_BE_HTTP_PORT": "8040",
        }
        for name, value in expected.items():
            with self.subTest(name=name):
                self.assertIn(f"{name}={value}", env)
                self.assertIn("${" + name + "}", compose)

    def test_bootstrap_accepts_an_isolated_acceptance_env_file_without_default_names(self):
        text = BOOTSTRAP.read_text(encoding="utf-8")
        self.assertIn("tmp/chapter-10-5/acceptance", text)
        self.assertIn("[System.IO.Path]::IsPathRooted($EnvFile)", text)
        self.assertIn("[switch]$IsolatedAcceptance", text)
        self.assertNotIn("ecom-minio", text)
        self.assertNotIn("ecom-kafka", text)


if __name__ == "__main__":
    unittest.main()
