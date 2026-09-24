import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "api"))

from app.config import load_settings  # noqa: E402


class G3AuthConfigTest(unittest.TestCase):
    def test_app_database_is_not_metastore_database(self):
        values = load_settings({
            "APP_DB_HOST": "app-postgres",
            "APP_DB_PORT": "5432",
            "APP_DB_NAME": "ecommerce_app",
            "APP_DB_USER": "app",
            "APP_DB_PASSWORD": "unit-secret",
            "METASTORE_POSTGRES_DB": "metastore",
            "METASTORE_POSTGRES_USER": "hive",
            "METASTORE_POSTGRES_PASSWORD": "hive-secret",
        })

        self.assertEqual(values.app_db_host, "app-postgres")
        self.assertEqual(values.app_db_port, 5432)
        self.assertEqual(values.app_db_name, "ecommerce_app")
        self.assertEqual(values.app_db_user, "app")
        self.assertEqual(values.app_db_password, "unit-secret")

    def test_auth_schema_has_required_keys_and_roles(self):
        schema = (ROOT / "infra/compose/app-postgres/init/001_auth.sql").read_text("utf-8")

        for fragment in (
            "CREATE TABLE IF NOT EXISTS app_users",
            "id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY",
            "username TEXT NOT NULL UNIQUE",
            "password_hash TEXT NOT NULL",
            "role TEXT NOT NULL CHECK (role IN ('admin','analyst','viewer'))",
            "active BOOLEAN NOT NULL DEFAULT TRUE",
            "created_at TIMESTAMPTZ NOT NULL DEFAULT now()",
            "CREATE TABLE IF NOT EXISTS app_sessions",
            "token_hash CHAR(64) PRIMARY KEY",
            "user_id BIGINT NOT NULL REFERENCES app_users(id)",
            "csrf_token TEXT NOT NULL",
            "expires_at TIMESTAMPTZ NOT NULL",
            "revoked_at TIMESTAMPTZ",
            "CREATE TABLE IF NOT EXISTS auth_audit",
            "user_id BIGINT REFERENCES app_users(id)",
            "action TEXT NOT NULL",
            "occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()",
        ):
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, schema)

    @unittest.skipUnless(shutil.which("docker"), "Docker Compose is unavailable")
    def test_lakehouse_profile_renders_without_new_app_secret(self):
        example = (ROOT / "infra/.env.example").read_text("utf-8")
        legacy_env = "\n".join(
            line for line in example.splitlines() if not line.startswith("APP_DB_PASSWORD=")
        ) + "\n"
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / "legacy.env"
            env_file.write_text(legacy_env, encoding="utf-8")
            process_env = os.environ.copy()
            process_env.pop("APP_DB_PASSWORD", None)
            result = subprocess.run(
                [
                    "docker", "compose", "--env-file", str(env_file),
                    "-f", str(ROOT / "infra/docker-compose.yml"),
                    "--profile", "lakehouse", "config", "--quiet",
                ],
                cwd=ROOT,
                env=process_env,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=30,
            )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_example_does_not_supply_an_app_database_password(self):
        example = (ROOT / "infra/.env.example").read_text("utf-8")
        self.assertIn("APP_DB_PASSWORD=\n", example)


if __name__ == "__main__":
    unittest.main()
