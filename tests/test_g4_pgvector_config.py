import json
from pathlib import Path
import shutil
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ROOT / "infra/docker-compose.yml"
MIGRATION = ROOT / "infra/compose/app-postgres/migrations/002_knowledge.sql"
PREPARE = ROOT / "scripts/prepare_g4_pgvector.ps1"


class G4PgvectorConfigTest(unittest.TestCase):
    def test_new_volume_does_not_reuse_old_volume(self):
        compose = COMPOSE.read_text(encoding="utf-8")
        app = compose.split("\n  app-postgres:\n", 1)[1].split("\n  hive-metastore:\n", 1)[0]
        metastore = compose.split("\n  metastore-postgres:\n", 1)[1].split("\n  app-postgres:\n", 1)[0]
        self.assertIn("image: pgvector/pgvector:0.8.7-pg16-bookworm", app)
        self.assertIn("app-postgres-g4-data:/var/lib/postgresql/data", app)
        self.assertNotIn("app-postgres-data:/var/lib/postgresql/data", app)
        self.assertIn("metastore-postgres-data:/var/lib/postgresql/data", metastore)
        self.assertIn("  app-postgres-data:", compose)
        self.assertIn("  app-postgres-g4-data:", compose)
        self.assertIn("to_regclass('knowledge.documents')", app)

    def test_schema_has_vector_and_single_published_version(self):
        sql = MIGRATION.read_text(encoding="utf-8")
        self.assertIn("CREATE EXTENSION IF NOT EXISTS vector", sql)
        self.assertIn("CREATE SCHEMA IF NOT EXISTS knowledge", sql)
        for table in ("documents", "versions", "chunks"):
            self.assertIn(f"CREATE TABLE IF NOT EXISTS knowledge.{table}", sql)
        self.assertIn("embedding vector(512)", sql)
        self.assertIn("WHERE status = 'published'", sql)
        self.assertIn("UNIQUE INDEX IF NOT EXISTS", sql)
        self.assertIn("visibility_roles", sql)
        self.assertIn("'admin','analyst','viewer'", sql)
        self.assertIn("original_bytes BYTEA", sql)
        self.assertIn("extracted_text TEXT", sql)

    def test_waits_for_final_postgres_server_before_migration(self):
        script = PREPARE.read_text(encoding="utf-8")
        self.assertIn("pg_isready -h 127.0.0.1", script)

    @unittest.skipUnless(shutil.which("pwsh"), "PowerShell 7 is unavailable")
    def test_preflight_refuses_unverified_old_data(self):
        compose = json.dumps({
            "name": "g4-test",
            "services": {"app-postgres": {"environment": {
                "POSTGRES_DB": "ecommerce_app", "POSTGRES_USER": "app",
                "POSTGRES_PASSWORD": "",
            }}},
        })
        fake = f"""
function docker {{
    $global:LASTEXITCODE = 0
    if ($args[0] -eq 'compose') {{
        if (($args -join ' ') -notmatch '--profile serving') {{ return '{{"name":"g4-test","services":{{}}}}' }}
        return '{compose}'
    }}
    if ($args[0] -eq 'volume' -and $args[1] -eq 'ls') {{ return 'g4-test_app-postgres-data' }}
    if ($args[0] -eq 'ps') {{ return '' }}
    if ($args[0] -eq 'run') {{ return 'DATA' }}
    throw "MUTATION_CALLED: $args"
}}
& '{PREPARE.as_posix()}' -PreflightOnly
"""
        result = subprocess.run(
            ["pwsh", "-NoProfile", "-NonInteractive", "-Command", fake],
            cwd=ROOT, capture_output=True, text=True, encoding="utf-8",
            errors="replace", timeout=30,
        )
        output = result.stdout + result.stderr
        self.assertNotEqual(result.returncode, 0, output)
        self.assertIn("old application data", output.lower())
        self.assertNotIn("MUTATION_CALLED", output)


if __name__ == "__main__":
    unittest.main()
