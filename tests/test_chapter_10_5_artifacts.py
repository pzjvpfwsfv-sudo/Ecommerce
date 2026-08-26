import json
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent.parent
LOCK_FILE = ROOT / "infra" / "runtime-dependencies.lock.json"
COMPOSE_FILE = ROOT / "infra" / "docker-compose.yml"
ENV_FILE = ROOT / "infra" / ".env.example"
CATALOG_RECOVERY = ROOT / "scripts" / "restore_chapter_10_5_catalog.ps1"
BOOTSTRAP = ROOT / "scripts" / "bootstrap_chapter_10_5.ps1"
MIGRATE = ROOT / "scripts" / "migrate_chapter_10_5.ps1"
RESET = ROOT / "scripts" / "reset_chapter_10_5_realtime.ps1"


class Chapter105ArtifactsTest(unittest.TestCase):
    def test_controlled_migration_and_reset_have_narrow_non_generic_surfaces(self):
        self.assertTrue(MIGRATE.is_file(), MIGRATE)
        self.assertTrue(RESET.is_file(), RESET)
        if not MIGRATE.is_file() or not RESET.is_file():
            return

        migrate_text = MIGRATE.read_text(encoding="utf-8")
        reset_text = RESET.read_text(encoding="utf-8")
        migrate_parameters = migrate_text.split(")", 1)[0]
        reset_parameters = reset_text.split(")", 1)[0]

        self.assertIn("[switch]$TrafficPaused", migrate_parameters)
        self.assertIn("[switch]$ConfirmRealtimeReset", migrate_parameters)
        self.assertIn("[switch]$ConfirmReset", reset_parameters)
        for parameters in (migrate_parameters, reset_parameters):
            self.assertNotIn("Volume", parameters)
            self.assertNotIn("Prefix", parameters)
        self.assertIn("tmp/chapter-10-5/migration-report.json", migrate_text)
        self.assertIn("tmp/chapter-10-5/realtime-reset-report.json", reset_text)

        combined = f"{migrate_text}\n{reset_text}"
        for forbidden in (
            "volume prune",
            "down -v",
            "docker rm",
            "Remove-Item -Recurse",
            "local/warehouse/",
        ):
            self.assertNotIn(forbidden.lower(), combined.lower())

    def test_controlled_migration_and_reset_functions_only_never_call_native_tools(self):
        self.assertTrue(MIGRATE.is_file(), MIGRATE)
        self.assertTrue(RESET.is_file(), RESET)
        if not MIGRATE.is_file() or not RESET.is_file():
            return

        command = r'''
$ErrorActionPreference = "Stop"
$script:nativeCalls = 0
function docker { $script:nativeCalls++; throw "docker must not run" }
function powershell { $script:nativeCalls++; throw "powershell must not run" }
. (Resolve-Path "scripts/reset_chapter_10_5_realtime.ps1") -FunctionsOnly
. (Resolve-Path "scripts/migrate_chapter_10_5.ps1") -FunctionsOnly
[ordered]@{
    native_calls = $script:nativeCalls
    reset_function = $null -ne (Get-Command Invoke-Chapter105RealtimeReset -ErrorAction SilentlyContinue)
    migrate_function = $null -ne (Get-Command Invoke-Chapter105Migration -ErrorAction SilentlyContinue)
} | ConvertTo-Json -Compress
'''
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", command],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(0, payload["native_calls"])
        self.assertTrue(payload["reset_function"])
        self.assertTrue(payload["migrate_function"])

    def test_bootstrap_surface_has_fixed_stages_and_no_destructive_commands(self):
        text = BOOTSTRAP.read_text(encoding="utf-8")

        for stage in (
            "preflight",
            "dependencies",
            "infrastructure",
            "initialization",
            "catalog",
            "jobs",
            "acceptance",
        ):
            self.assertIn(stage, text)
        self.assertIn("install_runtime_dependencies.ps1", text)
        self.assertIn("restore_chapter_10_5_catalog.ps1", text)
        self.assertIn("verify_chapter_10_tool_analysis.ps1", text)
        self.assertIn("'ps', '--all', '--format', 'json'", text)
        self.assertIn("'exec', '-T', 'kafka-broker'", text)
        self.assertNotIn("'exec', '-T', 'kafka', 'kafka-topics'", text)
        self.assertGreaterEqual(text.count("'-EnvFile', $envPath"), 2)
        self.assertIn("analytics.realtime_metrics", text)
        self.assertIn("lakehouse.analytics.user_behavior_detail", text)
        self.assertIn("/checkpoints", text)
        self.assertIn("latest_ack_timestamp", text)
        self.assertNotIn("down -v", text.lower())
        self.assertNotIn("docker rm", text.lower())
        self.assertNotIn("volume prune", text.lower())
        self.assertNotIn("Remove-Item -Recurse", text)

    def test_runtime_lock_has_unique_https_artifacts_with_real_hashes(self):
        lock = json.loads(LOCK_FILE.read_text(encoding="utf-8"))

        self.assertEqual(1, lock["version"])
        self.assertEqual(10, len(lock["artifacts"]))

        expected = {
            "flink-sql-connector-kafka-3.3.0-1.19.jar": (
                "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar",
                "f46f69333445c598eba9e5068b0a58dd2b4ba797738fd0fd3ee4e862fe281691",
                "infra/compose/flink/lib/flink-sql-connector-kafka-3.3.0-1.19.jar",
            ),
            "flink-doris-connector-1.19-25.1.0.jar": (
                "https://repo1.maven.org/maven2/org/apache/doris/flink-doris-connector-1.19/25.1.0/flink-doris-connector-1.19-25.1.0.jar",
                "ce1c35b6a16b24f67e61ee95b7dab9802b1fb654b9da4fe171c174b2f8b1ca36",
                "infra/compose/flink/lib/flink-doris-connector-1.19-25.1.0.jar",
            ),
            "flink-sql-connector-hive-3.1.3_2.12-1.19.2.jar": (
                "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/1.19.2/flink-sql-connector-hive-3.1.3_2.12-1.19.2.jar",
                "b7c401f01bf69dd72b052f4b0c548829abb3528dfaa1ddff68cd07eb4c552fef",
                "infra/compose/flink/lib/flink-sql-connector-hive-3.1.3_2.12-1.19.2.jar",
            ),
            "iceberg-flink-runtime-1.19-1.6.1.jar": (
                "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/1.6.1/iceberg-flink-runtime-1.19-1.6.1.jar",
                "d0b3fc51623e7091b4d5db96178d8ed79102e51a93f649e3ce82ee4471c080ab",
                "infra/compose/flink/lib/iceberg-flink-runtime-1.19-1.6.1.jar",
            ),
            "iceberg-aws-bundle-1.6.1.jar": (
                "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar",
                "d14a49ced66a20cbd30f73ebb379646248d784fc5cd49d7295d36524380330e3",
                "infra/compose/flink/lib/iceberg-aws-bundle-1.6.1.jar",
            ),
            "hadoop-client-api-3.3.6.jar": (
                "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar",
                "f3d2347a6e1c6885d5bcfd4f60c3ac3810ec11068fc161e04329baabf412d963",
                "infra/compose/flink/lib/hadoop-client-api-3.3.6.jar",
            ),
            "hadoop-client-runtime-3.3.6.jar": (
                "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar",
                "15f01bc804294df06d2effc87de363a83cf589f50558bdbf48f72541ad8de854",
                "infra/compose/flink/lib/hadoop-client-runtime-3.3.6.jar",
            ),
            "hadoop-aws-3.3.6.jar": (
                "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar",
                "fba9eb73e6f0f5458355627fe095f5124705d4048551f4d6aa4084777b824c13",
                "infra/compose/flink/lib/hadoop-aws-3.3.6.jar",
            ),
            "aws-java-sdk-bundle-1.12.262.jar": (
                "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
                "873fe7cf495126619997bec21c44de5d992544aea7e632fdc77adb1a0915bae5",
                "infra/compose/flink/lib/aws-java-sdk-bundle-1.12.262.jar",
            ),
            "postgresql-42.7.4.jar": (
                "https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar",
                "188976721ead8e8627eb6d8389d500dccc0c9bebd885268a3047180274a6031e",
                "infra/compose/hive-metastore/lib/postgresql-42.7.4.jar",
            ),
        }

        destinations = set()
        for artifact in lock["artifacts"]:
            self.assertEqual(
                {"name", "url", "destination", "sha256"}, set(artifact), artifact["name"]
            )
            self.assertTrue(artifact["url"].startswith("https://"))
            self.assertRegex(artifact["sha256"], r"^[0-9a-f]{64}$")
            self.assertNotIn(artifact["destination"], destinations)
            destinations.add(artifact["destination"])
            self.assertEqual(expected[artifact["name"]], (
                artifact["url"], artifact["sha256"], artifact["destination"]
            ))

    def test_installer_surface_is_locked_down(self):
        installer = (ROOT / "scripts" / "install_runtime_dependencies.ps1").read_text(
            encoding="utf-8"
        )
        module = (ROOT / "scripts" / "lib" / "Chapter105.Common.psm1").read_text(
            encoding="utf-8"
        )

        self.assertIn("Install-RuntimeDependencies", installer)
        self.assertIn("Install-RuntimeDependencies", module)
        self.assertIn(".partial.", module)
        self.assertIn("Move-Item", module)
        self.assertIn("infra/compose/flink/lib", module)
        self.assertIn("infra/compose/hive-metastore/lib", module)
        self.assertIn("infra/compose/hive-metastore/lib/", (ROOT / ".gitignore").read_text("utf-8"))

    def test_compose_persists_state_and_configures_postgres_metastore(self):
        compose_text = COMPOSE_FILE.read_text(encoding="utf-8")
        env_text = ENV_FILE.read_text(encoding="utf-8")
        result = subprocess.run(
            [
                "docker",
                "compose",
                "--env-file",
                str(ENV_FILE),
                "-f",
                str(COMPOSE_FILE),
                "--profile",
                "flink",
                "--profile",
                "serving",
                "--profile",
                "lakehouse",
                "config",
                "--format",
                "json",
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        config = json.loads(result.stdout)
        services = config["services"]

        self.assertIn("metastore-postgres", services)
        self.assertEqual("postgres:16.4-alpine", services["metastore-postgres"]["image"])
        self.assertIn("pg_isready", " ".join(services["metastore-postgres"]["healthcheck"]["test"]))
        self.assertEqual("metastore", services["metastore-postgres"]["environment"]["POSTGRES_DB"])
        self.assertEqual("hive", services["metastore-postgres"]["environment"]["POSTGRES_USER"])
        self.assertEqual("hive", services["metastore-postgres"]["environment"]["POSTGRES_PASSWORD"])
        self.assertEqual("postgres", services["hive-metastore"]["environment"]["DB_DRIVER"])
        self.assertIn(
            "jdbc:postgresql://metastore-postgres:5432/metastore",
            services["hive-metastore"]["environment"]["SERVICE_OPTS"],
        )
        self.assertIn(
            "org.postgresql.Driver", services["hive-metastore"]["environment"]["SERVICE_OPTS"]
        )
        self.assertEqual(
            "service_healthy",
            services["hive-metastore"]["depends_on"]["metastore-postgres"]["condition"],
        )
        self.assertEqual(
            "service_healthy", services["hive-metastore"]["depends_on"]["minio"]["condition"]
        )
        self.assertEqual(
            "service_completed_successfully",
            services["hive-metastore"]["depends_on"]["minio-init"]["condition"],
        )
        self.assertTrue(
            any(
                mount["target"] == "/opt/hive/lib/postgresql-42.7.4.jar"
                and mount["read_only"]
                and mount["source"].endswith("postgresql-42.7.4.jar")
                for mount in services["hive-metastore"]["volumes"]
            )
        )

        minio_block = compose_text.split("  minio-init:")[0].split("  minio:")[1]
        self.assertIn("- ${MINIO_DATA_DIR}:/data", minio_block)
        self.assertIn("mc ready local", " ".join(services["minio"]["healthcheck"]["test"]))
        for variable in (
            "MINIO_DATA_DIR=./compose/minio/data",
            "METASTORE_POSTGRES_DB=metastore",
            "METASTORE_POSTGRES_USER=hive",
            "METASTORE_POSTGRES_PASSWORD=hive",
            "API_BIND_HOST=127.0.0.1",
            "CHAPTER9_CHECKPOINT_URI=s3a://flink-state/checkpoints/chapter-9",
            "CHAPTER9_SAVEPOINT_URI=s3a://flink-state/savepoints/chapter-9",
        ):
            self.assertIn(variable, env_text)
        self.assertIn("mc mb --ignore-existing local/warehouse", services["minio-init"]["entrypoint"][-1])
        self.assertIn("mc mb --ignore-existing local/flink-state", services["minio-init"]["entrypoint"][-1])
        self.assertNotIn("tail -f /dev/null", services["minio-init"]["entrypoint"][-1])
        self.assertEqual("127.0.0.1", services["api"]["ports"][0]["host_ip"])

        for volume in (
            "kafka-controller-data",
            "kafka-broker-data",
            "doris-fe-meta",
            "doris-be-storage",
            "metastore-postgres-data",
        ):
            self.assertIn(volume, config["volumes"])

        volume_section = compose_text.split("\nvolumes:\n", 1)[1]
        self.assertNotIn("name:", volume_section)
        self.assertIn("kafka-controller-data:/var/lib/kafka/data", compose_text)
        self.assertIn("kafka-broker-data:/var/lib/kafka/data", compose_text)
        self.assertIn("doris-fe-meta:/opt/apache-doris/fe/doris-meta", compose_text)
        self.assertIn("doris-be-storage:/opt/apache-doris/be/storage", compose_text)
        self.assertIn("metastore-postgres-data:/var/lib/postgresql/data", compose_text)

        self.assertNotIn("DB_DRIVER: derby", compose_text)
        self.assertNotIn("tail -f /dev/null", compose_text.split("  minio-init:")[1].split("  hive-metastore:")[0])

    def test_core_site_consumers_receive_minio_credentials(self):
        result = subprocess.run(
            [
                "docker",
                "compose",
                "--env-file",
                str(ENV_FILE),
                "-f",
                str(COMPOSE_FILE),
                "--profile",
                "flink",
                "--profile",
                "lakehouse",
                "config",
                "--format",
                "json",
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        services = json.loads(result.stdout)["services"]
        expected = {
            "MINIO_ROOT_USER": "minioadmin",
            "MINIO_ROOT_PASSWORD": "minioadmin123",
        }

        for service_name in (
            "flink-jobmanager",
            "flink-taskmanager",
            "flink-sql-client",
            "hive-metastore",
        ):
            with self.subTest(service=service_name):
                environment = services[service_name]["environment"]
                actual = {name: environment.get(name) for name in expected}
                self.assertEqual(expected, actual)

    def test_catalog_recovery_runtime_prerequisites_are_available(self):
        compose_text = COMPOSE_FILE.read_text(encoding="utf-8")
        catalog_text = (ROOT / "infra" / "compose" / "trino" / "catalog" / "lakehouse.properties").read_text(
            encoding="utf-8"
        )

        self.assertTrue(CATALOG_RECOVERY.is_file())
        self.assertIn("iceberg.register-table-procedure.enabled=true", catalog_text)
        self.assertIn("MINIO_ROOT_USER: ${MINIO_ROOT_USER}", compose_text)
        self.assertIn("MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}", compose_text)


if __name__ == "__main__":
    unittest.main()
