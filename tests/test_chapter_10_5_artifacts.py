import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
LOCK_FILE = ROOT / "infra" / "runtime-dependencies.lock.json"


class Chapter105ArtifactsTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
