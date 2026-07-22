from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
MODULE = ROOT / "jobs" / "datastream-quality"
POM = MODULE / "pom.xml"
BUILD_SCRIPT = ROOT / "scripts" / "build_chapter_9_datastream.ps1"
RUN_SCRIPT = ROOT / "scripts" / "run_chapter_9_shadow.ps1"
VERIFY_SCRIPT = ROOT / "scripts" / "verify_chapter_9_shadow.ps1"
ENV_FILE = ROOT / "infra" / ".env.example"


class Chapter9ArtifactsTest(unittest.TestCase):
    def test_maven_module_pins_java_flink_kafka_and_fat_jar(self):
        self.assertTrue(POM.exists(), "Chapter 9 Maven module must exist")
        text = POM.read_text(encoding="utf-8")

        self.assertIn("<maven.compiler.release>17</maven.compiler.release>", text)
        self.assertIn("<flink.version>1.19.2</flink.version>", text)
        self.assertIn("<kafka.connector.version>3.3.0-1.19</kafka.connector.version>", text)
        self.assertIn("maven-shade-plugin", text)
        self.assertIn("com.ecommerce.quality.DataQualityJob", text)

    def test_build_and_run_scripts_are_java17_shadow_only_and_non_destructive(self):
        self.assertTrue(BUILD_SCRIPT.exists())
        self.assertTrue(RUN_SCRIPT.exists())
        build = BUILD_SCRIPT.read_text(encoding="utf-8")
        run = RUN_SCRIPT.read_text(encoding="utf-8")

        self.assertIn("maven:3.9.9-eclipse-temurin-17", build)
        self.assertIn("mvn -q clean test package", build)
        self.assertIn("--if-not-exists", run)
        self.assertIn("--mode shadow", run)
        self.assertIn("docker cp", run)
        self.assertIn("flink run -d", run)
        self.assertNotIn("--delete", run)
        self.assertNotIn("force-recreate", run)

    def test_verifier_uses_full_matrix_reconciliation_and_flink_rest_evidence(self):
        self.assertTrue(VERIFY_SCRIPT.exists())
        text = VERIFY_SCRIPT.read_text(encoding="utf-8")

        for marker in (
            "DUPLICATE_EVENT",
            "MALFORMED_JSON",
            "MISSING_REQUIRED_FIELD",
            "INVALID_EVENT_TIME",
            "FUTURE_EVENT_TIME",
            "late_events_total",
            "valid_events_total",
            "/jobs/",
            "/checkpoints",
            "raw = clean + dlq + late",
        ):
            self.assertIn(marker, text)

    def test_env_declares_chapter9_shadow_topics_without_changing_raw_topic(self):
        text = ENV_FILE.read_text(encoding="utf-8")
        self.assertIn("KAFKA_TOPIC_USER_BEHAVIOR=user_behavior_events", text)
        self.assertIn("CHAPTER9_CLEAN_SHADOW_TOPIC=user_behavior_clean_shadow", text)
        self.assertIn("CHAPTER9_DLQ_TOPIC=user_behavior_dlq", text)
        self.assertIn("CHAPTER9_LATE_TOPIC=user_behavior_late", text)


if __name__ == "__main__":
    unittest.main()
