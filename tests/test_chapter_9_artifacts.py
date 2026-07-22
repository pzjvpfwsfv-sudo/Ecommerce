from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
MODULE = ROOT / "jobs" / "datastream-quality"
POM = MODULE / "pom.xml"


class Chapter9ArtifactsTest(unittest.TestCase):
    def test_maven_module_pins_java_flink_kafka_and_fat_jar(self):
        self.assertTrue(POM.exists(), "Chapter 9 Maven module must exist")
        text = POM.read_text(encoding="utf-8")

        self.assertIn("<maven.compiler.release>17</maven.compiler.release>", text)
        self.assertIn("<flink.version>1.19.2</flink.version>", text)
        self.assertIn("<kafka.connector.version>3.3.0-1.19</kafka.connector.version>", text)
        self.assertIn("maven-shade-plugin", text)
        self.assertIn("com.ecommerce.quality.DataQualityJob", text)


if __name__ == "__main__":
    unittest.main()
