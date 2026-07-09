from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = REPO_ROOT / "infra" / ".env.example"
COMPOSE_FILE = REPO_ROOT / "infra" / "docker-compose.yml"
KAFKA_README = REPO_ROOT / "infra" / "compose" / "kafka" / "README.md"
TOP_LEVEL_README = REPO_ROOT / "README.md"


class Chapter7KRaftArtifactsTest(unittest.TestCase):
    def test_env_replaces_zookeeper_with_kraft_settings(self) -> None:
        text = ENV_FILE.read_text(encoding="utf-8")

        self.assertNotIn("ZOOKEEPER_CONTAINER_NAME", text)
        self.assertNotIn("ZOOKEEPER_PORT", text)
        self.assertIn("KAFKA_CONTROLLER_CONTAINER_NAME=", text)
        self.assertIn("KAFKA_CONTROLLER_PORT=", text)
        self.assertIn("KAFKA_BROKER_ID=", text)
        self.assertIn("KAFKA_CONTROLLER_NODE_ID=", text)
        self.assertIn("KAFKA_CLUSTER_ID=", text)

    def test_compose_defines_controller_and_broker_without_zookeeper(self) -> None:
        text = COMPOSE_FILE.read_text(encoding="utf-8")

        self.assertIn("kafka-controller:", text)
        self.assertIn("kafka-broker:", text)
        self.assertNotIn("zookeeper:", text)
        self.assertIn("KAFKA_PROCESS_ROLES: controller", text)
        self.assertIn("KAFKA_PROCESS_ROLES: broker", text)
        self.assertIn("container_name: ${KAFKA_CONTAINER_NAME}", text)
        self.assertIn("hostname: kafka", text)

    def test_kafka_docs_describe_kraft_evolution(self) -> None:
        kafka_text = KAFKA_README.read_text(encoding="utf-8")
        readme_text = TOP_LEVEL_README.read_text(encoding="utf-8")

        self.assertIn("KRaft", kafka_text)
        self.assertNotIn("ZooKeeper + Kafka", kafka_text)
        self.assertIn("controller + broker", kafka_text)
        self.assertIn("KRaft", readme_text)


if __name__ == "__main__":
    unittest.main()
