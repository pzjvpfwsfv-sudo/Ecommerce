from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent


class Chapter10ArtifactsTest(unittest.TestCase):
    def test_readme_closes_chapter9_and_renumbers_ai_evolution(self):
        text = (ROOT / "README.md").read_text(encoding="utf-8")
        for expected in (
            "第 9 章：Java DataStream 数据质量治理",
            "受控切流",
            "安全回滚",
            "第 10 章：受控工具调用与审计",
            "第 11 章：受控 NL2SQL",
            "第 10 章仍不生成或执行 SQL",
        ):
            self.assertIn(expected, text)

    def test_compose_passes_only_fixed_tool_runtime_configuration(self):
        env_example = (ROOT / "infra" / ".env.example").read_text(encoding="utf-8")
        compose = (ROOT / "infra" / "docker-compose.yml").read_text(encoding="utf-8")

        expected = (
            "FLINK_REST_URL=http://flink-jobmanager:8081",
            "CHAPTER9_PRODUCTION_JOB_NAME=chapter-9-datastream-quality-production",
            "AI_TOOL_PLANNER_MODE=rule_based",
            "AI_TOOL_MAX_CALLS=3",
            "AI_TOOL_TOTAL_TIMEOUT_SECONDS=20",
            "AI_TOOL_MAX_EVENT_TYPES=20",
        )
        for value in expected:
            self.assertIn(value, env_example)
        for name in (value.split("=", 1)[0] for value in expected):
            self.assertIn(f"      {name}: ${{{name}}}", compose)


if __name__ == "__main__":
    unittest.main()
