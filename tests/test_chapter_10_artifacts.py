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
        for value in expected:
            name, default = value.split("=", 1)
            self.assertIn(f"      {name}: ${{{name}:-{default}}}", compose)

    def test_application_and_compose_defaults_match_the_documented_safe_values(self):
        config = (ROOT / "services/api/app/config.py").read_text(encoding="utf-8")
        expected = {
            "FLINK_REST_URL": "http://flink-jobmanager:8081",
            "CHAPTER9_PRODUCTION_JOB_NAME": "chapter-9-datastream-quality-production",
            "AI_TOOL_PLANNER_MODE": "rule_based",
            "AI_TOOL_MAX_CALLS": "3",
            "AI_TOOL_TOTAL_TIMEOUT_SECONDS": "20",
            "AI_TOOL_MAX_EVENT_TYPES": "20",
        }
        for name, default in expected.items():
            self.assertIn(f'"{name}", "{default}"', config)

    def test_verifier_covers_three_tools_composite_and_prompt_injection(self):
        text = (ROOT / "scripts/verify_chapter_10_tool_analysis.ps1").read_text(
            encoding="utf-8"
        )
        for expected in (
            "/analysis/tools",
            "get_realtime_metrics",
            "get_historical_behavior_summary",
            "get_data_quality_health",
            "audit_id",
            "ignore whitelist",
            "analysis tools are temporarily unavailable",
        ):
            self.assertIn(expected, text)
        for forbidden in ("cancel", "stop-with-savepoint", "run_sql"):
            self.assertNotIn(forbidden, text.lower())
        self.assertIn(
            '"(?i)\\b(SELECT|INSERT|UPDATE|DELETE)\\b|https?://"',
            text,
        )

    def test_runbook_documents_safe_operation_and_strict_verification(self):
        text = (
            ROOT / "docs/chapter-10-controlled-tool-calling-runbook.md"
        ).read_text(encoding="utf-8")
        for expected in (
            "安全边界",
            "实时指标",
            "历史行为",
            "数据质量",
            "综合分析",
            "降级",
            "audit_id",
            "verify_chapter_10_tool_analysis.ps1",
            "不是 NL2SQL",
            "整体作废",
            "规则 planner",
            "fallback 计划也失败",
        ):
            self.assertIn(expected, text)


if __name__ == "__main__":
    unittest.main()
