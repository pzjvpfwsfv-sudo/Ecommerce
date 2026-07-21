from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent


class Chapter8ArtifactsTest(unittest.TestCase):
    def test_env_and_compose_define_safe_ai_defaults(self):
        env_text = (ROOT / "infra/.env.example").read_text(encoding="utf-8")
        compose_text = (ROOT / "infra/docker-compose.yml").read_text(encoding="utf-8")

        for setting in (
            "TRINO_BASE_URL=http://trino:8080",
            "TRINO_USER=ecommerce-ai",
            "TRINO_CATALOG=lakehouse",
            "TRINO_SCHEMA=analytics",
            "TRINO_REQUEST_TIMEOUT_SECONDS=10",
            "AI_ANALYZER_MODE=rule_based",
            "AI_API_KEY=",
            "AI_BASE_URL=",
            "AI_MODEL=",
            "AI_REQUEST_TIMEOUT_SECONDS=15",
            "AI_MAX_QUESTION_LENGTH=500",
        ):
            self.assertIn(setting, env_text)

        for variable in (
            "TRINO_BASE_URL",
            "TRINO_USER",
            "TRINO_CATALOG",
            "TRINO_SCHEMA",
            "TRINO_REQUEST_TIMEOUT_SECONDS",
            "AI_ANALYZER_MODE",
            "AI_API_KEY",
            "AI_BASE_URL",
            "AI_MODEL",
            "AI_REQUEST_TIMEOUT_SECONDS",
            "AI_MAX_QUESTION_LENGTH",
        ):
            self.assertIn(f"{variable}: ${{{variable}}}", compose_text)

    def test_verification_script_calls_real_analysis_endpoint_in_rule_mode(self):
        script_path = ROOT / "scripts/verify_chapter_8_analysis.ps1"
        if not script_path.exists():
            self.fail("Chapter 8 real verification script is missing")

        text = script_path.read_text(encoding="utf-8")
        for marker in (
            "verify_chapter_6_trino_queries.ps1",
            "run_chapter_4_pipeline.ps1",
            "flink-sql-connector-kafka-3.3.0-1.19.jar",
            "flink-doris-connector-1.19-25.1.0.jar",
            "/analysis/realtime",
            'AI_ANALYZER_MODE = "rule_based"',
            'AI_API_KEY = ""',
            "evidence",
        ):
            self.assertIn(marker, text)

    def test_obsolete_nginx_placeholder_is_absent(self):
        self.assertFalse((ROOT / "infra/compose/app").exists())

    def test_chapter5_dependency_preserves_empty_baseline_sets(self):
        text = (ROOT / "scripts/verify_chapter_5_end_to_end.ps1").read_text(encoding="utf-8")
        self.assertIn("return ,$names", text)

    def test_readme_documents_chapter_8_strict_trust_boundary(self):
        text = (ROOT / "README.md").read_text(encoding="utf-8")
        for marker in (
            "第 8 章",
            "verify_chapter_8_analysis.ps1",
            "POST /analysis/realtime",
            "严格可信模式",
            "预定义派生值",
            "NFKC",
            "fail-closed",
            "整句语义正确",
            "结构化 claim",
        ):
            self.assertIn(marker, text)

    def test_design_and_plan_record_strict_trust_hardening(self):
        paths = (
            ROOT / "docs/superpowers/specs/2026-07-18-chapter-8-grounded-ai-analysis-design.md",
            ROOT / "docs/superpowers/plans/2026-07-18-chapter-8-grounded-ai-analysis-implementation.md",
        )
        for path in paths:
            with self.subTest(path=path.name):
                text = path.read_text(encoding="utf-8")
                for marker in (
                    "严格可信模式加固记录",
                    "预定义派生值",
                    "NFKC",
                    "fail-closed",
                    "可见中文/英文数字分隔语义",
                    "主分析器与回退分析器",
                    "LogRecord.extra",
                    "异常链脱敏",
                    "四字段显式完整",
                    "数值可追溯不等于整句语义正确",
                    "结构化 claim",
                ):
                    self.assertIn(marker, text)


if __name__ == "__main__":
    unittest.main()
