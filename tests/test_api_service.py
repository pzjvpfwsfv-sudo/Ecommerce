from pathlib import Path
import sys
import unittest
from unittest.mock import Mock

from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT / "services" / "api" / "app" / "config.py"
REPOSITORY_FILE = ROOT / "services" / "api" / "app" / "repository.py"
MAIN_FILE = ROOT / "services" / "api" / "app" / "main.py"
REQUIREMENTS_FILE = ROOT / "services" / "api" / "requirements.txt"

sys.path.insert(0, str((ROOT / "services" / "api").resolve()))
from app.main import create_app  # noqa: E402


class ApiServiceArtifactsTest(unittest.TestCase):
    def test_requirements_define_fastapi_runtime(self):
        text = REQUIREMENTS_FILE.read_text(encoding="utf-8")

        self.assertIn("fastapi", text)
        self.assertIn("uvicorn", text)
        self.assertIn("pymysql", text)

    def test_config_defines_doris_settings(self):
        text = CONFIG_FILE.read_text(encoding="utf-8")

        self.assertIn("class ApiSettings", text)
        self.assertIn("doris_host", text)
        self.assertIn("doris_port", text)
        self.assertIn("doris_database", text)

    def test_repository_exposes_metric_queries(self):
        text = REPOSITORY_FILE.read_text(encoding="utf-8")

        self.assertIn("class RealtimeMetricsRepository", text)
        self.assertIn("def fetch_all_metrics", text)
        self.assertIn("def fetch_metric", text)
        self.assertIn("SELECT metric_name, metric_value, updated_at", text)

    def test_main_defines_required_endpoints(self):
        text = MAIN_FILE.read_text(encoding="utf-8")

        self.assertIn('@app.get("/health")', text)
        self.assertIn('@app.get("/metrics/realtime")', text)
        self.assertIn('@app.get("/metrics/{metric_name}")', text)
        self.assertIn('@app.post("/analysis/realtime"', text)
        self.assertIn('@app.post("/analysis/tools"', text)
        self.assertIn("build_analysis_service", text)
        self.assertIn("build_tool_analysis_service", text)
        self.assertIn("create_app", text)


class ApiServiceRuntimeTest(unittest.TestCase):
    def test_health_endpoint_returns_expected_payload(self):
        client = TestClient(create_app(repository=Mock()))

        response = client.get("/health")

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            {"status": "ok", "service": "realtime-metrics-api"},
            response.json(),
        )

    def test_realtime_metrics_endpoint_returns_repository_payload(self):
        repository = Mock()
        repository.fetch_all_metrics.return_value = {
            "pv": 12,
            "uv": 5,
            "updated_at": "2026-07-07T10:00:00",
        }
        client = TestClient(create_app(repository=repository))

        response = client.get("/metrics/realtime")

        self.assertEqual(200, response.status_code)
        self.assertEqual(repository.fetch_all_metrics.return_value, response.json())

    def test_single_metric_endpoint_returns_404_when_missing(self):
        repository = Mock()
        repository.fetch_metric.return_value = None
        client = TestClient(create_app(repository=repository))

        response = client.get("/metrics/gmv")

        self.assertEqual(404, response.status_code)
        self.assertEqual({"detail": "metric 'gmv' not found"}, response.json())

    def test_realtime_analysis_endpoint_remains_compatible_with_injected_service(self):
        repository = Mock()
        analysis_service = Mock()
        analysis_service.analyze.return_value = {
            "summary": "当前累计访问 2 次，覆盖 2 名用户。",
            "insights": [],
            "risks": [],
            "actions": [],
            "evidence": {"realtime": {"pv": 2, "uv": 2}},
            "warnings": [],
            "analyzer": "rule_based",
            "generated_at": "2026-08-03T00:00:00Z",
        }
        client = TestClient(create_app(repository=repository, analysis_service=analysis_service))

        response = client.post("/analysis/realtime", json={"question": "  实时分析  "})

        self.assertEqual(200, response.status_code)
        analysis_service.analyze.assert_called_once_with("实时分析")


if __name__ == "__main__":
    unittest.main()
