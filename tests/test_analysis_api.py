from pathlib import Path
import sys
import unittest
from unittest.mock import Mock

from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app.analysis_models import AnalysisEvidence, AnalysisResponse, RealtimeEvidence  # noqa: E402
from app.analysis_service import (  # noqa: E402
    AnalysisUnavailableError,
    RealtimeDataUnavailableError,
)
from app.config import ApiSettings  # noqa: E402
from app.main import create_app  # noqa: E402


class AnalysisApiTest(unittest.TestCase):
    def test_analysis_endpoint_returns_service_response(self):
        service = Mock()
        service.analyze.return_value = AnalysisResponse(
            summary="可信结论",
            evidence=AnalysisEvidence(realtime=RealtimeEvidence(pv=12, uv=5)),
            analyzer="rule_based",
            generated_at="2026-07-18T00:00:00Z",
        )
        client = TestClient(
            create_app(
                repository=Mock(),
                analysis_service=service,
                settings=ApiSettings(ai_max_question_length=20),
            )
        )

        response = client.post("/analysis/realtime", json={"question": "  分析活跃度  "})

        self.assertEqual(200, response.status_code)
        self.assertEqual("可信结论", response.json()["summary"])
        self.assertEqual({"pv": 12, "uv": 5, "updated_at": None}, response.json()["evidence"]["realtime"])
        self.assertEqual("rule_based", response.json()["analyzer"])
        service.analyze.assert_called_once_with("分析活跃度")

    def test_blank_question_returns_422(self):
        client = TestClient(create_app(repository=Mock(), analysis_service=Mock()))

        response = client.post("/analysis/realtime", json={"question": "   "})

        self.assertEqual(422, response.status_code)

    def test_question_over_configured_limit_returns_422(self):
        client = TestClient(
            create_app(
                repository=Mock(),
                analysis_service=Mock(),
                settings=ApiSettings(ai_max_question_length=3),
            )
        )

        response = client.post("/analysis/realtime", json={"question": "超过长度"})

        self.assertEqual(422, response.status_code)

    def test_realtime_data_failure_returns_safe_503(self):
        service = Mock()
        service.analyze.side_effect = RealtimeDataUnavailableError("Doris password leaked")
        client = TestClient(create_app(repository=Mock(), analysis_service=service))

        response = client.post("/analysis/realtime", json={"question": "分析活跃度"})

        self.assertEqual(503, response.status_code)
        self.assertEqual(
            {"detail": "realtime metrics are temporarily unavailable"}, response.json()
        )

    def test_analysis_failure_returns_safe_503(self):
        service = Mock()
        service.analyze.side_effect = AnalysisUnavailableError("provider secret leaked")
        client = TestClient(create_app(repository=Mock(), analysis_service=service))

        response = client.post("/analysis/realtime", json={"question": "分析活跃度"})

        self.assertEqual(503, response.status_code)
        self.assertEqual(
            {"detail": "analysis is temporarily unavailable"}, response.json()
        )


if __name__ == "__main__":
    unittest.main()
