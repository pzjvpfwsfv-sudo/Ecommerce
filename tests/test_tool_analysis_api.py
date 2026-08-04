from datetime import datetime, timezone
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock
from uuid import UUID

from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app.analysis_models import RealtimeEvidence
from app.config import ApiSettings
from app.main import create_app
from app.tool_analysis_service import ToolAnalysisUnavailableError
from app.tool_models import ToolAnalysisResponse, ToolCallSummary, ToolEvidence, ToolId


def valid_tool_response() -> ToolAnalysisResponse:
    return ToolAnalysisResponse(
        summary="当前累计访问 2 次，覆盖 2 名用户。",
        insights=["人均访问次数约为 1.0 次。"],
        risks=["当前实时指标为累计值，缺少时间窗口对照时不能判断上涨或下降。"],
        actions=["补充分级时间窗口指标后，再判断变化趋势。"],
        evidence=ToolEvidence(realtime=RealtimeEvidence(pv=2, uv=2)),
        tool_calls=[
            ToolCallSummary(
                tool_id=ToolId.REALTIME,
                status="success",
                duration_ms=1,
            )
        ],
        planner="rule_based",
        analyzer="rule_based",
        degraded=False,
        audit_id=UUID("00000000-0000-0000-0000-000000000001"),
        generated_at=datetime(2026, 8, 3, tzinfo=timezone.utc),
    )


class ToolAnalysisApiTest(unittest.TestCase):
    def setUp(self):
        self.service = Mock()
        self.client = TestClient(
            create_app(
                repository=Mock(),
                tool_analysis_service=self.service,
                settings=ApiSettings(ai_max_question_length=10),
            )
        )

    def test_endpoint_normalizes_question_and_returns_explicit_response(self):
        self.service.analyze.return_value = valid_tool_response()

        response = self.client.post("/analysis/tools", json={"question": "  综合分析  "})

        self.assertEqual(200, response.status_code)
        self.assertEqual("00000000-0000-0000-0000-000000000001", response.json()["audit_id"])
        self.service.analyze.assert_called_once_with("综合分析")

    def test_endpoint_rejects_blank_question_before_calling_service(self):
        response = self.client.post("/analysis/tools", json={"question": "   "})

        self.assertEqual(422, response.status_code)
        self.service.analyze.assert_not_called()

    def test_endpoint_rejects_overlong_question_before_calling_service(self):
        response = self.client.post("/analysis/tools", json={"question": "x" * 11})

        self.assertEqual(422, response.status_code)
        self.assertEqual({"detail": "question is too long"}, response.json())
        self.service.analyze.assert_not_called()

    def test_endpoint_maps_domain_failure_to_fixed_503(self):
        self.service.analyze.side_effect = ToolAnalysisUnavailableError("password=secret")

        response = self.client.post("/analysis/tools", json={"question": "综合分析"})

        self.assertEqual(503, response.status_code)
        self.assertEqual({"detail": "analysis tools are temporarily unavailable"}, response.json())
        self.assertNotIn("password=secret", response.text)

    def test_endpoint_maps_unknown_failure_to_fixed_503_without_details(self):
        self.service.analyze.side_effect = RuntimeError("password=secret sql=SELECT url=http://private")

        response = self.client.post("/analysis/tools", json={"question": "综合分析"})

        self.assertEqual(503, response.status_code)
        self.assertEqual({"detail": "analysis tools are temporarily unavailable"}, response.json())
        self.assertNotIn("password=secret", response.text)
        self.assertNotIn("SELECT", response.text)
        self.assertNotIn("http://private", response.text)

    def test_endpoint_maps_malformed_service_response_to_fixed_503(self):
        self.service.analyze.return_value = {"audit_id": "not-a-response"}

        response = self.client.post("/analysis/tools", json={"question": "综合分析"})

        self.assertEqual(503, response.status_code)
        self.assertEqual({"detail": "analysis tools are temporarily unavailable"}, response.json())


if __name__ == "__main__":
    unittest.main()
