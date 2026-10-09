import sys
import unittest
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "api"))

from app.agent_models import AgentAnswer  # noqa: E402
from app.auth_crypto import hash_password  # noqa: E402
from app.auth_service import AuthService  # noqa: E402
from app.main import create_app  # noqa: E402
from tests.test_g3_auth_api import MemoryAuthStore  # noqa: E402


class MemoryAgentStore:
    def __init__(self):
        self.answers = {}
        self.reports = {}
        self.history = []

    def last_turns(self, user_id, limit=3):
        return self.history[-limit:]

    def save_answer(self, owner_id, answer, *, question):
        answer_id = uuid4()
        self.answers[answer_id] = (owner_id, question, answer)
        return answer_id

    def save_report(self, owner_id, answer_id):
        if answer_id not in self.answers or self.answers[answer_id][0] != owner_id:
            raise PermissionError("answer unavailable")
        report_id = uuid4()
        self.reports[report_id] = (owner_id, answer_id)
        return report_id

    def list_reports(self, principal):
        return [{"report_id": report_id, "question": self.answers[answer_id][1]}
                for report_id, (owner_id, answer_id) in self.reports.items()
                if principal.role == "admin" or owner_id == principal.id]

    def load_report(self, report_id, principal):
        owner_id, answer_id = self.reports[report_id]
        if principal.role != "admin" and owner_id != principal.id:
            raise PermissionError("report unavailable")
        return {"report_id": report_id, "status": "current", "answer": self.answers[answer_id][2]}


class G4AgentApiTest(unittest.TestCase):
    def setUp(self):
        auth_store = MemoryAuthStore()
        for role in ("admin", "analyst", "viewer"):
            auth_store.create_user(role, hash_password(f"{role}-password"), role)
        self.store = MemoryAgentStore()
        self.service = Mock()
        self.service.ask.return_value = AgentAnswer(
            status="evidence_only", fallback_reason="model_off",
            fallback_summary="未调用模型。",
        )
        self.client = TestClient(create_app(
            auth_service=AuthService(auth_store), agent_store=self.store,
            agent_service=self.service,
        ))

    def login(self, role):
        self.client.cookies.clear()
        response = self.client.post("/api/v1/auth/login", json={
            "username": role, "password": f"{role}-password",
        })
        self.assertEqual(response.status_code, 200)
        return {"X-CSRF-Token": response.json()["csrf_token"]}

    def test_ask_requires_session_analyst_role_csrf_and_valid_question(self):
        path = "/api/v1/agent/ask"
        self.assertEqual(self.client.post(path, json={"question": "订单"}).status_code, 401)
        viewer_headers = self.login("viewer")
        self.assertEqual(self.client.post(path, headers=viewer_headers,
                                          json={"question": "订单"}).status_code, 403)
        headers = self.login("analyst")
        self.assertEqual(self.client.post(path, json={"question": "订单"}).status_code, 403)
        self.assertEqual(self.client.post(path, headers=headers,
                                          json={"question": "x" * 501}).status_code, 422)
        self.assertEqual(self.client.post(path, headers=headers,
                                          json={"question": "  "}).status_code, 422)
        self.service.ask.assert_not_called()

    def test_ask_saves_server_answer_and_only_sends_three_history_turns(self):
        headers = self.login("analyst")
        self.store.history = ["turn-a", "turn-b", "turn-c"]
        response = self.client.post("/api/v1/agent/ask", headers=headers, json={
            "question": "Olist 订单趋势？", "template_id": "orders_payments",
        })
        self.assertEqual(response.status_code, 200)
        answer_id = response.json()["answer_id"]
        self.assertEqual(len(self.store.answers), 1)
        self.assertEqual(self.store.answers[next(iter(self.store.answers))][1], "Olist 订单趋势？")
        self.assertEqual(response.json()["answer"]["status"], "evidence_only")
        args = self.service.ask.call_args.args
        self.assertEqual(args[0], "Olist 订单趋势？")
        self.assertEqual(args[1].role, "analyst")
        self.assertEqual(args[2], self.store.history)
        self.assertEqual(self.service.ask.call_args.kwargs["template_id"], "orders_payments")
        self.assertEqual(str(next(iter(self.store.answers))), answer_id)

    def test_ask_exposes_only_measured_usage(self):
        headers = self.login("analyst")
        self.service.ask.return_value = AgentAnswer(
            status="answered", usage={"input_tokens": 20, "output_tokens": 10,
                                      "generation_ms": 16},
        )
        response = self.client.post("/api/v1/agent/ask", headers=headers,
                                    json={"question": "订单口径？"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["usage"], {
            "input_tokens": 20, "output_tokens": 10, "generation_ms": 16.0,
        })

    def test_report_accepts_only_owned_answer_id_and_hides_other_users(self):
        analyst_headers = self.login("analyst")
        ask = self.client.post("/api/v1/agent/ask", headers=analyst_headers,
                               json={"question": "订单口径？"})
        answer_id = ask.json()["answer_id"]
        save_path = "/api/v1/agent/reports"
        self.assertEqual(self.client.post(save_path, json={"answer_id": answer_id}).status_code, 403)
        self.assertEqual(self.client.post(save_path, headers=analyst_headers, json={
            "answer_id": answer_id, "answer": {"status": "answered"},
        }).status_code, 422)
        self.assertEqual(self.client.post(save_path, headers=analyst_headers,
                                          json={"answer_id": str(uuid4())}).status_code, 403)
        saved = self.client.post(save_path, headers=analyst_headers, json={"answer_id": answer_id})
        self.assertEqual(saved.status_code, 201)
        report_id = saved.json()["report_id"]
        self.assertEqual(len(self.client.get(save_path).json()), 1)
        self.assertEqual(self.client.get(f"{save_path}/{report_id}").status_code, 200)

        other_headers = self.login("viewer")
        self.assertEqual(self.client.get(f"{save_path}/{report_id}").status_code, 403)
        self.assertEqual(self.client.post(save_path, headers=other_headers,
                                          json={"answer_id": answer_id}).status_code, 403)
        self.assertEqual(self.client.get(save_path).status_code, 403)
        admin_headers = self.login("admin")
        self.assertEqual(self.client.get(f"{save_path}/{report_id}").status_code, 200)
        self.assertEqual(self.client.get(save_path, headers=admin_headers).status_code, 200)


if __name__ == "__main__":
    unittest.main()
