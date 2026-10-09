import sys
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock
from uuid import uuid4


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "api"))

from app.agent_models import AgentAnswer, Insight, ToolEvidence  # noqa: E402
from app.agent_store import AgentStore  # noqa: E402
from app.auth_service import Principal  # noqa: E402
from app.config import ApiSettings  # noqa: E402
from app.knowledge_models import Citation, RevokedCitation  # noqa: E402


def knowledge_evidence(chunk_id, document_id, version_id):
    return ToolEvidence(
        evidence_id="ev-knowledge", kind="knowledge",
        meta={"chunk_id": str(chunk_id), "document_id": str(document_id),
              "version_id": str(version_id)},
        text="受控的知识片段", target="/knowledge",
    )


def metric_evidence(run_id):
    return ToolEvidence(
        evidence_id="ev-orders", kind="orders",
        meta={"metric_run_id": run_id, "dataset_id": "olist-brazilian-ecommerce"},
        rows=[{"order_count": 99441}], target="/orders",
    )


class G4AgentStoreTest(unittest.TestCase):
    def setUp(self):
        self.knowledge = Mock()
        self.behavior = Mock()
        self.orders = Mock()
        self.store = AgentStore(ApiSettings(), self.knowledge, self.behavior, self.orders)
        self.connection = MagicMock()
        self.connection.__enter__.return_value = self.connection
        self.store._connect = Mock(return_value=self.connection)
        self.principal = Principal(17, "analyst", "analyst", "private-csrf")

    def report_row(self, answer, *, owner_id=17):
        return {
            "report_id": uuid4(), "answer_id": uuid4(), "owner_id": owner_id,
            "question": "订单与知识口径？", "answer": answer.model_dump(mode="json"),
            "created_at": datetime(2026, 10, 9, tzinfo=UTC),
        }

    def test_migration_is_repeatable_and_answers_are_owned(self):
        sql = (ROOT / "infra/compose/app-postgres/migrations/003_agent_reports.sql").read_text(encoding="utf-8")
        self.assertIn("CREATE TABLE IF NOT EXISTS agent_answers", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS agent_reports", sql)
        self.assertIn("REFERENCES app_users(id)", sql)
        self.assertIn("JSONB", sql)

    def test_last_turns_sends_only_three_bounded_server_summaries(self):
        self.connection.execute.return_value.fetchall.return_value = [
            {"question": f"问题{i}", "answer": AgentAnswer(
                status="evidence_only", fallback_summary=f"摘要{i}包含已撤回内容",
            ).model_dump(mode="json")}
            for i in (4, 3, 2)
        ]
        turns = self.store.last_turns(17)
        self.assertEqual([turn.question for turn in turns], ["问题2", "问题3", "问题4"])
        self.assertTrue(all("需要重新检索" in turn.summary for turn in turns))
        self.assertNotIn("已撤回内容", str(turns))
        sql, params = self.connection.execute.call_args.args
        self.assertIn("WHERE owner_id = %s", sql)
        self.assertEqual(params, (17, 3))

    def test_save_report_uses_atomic_owner_check_and_rejects_foreign_answer(self):
        answer_id = uuid4()
        self.connection.execute.return_value.fetchone.return_value = None
        with self.assertRaises(PermissionError):
            self.store.save_report(17, answer_id)
        sql, params = self.connection.execute.call_args.args
        self.assertIn("owner_id = %s", sql)
        self.assertEqual(params, (answer_id, 17))

    def test_other_users_cannot_load_report(self):
        row = self.report_row(AgentAnswer(status="refused"), owner_id=99)
        self.connection.execute.return_value.fetchone.return_value = row
        with self.assertRaises(PermissionError):
            self.store.load_report(row["report_id"], self.principal)

    def test_withdrawn_document_invalidates_report_and_hides_old_body(self):
        chunk_id, document_id, version_id = uuid4(), uuid4(), uuid4()
        answer = AgentAnswer(status="answered", insights=[Insight(
            text="旧回答正文不可见", evidence_ids=["ev-knowledge"],
        )], evidence=[knowledge_evidence(chunk_id, document_id, version_id)])
        row = self.report_row(answer)
        self.connection.execute.return_value.fetchone.return_value = row
        self.knowledge.resolve_citation.return_value = RevokedCitation(chunk_id)
        report = self.store.load_report(row["report_id"], self.principal)
        self.assertEqual(report["status"], "invalidated")
        self.assertIsNone(report["answer"])
        self.assertNotIn("旧回答正文不可见", str(report))
        self.knowledge.resolve_citation.assert_called_once_with(chunk_id, self.principal)

    def test_changed_version_also_invalidates_even_if_chunk_is_resolved(self):
        chunk_id, document_id, version_id = uuid4(), uuid4(), uuid4()
        answer = AgentAnswer(status="evidence_only", evidence=[
            knowledge_evidence(chunk_id, document_id, version_id),
        ])
        row = self.report_row(answer)
        self.connection.execute.return_value.fetchone.return_value = row
        self.knowledge.resolve_citation.return_value = Citation(
            chunk_id, document_id, uuid4(), "口径", None, "内容", "doc.md",
        )
        report = self.store.load_report(row["report_id"], self.principal)
        self.assertEqual(report["status"], "invalidated")
        self.assertIsNone(report["answer"])

    def test_new_metric_run_marks_historical_without_replacing_old_values(self):
        old_run = "orders-v1-old"
        answer = AgentAnswer(status="evidence_only", evidence=[metric_evidence(old_run)])
        row = self.report_row(answer)
        self.connection.execute.return_value.fetchone.return_value = row
        self.orders.get_publication.return_value.meta.metric_run_id = "orders-v1-new"
        report = self.store.load_report(row["report_id"], self.principal)
        self.assertEqual(report["status"], "historical")
        self.assertEqual(report["answer"]["evidence"][0]["meta"]["metric_run_id"], old_run)
        self.assertEqual(report["answer"]["evidence"][0]["rows"][0]["order_count"], 99441)


if __name__ == "__main__":
    unittest.main()
