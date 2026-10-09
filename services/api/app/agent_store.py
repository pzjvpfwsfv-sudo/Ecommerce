from __future__ import annotations

from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.agent_models import AgentAnswer
from app.agent_service import ConversationTurn
from app.auth_service import Principal
from app.behavior_service import BehaviorMetricsService
from app.config import ApiSettings
from app.knowledge_models import Citation
from app.knowledge_store import KnowledgeStore
from app.order_service import OrderMetricsService


class AgentStore:
    def __init__(
        self, settings: ApiSettings, knowledge: KnowledgeStore,
        behavior: BehaviorMetricsService, orders: OrderMetricsService,
    ) -> None:
        self.settings = settings
        self.knowledge = knowledge
        self.behavior = behavior
        self.orders = orders

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(
            host=self.settings.app_db_host, port=self.settings.app_db_port,
            dbname=self.settings.app_db_name, user=self.settings.app_db_user,
            password=self.settings.app_db_password, row_factory=dict_row,
            connect_timeout=5,
        )

    def last_turns(self, user_id: int, limit: int = 3) -> list[ConversationTurn]:
        if user_id <= 0 or limit < 1:
            raise ValueError("invalid conversation owner or limit")
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT question, answer FROM agent_answers WHERE owner_id = %s "
                "ORDER BY created_at DESC, id DESC LIMIT %s",
                (user_id, min(limit, 3)),
            ).fetchall()
        turns = []
        for row in reversed(rows):
            answer = AgentAnswer.model_validate(row["answer"])
            if answer.status == "answered":
                summary = "上一轮已返回有据分析；新问题需要重新检索并核对来源。"
            elif answer.status == "evidence_only":
                summary = "上一轮仅展示证据；新问题需要重新检索并核对来源。"
            else:
                summary = "上一轮未找到足够证据；新问题需要重新检索并核对来源。"
            turns.append(ConversationTurn(row["question"][:500], summary))
        return turns

    def save_answer(self, owner_id: int, answer: AgentAnswer, *, question: str) -> UUID:
        question = question.strip()
        if owner_id <= 0 or not 1 <= len(question) <= 500:
            raise ValueError("invalid answer owner or question")
        with self._connect() as connection:
            row = connection.execute(
                "INSERT INTO agent_answers (owner_id, question, answer) "
                "VALUES (%s, %s, %s) RETURNING id",
                (owner_id, question, Jsonb(answer.model_dump(mode="json"))),
            ).fetchone()
        if row is None:
            raise RuntimeError("answer insert returned no id")
        return row["id"]

    def save_report(self, owner_id: int, answer_id: UUID) -> UUID:
        with self._connect() as connection:
            row = connection.execute(
                "INSERT INTO agent_reports (owner_id, answer_id) "
                "SELECT owner_id, id FROM agent_answers "
                "WHERE id = %s AND owner_id = %s RETURNING id",
                (answer_id, owner_id),
            ).fetchone()
        if row is None:
            raise PermissionError("answer unavailable")
        return row["id"]

    def list_reports(self, principal: Principal) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT r.id AS report_id, r.created_at, a.question "
                "FROM agent_reports r JOIN agent_answers a ON a.id = r.answer_id "
                "WHERE (%s = 'admin' OR r.owner_id = %s) "
                "ORDER BY r.created_at DESC, r.id DESC",
                (principal.role, principal.id),
            ).fetchall()
        return rows

    def load_report(self, report_id: UUID, principal: Principal) -> dict:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT r.id AS report_id, r.answer_id, r.owner_id, r.created_at, "
                "a.question, a.answer FROM agent_reports r "
                "JOIN agent_answers a ON a.id = r.answer_id AND a.owner_id = r.owner_id "
                "WHERE r.id = %s",
                (report_id,),
            ).fetchone()
        if row is None:
            raise LookupError("report unavailable")
        if principal.role != "admin" and row["owner_id"] != principal.id:
            raise PermissionError("report unavailable")

        answer = AgentAnswer.model_validate(row["answer"])
        report = {
            "report_id": row["report_id"], "answer_id": row["answer_id"],
            "question": row["question"], "created_at": row["created_at"],
            "status": "current", "answer": None,
        }
        for item in answer.evidence:
            if item.kind != "knowledge":
                continue
            try:
                chunk_id = UUID(str(item.meta["chunk_id"]))
                document_id = UUID(str(item.meta["document_id"]))
                version_id = UUID(str(item.meta["version_id"]))
            except (KeyError, TypeError, ValueError):
                report["status"] = "invalidated"
                return report
            citation = self.knowledge.resolve_citation(chunk_id, principal)
            if not isinstance(citation, Citation) or (
                citation.document_id != document_id or citation.version_id != version_id
            ):
                report["status"] = "invalidated"
                return report

        for kind, service in (("behavior", self.behavior), ("orders", self.orders)):
            saved_runs = {str(item.meta.get("metric_run_id")) for item in answer.evidence
                          if item.kind == kind}
            if not saved_runs:
                continue
            try:
                current_run = service.get_publication().meta.metric_run_id
            except Exception:
                report["status"] = "unverified"
                continue
            if saved_runs != {current_run} and report["status"] == "current":
                report["status"] = "historical"
        report["answer"] = answer.model_dump(mode="json")
        return report
