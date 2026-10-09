from __future__ import annotations

import logging
from typing import Literal
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from app.agent_service import AgentService
from app.agent_store import AgentStore
from app.auth_service import Principal, require_analyst, require_csrf


logger = logging.getLogger(__name__)


class AskBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str = Field(min_length=1, max_length=500)
    template_id: Literal[
        "orders_payments", "delivery_reviews", "rankings", "behavior_funnel",
        "definitions_quality",
    ] | None = None


class SaveReportBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    answer_id: UUID


def _run(operation):
    try:
        return operation()
    except ValueError:
        raise HTTPException(status_code=422, detail="invalid agent request") from None
    except PermissionError:
        raise HTTPException(status_code=403, detail="agent item unavailable") from None
    except LookupError:
        raise HTTPException(status_code=404, detail="agent item unavailable") from None
    except psycopg.Error as exc:
        logger.error("agent_store_unavailable", extra={"error_type": type(exc).__name__})
        raise HTTPException(status_code=503, detail="agent service temporarily unavailable") from None


def create_agent_router(service: AgentService, store: AgentStore) -> APIRouter:
    router = APIRouter(prefix="/api/v1/agent", tags=["agent"])

    @router.post("/ask")
    def ask(
        body: AskBody, principal: Principal = Depends(require_analyst),
        _: Principal = Depends(require_csrf),
    ):
        def perform():
            question = body.question.strip()
            if not question:
                raise ValueError("question is empty")
            history = store.last_turns(principal.id)
            answer = service.ask(question, principal, history, template_id=body.template_id)
            answer_id = store.save_answer(principal.id, answer, question=question)
            return {"answer_id": answer_id, "answer": answer, "usage": answer.usage}
        return _run(perform)

    @router.get("/reports")
    def reports(principal: Principal = Depends(require_analyst)):
        return _run(lambda: store.list_reports(principal))

    @router.get("/reports/{report_id}")
    def report(report_id: UUID, principal: Principal = Depends(require_analyst)):
        return _run(lambda: store.load_report(report_id, principal))

    @router.post("/reports", status_code=201)
    def save_report(
        body: SaveReportBody, principal: Principal = Depends(require_analyst),
        _: Principal = Depends(require_csrf),
    ):
        return _run(lambda: {"report_id": store.save_report(principal.id, body.answer_id)})

    return router
