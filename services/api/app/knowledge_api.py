from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Literal
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel

from .auth_service import Principal, require_admin, require_csrf, require_principal
from .config import ApiSettings
from .knowledge_ingest import (
    MAX_FILE_BYTES, Embedder, EmbeddingUnavailable, chunk_document,
    metric_definition_sections, parse_document,
)
from .knowledge_models import DocumentMetadata, EmbeddedChunk, ParsedDocument
from .knowledge_search import KnowledgeSearchService
from .knowledge_store import KnowledgeStore


logger = logging.getLogger(__name__)
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if not (_PROJECT_ROOT / "configs/knowledge/g4_sources.json").is_file():
    _PROJECT_ROOT = Path("/app")


class MetricImport(BaseModel):
    domain: Literal["behavior", "orders"]


def _run(operation):
    try:
        return operation()
    except (ValueError, UnicodeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    except (LookupError, PermissionError):
        raise HTTPException(status_code=404, detail="knowledge item unavailable") from None
    except (psycopg.Error, EmbeddingUnavailable, OSError) as exc:
        logger.error("knowledge_unavailable", extra={"error_type": type(exc).__name__})
        raise HTTPException(status_code=503, detail="knowledge service temporarily unavailable") from None


def _embedded(parsed: ParsedDocument, embedder: Embedder) -> list[EmbeddedChunk]:
    inputs = chunk_document(parsed)
    vectors = embedder.embed_many([chunk.text for chunk in inputs])
    if len(vectors) != len(inputs):
        raise ValueError("embedding count does not match document chunks")
    return [EmbeddedChunk(**vars(chunk), embedding=tuple(vector))
            for chunk, vector in zip(inputs, vectors)]


def _verified_project_source(source_ref: str, data: bytes) -> None:
    manifest = json.loads((_PROJECT_ROOT / "configs/knowledge/g4_sources.json").read_text(encoding="utf-8"))
    allowed = {item["path"] for item in manifest["sources"] if item["path"].startswith("docs/")}
    if source_ref not in allowed or (_PROJECT_ROOT / source_ref).read_bytes() != data:
        raise ValueError("project documents must match an approved repository source")


def create_knowledge_router(
    settings: ApiSettings, store: KnowledgeStore, search: KnowledgeSearchService,
    embedder: Embedder,
) -> APIRouter:
    router = APIRouter(prefix="/api/v1/knowledge", tags=["knowledge"])

    @router.get("/documents")
    def documents(principal: Principal = Depends(require_principal)):
        return _run(lambda: store.list_documents(principal.role))

    @router.get("/documents/{document_id}/versions")
    def versions(document_id: UUID, principal: Principal = Depends(require_principal)):
        return _run(lambda: store.list_versions(document_id, principal))

    @router.get("/documents/{document_id}/versions/{version_id}")
    def preview(document_id: UUID, version_id: UUID, principal: Principal = Depends(require_principal)):
        return _run(lambda: store.preview(document_id, version_id, principal))

    @router.get("/citations/{chunk_id}")
    def citation(chunk_id: UUID, principal: Principal = Depends(require_principal)):
        return _run(lambda: store.resolve_citation(chunk_id, principal))

    @router.get("/search")
    def search_knowledge(
        q: str = Query(min_length=1, max_length=500),
        principal: Principal = Depends(require_principal),
    ):
        return _run(lambda: search.search(q, principal))

    @router.post("/documents", status_code=201)
    def upload_document(
        file: UploadFile = File(...), title: str = Form(...), category: str = Form(...),
        source_type: Literal["project_doc", "external"] = Form(...),
        source_ref: str = Form(...), visibility_roles: str = Form(...),
        document_id: UUID | None = Form(None),
        admin: Principal = Depends(require_admin), _: Principal = Depends(require_csrf),
    ):
        def create():
            if not file.filename or "/" in file.filename or "\\" in file.filename:
                raise ValueError("invalid filename")
            data = file.file.read(MAX_FILE_BYTES + 1)
            parsed = parse_document(file.filename, data)
            metadata = DocumentMetadata(
                title=title, category=category, source_type=source_type, source_ref=source_ref,
                visibility_roles=tuple(role.strip() for role in visibility_roles.split(",") if role.strip()),
                document_id=document_id,
            )
            if source_type == "project_doc":
                _verified_project_source(source_ref, data)
            return store.create_draft(admin.id, metadata, parsed, _embedded(parsed, embedder))
        return _run(create)

    @router.post("/import-metrics", status_code=201)
    def import_metrics(
        request: MetricImport, admin: Principal = Depends(require_admin),
        _: Principal = Depends(require_csrf),
    ):
        def create():
            parsed = metric_definition_sections(request.domain, settings)
            source_ref = f"configs/metrics/{request.domain}-v1.json"
            previous = next((doc for doc in store.list_documents("admin")
                             if doc.source_ref == source_ref), None)
            metadata = DocumentMetadata(
                title=f"{request.domain} v1 指标口径", category="指标定义",
                source_type="project_doc", source_ref=source_ref,
                visibility_roles=("admin", "analyst", "viewer"),
                document_id=previous.id if previous else None,
            )
            return store.create_draft(admin.id, metadata, parsed, _embedded(parsed, embedder))
        return _run(create)

    @router.post("/documents/{document_id}/versions/{version_id}/publish")
    def publish(document_id: UUID, version_id: UUID, admin: Principal = Depends(require_admin),
                _: Principal = Depends(require_csrf)):
        return _run(lambda: store.publish(document_id, version_id))

    @router.post("/documents/{document_id}/withdraw")
    def withdraw(document_id: UUID, admin: Principal = Depends(require_admin),
                 _: Principal = Depends(require_csrf)):
        _run(lambda: store.withdraw(document_id))
        return {"status": "withdrawn"}

    @router.delete("/documents/{document_id}")
    def delete(document_id: UUID, admin: Principal = Depends(require_admin),
               _: Principal = Depends(require_csrf)):
        _run(lambda: store.delete(document_id))
        return {"status": "deleted"}

    return router
