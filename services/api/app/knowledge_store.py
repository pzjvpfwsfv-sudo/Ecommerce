from __future__ import annotations

from hashlib import sha256
import math
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from .auth_service import Principal
from .config import ApiSettings
from .knowledge_models import (
    Citation, DocumentMetadata, DocumentPreview, DocumentVersion, EmbeddedChunk,
    KnowledgeChunkPreview, KnowledgeDocument, RevokedCitation, Role, ParsedDocument,
)


def _version(row: dict) -> DocumentVersion:
    return DocumentVersion(
        id=row["id"], document_id=row["document_id"],
        version_no=row["version_no"], status=row["status"],
        created_at=row["created_at"], published_at=row["published_at"],
    )


def _validate_chunks(chunks: list[EmbeddedChunk]) -> None:
    if not chunks:
        raise ValueError("document must have at least one chunk")
    for ordinal, chunk in enumerate(chunks):
        if chunk.ordinal != ordinal or not 0 < len(chunk.text) <= 400:
            raise ValueError("invalid chunk order or size")
        if chunk.sha256 != sha256(chunk.text.encode("utf-8")).hexdigest():
            raise ValueError("chunk SHA-256 does not match text")
        if len(chunk.embedding) != 512 or not all(math.isfinite(v) for v in chunk.embedding):
            raise ValueError("embedding must have 512 finite values")


class KnowledgeStore:
    def __init__(self, settings: ApiSettings) -> None:
        self.settings = settings

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(
            host=self.settings.app_db_host, port=self.settings.app_db_port,
            dbname=self.settings.app_db_name, user=self.settings.app_db_user,
            password=self.settings.app_db_password, row_factory=dict_row,
            connect_timeout=5,
        )

    def create_draft(
        self, owner_id: int, metadata: DocumentMetadata, parsed: ParsedDocument,
        chunks: list[EmbeddedChunk],
    ) -> DocumentVersion:
        if not parsed.original_bytes or not parsed.sections:
            raise ValueError("document is empty")
        _validate_chunks(chunks)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                if metadata.document_id is None:
                    cursor.execute(
                        "INSERT INTO knowledge.documents "
                        "(title, category, source_type, source_ref, visibility_roles, owner_id) "
                        "VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
                        (metadata.title, metadata.category, metadata.source_type,
                         metadata.source_ref, list(metadata.visibility_roles), owner_id),
                    )
                    document_id = cursor.fetchone()["id"]
                else:
                    document_id = metadata.document_id
                    cursor.execute(
                        "SELECT title, category, source_type, source_ref, visibility_roles "
                        "FROM knowledge.documents WHERE id = %s FOR UPDATE",
                        (document_id,),
                    )
                    existing = cursor.fetchone()
                    if existing is None:
                        raise LookupError("document not found")
                    if (existing["title"], existing["category"], existing["source_type"],
                            existing["source_ref"], tuple(existing["visibility_roles"])) != (
                            metadata.title, metadata.category, metadata.source_type,
                            metadata.source_ref, metadata.visibility_roles):
                        raise ValueError("existing document metadata cannot change with a draft")
                cursor.execute(
                    "SELECT COALESCE(MAX(version_no), 0) + 1 AS next_version "
                    "FROM knowledge.versions WHERE document_id = %s",
                    (document_id,),
                )
                version_no = cursor.fetchone()["next_version"]
                cursor.execute(
                    "INSERT INTO knowledge.versions "
                    "(document_id, version_no, original_filename, original_bytes, "
                    "extracted_text, content_sha256) "
                    "VALUES (%s, %s, %s, %s, %s, %s) "
                    "RETURNING id, document_id, version_no, status, created_at, published_at",
                    (document_id, version_no, parsed.filename, parsed.original_bytes,
                     parsed.extracted_text, sha256(parsed.original_bytes).hexdigest()),
                )
                result = _version(cursor.fetchone())
                cursor.executemany(
                    "INSERT INTO knowledge.chunks "
                    "(version_id, ordinal, section, page, content, terms, sha256, embedding) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s::vector)",
                    [(result.id, chunk.ordinal, chunk.section, chunk.page, chunk.text,
                      list(chunk.terms), chunk.sha256,
                      "[" + ",".join(str(value) for value in chunk.embedding) + "]")
                     for chunk in chunks],
                )
        return result

    def publish(self, document_id: UUID, version_id: UUID) -> DocumentVersion:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id FROM knowledge.documents WHERE id = %s FOR UPDATE", (document_id,),
                )
                if cursor.fetchone() is None:
                    raise LookupError("document not found")
                cursor.execute(
                    "SELECT id FROM knowledge.versions WHERE id = %s AND document_id = %s "
                    "AND status = 'draft' FOR UPDATE",
                    (version_id, document_id),
                )
                if cursor.fetchone() is None:
                    raise ValueError("version is not a draft of this document")
                cursor.execute(
                    "UPDATE knowledge.versions SET status = 'withdrawn' "
                    "WHERE document_id = %s AND status = 'published'", (document_id,),
                )
                cursor.execute(
                    "UPDATE knowledge.versions SET status = 'published', published_at = now() "
                    "WHERE id = %s RETURNING id, document_id, version_no, status, created_at, published_at",
                    (version_id,),
                )
                result = _version(cursor.fetchone())
                cursor.execute(
                    "UPDATE knowledge.documents SET withdrawn_at = NULL WHERE id = %s", (document_id,),
                )
        return result

    def withdraw(self, document_id: UUID) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE knowledge.documents SET withdrawn_at = now() WHERE id = %s",
                    (document_id,),
                )
                if cursor.rowcount == 0:
                    raise LookupError("document not found")
                cursor.execute(
                    "UPDATE knowledge.versions SET status = 'withdrawn' "
                    "WHERE document_id = %s AND status = 'published'", (document_id,),
                )

    def delete(self, document_id: UUID) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM knowledge.documents WHERE id = %s", (document_id,))

    def list_documents(self, role: Role) -> list[KnowledgeDocument]:
        if role not in {"admin", "analyst", "viewer"}:
            raise ValueError("invalid role")
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT d.id, d.title, d.category, d.source_type, d.source_ref, "
                "d.visibility_roles, v.id AS published_version_id "
                "FROM knowledge.documents d "
                "LEFT JOIN knowledge.versions v ON v.document_id = d.id AND v.status = 'published' "
                "WHERE %s = 'admin' OR (v.id IS NOT NULL AND d.withdrawn_at IS NULL "
                "AND d.visibility_roles @> ARRAY[%s]::text[]) "
                "ORDER BY d.created_at DESC, d.id",
                (role, role),
            ).fetchall()
        return [KnowledgeDocument(
            id=row["id"], title=row["title"], category=row["category"],
            source_type=row["source_type"], source_ref=row["source_ref"],
            visibility_roles=tuple(row["visibility_roles"]),
            published_version_id=row["published_version_id"],
        ) for row in rows]

    def preview(self, document_id: UUID, version_id: UUID, principal: Principal) -> DocumentPreview:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT v.extracted_text FROM knowledge.documents d "
                    "JOIN knowledge.versions v ON v.document_id = d.id "
                    "WHERE d.id = %s AND v.id = %s AND "
                    "((%s = 'admin' AND v.status IN ('draft', 'published')) OR "
                    "(v.status = 'published' AND d.withdrawn_at IS NULL "
                    "AND d.visibility_roles @> ARRAY[%s]::text[])) "
                    "FOR SHARE OF d, v",
                    (document_id, version_id, principal.role, principal.role),
                )
                row = cursor.fetchone()
                if row is None:
                    raise PermissionError("document version is unavailable")
                cursor.execute(
                    "SELECT id, ordinal, section, page, content FROM knowledge.chunks "
                    "WHERE version_id = %s ORDER BY ordinal", (version_id,),
                )
                chunks = tuple(KnowledgeChunkPreview(
                    id=chunk["id"], ordinal=chunk["ordinal"], section=chunk["section"],
                    page=chunk["page"], text=chunk["content"],
                ) for chunk in cursor.fetchall())
                result = DocumentPreview(document_id, version_id, row["extracted_text"], chunks)
        return result

    def resolve_citation(self, chunk_id: UUID, principal: Principal) -> Citation | RevokedCitation:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT c.id AS chunk_id, d.id AS document_id, v.id AS version_id, "
                "c.section, c.page, c.content, d.source_ref "
                "FROM knowledge.chunks c "
                "JOIN knowledge.versions v ON v.id = c.version_id "
                "JOIN knowledge.documents d ON d.id = v.document_id "
                "WHERE c.id = %s AND v.status = 'published' AND d.withdrawn_at IS NULL "
                "AND d.visibility_roles @> ARRAY[%s]::text[]",
                (chunk_id, principal.role),
            ).fetchone()
        if row is None:
            return RevokedCitation(chunk_id)
        return Citation(
            chunk_id=row["chunk_id"], document_id=row["document_id"],
            version_id=row["version_id"], section=row["section"], page=row["page"],
            text=row["content"], source_ref=row["source_ref"],
        )
