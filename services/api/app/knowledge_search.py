from __future__ import annotations

import math
from time import perf_counter

from .auth_service import Principal
from .knowledge_ingest import Embedder, EmbeddingUnavailable, _terms
from .knowledge_models import KnowledgeHit, SearchResult
from .knowledge_store import KnowledgeStore


_VISIBLE_CHUNKS = (
    "FROM knowledge.chunks c "
    "JOIN knowledge.versions v ON v.id = c.version_id "
    "JOIN knowledge.documents d ON d.id = v.document_id "
    "WHERE v.status = 'published' AND d.withdrawn_at IS NULL "
    "AND d.visibility_roles @> ARRAY[%s]::text[] "
)
_COLUMNS = (
    "SELECT c.id AS chunk_id, d.id AS document_id, v.id AS version_id, "
    "c.ordinal, c.section, c.page, c.content, d.source_type, d.source_ref, "
)


def reciprocal_rank_fusion(
    keyword: list[dict], vector: list[dict], limit: int = 5,
) -> list[dict]:
    fused: dict[object, dict] = {}
    for label, candidates in (("keyword_rank", keyword), ("vector_rank", vector)):
        for rank, candidate in enumerate(candidates, 1):
            key = candidate["chunk_id"]
            if key not in fused:
                fused[key] = {**candidate, "keyword_rank": None, "vector_rank": None, "score": 0.0}
            fused[key][label] = rank
            fused[key]["score"] += 1.0 / (60 + rank)
    ranked = sorted(
        fused.values(),
        key=lambda row: (
            -row["score"], row.get("source_ref", ""), row.get("ordinal", 0), str(row["chunk_id"]),
        ),
    )
    chosen: list[dict] = []
    counts: dict[object, int] = {}
    for row in ranked:
        document_id = row["document_id"]
        if counts.get(document_id, 0) >= 2:
            continue
        chosen.append(row)
        counts[document_id] = counts.get(document_id, 0) + 1
        if len(chosen) == limit:
            break
    return chosen


class KnowledgeSearchService:
    def __init__(self, store: KnowledgeStore, embedder: Embedder | None = None) -> None:
        self.store = store
        self.embedder = embedder or Embedder()

    def search(
        self, query: str, principal: Principal, limit: int = 5,
        *, route: str = "hybrid",
    ) -> SearchResult:
        started = perf_counter()
        query = query.strip()
        if not query or len(query) > 500 or not 1 <= limit <= 5:
            raise ValueError("query must have 1-500 characters and limit must be 1-5")
        if route not in {"hybrid", "keyword", "vector"}:
            raise ValueError("unsupported search route")
        if principal.role not in {"admin", "analyst", "viewer"}:
            raise ValueError("invalid role")
        terms = list(_terms(query)) if route != "vector" else []
        vector = None
        if route != "keyword":
            try:
                vector = self.embedder.embed_many([query])[0]
            except EmbeddingUnavailable:
                if route == "vector":
                    raise
        if vector is not None and (len(vector) != 512 or not all(math.isfinite(v) for v in vector)):
            raise ValueError("query embedding must have 512 finite values")

        with self.store._connect() as connection:
            if terms:
                keyword = connection.execute(
                    _COLUMNS +
                    "(SELECT count(*) FROM unnest(c.terms) t WHERE t = ANY(%s::text[])) AS overlap " +
                    _VISIBLE_CHUNKS + "AND c.terms && %s::text[] "
                    "ORDER BY overlap DESC, d.source_ref, c.ordinal, c.id LIMIT 20",
                    (terms, principal.role, terms),
                ).fetchall()
            else:
                keyword = []
            if vector is None:
                nearest = []
            else:
                vector_literal = "[" + ",".join(str(value) for value in vector) + "]"
                nearest = connection.execute(
                    _COLUMNS + "c.embedding <=> %s::vector AS distance " +
                    _VISIBLE_CHUNKS + "ORDER BY distance, d.source_ref, c.ordinal, c.id LIMIT 20",
                    (vector_literal, principal.role),
                ).fetchall()

        hits = [KnowledgeHit(
            chunk_id=row["chunk_id"], document_id=row["document_id"],
            version_id=row["version_id"], section=row["section"], page=row["page"],
            text=row["content"], source_label=("项目文档" if row["source_type"] == "project_doc" else "外部来源"),
            source_ref=row["source_ref"], keyword_rank=row["keyword_rank"],
            vector_rank=row["vector_rank"], score=row["score"],
            locator=f"/knowledge/documents/{row['document_id']}/versions/{row['version_id']}#chunk-{row['chunk_id']}",
        ) for row in reciprocal_rank_fusion(keyword, nearest, limit)]
        return SearchResult(
            hits=hits, mode=("vector_only" if route == "vector" else
                            "hybrid" if vector is not None else "keyword_only"),
            elapsed_ms=(perf_counter() - started) * 1000,
        )
