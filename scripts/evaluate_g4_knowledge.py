"""Evaluate frozen G4-A retrieval questions against an isolated pgvector database."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
import os
from pathlib import Path
import sys
from uuid import UUID, uuid4


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "api"))

from app.auth_service import Principal  # noqa: E402
from app.config import ApiSettings  # noqa: E402
from app.knowledge_ingest import (  # noqa: E402
    Embedder, EmbeddingUnavailable, chunk_document, metric_definition_sections, parse_document,
)
from app.knowledge_models import Citation, DocumentMetadata, EmbeddedChunk  # noqa: E402
from app.knowledge_search import KnowledgeSearchService  # noqa: E402
from app.knowledge_store import KnowledgeStore  # noqa: E402


FIXTURE = ROOT / "tests" / "fixtures" / "g4_knowledge_questions.json"
MANIFEST = ROOT / "configs" / "knowledge" / "g4_sources.json"
DEFAULT_OUTPUT = Path("D:/EcommerceDev/temp/g4/knowledge-evaluation.json")
ROUTES = ("keyword", "vector", "hybrid")
SECURITY_KINDS = {"unauthorized", "withdrawn", "old_version"}


@dataclass(frozen=True)
class RetrievalCase:
    id: str
    split: str
    kind: str
    question: str
    role: str = "viewer"
    source_ref: str | None = None
    section: str | None = None
    text: str | None = None
    forbidden_document_ids: tuple[UUID, ...] = ()
    forbidden_version_ids: tuple[UUID, ...] = ()

    def with_forbidden_document_ids(self, ids: tuple[UUID, ...]) -> RetrievalCase:
        return replace(self, forbidden_document_ids=ids)


def load_cases(path: Path = FIXTURE, *, split: str = "holdout") -> list[RetrievalCase]:
    if split not in {"development", "holdout", "all"}:
        raise ValueError("invalid evaluation split")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if len(raw.get("development", [])) != 10 or len(raw.get("holdout", [])) < 30:
        raise ValueError("frozen fixture requires 10 development and at least 30 holdout cases")
    cases = []
    for group in ("development", "holdout"):
        for item in raw[group]:
            case = RetrievalCase(split=group, **item)
            if case.kind not in {"answer", "no_answer", *SECURITY_KINDS}:
                raise ValueError(f"invalid case kind: {case.id}")
            if not case.question or case.role not in {"admin", "analyst", "viewer"}:
                raise ValueError(f"invalid question or role: {case.id}")
            if case.kind == "answer" and (not case.source_ref or not (case.section or case.text)):
                raise ValueError(f"positive case lacks a source anchor: {case.id}")
            cases.append(case)
    if len({case.id for case in cases}) != len(cases):
        raise ValueError("duplicate evaluation case ID")
    return [case for case in cases if split == "all" or case.split == split]


def _target_found(case: RetrievalCase, hits: list) -> bool:
    return any(
        hit.source_ref == case.source_ref
        and (case.section is None or hit.section == case.section)
        and (case.text is None or case.text in hit.text)
        for hit in hits
    )


def _p95(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[math.ceil(0.95 * len(ordered)) - 1], 3)


def evaluate(cases: list[RetrievalCase], search: KnowledgeSearchService) -> dict:
    if not cases:
        raise ValueError("evaluation requires cases")
    routes = {name: {"available": True, "hits": 0, "denominator": 0,
                     "latencies_ms": [], "missed_case_ids": []} for name in ROUTES}
    citation_total = citation_ok = forbidden_leaks = no_answer_false_positives = 0
    no_answer_total = security_checks = 0
    leak_case_ids: set[str] = set()
    hybrid_top_scores: dict[str, float] = {}
    for case in cases:
        principal = Principal(1, "g4-evaluator", case.role, "evaluation")
        for route in ROUTES:
            try:
                result = search.search(case.question, principal, route=route)
            except EmbeddingUnavailable:
                if route == "keyword":
                    raise
                routes[route]["available"] = False
                continue
            if route == "hybrid" and result.mode != "hybrid":
                routes[route]["available"] = False
                continue
            if route == "vector" and result.mode != "vector_only":
                routes[route]["available"] = False
                continue
            route_data = routes[route]
            route_data["latencies_ms"].append(result.elapsed_ms)
            hits = result.hits[:5]
            if case.kind == "answer":
                route_data["denominator"] += 1
                if _target_found(case, hits):
                    route_data["hits"] += 1
                else:
                    route_data["missed_case_ids"].append(case.id)
            if case.kind in SECURITY_KINDS:
                security_checks += 1
                for hit in hits:
                    if (hit.document_id in case.forbidden_document_ids
                            or hit.version_id in case.forbidden_version_ids):
                        forbidden_leaks += 1
                        leak_case_ids.add(case.id)
            if route == "hybrid":
                hybrid_top_scores[case.id] = max((hit.score for hit in hits), default=0.0)
                if case.kind == "no_answer":
                    no_answer_total += 1
                    if hits:
                        no_answer_false_positives += 1
                if search.store is not None:
                    for hit in hits:
                        citation_total += 1
                        resolved = search.store.resolve_citation(hit.chunk_id, principal)
                        if (isinstance(resolved, Citation)
                                and resolved.document_id == hit.document_id
                                and resolved.version_id == hit.version_id
                                and resolved.section == hit.section
                                and resolved.page == hit.page
                                and resolved.text == hit.text
                                and resolved.source_ref == hit.source_ref
                                and str(hit.chunk_id) in hit.locator):
                            citation_ok += 1
    for route_data in routes.values():
        denominator = route_data.pop("denominator")
        latencies = route_data.pop("latencies_ms")
        route_data["denominator"] = denominator
        route_data["recall_at_5"] = (
            round(route_data["hits"] / denominator, 4)
            if route_data["available"] and denominator else None
        )
        route_data["p95_ms"] = _p95(latencies) if route_data["available"] else None
        route_data["query_count"] = len(latencies)
    hybrid = routes["hybrid"]
    single = [routes[name]["recall_at_5"] for name in ("keyword", "vector")]
    best_single = max((value for value in single if value is not None), default=None)
    development = all(case.split == "development" for case in cases)
    positive_scores = [hybrid_top_scores[c.id] for c in cases
                       if c.kind == "answer" and c.id in hybrid_top_scores]
    negative_scores = [hybrid_top_scores[c.id] for c in cases
                       if c.kind == "no_answer" and c.id in hybrid_top_scores]
    calibration = {"status": "not_calibrated_on_holdout"}
    if development:
        lowest_positive = min(positive_scores) if positive_scores else None
        highest_negative = max(negative_scores) if negative_scores else None
        separable = (lowest_positive is not None and highest_negative is not None
                     and lowest_positive > highest_negative)
        calibration = {
            "status": "separable" if separable else "overlap_or_missing_labels",
            "lowest_positive_top_score": lowest_positive,
            "highest_no_answer_top_score": highest_negative,
            "suggested_min_top_score": (
                (lowest_positive + highest_negative) / 2 if separable else None
            ),
            "note": "Diagnostic only; threshold is not deployed by this evaluator.",
        }
    return {
        "cases": {"total": len(cases), "positive": sum(c.kind == "answer" for c in cases),
                  "negative": sum(c.kind != "answer" for c in cases)},
        "routes": routes,
        "citations": {"resolved": citation_ok, "total": citation_total,
                      "success_rate": round(citation_ok / citation_total, 4) if citation_total else None},
        "security": {"checks": security_checks, "forbidden_leaks": forbidden_leaks,
                     "leak_case_ids": sorted(leak_case_ids)},
        "no_answer": {"total": no_answer_total,
                      "false_positive_queries": no_answer_false_positives},
        "hybrid_top_scores_by_case": hybrid_top_scores,
        "development_calibration": calibration,
        "checks": {
            "fusion_not_worse_than_best_single": (
                hybrid["recall_at_5"] >= best_single
                if hybrid["recall_at_5"] is not None and best_single is not None else None
            ),
            "citations_100_percent": citation_total > 0 and citation_ok == citation_total,
            "no_forbidden_leak": forbidden_leaks == 0,
            "hybrid_p95_under_2_seconds": (
                hybrid["p95_ms"] <= 2000 if hybrid["p95_ms"] is not None else None
            ),
            "no_answer_rejected": no_answer_total > 0 and no_answer_false_positives == 0,
        },
    }


def _rss_bytes() -> int | None:
    try:
        import psutil
        return psutil.Process().memory_info().rss
    except ImportError:
        if os.name != "nt":
            return None
        import ctypes
        from ctypes import wintypes

        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD), ("page_fault_count", wintypes.DWORD),
                ("peak_working_set_size", ctypes.c_size_t),
                ("working_set_size", ctypes.c_size_t),
                ("quota_peak_paged_pool_usage", ctypes.c_size_t),
                ("quota_paged_pool_usage", ctypes.c_size_t),
                ("quota_peak_non_paged_pool_usage", ctypes.c_size_t),
                ("quota_non_paged_pool_usage", ctypes.c_size_t),
                ("pagefile_usage", ctypes.c_size_t),
                ("peak_pagefile_usage", ctypes.c_size_t),
            ]

        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = (
            wintypes.HANDLE, ctypes.POINTER(ProcessMemoryCounters), wintypes.DWORD,
        )
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
        if not psapi.GetProcessMemoryInfo(
            kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb,
        ):
            return None
        return counters.working_set_size


def _add_document(
    store, owner_id, embedder, title, source_ref, roles, parsed,
    *, created: list[UUID], document_id=None,
):
    inputs = chunk_document(parsed)
    vectors = embedder.embed_many([chunk.text for chunk in inputs])
    chunks = [EmbeddedChunk(**vars(chunk), embedding=tuple(vector))
              for chunk, vector in zip(inputs, vectors)]
    version = store.create_draft(
        owner_id,
        DocumentMetadata(title, "G4 检索验收", "project_doc", source_ref, roles, document_id),
        parsed, chunks,
    )
    if version.document_id not in created:
        created.append(version.document_id)
    store.publish(version.document_id, version.id)
    return version, len(chunks)


def run_real_evaluation(split: str) -> dict:
    password = os.environ.get("G4_TEST_DB_PASSWORD")
    if not password:
        raise RuntimeError("G4_TEST_DB_PASSWORD is required for isolated evaluation")
    if os.environ.get("G4_TEST_DB_PORT", "55445") != "55445":
        raise RuntimeError("evaluation is restricted to isolated loopback port 55445")
    settings = ApiSettings(
        app_db_host="127.0.0.1", app_db_port=55445, app_db_name="ecommerce_app",
        app_db_user="app", app_db_password=password,
        behavior_metric_definitions_path=ROOT / "configs/metrics/behavior-v1.json",
        order_metric_definitions_path=ROOT / "configs/metrics/orders-v1.json",
    )
    store, embedder = KnowledgeStore(settings), Embedder()
    cases = load_cases(split=split)
    created = []
    source_digests = {}
    chunk_count = 0
    memory_before = _rss_bytes()
    try:
        with store._connect() as connection:
            if connection.execute("SELECT count(*) AS n FROM knowledge.documents").fetchone()["n"]:
                raise RuntimeError("isolated evaluation database must have no knowledge documents")
            owner = connection.execute("SELECT id FROM app_users ORDER BY id LIMIT 1").fetchone()
            if owner is None:
                raise RuntimeError("isolated evaluation database has no app user")
            owner_id = owner["id"]
        sources = json.loads(MANIFEST.read_text(encoding="utf-8"))["sources"]
        for item in sources:
            source_ref = item["path"]
            source = ROOT / source_ref
            source_digests[source_ref] = sha256(source.read_bytes()).hexdigest()
            if source.suffix == ".json":
                domain = "behavior" if source.stem == "behavior-v1" else "orders"
                parsed = metric_definition_sections(domain, settings)
            else:
                parsed = parse_document(source.name, source.read_bytes())
            version, count = _add_document(
                store, owner_id, embedder, source.stem, source_ref,
                ("admin", "analyst", "viewer"), parsed, created=created,
            )
            chunk_count += count
        if split == "holdout" or split == "all":
            private, _ = _add_document(
                store, owner_id, embedder, "G4 安全夹具", "tests/fixtures/g4-private.txt",
                ("admin",), parse_document("private.txt", b"PRIVATE_EVAL_MARKER_ALPHA"),
                created=created,
            )
            withdrawn, _ = _add_document(
                store, owner_id, embedder, "G4 撤回夹具", "tests/fixtures/g4-withdrawn.txt",
                ("viewer",), parse_document("withdrawn.txt", b"WITHDRAWN_EVAL_MARKER_BETA"),
                created=created,
            )
            store.withdraw(withdrawn.document_id)
            old, _ = _add_document(
                store, owner_id, embedder, "G4 版本夹具", "tests/fixtures/g4-version.txt",
                ("viewer",), parse_document("version.txt", b"OLD_VERSION_EVAL_MARKER_GAMMA"),
                created=created,
            )
            _add_document(
                store, owner_id, embedder, "G4 版本夹具", "tests/fixtures/g4-version.txt",
                ("viewer",), parse_document("version.txt", b"Current version without old marker"),
                created=created, document_id=old.document_id,
            )
            cases = [
                replace(case, forbidden_document_ids=(private.document_id,))
                if case.kind == "unauthorized" else
                replace(case, forbidden_document_ids=(withdrawn.document_id,))
                if case.kind == "withdrawn" else
                replace(case, forbidden_version_ids=(old.id,))
                if case.kind == "old_version" else case
                for case in cases
            ]
        report = evaluate(cases, KnowledgeSearchService(store, embedder))
        cache = embedder.cache_dir
        cache_bytes = sum(path.stat().st_size for path in cache.rglob("*") if path.is_file()) if cache.exists() else 0
        report["provenance"] = {
            "evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
            "split": split, "fixture_sha256": sha256(FIXTURE.read_bytes()).hexdigest(),
            "source_sha256": source_digests, "source_documents": len(sources),
            "source_chunks": chunk_count, "model": "BAAI/bge-small-zh-v1.5",
            "model_cache_dir": str(cache), "model_cache_bytes": cache_bytes,
            "rss_before_bytes": memory_before, "rss_after_bytes": _rss_bytes(),
            "memory_note": "Process RSS only; not total Docker/Windows memory.",
            "scope_note": "Only isolated G4-A retrieval; security documents are test fixtures.",
        }
        return report
    finally:
        for document_id in reversed(created):
            store.delete(document_id)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split", choices=("development", "holdout"), default="holdout")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    output = args.output.resolve()
    if os.name == "nt" and output.drive.upper() != "D:":
        parser.error("evaluation output must be on D:")
    report = run_real_evaluation(args.split)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output), "checks": report["checks"],
                      "routes": report["routes"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
