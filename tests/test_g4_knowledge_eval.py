import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from uuid import uuid4


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "api"))
sys.path.insert(0, str(ROOT / "scripts"))

from app.config import ApiSettings  # noqa: E402
from app.knowledge_ingest import (  # noqa: E402
    EmbeddingUnavailable, chunk_document, metric_definition_sections, parse_document,
)
from app.knowledge_models import Citation, KnowledgeHit, SearchResult  # noqa: E402
from evaluate_g4_knowledge import _add_document, _rss_bytes, evaluate, load_cases  # noqa: E402


FIXTURE = ROOT / "tests" / "fixtures" / "g4_knowledge_questions.json"


class G4KnowledgeEvaluationTest(unittest.TestCase):
    def test_frozen_cases_are_distinct_and_grounded_in_source_chunks(self):
        raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
        self.assertEqual(len(raw["development"]), 10)
        self.assertEqual(len(raw["holdout"]), 30)
        cases = load_cases(FIXTURE, split="all")
        self.assertEqual(len({case.id for case in cases}), 40)
        self.assertEqual(sum(case.kind == "no_answer" for case in cases if case.split == "development"), 2)
        self.assertEqual(sum(case.kind == "answer" for case in cases if case.split == "holdout"), 24)
        self.assertEqual(sum(case.kind != "answer" for case in cases if case.split == "holdout"), 6)
        for case in cases:
            if case.kind != "answer":
                continue
            source = ROOT / case.source_ref
            if source.suffix == ".json":
                domain = "behavior" if source.stem == "behavior-v1" else "orders"
                parsed = metric_definition_sections(domain, ApiSettings())
            else:
                parsed = parse_document(source.name, source.read_bytes())
            chunks = chunk_document(parsed)
            self.assertTrue(any(
                (case.section is None or chunk.section == case.section)
                and (case.text is None or case.text in chunk.text)
                for chunk in chunks
            ), case.id)

    def test_eval_denominator_and_no_leak(self):
        source_id, private_id = uuid4(), uuid4()
        chunk_id, version_id = uuid4(), uuid4()
        hit = KnowledgeHit(
            chunk_id=chunk_id, document_id=source_id, version_id=version_id,
            section="behavior-v1 / event_count", page=None, text="event_count",
            source_label="项目文档", source_ref="configs/metrics/behavior-v1.json",
            keyword_rank=1, vector_rank=1, score=1.0, locator=f"#chunk-{chunk_id}",
        )

        class FakeStore:
            def resolve_citation(self, current_chunk_id, principal):
                return Citation(
                    chunk_id=current_chunk_id, document_id=source_id,
                    version_id=version_id, section=hit.section, page=None,
                    text=hit.text, source_ref=hit.source_ref,
                )

        class FakeSearch:
            store = FakeStore()

            def search(self, query, principal, limit=5, *, route="hybrid"):
                hits = [hit] if "event_count" in query else []
                return SearchResult(hits=hits, mode={
                    "keyword": "keyword_only", "vector": "vector_only", "hybrid": "hybrid",
                }[route], elapsed_ms=10.0)

        cases = load_cases(FIXTURE, split="holdout")
        selected = [case for case in cases if case.id in {"h25", "h27"}]
        answer = load_cases(FIXTURE, split="development")[0]
        selected.insert(0, answer)
        selected[-1] = selected[-1].with_forbidden_document_ids((private_id,))
        report = evaluate(selected, FakeSearch())
        self.assertEqual(report["routes"]["hybrid"]["denominator"], 1)
        self.assertEqual(report["routes"]["hybrid"]["hits"], 1)
        self.assertEqual(report["security"]["forbidden_leaks"], 0)
        self.assertEqual(report["citations"]["success_rate"], 1.0)

    def test_missing_vector_never_reports_hybrid_recall(self):
        class MissingVector:
            store = None

            def search(self, query, principal, limit=5, *, route="hybrid"):
                if route == "vector":
                    raise EmbeddingUnavailable("cache missing")
                return SearchResult(hits=[], mode="keyword_only", elapsed_ms=1.0)

        report = evaluate(load_cases(FIXTURE, split="development")[:1], MissingVector())
        self.assertFalse(report["routes"]["hybrid"]["available"])
        self.assertIsNone(report["routes"]["hybrid"]["recall_at_5"])
        self.assertFalse(report["routes"]["vector"]["available"])

    def test_security_leak_and_no_answer_are_reported_not_counted_as_recall(self):
        private_id, chunk_id = uuid4(), uuid4()
        leaked = KnowledgeHit(
            chunk_id=chunk_id, document_id=private_id, version_id=uuid4(),
            section="secret", page=None, text="PRIVATE_EVAL_MARKER_ALPHA",
            source_label="项目文档", source_ref="tests/fixtures/g4-private.txt",
            keyword_rank=1, vector_rank=1, score=1.0, locator=f"#chunk-{chunk_id}",
        )

        class LeakingSearch:
            store = None

            def search(self, query, principal, limit=5, *, route="hybrid"):
                return SearchResult(
                    hits=[leaked], mode={"keyword": "keyword_only", "vector": "vector_only",
                                         "hybrid": "hybrid"}[route], elapsed_ms=5.0,
                )

        cases = [case for case in load_cases(FIXTURE, split="holdout")
                 if case.id in {"h25", "h27"}]
        cases = [case.with_forbidden_document_ids((private_id,)) if case.id == "h27" else case
                 for case in cases]
        report = evaluate(cases, LeakingSearch())
        self.assertEqual(report["routes"]["hybrid"]["denominator"], 0)
        self.assertIsNone(report["routes"]["hybrid"]["recall_at_5"])
        self.assertEqual(report["security"]["forbidden_leaks"], 3)
        self.assertEqual(report["no_answer"]["false_positive_queries"], 1)

    def test_draft_is_registered_for_cleanup_before_publish(self):
        document_id = uuid4()

        class FakeStore:
            def create_draft(self, *args):
                return SimpleNamespace(document_id=document_id, id=uuid4())

            def publish(self, *args):
                raise RuntimeError("publish failed")

        class FakeEmbedder:
            def embed_many(self, texts):
                return [[0.0] * 512 for _ in texts]

        created = []
        with self.assertRaisesRegex(RuntimeError, "publish failed"):
            _add_document(
                FakeStore(), 1, FakeEmbedder(), "test", "docs/test.txt", ("viewer",),
                parse_document("test.txt", b"test document"), created=created,
            )
        self.assertEqual(created, [document_id])

    @unittest.skipUnless(sys.platform == "win32", "Windows working-set observation")
    def test_windows_memory_observation_is_available(self):
        self.assertGreater(_rss_bytes(), 0)


if __name__ == "__main__":
    unittest.main()
