import os
from dataclasses import replace
from pathlib import Path
import sys
import unittest
from uuid import uuid4

import psycopg


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "api"))

from app.auth_service import Principal  # noqa: E402
from app.config import ApiSettings  # noqa: E402
from app.knowledge_ingest import chunk_document, parse_document  # noqa: E402
from app.knowledge_models import DocumentMetadata, EmbeddedChunk, RevokedCitation  # noqa: E402
from app.knowledge_store import KnowledgeStore  # noqa: E402


@unittest.skipUnless(os.environ.get("G4_TEST_DB_PASSWORD"), "isolated G4 PostgreSQL is not configured")
class G4KnowledgeStoreTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = ApiSettings(
            app_db_host="127.0.0.1", app_db_port=int(os.environ.get("G4_TEST_DB_PORT", "55445")),
            app_db_name="ecommerce_app", app_db_user="app",
            app_db_password=os.environ["G4_TEST_DB_PASSWORD"],
        )
        with psycopg.connect(
            host=cls.settings.app_db_host, port=cls.settings.app_db_port,
            dbname=cls.settings.app_db_name, user=cls.settings.app_db_user,
            password=cls.settings.app_db_password,
        ) as connection:
            cls.owner_id = connection.execute("SELECT id FROM app_users ORDER BY id LIMIT 1").fetchone()[0]

    def setUp(self):
        self.store = KnowledgeStore(self.settings)
        self.created = []
        self.admin = Principal(self.owner_id, "admin", "admin", "csrf")
        self.analyst = Principal(self.owner_id, "analyst", "analyst", "csrf")
        self.viewer = Principal(self.owner_id, "viewer", "viewer", "csrf")

    def tearDown(self):
        for document_id in self.created:
            self.store.delete(document_id)

    def draft(self, body, *, document_id=None, roles=("admin", "analyst", "viewer")):
        parsed = parse_document("guide.txt", body.encode())
        chunks = [EmbeddedChunk(**vars(chunk), embedding=tuple([1.0] + [0.0] * 511))
                  for chunk in chunk_document(parsed)]
        metadata = DocumentMetadata(
            document_id=document_id, title="真实项目资料", category="口径",
            source_type="project_doc", source_ref="docs/graduation/behavior-metrics-api-runbook.md",
            visibility_roles=roles,
        )
        version = self.store.create_draft(self.owner_id, metadata, parsed, chunks)
        if document_id is None:
            self.created.append(version.document_id)
        return version

    def first_chunk(self, version_id):
        with self.store._connect() as connection:
            return connection.execute(
                "SELECT id FROM knowledge.chunks WHERE version_id = %s ORDER BY ordinal LIMIT 1",
                (version_id,),
            ).fetchone()["id"]

    def test_draft_failure_preserves_published_version_and_delete_removes_body(self):
        first = self.draft("原始口径正文")
        first_chunk = self.first_chunk(first.id)
        self.assertEqual(self.store.list_documents("viewer"), [])
        with self.assertRaises(PermissionError):
            self.store.preview(first.document_id, first.id, self.viewer)
        self.assertIn("原始口径", self.store.preview(first.document_id, first.id, self.admin).extracted_text)

        self.store.publish(first.document_id, first.id)
        self.assertEqual(len(self.store.list_documents("viewer")), 1)
        self.assertIn("原始口径", self.store.resolve_citation(first_chunk, self.viewer).text)

        parsed = parse_document("bad.txt", "坏草稿".encode())
        broken = [EmbeddedChunk(**vars(chunk), embedding=(1.0,) * 511)
                  for chunk in chunk_document(parsed)]
        metadata = DocumentMetadata(
            document_id=first.document_id, title="真实项目资料", category="口径",
            source_type="project_doc", source_ref="docs/graduation/behavior-metrics-api-runbook.md",
            visibility_roles=("admin", "analyst", "viewer"),
        )
        with self.assertRaisesRegex(ValueError, "512"):
            self.store.create_draft(self.owner_id, metadata, parsed, broken)
        self.assertIn("原始口径", self.store.resolve_citation(first_chunk, self.viewer).text)

        second = self.draft("修订口径正文", document_id=first.document_id)
        second_chunk = self.first_chunk(second.id)
        self.store.publish(first.document_id, second.id)
        self.assertIsInstance(self.store.resolve_citation(first_chunk, self.viewer), RevokedCitation)
        self.assertIn("修订口径", self.store.resolve_citation(second_chunk, self.viewer).text)
        with self.store._connect() as connection:
            published = connection.execute(
                "SELECT count(*) AS version_count FROM knowledge.versions WHERE document_id = %s AND status = 'published'",
                (first.document_id,),
            ).fetchone()["version_count"]
        self.assertEqual(published, 1)

        self.store.delete(first.document_id)
        self.created.clear()
        self.assertIsInstance(self.store.resolve_citation(second_chunk, self.viewer), RevokedCitation)
        with self.store._connect() as connection:
            self.assertEqual(connection.execute(
                "SELECT count(*) AS version_count FROM knowledge.versions WHERE document_id = %s",
                (first.document_id,),
            ).fetchone()["version_count"], 0)

    def test_roles_and_withdraw_filter_before_preview_and_citation(self):
        version = self.draft("仅分析员可见", roles=("admin", "analyst"))
        chunk_id = self.first_chunk(version.id)
        self.store.publish(version.document_id, version.id)
        self.assertEqual(self.store.list_documents("viewer"), [])
        self.assertEqual(len(self.store.list_documents("analyst")), 1)
        with self.assertRaises(PermissionError):
            self.store.preview(version.document_id, version.id, self.viewer)
        self.assertIsInstance(self.store.resolve_citation(chunk_id, self.viewer), RevokedCitation)
        self.assertIn("仅分析员", self.store.preview(version.document_id, version.id, self.analyst).extracted_text)
        self.store.withdraw(version.document_id)
        self.assertEqual(self.store.list_documents("analyst"), [])
        self.assertIsInstance(self.store.resolve_citation(chunk_id, self.analyst), RevokedCitation)

    def test_existing_document_metadata_cannot_silently_change_before_publish(self):
        first = self.draft("公开版本")
        self.store.publish(first.document_id, first.id)
        parsed = parse_document("guide.txt", "私人草稿".encode())
        chunks = [EmbeddedChunk(**vars(chunk), embedding=tuple([1.0] + [0.0] * 511))
                  for chunk in chunk_document(parsed)]
        changed = DocumentMetadata(
            document_id=first.document_id, title="真实项目资料", category="口径",
            source_type="project_doc", source_ref="docs/graduation/behavior-metrics-api-runbook.md",
            visibility_roles=("admin",),
        )
        with self.assertRaises(ValueError):
            self.store.create_draft(self.owner_id, changed, parsed, chunks)
        self.assertEqual(len(self.store.list_documents("viewer")), 1)

    def test_database_insert_failure_rolls_back_draft_and_preserves_publication(self):
        first = self.draft("稳定的旧正文")
        first_chunk = self.first_chunk(first.id)
        self.store.publish(first.document_id, first.id)
        parsed = parse_document("guide.txt", "新正文".encode())
        valid = chunk_document(parsed)[0]
        invalid = replace(valid, terms=("not\x00valid",))
        metadata = DocumentMetadata(
            document_id=first.document_id, title="真实项目资料", category="口径",
            source_type="project_doc", source_ref="docs/graduation/behavior-metrics-api-runbook.md",
            visibility_roles=("admin", "analyst", "viewer"),
        )
        with self.assertRaises(psycopg.Error):
            self.store.create_draft(
                self.owner_id, metadata, parsed,
                [EmbeddedChunk(**vars(invalid), embedding=tuple([1.0] + [0.0] * 511))],
            )
        self.assertIn("稳定的旧正文", self.store.resolve_citation(first_chunk, self.viewer).text)
        with self.store._connect() as connection:
            count = connection.execute(
                "SELECT count(*) AS version_count FROM knowledge.versions WHERE document_id = %s",
                (first.document_id,),
            ).fetchone()["version_count"]
        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
