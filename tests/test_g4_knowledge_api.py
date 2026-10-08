import sys
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import psycopg
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "api"))

from app.auth_crypto import hash_password  # noqa: E402
from app.auth_service import AuthService  # noqa: E402
from app.config import ApiSettings  # noqa: E402
from app.knowledge_models import (  # noqa: E402
    Citation, DocumentPreview, DocumentVersion, KnowledgeChunkPreview, KnowledgeDocument,
    KnowledgeHit, RevokedCitation, SearchResult,
)
from app.main import create_app  # noqa: E402
from app.knowledge_search import KnowledgeSearchService  # noqa: E402
from app.knowledge_store import KnowledgeStore  # noqa: E402
from tests.test_g3_auth_api import MemoryAuthStore  # noqa: E402


class FakeKnowledgeStore:
    def __init__(self):
        self.document_id = uuid4()
        self.version_id = uuid4()
        self.chunk_id = uuid4()
        self.created = []
        self.published = False
        self.withdrawn = False
        self.fail = False

    def _check(self):
        if self.fail:
            raise psycopg.OperationalError("database offline")

    def create_draft(self, owner_id, metadata, parsed, chunks):
        self._check()
        self.created.append((owner_id, metadata, parsed, chunks))
        return DocumentVersion(self.version_id, self.document_id, 1, "draft", datetime.now(timezone.utc), None)

    def publish(self, document_id, version_id):
        self._check()
        self.published = True
        return DocumentVersion(version_id, document_id, 1, "published", datetime.now(timezone.utc), datetime.now(timezone.utc))

    def withdraw(self, document_id):
        self._check()
        self.withdrawn = True

    def delete(self, document_id):
        self._check()
        self.withdrawn = True

    def list_documents(self, role):
        self._check()
        if role != "admin" and (not self.published or self.withdrawn):
            return []
        return [KnowledgeDocument(
            self.document_id, "订单说明", "指标", "external", "https://example.org/guide",
            ("admin", "viewer"), self.version_id if self.published and not self.withdrawn else None,
        )]

    def list_versions(self, document_id, principal):
        self._check()
        if principal.role != "admin" and (not self.published or self.withdrawn):
            return []
        return [DocumentVersion(self.version_id, self.document_id, 1, "published" if self.published else "draft", datetime.now(timezone.utc), None)]

    def preview(self, document_id, version_id, principal):
        self._check()
        if principal.role != "admin" and (not self.published or self.withdrawn):
            raise PermissionError("unavailable")
        return DocumentPreview(document_id, version_id, "原文 <script>alert(1)</script>", (
            KnowledgeChunkPreview(self.chunk_id, 0, "口径", None, "metric_run_id 口径"),
        ))

    def resolve_citation(self, chunk_id, principal):
        self._check()
        if self.withdrawn:
            return RevokedCitation(chunk_id)
        return Citation(chunk_id, self.document_id, self.version_id, "口径", None, "metric_run_id 口径", "https://example.org/guide")


class FakeEmbedder:
    def embed_many(self, texts):
        return [[1.0] + [0.0] * 511 for _ in texts]


class FakeSearch:
    def __init__(self, store):
        self.store = store

    def search(self, query, principal, limit=5):
        self.store._check()
        if not self.store.published or self.store.withdrawn:
            return SearchResult([], "hybrid", 2.5)
        return SearchResult([KnowledgeHit(
            self.store.chunk_id, self.store.document_id, self.store.version_id,
            "口径", None, "metric_run_id 口径", "外部来源", "https://example.org/guide",
            1, 1, .03, "/knowledge/documents/preview",
        )], "hybrid", 2.5)


class G4KnowledgeApiTest(unittest.TestCase):
    def setUp(self):
        auth_store = MemoryAuthStore()
        auth_store.create_user("admin", hash_password("admin-password"), "admin")
        auth_store.create_user("viewer", hash_password("viewer-password"), "viewer")
        self.store = FakeKnowledgeStore()
        self.client = TestClient(create_app(
            auth_service=AuthService(auth_store), knowledge_store=self.store,
            knowledge_search=FakeSearch(self.store), knowledge_embedder=FakeEmbedder(),
        ))

    def login(self, username="admin"):
        response = self.client.post("/api/v1/auth/login", json={
            "username": username, "password": f"{username}-password",
        })
        self.assertEqual(response.status_code, 200)
        return {"X-CSRF-Token": response.json()["csrf_token"]}

    def upload(self, headers=None, filename="guide.md", content=b"# Definition\nmetric_run_id defines publication", **fields):
        body = {
            "title": "订单说明", "category": "指标", "source_type": "external",
            "source_ref": "https://example.org/guide", "visibility_roles": "admin,viewer",
            **fields,
        }
        return self.client.post(
            "/api/v1/knowledge/documents", headers=headers or {}, data=body,
            files={"file": (filename, content)},
        )

    def test_auth_role_and_csrf_guard_all_writes(self):
        self.assertEqual(self.client.get("/api/v1/knowledge/documents").status_code, 401)
        self.assertEqual(self.client.get("/api/v1/knowledge/search?q=metric_run_id").status_code, 401)
        self.assertEqual(self.upload().status_code, 401)
        viewer_csrf = self.login("viewer")
        self.assertEqual(self.upload(viewer_csrf).status_code, 403)
        self.assertEqual(self.client.post("/api/v1/knowledge/import-metrics", json={"domain": "orders"}, headers=viewer_csrf).status_code, 403)
        self.assertEqual(self.client.post(f"/api/v1/knowledge/documents/{self.store.document_id}/withdraw", headers=viewer_csrf).status_code, 403)
        self.assertEqual(self.client.delete(f"/api/v1/knowledge/documents/{self.store.document_id}", headers=viewer_csrf).status_code, 403)
        self.login()
        self.assertEqual(self.upload().status_code, 403)
        self.assertEqual(self.client.post("/api/v1/knowledge/import-metrics", json={"domain": "orders"}).status_code, 403)
        self.assertEqual(self.client.post(f"/api/v1/knowledge/documents/{self.store.document_id}/versions/{self.store.version_id}/publish").status_code, 403)
        self.assertFalse(self.store.created)

    def test_invalid_uploads_are_422_without_draft(self):
        csrf = self.login()
        for response in (
            self.upload(csrf, filename="fake.pdf", content=b"not a PDF"),
            self.upload(csrf, source_ref="javascript:alert(1)"),
            self.upload(csrf, source_type="project_doc", source_ref="docs/graduation/olist-order-domain-runbook.md"),
            self.upload(csrf, content=b"x" * (10 * 1024 * 1024 + 1)),
        ):
            self.assertEqual(response.status_code, 422)
        self.assertFalse(self.store.created)

    def test_upload_publish_search_preview_and_revocation(self):
        csrf = self.login()
        draft = self.upload(csrf)
        self.assertEqual(draft.status_code, 201)
        self.assertEqual(draft.json()["status"], "draft")
        self.assertEqual(len(self.store.created[0][3]), 1)
        viewer_csrf = self.login("viewer")
        self.assertEqual(self.client.get("/api/v1/knowledge/documents").json(), [])
        self.assertEqual(self.client.get(f"/api/v1/knowledge/documents/{self.store.document_id}/versions/{self.store.version_id}").status_code, 404)
        csrf = self.login()
        path = f"/api/v1/knowledge/documents/{self.store.document_id}/versions/{self.store.version_id}"
        self.assertEqual(self.client.post(path + "/publish", headers=csrf).status_code, 200)
        self.login("viewer")
        self.assertEqual(len(self.client.get("/api/v1/knowledge/documents").json()), 1)
        self.assertEqual(len(self.client.get(f"/api/v1/knowledge/documents/{self.store.document_id}/versions").json()), 1)
        self.assertEqual(self.client.get(path).status_code, 200)
        result = self.client.get("/api/v1/knowledge/search?q=metric_run_id").json()
        self.assertEqual(result["mode"], "hybrid")
        self.assertEqual(result["hits"][0]["source_label"], "外部来源")
        csrf = self.login()
        self.assertEqual(self.client.post(f"/api/v1/knowledge/documents/{self.store.document_id}/withdraw", headers=csrf).status_code, 200)
        self.login("viewer")
        self.assertEqual(self.client.get(path).status_code, 404)
        citation = self.client.get(f"/api/v1/knowledge/citations/{self.store.chunk_id}").json()
        self.assertEqual(citation, {"chunk_id": str(self.store.chunk_id), "status": "revoked"})

    def test_database_outage_returns_503(self):
        self.login()
        self.store.fail = True
        self.assertEqual(self.client.get("/api/v1/knowledge/documents").status_code, 503)
        self.assertEqual(self.client.get("/api/v1/knowledge/search?q=order").status_code, 503)

    def test_short_embedding_batch_cannot_create_partial_draft(self):
        class ShortEmbedder:
            def embed_many(self, texts):
                return []
        auth_service = self.client.app.state.auth_service
        self.client = TestClient(create_app(
            auth_service=auth_service, knowledge_store=self.store,
            knowledge_search=FakeSearch(self.store), knowledge_embedder=ShortEmbedder(),
        ))
        csrf = self.login()
        response = self.upload(csrf)
        self.assertEqual(response.status_code, 422)
        self.assertFalse(self.store.created)


@unittest.skipUnless(os.environ.get("G4_TEST_DB_PASSWORD"), "isolated G4 PostgreSQL is not configured")
class G4KnowledgeApiDbTest(unittest.TestCase):
    def setUp(self):
        settings = ApiSettings(
            app_db_host="127.0.0.1", app_db_port=int(os.environ.get("G4_TEST_DB_PORT", "55445")),
            app_db_name="ecommerce_app", app_db_user="app",
            app_db_password=os.environ["G4_TEST_DB_PASSWORD"],
        )
        self.store = KnowledgeStore(settings)
        with self.store._connect() as connection:
            owner_id = connection.execute("SELECT id FROM app_users ORDER BY id LIMIT 1").fetchone()["id"]
        auth_store = MemoryAuthStore()
        auth_store.next_id = owner_id
        auth_store.create_user("admin", hash_password("admin-password"), "admin")
        auth_store.create_user("viewer", hash_password("viewer-password"), "viewer")
        embedder = FakeEmbedder()
        self.client = TestClient(create_app(
            settings=settings, auth_service=AuthService(auth_store), knowledge_store=self.store,
            knowledge_search=KnowledgeSearchService(self.store, embedder), knowledge_embedder=embedder,
        ))
        self.document_id = None

    def tearDown(self):
        if self.document_id is not None:
            self.store.delete(self.document_id)

    def login(self, username):
        response = self.client.post("/api/v1/auth/login", json={
            "username": username, "password": f"{username}-password",
        })
        self.assertEqual(response.status_code, 200)
        return {"X-CSRF-Token": response.json()["csrf_token"]}

    def test_real_metric_import_to_revoked_citation_over_http(self):
        csrf = self.login("admin")
        created = self.client.post(
            "/api/v1/knowledge/import-metrics", json={"domain": "orders"}, headers=csrf,
        )
        self.assertEqual(created.status_code, 201, created.text)
        self.document_id = created.json()["document_id"]
        version_id = created.json()["id"]
        self.login("viewer")
        self.assertNotIn(self.document_id, [item["id"] for item in self.client.get("/api/v1/knowledge/documents").json()])
        self.login("admin")
        self.assertEqual(self.client.post(
            f"/api/v1/knowledge/documents/{self.document_id}/versions/{version_id}/publish",
            headers=csrf,
        ).status_code, 403)
        csrf = self.login("admin")
        self.assertEqual(self.client.post(
            f"/api/v1/knowledge/documents/{self.document_id}/versions/{version_id}/publish",
            headers=csrf,
        ).status_code, 200)
        self.login("viewer")
        result = self.client.get("/api/v1/knowledge/search?q=order_count")
        self.assertEqual(result.status_code, 200, result.text)
        self.assertTrue(result.json()["hits"])
        hit = result.json()["hits"][0]
        self.assertEqual(hit["document_id"], self.document_id)
        self.assertEqual(self.client.get(
            f"/api/v1/knowledge/documents/{self.document_id}/versions/{version_id}"
        ).status_code, 200)
        self.assertEqual(self.client.get(f"/api/v1/knowledge/citations/{hit['chunk_id']}").status_code, 200)
        csrf = self.login("admin")
        self.assertEqual(self.client.post(
            f"/api/v1/knowledge/documents/{self.document_id}/withdraw", headers=csrf,
        ).status_code, 200)
        self.login("viewer")
        self.assertEqual(self.client.get(f"/api/v1/knowledge/citations/{hit['chunk_id']}").json()["status"], "revoked")


if __name__ == "__main__":
    unittest.main()
