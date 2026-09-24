import hashlib
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "api"))

from app.auth_crypto import hash_password, verify_password  # noqa: E402
from app.auth_store import StoredSession, StoredUser  # noqa: E402
from app.main import create_app  # noqa: E402


class MemoryAuthStore:
    def __init__(self):
        self.users = {}
        self.sessions = {}
        self.actions = []
        self.fail = False
        self.next_id = 1

    def _check(self):
        if self.fail:
            raise RuntimeError("database unavailable")

    def create_user(self, username, password_hash, role):
        self._check()
        username = username.strip().casefold()
        if username in self.users:
            raise ValueError("duplicate username")
        user = StoredUser(self.next_id, username, password_hash, role, True)
        self.next_id += 1
        self.users[username] = user
        return user

    def get_user(self, username):
        self._check()
        return self.users.get(username.strip().casefold())

    def get_user_by_id(self, user_id):
        self._check()
        return next((user for user in self.users.values() if user.id == user_id), None)

    def list_users(self):
        self._check()
        return list(self.users.values())

    def create_session(self, raw_token, user_id, csrf_token, expires_at):
        self._check()
        digest = hashlib.sha256(raw_token.encode()).hexdigest()
        self.sessions[digest] = StoredSession(digest, user_id, csrf_token, expires_at, None)

    def get_session(self, raw_token):
        self._check()
        return self.sessions.get(hashlib.sha256(raw_token.encode()).hexdigest())

    def revoke_session(self, raw_token):
        self._check()
        digest = hashlib.sha256(raw_token.encode()).hexdigest()
        session = self.sessions.get(digest)
        if session:
            self.sessions[digest] = StoredSession(
                digest, session.user_id, session.csrf_token, session.expires_at,
                datetime.now(timezone.utc),
            )

    def record_audit(self, user_id, action):
        self._check()
        self.actions.append((user_id, action))


class G3AuthApiTest(unittest.TestCase):
    def setUp(self):
        from app.auth_service import AuthService

        self.store = MemoryAuthStore()
        self.admin = self.store.create_user("admin", hash_password("admin-password"), "admin")
        self.analyst = self.store.create_user(
            "analyst", hash_password("analyst-password"), "analyst"
        )
        self.viewer = self.store.create_user("viewer", hash_password("viewer-password"), "viewer")
        self.client = TestClient(create_app(auth_service=AuthService(self.store)))

    def login(self, username="admin", password="admin-password"):
        return self.client.post(
            "/api/v1/auth/login", json={"username": username, "password": password},
            headers={"Origin": "http://testserver"},
        )

    def test_login_cookie_and_me_have_no_password_or_session_token_in_body(self):
        response = self.login()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["role"], "admin")
        self.assertIn("csrf_token", response.json())
        cookie = response.headers["set-cookie"]
        self.assertIn("ecom_session=", cookie)
        self.assertIn("HttpOnly", cookie)
        self.assertIn("SameSite=lax", cookie)
        self.assertIn("Max-Age=28800", cookie)
        self.assertNotIn("Secure", cookie)
        raw_token = self.client.cookies.get("ecom_session")
        self.assertNotIn(raw_token, response.text)
        self.assertNotIn("admin-password", response.text)
        self.assertNotIn(raw_token, self.store.sessions)
        self.assertEqual(self.client.get("/api/v1/auth/me").json(), response.json())

    def test_wrong_credentials_and_untrusted_origin_fail(self):
        self.assertEqual(self.login(password="wrong-password").status_code, 401)
        self.assertEqual(
            self.client.post(
                "/api/v1/auth/login", json={"username": "admin", "password": "admin-password"},
                headers={"Origin": "https://evil.example"},
            ).status_code,
            403,
        )
        self.assertFalse(self.client.cookies.get("ecom_session"))

    def test_https_login_sets_secure_cookie(self):
        secure_client = TestClient(self.client.app, base_url="https://testserver")
        response = secure_client.post(
            "/api/v1/auth/login",
            json={"username": "admin", "password": "admin-password"},
            headers={"Origin": "https://testserver"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("Secure", response.headers["set-cookie"])

    def test_missing_revoked_and_expired_sessions_fail(self):
        self.assertEqual(self.client.get("/api/v1/auth/me").status_code, 401)
        self.login()
        token = self.client.cookies.get("ecom_session")
        digest = hashlib.sha256(token.encode()).hexdigest()
        session = self.store.sessions[digest]
        self.store.sessions[digest] = StoredSession(
            digest, session.user_id, session.csrf_token,
            datetime.now(timezone.utc) - timedelta(seconds=1), None,
        )
        self.assertEqual(self.client.get("/api/v1/auth/me").status_code, 401)
        self.store.sessions[digest] = session
        self.store.revoke_session(token)
        self.assertEqual(self.client.get("/api/v1/auth/me").status_code, 401)

    def test_inactive_user_fails_and_store_outage_is_503(self):
        self.login()
        self.store.users["admin"] = StoredUser(
            self.admin.id, "admin", self.admin.password_hash, "admin", False
        )
        self.assertEqual(self.client.get("/api/v1/auth/me").status_code, 401)
        self.store.fail = True
        self.assertEqual(self.client.get("/api/v1/auth/me").status_code, 503)
        self.assertEqual(self.login().status_code, 503)

    def test_logout_requires_csrf_and_revokes(self):
        login = self.login()
        self.assertEqual(self.client.post("/api/v1/auth/logout").status_code, 403)
        self.assertEqual(
            self.client.post(
                "/api/v1/auth/logout", headers={"X-CSRF-Token": "wrong"}
            ).status_code,
            403,
        )
        token = self.client.cookies.get("ecom_session")
        response = self.client.post(
            "/api/v1/auth/logout",
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.client.get("/api/v1/auth/me").status_code, 401)
        self.client.cookies.set("ecom_session", token)
        self.assertEqual(self.client.get("/api/v1/auth/me").status_code, 401)

    def test_role_matrix_for_user_management(self):
        body = {"username": "new-user", "password": "long-password", "role": "viewer"}
        for role, password in (("viewer", "viewer-password"), ("analyst", "analyst-password")):
            with self.subTest(role=role):
                login = self.login(role, password)
                self.assertEqual(self.client.get("/api/v1/auth/users").status_code, 403)
                self.assertEqual(
                    self.client.post(
                        "/api/v1/auth/users", json=body,
                        headers={"X-CSRF-Token": login.json()["csrf_token"]},
                    ).status_code,
                    403,
                )

    def test_admin_create_user_hashes_password_and_handles_duplicate(self):
        login = self.login()
        csrf = {"X-CSRF-Token": login.json()["csrf_token"]}
        body = {"username": "  NEW-USER  ", "password": "long-password", "role": "viewer"}
        self.assertEqual(self.client.post("/api/v1/auth/users", json=body).status_code, 403)
        self.assertEqual(
            self.client.post("/api/v1/auth/users", json=body, headers=csrf).status_code,
            201,
        )
        self.assertTrue(verify_password("long-password", self.store.users["new-user"].password_hash))
        self.assertNotEqual(self.store.users["new-user"].password_hash, "long-password")
        self.assertEqual(
            self.client.post("/api/v1/auth/users", json=body, headers=csrf).status_code,
            409,
        )
        body["password"] = "short"
        self.assertEqual(
            self.client.post("/api/v1/auth/users", json=body, headers=csrf).status_code,
            422,
        )
        users = self.client.get("/api/v1/auth/users").json()
        self.assertTrue(any(user["username"] == "new-user" for user in users))
        self.assertNotIn("password_hash", str(users))

    def test_health_stays_public(self):
        self.assertEqual(self.client.get("/health").status_code, 200)


if __name__ == "__main__":
    unittest.main()
