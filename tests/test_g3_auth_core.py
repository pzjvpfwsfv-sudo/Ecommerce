import hashlib
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "api"))

from app.auth_crypto import hash_password, verify_password  # noqa: E402
from app.auth_store import AuthStore, StoredSession, StoredUser  # noqa: E402
from app.config import ApiSettings  # noqa: E402


class RecordingCursor:
    def __init__(self, *, one=None, rows=()):
        self.one = one
        self.rows = rows
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, parameters):
        self.calls.append((sql, parameters))

    def fetchone(self):
        return self.one

    def fetchall(self):
        return self.rows


class RecordingConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def cursor(self):
        return self._cursor


class PasswordHashTest(unittest.TestCase):
    def test_hash_uses_a_new_salt_and_verifies_password(self):
        first = hash_password("correct horse")
        second = hash_password("correct horse")

        self.assertNotEqual(first, second)
        self.assertTrue(first.startswith("scrypt$16384$8$1$"))
        self.assertTrue(verify_password("correct horse", first))
        self.assertTrue(verify_password("correct horse", second))

    def test_wrong_password_does_not_verify(self):
        self.assertFalse(verify_password("wrong", hash_password("correct horse")))

    def test_malformed_or_unsupported_encoding_does_not_verify(self):
        valid = hash_password("correct horse")
        fields = valid.split("$")
        malformed = (
            "malformed",
            valid + "$extra",
            valid.replace("scrypt$", "other$", 1),
            valid.replace("$16384$", "$32768$", 1),
            "$".join([*fields[:4], "not-hex", fields[5]]),
            "$".join([*fields[:4], "00", fields[5]]),
            "$".join([*fields[:5], "00"]),
        )
        for encoded in malformed:
            with self.subTest(encoded=encoded):
                self.assertFalse(verify_password("correct horse", encoded))


class AuthStoreTest(unittest.TestCase):
    def setUp(self):
        self.settings = ApiSettings(
            app_db_host="app-postgres", app_db_port=5432,
            app_db_name="ecommerce_app", app_db_user="app",
            app_db_password="test-only",
        )
        self.store = AuthStore(self.settings)
        self.user_row = {
            "id": 7, "username": "alice", "password_hash": "encoded-hash",
            "role": "analyst", "active": True,
        }
        self.expiry = datetime.now(timezone.utc) + timedelta(hours=8)
        self.session_token = "raw-session-token"
        self.token_hash = hashlib.sha256(self.session_token.encode("utf-8")).hexdigest()

    def call_store(self, call, *, one=None, rows=()):
        cursor = RecordingCursor(one=one, rows=rows)
        connection = RecordingConnection(cursor)
        with patch("app.auth_store.psycopg.connect", return_value=connection) as connect:
            result = call()
        return result, cursor.calls, connect.call_args.kwargs

    def test_create_user_normalizes_name_and_uses_app_database(self):
        result, calls, options = self.call_store(
            lambda: self.store.create_user("  ALICE  ", "encoded-hash", "analyst"),
            one=self.user_row,
        )

        self.assertEqual(
            result, StoredUser(7, "alice", "encoded-hash", "analyst", True)
        )
        self.assertEqual(len(calls), 1)
        sql, parameters = calls[0]
        self.assertIn("INSERT INTO app_users", sql)
        self.assertIn("RETURNING", sql)
        self.assertEqual(parameters, ("alice", "encoded-hash", "analyst"))
        self.assertEqual(sql.count("%s"), 3)
        self.assertNotIn("encoded-hash", sql)
        self.assertEqual(
            {key: options[key] for key in ("host", "port", "dbname", "user", "password")},
            {
                "host": "app-postgres", "port": 5432, "dbname": "ecommerce_app",
                "user": "app", "password": "test-only",
            },
        )

    def test_user_lookups_and_listing_map_rows(self):
        by_name, name_calls, _ = self.call_store(
            lambda: self.store.get_user("  ALICE  "), one=self.user_row
        )
        by_id, id_calls, _ = self.call_store(
            lambda: self.store.get_user_by_id(7), one=self.user_row
        )
        listed, list_calls, _ = self.call_store(
            self.store.list_users, rows=[self.user_row]
        )

        expected = StoredUser(7, "alice", "encoded-hash", "analyst", True)
        self.assertEqual((by_name, by_id, listed), (expected, expected, [expected]))
        self.assertIn("FROM app_users", name_calls[0][0])
        self.assertEqual(name_calls[0][1], ("alice",))
        self.assertEqual(id_calls[0][1], (7,))
        self.assertIn("ORDER BY username", list_calls[0][0])
        self.assertIsNone(self.call_store(lambda: self.store.get_user("nobody"))[0])
        self.assertIsNone(self.call_store(lambda: self.store.get_user_by_id(9))[0])

    def test_blank_and_overlong_names_fail_before_database_access(self):
        with patch("app.auth_store.psycopg.connect") as connect:
            for username in ("  ", "x" * 256):
                with self.subTest(username=username):
                    with self.assertRaises(ValueError):
                        self.store.create_user(username, "encoded-hash", "viewer")
                    with self.assertRaises(ValueError):
                        self.store.get_user(username)
            connect.assert_not_called()

    def test_session_create_lookup_and_revoke_use_hash_parameters(self):
        session_row = {
            "token_hash": self.token_hash, "user_id": 7,
            "csrf_token": "csrf-secret", "expires_at": self.expiry,
            "revoked_at": None,
        }
        _, create_calls, _ = self.call_store(
            lambda: self.store.create_session(
                self.session_token, 7, "csrf-secret", self.expiry
            )
        )
        session, get_calls, _ = self.call_store(
            lambda: self.store.get_session(self.session_token), one=session_row
        )
        _, revoke_calls, _ = self.call_store(
            lambda: self.store.revoke_session(self.session_token)
        )

        self.assertEqual(
            session, StoredSession(self.token_hash, 7, "csrf-secret", self.expiry, None)
        )
        self.assertEqual(create_calls[0][1], (self.token_hash, 7, "csrf-secret", self.expiry))
        self.assertEqual(get_calls[0][1], (self.token_hash,))
        self.assertEqual(revoke_calls[0][1], (self.token_hash,))
        self.assertIn("INSERT INTO app_sessions", create_calls[0][0])
        self.assertIn("FROM app_sessions", get_calls[0][0])
        self.assertIn("UPDATE app_sessions", revoke_calls[0][0])
        self.assertIn("revoked_at", revoke_calls[0][0])
        for sql, parameters in (*create_calls, *get_calls, *revoke_calls):
            self.assertNotIn(self.session_token, sql)
            self.assertNotIn(self.session_token, parameters)
        self.assertIsNone(self.call_store(lambda: self.store.get_session(self.session_token))[0])

    def test_hex_shaped_raw_token_is_hashed_before_every_session_query(self):
        raw_token = "a" * 64
        digest = hashlib.sha256(raw_token.encode("utf-8")).hexdigest()
        session_row = {
            "token_hash": digest, "user_id": 7,
            "csrf_token": "csrf-secret", "expires_at": self.expiry,
            "revoked_at": None,
        }

        _, create_calls, _ = self.call_store(
            lambda: self.store.create_session(raw_token, 7, "csrf-secret", self.expiry)
        )
        session, get_calls, _ = self.call_store(
            lambda: self.store.get_session(raw_token), one=session_row
        )
        _, revoke_calls, _ = self.call_store(
            lambda: self.store.revoke_session(raw_token)
        )

        self.assertNotEqual(raw_token, digest)
        self.assertEqual(create_calls[0][1][0], digest)
        self.assertEqual(get_calls[0][1], (digest,))
        self.assertEqual(revoke_calls[0][1], (digest,))
        self.assertEqual(session.token_hash, digest)
        for sql, parameters in (*create_calls, *get_calls, *revoke_calls):
            self.assertNotIn(raw_token, sql)
            self.assertNotIn(raw_token, parameters)

    def test_audit_records_nullable_user_id_with_parameters(self):
        for user_id in (None, 7):
            with self.subTest(user_id=user_id):
                _, calls, _ = self.call_store(
                    lambda: self.store.record_audit(user_id, "login_failed")
                )
                self.assertIn("INSERT INTO auth_audit", calls[0][0])
                self.assertEqual(calls[0][1], (user_id, "login_failed"))

    def test_database_failure_is_not_mapped_to_an_authenticated_user(self):
        with patch(
            "app.auth_store.psycopg.connect", side_effect=RuntimeError("database unavailable")
        ):
            with self.assertRaisesRegex(RuntimeError, "database unavailable"):
                self.store.get_user("alice")
            with self.assertRaisesRegex(RuntimeError, "database unavailable"):
                self.store.get_session(self.session_token)


if __name__ == "__main__":
    unittest.main()
