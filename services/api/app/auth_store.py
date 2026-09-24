from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Literal

import psycopg
from psycopg.rows import dict_row

from app.config import ApiSettings


Role = Literal["admin", "analyst", "viewer"]
_MAX_USERNAME_LENGTH = 128
_TOKEN_HASH_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class StoredUser:
    id: int
    username: str
    password_hash: str
    role: Role
    active: bool


@dataclass(frozen=True)
class StoredSession:
    token_hash: str
    user_id: int
    csrf_token: str
    expires_at: datetime
    revoked_at: datetime | None


def _normalize_username(username: str) -> str:
    normalized = username.strip().casefold()
    if not normalized or len(normalized) > _MAX_USERNAME_LENGTH:
        raise ValueError("username must contain 1 to 128 characters")
    return normalized


def _validate_token_hash(token_hash: str) -> None:
    if not _TOKEN_HASH_PATTERN.fullmatch(token_hash):
        raise ValueError("token_hash must be a lowercase SHA-256 hex digest")


def _stored_user(row: dict) -> StoredUser:
    return StoredUser(
        id=row["id"], username=row["username"],
        password_hash=row["password_hash"], role=row["role"], active=row["active"],
    )


def _stored_session(row: dict) -> StoredSession:
    return StoredSession(
        token_hash=row["token_hash"], user_id=row["user_id"],
        csrf_token=row["csrf_token"], expires_at=row["expires_at"],
        revoked_at=row["revoked_at"],
    )


class AuthStore:
    def __init__(self, settings: ApiSettings) -> None:
        self._settings = settings

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(
            host=self._settings.app_db_host,
            port=self._settings.app_db_port,
            dbname=self._settings.app_db_name,
            user=self._settings.app_db_user,
            password=self._settings.app_db_password,
            row_factory=dict_row,
            connect_timeout=5,
        )

    def create_user(self, username: str, password_hash: str, role: Role) -> StoredUser:
        normalized = _normalize_username(username)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO app_users (username, password_hash, role) "
                    "VALUES (%s, %s, %s) "
                    "RETURNING id, username, password_hash, role, active",
                    (normalized, password_hash, role),
                )
                row = cursor.fetchone()
        if row is None:
            raise RuntimeError("user creation returned no row")
        return _stored_user(row)

    def get_user(self, username: str) -> StoredUser | None:
        normalized = _normalize_username(username)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id, username, password_hash, role, active "
                    "FROM app_users WHERE username = %s",
                    (normalized,),
                )
                row = cursor.fetchone()
        return _stored_user(row) if row is not None else None

    def get_user_by_id(self, user_id: int) -> StoredUser | None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id, username, password_hash, role, active "
                    "FROM app_users WHERE id = %s",
                    (user_id,),
                )
                row = cursor.fetchone()
        return _stored_user(row) if row is not None else None

    def list_users(self) -> list[StoredUser]:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id, username, password_hash, role, active "
                    "FROM app_users ORDER BY username",
                    (),
                )
                rows = cursor.fetchall()
        return [_stored_user(row) for row in rows]

    def create_session(
        self, token_hash: str, user_id: int, csrf_token: str, expires_at: datetime
    ) -> None:
        _validate_token_hash(token_hash)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO app_sessions (token_hash, user_id, csrf_token, expires_at) "
                    "VALUES (%s, %s, %s, %s)",
                    (token_hash, user_id, csrf_token, expires_at),
                )

    def get_session(self, token_hash: str) -> StoredSession | None:
        _validate_token_hash(token_hash)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT token_hash, user_id, csrf_token, expires_at, revoked_at "
                    "FROM app_sessions WHERE token_hash = %s",
                    (token_hash,),
                )
                row = cursor.fetchone()
        return _stored_session(row) if row is not None else None

    def revoke_session(self, token_hash: str) -> None:
        _validate_token_hash(token_hash)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE app_sessions SET revoked_at = now() "
                    "WHERE token_hash = %s AND revoked_at IS NULL",
                    (token_hash,),
                )

    def record_audit(self, user_id: int | None, action: str) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO auth_audit (user_id, action) VALUES (%s, %s)",
                    (user_id, action),
                )
