from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hmac
import secrets

import psycopg
from fastapi import Depends, HTTPException, Request, status

from app.auth_crypto import hash_password, verify_password
from app.auth_store import AuthStore, Role, StoredUser


SESSION_COOKIE = "ecom_session"
SESSION_MAX_AGE_SECONDS = 8 * 3600


@dataclass(frozen=True)
class Principal:
    id: int
    username: str
    role: Role
    csrf_token: str


@dataclass(frozen=True)
class LoginResult:
    session_token: str
    public_user: dict[str, object]


def _public_user(user: StoredUser, csrf_token: str | None = None) -> dict[str, object]:
    result: dict[str, object] = {
        "id": user.id, "username": user.username, "role": user.role,
    }
    if csrf_token is not None:
        result["csrf_token"] = csrf_token
    return result


def _unavailable() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="authentication service is temporarily unavailable",
    )


def _unauthorized() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="authentication required",
    )


class AuthService:
    def __init__(self, store: AuthStore) -> None:
        self.store = store

    def login(self, username: str, password: str) -> LoginResult:
        try:
            user = self.store.get_user(username)
        except ValueError:
            user = None
        except Exception:
            raise _unavailable() from None
        if user is None or not user.active or not verify_password(password, user.password_hash):
            try:
                self.store.record_audit(user.id if user else None, "login_failed")
            except Exception:
                raise _unavailable() from None
            raise HTTPException(status_code=401, detail="invalid credentials")

        session_token = secrets.token_urlsafe(32)
        csrf_token = secrets.token_urlsafe(32)
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=SESSION_MAX_AGE_SECONDS)
        try:
            self.store.create_session(session_token, user.id, csrf_token, expires_at)
            self.store.record_audit(user.id, "login_success")
        except Exception:
            raise _unavailable() from None
        return LoginResult(session_token, _public_user(user, csrf_token))

    def resolve(self, session_token: str) -> Principal:
        try:
            session = self.store.get_session(session_token)
            if session is None:
                raise _unauthorized()
            if session.revoked_at is not None or session.expires_at <= datetime.now(timezone.utc):
                raise _unauthorized()
            user = self.store.get_user_by_id(session.user_id)
        except HTTPException:
            raise
        except Exception:
            raise _unavailable() from None
        if user is None or not user.active:
            raise _unauthorized()
        return Principal(user.id, user.username, user.role, session.csrf_token)

    def logout(self, session_token: str, principal: Principal) -> None:
        try:
            self.store.revoke_session(session_token)
            self.store.record_audit(principal.id, "logout")
        except Exception:
            raise _unavailable() from None

    def list_users(self) -> list[dict[str, object]]:
        try:
            return [_public_user(user) for user in self.store.list_users()]
        except Exception:
            raise _unavailable() from None

    def create_user(self, username: str, password: str, role: Role) -> dict[str, object]:
        normalized = username.strip().casefold()
        if not normalized or len(normalized) > 128:
            raise HTTPException(status_code=422, detail="invalid username")
        if len(password) < 12:
            raise HTTPException(status_code=422, detail="password is too short")
        try:
            if self.store.get_user(normalized) is not None:
                raise HTTPException(status_code=409, detail="username already exists")
        except HTTPException:
            raise
        except Exception:
            raise _unavailable() from None
        try:
            user = self.store.create_user(normalized, hash_password(password), role)
        except (psycopg.errors.UniqueViolation, ValueError):
            raise HTTPException(status_code=409, detail="username already exists") from None
        except Exception:
            raise _unavailable() from None
        try:
            self.store.record_audit(user.id, "user_created")
        except Exception:
            raise _unavailable() from None
        return _public_user(user)


def require_principal(request: Request) -> Principal:
    session_token = request.cookies.get(SESSION_COOKIE)
    if not session_token:
        raise _unauthorized()
    return request.app.state.auth_service.resolve(session_token)


def require_analyst(principal: Principal = Depends(require_principal)) -> Principal:
    if principal.role not in {"analyst", "admin"}:
        raise HTTPException(status_code=403, detail="insufficient role")
    return principal


def require_admin(principal: Principal = Depends(require_principal)) -> Principal:
    if principal.role != "admin":
        raise HTTPException(status_code=403, detail="insufficient role")
    return principal


def require_csrf(
    request: Request, principal: Principal = Depends(require_principal)
) -> Principal:
    candidate = request.headers.get("X-CSRF-Token", "")
    if not candidate or not hmac.compare_digest(candidate, principal.csrf_token):
        raise HTTPException(status_code=403, detail="invalid CSRF token")
    return principal
