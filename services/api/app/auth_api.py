from __future__ import annotations

from typing import Literal
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field

from app.auth_service import (
    Principal,
    SESSION_COOKIE,
    SESSION_MAX_AGE_SECONDS,
    require_admin,
    require_csrf,
    require_principal,
)


router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


class LoginBody(BaseModel):
    username: str
    password: str


class CreateUserBody(BaseModel):
    username: str
    password: str = Field(min_length=12)
    role: Literal["admin", "analyst", "viewer"]


def _check_login_origin(request: Request) -> None:
    origin = request.headers.get("Origin")
    if not origin:
        return
    parsed = urlsplit(origin)
    expected = urlsplit(str(request.base_url))
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.scheme != expected.scheme
        or parsed.netloc != expected.netloc
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise HTTPException(status_code=403, detail="invalid origin")


@router.post("/login")
def login(body: LoginBody, request: Request, response: Response) -> dict[str, object]:
    _check_login_origin(request)
    result = request.app.state.auth_service.login(body.username, body.password)
    response.set_cookie(
        SESSION_COOKIE,
        result.session_token,
        max_age=SESSION_MAX_AGE_SECONDS,
        httponly=True,
        samesite="lax",
        secure=request.url.scheme == "https",
        path="/",
    )
    return result.public_user


@router.get("/me")
def me(principal: Principal = Depends(require_principal)) -> dict[str, object]:
    return {
        "id": principal.id, "username": principal.username,
        "role": principal.role, "csrf_token": principal.csrf_token,
    }


@router.post("/logout", dependencies=[Depends(require_csrf)])
def logout(request: Request, response: Response, principal: Principal = Depends(require_principal)) -> dict[str, str]:
    request.app.state.auth_service.logout(request.cookies[SESSION_COOKIE], principal)
    response.delete_cookie(SESSION_COOKIE, path="/", httponly=True, samesite="lax")
    return {"status": "ok"}


@router.get("/users")
def list_users(request: Request, _principal: Principal = Depends(require_admin)) -> list[dict[str, object]]:
    return request.app.state.auth_service.list_users()


@router.post("/users", status_code=201, dependencies=[Depends(require_csrf)])
def create_user(
    body: CreateUserBody, request: Request, _principal: Principal = Depends(require_admin)
) -> dict[str, object]:
    return request.app.state.auth_service.create_user(body.username, body.password, body.role)
