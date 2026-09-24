# G3 Identity And Access Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the existing FastAPI service a separate business database, real login, server-side sessions, and backend role enforcement before the workbench consumes it.

**Architecture:** Add an isolated `app-postgres` service and a small auth layer without moving lakehouse metadata. Existing metrics and analysis handlers keep their response contracts; route dependencies reject anonymous or unauthorized requests before handlers run. The frontend plan `2026-09-25-g3-visual-workbench-implementation.md` consumes this auth contract.

**Tech Stack:** Python 3.12, FastAPI 0.115.0, PostgreSQL 16, psycopg 3, Python standard-library `hashlib.scrypt`, `unittest`, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-09-25-graduation-g3-visual-workbench-design.md`

## Global Constraints

- Olist `orders-v1` and REES46 `behavior-v1` remain separate; do not change their metric formulas or response bodies.
- `app-postgres` has its own role and volume; do not reuse `metastore-postgres` or touch existing lakehouse data.
- No default anonymous bypass in real runtime. `/health` and `/ready` remain nonsensitive probes; every metric and analysis route requires backend auth.
- Session cookie is HttpOnly and SameSite=Lax, with an eight-hour expiry; authenticated POST requests require `X-CSRF-Token`. Secure is true behind HTTPS and false only for local HTTP development.
- Roles: `admin`, `analyst`, `viewer`. All read metrics; admin/analyst call analysis; only admin manages users. Knowledge permissions arrive in G4.
- Existing API tests use explicit test-auth injection; do not add an environment switch that disables protection in normal runtime.
- On this Windows host use PowerShell, keep Docker data on the existing D-drive Docker installation, and never commit raw credentials.

## Review Focus

1. Missing, malformed, expired, or revoked cookies return 401 before metric handlers run; Tasks 3-4 test this.
2. Direct calls to old `/metrics/*`, `/api/v1/*`, and `/analysis/*` routes cannot bypass auth; Task 4 tests this.
3. A valid cookie with absent/wrong CSRF token on POST returns 403; Tasks 3-4 test this.
4. A viewer cannot analyze or manage users; an analyst cannot manage users; Tasks 3-4 test this.
5. If app PostgreSQL is down, login and protected requests fail closed with 503, never serve sample data; Tasks 3 and 5 test this.

## File Map

- `infra/docker-compose.yml`, `infra/.env.example`, `infra/compose/app-postgres/init/001_auth.sql`: isolated database service and schema.
- `services/api/app/config.py`, `services/api/requirements.txt`: database settings and psycopg dependency.
- `services/api/app/auth_crypto.py`: password hash and verification.
- `services/api/app/auth_store.py`: parameterized PostgreSQL user, session, and audit persistence.
- `services/api/app/auth_service.py`: credential checks, sessions, role rules, and FastAPI dependencies.
- `services/api/app/auth_api.py`: login, current user, logout, and admin user endpoints.
- `services/api/app/main.py`: mount auth router and guard existing handlers without changing metric contracts.
- `scripts/bootstrap_g3_admin.py`, `docs/graduation/g3-auth-runbook.md`: one-time admin bootstrap and operational verification.
- `tests/test_g3_auth_config.py`, `tests/test_g3_auth_core.py`, `tests/test_g3_auth_api.py`, `tests/api_auth_helpers.py` plus five existing API test files: regression coverage.

---

### Task 1: Isolated Database And Configuration

**Files:**
- Create: `infra/compose/app-postgres/init/001_auth.sql`, `tests/test_g3_auth_config.py`
- Modify: `infra/docker-compose.yml`, `infra/.env.example`, `services/api/app/config.py`, `services/api/requirements.txt`

**Interfaces:**
- Produces: `ApiSettings.app_db_host: str`, `app_db_port: int`, `app_db_name: str`, `app_db_user: str`, `app_db_password: str`; Compose service `app-postgres` on `platform-net`.
- Database tables: `app_users(id, username, password_hash, role, active, created_at)`, `app_sessions(token_hash, user_id, csrf_token, expires_at, revoked_at)`, `auth_audit(id, user_id, action, occurred_at)`.

- [ ] **Step 1: Write failing configuration and schema tests.**

~~~python
def test_app_database_is_not_metastore_database(self):
    values = load_settings({
        "APP_DB_HOST": "app-postgres", "APP_DB_PORT": "5432",
        "APP_DB_NAME": "ecommerce_app", "APP_DB_USER": "app",
        "APP_DB_PASSWORD": "unit-secret",
    })
    self.assertEqual(values.app_db_host, "app-postgres")
    self.assertEqual(values.app_db_name, "ecommerce_app")
    schema = (ROOT / "infra/compose/app-postgres/init/001_auth.sql").read_text("utf-8")
    self.assertIn("CREATE TABLE IF NOT EXISTS app_sessions", schema)
    self.assertIn("UNIQUE", schema)
~~~

- [ ] **Step 2: Run `python -m unittest tests.test_g3_auth_config -v`; expect import/file assertion failure.**
- [ ] **Step 3: Add a separate Compose service and exact schema; pass database fields separately rather than interpolating a password into a URL.**

~~~sql
CREATE TABLE IF NOT EXISTS app_users (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  username TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('admin','analyst','viewer')),
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS app_sessions (
  token_hash CHAR(64) PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES app_users(id),
  csrf_token TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS auth_audit (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  user_id BIGINT REFERENCES app_users(id),
  action TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
~~~

- [ ] **Step 4: Add `psycopg[binary]==3.2.3`, wire `APP_DB_*` through `load_settings` and Compose, then run `python -m unittest tests.test_g3_auth_config -v` and `docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile serving config --quiet`; expect exit 0.**
- [ ] **Step 5: Commit this task's files with `git commit -m "feat: add isolated G3 app database"`.**

### Task 2: Password And Session Persistence

**Files:**
- Create: `services/api/app/auth_crypto.py`, `services/api/app/auth_store.py`, `tests/test_g3_auth_core.py`

**Interfaces:**
- Produces: `hash_password(password: str) -> str`, `verify_password(password: str, encoded: str) -> bool`.
- Produces: `Role = Literal["admin","analyst","viewer"]` in `auth_store.py`, `StoredUser(id: int, username: str, password_hash: str, role: Role, active: bool)`, and `StoredSession(token_hash: str, user_id: int, csrf_token: str, expires_at: datetime, revoked_at: datetime | None)`. `AuthStore(settings: ApiSettings)` exposes `create_user(username: str, password_hash: str, role: Role) -> StoredUser`, `get_user(username: str) -> StoredUser | None`, `get_user_by_id(user_id: int) -> StoredUser | None`, `list_users() -> list[StoredUser]`, `create_session(token_hash: str, user_id: int, csrf_token: str, expires_at: datetime) -> None`, `get_session(token_hash: str) -> StoredSession | None`, `revoke_session(token_hash: str) -> None`, and `record_audit(user_id: int | None, action: str) -> None`. Store the session token only as a SHA-256 hash; store the separate CSRF token so `/me` can return it after a page reload.

- [ ] **Step 1: Write failing tests for salted nonrepeatable hashes, correct/wrong passwords, invalid encoding, and parameterized SQL store calls.**

~~~python
def test_password_hash_is_salted(self):
    first, second = hash_password("correct horse"), hash_password("correct horse")
    self.assertNotEqual(first, second)
    self.assertTrue(verify_password("correct horse", first))
    self.assertFalse(verify_password("wrong", first))
    self.assertFalse(verify_password("correct horse", "malformed"))
~~~

- [ ] **Step 2: Run `python -m unittest tests.test_g3_auth_core -v`; expect import failure.**
- [ ] **Step 3: Implement scrypt and store methods with SQL parameters. Normalize usernames with `strip().casefold()`; reject blank or overlong names before SQL.**

~~~python
salt = secrets.token_bytes(16)
derived = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=16384, r=8, p=1, dklen=32)
encoded = "scrypt$16384$8$1$" + salt.hex() + "$" + derived.hex()
# Verification parses exactly six fields and uses hmac.compare_digest.
# All SQL uses psycopg %s parameters, never string interpolation of values.
~~~

- [ ] **Step 4: Run `python -m unittest tests.test_g3_auth_core -v`; expect pass. Confirm a DB exception is not converted into an authenticated user.**
- [ ] **Step 5: Commit with `git commit -m "feat: persist G3 users and sessions"`.**

### Task 3: Login, CSRF, And Role-Aware Auth API

**Files:**
- Create: `services/api/app/auth_service.py`, `services/api/app/auth_api.py`, `tests/test_g3_auth_api.py`
- Modify: `services/api/app/main.py`

**Interfaces:**
- Consumes: Task 2 `Role`, `StoredUser`, `StoredSession`, and `AuthStore`.
- Produces: `Principal(id: int, username: str, role: Role, csrf_token: str)` and dependencies `require_principal`, `require_analyst`, `require_admin`, `require_csrf`. `AuthService.login(username: str, password: str) -> LoginResult` returns `session_token` and `public_user`; `AuthService.resolve(session_token: str) -> Principal` rejects inactive, expired, or revoked sessions.
- Produces: `APIRouter(prefix="/api/v1/auth")` with `POST /login` accepting `{username,password}`, `GET /me`, `POST /logout`, `GET /users`, and `POST /users`. Login/me return `{id,username,role,csrf_token}`; cookie name `ecom_session`, eight-hour max age. User creation returns `{id,username,role}`; duplicate normalized username returns 409 and password shorter than 12 characters returns 422.
- Add `auth_service: AuthService | None = None` as the final `create_app` parameter for explicit fake injection in tests; production default builds the PostgreSQL-backed service.

- [ ] **Step 1: Write failing TestClient tests for cookie flags, wrong credentials, revoked/expired cookie, CSRF, role matrix, duplicate username, short password, and store outage.**

~~~python
def test_viewer_cannot_create_user(self):
    client = TestClient(create_app(auth_service=self.fake_auth))
    client.cookies.set("ecom_session", "viewer-token")
    response = client.post(
        "/api/v1/auth/users",
        headers={"X-CSRF-Token": "viewer-csrf"},
        json={"username": "new-user", "password": "long-password", "role": "analyst"},
    )
    self.assertEqual(response.status_code, 403)
~~~

- [ ] **Step 2: Run `python -m unittest tests.test_g3_auth_api -v`; expect endpoint/import failure.**
- [ ] **Step 3: Implement auth service and router. Generate session and CSRF secrets with `secrets.token_urlsafe(32)`; hash the session token with SHA-256 before storing, but keep the CSRF token in the server-side session so `/me` can return it. Verify expiry, active user, and revocation on every request. Validate `Origin` for login and compare `X-CSRF-Token` with `hmac.compare_digest` on authenticated POST. Map DB unavailability to 503.**

~~~python
@router.post("/login")
def login(body: LoginBody, request: Request, response: Response):
    result = request.app.state.auth_service.login(body.username, body.password)
    response.set_cookie(
        "ecom_session", result.session_token, max_age=8 * 3600,
        httponly=True, samesite="lax", secure=request.url.scheme == "https",
        path="/",
    )
    return result.public_user
~~~

- [ ] **Step 4: Run `python -m unittest tests.test_g3_auth_api -v`; expect pass. Confirm login response and logs never contain password or raw session token.**
- [ ] **Step 5: Commit with `git commit -m "feat: add G3 login and role-aware sessions"`.**

### Task 4: Protect Existing API Contracts

**Files:**
- Modify: `services/api/app/main.py`, `tests/test_api_service.py`, `tests/test_analysis_api.py`, `tests/test_tool_analysis_api.py`, `tests/test_behavior_metrics_api.py`, `tests/test_order_metrics_api.py`
- Create: `tests/api_auth_helpers.py`

**Interfaces:**
- Consumes: Task 3 `require_principal`, `require_analyst`, `require_csrf`.
- Produces: identical successful metric payloads for authenticated users; 401 anonymous, 403 forbidden, 503 auth-store outage. `/health` and `/ready` stay callable without session. `tests/api_auth_helpers.py` exports `make_fake_auth_service()` and `grant_test_role(app, role: Role = "admin") -> None` to override auth dependencies explicitly.

- [ ] **Step 1: Add failing route-family tests: anonymous direct requests to `/api/v1/orders/overview`, `/api/v1/behavior/funnel`, `/metrics/realtime`, `/analysis/tools` return 401; viewer analysis returns 403; missing CSRF returns 403; health remains 200.**

~~~python
for path in ("/api/v1/orders/overview", "/api/v1/behavior/funnel", "/metrics/realtime"):
    self.assertEqual(self.client.get(path).status_code, 401)
self.assertEqual(self.client.post("/analysis/tools", json={"question": "x"}).status_code, 401)
~~~

- [ ] **Step 2: Run `python -m unittest tests.test_g3_auth_api -v`; expect new route assertions to fail while old routes remain open.**
- [ ] **Step 3: Add FastAPI dependency guards to every metric, definition, publication, and quality route and both analysis POST routes. Apply `require_csrf` after `require_analyst` to analysis POST. Do not alter existing query validation or response models.**

~~~python
@app.get(
    "/api/v1/orders/overview",
    response_model=OrderOverviewResponse,
    dependencies=[Depends(require_principal)],
)
~~~

- [ ] **Step 4: In the five existing TestClient suites, explicitly override auth dependencies through `tests/api_auth_helpers.py` rather than adding a runtime bypass. Run `python -m unittest tests.test_g3_auth_api tests.test_api_service tests.test_analysis_api tests.test_tool_analysis_api tests.test_behavior_metrics_api tests.test_order_metrics_api -v`; expect pass.**
- [ ] **Step 5: Run `python -m unittest discover -s tests -q`; record total/failures, then commit with `git commit -m "feat: enforce auth on metrics and analysis"`.**

### Task 5: Bootstrap, Real Database Check, And Runbook

**Files:**
- Create: `scripts/bootstrap_g3_admin.py`, `docs/graduation/g3-auth-runbook.md`
- Modify: `tests/test_g3_auth_api.py`

**Interfaces:**
- Consumes: Task 2 `AuthStore.create_user` and Task 3 login API.
- Produces: `bootstrap_admin(store: AuthStore) -> int` reading `APP_ADMIN_USERNAME` and `APP_ADMIN_PASSWORD` from environment, rejecting missing/short password and non-admin collision without printing secrets. The CLI `main()` constructs `AuthStore(load_settings())` and exits with that return code.

- [ ] **Step 1: Write failing bootstrap tests for missing password, second invocation, and username collision; add session revocation check.**

~~~python
with patch.dict(os.environ, {"APP_ADMIN_USERNAME": "owner", "APP_ADMIN_PASSWORD": ""}):
    self.assertEqual(bootstrap_admin(self.store), 2)
self.store.revoke_session(hashlib.sha256(self.token.encode()).hexdigest())
self.assertEqual(self.client.get("/api/v1/auth/me").status_code, 401)
~~~

- [ ] **Step 2: Run `python -m unittest tests.test_g3_auth_api -v`; expect bootstrap import/test failure.**
- [ ] **Step 3: Implement bootstrap with minimum 12-character password, existing-admin no-op, collision failure; document local secret entry, service startup, login/logout, role checks, and D-drive volume location.**

~~~python
username = os.environ.get("APP_ADMIN_USERNAME", "").strip().casefold()
password = os.environ.get("APP_ADMIN_PASSWORD", "")
if not username or len(password) < 12:
    return 2
existing = store.get_user(username)
if existing is not None:
    return 0 if existing.role == "admin" else 3
store.create_user(username, hash_password(password), "admin")
return 0
~~~

- [ ] **Step 4: Run `python -m unittest tests.test_g3_auth_api -v`, full `python -m unittest discover -s tests -q`, and Compose config. If Docker is available, start only `app-postgres` and API dependencies, verify schema, admin login, anonymous 401, viewer 403, CSRF 403, revoked 401, and DB-outage 503. Record actual results rather than claiming unrun dynamic verification.**
- [ ] **Step 5: Commit with `git commit -m "docs: verify G3 auth foundation"`.**
