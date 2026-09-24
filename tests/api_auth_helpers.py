from app.auth_service import Principal, require_csrf, require_principal
from app.auth_store import Role


class FakeAuthService:
    def __init__(self, role: Role = "admin") -> None:
        self.principal = Principal(0, "test-user", role, "test-csrf")

    def resolve(self, _session_token: str) -> Principal:
        return self.principal


def make_fake_auth_service(role: Role = "admin") -> FakeAuthService:
    return FakeAuthService(role)


def grant_test_role(app, role: Role = "admin") -> None:
    principal = Principal(0, "test-user", role, "test-csrf")
    app.dependency_overrides[require_principal] = lambda: principal
    app.dependency_overrides[require_csrf] = lambda: principal
