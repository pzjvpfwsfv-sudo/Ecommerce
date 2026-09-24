from __future__ import annotations

import os
from pathlib import Path
import sys


API_ROOT = Path(__file__).resolve().parents[1] / "services" / "api"
if API_ROOT.is_dir():
    sys.path.insert(0, str(API_ROOT))

from app.auth_crypto import hash_password  # noqa: E402
from app.auth_store import AuthStore  # noqa: E402
from app.config import load_settings  # noqa: E402


def bootstrap_admin(store: AuthStore) -> int:
    username = os.environ.get("APP_ADMIN_USERNAME", "").strip().casefold()
    password = os.environ.get("APP_ADMIN_PASSWORD", "")
    if not username or len(username) > 128 or len(password) < 12:
        return 2
    try:
        existing = store.get_user(username)
        if existing is not None:
            return 0 if existing.role == "admin" else 3
        created = store.create_user(username, hash_password(password), "admin")
        store.record_audit(created.id, "admin_bootstrapped")
    except Exception:
        return 4
    return 0


def main() -> int:
    try:
        return bootstrap_admin(AuthStore(load_settings()))
    except Exception:
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
