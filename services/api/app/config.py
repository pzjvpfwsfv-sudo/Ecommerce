from __future__ import annotations

from dataclasses import dataclass
from os import getenv


@dataclass(frozen=True)
class ApiSettings:
    doris_host: str = "127.0.0.1"
    doris_port: int = 9030
    doris_database: str = "analytics"
    doris_username: str = "root"
    doris_password: str = ""


def load_settings() -> ApiSettings:
    return ApiSettings(
        doris_host=getenv("DORIS_HOST", "127.0.0.1"),
        doris_port=int(getenv("DORIS_PORT", "9030")),
        doris_database=getenv("DORIS_DATABASE", "analytics"),
        doris_username=getenv("DORIS_USERNAME", "root"),
        doris_password=getenv("DORIS_PASSWORD", ""),
    )
