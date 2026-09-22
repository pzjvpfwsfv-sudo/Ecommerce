from __future__ import annotations

from contextlib import contextmanager
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from typing import Iterator, TextIO


def file_sha256(path: Path) -> str:
    digest = sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@contextmanager
def new_text_file(path: Path) -> Iterator[TextIO]:
    target = Path(path)
    if target.exists():
        raise FileExistsError(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    stream = tempfile.NamedTemporaryFile(
        mode="w",
        dir=target.parent,
        suffix=".part",
        encoding="utf-8",
        newline="",
        delete=False,
    )
    temporary = Path(stream.name)
    try:
        with stream:
            yield stream
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def write_json(path: Path, value: dict[str, object]) -> None:
    with new_text_file(path) as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
