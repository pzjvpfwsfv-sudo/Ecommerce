from __future__ import annotations

from contextlib import contextmanager
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile


def file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@contextmanager
def _new_file(path: Path, *, binary: bool):
    """Publish a complete artifact without overwriting an existing file."""
    path = Path(path)
    if path.exists():
        raise FileExistsError(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    options = {} if binary else {"encoding": "utf-8", "newline": ""}
    stream = tempfile.NamedTemporaryFile(mode="wb" if binary else "w", dir=path.parent, suffix=".part", delete=False, **options)
    temporary = Path(stream.name)
    try:
        with stream:
            yield stream
        # A same-filesystem hard link is atomic and fails if another writer won.
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def new_text_file(path: Path):
    return _new_file(path, binary=False)


def new_binary_file(path: Path):
    return _new_file(path, binary=True)


def write_json(path: Path, value: dict) -> None:
    with new_text_file(path) as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
