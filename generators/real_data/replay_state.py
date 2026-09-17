from contextlib import contextmanager
import json
import os
from pathlib import Path
import tempfile
from uuid import UUID, uuid4

from .replay_source import read_metadata


@contextmanager
def checkpoint_lock(path):
    path = Path(str(path) + ".lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as stream:
        if os.fstat(stream.fileno()).st_size == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise ValueError("a replay using this checkpoint is already running") from None
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream, fcntl.LOCK_UN)


def save_checkpoint(path, state):
    path = Path(path)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, suffix=".part", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(state, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def load_checkpoint(path, identity):
    if not Path(path).exists():
        return {"schema_version": 1, "replay_id": str(uuid4()), "identity": identity, "confirmed_records": 0, "last_event_id": None}
    state = read_metadata(path)
    if state.get("identity") != identity:
        raise ValueError("checkpoint identity differs from input, mode or destination")
    count = state.get("confirmed_records")
    if (type(state.get("schema_version")) is not int or state["schema_version"] != 1
            or type(count) is not int or not 0 <= count <= identity["sample_rows"]
            or (count == 0 and state.get("last_event_id") is not None)
            or (count > 0 and not isinstance(state.get("last_event_id"), str))):
        raise ValueError("invalid checkpoint progress")
    try:
        if str(UUID(state["replay_id"])) != state["replay_id"]:
            raise ValueError
    except (ValueError, TypeError, KeyError, AttributeError):
        raise ValueError("invalid checkpoint task identity") from None
    return state
