from datetime import UTC, datetime
from hashlib import file_digest
import json
import os
from pathlib import Path
import re

from .download import source_config
from .normalization import FIELDS, OPTIONAL_FIELDS, normalize_event


MAX_BYTES = 65536


def strict_json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def invalid_constant(value):
        raise ValueError("non-finite JSON value")

    try:
        return json.loads(data, object_pairs_hook=pairs, parse_constant=invalid_constant)
    except RecursionError:
        raise ValueError("JSON nesting exceeds parser limit") from None


def read_metadata(path):
    with Path(path).open("rb") as stream:
        data = stream.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise ValueError("metadata exceeds size limit")
    result = strict_json(data)
    if not isinstance(result, dict):
        raise ValueError("metadata must be an object")
    return result


def source_stamp(stream):
    stat = os.fstat(stream.fileno())
    return stat.st_size, stat.st_mtime_ns


def verify_source(stream, manifest_path):
    manifest = read_metadata(manifest_path)
    config = source_config()
    initial = source_stamp(stream)
    if (manifest.get("scope") != "user_sample" or type(manifest.get("schema_version")) is not int
            or manifest["schema_version"] != 1 or manifest.get("dataset_id") != config["dataset_id"]
            or type(manifest.get("sample_rows")) is not int or manifest["sample_rows"] <= 0
            or type(manifest.get("artifact_bytes")) is not int or manifest["artifact_bytes"] != initial[0]
            or not re.fullmatch(r"[0-9a-f]{64}", str(manifest.get("sha256", "")))
            or manifest.get("selected_paths_lossless") is not True
            or any(type(manifest.get(key)) is not int or manifest[key] != 0 for key in (
                "selected_rejected_rows", "source_shape_rejected_rows", "out_of_order_sample_rows"))):
        raise ValueError("source manifest is not a lossless ordered G1 sample")
    sources = manifest.get("sources")
    registry = {item["source_file"]: item for item in config["months"].values()}
    if not isinstance(sources, list) or len(sources) < 2:
        raise ValueError("source requires consecutive registered months")
    windows = []
    for item in sources:
        registered = registry.get(item["source_file"]) if isinstance(item, dict) and isinstance(item.get("source_file"), str) else None
        if not registered or item.get("source_url") != registered["source_url"] or item.get("gzip_integrity_verified") is not True:
            raise ValueError("source month is unverified")
        windows.append(registered)
    if (any(a["window_end"] != b["window_start"] for a, b in zip(windows, windows[1:]))
            or manifest.get("window_start") != windows[0]["window_start"]
            or manifest.get("window_end_exclusive") != windows[-1]["window_end"]):
        raise ValueError("source windows are inconsistent")
    if file_digest(stream, "sha256").hexdigest() != manifest["sha256"] or source_stamp(stream) != initial:
        raise ValueError("source checksum mismatch or file changed")
    stream.seek(0)
    manifest["_windows"] = {item["source_file"]: (datetime.fromisoformat(item["window_start"]), datetime.fromisoformat(item["window_end"])) for item in windows}
    return manifest, initial


def read_line(stream):
    line = stream.readline(MAX_BYTES + 1)
    if len(line) > MAX_BYTES:
        raise ValueError("JSONL record exceeds size limit")
    return line


def validate_event(line, manifest):
    event = strict_json(line)
    expected_keys = set(FIELDS) | {"schema_version", "event_id", "dataset_id", "source_file", "source_row_number"}
    if (not isinstance(event, dict) or set(event) != expected_keys
            or type(event.get("schema_version")) is not int or event["schema_version"] != 1
            or type(event.get("source_row_number")) is not int or event["source_row_number"] < 1
            or event.get("dataset_id") != manifest["dataset_id"]
            or not isinstance(event.get("source_file"), str) or event["source_file"] not in manifest["_windows"]):
        raise ValueError("invalid normalized event schema or source identity")
    try:
        timestamp = datetime.fromisoformat(event["event_time"])
        start, end = manifest["_windows"][event["source_file"]]
        if timestamp.tzinfo != UTC or not start <= timestamp < end:
            raise ValueError
        raw = {key: "" if key in OPTIONAL_FIELDS and event[key] is None else event[key] for key in FIELDS}
        raw["event_time"] = timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
        normalized = normalize_event(raw, dataset_id=event["dataset_id"], source_file=event["source_file"], row_number=event["source_row_number"])
    except (ValueError, TypeError):
        raise ValueError("invalid normalized business fields") from None
    if normalized != event:
        raise ValueError("normalized event identity or canonical content mismatch")
    return event


def skip_confirmed(stream, manifest, state):
    last = None
    for _ in range(state["confirmed_records"]):
        last = read_line(stream)
        if not last:
            raise ValueError("checkpoint exceeds source records")
    if last is not None and validate_event(last, manifest)["event_id"] != state["last_event_id"]:
        raise ValueError("checkpoint prefix identity mismatch")
    if state["confirmed_records"] == manifest["sample_rows"] and read_line(stream):
        raise ValueError("source contains more records than its manifest")
