from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
from typing import BinaryIO
from urllib.parse import urlparse
from uuid import uuid4
import zipfile

from .csv_io import bounded_csv_reader
from .file_io import file_sha256, new_text_file, write_json
from .normalization import normalize_row
from .schemas import (
    DATASET_ID,
    OFFICIAL_SOURCE_URL,
    SCHEMA_VERSION,
    SOURCE_FILES,
    TABLE_SPECS,
    TableSpec,
)


@dataclass(frozen=True)
class Acquisition:
    kaggle_version: str
    acquired_at: str
    license_name: str
    license_url: str


def _validate_acquisition(value: Acquisition) -> None:
    if value.kaggle_version.strip() != "2":
        raise ValueError("invalid_kaggle_version")
    try:
        timestamp = datetime.fromisoformat(value.acquired_at.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("invalid_acquired_at") from None
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("invalid_acquired_at")
    if not value.license_name.strip() or len(value.license_name.strip()) > 200:
        raise ValueError("invalid_license_name")
    parsed = urlparse(value.license_url)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("invalid_license_url")


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _is_link_or_junction(path: Path) -> bool:
    is_junction = getattr(os.path, "isjunction", lambda _: False)
    return path.is_symlink() or bool(is_junction(path))


def _validate_paths(archive_path: Path, input_dir: Path, output_root: Path) -> tuple[Path, Path, Path]:
    archive = Path(archive_path)
    source = Path(input_dir)
    output = Path(output_root)
    if not archive.is_absolute() or not source.is_absolute() or not output.is_absolute():
        raise ValueError("paths_must_be_absolute")
    if _is_link_or_junction(archive):
        raise ValueError("symlink_archive")
    if _is_link_or_junction(source):
        raise ValueError("symlink_input_directory")
    if output.exists() and _is_link_or_junction(output):
        raise ValueError("symlink_output_directory")
    archive_resolved = archive.resolve(strict=True)
    source_resolved = source.resolve(strict=True)
    output_resolved = output.resolve(strict=False)
    if not archive_resolved.is_file():
        raise ValueError("invalid_archive_file")
    if not source_resolved.is_dir():
        raise ValueError("invalid_input_directory")
    if _is_within(output_resolved, source_resolved) or output_resolved == source_resolved:
        raise ValueError("output_overlaps_input")
    if output_resolved == archive_resolved:
        raise ValueError("output_overlaps_archive")
    return archive_resolved, source_resolved, output_resolved


def _expected_by_filename() -> dict[str, TableSpec]:
    return {spec.source_file: spec for spec in TABLE_SPECS.values()}


def _source_paths(input_dir: Path) -> dict[str, Path]:
    expected = _expected_by_filename()
    result: dict[str, Path] = {}
    for name in sorted(SOURCE_FILES):
        path = input_dir / name
        if not path.exists():
            raise ValueError("missing_source_file")
        if _is_link_or_junction(path):
            raise ValueError("symlink_source_file")
        if not path.is_file() or path.resolve(strict=True).parent != input_dir:
            raise ValueError("invalid_source_file")
        result[name] = path

    csv_names = [entry.name for entry in input_dir.iterdir() if entry.is_file() and entry.suffix.lower() == ".csv"]
    lowered = [name.casefold() for name in csv_names]
    if len(lowered) != len(set(lowered)):
        raise ValueError("duplicate_source_filename")
    if set(csv_names) != set(expected):
        raise ValueError("unexpected_csv_file")
    return result


def _safe_archive_name(info: zipfile.ZipInfo) -> str | None:
    raw = info.filename
    if "\\" in raw or raw.startswith("/") or re.match(r"^[A-Za-z]:", raw):
        raise ValueError("unsafe_archive_member")
    path = PurePosixPath(raw)
    if any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("unsafe_archive_member")
    mode = (info.external_attr >> 16) & 0o170000
    if mode == stat.S_IFLNK:
        raise ValueError("symlink_archive_member")
    if info.is_dir():
        return None
    return path.name


def _compare_streams(first: BinaryIO, second: BinaryIO) -> bool:
    while True:
        left = first.read(1024 * 1024)
        right = second.read(1024 * 1024)
        if left != right:
            return False
        if not left:
            return True


def _verify_archive(archive: Path, sources: dict[str, Path]) -> None:
    expected_lower = {name.casefold(): name for name in SOURCE_FILES}
    with zipfile.ZipFile(archive) as bundle:
        members: dict[str, zipfile.ZipInfo] = {}
        seen_lower: set[str] = set()
        for info in bundle.infolist():
            basename = _safe_archive_name(info)
            if basename is None:
                continue
            lowered = basename.casefold()
            if lowered in seen_lower:
                raise ValueError("duplicate_archive_member")
            seen_lower.add(lowered)
            if lowered not in expected_lower:
                raise ValueError("unexpected_archive_file")
            expected_name = expected_lower[lowered]
            if basename != expected_name:
                raise ValueError("invalid_archive_filename")
            members[expected_name] = info
        if set(members) != set(SOURCE_FILES):
            raise ValueError("missing_archive_file")
        for name in sorted(SOURCE_FILES):
            with bundle.open(members[name], "r") as archived, sources[name].open("rb") as extracted:
                if not _compare_streams(archived, extracted):
                    raise ValueError("archive_file_mismatch")


def _inspect_source(spec: TableSpec, path: Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = bounded_csv_reader(stream)
        header = next(reader, None)
        if header is None or tuple(header) != spec.headers:
            raise ValueError("invalid_header")
        row_count = 0
        for row_count, values in enumerate(reader, start=1):
            if len(values) != len(spec.headers):
                raise ValueError("invalid_column_count")
            normalize_row(spec, dict(zip(spec.headers, values)), row_count)
    if row_count == 0:
        raise ValueError("empty_source_file")
    return {
        "entity": spec.entity,
        "name": spec.source_file,
        "sha256": file_sha256(path),
        "bytes": path.stat().st_size,
        "row_count": row_count,
        "header": list(spec.headers),
    }


def _bundle_sha256(entries: list[dict[str, object]], sources: dict[str, Path]) -> str:
    digest = sha256()
    for entry in sorted(entries, key=lambda item: str(item["name"])):
        name = str(entry["name"])
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        with sources[name].open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        digest.update(b"\0")
        digest.update(str(entry["row_count"]).encode("ascii"))
        digest.update(b"\0")
        digest.update(str(entry["sha256"]).encode("ascii"))
    return digest.hexdigest()


def _write_normalized_file(
    spec: TableSpec,
    source: Path,
    target: Path,
    source_bundle_sha256: str,
) -> dict[str, object]:
    output_digest = sha256()
    identity_digest = sha256()
    bytes_written = 0
    row_count = 0
    with source.open("r", encoding="utf-8-sig", newline="") as input_stream:
        reader = bounded_csv_reader(input_stream)
        header = next(reader, None)
        if header is None or tuple(header) != spec.headers:
            raise ValueError("invalid_header")
        with new_text_file(target) as output_stream:
            for row_count, values in enumerate(reader, start=1):
                if len(values) != len(spec.headers):
                    raise ValueError("invalid_column_count")
                normalized = normalize_row(
                    spec,
                    dict(zip(spec.headers, values)),
                    row_count,
                ) | {"source_bundle_sha256": source_bundle_sha256}
                line = json.dumps(
                    normalized,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ) + "\n"
                encoded = line.encode("utf-8")
                output_stream.write(line)
                output_digest.update(encoded)
                identity_digest.update(str(normalized["source_row_id"]).encode("ascii"))
                identity_digest.update(b"\n")
                bytes_written += len(encoded)
    return {
        "name": f"normalized/{spec.entity}.jsonl",
        "sha256": output_digest.hexdigest(),
        "bytes": bytes_written,
        "row_count": row_count,
        "source_row_id_sequence_sha256": identity_digest.hexdigest(),
    }


def _remove_staging(path: Path, prepared_root: Path) -> None:
    if (
        path.name.startswith(".staging-")
        and path.parent == prepared_root
        and path.exists()
    ):
        shutil.rmtree(path)


def prepare_bundle(
    archive_path: Path,
    input_dir: Path,
    output_root: Path,
    acquisition: Acquisition,
) -> dict[str, object]:
    _validate_acquisition(acquisition)
    archive, source_dir, output = _validate_paths(archive_path, input_dir, output_root)
    sources = _source_paths(source_dir)
    _verify_archive(archive, sources)

    entries = [
        _inspect_source(spec, sources[spec.source_file])
        for spec in sorted(TABLE_SPECS.values(), key=lambda item: item.source_file)
    ]
    bundle_sha = _bundle_sha256(entries, sources)
    prepared_root = output / "prepared"
    if prepared_root.exists() and (
        _is_link_or_junction(prepared_root)
        or prepared_root.resolve(strict=True).parent != output
    ):
        raise ValueError("unsafe_prepared_directory")
    final = prepared_root / bundle_sha
    if final.exists():
        raise FileExistsError(final)

    prepared_root.mkdir(parents=True, exist_ok=True)
    staging = prepared_root / f".staging-{uuid4().hex}"
    staging.mkdir()
    try:
        normalized_root = staging / "normalized"
        normalized_root.mkdir()
        entry_by_entity = {str(entry["entity"]): entry for entry in entries}
        for spec in sorted(TABLE_SPECS.values(), key=lambda item: item.source_file):
            normalized = _write_normalized_file(
                spec,
                sources[spec.source_file],
                normalized_root / f"{spec.entity}.jsonl",
                bundle_sha,
            )
            if normalized["row_count"] != entry_by_entity[spec.entity]["row_count"]:
                raise ValueError("normalized_row_count_mismatch")
            entry_by_entity[spec.entity]["normalized"] = normalized

        manifest: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "dataset_id": DATASET_ID,
            "kaggle_version": acquisition.kaggle_version.strip(),
            "source_url": OFFICIAL_SOURCE_URL,
            "acquired_at": acquisition.acquired_at,
            "license": {
                "name": acquisition.license_name.strip(),
                "url": acquisition.license_url,
            },
            "archive": {
                "name": archive.name,
                "sha256": file_sha256(archive),
                "bytes": archive.stat().st_size,
            },
            "source_bundle_sha256": bundle_sha,
            "files": entries,
        }
        write_json(staging / "source-bundle.json", manifest)
        if final.exists():
            raise FileExistsError(final)
        os.rename(staging, final)
        return manifest
    except BaseException:
        _remove_staging(staging, prepared_root)
        raise
