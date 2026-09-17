from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
import gzip
from hashlib import sha256
import io
import json
from pathlib import Path
import re
import sys

from .download import source_config
from .csv_io import bounded_csv_reader
from .file_io import new_text_file, write_json
from .normalization import FIELDS, OPTIONAL_FIELDS, EventValidationError, normalize_event


class DigestReader:
    def __init__(self, stream):
        self.stream = stream
        self.digest = sha256()

    def read(self, size=-1):
        data = self.stream.read(size)
        self.digest.update(data)
        return data


def verified_sources(inputs: list[Path], config: dict) -> list[dict]:
    if len(inputs) < 2:
        raise ValueError("at least two complete consecutive registered months are required")
    registry = {entry["source_file"]: entry for entry in config["months"].values()}
    sources = []
    for path in map(Path, inputs):
        manifest = json.loads(Path(str(path) + ".provenance.json").read_text(encoding="utf-8"))
        if not isinstance(manifest, dict) or manifest.get("scope") != "full_file" or manifest.get("full_source_downloaded") is not True:
            raise ValueError("sampling requires a complete source manifest, not a prefix")
        name = manifest.get("source_file")
        registered = registry.get(name)
        if registered is None or manifest.get("dataset_id") != config["dataset_id"]:
            raise ValueError("unregistered source identity")
        if any(manifest.get(key) != registered[key] for key in ("source_url", "window_start", "window_end")):
            raise ValueError("manifest contradicts the registered source window")
        if manifest.get("artifact_bytes") != path.stat().st_size or not re.fullmatch(r"[0-9a-f]{64}", str(manifest.get("sha256", ""))):
            raise ValueError("source size or checksum metadata is invalid")
        sources.append(registered | {"path": path, "sha256": manifest["sha256"]})
    sources.sort(key=lambda source: source["window_start"])
    if len({source["source_file"] for source in sources}) != len(sources):
        raise ValueError("duplicate source month")
    if any(left["window_end"] != right["window_start"] for left, right in zip(sources, sources[1:])):
        raise ValueError("source windows must be consecutive without gaps")
    return sources


def sample_users(
    inputs: list[Path], output_path: Path, *, basis_points: int = 200,
    seed: str = "graduation-v1", distinct_limit: int = 200000,
) -> dict:
    output_path = Path(output_path)
    manifest_path = Path(str(output_path) + ".provenance.json")
    for path in (output_path, manifest_path):
        if path.exists():
            raise FileExistsError(path)
    if not 1 <= basis_points <= 10000 or distinct_limit < 1 or not seed or len(seed) > 128:
        raise ValueError("valid basis points, positive distinct limit and a nonempty bounded seed are required")
    config = source_config()
    sources = verified_sources(inputs, config)
    selection_prefix = (config["dataset_id"] + "\0" + seed + "\0").encode("utf-8")
    events, days, reasons = Counter(), Counter(), Counter()
    missing = Counter({key: 0 for key in OPTIONAL_FIELDS})
    distinct = {key: set() for key in ("user_id", "product_id", "user_session")}
    exact = dict.fromkeys(distinct, True)
    candidates = emitted = shape_rejected = out_of_order = amount_cents = 0
    previous_selected = None
    output_digest = sha256()
    source_reports = []
    with new_text_file(output_path) as target:
        for source in sources:
            start = datetime.fromisoformat(source["window_start"]).replace(tzinfo=None)
            end = datetime.fromisoformat(source["window_end"]).replace(tzinfo=None)
            source_days = Counter()
            scanned = source_rejected = source_selected = source_emitted = source_order_errors = 0
            first_time = last_time = previous_time = None
            with source["path"].open("rb") as raw:
                digest_reader = DigestReader(raw)
                with gzip.GzipFile(fileobj=digest_reader) as compressed:
                    with io.TextIOWrapper(compressed, encoding="utf-8-sig", newline="") as text:
                        reader = bounded_csv_reader(text)
                        if next(reader, None) != list(FIELDS):
                            raise ValueError("source CSV header does not match the registered schema")
                        for number, row in enumerate(reader, start=1):
                            scanned += 1
                            if scanned % 2000000 == 0:
                                print(f"scan {source['source_file']}: {scanned} source rows, {source_emitted} sample rows", file=sys.stderr, flush=True)
                            if not row:
                                source_rejected += 1
                                continue
                            stamp = row[0].strip()
                            try:
                                if len(stamp) != 23 or not stamp.endswith(" UTC"):
                                    raise ValueError
                                timestamp = datetime.fromisoformat(stamp[:-4])
                                if timestamp.tzinfo is not None or not start <= timestamp < end:
                                    raise ValueError
                            except ValueError:
                                raise ValueError(f"record {number} falls outside the registered source window: {source['source_file']}") from None
                            if len(row) != len(FIELDS) or not row[7].strip():
                                source_rejected += 1
                                continue
                            first_time = min(first_time, stamp) if first_time else stamp
                            last_time = max(last_time, stamp) if last_time else stamp
                            source_order_errors += previous_time is not None and stamp < previous_time
                            previous_time = stamp
                            source_days[stamp[:10]] += 1
                            user = row[7].strip()
                            bucket = int.from_bytes(sha256(selection_prefix + user.encode("utf-8")).digest()[:8], "big") % 10000
                            if bucket >= basis_points:
                                continue
                            candidates += 1
                            source_selected += 1
                            try:
                                event = normalize_event(dict(zip(FIELDS, row)), dataset_id=config["dataset_id"], source_file=source["source_file"], row_number=number)
                            except EventValidationError as exc:
                                reasons[str(exc)] += 1
                                continue
                            emitted += 1
                            source_emitted += 1
                            events[event["event_type"]] += 1
                            days[event["event_time"][:10]] += 1
                            out_of_order += previous_selected is not None and event["event_time"] < previous_selected
                            previous_selected = event["event_time"]
                            if event["event_type"] == "purchase":
                                amount_cents += int(Decimal(event["price"]) * 100)
                            for key in OPTIONAL_FIELDS:
                                missing[key] += event[key] is None
                            for key, values in distinct.items():
                                value = event[key]
                                if value is None or value in values:
                                    continue
                                if len(values) < distinct_limit:
                                    values.add(value)
                                else:
                                    exact[key] = False
                            line = json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n"
                            target.write(line)
                            output_digest.update(line.encode("utf-8"))
                if digest_reader.digest.hexdigest() != source["sha256"]:
                    raise ValueError(f"source checksum mismatch: {source['source_file']}")
            shape_rejected += source_rejected
            source_reports.append({
                "source_file": source["source_file"], "source_url": source["source_url"],
                "sha256": source["sha256"], "gzip_integrity_verified": True,
                "rows_scanned": scanned, "shape_or_user_rejected_rows": source_rejected,
                "selected_candidates": source_selected, "sample_rows": source_emitted,
                "time_range": {"start": first_time, "end": last_time},
                "daily_counts": dict(sorted(source_days.items())),
                "out_of_order_rows": source_order_errors,
            })
            print(f"verified {source['source_file']}: {scanned} source rows, {source_emitted} sample rows", file=sys.stderr, flush=True)
    result = {
        "schema_version": 1, "dataset_id": config["dataset_id"] , "scope": "user_sample",
        "window_start": sources[0]["window_start"], "window_end_exclusive": sources[-1]["window_end"],
        "selection": {"algorithm": "sha256_first_8_bytes_big_endian_mod_10000", "basis_points": basis_points, "seed": seed, "key": "dataset_id + NUL + seed + NUL + stripped_user_id"},
        "sources": source_reports, "sample_rows": emitted,
        "selected_candidates": candidates, "selected_rejected_rows": sum(reasons.values()),
        "source_shape_rejected_rows": shape_rejected,
        "selected_rejection_reasons": dict(sorted(reasons.items())),
        "selected_paths_lossless": not reasons and shape_rejected == 0,
        "out_of_order_sample_rows": out_of_order,
        "event_counts": dict(sorted(events.items())), "daily_counts": dict(sorted(days.items())),
        "missing_on_sample_rows": dict(missing),
        "cardinality": {key: {"observed_distinct": len(values), "exact": exact[key]} for key, values in distinct.items()},
        "purchase_event_price_sum": f"{amount_cents // 100}.{amount_cents % 100:02d}",
        "artifact": str(output_path.resolve()), "artifact_bytes": output_path.stat().st_size,
        "sha256": output_digest.hexdigest(), "created_at": datetime.now(UTC).isoformat(),
        "warnings": ["sample_statistics_are_not_full_store_totals", "window_boundaries_can_censor_sessions_and_retention", "only_selected_rows_receive_full_business_field_validation", "purchase_event_price_is_not_financial_revenue", "capped_cardinality_requires_exact_flag_check"],
    }
    try:
        write_json(manifest_path, result)
    except Exception:
        output_path.unlink(missing_ok=True)
        raise
    return result
