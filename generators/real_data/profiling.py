from __future__ import annotations

from collections import Counter
from contextlib import nullcontext
from datetime import UTC, datetime
from decimal import Decimal
import gzip
from itertools import islice
import json
from pathlib import Path

from .file_io import file_sha256, new_text_file
from .csv_io import bounded_csv_reader
from .normalization import FIELDS, OPTIONAL_FIELDS, EventValidationError, normalize_event


def profile_file(
    input_path: Path,
    *,
    dataset_id: str,
    source_file: str,
    scope: str = "unverified",
    max_rows: int | None = None,
    normalized_path: Path | None = None,
    distinct_limit: int = 10000,
) -> dict:
    if scope not in {"unverified", "prefix", "user_sample", "full_file"}:
        raise ValueError("unsupported input scope")
    if not dataset_id or not source_file or distinct_limit < 1 or (max_rows is not None and max_rows < 1):
        raise ValueError("source identity and positive limits are required")
    input_path = Path(input_path)
    if normalized_path is not None:
        if scope == "user_sample":
            raise ValueError("user samples need original record numbers before normalization")
        normalized_path = Path(normalized_path)
        if normalized_path.resolve() == input_path.resolve():
            raise ValueError("normalized output cannot overwrite input")
    initial_stat = input_path.stat()
    digest = file_sha256(input_path)
    events, days, reasons = Counter(), Counter(), Counter()
    missing = Counter({key: 0 for key in OPTIONAL_FIELDS})
    distinct = {key: set() for key in ("user_id", "product_id", "user_session")}
    exact = dict.fromkeys(distinct, True)
    scanned = valid = out_of_order = 0
    earliest = latest = previous = None
    amount_cents = 0
    opener = gzip.open if input_path.suffix.lower() == ".gz" else open
    with opener(input_path, "rt", encoding="utf-8-sig", newline="") as source:
        reader = bounded_csv_reader(source)
        if next(reader, None) != list(FIELDS):
            raise ValueError("CSV header does not match the REES46 event schema")
        output_context = new_text_file(normalized_path) if normalized_path else nullcontext(None)
        with output_context as output:
            for number, row in enumerate(islice(reader, max_rows), start=1):
                scanned += 1
                try:
                    if len(row) != len(FIELDS):
                        raise EventValidationError("invalid_row_shape")
                    event = normalize_event(dict(zip(FIELDS, row)), dataset_id=dataset_id, source_file=source_file, row_number=number)
                except EventValidationError as exc:
                    reasons[str(exc)] += 1
                    continue
                valid += 1
                timestamp = event["event_time"]
                earliest = min(earliest, timestamp) if earliest else timestamp
                latest = max(latest, timestamp) if latest else timestamp
                if previous is not None and timestamp < previous:
                    out_of_order += 1
                previous = timestamp
                events[event["event_type"]] += 1
                days[timestamp[:10]] += 1
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
                if output is not None:
                    output.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
            final_stat = input_path.stat()
            if (initial_stat.st_size, initial_stat.st_mtime_ns) != (final_stat.st_size, final_stat.st_mtime_ns):
                raise ValueError("input changed during profiling")
    # At the limit we conservatively avoid claiming that the entire file was read.
    limited = max_rows is not None and scanned == max_rows
    warnings = ["profile_is_not_business_acceptance", "purchase_event_price_is_not_financial_revenue"]
    if scope != "full_file" or limited:
        warnings.append("not_a_complete_analysis_window")
    if out_of_order:
        warnings.append("input_requires_event_time_ordering_for_replay")
    if not all(exact.values()):
        warnings.append("capped_cardinality_is_a_lower_bound_not_uv")
    if reasons:
        warnings.append("invalid_rows_excluded_from_business_statistics")
    return {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "source_file": source_file,
        "input_file": str(input_path.resolve()),
        "input_sha256": digest,
        "input_bytes": initial_stat.st_size,
        "profiled_at": datetime.now(UTC).isoformat(),
        "declared_scope": scope,
        "scan_limited": limited,
        "rows_scanned": scanned,
        "valid_rows": valid,
        "rejected_rows": scanned - valid,
        "rejection_reasons": dict(sorted(reasons.items())),
        "event_counts": dict(sorted(events.items())),
        "daily_counts": dict(sorted(days.items())),
        "time_range": {"start": earliest, "end": latest},
        "out_of_order_rows": out_of_order,
        "missing_on_valid_rows": dict(missing),
        "purchase_event_price_sum": f"{amount_cents // 100}.{amount_cents % 100:02d}",
        "cardinality": {key: {"observed_distinct": len(values), "exact": exact[key]} for key, values in distinct.items()},
        "warnings": warnings,
    }
