"""Bounded-memory audit for the full G2-D dimension candidate and readback."""

import argparse
import csv
from datetime import date
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile


SOURCE_COLUMNS = (
    "window_type", "window_start", "window_end", "dimension_type", "dimension_id",
    "dimension_name", "is_unknown", "view_count", "cart_count", "purchase_count",
    "unique_user_count", "purchase_amount_proxy",
)
COLUMNS = ("metric_run_id", "dataset_id", "metric_version") + SOURCE_COLUMNS
KEY_COLUMNS = ("metric_run_id", "window_type", "window_start", "dimension_type", "dimension_id")
DIMENSION_TYPES = ("product", "category", "brand")
COUNT_COLUMNS = ("view_count", "cart_count", "purchase_count", "unique_user_count")


def _nonnegative_int(value, name):
    text = str(value)
    if not text.isdecimal() or len(text) > 19:
        raise ValueError(f"{name} must be a nonnegative Int64")
    number = int(text)
    if number > 9223372036854775807:
        raise ValueError(f"{name} exceeds Int64")
    return number


def _money(value):
    try:
        amount = Decimal(str(value))
        cents = amount * 100
        if not amount.is_finite() or amount < 0 or cents != cents.to_integral_value():
            raise ValueError("purchase_amount_proxy must be nonnegative cents")
        return int(cents)
    except (InvalidOperation, TypeError) as exc:
        raise ValueError("purchase_amount_proxy is invalid") from exc


def _json_token(value, column):
    if value is None:
        return "null"
    if column == "is_unknown":
        return str(value)
    if column in COUNT_COLUMNS:
        return str(value)
    if column == "purchase_amount_proxy":
        return f"{Decimal(value):.2f}"
    return json.dumps(str(value), ensure_ascii=False, separators=(",", ":"))


def _normalize(row, metric_run_id, dataset_id, metric_version, window_start, window_end,
               source_has_identity):
    expected_columns = set(COLUMNS if source_has_identity else SOURCE_COLUMNS)
    if set(row) != expected_columns:
        raise ValueError("dimension columns do not match the fixed contract")
    if source_has_identity:
        for column, expected in (
            ("metric_run_id", metric_run_id), ("dataset_id", dataset_id),
            ("metric_version", metric_version),
        ):
            if row[column] != expected:
                raise ValueError("dimension identity does not match the selected run")
    values = {
        "metric_run_id": metric_run_id,
        "dataset_id": dataset_id,
        "metric_version": metric_version,
    }
    for column in SOURCE_COLUMNS:
        values[column] = row[column]
    if values["window_type"] not in ("DAY", "FULL"):
        raise ValueError("dimension window type is invalid")
    try:
        start = date.fromisoformat(values["window_start"])
        end = date.fromisoformat(values["window_end"])
    except (TypeError, ValueError) as exc:
        raise ValueError("dimension window date is invalid") from exc
    if values["window_type"] == "DAY":
        if start != end or not window_start <= values["window_start"] <= window_end:
            raise ValueError("dimension DAY window is outside the source")
    elif values["window_start"] != window_start or values["window_end"] != window_end:
        raise ValueError("dimension FULL window does not match the source")
    if values["dimension_type"] not in DIMENSION_TYPES:
        raise ValueError("dimension type is invalid")
    if not values["dimension_id"] or len(values["dimension_id"]) > 256:
        raise ValueError("dimension ID is invalid")
    if values["dimension_name"] is None:
        values["dimension_name"] = ""
    if len(values["dimension_name"]) > 512:
        raise ValueError("dimension name exceeds Doris capacity")
    flag = str(values["is_unknown"]).lower()
    if flag not in ("true", "false", "0", "1"):
        raise ValueError("dimension unknown flag is invalid")
    values["is_unknown"] = "1" if flag in ("true", "1") else "0"
    if values["dimension_id"] == "UNKNOWN" and values["is_unknown"] != "1":
        raise ValueError("UNKNOWN dimension ID must be marked unknown")
    if values["is_unknown"] == "0" and not values["dimension_name"].strip():
        raise ValueError("known dimension needs a display name")
    for column in COUNT_COLUMNS:
        values[column] = str(_nonnegative_int(values[column], column))
    cents = _money(values["purchase_amount_proxy"])
    values["purchase_amount_proxy"] = f"{Decimal(cents) / 100:.2f}"
    return values, cents


def _mysql_cell(text):
    if text == "NULL":
        return None
    decoded = []
    index = 0
    escapes = {"n": "\n", "r": "\r", "t": "\t", "0": "\0", "Z": chr(26)}
    while index < len(text):
        if text[index] == "\\" and index + 1 < len(text):
            index += 1
            decoded.append(escapes.get(text[index], text[index]))
        else:
            decoded.append(text[index])
        index += 1
    return "".join(decoded)


def parse_mysql_batch(lines):
    iterator = iter(lines)
    try:
        headers = next(iterator).rstrip("\r\n").split("\t")
    except StopIteration as exc:
        raise ValueError("Doris readback is empty") from exc
    if not headers or any(not header for header in headers) or len(set(headers)) != len(headers):
        raise ValueError("Doris readback headers are invalid")
    for line in iterator:
        cells = line.rstrip("\r\n").split("\t")
        if len(cells) != len(headers):
            raise ValueError("Doris readback column count is invalid")
        yield dict(zip(headers, (_mysql_cell(cell) for cell in cells)))


def audit_dimension_rows(rows, *, output_path, scratch_dir, metric_run_id, dataset_id,
                         metric_version, overview_rows, window_start, window_end,
                         source_has_identity=False):
    scratch_dir = Path(scratch_dir)
    output_path = None if output_path is None else Path(output_path)
    if output_path is not None and output_path.parent.resolve() != scratch_dir.resolve():
        raise ValueError("dimension candidate must stay in the selected run directory")
    descriptor, db_name = tempfile.mkstemp(prefix="g2d-dimension-", suffix=".sqlite", dir=scratch_dir)
    os.close(descriptor)
    temporary = None
    connection = None
    stream = None
    try:
        connection = sqlite3.connect(db_name)
        connection.execute("PRAGMA journal_mode=OFF")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA temp_store=FILE")
        connection.execute("""
            CREATE TABLE dimension (
                sort_key BLOB PRIMARY KEY, canonical TEXT NOT NULL,
                window_type TEXT NOT NULL, window_start TEXT NOT NULL,
                dimension_type TEXT NOT NULL, dimension_id TEXT NOT NULL,
                view_count INTEGER NOT NULL, cart_count INTEGER NOT NULL,
                purchase_count INTEGER NOT NULL, amount_cents INTEGER NOT NULL
            )
        """)
        writer = None
        if output_path is not None:
            fd, temporary = tempfile.mkstemp(prefix="dimension-", suffix=".part", dir=scratch_dir)
            stream = os.fdopen(fd, "w", newline="", encoding="utf-8")
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(COLUMNS)
        expected = {}
        for overview in overview_rows:
            key = (overview["window_type"], overview["window_start"], overview["window_end"])
            if key in expected:
                raise ValueError("duplicate overview window")
            expected[key] = (
                *(_nonnegative_int(overview[column], column) for column in COUNT_COLUMNS[:3]),
                _money(overview["purchase_amount_proxy"]),
            )
        summary = {}
        count = 0
        for row in rows:
            values, cents = _normalize(
                row, metric_run_id, dataset_id, metric_version, window_start, window_end,
                source_has_identity,
            )
            tokens = {column: _json_token(values[column], column) for column in COLUMNS}
            key = "[" + ",".join(tokens[column] for column in KEY_COLUMNS) + "]"
            canonical = "[" + ",".join(tokens[column] for column in COLUMNS) + "]"
            try:
                connection.execute(
                    "INSERT INTO dimension VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (key.encode("utf-16-be"), canonical, values["window_type"],
                     values["window_start"], values["dimension_type"],
                     values["dimension_id"], int(values["view_count"]),
                     int(values["cart_count"]), int(values["purchase_count"]), cents),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError("duplicate normalized dimension key") from exc
            group = (values["window_type"], values["window_start"],
                     values["window_end"], values["dimension_type"])
            totals = summary.setdefault(group, [0, 0, 0, 0])
            for index, column in enumerate(COUNT_COLUMNS[:3]):
                totals[index] += int(values[column])
            totals[3] += cents
            if writer is not None:
                writer.writerow([values[column] for column in COLUMNS])
            count += 1
        if stream is not None:
            stream.flush()
            os.fsync(stream.fileno())
            stream.close()
            stream = None
        if count == 0:
            raise ValueError("dimension result is empty")
        required = {(kind, start, end, dimension_type)
                    for kind, start, end in expected for dimension_type in DIMENSION_TYPES}
        if set(summary) != required:
            raise ValueError("dimension windows do not cover every overview window")
        for kind, start, end, dimension_type in required:
            if tuple(summary[(kind, start, end, dimension_type)]) != expected[(kind, start, end)]:
                raise ValueError("dimension totals do not match overview")
        connection.commit()
        day = """
            SELECT dimension_type, dimension_id, sum(view_count), sum(cart_count),
                   sum(purchase_count), sum(amount_cents)
            FROM dimension WHERE window_type = 'DAY'
            GROUP BY dimension_type, dimension_id
        """
        full = """
            SELECT dimension_type, dimension_id, view_count, cart_count,
                   purchase_count, amount_cents
            FROM dimension WHERE window_type = 'FULL'
        """
        for left, right in ((day, full), (full, day)):
            if connection.execute(f"SELECT * FROM ({left} EXCEPT {right}) LIMIT 1").fetchone():
                raise ValueError("dimension FULL identities or totals do not match DAY rows")
        digest = hashlib.sha256()
        first = True
        for (canonical,) in connection.execute("SELECT canonical FROM dimension ORDER BY sort_key"):
            if not first:
                digest.update(b"\n")
            digest.update(canonical.encode("utf-8"))
            first = False
        evidence = {
            "row_count": count,
            "canonical_sha256": digest.hexdigest(),
            "summary_rows": [
                {"window_type": kind, "window_start": start, "window_end": end,
                 "dimension_type": dimension_type, "dimension_id": "ALL",
                 "dimension_name": "All", "is_unknown": "0",
                 "view_count": str(totals[0]), "cart_count": str(totals[1]),
                 "purchase_count": str(totals[2]), "unique_user_count": "0",
                 "purchase_amount_proxy": f"{Decimal(totals[3]) / 100:.2f}"}
                for (kind, start, end, dimension_type), totals in sorted(summary.items())
            ],
        }
        if output_path is not None:
            file_hash = hashlib.sha256()
            with open(temporary, "rb") as candidate:
                for chunk in iter(lambda: candidate.read(1024 * 1024), b""):
                    file_hash.update(chunk)
            evidence["file_sha256"] = file_hash.hexdigest()
            os.replace(temporary, output_path)
            temporary = None
        return evidence
    finally:
        if connection is not None:
            connection.close()
        if stream is not None:
            stream.close()
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)
        Path(db_name).unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "readback"))
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if (args.mode == "prepare") != (args.output is not None):
        parser.error("--output is required only for prepare")
    config = json.loads(args.config.read_text(encoding="utf-8-sig"))
    expected = {"metric_run_id", "dataset_id", "metric_version", "overview_rows",
                "window_start", "window_end"}
    if set(config) != expected:
        raise ValueError("dimension config does not match the fixed contract")
    with args.input.open("r", encoding="utf-8-sig", newline="") as source:
        if args.mode == "prepare":
            reader = csv.DictReader(source)
            if tuple(reader.fieldnames or ()) != SOURCE_COLUMNS:
                raise ValueError("Trino dimension CSV columns do not match the fixed contract")
            rows = reader
        else:
            rows = parse_mysql_batch(source)
        evidence = audit_dimension_rows(
            rows, output_path=args.output, scratch_dir=args.input.parent,
            metric_run_id=config["metric_run_id"], dataset_id=config["dataset_id"],
            metric_version=config["metric_version"],
            overview_rows=config["overview_rows"],
            window_start=config["window_start"], window_end=config["window_end"],
            source_has_identity=args.mode == "readback",
        )
    print(json.dumps(evidence, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
