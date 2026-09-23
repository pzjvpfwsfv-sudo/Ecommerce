from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import re
from typing import Mapping

from .schemas import DATASET_ID, MONEY_FIELDS, SCHEMA_VERSION, FieldSpec, TableSpec


_UINT = re.compile(r"[0-9]+\Z")
_MONEY = re.compile(r"[0-9]{1,18}(?:\.[0-9]{1,2})?\Z")
_NONNEGATIVE_DECIMAL = re.compile(r"[0-9]{1,18}(?:\.[0-9]{1,6})?\Z")
_COORDINATE = re.compile(r"-?[0-9]{1,3}(?:\.[0-9]{1,20})?\Z")
_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


class RowValidationError(ValueError):
    """A stable reason code; source row content is never included."""


def _validate_decimal(field: FieldSpec, value: str) -> tuple[str, str | None]:
    pattern = _MONEY if field.name in MONEY_FIELDS else _NONNEGATIVE_DECIMAL
    if not pattern.fullmatch(value):
        raise RowValidationError(f"invalid_{field.name}")
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        raise RowValidationError(f"invalid_{field.name}") from None
    if field.minimum is not None and parsed < field.minimum:
        raise RowValidationError(f"invalid_{field.name}")
    derived = format(parsed, ".2f") if field.name in MONEY_FIELDS else None
    return value, derived


def _normalize_field(field: FieldSpec, raw: str) -> tuple[object, str | None]:
    value = raw.strip()
    if not value:
        if field.required:
            raise RowValidationError(f"missing_{field.name}")
        return None, None
    if len(value) > 65_536:
        raise RowValidationError(f"field_too_long_{field.name}")
    if "\x00" in value or (
        field.kind in {"id", "zip"}
        and any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise RowValidationError(f"invalid_{field.name}")
    if field.kind in {"id", "text", "zip"}:
        return value, None
    if field.kind == "timestamp":
        try:
            parsed = datetime.strptime(value, _TIMESTAMP_FORMAT)
        except ValueError:
            raise RowValidationError(f"invalid_{field.name}") from None
        return parsed.strftime(_TIMESTAMP_FORMAT), None
    if field.kind == "uint":
        if not _UINT.fullmatch(value):
            raise RowValidationError(f"invalid_{field.name}")
        parsed = Decimal(value)
        if field.minimum is not None and parsed < field.minimum:
            raise RowValidationError(f"invalid_{field.name}")
        return value, None
    if field.kind == "decimal":
        return _validate_decimal(field, value)
    if field.kind == "coordinate":
        if not _COORDINATE.fullmatch(value):
            raise RowValidationError(f"invalid_{field.name}")
        parsed = Decimal(value)
        bound = Decimal("90") if field.name.endswith("lat") else Decimal("180")
        if parsed < -bound or parsed > bound:
            raise RowValidationError(f"invalid_{field.name}")
        return format(parsed, "f"), None
    raise ValueError(f"unsupported field kind: {field.kind}")


def normalize_row(
    spec: TableSpec,
    row: Mapping[str, str],
    row_number: int,
) -> dict[str, object]:
    if row_number < 1:
        raise ValueError("positive record number is required")
    if set(row) != set(spec.headers) or any(not isinstance(value, str) for value in row.values()):
        raise RowValidationError("invalid_row_shape")

    normalized: dict[str, object] = {}
    for field in spec.fields:
        value, money = _normalize_field(field, row[field.name])
        normalized[field.name] = value
        if money is not None:
            normalized[f"{field.name}_decimal"] = money

    canonical_row = json.dumps(
        [normalized[field.name] for field in spec.fields],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    identity = "\0".join(
        (DATASET_ID, spec.source_file, str(row_number), canonical_row)
    )
    return normalized | {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "source_file": spec.source_file,
        "source_row_number": row_number,
        "source_row_id": sha256(identity.encode("utf-8")).hexdigest(),
    }
