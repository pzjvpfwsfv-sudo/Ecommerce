from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
import json
import re


FIELDS = (
    "event_time", "event_type", "product_id", "category_id", "category_code",
    "brand", "price", "user_id", "user_session",
)
OPTIONAL_FIELDS = ("category_id", "category_code", "brand", "user_session")
EVENT_TYPES = frozenset({"view", "cart", "remove_from_cart", "purchase"})
PRICE = re.compile(r"[0-9]{1,18}(?:\.[0-9]{1,2})?\Z")


class EventValidationError(ValueError):
    """The message is a stable reason code, never raw source content."""


def normalize_event(row: dict, *, dataset_id: str, source_file: str, row_number: int) -> dict:
    if not dataset_id or not source_file or row_number < 1:
        raise ValueError("source identity and positive record number are required")
    if set(row) != set(FIELDS) or any(not isinstance(value, str) for value in row.values()):
        raise EventValidationError("invalid_row_shape")
    cleaned = {key: value.strip() for key, value in row.items()}
    if any(len(value) > 512 for value in cleaned.values()):
        raise EventValidationError("field_too_long")
    for key in ("user_id", "product_id"):
        if not cleaned[key]:
            raise EventValidationError(f"missing_{key}")
    if cleaned["event_type"] not in EVENT_TYPES:
        raise EventValidationError("unsupported_event_type")
    try:
        timestamp = datetime.strptime(cleaned["event_time"], "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=UTC)
    except ValueError:
        raise EventValidationError("invalid_event_time") from None
    if not PRICE.fullmatch(cleaned["price"]):
        raise EventValidationError("invalid_price")
    event = cleaned | {
        "event_time": timestamp.isoformat(),
        "price": format(Decimal(cleaned["price"]), ".2f"),
    }
    for key in OPTIONAL_FIELDS:
        event[key] = cleaned[key] or None
    # Content changes cannot silently reuse the same source-row identity.
    identity = json.dumps([dataset_id, source_file, row_number, event], sort_keys=True, separators=(",", ":"))
    return event | {
        "schema_version": 1,
        "event_id": "real_" + sha256(identity.encode("utf-8")).hexdigest(),
        "dataset_id": dataset_id,
        "source_file": source_file,
        "source_row_number": row_number,
    }
