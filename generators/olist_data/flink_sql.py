from __future__ import annotations

from pathlib import Path
import re

from .schemas import DATASET_ID, MONEY_FIELDS, SCHEMA_VERSION, TABLE_SPECS, FieldSpec


_BUNDLE_ID = re.compile(r"[0-9a-f]{64}\Z")
_TOKEN = re.compile(r"__[A-Z0-9_]+__")
_TOKENS = frozenset(
    {
        "__PIPELINE_NAME__",
        "__SOURCE_PATH__",
        "__SOURCE_COLUMNS__",
        "__TARGET_TABLE__",
        "__TARGET_COLUMNS__",
        "__SELECT_COLUMNS__",
        "__SOURCE_BUNDLE_SHA256__",
    }
)
_TEMPLATE_PATH = (
    Path(__file__).resolve().parents[2]
    / "jobs"
    / "sql"
    / "19_olist_source_ingest.sql.template"
)


def _source_columns(fields: tuple[FieldSpec, ...]) -> list[str]:
    columns: list[str] = []
    for field in fields:
        columns.append(f"{field.name} STRING")
        if field.name in MONEY_FIELDS:
            columns.append(f"{field.name}_decimal STRING")
    columns.extend(
        (
            "dataset_id STRING",
            "source_file STRING",
            "source_row_number BIGINT",
            "source_row_id STRING",
            "source_bundle_sha256 STRING",
            "schema_version INT",
        )
    )
    return columns


def _target_columns(fields: tuple[FieldSpec, ...]) -> list[str]:
    columns: list[str] = []
    for field in fields:
        if field.name in MONEY_FIELDS:
            columns.extend(
                (f"{field.name}_raw VARCHAR", f"{field.name} DECIMAL(38, 2)")
            )
        elif field.kind in {"id", "text", "zip"}:
            columns.append(f"{field.name} VARCHAR")
        elif field.kind == "uint":
            columns.append(f"{field.name} BIGINT")
        elif field.kind == "decimal":
            columns.append(f"{field.name} DECIMAL(38, 6)")
        elif field.kind == "coordinate":
            columns.append(f"{field.name} DECIMAL(38, 12)")
        elif field.kind == "timestamp":
            columns.append(f"{field.name} TIMESTAMP(3)")
        else:  # pragma: no cover - the frozen registry makes this unreachable.
            raise ValueError(f"unsupported field kind: {field.kind}")
    columns.extend(
        (
            "source_file VARCHAR NOT NULL",
            "source_row_number BIGINT NOT NULL",
            "source_row_id VARCHAR NOT NULL",
            "source_bundle_sha256 VARCHAR NOT NULL",
            "schema_version INTEGER NOT NULL",
        )
    )
    return columns


def _select_columns(fields: tuple[FieldSpec, ...]) -> list[str]:
    columns: list[str] = []
    for field in fields:
        if field.name in MONEY_FIELDS:
            columns.extend(
                (
                    f"{field.name} AS {field.name}_raw",
                    f"CAST({field.name}_decimal AS DECIMAL(38, 2)) AS {field.name}",
                )
            )
        elif field.kind in {"id", "text", "zip"}:
            columns.append(field.name)
        elif field.kind == "uint":
            columns.append(f"CAST({field.name} AS BIGINT) AS {field.name}")
        elif field.kind == "decimal":
            columns.append(
                f"CAST({field.name} AS DECIMAL(38, 6)) AS {field.name}"
            )
        elif field.kind == "coordinate":
            columns.append(
                f"CAST({field.name} AS DECIMAL(38, 12)) AS {field.name}"
            )
        elif field.kind == "timestamp":
            columns.append(
                f"TO_TIMESTAMP({field.name}, 'yyyy-MM-dd HH:mm:ss') AS {field.name}"
            )
        else:  # pragma: no cover - the frozen registry makes this unreachable.
            raise ValueError(f"unsupported field kind: {field.kind}")
    columns.extend(
        (
            "source_file",
            "source_row_number",
            "source_row_id",
            "source_bundle_sha256",
            "schema_version",
        )
    )
    return columns


def render_flink_ingest_sql(entity: str, source_bundle_sha256: str) -> str:
    if entity not in TABLE_SPECS:
        raise ValueError("unknown_entity")
    if not _BUNDLE_ID.fullmatch(source_bundle_sha256):
        raise ValueError("invalid_source_bundle_sha256")

    spec = TABLE_SPECS[entity]
    template = _TEMPLATE_PATH.read_text(encoding="utf-8")
    if set(_TOKEN.findall(template)) != _TOKENS:
        raise ValueError("invalid_ingest_template_tokens")

    values = {
        "__PIPELINE_NAME__": f"graduation-g2e-{entity}-{source_bundle_sha256}",
        "__SOURCE_PATH__": (
            f"/data/olist/prepared/{source_bundle_sha256}/normalized/{entity}.jsonl"
        ),
        "__SOURCE_COLUMNS__": ",\n    ".join(_source_columns(spec.fields)),
        "__TARGET_TABLE__": spec.target_table,
        "__TARGET_COLUMNS__": ",\n    ".join(_target_columns(spec.fields)),
        "__SELECT_COLUMNS__": ",\n    ".join(_select_columns(spec.fields)),
        "__SOURCE_BUNDLE_SHA256__": source_bundle_sha256,
    }
    sql = template
    for token, value in values.items():
        sql = sql.replace(token, value)
    if _TOKEN.search(sql):
        raise ValueError("unresolved_ingest_template_token")
    if DATASET_ID not in sql or f"schema_version = {SCHEMA_VERSION}" not in sql:
        raise ValueError("incomplete_ingest_identity_guard")
    return sql
