from __future__ import annotations

import csv
from datetime import UTC, datetime
import gzip
import io
import json
from pathlib import Path
import re
from time import monotonic
from urllib.request import Request, urlopen

from .file_io import file_sha256, new_text_file, write_json
from .csv_io import bounded_csv_reader
from .normalization import FIELDS


SOURCE_CONFIG = Path(__file__).resolve().parents[2] / "configs" / "datasets" / "rees46-multicategory.json"


def source_config() -> dict:
    return json.loads(SOURCE_CONFIG.read_text(encoding="utf-8"))


class BudgetReader:
    def __init__(self, stream, limit: int):
        self.stream, self.limit, self.consumed = stream, limit, 0
        self.deadline = monotonic() + 300

    def read(self, size: int = -1) -> bytes:
        if monotonic() > self.deadline:
            raise TimeoutError("prefix download exceeded its five-minute budget")
        remaining = self.limit - self.consumed
        data = self.stream.read(min(size, remaining + 1) if size >= 0 else remaining + 1)
        self.consumed += len(data)
        if self.consumed > self.limit:
            raise ValueError("compressed download byte budget exceeded")
        return data


def download_prefix(output_path: Path, *, rows: int, max_compressed_bytes: int = 67108864) -> dict:
    if not 1 <= rows <= 1000000 or max_compressed_bytes < 1:
        raise ValueError("rows must be 1..1000000 and compressed byte budget must be positive")
    output_path = Path(output_path)
    if output_path.suffix.lower() != ".csv":
        raise ValueError("prefix output must be a .csv file")
    provenance_path = Path(str(output_path) + ".provenance.json")
    for path in (output_path, provenance_path):
        if path.exists():
            raise FileExistsError(path)
    config = source_config()
    request = Request(config["source_url"], headers={"Range": f"bytes=0-{max_compressed_bytes - 1}", "Accept-Encoding": "identity", "User-Agent": "EcommerceGraduationDataProfiler/1.0"})
    with urlopen(request, timeout=30) as response:
        if response.status != 206:
            raise ValueError("server did not honor the bounded range request")
        content_range = response.headers.get("Content-Range", "")
        match = re.fullmatch(r"bytes 0-(\d+)/(\d+)", content_range)
        if not match or not 0 <= int(match[1]) < min(max_compressed_bytes, int(match[2])):
            raise ValueError("invalid or oversized HTTP Content-Range")
        budgeted = BudgetReader(response, max_compressed_bytes)
        with gzip.GzipFile(fileobj=budgeted) as decompressed:
            with io.TextIOWrapper(decompressed, encoding="utf-8-sig", newline="") as text:
                reader = bounded_csv_reader(text)
                if next(reader, None) != list(FIELDS):
                    raise ValueError("downloaded CSV header does not match the registered source")
                with new_text_file(output_path) as output:
                    writer = csv.writer(output, lineterminator="\n")
                    writer.writerow(FIELDS)
                    for _ in range(rows):
                        row = next(reader, None)
                        if row is None:
                            raise ValueError("source ended before the requested prefix length")
                        writer.writerow(row)
    try:
        provenance = {
            "dataset_id": config["dataset_id"],
            "source_file": config["source_file"],
            "source_url": config["source_url"],
            "description_url": config["description_url"],
            "usage_note": config["usage_note"],
            "scope": "prefix",
            "full_source_downloaded": False,
            "rows_written": rows,
            "first_source_row_number": 1,
            "http_content_range": content_range,
            "source_compressed_bytes": int(match[2]),
            "compressed_bytes_read": budgeted.consumed,
            "downloaded_at": datetime.now(UTC).isoformat(),
            "artifact": str(output_path.resolve()),
            "artifact_bytes": output_path.stat().st_size,
            "sha256": file_sha256(output_path),
            "serialization": "CSV records in original order; LF line endings; no business values generated",
        }
        write_json(provenance_path, provenance)
    except Exception:
        output_path.unlink(missing_ok=True)
        raise
    return provenance
