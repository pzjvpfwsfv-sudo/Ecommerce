from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
import shutil
import sys
from time import monotonic
from urllib.request import Request, urlopen

from .download import source_config
from .file_io import new_binary_file, write_json


def download_month(month: str, output_dir: Path, *, max_bytes: int = 3221225472) -> dict:
    config = source_config()
    if month not in config["months"] or max_bytes < 1:
        raise ValueError("a registered month and positive byte budget are required")
    source = config["months"][month]
    output_dir = Path(output_dir)
    output = output_dir / source["source_file"]
    manifest = Path(str(output) + ".provenance.json")
    for path in (output, manifest):
        if path.exists():
            raise FileExistsError(path)
    output_dir.mkdir(parents=True, exist_ok=True)
    request = Request(source["source_url"], headers={"Accept-Encoding": "identity", "User-Agent": "EcommerceGraduationDataProfiler/1.0"})
    digest = sha256()
    received = 0
    started = monotonic()
    with urlopen(request, timeout=30) as response:
        if response.status != 200:
            raise ValueError("full-month download requires a complete HTTP 200 response")
        expected = int(response.headers.get("Content-Length", "0"))
        if not 0 < expected <= max_bytes:
            raise ValueError("unknown or oversized full-month response length")
        if shutil.disk_usage(output_dir).free < expected + 1024 ** 3:
            raise OSError("not enough disk space for the source plus a 1 GiB safety reserve")
        with new_binary_file(output) as target:
            while chunk := response.read(min(1024 * 1024, expected - received + 1)):
                received += len(chunk)
                if received > expected or monotonic() - started > 1800:
                    raise ValueError("month download exceeded its size or 30-minute time budget")
                target.write(chunk)
                digest.update(chunk)
                if received % (128 * 1024 * 1024) == 0:
                    print(f"download {month}: {received}/{expected} bytes", file=sys.stderr, flush=True)
            if received != expected:
                raise ValueError("incomplete month response")
        remote_etag = response.headers.get("ETag")
        remote_modified = response.headers.get("Last-Modified")
    result = source | {
        "dataset_id": config["dataset_id"], "month": month,
        "scope": "full_file", "full_source_downloaded": True,
        "gzip_integrity_verified": False,
        "artifact": str(output.resolve()), "artifact_bytes": received,
        "sha256": digest.hexdigest(), "http_etag": remote_etag,
        "http_last_modified": remote_modified,
        "downloaded_at": datetime.now(UTC).isoformat(),
        "description_url": config["description_url"], "usage_note": config["usage_note"],
    }
    try:
        write_json(manifest, result)
    except Exception:
        output.unlink(missing_ok=True)
        raise
    return result
