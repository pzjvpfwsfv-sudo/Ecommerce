from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys

from .download import download_prefix, source_config
from .file_io import file_sha256, write_json
from .monthly import download_month
from .profiling import profile_file
from .sampling import sample_users


def positive_int(text: str) -> int:
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prepare attributable REES46 historical events; no Kafka writes.")
    commands = parser.add_subparsers(dest="command", required=True)
    fetch = commands.add_parser("fetch-prefix", help="Download a bounded first-N sample, never a complete analysis window.")
    fetch.add_argument("--output", type=Path, required=True)
    fetch.add_argument("--rows", type=positive_int, default=10000)
    fetch.add_argument("--max-compressed-mib", type=positive_int, default=64)
    month = commands.add_parser("fetch-month", help="Download one registered complete archive, without decompression.")
    month.add_argument("--month", choices=tuple(source_config()["months"]), required=True)
    month.add_argument("--output-dir", type=Path, required=True)
    sample = commands.add_parser("sample-users", help="Keep all valid events of stable users across complete consecutive months.")
    sample.add_argument("--inputs", type=Path, nargs="+", required=True)
    sample.add_argument("--output", type=Path, required=True)
    sample.add_argument("--basis-points", type=positive_int, default=200)
    sample.add_argument("--seed", default="graduation-v1")
    sample.add_argument("--distinct-limit", type=positive_int, default=200000)
    profile = commands.add_parser("profile", help="Stream CSV/CSV.GZ and write a quality report.")
    profile.add_argument("--input", type=Path, required=True)
    profile.add_argument("--source-file", required=True, help="Original published filename, not a renamed local sample.")
    profile.add_argument("--scope", choices=("unverified", "prefix", "user_sample", "full_file"), default="unverified")
    profile.add_argument("--max-rows", type=positive_int)
    profile.add_argument("--distinct-limit", type=positive_int, default=10000)
    profile.add_argument("--report", type=Path, required=True)
    profile.add_argument("--normalized", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "fetch-prefix":
            result = download_prefix(args.output, rows=args.rows, max_compressed_bytes=args.max_compressed_mib * 1024 * 1024)
        elif args.command == "fetch-month":
            result = download_month(args.month, args.output_dir)
        elif args.command == "sample-users":
            result = sample_users(args.inputs, args.output, basis_points=args.basis_points, seed=args.seed, distinct_limit=args.distinct_limit)
        else:
            paths = [args.input.resolve(), args.report.resolve()]
            if args.normalized:
                paths.append(args.normalized.resolve())
            if len(set(paths)) != len(paths):
                raise ValueError("input, report and normalized output must use different paths")
            for path in (args.report, args.normalized):
                if path is not None and path.exists():
                    raise FileExistsError(path)
            config = source_config()
            provenance_path = Path(str(args.input) + ".provenance.json")
            provenance = None
            if provenance_path.exists():
                provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
                if not isinstance(provenance, dict):
                    raise ValueError("provenance manifest must be a JSON object")
                if provenance.get("dataset_id") != config["dataset_id"] or provenance.get("source_file") != args.source_file:
                    raise ValueError("source identity does not match the provenance manifest")
                if args.scope not in ("unverified", provenance.get("scope")):
                    raise ValueError("declared scope contradicts the provenance manifest")
                if provenance.get("sha256") != file_sha256(args.input):
                    raise ValueError("input checksum does not match the provenance manifest")
            result = profile_file(
                args.input, dataset_id=config["dataset_id"], source_file=args.source_file,
                scope=provenance["scope"] if provenance else args.scope,
                max_rows=args.max_rows, normalized_path=args.normalized, distinct_limit=args.distinct_limit,
            )
            if provenance is not None:
                result["provenance_file"] = str(provenance_path.resolve())
            write_json(args.report, result)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError, EOFError, csv.Error) as exc:
        print(f"real-data error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
