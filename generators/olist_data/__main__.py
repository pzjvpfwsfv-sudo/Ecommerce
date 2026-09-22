from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys
import zipfile

from .bundle import Acquisition, prepare_bundle


def _d_drive(path: Path) -> bool:
    return Path(path).resolve(strict=False).drive.casefold() == "d:"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify and normalize the official Olist Version 2 bundle."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser(
        "prepare-bundle",
        help="Verify one official archive plus extracted directory and publish canonical JSONL.",
    )
    prepare.add_argument("--archive", type=Path, required=True)
    prepare.add_argument("--input-dir", type=Path, required=True)
    prepare.add_argument("--output-root", type=Path, required=True)
    prepare.add_argument("--acquired-at", required=True)
    prepare.add_argument("--license-name", required=True)
    prepare.add_argument("--license-url", required=True)
    args = parser.parse_args(argv)

    try:
        paths = (args.archive, args.input_dir, args.output_root)
        if not all(path.is_absolute() and _d_drive(path) for path in paths):
            raise ValueError("paths_must_be_on_d_drive")
        result = prepare_bundle(
            args.archive,
            args.input_dir,
            args.output_root,
            Acquisition(
                kaggle_version="2",
                acquired_at=args.acquired_at,
                license_name=args.license_name,
                license_url=args.license_url,
            ),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except FileExistsError:
        print("olist-data error: bundle already exists", file=sys.stderr)
        return 1
    except (csv.Error, UnicodeError, zipfile.BadZipFile):
        print("olist-data error: malformed_source_bundle", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"olist-data error: {exc}", file=sys.stderr)
        return 1
    except OSError:
        print("olist-data error: io_failure", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
