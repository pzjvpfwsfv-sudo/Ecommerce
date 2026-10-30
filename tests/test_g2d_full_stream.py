import csv
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parent.parent


class G2dFullDimensionStreamTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir="D:/EcommerceDev/temp")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.path = self.root / "dimension.csv"
        self.overview = [
            {"window_type": "DAY", "window_start": "2019-10-01", "window_end": "2019-10-01",
             "view_count": "1", "cart_count": "1", "purchase_count": "0", "purchase_amount_proxy": "0.00"},
            {"window_type": "DAY", "window_start": "2019-10-02", "window_end": "2019-10-02",
             "view_count": "1", "cart_count": "0", "purchase_count": "1", "purchase_amount_proxy": "10.00"},
            {"window_type": "FULL", "window_start": "2019-10-01", "window_end": "2019-10-02",
             "view_count": "2", "cart_count": "1", "purchase_count": "1", "purchase_amount_proxy": "10.00"},
        ]

    def rows(self):
        rows = []
        for dimension_type in ("product", "category", "brand"):
            dimension_id = "UNKNOWN" if dimension_type == "brand" else f"{dimension_type}-1"
            name = "" if dimension_type == "brand" else f"{dimension_type} one"
            for overview in self.overview:
                rows.append({
                    "window_type": overview["window_type"],
                    "window_start": overview["window_start"],
                    "window_end": overview["window_end"],
                    "dimension_type": dimension_type,
                    "dimension_id": dimension_id,
                    "dimension_name": name,
                    "is_unknown": "true" if dimension_type == "brand" else "false",
                    "view_count": overview["view_count"],
                    "cart_count": overview["cart_count"],
                    "purchase_count": overview["purchase_count"],
                    "unique_user_count": "1",
                    "purchase_amount_proxy": overview["purchase_amount_proxy"],
                })
        return rows

    def audit(self, rows):
        try:
            from scripts.g2d_full_stream import audit_dimension_rows
        except ImportError as exc:
            self.fail(f"streaming dimension audit is missing: {exc}")
        return audit_dimension_rows(
            rows, output_path=self.path, scratch_dir=self.root,
            metric_run_id="behavior-v1-s9", dataset_id="rees46-multicategory",
            metric_version="behavior-v1", overview_rows=self.overview,
            window_start="2019-10-01", window_end="2019-10-02",
        )

    def test_streamed_candidate_matches_existing_canonical_digest(self):
        evidence = self.audit(iter(self.rows()))
        self.assertEqual(9, evidence["row_count"])
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), evidence["file_sha256"])
        with self.path.open(newline="", encoding="utf-8") as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual("1", next(row for row in rows if row["dimension_type"] == "brand")["is_unknown"])
        self.assertEqual("behavior-v1-s9", rows[0]["metric_run_id"])
        self.assertEqual(9, len(rows))
        escaped = str(self.path).replace("'", "''")
        script = (
            "Import-Module ./scripts/lib/G2d.BehaviorMetrics.psm1 -Force; "
            f"$rows=ConvertFrom-G2dCsv -CsvText ([IO.File]::ReadAllText('{escaped}')); "
            "$columns=@('metric_run_id','dataset_id','metric_version','window_type',"
            "'window_start','window_end','dimension_type','dimension_id','dimension_name',"
            "'is_unknown','view_count','cart_count','purchase_count','unique_user_count',"
            "'purchase_amount_proxy'); "
            "$keys=@('metric_run_id','window_type','window_start','dimension_type','dimension_id'); "
            "Get-G2dCanonicalDigest -Rows $rows -Columns $columns -UniqueKeyColumns $keys"
        )
        result = subprocess.run(
            ["pwsh", "-NoProfile", "-Command", script], cwd=ROOT,
            capture_output=True, text=True, check=True,
        )
        self.assertEqual(result.stdout.strip(), evidence["canonical_sha256"])

    def test_duplicate_key_and_mismatched_full_rollup_are_rejected(self):
        rows = self.rows()
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.audit(iter(rows + [rows[0]]))
        self.assertFalse(self.path.exists())
        changed = self.rows()
        changed[-1]["purchase_count"] = "0"
        with self.assertRaisesRegex(ValueError, "dimension totals"):
            self.audit(iter(changed))
        self.assertFalse(self.path.exists())

    def test_candidate_file_hash_does_not_load_the_whole_file_into_memory(self):
        with patch("pathlib.Path.read_bytes", side_effect=AssertionError("unbounded file read")):
            evidence = self.audit(iter(self.rows()))
        self.assertEqual(9, evidence["row_count"])

    def test_full_dimension_id_union_must_match_days_even_when_totals_match(self):
        rows = self.rows()
        extra = dict(rows[0], dimension_id="product-2", dimension_name="product two",
                     view_count="0", cart_count="0", purchase_count="0",
                     unique_user_count="0", purchase_amount_proxy="0.00")
        rows.append(extra)
        with self.assertRaisesRegex(ValueError, "FULL identities or totals"):
            self.audit(iter(rows))
        self.assertFalse(self.path.exists())

    def test_mysql_batch_decoder_handles_escaped_cells_without_loading_all_rows(self):
        try:
            from scripts.g2d_full_stream import parse_mysql_batch
        except ImportError as exc:
            self.fail(f"streaming MySQL decoder is missing: {exc}")
        lines = iter(["dimension_id\tdimension_name\tis_unknown\n", "p1\tline\\ntext\t1\n"])
        rows = parse_mysql_batch(lines)
        self.assertEqual(
            [{"dimension_id": "p1", "dimension_name": "line\ntext", "is_unknown": "1"}],
            list(rows),
        )

    def test_cli_prepares_candidate_and_verifies_readback_from_files(self):
        config = self.root / "config.json"
        config.write_text(json.dumps({
            "metric_run_id": "behavior-v1-s9", "dataset_id": "rees46-multicategory",
            "metric_version": "behavior-v1", "overview_rows": self.overview,
            "window_start": "2019-10-01", "window_end": "2019-10-02",
        }), encoding="utf-8")
        raw = self.root / "trino.csv"
        with raw.open("w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=list(self.rows()[0]))
            writer.writeheader()
            writer.writerows(self.rows())
        command = [sys.executable, str(ROOT / "scripts/g2d_full_stream.py")]
        prepared = subprocess.run(
            command + ["prepare", "--input", str(raw), "--config", str(config),
                       "--output", str(self.path)],
            cwd=ROOT, capture_output=True, text=True, encoding="utf-8", check=True,
        )
        evidence = json.loads(prepared.stdout)
        self.assertEqual(9, evidence["row_count"])
        mysql = self.root / "doris.tsv"
        with self.path.open(newline="", encoding="utf-8") as candidate:
            rows = list(csv.DictReader(candidate))
        with mysql.open("w", encoding="utf-8", newline="") as output:
            output.write("\t".join(rows[0]) + "\n")
            for row in rows:
                output.write("\t".join(row.values()) + "\n")
        readback = subprocess.run(
            command + ["readback", "--input", str(mysql), "--config", str(config)],
            cwd=ROOT, capture_output=True, text=True, check=True,
        )
        self.assertEqual(evidence["canonical_sha256"],
                         json.loads(readback.stdout)["canonical_sha256"])
        self.assertEqual(evidence["file_sha256"],
                         hashlib.sha256(self.path.read_bytes()).hexdigest())

    def test_staged_refresh_plan_is_fixed_to_the_full_run(self):
        script = ROOT / "scripts/refresh_g2d_full_staged.ps1"
        planned = subprocess.run(
            ["pwsh", "-NoProfile", "-File", str(script), "-Phase", "Prepare",
             "-SourceRunId", "g5-full-01", "-PlanOnly"],
            cwd=ROOT, capture_output=True, text=True, encoding="utf-8", check=True,
        )
        plan = json.loads(planned.stdout)
        self.assertEqual("real_behavior_detail_v1_g5_full_01", plan["source_table"])
        self.assertEqual(["Prepare", "Publish"], plan["phases"])
        rejected = subprocess.run(
            ["pwsh", "-NoProfile", "-File", str(script), "-Phase", "Prepare",
             "-SourceRunId", "g5-a100k-02", "-PlanOnly"],
            cwd=ROOT, capture_output=True, text=True, encoding="utf-8",
        )
        self.assertNotEqual(0, rejected.returncode)

    def test_staged_doris_readback_uses_unambiguous_host_argument(self):
        script = (ROOT / "scripts/refresh_g2d_full_staged.ps1").read_text(encoding="utf-8")
        self.assertIn("mysql --host=127.0.0.1 --port=9030 --user=root --batch", script)
