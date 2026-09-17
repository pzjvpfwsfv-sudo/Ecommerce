"""Artificial small source archives exercise sampling; not demo data."""

import csv
import gzip
from hashlib import sha256
import importlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from generators.real_data.download import source_config
from generators.real_data.normalization import FIELDS, normalize_event


def row(month="10", user="user-test-only", kind="view", price="1.20"):
    return [f"2019-{month}-01 01:00:00 UTC", kind, "product-test-only", "123", "electronics.phone", "", price, user, "session-test-only"]


class CrossMonthSampleTest(unittest.TestCase):
    def setUp(self):
        try:
            self.module = importlib.import_module("generators.real_data.sampling")
        except ModuleNotFoundError as exc:
            self.fail(f"cross-month sampling functionality is missing: {exc}")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = source_config()

    def archive(self, month, rows):
        source = self.config["months"][month]
        text = io.StringIO(newline="")
        writer = csv.writer(text)
        writer.writerow(FIELDS)
        writer.writerows(rows)
        path = self.root / source["source_file"]
        payload = gzip.compress(text.getvalue().encode("utf-8"))
        path.write_bytes(payload)
        manifest = source | {"dataset_id": self.config["dataset_id"], "scope": "full_file", "full_source_downloaded": True, "artifact_bytes": len(payload), "sha256": sha256(payload).hexdigest()}
        Path(str(path) + ".provenance.json").write_text(json.dumps(manifest), encoding="utf-8")
        return path

    def sample(self, inputs, output=None, **kwargs):
        return self.module.sample_users(inputs, output or self.root / "sample.jsonl", **({"basis_points": 10000} | kwargs))

    def test_all_selected_events_keep_original_ids_across_sorted_months(self):
        october = self.archive("2019-Oct", [row(), row(kind="cart")])
        november = self.archive("2019-Nov", [row("11", kind="purchase")])
        result = self.sample([november, october])
        events = [json.loads(line) for line in (self.root / "sample.jsonl").read_text(encoding="utf-8").splitlines()]
        self.assertEqual(["2019-Oct.csv.gz", "2019-Oct.csv.gz", "2019-Nov.csv.gz"], [x["source_file"] for x in events])
        self.assertEqual([1, 2, 1], [x["source_row_number"] for x in events])
        expected = normalize_event(dict(zip(FIELDS, row(kind="cart"))), dataset_id=self.config["dataset_id"], source_file="2019-Oct.csv.gz", row_number=2)
        self.assertEqual(expected, events[1])
        self.assertEqual(3, result["sample_rows"])
        self.assertEqual({"cart": 1, "purchase": 1, "view": 1}, result["event_counts"])
        self.assertTrue(all(x["gzip_integrity_verified"] for x in result["sources"]))
        self.assertEqual("2019-12-01T00:00:00+00:00", result["window_end_exclusive"])
        self.assertEqual(sha256((self.root / "sample.jsonl").read_bytes()).hexdigest(), result["sha256"])

    def test_hash_sample_keeps_all_events_of_selected_users_across_months(self):
        users = [f"test-{i}" for i in range(60)]
        october = self.archive("2019-Oct", [row(user=user) for user in users])
        november = self.archive("2019-Nov", [row("11", user=user, kind="purchase") for user in users])
        result = self.sample([october, november], basis_points=5000, seed="fixed-test-seed")
        events = [json.loads(line) for line in (self.root / "sample.jsonl").read_text(encoding="utf-8").splitlines()]
        chosen = {user for user in users if int.from_bytes(sha256((self.config["dataset_id"] + "\0fixed-test-seed\0" + user).encode()).digest()[:8], "big") % 10000 < 5000}
        self.assertGreater(len(chosen), 0)
        self.assertLess(len(chosen), len(users))
        self.assertEqual(2 * len(chosen), result["sample_rows"])
        self.assertEqual(chosen, {e["user_id"] for e in events if e["source_file"].startswith("2019-Oct")})
        self.assertEqual(chosen, {e["user_id"] for e in events if e["source_file"].startswith("2019-Nov")})

    def test_invalid_selected_event_is_counted_not_hidden(self):
        october = self.archive("2019-Oct", [row(price="bad"), row(kind="cart")])
        november = self.archive("2019-Nov", [row("11")])
        result = self.sample([october, november])
        self.assertEqual(3, result["selected_candidates"])
        self.assertEqual(2, result["sample_rows"])
        self.assertEqual({"invalid_price": 1}, result["selected_rejection_reasons"])
        self.assertFalse(result["selected_paths_lossless"])

    def test_prefix_duplicate_and_missing_month_are_rejected(self):
        october = self.archive("2019-Oct", [row()])
        november = self.archive("2019-Nov", [row("11")])
        for inputs in ([october], [october, october]):
            with self.subTest(inputs=inputs), self.assertRaises(ValueError):
                self.sample(inputs)
        path = Path(str(november) + ".provenance.json")
        manifest = json.loads(path.read_text()) | {"scope": "prefix", "full_source_downloaded": False}
        path.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaises(ValueError):
            self.sample([october, november])
        self.assertFalse((self.root / "sample.jsonl").exists())

    def test_checksum_or_crc_failure_does_not_publish_sample(self):
        for corrupt in (False, True):
            with self.subTest(corrupt=corrupt):
                october = self.archive("2019-Oct", [row()])
                november = self.archive("2019-Nov", [row("11")])
                manifest_path = Path(str(november) + ".provenance.json")
                manifest = json.loads(manifest_path.read_text())
                if corrupt:
                    november.write_bytes(november.read_bytes()[:-5])
                    manifest["artifact_bytes"] = november.stat().st_size
                    manifest["sha256"] = sha256(november.read_bytes()).hexdigest()
                else:
                    manifest["sha256"] = "0" * 64
                manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                with self.assertRaises((ValueError, OSError, EOFError)):
                    self.sample([october, november])
                self.assertFalse((self.root / "sample.jsonl").exists())
                self.assertFalse((self.root / "sample.jsonl.provenance.json").exists())

    def test_month_mismatch_even_on_unselected_record_fails(self):
        october = self.archive("2019-Oct", [row("11")])
        november = self.archive("2019-Nov", [row("11")])
        with self.assertRaisesRegex(ValueError, "window"):
            self.sample([october, november], basis_points=1)

    def test_month_mismatch_cannot_hide_behind_missing_user_or_wrong_shape(self):
        for index, invalid in enumerate((row("11", user=""), row("11")[:-1])):
            with self.subTest(invalid=invalid):
                october = self.archive("2019-Oct", [invalid])
                november = self.archive("2019-Nov", [row("11")])
                output = self.root / f"sample-{index}.jsonl"
                with self.assertRaisesRegex(ValueError, "window"):
                    self.sample([october, november], output=output)
                self.assertFalse(output.exists())
                self.assertFalse(Path(str(output) + ".provenance.json").exists())

    def test_excessively_wide_record_does_not_publish_sample(self):
        october = self.archive("2019-Oct", [row() + ["x"] * 200000])
        november = self.archive("2019-Nov", [row("11")])
        with self.assertRaisesRegex(ValueError, "record.*limit"):
            self.sample([october, november])
        self.assertFalse((self.root / "sample.jsonl").exists())
        self.assertFalse((self.root / "sample.jsonl.provenance.json").exists())

    def test_missing_user_within_window_remains_an_explicit_rejection(self):
        october = self.archive("2019-Oct", [row(user=""), row()])
        november = self.archive("2019-Nov", [row("11")])
        result = self.sample([october, november])
        self.assertEqual(1, result["source_shape_rejected_rows"])
        self.assertEqual(2, result["sample_rows"])
        self.assertFalse(result["selected_paths_lossless"])

    def test_capped_cardinality_and_blank_source_records_remain_explicit(self):
        october = self.archive("2019-Oct", [row(user="a"), [], row(user="b")])
        november = self.archive("2019-Nov", [row("11", user="c")])
        result = self.sample([october, november], distinct_limit=1)
        self.assertEqual(4, sum(x["rows_scanned"] for x in result["sources"]))
        self.assertEqual(1, result["source_shape_rejected_rows"])
        self.assertEqual({"observed_distinct": 1, "exact": False}, result["cardinality"]["user_id"])
        events = [json.loads(line) for line in (self.root / "sample.jsonl").read_text(encoding="utf-8").splitlines()]
        self.assertEqual(3, events[1]["source_row_number"])

    def test_existing_output_is_untouched(self):
        path = self.root / "sample.jsonl"
        path.write_text("keep", encoding="utf-8")
        with self.assertRaises(FileExistsError):
            self.sample([])
        self.assertEqual("keep", path.read_text())


if __name__ == "__main__":
    unittest.main()
