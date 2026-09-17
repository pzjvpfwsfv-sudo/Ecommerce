"""Small fabricated edge cases for tests only, never demonstration data."""

import csv
import gzip
import importlib
import json
from pathlib import Path
import tempfile
import unittest


FIELDS = "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session".split(",")


def fixture(**changes):
    row = dict(zip(FIELDS, ["2019-10-01 00:00:00 UTC", "view", "1001", "2053013552226107603", "electronics.phone", "", "12.30", "5001", "session-test-only"]))
    return row | changes


class RealDataTest(unittest.TestCase):
    def setUp(self):
        try:
            self.normalization = importlib.import_module("generators.real_data.normalization")
            self.profiling = importlib.import_module("generators.real_data.profiling")
        except ModuleNotFoundError as exc:
            self.fail(f"real data functionality is missing: {exc}")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def normalize(self, row, **kwargs):
        return self.normalization.normalize_event(row, **({"dataset_id": "test-only-v1", "source_file": "fixture.csv.gz", "row_number": 1} | kwargs))

    def write_csv(self, rows, name="input.csv"):
        path = self.root / name
        opener = gzip.open if name.endswith(".gz") else open
        with opener(path, "wt", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def profile(self, path, **kwargs):
        return self.profiling.profile_file(path, dataset_id="test-only-v1", source_file="fixture.csv.gz", **kwargs)

    def test_preserves_real_fields_time_and_missing_values(self):
        event = self.normalize(fixture(user_session="", category_code=""))
        self.assertEqual("2019-10-01T00:00:00+00:00", event["event_time"])
        self.assertEqual("2053013552226107603", event["category_id"])
        self.assertEqual("12.30", event["price"])
        self.assertIsNone(event["brand"])
        self.assertIsNone(event["user_session"])
        self.assertIsNone(event["category_code"])
        self.assertFalse({"channel", "device_type", "page_id", "order_id"} & event.keys())

    def test_ids_are_stable_but_isolate_source_rows_and_datasets(self):
        first = self.normalize(fixture())
        self.assertEqual(first, self.normalize(fixture()))
        for overrides in ({"row_number": 2}, {"dataset_id": "another"}, {"source_file": "other.csv.gz"}):
            self.assertNotEqual(first["event_id"], self.normalize(fixture(), **overrides)["event_id"])
        self.assertNotEqual(first["event_id"], self.normalize(fixture(price="13.00"))["event_id"])

    def test_invalid_business_values_are_rejected(self):
        for change in ({"price": "NaN"}, {"price": "Infinity"}, {"price": "-1"}, {"price": "1e1000000"}, {"price": "1.001"}, {"event_type": "click"}, {"event_time": "2020-02-30 00:00:00 UTC"}, {"user_id": ""}, {"product_id": ""}):
            with self.subTest(change=change):
                with self.assertRaises(self.normalization.EventValidationError):
                    self.normalize(fixture(**change))

    def test_profiles_counts_order_missing_and_exact_purchase_price_sum(self):
        path = self.write_csv([fixture(event_type="purchase", price="0.10", event_time="2019-10-02 00:00:00 UTC"), fixture(event_type="purchase", price="0.20"), fixture(user_session=""), fixture(price="bad")])
        report = self.profile(path, scope="prefix")
        self.assertEqual((4, 3, 1), (report["rows_scanned"], report["valid_rows"], report["rejected_rows"]))
        self.assertEqual({"purchase": 2, "view": 1}, report["event_counts"])
        self.assertEqual({"2019-10-01": 2, "2019-10-02": 1}, report["daily_counts"])
        self.assertEqual("0.30", report["purchase_event_price_sum"])
        self.assertEqual(3, report["missing_on_valid_rows"]["brand"])
        self.assertEqual(1, report["missing_on_valid_rows"]["user_session"])
        self.assertEqual(1, report["out_of_order_rows"])
        self.assertEqual("prefix", report["declared_scope"])
        self.assertIn("not_a_complete_analysis_window", report["warnings"])
        self.assertEqual(64, len(report["input_sha256"]))

    def test_gzip_and_plain_csv_have_same_business_profile(self):
        plain = self.profile(self.write_csv([fixture()]))
        compressed = self.profile(self.write_csv([fixture()], "input.csv.gz"))
        for key in ("valid_rows", "event_counts", "time_range", "cardinality"):
            self.assertEqual(plain[key], compressed[key])

    def test_cap_and_scan_limit_do_not_claim_full_counts(self):
        path = self.write_csv([fixture(user_id=str(i)) for i in range(4)])
        report = self.profile(path, max_rows=3, distinct_limit=2, scope="full_file")
        self.assertEqual(3, report["rows_scanned"])
        self.assertTrue(report["scan_limited"])
        self.assertEqual({"observed_distinct": 2, "exact": False}, report["cardinality"]["user_id"])
        self.assertIn("not_a_complete_analysis_window", report["warnings"])

    def test_normalized_output_retains_source_record_numbers(self):
        path = self.write_csv([fixture(), fixture(price="bad"), fixture(event_type="cart")])
        output = self.root / "normalized.jsonl"
        report = self.profile(path, normalized_path=output)
        events = [json.loads(line) for line in output.read_text().splitlines()]
        self.assertEqual([1, 3], [event["source_row_number"] for event in events])
        self.assertEqual(report["valid_rows"], len(events))

    def test_blank_records_are_rejected_without_shifting_source_identity(self):
        path = self.write_csv([fixture(), fixture(event_type="cart")])
        lines = path.read_text(encoding="utf-8").splitlines()
        path.write_text("\n".join([lines[0], lines[1], "", lines[2]]) + "\n", encoding="utf-8")
        output = self.root / "normalized.jsonl"
        report = self.profile(path, scope="prefix", normalized_path=output)
        events = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(3, report["rows_scanned"])
        self.assertEqual(1, report["rejection_reasons"]["invalid_row_shape"])
        self.assertEqual([1, 3], [event["source_row_number"] for event in events])

    def test_user_sample_cannot_invent_original_row_numbers(self):
        path = self.write_csv([fixture()])
        output = self.root / "sample.jsonl"
        with self.assertRaisesRegex(ValueError, "original record"):
            self.profile(path, scope="user_sample", normalized_path=output)
        self.assertFalse(output.exists())
        self.assertEqual(1, self.profile(path, scope="user_sample")["valid_rows"])

    def test_existing_output_and_input_are_never_overwritten(self):
        path = self.write_csv([fixture()])
        before = path.read_bytes()
        with self.assertRaises((ValueError, FileExistsError)):
            self.profile(path, normalized_path=path)
        self.assertEqual(before, path.read_bytes())
        output = self.root / "existing.jsonl"
        output.write_text("preserve", encoding="utf-8")
        with self.assertRaises(FileExistsError):
            self.profile(path, normalized_path=output)
        self.assertEqual("preserve", output.read_text())

    def test_bad_header_and_invalid_limits_fail_before_output(self):
        path = self.root / "bad.csv"
        path.write_text("user_id,price\n1,2\n", encoding="utf-8")
        output = self.root / "out.jsonl"
        with self.assertRaises(ValueError):
            self.profile(path, normalized_path=output)
        self.assertFalse(output.exists())
        valid = self.write_csv([fixture()])
        for kwargs in ({"max_rows": 0}, {"distinct_limit": 0}, {"scope": "complete_shop"}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.profile(valid, **kwargs)

    def test_damaged_gzip_does_not_publish_partial_normalized_output(self):
        path = self.write_csv([fixture()], "input.csv.gz")
        path.write_bytes(path.read_bytes()[:-6])
        output = self.root / "out.jsonl"
        with self.assertRaises((EOFError, OSError)):
            self.profile(path, normalized_path=output)
        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
