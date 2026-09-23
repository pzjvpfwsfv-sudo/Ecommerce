"""All Olist records in this module are synthetic boundary fixtures."""

from __future__ import annotations

import csv
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from generators.olist_data.bundle import Acquisition, prepare_bundle
from generators.olist_data.csv_io import bounded_csv_reader
from generators.olist_data.normalization import RowValidationError, normalize_row
from generators.olist_data.schemas import DATASET_ID, TABLE_SPECS


EXPECTED_HEADERS = {
    "orders": (
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ),
    "order_items": (
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ),
    "order_payments": (
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    ),
    "order_reviews": (
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
    ),
    "customers": (
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ),
    "products": (
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ),
    "sellers": (
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ),
    "geolocation": (
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state",
    ),
    "category_translation": (
        "product_category_name",
        "product_category_name_english",
    ),
}

EXPECTED_FILES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

VALID_ROWS = {
    "orders": [
        "order-1",
        "customer-1",
        "delivered",
        "2017-01-02 10:11:12",
        "2017-01-02 10:15:00",
        "2017-01-03 09:00:00",
        "2017-01-05 12:30:00",
        "2017-01-08 00:00:00",
    ],
    "order_items": [
        "order-1",
        "1",
        "product-1",
        "seller-1",
        "2017-01-04 12:00:00",
        "0.1",
        "2.30",
    ],
    "order_payments": ["order-1", "1", "credit_card", "2", "2.40"],
    "order_reviews": [
        "review-1",
        "order-1",
        "5",
        "",
        "otimo\nproduto",
        "2017-01-06 00:00:00",
        "2017-01-07 10:00:00",
    ],
    "customers": ["customer-1", "person-1", "01001", "sao paulo", "SP"],
    "products": [
        "product-1",
        "beleza_saude",
        "20",
        "100",
        "1",
        "100.5",
        "10",
        "5",
        "8",
    ],
    "sellers": ["seller-1", "01001", "sao paulo", "SP"],
    "geolocation": ["01001", "-23.5505", "-46.6333", "sao paulo", "SP"],
    "category_translation": ["beleza_saude", "health_beauty"],
}


def _csv_bytes(entity: str, rows: list[list[str]] | None = None, *, bom: bool = False) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(EXPECTED_HEADERS[entity])
    for row in rows or [VALID_ROWS[entity]]:
        writer.writerow(row)
    prefix = "\ufeff" if bom else ""
    return (prefix + stream.getvalue()).encode("utf-8")


def _write_bundle(
    root: Path,
    *,
    overrides: dict[str, bytes] | None = None,
    archive_members: list[tuple[str, bytes]] | None = None,
) -> tuple[Path, Path, dict[str, bytes]]:
    input_dir = root / "raw"
    input_dir.mkdir(parents=True)
    payloads = {
        EXPECTED_FILES[entity]: _csv_bytes(entity)
        for entity in EXPECTED_HEADERS
    }
    payloads.update(overrides or {})
    for name, payload in payloads.items():
        (input_dir / name).write_bytes(payload)
    archive = root / "olist.zip"
    members = archive_members or sorted(payloads.items())
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, payload in members:
            bundle.writestr(name, payload)
    return archive, input_dir, payloads


def _acquisition() -> Acquisition:
    return Acquisition(
        kaggle_version="2",
        acquired_at="2026-09-22T00:00:00+00:00",
        license_name="synthetic-test-license",
        license_url="https://example.test/license",
    )


def _independent_bundle_digest(files: list[dict[str, object]], payloads: dict[str, bytes]) -> str:
    digest = sha256()
    for entry in sorted(files, key=lambda item: str(item["name"])):
        name = str(entry["name"])
        payload = payloads[name]
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(payload)
        digest.update(b"\0")
        digest.update(str(entry["row_count"]).encode("ascii"))
        digest.update(b"\0")
        digest.update(sha256(payload).hexdigest().encode("ascii"))
    return digest.hexdigest()


class OlistSchemaAndNormalizationTest(unittest.TestCase):
    def test_registry_pins_official_headers_files_keys_and_targets(self):
        self.assertEqual(set(EXPECTED_HEADERS), set(TABLE_SPECS))
        for entity, expected in EXPECTED_HEADERS.items():
            with self.subTest(entity=entity):
                spec = TABLE_SPECS[entity]
                self.assertEqual(expected, spec.headers)
                self.assertEqual(EXPECTED_FILES[entity], spec.source_file)
                self.assertEqual(f"{entity}_src_v1", spec.target_table)
                self.assertTrue(spec.key_fields)

    def test_money_zip_null_and_timestamp_preserve_source_semantics(self):
        item = normalize_row(
            TABLE_SPECS["order_items"],
            dict(zip(EXPECTED_HEADERS["order_items"], VALID_ROWS["order_items"])),
            row_number=1,
        )
        customer = normalize_row(
            TABLE_SPECS["customers"],
            dict(zip(EXPECTED_HEADERS["customers"], VALID_ROWS["customers"])),
            row_number=1,
        )
        review = normalize_row(
            TABLE_SPECS["order_reviews"],
            dict(zip(EXPECTED_HEADERS["order_reviews"], VALID_ROWS["order_reviews"])),
            row_number=1,
        )
        self.assertEqual("0.1", item["price"])
        self.assertEqual("0.10", item["price_decimal"])
        self.assertEqual("2.30", item["freight_value"])
        self.assertEqual("2.30", item["freight_value_decimal"])
        self.assertEqual("01001", customer["customer_zip_code_prefix"])
        self.assertIsNone(review["review_comment_title"])
        self.assertEqual("otimo\nproduto", review["review_comment_message"])
        self.assertEqual("2017-01-04 12:00:00", item["shipping_limit_date"])
        self.assertNotIn("Z", item["shipping_limit_date"])

    def test_official_coordinate_precision_is_preserved_with_strict_world_bounds(self):
        spec = TABLE_SPECS["geolocation"]
        source = dict(zip(EXPECTED_HEADERS["geolocation"], VALID_ROWS["geolocation"]))
        normalized = normalize_row(
            spec,
            source
            | {
                "geolocation_lat": "-0.00004367379244740171",
                "geolocation_lng": "-6.8985590510710155",
            },
            row_number=1,
        )
        self.assertEqual("-0.00004367379244740171", normalized["geolocation_lat"])
        self.assertEqual("-6.8985590510710155", normalized["geolocation_lng"])
        for field, value in (
            ("geolocation_lat", "90.00000000000000000001"),
            ("geolocation_lng", "-180.00000000000000000001"),
        ):
            with self.subTest(field=field):
                with self.assertRaisesRegex(RowValidationError, f"invalid_{field}"):
                    normalize_row(spec, source | {field: value}, row_number=1)

    def test_identity_is_stable_and_changes_with_record_number_or_content(self):
        spec = TABLE_SPECS["order_items"]
        source = dict(zip(EXPECTED_HEADERS["order_items"], VALID_ROWS["order_items"]))
        first = normalize_row(spec, source, row_number=1)
        repeated = normalize_row(spec, source, row_number=1)
        second = normalize_row(spec, source, row_number=2)
        changed = normalize_row(spec, source | {"price": "0.2"}, row_number=1)
        canonical = json.dumps(
            [source[name].strip() for name in EXPECTED_HEADERS["order_items"]],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        expected = sha256(
            "\0".join((DATASET_ID, spec.source_file, "1", canonical)).encode("utf-8")
        ).hexdigest()
        self.assertEqual(expected, first["source_row_id"])
        self.assertEqual(first, repeated)
        self.assertNotEqual(first["source_row_id"], second["source_row_id"])
        self.assertNotEqual(first["source_row_id"], changed["source_row_id"])

    def test_invalid_amount_ids_dates_and_row_shapes_fail_with_reason_codes(self):
        item_spec = TABLE_SPECS["order_items"]
        base = dict(zip(EXPECTED_HEADERS["order_items"], VALID_ROWS["order_items"]))
        for value in ("-1.00", "1e2", "NaN", "1.234", "+1.00", "1000000000000000000.00"):
            with self.subTest(price=value):
                with self.assertRaisesRegex(RowValidationError, "invalid_price"):
                    normalize_row(item_spec, base | {"price": value}, 1)
        with self.assertRaisesRegex(RowValidationError, "missing_order_id"):
            normalize_row(item_spec, base | {"order_id": "  "}, 1)
        with self.assertRaisesRegex(RowValidationError, "invalid_shipping_limit_date"):
            normalize_row(item_spec, base | {"shipping_limit_date": "2017-02-30 12:00:00"}, 1)
        with self.assertRaisesRegex(RowValidationError, "invalid_row_shape"):
            normalize_row(item_spec, base | {"unknown": "x"}, 1)
        with self.assertRaisesRegex(RowValidationError, "invalid_order_id"):
            normalize_row(item_spec, base | {"order_id": "order\x00hidden"}, 1)
        with self.assertRaisesRegex(ValueError, "positive record number"):
            normalize_row(item_spec, base, 0)

    def test_bounded_reader_handles_multiline_bom_and_wide_records(self):
        source = io.StringIO('a,b\n1,"two\nlines"\n', newline="")
        self.assertEqual([["a", "b"], ["1", "two\nlines"]], list(bounded_csv_reader(source)))
        self.assertEqual([["a", "b"]], list(bounded_csv_reader(io.StringIO("\ufeffa,b\n"))))
        with self.assertRaisesRegex(ValueError, "record.*limit"):
            list(bounded_csv_reader(io.StringIO("x" * 70_000), max_chars=65_536))


class OlistBundleTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir="D:\\EcommerceDev\\temp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_prepare_bundle_publishes_reconciled_manifest_and_jsonl(self):
        archive, input_dir, payloads = _write_bundle(self.root)
        output_root = self.root / "output"
        manifest = prepare_bundle(archive, input_dir, output_root, _acquisition())
        digest = manifest["source_bundle_sha256"]
        prepared = output_root / "prepared" / digest
        self.assertEqual(DATASET_ID, manifest["dataset_id"])
        self.assertEqual(digest, _independent_bundle_digest(manifest["files"], payloads))
        self.assertEqual(9, len(manifest["files"]))
        self.assertEqual(manifest, json.loads((prepared / "source-bundle.json").read_text(encoding="utf-8")))
        self.assertNotIn(str(self.root), json.dumps(manifest))
        self.assertNotIn("cookie", json.dumps(manifest).lower())
        self.assertNotIn("token", json.dumps(manifest).lower())
        for entry in manifest["files"]:
            with self.subTest(entity=entry["entity"]):
                self.assertEqual(1, entry["row_count"])
                self.assertEqual(1, entry["normalized"]["row_count"])
                target = prepared / entry["normalized"]["name"]
                line = json.loads(target.read_text(encoding="utf-8"))
                self.assertEqual(digest, line["source_bundle_sha256"])
                self.assertEqual(1, line["source_row_number"])
                self.assertRegex(line["source_row_id"], r"^[0-9a-f]{64}$")
        self.assertEqual([], list(output_root.rglob("*.part")))
        self.assertEqual([], list((output_root / "prepared").glob(".staging-*")))

    def test_exact_file_set_header_and_archive_bytes_are_required(self):
        cases = []
        archive, input_dir, payloads = _write_bundle(self.root / "missing")
        (input_dir / EXPECTED_FILES["sellers"]).unlink()
        cases.append((archive, input_dir, self.root / "out-missing", "missing_source_file"))

        archive, input_dir, payloads = _write_bundle(self.root / "extra")
        (input_dir / "extra.csv").write_text("x\n1\n", encoding="utf-8")
        cases.append((archive, input_dir, self.root / "out-extra", "unexpected_csv_file"))

        bad_header = _csv_bytes("orders").replace(b"order_id", b"order_uuid", 1)
        archive, input_dir, payloads = _write_bundle(
            self.root / "header", overrides={EXPECTED_FILES["orders"]: bad_header}
        )
        cases.append((archive, input_dir, self.root / "out-header", "invalid_header"))

        archive, input_dir, payloads = _write_bundle(self.root / "mismatch")
        (input_dir / EXPECTED_FILES["orders"]).write_bytes(
            _csv_bytes("orders", [VALID_ROWS["orders"][:-1] + ["2017-01-09 00:00:00"]])
        )
        cases.append((archive, input_dir, self.root / "out-mismatch", "archive_file_mismatch"))

        for archive, input_dir, output, reason in cases:
            with self.subTest(reason=reason):
                with self.assertRaisesRegex(ValueError, reason):
                    prepare_bundle(archive, input_dir, output, _acquisition())
                self.assertFalse((output / "prepared").exists())

    def test_archive_traversal_absolute_and_case_duplicates_are_rejected(self):
        for name in ("../evil.csv", "/absolute.csv", "C:/absolute.csv"):
            root = self.root / sha256(name.encode()).hexdigest()[:8]
            _, input_dir, payloads = _write_bundle(root)
            members = sorted(payloads.items()) + [(name, b"x")]
            archive = root / "bad.zip"
            with zipfile.ZipFile(archive, "w") as bundle:
                for member, payload in members:
                    bundle.writestr(member, payload)
            with self.subTest(member=name):
                with self.assertRaisesRegex(ValueError, "unsafe_archive_member"):
                    prepare_bundle(archive, input_dir, root / "out", _acquisition())

        root = self.root / "duplicate"
        _, input_dir, payloads = _write_bundle(root)
        members = sorted(payloads.items()) + [(EXPECTED_FILES["orders"].upper(), payloads[EXPECTED_FILES["orders"]])]
        archive = root / "duplicate.zip"
        with zipfile.ZipFile(archive, "w") as bundle:
            for member, payload in members:
                bundle.writestr(member, payload)
        with self.assertRaisesRegex(ValueError, "duplicate_archive_member"):
            prepare_bundle(archive, input_dir, root / "out", _acquisition())

    def test_output_inside_input_existing_target_and_bad_rows_leave_no_partial_data(self):
        archive, input_dir, _ = _write_bundle(self.root / "safe")
        with self.assertRaisesRegex(ValueError, "output_overlaps_input"):
            prepare_bundle(archive, input_dir, input_dir / "output", _acquisition())

        output = self.root / "published"
        manifest = prepare_bundle(archive, input_dir, output, _acquisition())
        manifest_path = output / "prepared" / manifest["source_bundle_sha256"] / "source-bundle.json"
        original = manifest_path.read_bytes()
        with self.assertRaises(FileExistsError):
            prepare_bundle(archive, input_dir, output, _acquisition())
        self.assertEqual(original, manifest_path.read_bytes())

        bad_root = self.root / "bad-row"
        bad_item = VALID_ROWS["order_items"].copy()
        bad_item[5] = "-1.00"
        bad_payload = _csv_bytes("order_items", [bad_item])
        archive, input_dir, _ = _write_bundle(
            bad_root, overrides={EXPECTED_FILES["order_items"]: bad_payload}
        )
        bad_output = bad_root / "output"
        with self.assertRaisesRegex(RowValidationError, "invalid_price"):
            prepare_bundle(archive, input_dir, bad_output, _acquisition())
        self.assertEqual([], list(bad_output.rglob("*.part")))
        self.assertEqual([], list((bad_output / "prepared").glob("[0-9a-f]" * 64)))

    def test_symlinked_input_file_is_rejected_when_supported(self):
        archive, input_dir, _ = _write_bundle(self.root / "symlink")
        source = input_dir / EXPECTED_FILES["orders"]
        real = input_dir / "orders-real.txt"
        source.replace(real)
        try:
            source.symlink_to(real)
        except OSError:
            real.replace(source)
            original = Path.is_symlink

            def simulated_symlink(path: Path) -> bool:
                return path == source or original(path)

            with patch.object(Path, "is_symlink", simulated_symlink):
                with self.assertRaisesRegex(ValueError, "symlink_source_file"):
                    prepare_bundle(archive, input_dir, self.root / "symlink-output", _acquisition())
        else:
            with self.assertRaisesRegex(ValueError, "symlink_source_file"):
                prepare_bundle(archive, input_dir, self.root / "symlink-output", _acquisition())

    @unittest.skipUnless(os.name == "nt" and hasattr(os.path, "isjunction"), "Windows junction test")
    def test_prepared_directory_junction_cannot_escape_output_root(self):
        archive, input_dir, _ = _write_bundle(self.root / "junction")
        output = self.root / "junction-output"
        outside = self.root / "outside"
        output.mkdir()
        outside.mkdir()
        junction = output / "prepared"
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(junction), str(outside)],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            self.skipTest("junction creation unavailable")
        self.assertTrue(os.path.isjunction(junction))
        try:
            with self.assertRaisesRegex(ValueError, "unsafe_prepared_directory"):
                prepare_bundle(archive, input_dir, output, _acquisition())
        finally:
            if os.path.isjunction(junction):
                os.rmdir(junction)

    def test_failure_during_normalization_removes_staging_directory(self):
        archive, input_dir, _ = _write_bundle(self.root / "interrupted")
        output = self.root / "interrupted-output"
        with patch(
            "generators.olist_data.bundle._write_normalized_file",
            side_effect=OSError("synthetic write failure"),
        ):
            with self.assertRaisesRegex(OSError, "synthetic write failure"):
                prepare_bundle(archive, input_dir, output, _acquisition())
        self.assertEqual([], list((output / "prepared").glob(".staging-*")))
        self.assertEqual([], list(output.rglob("*.part")))


if __name__ == "__main__":
    unittest.main()
