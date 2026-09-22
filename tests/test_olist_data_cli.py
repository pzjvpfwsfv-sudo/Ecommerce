"""CLI tests use synthetic Olist-shaped files, never business data."""

from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest

from generators.olist_data import __main__ as cli
from tests.test_olist_data import _write_bundle


class OlistDataCliTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir="D:\\EcommerceDev\\temp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.archive, self.input_dir, _ = _write_bundle(self.root)

    def call_cli(self, *args: str) -> tuple[int, str, str]:
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = cli.main(list(args))
        return code, stdout.getvalue(), stderr.getvalue()

    def valid_args(self, output: Path) -> tuple[str, ...]:
        return (
            "prepare-bundle",
            "--archive",
            str(self.archive),
            "--input-dir",
            str(self.input_dir),
            "--output-root",
            str(output),
            "--acquired-at",
            "2026-09-22T00:00:00+00:00",
            "--license-name",
            "synthetic-test-license",
            "--license-url",
            "https://example.test/license",
        )

    def test_prepare_bundle_prints_only_completed_manifest(self):
        output = self.root / "output"
        code, stdout, stderr = self.call_cli(*self.valid_args(output))
        self.assertEqual(0, code, stderr)
        self.assertEqual("", stderr)
        manifest = json.loads(stdout)
        self.assertRegex(manifest["source_bundle_sha256"], r"^[0-9a-f]{64}$")
        published = output / "prepared" / manifest["source_bundle_sha256"] / "source-bundle.json"
        self.assertTrue(published.is_file())

    def test_cli_rejects_non_d_drive_paths_before_publication(self):
        output = Path("C:/olist-must-not-be-created")
        code, stdout, stderr = self.call_cli(*self.valid_args(output))
        self.assertEqual(1, code)
        self.assertEqual("", stdout)
        self.assertIn("paths_must_be_on_d_drive", stderr)
        self.assertFalse(output.exists())

    def test_cli_rejects_invalid_acquisition_metadata_without_traceback(self):
        output = self.root / "output"
        args = list(self.valid_args(output))
        args[args.index("2026-09-22T00:00:00+00:00")] = "2026-09-22"
        code, stdout, stderr = self.call_cli(*args)
        self.assertEqual(1, code)
        self.assertEqual("", stdout)
        self.assertIn("invalid_acquired_at", stderr)
        self.assertNotIn("Traceback", stderr)
        self.assertFalse(output.exists())

        args = list(self.valid_args(output))
        args[args.index("https://example.test/license")] = "http://example.test/license"
        code, _, stderr = self.call_cli(*args)
        self.assertEqual(1, code)
        self.assertIn("invalid_license_url", stderr)

        args = list(self.valid_args(output))
        args[args.index("https://example.test/license")] = "https://example.test/license?token=secret"
        code, _, stderr = self.call_cli(*args)
        self.assertEqual(1, code)
        self.assertIn("invalid_license_url", stderr)
        self.assertFalse(output.exists())

    def test_cli_reports_existing_bundle_without_overwriting_it(self):
        output = self.root / "output"
        first_code, first_stdout, _ = self.call_cli(*self.valid_args(output))
        self.assertEqual(0, first_code)
        manifest = json.loads(first_stdout)
        path = output / "prepared" / manifest["source_bundle_sha256"] / "source-bundle.json"
        original = path.read_bytes()
        second_code, second_stdout, second_stderr = self.call_cli(*self.valid_args(output))
        self.assertEqual(1, second_code)
        self.assertEqual("", second_stdout)
        self.assertIn("already exists", second_stderr)
        self.assertEqual(original, path.read_bytes())


if __name__ == "__main__":
    unittest.main()
