"""All in-memory CSV records below are test fixtures, not real business data."""

from contextlib import redirect_stderr, redirect_stdout
import gzip
from hashlib import sha256
import importlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


CSV = "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session\n"
CSV += "2019-10-01 00:00:00 UTC,view,1,2,,,0.10,3,test-only\n"
CSV += "2019-10-01 00:00:01 UTC,purchase,1,2,,,0.20,3,test-only\n"


class Response(io.BytesIO):
    def __init__(self, payload, status=206, content_range=None):
        super().__init__(payload)
        self.status = status
        self.headers = {"Content-Range": content_range or f"bytes 0-{len(payload) - 1}/{len(payload)}"}


class RealDataCliTest(unittest.TestCase):
    def setUp(self):
        try:
            self.download = importlib.import_module("generators.real_data.download")
            self.cli = importlib.import_module("generators.real_data.__main__")
        except ModuleNotFoundError as exc:
            self.fail(f"real data download/CLI functionality is missing: {exc}")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def call_cli(self, *args):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            result = self.cli.main(list(args))
        return result, stdout.getvalue(), stderr.getvalue()

    def test_download_prefix_has_source_hash_and_honest_scope(self):
        output = self.root / "prefix.csv"
        with patch.object(self.download, "urlopen", return_value=Response(gzip.compress(CSV.encode()))) as request:
            result = self.download.download_prefix(output, rows=1, max_compressed_bytes=1024)
        self.assertEqual(2, len(output.read_text().splitlines()))
        self.assertEqual("prefix", result["scope"])
        self.assertEqual(1, result["rows_written"])
        self.assertFalse(result["full_source_downloaded"])
        self.assertEqual(sha256(output.read_bytes()).hexdigest(), result["sha256"])
        self.assertEqual(result, json.loads(Path(str(output) + ".provenance.json").read_text(encoding="utf-8")))
        self.assertEqual("bytes=0-1023", request.call_args.args[0].get_header("Range"))
        self.assertTrue(request.call_args.args[0].full_url.startswith("https://data.rees46.com/datasets/marketplace/"))

    def test_ignored_range_damaged_stream_and_wrong_range_fail_cleanly(self):
        for response in (Response(gzip.compress(CSV.encode()), status=200), Response(b"not gzip"), Response(gzip.compress(CSV.encode()), content_range="bytes 1-99/999")):
            with self.subTest(response=response):
                output = self.root / "prefix.csv"
                with patch.object(self.download, "urlopen", return_value=response):
                    with self.assertRaises((ValueError, OSError, EOFError)):
                        self.download.download_prefix(output, rows=1)
                self.assertFalse(output.exists())
                self.assertFalse(Path(str(output) + ".provenance.json").exists())
                self.assertEqual([], list(self.root.glob("*.part")))

    def test_byte_budget_and_insufficient_rows_fail_without_partial_output(self):
        for limit, rows in ((20, 1), (1024, 3)):
            with self.subTest(limit=limit, rows=rows):
                output = self.root / "prefix.csv"
                with patch.object(self.download, "urlopen", return_value=Response(gzip.compress(CSV.encode()))):
                    with self.assertRaises((ValueError, OSError, EOFError)):
                        self.download.download_prefix(output, rows=rows, max_compressed_bytes=limit)
                self.assertFalse(output.exists())

    def test_existing_artifact_or_manifest_prevents_network(self):
        output = self.root / "prefix.csv"
        manifest = Path(str(output) + ".provenance.json")
        for existing in (output, manifest):
            existing.write_text("keep", encoding="utf-8")
            with patch.object(self.download, "urlopen") as request:
                with self.assertRaises(FileExistsError):
                    self.download.download_prefix(output, rows=1)
                request.assert_not_called()
            self.assertEqual("keep", existing.read_text())
            existing.unlink()

    def test_profile_cli_writes_report_and_rejects_conflicting_paths(self):
        source, report = self.root / "input.csv", self.root / "report.json"
        source.write_text(CSV, encoding="utf-8")
        args = ("profile", "--input", str(source), "--source-file", "2019-Oct.csv.gz", "--scope", "prefix")
        code, _, error = self.call_cli(*args, "--report", str(report))
        self.assertEqual(0, code, error)
        self.assertEqual(2, json.loads(report.read_text(encoding="utf-8"))["valid_rows"])
        code, _, _ = self.call_cli(*args, "--report", str(source))
        self.assertNotEqual(0, code)
        self.assertEqual(CSV, source.read_text())
        target = self.root / "same.json"
        code, _, _ = self.call_cli(*args, "--report", str(target), "--normalized", str(target))
        self.assertNotEqual(0, code)
        self.assertFalse(target.exists())

    def test_provenance_prevents_relabeling_prefix_as_complete(self):
        output = self.root / "prefix.csv"
        with patch.object(self.download, "urlopen", return_value=Response(gzip.compress(CSV.encode()))):
            self.download.download_prefix(output, rows=1)
        report = self.root / "report.json"
        code, _, _ = self.call_cli("profile", "--input", str(output), "--source-file", "2019-Oct.csv.gz", "--scope", "full_file", "--report", str(report))
        self.assertNotEqual(0, code)
        self.assertFalse(report.exists())

    def test_manifest_hash_mismatch_fails_before_normalized_publication(self):
        output = self.root / "prefix.csv"
        with patch.object(self.download, "urlopen", return_value=Response(gzip.compress(CSV.encode()))):
            self.download.download_prefix(output, rows=1)
        output.write_text(CSV, encoding="utf-8")
        normalized = self.root / "events.jsonl"
        code, _, _ = self.call_cli("profile", "--input", str(output), "--source-file", "2019-Oct.csv.gz", "--scope", "prefix", "--report", str(self.root / "report.json"), "--normalized", str(normalized))
        self.assertNotEqual(0, code)
        self.assertFalse(normalized.exists())

    def test_malformed_manifest_returns_safe_cli_error(self):
        source = self.root / "input.csv"
        source.write_text(CSV, encoding="utf-8")
        Path(str(source) + ".provenance.json").write_text("[]", encoding="utf-8")
        code, _, error = self.call_cli("profile", "--input", str(source), "--source-file", "2019-Oct.csv.gz", "--report", str(self.root / "report.json"))
        self.assertNotEqual(0, code)
        self.assertIn("provenance manifest", error)

    def test_nonpositive_cli_limits_are_rejected_before_download(self):
        with patch.object(self.download, "urlopen") as request:
            with self.assertRaises(SystemExit) as result:
                self.call_cli("fetch-prefix", "--output", str(self.root / "prefix.csv"), "--rows", "0")
            self.assertEqual(2, result.exception.code)
            request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
