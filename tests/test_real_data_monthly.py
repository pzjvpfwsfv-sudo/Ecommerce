"""Transport fixtures only; these bytes are not business demonstration data."""

import gzip
from hashlib import sha256
import importlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


class Response(io.BytesIO):
    def __init__(self, payload, status=200, length=None):
        super().__init__(payload)
        self.status = status
        self.headers = {"Content-Length": str(len(payload) if length is None else length), "ETag": '"test-only"'}


class MonthlyDownloadTest(unittest.TestCase):
    def setUp(self):
        try:
            self.module = importlib.import_module("generators.real_data.monthly")
        except ModuleNotFoundError as exc:
            self.fail(f"monthly download functionality is missing: {exc}")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.payload = gzip.compress(b"fixture-only\n")

    def test_complete_transport_records_hash_but_not_gzip_validation(self):
        with patch.object(self.module, "urlopen", return_value=Response(self.payload)) as request:
            result = self.module.download_month("2019-Oct", self.root)
        output = self.root / "2019-Oct.csv.gz"
        self.assertEqual(self.payload, output.read_bytes())
        self.assertEqual(sha256(self.payload).hexdigest(), result["sha256"])
        self.assertEqual("full_file", result["scope"])
        self.assertTrue(result["full_source_downloaded"])
        self.assertFalse(result["gzip_integrity_verified"])
        self.assertEqual("2019-10-01T00:00:00+00:00", result["window_start"])
        self.assertEqual(result, json.loads(Path(str(output) + ".provenance.json").read_text(encoding="utf-8")))
        self.assertEqual("https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz", request.call_args.args[0].full_url)

    def test_partial_short_and_oversized_responses_never_publish(self):
        for response, limit in ((Response(self.payload, status=206), 1024), (Response(self.payload, length=len(self.payload) + 1), 1024), (Response(self.payload), 1)):
            with self.subTest(response=response):
                with patch.object(self.module, "urlopen", return_value=response):
                    with self.assertRaises(ValueError):
                        self.module.download_month("2019-Oct", self.root, max_bytes=limit)
                self.assertEqual([], list(self.root.iterdir()))

    def test_unknown_month_and_existing_file_prevent_network(self):
        with patch.object(self.module, "urlopen") as request:
            with self.assertRaises(ValueError):
                self.module.download_month("2099-Dec", self.root)
            output = self.root / "2019-Oct.csv.gz"
            output.write_bytes(b"keep")
            with self.assertRaises(FileExistsError):
                self.module.download_month("2019-Oct", self.root)
            request.assert_not_called()
            self.assertEqual(b"keep", output.read_bytes())

    def test_manifest_failure_removes_only_owned_output(self):
        with patch.object(self.module, "urlopen", return_value=Response(self.payload)), patch.object(self.module, "write_json", side_effect=OSError("test failure")):
            with self.assertRaises(OSError):
                self.module.download_month("2019-Oct", self.root)
        self.assertEqual([], list(self.root.iterdir()))


if __name__ == "__main__":
    unittest.main()
