import hashlib
import http.server
import json
from pathlib import Path
import shutil
import socketserver
import subprocess
import tempfile
import threading
import unittest


ROOT = Path(__file__).resolve().parent.parent
INSTALLER = ROOT / "scripts" / "install_runtime_dependencies.ps1"
MODULE = ROOT / "scripts" / "lib" / "Chapter105.Common.psm1"


class _Handler(http.server.BaseHTTPRequestHandler):
    payload = b"fixture dependency\n"
    requests = 0

    def do_GET(self):
        type(self).requests += 1
        self.send_response(200)
        self.send_header("Content-Length", str(len(self.payload)))
        self.end_headers()
        self.wfile.write(self.payload)

    def log_message(self, format, *args):
        pass


class Chapter105DependencyInstallerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = socketserver.TCPServer(("127.0.0.1", 0), _Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.repo = Path(self.tempdir.name)
        (self.repo / "scripts" / "lib").mkdir(parents=True)
        (self.repo / "infra" / "compose" / "flink" / "lib").mkdir(parents=True)
        shutil.copy2(INSTALLER, self.repo / "scripts" / INSTALLER.name)
        shutil.copy2(MODULE, self.repo / "scripts" / "lib" / MODULE.name)
        _Handler.requests = 0

    def tearDown(self):
        self.tempdir.cleanup()

    def _lock(self, destination, sha256=None):
        digest = sha256 or hashlib.sha256(_Handler.payload).hexdigest()
        lock = {
            "version": 1,
            "artifacts": [{
                "name": "fixture.jar",
                "url": f"http://127.0.0.1:{self.server.server_address[1]}/fixture.jar",
                "destination": destination,
                "sha256": digest,
            }],
        }
        path = self.repo / "lock.json"
        path.write_text(json.dumps(lock), encoding="utf-8")
        return path

    def _run(self, lock, *extra):
        return subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(self.repo / "scripts" / INSTALLER.name),
                "-LockFile",
                str(lock),
                "-RepositoryRoot",
                str(self.repo),
                "-AllowInsecureHttpForTest",
                *extra,
            ],
            capture_output=True,
            text=True,
            check=False,
        )

    def test_correct_file_is_cached_without_network_request(self):
        destination = "infra/compose/flink/lib/fixture.jar"
        target = self.repo / destination
        target.write_bytes(_Handler.payload)

        result = self._run(self._lock(destination))

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("fixture.jar cached", result.stdout.strip())
        self.assertEqual(0, _Handler.requests)

    def test_bad_download_does_not_replace_existing_file(self):
        destination = "infra/compose/flink/lib/fixture.jar"
        target = self.repo / destination
        old_contents = b"corrupt existing artifact"
        target.write_bytes(old_contents)

        result = self._run(self._lock(destination, "0" * 64))

        self.assertNotEqual(0, result.returncode)
        self.assertEqual(old_contents, target.read_bytes())
        self.assertEqual([], list(target.parent.glob("fixture.jar.partial.*")))

    def test_successful_download_installs_from_a_temporary_file(self):
        destination = "infra/compose/flink/lib/fixture.jar"
        target = self.repo / destination

        result = self._run(self._lock(destination))

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("fixture.jar installed", result.stdout.strip())
        self.assertEqual(_Handler.payload, target.read_bytes())
        self.assertEqual([], list(target.parent.glob("fixture.jar.partial.*")))

    def test_path_escape_is_rejected_before_downloading(self):
        result = self._run(self._lock("infra/compose/flink/lib/../escape.jar"))

        self.assertNotEqual(0, result.returncode)
        self.assertFalse((self.repo / "infra" / "compose" / "flink" / "escape.jar").exists())
        self.assertEqual(0, _Handler.requests)


if __name__ == "__main__":
    unittest.main()
