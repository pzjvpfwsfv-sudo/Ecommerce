import hashlib
import http.server
import json
import os
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
        if self.path == "/redirect.jar":
            self.send_response(302)
            self.send_header("Location", "/fixture.jar")
            self.end_headers()
            self.wfile.write(b"redirect response body must stay private")
            return
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

    def _lock(self, destination, sha256=None, url=None):
        digest = sha256 or hashlib.sha256(_Handler.payload).hexdigest()
        lock = {
            "version": 1,
            "artifacts": [{
                "name": "fixture.jar",
                "url": url or f"http://127.0.0.1:{self.server.server_address[1]}/fixture.jar",
                "destination": destination,
                "sha256": digest,
            }],
        }
        path = self.repo / "lock.json"
        path.write_text(json.dumps(lock), encoding="utf-8")
        return path

    def _run(self, lock, *extra, allow_insecure=True):
        command = [
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
        ]
        if allow_insecure:
            command.append("-AllowInsecureHttpForTest")
        command.extend(extra)
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_production_invocation_rejects_loopback_http_before_download(self):
        destination = "infra/compose/flink/lib/fixture.jar"

        result = self._run(self._lock(destination), allow_insecure=False)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("Runtime dependency URL must use HTTPS.", result.stderr)
        self.assertEqual(0, _Handler.requests)
        self.assertFalse((self.repo / destination).exists())

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

    def test_redirect_is_rejected_without_replacing_destination_or_leaking_response(self):
        destination = "infra/compose/flink/lib/fixture.jar"
        target = self.repo / destination
        old_contents = b"existing artifact stays intact"
        target.write_bytes(old_contents)
        url = f"http://127.0.0.1:{self.server.server_address[1]}/redirect.jar"

        result = self._run(self._lock(destination, url=url))

        self.assertNotEqual(0, result.returncode)
        self.assertIn("Runtime dependency download failed for fixture.jar.", result.stderr)
        self.assertNotIn("redirect response body", result.stderr + result.stdout)
        self.assertEqual(old_contents, target.read_bytes())
        self.assertEqual([], list(target.parent.glob("fixture.jar.partial.*")))
        self.assertEqual(1, _Handler.requests)

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

    def test_reparse_point_at_allowed_lib_is_rejected_before_outside_write(self):
        destination = "infra/compose/flink/lib/fixture.jar"
        lib = self.repo / "infra" / "compose" / "flink" / "lib"
        outside_tempdir = tempfile.TemporaryDirectory()
        outside = Path(outside_tempdir.name)
        lib.rmdir()
        creation_errors = []
        try:
            try:
                os.symlink(outside, lib, target_is_directory=True)
            except OSError as error:
                creation_errors.append(f"symlink: {error}")
                if os.name == "nt":
                    junction = subprocess.run(
                        ["cmd", "/c", "mklink", "/J", str(lib), str(outside)],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    if junction.returncode != 0:
                        creation_errors.append(f"junction: {junction.stderr or junction.stdout}")
                if not lib.exists():
                    lib.mkdir()
                    self.skipTest("cannot create symlink or junction: " + "; ".join(creation_errors))

            result = self._run(self._lock(destination))

            self.assertNotEqual(0, result.returncode)
            self.assertIn("Runtime dependency path contains a reparse point.", result.stderr)
            self.assertEqual(0, _Handler.requests)
            self.assertFalse((outside / "fixture.jar").exists())
            self.assertEqual([], list(outside.glob("fixture.jar.partial.*")))
        finally:
            if lib.is_symlink() or (os.name == "nt" and os.path.isjunction(lib)):
                lib.rmdir()
                lib.mkdir()
            outside_tempdir.cleanup()


if __name__ == "__main__":
    unittest.main()
