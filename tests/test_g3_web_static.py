import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "api"))

from app.config import ApiSettings, load_settings  # noqa: E402
from app.main import create_app  # noqa: E402


class G3WebStaticTest(unittest.TestCase):
    def make_app(self, settings):
        return create_app(
            settings=settings,
            repository=Mock(), analysis_service=Mock(), tool_analysis_service=Mock(),
            readiness_service=Mock(), behavior_service=Mock(), order_service=Mock(),
            auth_service=Mock(),
        )

    def test_configured_missing_directory_fails_instead_of_starting_empty_ui(self):
        with tempfile.TemporaryDirectory() as root:
            missing = Path(root) / "missing"
            with self.assertRaisesRegex(RuntimeError, "WEB_DIST_DIR.*index.html"):
                self.make_app(ApiSettings(web_dist_dir=missing))

    def test_built_directory_serves_app_without_shadowing_existing_api(self):
        with tempfile.TemporaryDirectory() as root:
            dist = Path(root)
            (dist / "index.html").write_text("<h1>G3 workbench</h1>", encoding="utf-8")
            assets = dist / "assets"
            assets.mkdir()
            (assets / "app.js").write_text("window.g3=true", encoding="utf-8")
            client = TestClient(self.make_app(ApiSettings(web_dist_dir=dist)))
            self.assertIn("G3 workbench", client.get("/app/").text)
            self.assertIn("window.g3=true", client.get("/app/assets/app.js").text)
            self.assertEqual(client.get("/health").status_code, 200)
            self.assertEqual(client.get("/api/v1/orders/overview").status_code, 401)

    def test_empty_web_dist_environment_disables_static_mount(self):
        settings = load_settings({"WEB_DIST_DIR": ""})
        self.assertIsNone(settings.web_dist_dir)
        client = TestClient(self.make_app(settings))
        self.assertEqual(client.get("/app/").status_code, 404)
