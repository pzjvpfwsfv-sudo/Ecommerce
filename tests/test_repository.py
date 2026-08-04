import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app.config import ApiSettings
from app.repository import RealtimeMetricsRepository
from app.tool_deadline import use_tool_deadline


class RealtimeMetricsRepositoryTest(unittest.TestCase):
    def test_settings_connection_applies_finite_connect_read_and_write_timeouts(self):
        settings = ApiSettings()
        repository = RealtimeMetricsRepository.from_settings(settings)

        with patch("pymysql.connect") as connect:
            connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchall.return_value = []
            with use_tool_deadline(0.25):
                repository.fetch_all_metrics()

        kwargs = connect.call_args.kwargs
        self.assertGreater(kwargs["connect_timeout"], 0)
        self.assertGreater(kwargs["read_timeout"], 0)
        self.assertGreater(kwargs["write_timeout"], 0)
        self.assertTrue(all(value <= 1 for value in (
            kwargs["connect_timeout"], kwargs["read_timeout"], kwargs["write_timeout"]
        )))


if __name__ == "__main__":
    unittest.main()
