from pathlib import Path
import sys
import unittest
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app.readiness_service import ReadinessService  # noqa: E402


class ReadinessServiceTest(unittest.TestCase):
    def test_check_reports_ready_only_after_all_dependencies_succeed(self):
        doris = Mock()
        trino = Mock()
        flink = Mock()

        result = ReadinessService(doris, trino, flink).check()

        self.assertEqual(
            {"status": "ready", "dependencies": {"doris": "ready", "trino": "ready", "flink": "ready"}},
            result,
        )
        doris.fetch_all_metrics.assert_called_once_with()
        trino.fetch_summary.assert_called_once_with()
        flink.fetch_health.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
