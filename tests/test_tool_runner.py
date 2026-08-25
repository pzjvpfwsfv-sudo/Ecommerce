from pathlib import Path
import sys
import threading
import time
import unittest


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app.tool_runner import BoundedToolRunner, ToolRunnerUnavailableError


class BoundedToolRunnerTest(unittest.TestCase):
    def test_timed_out_operations_hold_capacity_until_they_finish(self):
        runner = BoundedToolRunner(max_workers=2)
        release = threading.Event()
        started = threading.Barrier(3)
        failures: list[BaseException] = []

        def blocked_operation() -> None:
            started.wait(timeout=1)
            release.wait(timeout=1)

        def time_out() -> None:
            try:
                runner.run(blocked_operation, timeout_seconds=0.01)
            except BaseException as error:
                failures.append(error)

        callers = [threading.Thread(target=time_out) for _ in range(2)]
        for caller in callers:
            caller.start()
        started.wait(timeout=1)
        for caller in callers:
            caller.join(timeout=1)

        worker_count = sum(thread.name.startswith("tool-worker") for thread in threading.enumerate())
        with self.assertRaisesRegex(ToolRunnerUnavailableError, "^tool runner unavailable$"):
            runner.run(lambda: "unexpected", timeout_seconds=0.01)
        self.assertEqual(worker_count, sum(thread.name.startswith("tool-worker") for thread in threading.enumerate()))
        self.assertEqual(2, len(failures))
        self.assertTrue(all(str(error) == "tool runner unavailable" for error in failures))

        release.set()
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            try:
                self.assertEqual("available", runner.run(lambda: "available", timeout_seconds=0.1))
                break
            except ToolRunnerUnavailableError:
                time.sleep(0.01)
        else:
            self.fail("runner capacity was not released after operations completed")
        runner.close()

    def test_close_rejects_new_operations_with_a_safe_error(self):
        runner = BoundedToolRunner(max_workers=1)
        runner.close()

        with self.assertRaisesRegex(ToolRunnerUnavailableError, "^tool runner unavailable$") as error:
            runner.run(lambda: (_ for _ in ()).throw(RuntimeError("password=secret")), timeout_seconds=1)

        self.assertIsNone(error.exception.__cause__)


if __name__ == "__main__":
    unittest.main()
