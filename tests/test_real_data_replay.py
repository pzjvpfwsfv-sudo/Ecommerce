"""Artificial fixtures exercise replay failures; they are not business demo data."""

from datetime import UTC, datetime
from hashlib import sha256
import importlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from generators.real_data.download import source_config
from generators.real_data.normalization import normalize_event


class Clock:
    def __init__(self):
        self.value = 0.0

    def now(self):
        return self.value

    def sleep(self, delay):
        assert delay >= 0
        self.value += delay


class RecordingSink:
    def __init__(self, failure=None):
        self.messages = []
        self.keys = []
        self.failure = failure
        self.closed = False

    def send(self, message, key):
        if self.failure:
            self.failure(len(self.messages))
        self.messages.append(message)
        self.keys.append(key)

    def close(self):
        self.closed = True


class RealDataReplayTest(unittest.TestCase):
    def setUp(self):
        try:
            self.module = importlib.import_module("generators.real_data.replay")
        except ModuleNotFoundError as exc:
            self.fail(f"replay implementation is missing: {exc}")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.input = self.root / "events.jsonl"
        self.checkpoint = self.root / "checkpoint.json"
        self.config = source_config()
        self.events = [normalize_event({
            "event_time": f"2019-10-01 00:00:0{i} UTC", "event_type": "view",
            "product_id": "product-test-only", "category_id": "", "category_code": "",
            "brand": "", "price": "1.20", "user_id": "user-test-only", "user_session": "",
        }, dataset_id=self.config["dataset_id"], source_file="2019-Oct.csv.gz", row_number=i + 1) for i in range(5)]
        self.input.write_text("".join(json.dumps(event) + "\n" for event in self.events), encoding="utf-8")
        self.write_manifest()
        self.clock = Clock()

    def write_manifest(self):
        self.manifest = {
            "schema_version": 1, "scope": "user_sample", "dataset_id": self.config["dataset_id"],
            "sha256": sha256(self.input.read_bytes()).hexdigest(), "artifact_bytes": self.input.stat().st_size,
            "sample_rows": 5, "selected_paths_lossless": True, "selected_rejected_rows": 0,
            "source_shape_rejected_rows": 0, "out_of_order_sample_rows": 0,
            "window_start": "2019-10-01T00:00:00+00:00", "window_end_exclusive": "2019-12-01T00:00:00+00:00",
            "sources": [source | {"gzip_integrity_verified": True} for source in self.config["months"].values()],
        }
        Path(str(self.input) + ".provenance.json").write_text(json.dumps(self.manifest), encoding="utf-8")

    def run_replay(self, sink=None, **kwargs):
        sink = sink or RecordingSink()
        return self.module.replay_file(self.input, self.checkpoint, **({
            "mode": "dry-run", "bootstrap_servers": "localhost:9092", "topic": "real_behavior_events_v1",
            "rate": 10, "max_events": 1000, "checkpoint_every": 2,
            "sink_factory": lambda: sink, "clock": self.clock.now, "sleep": self.clock.sleep,
        } | kwargs))

    def state(self):
        return json.loads(self.checkpoint.read_text(encoding="utf-8"))

    def test_resume_preserves_business_fields_and_task_identity(self):
        first, second = RecordingSink(), RecordingSink()
        result1 = self.run_replay(first, max_events=2)
        result2 = self.run_replay(second)
        self.assertEqual((2, 3, 5), (result1["processed_this_run"], result2["processed_this_run"], result2["confirmed_records"]))
        self.assertEqual("complete", result2["status"])
        self.assertEqual(result1["replay_id"], result2["replay_id"])
        for expected, actual in zip(self.events, first.messages + second.messages):
            self.assertEqual(expected, {key: actual[key] for key in expected})
            self.assertEqual(set(expected) | {"replay_id", "replayed_at", "replay_schema_version"}, set(actual))
            self.assertEqual(result1["replay_id"], actual["replay_id"])
            self.assertEqual(UTC, datetime.fromisoformat(actual["replayed_at"]).tzinfo)
        self.assertEqual([self.config["dataset_id"] + ":user-test-only"] * 3, second.keys)
        self.assertTrue(first.closed and second.closed)
        self.assertEqual(0, self.run_replay()["processed_this_run"])

    def test_failed_unconfirmed_send_never_skips_a_record_on_resume(self):
        def fail(index):
            if index == 3:
                raise TimeoutError("test-only unknown delivery")
        sink = RecordingSink(fail)
        with self.assertRaises(TimeoutError):
            self.run_replay(sink)
        self.assertEqual(2, self.state()["confirmed_records"])
        recovered = RecordingSink()
        self.run_replay(recovered)
        self.assertEqual([event["event_id"] for event in self.events[2:]], [message["event_id"] for message in recovered.messages])
        self.assertTrue(sink.closed)

    def test_keyboard_interrupt_saves_only_confirmed_prefix(self):
        def pause(index):
            if index == 3:
                raise KeyboardInterrupt
        result = self.run_replay(RecordingSink(pause))
        self.assertEqual("paused", result["status"])
        self.assertEqual(3, self.state()["confirmed_records"])
        self.assertEqual(2, self.run_replay()["processed_this_run"])

    def test_interrupt_between_progress_fields_cannot_corrupt_checkpoint(self):
        def interrupt_after_confirmation(frame, event, arg):
            state = frame.f_locals.get("state", {})
            if frame.f_code.co_name == "replay_file" and event == "line" and state.get("confirmed_records") == 1:
                raise KeyboardInterrupt
            return interrupt_after_confirmation
        previous = sys.gettrace()
        try:
            sys.settrace(interrupt_after_confirmation)
            result = self.run_replay()
        finally:
            sys.settrace(previous)
        self.assertEqual("paused", result["status"])
        self.assertEqual(self.events[0]["event_id"], self.state()["last_event_id"])
        self.assertEqual(4, self.run_replay()["processed_this_run"])

    def test_invalid_manifest_source_type_fails_cleanly_before_connecting(self):
        self.manifest["sources"][0]["source_file"] = []
        Path(str(self.input) + ".provenance.json").write_text(json.dumps(self.manifest), encoding="utf-8")
        with self.assertRaises(ValueError):
            self.run_replay(sink_factory=lambda: self.fail("must not connect"))

    def test_invalid_event_source_type_is_a_validation_error(self):
        self.input.write_text(json.dumps(self.events[0] | {"source_file": []}) + "\n", encoding="utf-8")
        self.write_manifest()
        with self.assertRaises(ValueError):
            self.run_replay()

    def test_deep_json_is_rejected_as_invalid_input(self):
        self.input.write_text("[" * 2000 + "0" + "]" * 2000 + "\n", encoding="utf-8")
        self.write_manifest()
        with self.assertRaises(ValueError):
            self.run_replay()

    def test_file_mutation_during_replay_prevents_further_sends(self):
        def mutate(index):
            if index == 0:
                with self.input.open("ab") as stream:
                    stream.write(b"\n")
        sink = RecordingSink(mutate)
        with self.assertRaisesRegex(ValueError, "changed"):
            self.run_replay(sink)
        self.assertEqual(1, len(sink.messages))
        self.assertEqual(0, self.state()["confirmed_records"])

    def test_mode_or_destination_changes_fail_before_sink_creation(self):
        self.run_replay(max_events=1)
        for change in ({"mode": "kafka"}, {"bootstrap_servers": "localhost:19092"}, {"topic": "real_behavior_events_v1_other"}):
            with self.subTest(change=change):
                with self.assertRaisesRegex(ValueError, "identity"):
                    self.run_replay(sink_factory=lambda: self.fail("must not connect"), **change)

    def test_modified_file_or_manifest_rejects_before_connecting(self):
        self.input.write_bytes(self.input.read_bytes() + b"\n")
        with self.assertRaisesRegex(ValueError, "source"):
            self.run_replay(sink_factory=lambda: self.fail("must not connect"))
        self.assertFalse(self.checkpoint.exists())

    def test_corrupt_checkpoint_or_bad_prefix_is_not_reset_silently(self):
        self.run_replay(max_events=2)
        original = self.state()
        for change in ({"confirmed_records": -1}, {"confirmed_records": True}, {"confirmed_records": 9}, {"last_event_id": "bad"}):
            with self.subTest(change=change):
                self.checkpoint.write_text(json.dumps(original | change), encoding="utf-8")
                with self.assertRaises(ValueError):
                    self.run_replay(sink_factory=lambda: self.fail("must not connect"))

    def test_invalid_event_identity_and_duplicate_json_keys_are_not_sent(self):
        for corrupt in (json.dumps(self.events[0] | {"price": "9.99"}) + "\n", '{"event_id":"a","event_id":"b"}\n'):
            with self.subTest(corrupt=corrupt[:40]):
                self.input.write_text(corrupt, encoding="utf-8")
                self.write_manifest()
                sink = RecordingSink()
                with self.assertRaises(ValueError):
                    self.run_replay(sink)
                self.assertEqual([], sink.messages)
                self.checkpoint.unlink(missing_ok=True)

    def test_oversized_json_line_is_rejected_without_delivery(self):
        self.input.write_text("x" * 70000 + "\n", encoding="utf-8")
        self.write_manifest()
        sink = RecordingSink()
        with self.assertRaisesRegex(ValueError, "limit"):
            self.run_replay(sink)
        self.assertEqual([], sink.messages)

    def test_rate_is_a_ceiling_without_catchup_bursts(self):
        times = []
        def record(index):
            times.append(self.clock.now())
            if index == 1:
                self.clock.sleep(1)
        self.run_replay(RecordingSink(record), rate=10)
        self.assertEqual(5, len(times))
        self.assertTrue(all(b - a >= 0.099999 for a, b in zip(times, times[1:])))
        self.assertGreaterEqual(times[2] - times[1], 1)

    def test_invalid_options_old_topic_and_path_collisions_are_rejected(self):
        for option in ({"rate": 0}, {"rate": float("nan")}, {"max_events": 0}, {"checkpoint_every": 0}, {"topic": "user_behavior_events"}, {"mode": "wrong"}):
            with self.subTest(option=option), self.assertRaises(ValueError):
                self.run_replay(**option)
        self.checkpoint = self.input
        with self.assertRaises(ValueError):
            self.run_replay()

    def test_file_lock_rejects_concurrent_replay_and_releases_after_exit(self):
        from generators.real_data.replay_state import checkpoint_lock
        with checkpoint_lock(self.checkpoint):
            with self.assertRaisesRegex(ValueError, "running"):
                self.run_replay()
        self.assertEqual(5, self.run_replay()["confirmed_records"])

    def test_atomic_checkpoint_failure_preserves_previous_progress(self):
        self.run_replay(max_events=2)
        before = self.checkpoint.read_bytes()
        with patch("generators.real_data.replay_state.os.replace", side_effect=OSError("test-only disk failure")):
            with self.assertRaises(OSError):
                self.run_replay()
        self.assertEqual(before, self.checkpoint.read_bytes())
        self.assertEqual([], list(self.root.glob("*.part")))

    def test_cli_defaults_to_dry_run_and_returns_nonzero_for_errors(self):
        with patch("generators.real_data.replay_kafka.ReplayKafkaSink", side_effect=AssertionError("must stay offline")):
            self.assertEqual(0, self.module.main(["--input", str(self.input), "--checkpoint", str(self.checkpoint), "--rate", "10000", "--max-events", "2"]))
        self.assertEqual("dry-run", self.state()["identity"]["mode"])
        self.assertEqual(1, self.module.main(["--input", str(self.input), "--checkpoint", str(self.checkpoint), "--topic", "user_behavior_events"]))


if __name__ == "__main__":
    unittest.main()
