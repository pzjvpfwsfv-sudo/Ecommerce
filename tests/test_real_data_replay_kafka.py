import importlib
import unittest
from unittest.mock import patch


class Future:
    def __init__(self, failure=None):
        self.failure = failure
        self.waited = False

    def get(self, timeout):
        assert 0 < timeout <= 30
        self.waited = True
        if self.failure:
            raise self.failure


class Producer:
    def __init__(self, partitions=None, failure=None):
        self.partitions = partitions
        self.future = Future(failure)
        self.sent = []
        self.closed = False

    def partitions_for(self, topic):
        return self.partitions

    def send(self, topic, **kwargs):
        self.sent.append((topic, kwargs))
        return self.future

    def close(self, timeout):
        assert 0 <= timeout <= 10
        self.closed = True


class ReplayKafkaTest(unittest.TestCase):
    def setUp(self):
        try:
            self.module = importlib.import_module("generators.real_data.replay_kafka")
        except ModuleNotFoundError as exc:
            self.fail(f"Kafka replay adapter is missing: {exc}")

    def test_acknowledgment_is_awaited_and_key_is_serialized(self):
        producer = Producer({0})
        with patch("kafka.KafkaProducer", return_value=producer) as factory:
            sink = self.module.ReplayKafkaSink("localhost:9092", "real_behavior_events_v1_test")
            sink.send({"event_id": "real_test-only", "event_time": "2019-10-01T00:00:00+00:00"}, "dataset:user")
            sink.close()
        self.assertTrue(producer.future.waited and producer.closed)
        self.assertEqual(b"dataset:user", producer.sent[0][1]["key"])
        self.assertEqual("2019-10-01T00:00:00+00:00", producer.sent[0][1]["value"]["event_time"])
        config = factory.call_args.kwargs
        self.assertEqual("all", config["acks"])
        self.assertEqual(1, config["max_in_flight_requests_per_connection"])
        self.assertFalse(config["allow_auto_create_topics"])
        self.assertTrue(0 < config["max_block_ms"] <= 15000)

    def test_missing_topic_is_not_automatically_created_and_client_is_closed(self):
        producer = Producer(None)
        with patch("kafka.KafkaProducer", return_value=producer):
            with self.assertRaisesRegex(ValueError, "exist"):
                self.module.ReplayKafkaSink("localhost:9092", "real_behavior_events_v1")
        self.assertTrue(producer.closed)
        self.assertEqual([], producer.sent)

    def test_timeout_is_a_failure_not_a_successful_enqueue(self):
        from kafka.errors import KafkaTimeoutError
        producer = Producer({0}, KafkaTimeoutError("test-only timeout"))
        with patch("kafka.KafkaProducer", return_value=producer):
            sink = self.module.ReplayKafkaSink("localhost:9092", "real_behavior_events_v1")
            with self.assertRaisesRegex(RuntimeError, "confirmed"):
                sink.send({}, "key")
            sink.close()

    def test_old_topic_is_rejected_without_network_calls(self):
        with patch("kafka.KafkaProducer", side_effect=AssertionError("must not connect")):
            with self.assertRaises(ValueError):
                self.module.ReplayKafkaSink("localhost:9092", "user_behavior_events")

    def test_metadata_timeout_is_reported_and_client_is_closed(self):
        from kafka.errors import KafkaTimeoutError
        producer = Producer()
        with patch("kafka.KafkaProducer", return_value=producer), patch.object(producer, "partitions_for", side_effect=KafkaTimeoutError("test-only")):
            with self.assertRaises(RuntimeError):
                self.module.ReplayKafkaSink("localhost:9092", "real_behavior_events_v1")
        self.assertTrue(producer.closed)


if __name__ == "__main__":
    unittest.main()
