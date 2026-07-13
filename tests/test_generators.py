import unittest
from unittest.mock import Mock

from generators.run_generator import run_once
from generators.user_behavior_generator import UserBehaviorGenerator


class UserBehaviorGeneratorTest(unittest.TestCase):
    def test_generate_event_returns_required_schema(self):
        generator = UserBehaviorGenerator(seed=7)

        event = generator.generate_event()

        self.assertEqual(
            {
                "event_id",
                "user_id",
                "product_id",
                "event_type",
                "event_time",
                "channel",
                "device_type",
                "page_id",
            },
            set(event.keys()),
        )
        self.assertIn(event["event_type"], {"view", "click", "cart"})
        self.assertTrue(event["event_id"].startswith("evt_"))
        self.assertTrue(event["user_id"].startswith("u_"))
        self.assertTrue(event["product_id"].startswith("p_"))


class RunGeneratorTest(unittest.TestCase):
    def test_run_once_generates_and_sends_one_event(self):
        generator = Mock()
        producer = Mock()
        generator.generate_event.return_value = {
            "event_id": "evt_000001",
            "user_id": "u_1001",
            "product_id": "p_2001",
            "event_type": "click",
            "event_time": "2026-07-07T00:00:00+00:00",
            "channel": "app",
            "device_type": "android",
            "page_id": "product_detail",
        }

        sent_event = run_once(generator, producer)

        generator.generate_event.assert_called_once_with()
        producer.send_event.assert_called_once_with(generator.generate_event.return_value)
        self.assertEqual(generator.generate_event.return_value, sent_event)


if __name__ == "__main__":
    unittest.main()
