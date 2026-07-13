from __future__ import annotations

from os import getenv
from time import sleep

from generators.kafka_producer import KafkaEventProducer
from generators.user_behavior_generator import UserBehaviorGenerator


def run_once(generator: UserBehaviorGenerator, producer: KafkaEventProducer) -> dict[str, str]:
    event = generator.generate_event()
    producer.send_event(event)
    return event


def main() -> None:
    bootstrap_servers = getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = getenv("KAFKA_TOPIC_USER_BEHAVIOR", "user_behavior_events")
    interval_seconds = float(getenv("GENERATOR_INTERVAL_SECONDS", "1"))

    generator = UserBehaviorGenerator()
    producer = KafkaEventProducer(bootstrap_servers=bootstrap_servers, topic=topic)

    sent_count = 0
    while True:
        event = run_once(generator, producer)
        sent_count += 1
        print(
            f"[generator] sent #{sent_count} "
            f"event_id={event['event_id']} type={event['event_type']} topic={topic}"
        )
        sleep(interval_seconds)


if __name__ == "__main__":
    main()
