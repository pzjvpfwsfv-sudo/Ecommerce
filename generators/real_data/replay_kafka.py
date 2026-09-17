import json

from .replay import validate_topic


class ReplayKafkaSink:
    def __init__(self, bootstrap_servers, topic):
        validate_topic(topic)
        from kafka import KafkaProducer
        from kafka.errors import KafkaError
        self.topic = topic
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers, acks="all", retries=3,
                max_in_flight_requests_per_connection=1, allow_auto_create_topics=False,
                buffer_memory=4194304, max_block_ms=5000, request_timeout_ms=10000,
                api_version_auto_timeout_ms=3000,
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8"),
            )
        except KafkaError:
            raise RuntimeError("Kafka connection failed") from None
        try:
            if not self.producer.partitions_for(topic):
                raise ValueError("Kafka replay topic must already exist")
        except BaseException:
            self.close()
            raise

    def send(self, message, key):
        from kafka.errors import KafkaError
        try:
            self.producer.send(self.topic, key=key.encode("utf-8"), value=message).get(timeout=15)
        except KafkaError:
            raise RuntimeError("Kafka delivery was not confirmed; resume may resend this record") from None

    def close(self):
        self.producer.close(timeout=5)
