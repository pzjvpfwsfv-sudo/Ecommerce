from __future__ import annotations

import json


class KafkaEventProducer:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        try:
            from kafka import KafkaProducer
        except ImportError as exc:
            raise RuntimeError(
                "缺少 kafka-python 依赖，请先安装 generators/requirements.txt"
            ) from exc

        self.topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )

    def send_event(self, event: dict[str, str]) -> None:
        self._producer.send(self.topic, event)
        self._producer.flush()
