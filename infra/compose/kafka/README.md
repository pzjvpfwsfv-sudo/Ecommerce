# Kafka Compose Notes

This directory documents the Kafka Compose configuration and its evolution.

Current stage:

- Use a `KRaft controller + broker` topology for messaging.
- The controller is available only on the container network.
- The broker keeps both `localhost:9092` and `kafka:29092` entrypoints.

Migration goals:

- Remove ZooKeeper completely.
- Preserve the generator and Flink broker connection paths.
- Keep the project's `ZooKeeper -> KRaft` architecture evolution story clear.
