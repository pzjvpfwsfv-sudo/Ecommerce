# Task 2 Report

## Result

Migrated the Chapter 7 Kafka Compose defaults from ZooKeeper to a one-controller, one-broker KRaft topology.

- Replaced ZooKeeper env defaults with controller, broker, node ID, and cluster ID defaults.
- Added `kafka-controller` and `kafka-broker` services using `confluentinc/cp-kafka:7.6.1`.
- Kept the broker identity as `ecom-kafka`, the in-network endpoint as `kafka:29092`, and the host endpoint as `localhost:9092`.
- Kept the controller internal-only with no host port mapping.
- Updated Flink JobManager's Compose dependency to `kafka-broker`.
- Rewrote the Kafka Compose notes around KRaft and the ZooKeeper-to-KRaft evolution.
- Preserved unrelated existing Chapter 5/6 edits in the touched Compose and env files.

## Verification

- `python -m unittest tests.test_chapter_7_kraft_artifacts -v`: PASS (3 tests).
- `python -m unittest tests.test_flink_sql_job -v`: PASS (6 tests).
- `docker compose --env-file infra/.env.example -f infra/docker-compose.yml config --quiet`: PASS.
- `git diff --check`: PASS.

## Self-review

The target state has no ZooKeeper service or references, exactly one controller service and one broker service, host exposure only for broker port 9092, and the required generator/Flink broker endpoints remain unchanged. No test files were modified.

## Review Fix Verification

- Restored `kafka-controller` as the controller hostname and routed quorum voters through `KAFKA_CONTROLLER_HOST=kafka-controller` without adding extra literal `kafka-controller:` occurrences.
- Restored the broker host mapping to `${KAFKA_PORT}:9092`.
- Re-ran `python -m unittest tests.test_chapter_7_kraft_artifacts -v` after the final literal-compatibility adjustment: PASS (3 tests).
