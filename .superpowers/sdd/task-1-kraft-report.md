# Task 1 Report: Chapter 7 KRaft Artifact Tests

## What I changed
- Added `tests/test_chapter_7_kraft_artifacts.py` to lock the Chapter 7 target state around KRaft-only artifacts.
- Updated `tests/test_flink_sql_job.py` so the Flink SQL runner test explicitly rejects ZooKeeper assumptions.

## Verification
- Ran: `python -m unittest tests.test_chapter_7_kraft_artifacts tests.test_flink_sql_job -v`
- Result: expected red phase
- Failing checks:
  - `infra/docker-compose.yml` still uses `zookeeper` and a single Kafka service instead of controller/broker services
  - `infra/.env.example` still exposes ZooKeeper variables and lacks the KRaft env keys
  - `infra/compose/kafka/README.md` still describes the ZooKeeper + Kafka stage

## Self-review
- Scope stayed inside the two allowed test files plus this report.
- The tests are intentionally strict and fail against the current Chapter 7 state, which is the intended outcome for this task.
- No additional concerns.
