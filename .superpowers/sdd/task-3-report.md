Task 3 report

Summary:
- Updated Chapter 5 stale-container cleanup to reflect the KRaft topology by replacing `ecom-zookeeper` with `ecom-kafka-controller` and keeping `ecom-kafka` canonical.
- Left the Flink runner and validation scripts broker-focused, since they already target `ecom-kafka` and `kafka:29092` correctly.
- Did not change the Flink unit test because it already matches the broker-only contract.

Verification:
- `python -m unittest tests.test_flink_sql_job -v` passed.

Self-review:
- The change is intentionally minimal and limited to the owned KRaft compatibility surface.
- No ZooKeeper references remain in the owned scripts that are supposed to be broker-facing.

Concerns:
- None.
