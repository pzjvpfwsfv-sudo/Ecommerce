# Task 3 Report: Chapter 6 Trino Query SQL and Verification Script

## Scope

- Updated `tests/test_chapter_6_trino_artifacts.py`
- Updated `jobs/sql/11_trino_read_iceberg_user_behavior.sql`
- Updated `scripts/verify_chapter_6_trino_queries.ps1`

## TDD Cycle

### Red

1. Added `test_verification_script_checks_nonzero_rows` to assert the Chapter 6 verification script contains:
   - `event_count`
   - `Invoke-RestMethod`
   - `nextUri`
   - `throw`
2. Ran:

```powershell
python -m unittest tests.test_chapter_6_trino_artifacts.Chapter6TrinoArtifactsTest.test_query_sql_covers_count_and_group_by tests.test_chapter_6_trino_artifacts.Chapter6TrinoArtifactsTest.test_verification_script_checks_nonzero_rows -v
```

3. Observed expected failures because both Chapter 6 artifact files were empty:
   - SQL assertion failed because `SELECT COUNT(*) AS event_count` was missing
   - Script assertion failed because `event_count` was missing

### Green

1. Implemented `jobs/sql/11_trino_read_iceberg_user_behavior.sql` with:
   - a total event count query
   - an `event_type` group-by count query ordered by count desc and event type asc
2. Implemented `scripts/verify_chapter_6_trino_queries.ps1` to:
   - reuse `verify_chapter_5_end_to_end.ps1` for Iceberg data preparation
   - start Trino through `docker compose`
   - wait for `/v1/info` readiness
   - submit SQL through `/v1/statement`
   - follow `nextUri` until all rows are collected
   - throw if the count query returns zero rows
   - throw if the grouped query returns no `event_type` rows
   - print `event_count` plus the top grouped result

### Verification

Focused regression checks:

```powershell
python -m unittest tests.test_chapter_6_trino_artifacts.Chapter6TrinoArtifactsTest.test_query_sql_covers_count_and_group_by tests.test_chapter_6_trino_artifacts.Chapter6TrinoArtifactsTest.test_verification_script_checks_nonzero_rows -v
```

Result: both tests passed.

Broader artifact suite:

```powershell
python -m unittest tests.test_chapter_6_trino_artifacts -v
```

Result:
- Chapter 6 SQL/script artifact tests passed
- `test_docs_mention_chapter6_trino_validation` still failed because README/docs updates are intentionally out of scope for Task 3 and belong to Task 4

## Notes

- I did not modify README or `jobs/README.md` per controller instructions.
- I did not change any infra files; the script reuses the existing Trino and Chapter 5 scaffolding already present in the repository.
