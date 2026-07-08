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

## Review Fix Follow-Up

### Script/Test Hardening

1. Updated `scripts/verify_chapter_6_trino_queries.ps1` to:
   - wrap `docker compose --env-file ... up -d trino` in an explicit exit-code check
   - fail with `Failed to start Trino with docker compose.` if startup returns non-zero
   - validate the SQL file contains exactly 2 non-empty statements before reading `$statements[0]` and `$statements[1]`
   - fail with `Expected exactly 2 non-empty SQL statements in <path> but found <count>.` when the SQL artifact shape is wrong
2. Updated `tests/test_chapter_6_trino_artifacts.py` to assert those guardrails are present in the verifier script.

### Verification After Review Fixes

Focused checks:

```powershell
python -m unittest tests.test_chapter_6_trino_artifacts.Chapter6TrinoArtifactsTest.test_query_sql_covers_count_and_group_by tests.test_chapter_6_trino_artifacts.Chapter6TrinoArtifactsTest.test_verification_script_checks_nonzero_rows -v
```

Result:
- `test_query_sql_covers_count_and_group_by` passed
- `test_verification_script_checks_nonzero_rows` passed

Broader artifact suite:

```powershell
python -m unittest tests.test_chapter_6_trino_artifacts -v
```

Result:
- All artifact checks except `test_docs_mention_chapter6_trino_validation` passed
- The remaining failure is still the out-of-scope README/docs assertion already noted above

### Real Runtime Attempt

I ran the Chapter 6 verifier for real:

```powershell
./scripts/verify_chapter_6_trino_queries.ps1
```

Observed output at the start of the real run:

```text
[chapter6-verify] preparing iceberg data through chapter 5 verification...
Image trinodb/trino:458 Pulling
```

To determine whether this was a silent hang or a real infrastructure blocker, I also ran:

```powershell
docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile lakehouse up -d trino
```

Observed compose/pull progress during that real environment check:

```text
27d8051d998e Already exists 0B
04bda59a189a Already exists 0B
b3fd6fcb9534 Download complete 0B
b3fd6fcb9534 Pull complete 0B
471e29635bbc Downloading 222.3MB
```

Actual outcome:
- The verifier did not reach Trino readiness or query execution during the observed run window.
- The concrete blocker was the first-time Docker pull for `trinodb/trino:458`, which remained in progress while the largest layer was still downloading/extracting.
- Because runtime proof of a successful Chapter 6 query execution was not obtained in this pass, this follow-up should be treated as `DONE_WITH_CONCERNS` rather than a clean done.
