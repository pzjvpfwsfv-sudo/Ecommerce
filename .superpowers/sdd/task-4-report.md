# Task 4 Report: Chapter 6 Docs and Storyline

## Scope

- Updated `README.md`
- Updated `jobs/README.md`
- Updated `tests/test_chapter_6_trino_artifacts.py`

## TDD Cycle

### Red

1. Updated the Chapter 6 docs test to use the real title `第 6 章：Trino + Iceberg 湖表查询` and to require the explicit verifier entrypoint `./scripts/verify_chapter_6_trino_queries.ps1`.
2. Ran:

```powershell
python -m unittest tests.test_chapter_6_trino_artifacts
```

3. Observed the expected failure because the docs still described the broader Chapter 6 roadmap instead of the stricter Trino + Iceberg storyline.

### Green

1. Updated `README.md` to add a dedicated `第 6 章：Trino + Iceberg 湖表查询` section with:
   - the chapter storyline
   - the explicit command entrypoint `./scripts/verify_chapter_6_trino_queries.ps1`
   - a short narrative tying Chapter 5 Iceberg output to Chapter 6 Trino readback
2. Updated `jobs/README.md` to add the same Chapter 6 title, storyline, command entrypoint, and the `11_trino_read_iceberg_user_behavior.sql` reference.
3. Updated `tests/test_chapter_6_trino_artifacts.py` so the docs contract now checks:
   - the exact Chapter 6 title
   - the Trino + Iceberg wording
   - the explicit verifier command path
   - the Chapter 6 SQL artifact name

## Verification

Focused Chapter 6 regression:

```powershell
python -m unittest tests.test_chapter_6_trino_artifacts
```

Result: `7` tests passed.

## Notes

- The requested brief path from the controller notes was not present in the workspace, so I used the controller guidance plus the existing Chapter 6 tests to drive the docs wording.
- I kept the work inside the allowed ownership boundary and did not touch scripts, SQL, or infra files.
