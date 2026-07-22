# Chapter 9 Phase B Final Fix Report

## Fixed

- Cutover partial state is schema-versioned and phase-based. Every shadow stop, job submission, and finalization writes an atomic intent before mutation and an atomic result after mutation. ResumePartial supports savepoint-only, production-only, Doris-only, Iceberg-only, failed REST/output windows, exact-name RUNNING adoption, and fail-closed ambiguity or terminal states.
- Cutover finalization remains atomic and resumable. A durable final manifest is recognized as completed, while an ordinary partial manifest is rejected unless ResumePartial is explicit. Resume does not require the shadow job to remain RUNNING and does not use allowNonRestoredState or broad cancellation.
- Rollback now has atomic tmp/chapter-9/rollback-progress.json state, explicit Resume reconciliation, intent/result records for stop/cancel/submit/finalization, exact manifest identity checks, safe production savepoint recovery including a unique-new-directory fallback, exact-name adoption, duplicate-submission prevention, and durable rollback IDs. DryRun creates no progress and performs no mutation.
- Verifier now durably records the original batch start, Doris timestamps, Trino baseline, and checkpoint baselines before sends. Stage evidence is persisted through output, groups, checkpoints, Doris, Trino, pre-API, API, and failure paths. Resume and read-only finalize use durable causal evidence only; direct Doris and Trino values are compared exactly with API evidence, including available latest event time.
- Resize verifies the exact TaskManager /workspace bind source again after recreation.
- Added recovery, crash-window, adoption ambiguity, ABORT control, final evidence round-trip, late API failure, and DryRun contract tests.

## Verification

- `python -m unittest tests.test_chapter_9_phase_b_artifacts -q`: passed, 45 tests.
- `python -m unittest discover -s tests -q`: passed.
- PowerShell Parser validation for all four scripts: passed.
- ASCII validation for all four scripts: passed.
- `git diff --check`: passed.

## Not Run

- Real resize, cutover, verifier, rollback, Docker compose operations, REST mutation, job/container changes, Kafka event sends, SQL submissions, and production validation were intentionally not run.

## Concerns

- Runtime REST/Docker behavior remains unexercised by design. The final merge review should still inspect live Flink response shapes and the target PowerShell version before any controlled runtime execution.
- Evidence paths are normalized to forward slashes in durable JSON so Windows PowerShell ConvertFrom-Json can round-trip them reliably; filesystem operations continue to use native local paths.
