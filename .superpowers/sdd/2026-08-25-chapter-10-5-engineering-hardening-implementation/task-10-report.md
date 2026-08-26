# Task 10 Report

## Status

Completed the two Task 9 carried load-bearing findings before implementing the controlled Chapter 10.5 migration and realtime reset tools. All destructive boundaries were verified with PowerShell mocks and static scans only. No real Docker volume removal, MinIO removal, Compose mutation, Flink submission, or catalog mutation was executed.

## Task 9 Carried Findings RED

- `test_resume_partial_control_flow_rejects_legacy_envelope_before_normalization` failed because `Invoke-CutoverResumePartialControlFlow` did not exist. The existing real `ResumePartial` prelude could read and normalize the legacy partial outside the canonical lock.
- `test_resume_partial_control_flow_fails_closed_on_canonical_lock_before_touching_legacy` failed for the same missing real control-flow boundary. This established that the previous tests exercised only production-submit stage helpers, not the actual ResumePartial orchestration.

## Task 9 Carried Findings GREEN

- The real `ResumePartial` branch now enters `Invoke-CutoverResumePartialControlFlow`. It acquires the canonical production-submit lock before reading, validating, migrating, or rewriting legacy production evidence.
- The legacy envelope requires exact case-sensitive schema and a fixed phase. Missing schema/phase, mixed-case phase, and arbitrary phase return only `Production submission recovery state is unsafe.`, leave the partial byte-for-byte unchanged, create no canonical state, and invoke no continuation.
- Exact legacy production evidence is removed under the canonical lock before the outer recovery prelude may call `Ensure-CutoverRecoveryState`; the outer prelude therefore cannot compete for the same production legacy `.next` replacement.
- Focused carried tests: 2 passed in 1.279 seconds.
- Final Task 9/Task 4 carried regression: `python -m unittest tests.test_chapter_10_5_bootstrap tests.test_chapter_9_phase_b_artifacts -v` passed 100 tests in 47.558 seconds.

## Task 10 RED

`python -m unittest tests.test_chapter_10_5_bootstrap tests.test_chapter_10_5_artifacts -v` ran 49 tests with 7 expected failures. All seven failures were caused by the missing `migrate_chapter_10_5.ps1` and `reset_chapter_10_5_realtime.ps1` production interfaces.

## Task 10 GREEN

- `reset_chapter_10_5_realtime.ps1` requires `-ConfirmReset` before any native call. It derives exactly five volume names from the current Compose project, validates both Compose project and logical-volume labels for all five before mutation, and deletes only the two fixed Chapter 9 Flink-state prefixes.
- `migrate_chapter_10_5.ps1` requires both `-TrafficPaused` and `-ConfirmRealtimeReset` before any call. Its order is fixed to lake evidence, controlled reset/catalog recovery, production Bootstrap with strict acceptance, and final lake evidence.
- Both reports use fixed paths, retain only parsed numeric lake evidence and the fixed Iceberg table snapshot ID, and pass through the existing recursive atomic redaction writer. Warehouse count/size and snapshot ID must remain unchanged.
- Any failure stops immediately, preserves the current state, emits only the fixed safe diagnostic command, and performs no automatic rollback or cleanup.
- Focused Task 10 safety tests: 7 passed.
- Brief target: `python -m unittest tests.test_chapter_10_5_bootstrap tests.test_chapter_10_5_artifacts -v` passed 49 tests in 24.364 seconds.
- Catalog and Chapter 4 related regression: 23 passed in 3.734 seconds.

## Final Verification

- `PYTHONPATH=services/api python -m unittest discover -s tests -p "test_*.py" -v`: 350 passed in 118.859 seconds.
- Windows PowerShell `5.1.22621.6133`: all three changed PowerShell scripts parsed; combined `-FunctionsOnly` smoke made zero native calls.
- Destructive lexical scan: zero broad destructive-command hits; exactly one controlled volume-remove site and one controlled MinIO-remove site.
- `git diff --check`: passed; Git emitted only line-ending normalization warnings.

## Safety Boundary

Tests mocked every Task 10 Docker, MinIO, Compose-mutation, catalog, and Bootstrap invocation. Existing artifact tests used only read-only Compose configuration rendering. No live destructive or service-mutating command was authorized or run.
