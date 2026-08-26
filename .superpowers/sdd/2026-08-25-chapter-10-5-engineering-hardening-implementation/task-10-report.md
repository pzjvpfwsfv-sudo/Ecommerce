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

## Fix Round 1

### Carried Finding RED/GREEN

- RED: `test_resume_partial_outer_flow_normalizes_legacy_evidence_only_while_lock_is_held` proved that the real `Invoke-CutoverResumePartialControlFlow` released the canonical production-submit lock before calling `Ensure-CutoverRecoveryState` and could therefore perform a stale legacy write.
- GREEN: the lock-held state boundary now validates the exact legacy envelope, reconciles production evidence, calls `Ensure-CutoverRecoveryState`, rejects any remaining `production_submit`, and returns the validated recovery state before releasing the canonical lock. The outer flow has no legacy `Ensure` or write site.
- The earlier report statement that the outer prelude could safely call `Ensure-CutoverRecoveryState` after reconciliation is superseded: all production legacy reconciliation and normalization now completes under the shared canonical lock.

### Review Findings 1-7 RED/GREEN

- RED 1: real denied and confirmed migrate public-entry calls both returned without work because reset's dot-sourced `FunctionsOnly` value replaced migrate's entry value. GREEN: migrate preserves its own entry switch before import; a denied real entry makes zero calls, while the doubly confirmed real entry completes through mocked evidence, reset, catalog, Bootstrap, and report writing.
- RED 2: post-inspection Compose argv omitted a fixed project and could reread a rewritten EnvFile. GREEN: project inspection occurs once, then every reset/migration Compose, MinIO, and evidence argv uses separate `--project-name`, `<frozen-project>` entries. Restore and Bootstrap receive the same validated project explicitly.
- RED 3: the real ResumePartial outer flow called `Ensure-CutoverRecoveryState` after lock release. GREEN: the new real outer-flow lock probe observes `Ensure` only while the canonical lock is held.
- RED 4: reset stopped containers, removed MinIO prefixes, and then attempted volume removal. GREEN: it removes only the fixed five volume-owner services through project-bound Compose, validates all five volumes, then revalidates/removes volumes before deleting either fixed Flink-state prefix.
- RED 5: unconfirmed reset resolved Git state and wrote a failure report. GREEN: the confirmation branch is the first public-entry guard; the real unconfirmed entry makes zero native calls and creates no report or other persistent file.
- RED 6: PowerShell's case-insensitive JSON projection accepted unsafe label spellings and duplicate keys. GREEN: raw label JSON requires unique exact-lowercase keys and exact string values; uppercase, duplicate, non-string, foreign-project, and wrong-logical-volume labels fail closed.
- RED 7: volumes were inspected only once. GREEN: each exact volume is fingerprinted during the complete preflight, then inspected again immediately before its sole `volume rm`; changed identity or labels stop the flow before that removal.

### Mutation And Failure Semantics

The only success-path mutation order is: fixed-project Compose remove of `kafka-controller`, `kafka-broker`, `doris-fe`, `doris-be`, and `metastore-postgres`; immediate inspect/remove for each of the five allowlisted volumes; deletion of the two fixed Chapter 9 Flink-state prefixes; fixed-project service rebuild; fixed-project catalog restore; final lake evidence. There is no broad `down -v`, arbitrary `docker rm`, arbitrary volume/prefix parameter, warehouse deletion, or MinIO host-path deletion.

Any failure stops at the failing operation and retains the resulting on-disk state for diagnosis. No rollback, replacement deletion, prefix cleanup, or automatic compensating mutation runs after failure; output remains the fixed redacted diagnostic guidance.

### Fix Round 1 Verification

- Seven focused round1 tests passed in 3.131 seconds after each corresponding RED was captured.
- Final Task 10 target: 56 passed in 26.292 seconds.
- Task 9/Task 4 carried regression: 107 passed in 48.505 seconds.
- Catalog, dependency, and Chapter 4 related regression: 30 passed in 6.804 seconds.
- Final full Python discovery with `PYTHONPATH=services/api`: 357 passed in 119.152 seconds.
- Windows PowerShell `5.1.22621.6133`: five changed scripts parsed with zero errors; combined `FunctionsOnly` smoke made zero native calls.
- Destructive lexical scan: zero forbidden broad-command hits, one controlled volume-remove site, one controlled MinIO-remove site, and zero `down`, volume flag, or arbitrary Docker-container removal tokens.
- `git diff --check`: passed with line-ending normalization warnings only.
- Review finding 8 remains deferred exactly as requested; this round does not change the report tracking decision.
- All Docker, Compose, MinIO, catalog, and Bootstrap mutation paths were mocked. No real destructive or Docker mutation was executed.
