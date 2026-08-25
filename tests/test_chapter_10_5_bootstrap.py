import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
BOOTSTRAP = ROOT / "scripts" / "bootstrap_chapter_10_5.ps1"


class Chapter105BootstrapTest(unittest.TestCase):
    def _run_powershell(self, command):
        return subprocess.run(
            ["powershell", "-NoProfile", "-Command", command],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )

    def _powershell_payload(self, command):
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        return json.loads(result.stdout.strip().splitlines()[-1])

    def test_functions_only_parses_env_and_rejects_duplicate_or_empty_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            env_file.write_text("GOOD=value\n", encoding="utf-8")
            duplicate_file = Path(directory) / "duplicate.env"
            duplicate_file.write_text("GOOD=one\nGOOD=two\n", encoding="utf-8")
            empty_key_file = Path(directory) / "empty.env"
            empty_key_file.write_text("=value\n", encoding="utf-8")
            payload = self._powershell_payload(
                rf'''
. (Resolve-Path "scripts/bootstrap_chapter_10_5.ps1") -FunctionsOnly
$valid = Read-Chapter105EnvFile -Path "{env_file}"
$duplicateRejected = $false
$emptyRejected = $false
try {{ Read-Chapter105EnvFile -Path "{duplicate_file}" | Out-Null }} catch {{ $duplicateRejected = $true }}
try {{ Read-Chapter105EnvFile -Path "{empty_key_file}" | Out-Null }} catch {{ $emptyRejected = $true }}
[ordered]@{{ valid = $valid["GOOD"]; duplicate_rejected = $duplicateRejected; empty_rejected = $emptyRejected }} | ConvertTo-Json -Compress
'''
            )
        self.assertEqual(
            {"valid": "value", "duplicate_rejected": True, "empty_rejected": True}, payload
        )

    def test_functions_only_resolves_stable_minio_path_at_primary_repo_root(self):
        payload = self._powershell_payload(
            r'''
. (Resolve-Path "scripts/bootstrap_chapter_10_5.ps1") -FunctionsOnly
$root = Get-Chapter105PrimaryRepositoryRoot -StartPath (Get-Location)
$path = Get-Chapter105StableMinioDataPath -RepositoryRoot $root
[ordered]@{ root = $root; path = $path; inside_worktree = $path -match "[\\/]\.worktrees[\\/]" } | ConvertTo-Json -Compress
'''
        )
        expected_root = ROOT.parent.parent
        self.assertEqual(str(expected_root), payload["root"])
        self.assertEqual(str(expected_root / "infra" / "compose" / "minio" / "data"), payload["path"])
        self.assertFalse(payload["inside_worktree"])

    def test_native_failure_is_scrubbed_and_retry_stops_at_limit(self):
        payload = self._powershell_payload(
            r'''
. (Resolve-Path "scripts/bootstrap_chapter_10_5.ps1") -FunctionsOnly
function docker { $global:LASTEXITCODE = 17; return "PASSWORD=native-secret" }
$nativeFailure = ""
try { Invoke-Chapter105Native -FilePath "docker" -Arguments @("version") -FailureMessage "Docker failed." | Out-Null } catch { $nativeFailure = $_.Exception.Message }
$script:attempts = 0
$retryFailure = ""
try {
    Invoke-Chapter105Retry -Attempts 3 -SleepSeconds 0 -FailureMessage "Retry exhausted." -Action {
        $script:attempts++
        throw "SECRET=retry-secret"
    } | Out-Null
} catch { $retryFailure = $_.Exception.Message }
[ordered]@{
    native_failure = $nativeFailure
    native_safe = $nativeFailure -notmatch "native-secret|PASSWORD"
    retry_failure = $retryFailure
    attempts = $script:attempts
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual("Docker failed.", payload["native_failure"])
        self.assertTrue(payload["native_safe"])
        self.assertEqual("Retry exhausted.", payload["retry_failure"])
        self.assertEqual(3, payload["attempts"])

    def test_flink_job_decision_fails_closed_except_one_running_or_no_match(self):
        payload = self._powershell_payload(
            r'''
. (Resolve-Path "scripts/bootstrap_chapter_10_5.ps1") -FunctionsOnly
$target = "chapter-9-datastream-quality-production"
$running = Get-Chapter105FlinkJobDecision -Overview ('{"jobs":[{"jid":"11111111111111111111111111111111","name":"' + $target + '","state":"RUNNING"}]} ' | ConvertFrom-Json) -JobName $target
$missing = Get-Chapter105FlinkJobDecision -Overview ('{"jobs":[]}' | ConvertFrom-Json) -JobName $target
$duplicateRejected = $false
$stoppedRejected = $false
try {
    Get-Chapter105FlinkJobDecision -Overview ('{"jobs":[{"jid":"11111111111111111111111111111111","name":"' + $target + '","state":"RUNNING"},{"jid":"22222222222222222222222222222222","name":"' + $target + '","state":"RUNNING"}]} ' | ConvertFrom-Json) -JobName $target | Out-Null
} catch { $duplicateRejected = $true }
try {
    Get-Chapter105FlinkJobDecision -Overview ('{"jobs":[{"jid":"11111111111111111111111111111111","name":"' + $target + '","state":"FAILED"}]} ' | ConvertFrom-Json) -JobName $target | Out-Null
} catch { $stoppedRejected = $true }
[ordered]@{ running = $running.action; running_id = $running.job_id; missing = $missing.action; duplicate_rejected = $duplicateRejected; stopped_rejected = $stoppedRejected } | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(
            {
                "running": "no_op",
                "running_id": "11111111111111111111111111111111",
                "missing": "submit",
                "duplicate_rejected": True,
                "stopped_rejected": True,
            },
            payload,
        )

    def test_report_is_atomic_and_recursively_redacts_keys_and_values(self):
        with tempfile.TemporaryDirectory() as directory:
            report_file = Path(directory) / "nested" / "bootstrap.json"
            payload = self._powershell_payload(
                rf'''
. (Resolve-Path "scripts/bootstrap_chapter_10_5.ps1") -FunctionsOnly
$opaquePassword = "opaque-7f3a-value"
$opaqueApiKey = "opaque-91bd-value"
$report = [ordered]@{{
    status = "failed"
    details = [ordered]@{{ PASSWORD = $opaquePassword; nested = @([ordered]@{{ API_KEY = $opaqueApiKey; message = "SECRET=inline-secret" }}) }}
}}
Write-Chapter105BootstrapReport -Report $report -Path "{report_file}"
$text = [System.IO.File]::ReadAllText("{report_file}")
[ordered]@{{
    exists = Test-Path -LiteralPath "{report_file}"
    secret_free = $text -notmatch "opaque-7f3a|opaque-91bd|inline-secret|PASSWORD|API_KEY"
    parsed = ($text | ConvertFrom-Json).status
}} | ConvertTo-Json -Compress
'''
            )
        self.assertEqual({"exists": True, "secret_free": True, "parsed": "failed"}, payload)

    def test_compose_readiness_uses_all_and_requires_every_service_state(self):
        payload = self._powershell_payload(
            r'''
. (Resolve-Path "scripts/bootstrap_chapter_10_5.ps1") -FunctionsOnly
$script:calls = @()
function Invoke-Chapter105Native {
    param([string]$FilePath, [string[]]$Arguments, [string]$FailureMessage)
    $script:calls += ,@($Arguments)
    return @(
        '{"Service":"kafka-controller","State":"running","Health":""}',
        '{"Service":"kafka-broker","State":"running","Health":""}',
        '{"Service":"api","State":"running","Health":""}',
        '{"Service":"flink-jobmanager","State":"running","Health":""}',
        '{"Service":"flink-taskmanager","State":"running","Health":""}',
        '{"Service":"flink-sql-client","State":"running","Health":""}',
        '{"Service":"doris-fe","State":"running","Health":""}',
        '{"Service":"doris-be","State":"running","Health":""}',
        '{"Service":"minio","State":"running","Health":"healthy"}',
        '{"Service":"minio-init","State":"exited","Health":"","ExitCode":0}',
        '{"Service":"metastore-postgres","State":"running","Health":"healthy"}',
        '{"Service":"hive-metastore","State":"running","Health":""}',
        '{"Service":"trino","State":"running","Health":""}'
    )
}
$result = Wait-Chapter105ComposeReady -ComposePrefix @("compose-prefix") -Attempts 1 -SleepSeconds 0
[ordered]@{ all = $script:calls[0] -contains "--all"; services = $result.services } | ConvertTo-Json -Compress
'''
        )
        self.assertEqual({"all": True, "services": 13}, payload)

    def test_job_submission_restores_only_when_validated_recovery_state_exists(self):
        with tempfile.TemporaryDirectory() as directory:
            recovery_root = Path(directory)
            (recovery_root / "tmp" / "chapter-9").mkdir(parents=True)
            (recovery_root / "jobs" / "datastream-quality").mkdir(parents=True)
            (recovery_root / "tmp" / "chapter-9" / "cutover-manifest.json").write_text(
                json.dumps(
                    {
                        "savepoint_path": "s3a://flink-state/savepoints/chapter-9/savepoint-fixed",
                        "production_job_id": "1" * 32,
                    }
                ),
                encoding="utf-8",
            )
            payload = self._powershell_payload(
                rf'''
. (Resolve-Path "scripts/bootstrap_chapter_10_5.ps1") -FunctionsOnly
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
$script:commands = @()
function Invoke-Chapter105Native {{
    param([string]$FilePath, [string[]]$Arguments, [string]$FailureMessage)
    $script:commands += ,@($Arguments)
    return @("Job has been submitted with JobID 22222222222222222222222222222222")
}}
$recovery = Get-Chapter105Task4RecoveryPlan -RepositoryRoot "{recovery_root}"
$restoredId = Invoke-Chapter105JobSubmission -RepositoryRoot "{recovery_root}" -ComposePrefix @("compose") -CheckpointUri "s3a://flink-state/checkpoints/chapter-9" -SavepointPath $recovery.savepoint_path -SkipBuild
Remove-Item -LiteralPath "{recovery_root / 'tmp' / 'chapter-9' / 'cutover-manifest.json'}" -Force
$fresh = Get-Chapter105Task4RecoveryPlan -RepositoryRoot "{recovery_root}"
$freshId = Invoke-Chapter105JobSubmission -RepositoryRoot "{recovery_root}" -ComposePrefix @("compose") -CheckpointUri "s3a://flink-state/checkpoints/chapter-9" -SkipBuild
[ordered]@{{
    recovery_action = $recovery.action
    fresh_action = $fresh.action
    restored_has_s = $script:commands[0] -contains "-s"
    restored_path = $script:commands[0][[array]::IndexOf($script:commands[0], "-s") + 1]
    fresh_has_s = $script:commands[1] -contains "-s"
    restored_id = $restoredId
    fresh_id = $freshId
}} | ConvertTo-Json -Compress
'''
            )
        self.assertEqual("restore", payload["recovery_action"])
        self.assertEqual("fresh", payload["fresh_action"])
        self.assertTrue(payload["restored_has_s"])
        self.assertEqual(
            "s3a://flink-state/savepoints/chapter-9/savepoint-fixed",
            payload["restored_path"],
        )
        self.assertFalse(payload["fresh_has_s"])
        self.assertEqual("2" * 32, payload["restored_id"])
        self.assertEqual("2" * 32, payload["fresh_id"])

    def test_repository_paths_reject_traversal_escape_and_reparse_points(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory) / "repo"
            outside = Path(directory) / "outside"
            (repo / "infra").mkdir(parents=True)
            (repo / "tmp" / "chapter-10-5").mkdir(parents=True)
            outside.mkdir()
            (repo / "infra" / "custom.env").touch()
            (repo / "tmp" / "chapter-10-5" / "report.json").touch()
            payload = self._powershell_payload(
                rf'''
. (Resolve-Path "scripts/bootstrap_chapter_10_5.ps1") -FunctionsOnly
$validEnv = Resolve-Chapter105RepositoryPath -RepositoryRoot "{repo}" -Path "infra/custom.env" -AllowedRelativeRoot "infra"
$validReport = Resolve-Chapter105RepositoryPath -RepositoryRoot "{repo}" -Path "tmp/chapter-10-5/report.json" -AllowedRelativeRoot "tmp/chapter-10-5"
$traversal = $false
$escape = $false
$reparse = $false
try {{ Resolve-Chapter105RepositoryPath -RepositoryRoot "{repo}" -Path "tmp/chapter-10-5/../escape.json" -AllowedRelativeRoot "tmp/chapter-10-5" | Out-Null }} catch {{ $traversal = $true }}
try {{ Resolve-Chapter105RepositoryPath -RepositoryRoot "{repo}" -Path "{outside / 'escape.json'}" -AllowedRelativeRoot "tmp/chapter-10-5" | Out-Null }} catch {{ $escape = $true }}
New-Item -ItemType Junction -Path "{repo / 'tmp' / 'chapter-10-5' / 'linked'}" -Target "{outside}" | Out-Null
try {{ Resolve-Chapter105RepositoryPath -RepositoryRoot "{repo}" -Path "tmp/chapter-10-5/linked/report.json" -AllowedRelativeRoot "tmp/chapter-10-5" | Out-Null }} catch {{ $reparse = $true }}
[ordered]@{{ valid_env = $validEnv; valid_report = $validReport; traversal = $traversal; escape = $escape; reparse = $reparse }} | ConvertTo-Json -Compress
'''
            )
            self.assertTrue(
                os.path.samefile(repo / "infra" / "custom.env", payload["valid_env"])
            )
            self.assertTrue(
                os.path.samefile(
                    repo / "tmp" / "chapter-10-5" / "report.json",
                    payload["valid_report"],
                )
            )
            self.assertTrue(payload["traversal"])
            self.assertTrue(payload["escape"])
            self.assertTrue(payload["reparse"])

    def test_malformed_flink_job_collections_fail_before_mutation(self):
        payload = self._powershell_payload(
            r'''
. (Resolve-Path "scripts/bootstrap_chapter_10_5.ps1") -FunctionsOnly
$target = "chapter-9-datastream-quality-production"
$script:mutations = 0
function Invoke-Chapter105JobSubmission { $script:mutations++ }
$payloads = @(
    '{"jobs":"malformed"}',
    '{"jobs":{}}',
    '{"jobs":[null]}',
    '{"jobs":["bad"]}',
    '{"jobs":[{"jid":7,"name":"other","state":"RUNNING"}]}',
    '{"jobs":[{"jid":"11111111111111111111111111111111","name":7,"state":"RUNNING"}]}',
    '{"jobs":[{"jid":"11111111111111111111111111111111","name":"other","state":"UNKNOWN"}]}'
)
$rejected = 0
foreach ($json in $payloads) {
    try {
        Invoke-Chapter105EnsureJob -Overview ($json | ConvertFrom-Json) -JobName $target -SubmitAction { Invoke-Chapter105JobSubmission }
    } catch { $rejected++ }
}
[ordered]@{ function_available = $null -ne (Get-Command Invoke-Chapter105EnsureJob -ErrorAction SilentlyContinue); rejected = $rejected; mutations = $script:mutations } | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(
            {"function_available": True, "rejected": 7, "mutations": 0}, payload
        )

    def test_task4_uri_reads_the_controlled_env_map_and_mutation_state_is_persisted(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "job-recovery.json"
            payload = self._powershell_payload(
                rf'''
. (Resolve-Path "scripts/bootstrap_chapter_10_5.ps1") -FunctionsOnly
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
$configured = [ordered]@{{
    CHAPTER9_CHECKPOINT_URI = "s3a://flink-state/checkpoints/chapter-9/custom"
    CHAPTER9_SAVEPOINT_URI = "s3a://flink-state/savepoints/chapter-9/custom"
}}
$checkpoint = Get-Chapter9StateUri -Kind checkpoint -Environment $configured
$savepoint = Get-Chapter9StateUri -Kind savepoint -Environment $configured
$state = New-CutoverRecoveryState -CutoverId "bootstrap-test" -Path "{state_path}"
$script:submissions = 0
$result = Invoke-Chapter105PersistentJobMutation -State $state -StatePath "{state_path}" -SubmitAction {{
    $script:submissions++
    [pscustomobject]@{{ job_id = "33333333333333333333333333333333" }}
}}
$replayRejected = $false
try {{
    Invoke-Chapter105PersistentJobMutation -State $state -StatePath "{state_path}" -SubmitAction {{
        $script:submissions++
    }} | Out-Null
}} catch {{ $replayRejected = $true }}
$saved = Get-Content -Raw "{state_path}" | ConvertFrom-Json
[ordered]@{{
    checkpoint = $checkpoint
    savepoint = $savepoint
    job_id = $result.job_id
    status = $saved.mutations.production_submit.status
    operation = $saved.mutations.production_submit.intent.operation
    persisted_job_id = $saved.mutations.production_submit.result.details.job_id
    replay_rejected = $replayRejected
    submissions = $script:submissions
}} | ConvertTo-Json -Compress
'''
            )
        self.assertEqual(
            {
                "checkpoint": "s3a://flink-state/checkpoints/chapter-9/custom",
                "savepoint": "s3a://flink-state/savepoints/chapter-9/custom",
                "job_id": "3" * 32,
                "status": "result",
                "operation": "bootstrap_production_submit",
                "persisted_job_id": "3" * 32,
                "replay_rejected": True,
                "submissions": 1,
            },
            payload,
        )

    def test_ready_and_completed_checkpoint_use_bounded_retry(self):
        payload = self._powershell_payload(
            r'''
. (Resolve-Path "scripts/bootstrap_chapter_10_5.ps1") -FunctionsOnly
$script:readyCalls = 0
function Invoke-WebRequest {
    $script:readyCalls++
    if ($script:readyCalls -lt 3) { throw "not ready" }
    [pscustomobject]@{ StatusCode = 200; Content = '{"status":"ready"}' }
}
$ready = Wait-Chapter105Ready -ApiPort 8000 -Attempts 3 -SleepSeconds 0
$script:checkpointCalls = 0
function Invoke-Chapter105FlinkResource {
    $script:checkpointCalls++
    $timestamp = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    if ($script:checkpointCalls -lt 3) {
        return ('{"counts":{"completed":0},"latest":{}}' | ConvertFrom-Json)
    }
    return ('{"counts":{"completed":1},"latest":{"completed":{"status":"COMPLETED","latest_ack_timestamp":' + $timestamp + '}}}' | ConvertFrom-Json)
}
$checkpoint = Wait-Chapter105CompletedCheckpoint -FlinkPort 8081 -JobId "11111111111111111111111111111111" -MaxAgeSeconds 120 -Attempts 3 -SleepSeconds 0
[ordered]@{ ready = $ready; ready_calls = $script:readyCalls; checkpoint = $checkpoint; checkpoint_calls = $script:checkpointCalls } | ConvertTo-Json -Compress
'''
        )
        self.assertEqual("ready", payload["ready"])
        self.assertEqual(3, payload["ready_calls"])
        self.assertEqual("completed", payload["checkpoint"])
        self.assertEqual(3, payload["checkpoint_calls"])

    def test_preflight_env_failure_closes_stage_and_writes_safe_report(self):
        report_name = f"preflight-failure-{next(tempfile._get_candidate_names())}.json"
        report_path = ROOT.parent.parent / "tmp" / "chapter-10-5" / report_name
        result = self._run_powershell(
            f'& "{BOOTSTRAP}" -EnvFile "infra/missing-task-9.env" -ReportPath "tmp/chapter-10-5/{report_name}"'
        )
        self.assertNotEqual(0, result.returncode)
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        finally:
            if report_path.exists():
                report_path.unlink()
        preflight = report["stages"]["preflight"]
        self.assertEqual("failed", preflight["status"])
        self.assertIsNotNone(preflight["started_at"])
        self.assertIsNotNone(preflight["completed_at"])
        self.assertEqual("failed", report["status"])

    def test_functions_only_preserves_caller_strict_mode_and_module_state(self):
        payload = self._powershell_payload(
            r'''
Set-StrictMode -Off
$beforeModules = @((Get-Module).Name)
$beforeUndefinedThrows = $false
try { $null = $undefinedBefore } catch { $beforeUndefinedThrows = $true }
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
$afterModules = @((Get-Module).Name)
$afterUndefinedThrows = $false
try { $null = $undefinedAfter } catch { $afterUndefinedThrows = $true }
[ordered]@{
    modules_equal = (($beforeModules -join "|") -eq ($afterModules -join "|"))
    strict_equal = $beforeUndefinedThrows -eq $afterUndefinedThrows
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual({"modules_equal": True, "strict_equal": True}, payload)

    def test_functions_only_import_never_runs_native_commands(self):
        payload = self._powershell_payload(
            r'''
$script:dockerCalls = 0
function docker { $script:dockerCalls++; throw "must not execute during FunctionsOnly import" }
. (Resolve-Path "scripts/bootstrap_chapter_10_5.ps1") -FunctionsOnly
[ordered]@{ docker_calls = $script:dockerCalls } | ConvertTo-Json -Compress
'''
        )
        self.assertEqual({"docker_calls": 0}, payload)


if __name__ == "__main__":
    unittest.main()
