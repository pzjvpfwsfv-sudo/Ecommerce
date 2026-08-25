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

    def test_compose_ps_parser_accepts_array_ndjson_and_single_object_on_powershell_51(self):
        payload = self._powershell_payload(
            r'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
$services = @(
    [pscustomobject]@{ Service = "minio"; State = "running"; Health = "healthy" },
    [pscustomobject]@{ Service = "minio-init"; State = "exited"; Health = ""; ExitCode = 0 }
)
$arrayJson = @($services | ConvertTo-Json -Compress)
$ndjson = @($services | ForEach-Object { $_ | ConvertTo-Json -Compress })
$single = @($services[0] | ConvertTo-Json -Compress)
$fromArray = @(ConvertFrom-Chapter105ComposePsOutput -Lines $arrayJson)
$fromNdjson = @(ConvertFrom-Chapter105ComposePsOutput -Lines $ndjson)
$fromSingle = @(ConvertFrom-Chapter105ComposePsOutput -Lines $single)
[ordered]@{
    array_count = $fromArray.Count
    array_second = $fromArray[1].Service
    ndjson_count = $fromNdjson.Count
    ndjson_second = $fromNdjson[1].Service
    single_count = $fromSingle.Count
    single_service = $fromSingle[0].Service
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(
            {
                "array_count": 2,
                "array_second": "minio-init",
                "ndjson_count": 2,
                "ndjson_second": "minio-init",
                "single_count": 1,
                "single_service": "minio",
            },
            payload,
        )

    def test_job_submission_restores_only_when_validated_recovery_state_exists(self):
        with tempfile.TemporaryDirectory() as directory:
            recovery_root = Path(directory)
            (recovery_root / "tmp" / "chapter-9").mkdir(parents=True)
            (recovery_root / "jobs" / "datastream-quality").mkdir(parents=True)
            (recovery_root / "tmp" / "chapter-9" / "cutover-manifest.json.partial").write_text(
                json.dumps(
                    {
                        "savepoint_path": "s3a://flink-state/savepoints/chapter-9/savepoint-fixed",
                        "production_job_id": "1" * 32,
                        "mutations": {
                            "production_submit": {
                                "status": "not_started",
                                "intent": None,
                                "result": None,
                            }
                        },
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
$recovery = Get-Chapter105Task4RecoveryPlan -RepositoryRoot "{recovery_root}" -SavepointUri "s3a://flink-state/savepoints/chapter-9"
$restoredId = Invoke-Chapter105JobSubmission -RepositoryRoot "{recovery_root}" -ComposePrefix @("compose") -CheckpointUri "s3a://flink-state/checkpoints/chapter-9" -SavepointPath $recovery.savepoint_path -SkipBuild
Remove-Item -LiteralPath "{recovery_root / 'tmp' / 'chapter-9' / 'cutover-manifest.json.partial'}" -Force
$fresh = Get-Chapter105Task4RecoveryPlan -RepositoryRoot "{recovery_root}" -SavepointUri "s3a://flink-state/savepoints/chapter-9"
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

    def test_repository_path_rejects_a_reparse_repository_root(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "target"
            root_link = Path(directory) / "repo-link"
            (target / "infra").mkdir(parents=True)
            payload = self._powershell_payload(
                rf'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
New-Item -ItemType Junction -Path "{root_link}" -Target "{target}" | Out-Null
$rejected = $false
try {{
    Resolve-Chapter105RepositoryPath -RepositoryRoot "{root_link}" -Path "infra/custom.env" -AllowedRelativeRoot "infra" | Out-Null
}} catch {{ $rejected = $true }}
[ordered]@{{ rejected = $rejected }} | ConvertTo-Json -Compress
'''
            )
        self.assertEqual({"rejected": True}, payload)

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

    def test_mixed_case_flink_states_fail_closed_before_mutation(self):
        payload = self._powershell_payload(
            r'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
$target = "chapter-9-datastream-quality-production"
$script:mutations = 0
$payloads = @(
    '{"jobs":[{"jid":"11111111111111111111111111111111","name":"other","state":"running"}]}',
    '{"jobs":[{"jid":"11111111111111111111111111111111","name":"other","state":"RunNing"}]}',
    ('{"jobs":[{"jid":"11111111111111111111111111111111","name":"' + $target + '","state":"running"}]}')
)
$rejected = 0
foreach ($json in $payloads) {
    try {
        Invoke-Chapter105EnsureJob -Overview ($json | ConvertFrom-Json) -JobName $target -SubmitAction {
            $script:mutations++
        } | Out-Null
    } catch { $rejected++ }
}
[ordered]@{ rejected = $rejected; mutations = $script:mutations } | ConvertTo-Json -Compress
'''
        )
        self.assertEqual({"rejected": 3, "mutations": 0}, payload)

    def test_bootstrap_and_task4_share_one_authoritative_submit_boundary_in_both_orders(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = self._powershell_payload(
                rf'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
. "scripts/run_chapter_9_production_cutover.ps1" -FunctionsOnly
$script:mutations = 0
$name = "chapter-9-datastream-quality-production"
$emptyJobs = [pscustomobject]@{{ jobs = @() }}

$bootstrapFirstPath = "{root / 'bootstrap-first.partial'}"
$bootstrapFirst = New-CutoverRecoveryState -CutoverId "bootstrap-first" -Path $bootstrapFirstPath
$bootstrapResult = Invoke-Chapter105ProductionSubmitBoundary -State $bootstrapFirst `
    -StatePath $bootstrapFirstPath -Jobs $emptyJobs -JobName $name -SubmitAction {{
        $script:mutations++
        [pscustomobject]@{{ job_id = "33333333333333333333333333333333" }}
    }}
$afterBootstrap = $script:mutations
$bootstrapPersisted = Get-Content -Raw $bootstrapFirstPath | ConvertFrom-Json
$bootstrapRunning = [pscustomobject]@{{ jobs = @(
    [pscustomobject]@{{ jid = $bootstrapResult.job_id; name = $name; state = "RUNNING" }}
) }}
$task4AfterBootstrap = Invoke-CutoverProductionSubmitBoundary -State $bootstrapPersisted `
    -Path $bootstrapFirstPath -Jobs $bootstrapRunning -ExpectedName $name `
    -Operation "submit_production_from_savepoint" -Details @{{ source = "task4" }} -Action {{
        $script:mutations++
        [pscustomobject]@{{ job_id = "ffffffffffffffffffffffffffffffff" }}
    }}
$afterTask4 = $script:mutations

$task4FirstPath = "{root / 'task4-first.partial'}"
$task4First = New-CutoverRecoveryState -CutoverId "task4-first" -Path $task4FirstPath
$task4Result = Invoke-CutoverProductionSubmitBoundary -State $task4First `
    -Path $task4FirstPath -Jobs $emptyJobs -ExpectedName $name `
    -Operation "submit_production_from_savepoint" -Details @{{ source = "task4" }} -Action {{
        $script:mutations++
        [pscustomobject]@{{ job_id = "44444444444444444444444444444444" }}
    }}
$afterTask4First = $script:mutations
$task4Persisted = Get-Content -Raw $task4FirstPath | ConvertFrom-Json
$task4Running = [pscustomobject]@{{ jobs = @(
    [pscustomobject]@{{ jid = $task4Result.job_id; name = $name; state = "RUNNING" }}
) }}
$bootstrapAfterTask4 = Invoke-Chapter105ProductionSubmitBoundary -State $task4Persisted `
    -StatePath $task4FirstPath -Jobs $task4Running -JobName $name -SubmitAction {{
        $script:mutations++
        [pscustomobject]@{{ job_id = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" }}
    }}
$afterBootstrapSecond = $script:mutations
$bootstrapSaved = Get-Content -Raw $bootstrapFirstPath | ConvertFrom-Json
$task4Saved = Get-Content -Raw $task4FirstPath | ConvertFrom-Json

[ordered]@{{
    bootstrap_then_task4 = $afterTask4 - $afterBootstrap + 1
    task4_then_bootstrap = $afterBootstrapSecond - $afterTask4First + 1
    total_mutations = $script:mutations
    bootstrap_job_id = $task4AfterBootstrap.job_id
    task4_job_id = $bootstrapAfterTask4.job_id
    bootstrap_status = $bootstrapSaved.mutations.production_submit.status
    task4_status = $task4Saved.mutations.production_submit.status
    bootstrap_atomic_job_id = $bootstrapSaved.mutations.production_submit.result.job_id
    task4_atomic_job_id = $task4Saved.mutations.production_submit.result.job_id
}} | ConvertTo-Json -Compress
'''
            )
        self.assertEqual(1, payload["bootstrap_then_task4"])
        self.assertEqual(1, payload["task4_then_bootstrap"])
        self.assertEqual(2, payload["total_mutations"])
        self.assertEqual("3" * 32, payload["bootstrap_job_id"])
        self.assertEqual("4" * 32, payload["task4_job_id"])
        self.assertEqual("result", payload["bootstrap_status"])
        self.assertEqual("result", payload["task4_status"])
        self.assertEqual("3" * 32, payload["bootstrap_atomic_job_id"])
        self.assertEqual("4" * 32, payload["task4_atomic_job_id"])
        cutover_source = (ROOT / "scripts" / "run_chapter_9_production_cutover.ps1").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "$productionResult = Invoke-CutoverProductionSubmitBoundary -State $recoveryState",
            cutover_source,
        )
        self.assertIn(
            "$productionResult = Invoke-CutoverProductionSubmitBoundary -State $manifest",
            cutover_source,
        )

    def test_shared_submit_boundary_reconciles_intent_and_rejects_unsafe_tuples_without_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = self._powershell_payload(
                rf'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
. "scripts/run_chapter_9_production_cutover.ps1" -FunctionsOnly
$name = "chapter-9-datastream-quality-production"
$jobId = "55555555555555555555555555555555"
$safeError = "Production submission recovery state is unsafe."
$secret = "PASSWORD=do-not-leak"
$script:mutations = 0
$errors = @()
$cases = @(
    [pscustomobject]@{{ name = "intent_missing_job"; mutation = [pscustomobject]@{{ status = "intent"; intent = [pscustomobject]@{{ operation = "submit"; details = [pscustomobject]@{{}} }}; result = $null }}; jobs = [pscustomobject]@{{ jobs = @() }} }},
    [pscustomobject]@{{ name = "intent_ambiguous"; mutation = [pscustomobject]@{{ status = "intent"; intent = [pscustomobject]@{{ operation = "submit"; details = [pscustomobject]@{{}} }}; result = $null }}; jobs = [pscustomobject]@{{ jobs = @(
        [pscustomobject]@{{ jid = $jobId; name = $name; state = "RUNNING" }},
        [pscustomobject]@{{ jid = "66666666666666666666666666666666"; name = $name; state = "RUNNING" }}
    ) }} }},
    [pscustomobject]@{{ name = "failed"; mutation = [pscustomobject]@{{ status = "failed"; intent = [pscustomobject]@{{ operation = "submit" }}; result = [pscustomobject]@{{ status = "failed"; details = [pscustomobject]@{{ error = $secret }} }} }}; jobs = [pscustomobject]@{{ jobs = @() }} }},
    [pscustomobject]@{{ name = "mixed_case"; mutation = [pscustomobject]@{{ status = "Result"; intent = [pscustomobject]@{{ operation = "submit" }}; result = [pscustomobject]@{{ status = "result"; job_id = $jobId }} }}; jobs = [pscustomobject]@{{ jobs = @() }} }},
    [pscustomobject]@{{ name = "partial_not_started"; mutation = [pscustomobject]@{{ status = "not_started"; intent = [pscustomobject]@{{ operation = "submit" }}; result = $null }}; jobs = [pscustomobject]@{{ jobs = @() }} }},
    [pscustomobject]@{{ name = "partial_result"; mutation = [pscustomobject]@{{ status = "result"; intent = $null; result = [pscustomobject]@{{ status = "result"; job_id = $jobId }} }}; jobs = [pscustomobject]@{{ jobs = @() }} }},
    [pscustomobject]@{{ name = "partial_intent_record"; mutation = [pscustomobject]@{{ status = "intent"; intent = [pscustomobject]@{{ details = [pscustomobject]@{{}} }}; result = $null }}; jobs = [pscustomobject]@{{ jobs = @([pscustomobject]@{{ jid = $jobId; name = $name; state = "RUNNING" }}) }} }},
    [pscustomobject]@{{ name = "partial_result_intent"; mutation = [pscustomobject]@{{ status = "result"; intent = [pscustomobject]@{{ details = [pscustomobject]@{{}} }}; result = [pscustomobject]@{{ status = "result"; job_id = $jobId }} }}; jobs = [pscustomobject]@{{ jobs = @([pscustomobject]@{{ jid = $jobId; name = $name; state = "RUNNING" }}) }} }}
)
foreach ($case in $cases) {{
    foreach ($entry in @("bootstrap", "task4")) {{
        $path = Join-Path "{root}" "$($case.name)-$entry.partial"
        $state = [pscustomobject]@{{
            schema_version = 2
            cutover_id = "$($case.name)-$entry"
            phase = "production_submit"
            mutations = [pscustomobject]@{{ production_submit = $case.mutation }}
        }}
        Write-CutoverStateAtomic -State $state -Path $path
        try {{
            if ($entry -eq "bootstrap") {{
                Invoke-Chapter105ProductionSubmitBoundary -State $state -StatePath $path `
                    -Jobs $case.jobs -JobName $name -SubmitAction {{
                        $script:mutations++
                        [pscustomobject]@{{ job_id = "77777777777777777777777777777777" }}
                    }} | Out-Null
            }} else {{
                Invoke-CutoverProductionSubmitBoundary -State $state -Path $path `
                    -Jobs $case.jobs -ExpectedName $name -Operation "submit" -Details @{{}} -Action {{
                        $script:mutations++
                        [pscustomobject]@{{ job_id = "77777777777777777777777777777777" }}
                    }} | Out-Null
            }}
            $errors += "NO_ERROR"
        }} catch {{ $errors += $_.Exception.Message }}
    }}
}}

$intentPath = Join-Path "{root}" "intent-reconcile.partial"
$intentState = [pscustomobject]@{{
    schema_version = 2; cutover_id = "intent-reconcile"; phase = "production_submit"
    mutations = [pscustomobject]@{{ production_submit = [pscustomobject]@{{
        status = "intent"; intent = [pscustomobject]@{{ operation = "submit"; details = [pscustomobject]@{{}} }}; result = $null
    }} }}
}}
Write-CutoverStateAtomic -State $intentState -Path $intentPath
$running = [pscustomobject]@{{ jobs = @([pscustomobject]@{{ jid = $jobId; name = $name; state = "RUNNING" }}) }}
$intentBootstrap = Invoke-Chapter105ProductionSubmitBoundary -State $intentState -StatePath $intentPath `
    -Jobs $running -JobName $name -SubmitAction {{ $script:mutations++; throw $secret }}
$intentPersisted = Get-Content -Raw $intentPath | ConvertFrom-Json
$intentTask4 = Invoke-CutoverProductionSubmitBoundary -State $intentPersisted -Path $intentPath `
    -Jobs $running -ExpectedName $name -Operation "submit" -Details @{{}} -Action {{ $script:mutations++; throw $secret }}

[ordered]@{{
    rejected = @($errors | Where-Object {{ $_ -ceq $safeError }}).Count
    error_count = $errors.Count
    safe = (@($errors | Where-Object {{ $_ -match "do-not-leak|PASSWORD|NO_ERROR" }}).Count -eq 0)
    mutations = $script:mutations
    adopted_bootstrap = $intentBootstrap.job_id
    adopted_task4 = $intentTask4.job_id
    adopted_status = (Get-Content -Raw $intentPath | ConvertFrom-Json).mutations.production_submit.status
}} | ConvertTo-Json -Compress
'''
            )
        self.assertEqual(16, payload["rejected"])
        self.assertEqual(16, payload["error_count"])
        self.assertTrue(payload["safe"])
        self.assertEqual(0, payload["mutations"])
        self.assertEqual("5" * 32, payload["adopted_bootstrap"])
        self.assertEqual("5" * 32, payload["adopted_task4"])
        self.assertEqual("result", payload["adopted_status"])

    def test_custom_savepoint_uri_is_validated_and_constrains_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_dir = root / "tmp" / "chapter-9"
            state_dir.mkdir(parents=True)
            manifest = state_dir / "cutover-manifest.json.partial"
            payload = self._powershell_payload(
                rf'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
. "scripts/run_chapter_9_production_cutover.ps1" -FunctionsOnly
$environment = [ordered]@{{
    CHAPTER9_CHECKPOINT_URI = "s3a://flink-state/checkpoints/chapter-9/custom"
    CHAPTER9_SAVEPOINT_URI = "s3a://flink-state/savepoints/chapter-9/custom"
}}
$checkpoint = Get-Chapter9StateUri -Kind checkpoint -Environment $environment
$savepoint = Get-Chapter9StateUri -Kind savepoint -Environment $environment
$state = [ordered]@{{
    savepoint_path = "$savepoint/savepoint-fixed"
    mutations = [ordered]@{{ production_submit = [ordered]@{{ status = "not_started"; intent = $null; result = $null }} }}
}}
$state | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath "{manifest}" -Encoding UTF8
$plan = Get-Chapter105Task4RecoveryPlan -RepositoryRoot "{root}" -SavepointUri $savepoint
$state.savepoint_path = "s3a://flink-state/savepoints/chapter-9/other/savepoint-fixed"
$state | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath "{manifest}" -Encoding UTF8
$wrongBaseRejected = $false
try {{ Get-Chapter105Task4RecoveryPlan -RepositoryRoot "{root}" -SavepointUri $savepoint | Out-Null }} catch {{ $wrongBaseRejected = $true }}
[ordered]@{{ checkpoint = $checkpoint; savepoint = $savepoint; recovered = $plan.savepoint_path; wrong_base_rejected = $wrongBaseRejected }} | ConvertTo-Json -Compress
'''
            )
        self.assertEqual(
            {
                "checkpoint": "s3a://flink-state/checkpoints/chapter-9/custom",
                "savepoint": "s3a://flink-state/savepoints/chapter-9/custom",
                "recovered": "s3a://flink-state/savepoints/chapter-9/custom/savepoint-fixed",
                "wrong_base_rejected": True,
            },
            payload,
        )

    def test_task4_uri_reads_the_controlled_env_map_and_authoritative_state_is_persisted(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "cutover-manifest.json.partial"
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
$result = Invoke-Chapter105ProductionSubmitBoundary -State $state -StatePath "{state_path}" `
    -Jobs ([pscustomobject]@{{ jobs = @() }}) -JobName "chapter-9-datastream-quality-production" -SubmitAction {{
    $script:submissions++
    [pscustomobject]@{{ job_id = "33333333333333333333333333333333" }}
}}
$running = [pscustomobject]@{{ jobs = @([pscustomobject]@{{
    jid = $result.job_id; name = "chapter-9-datastream-quality-production"; state = "RUNNING"
}}) }}
$replay = Invoke-Chapter105ProductionSubmitBoundary -State $state -StatePath "{state_path}" `
    -Jobs $running -JobName "chapter-9-datastream-quality-production" -SubmitAction {{
        $script:submissions++
        throw "must not replay"
    }}
$saved = Get-Content -Raw "{state_path}" | ConvertFrom-Json
[ordered]@{{
    checkpoint = $checkpoint
    savepoint = $savepoint
    job_id = $result.job_id
    status = $saved.mutations.production_submit.status
    operation = $saved.mutations.production_submit.intent.operation
    persisted_job_id = $saved.mutations.production_submit.result.details.job_id
    replay_job_id = $replay.job_id
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
                "replay_job_id": "3" * 32,
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

    def test_early_root_and_default_report_failures_use_the_fixed_fallback_envelope(self):
        fallback = ROOT / "tmp" / "chapter-10-5" / "bootstrap-report.fallback.json"
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "target"
            root_link = Path(directory) / "repo-link"
            (target / "tmp" / "chapter-10-5").mkdir(parents=True)
            if fallback.exists():
                fallback.unlink()
            payload = self._powershell_payload(
                rf'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
$script:rootMode = "lookup"
function Get-Chapter105PrimaryRepositoryRoot {{
    if ($script:rootMode -eq "lookup") {{ throw "root lookup leaked detail" }}
    return "{root_link}"
}}
$lookupError = ""
try {{ Invoke-Chapter105Bootstrap -EnvFile "infra/.env" -ReportPath "tmp/chapter-10-5/report.json" }} catch {{ $lookupError = $_.Exception.Message }}
$lookupReport = Get-Content -LiteralPath "{fallback}" -Raw | ConvertFrom-Json
Remove-Item -LiteralPath "{fallback}" -Force
New-Item -ItemType Junction -Path "{root_link}" -Target "{target}" | Out-Null
$script:rootMode = "junction"
$junctionError = ""
try {{ Invoke-Chapter105Bootstrap -EnvFile "infra/.env" -ReportPath "tmp/chapter-10-5/report.json" }} catch {{ $junctionError = $_.Exception.Message }}
$junctionReport = Get-Content -LiteralPath "{fallback}" -Raw | ConvertFrom-Json
[ordered]@{{
    lookup_error = $lookupError
    lookup_status = $lookupReport.status
    lookup_preflight = $lookupReport.stages.preflight.status
    junction_error = $junctionError
    junction_status = $junctionReport.status
    junction_preflight = $junctionReport.stages.preflight.status
}} | ConvertTo-Json -Compress
'''
            )
        if fallback.exists():
            fallback.unlink()
        expected_error = "Chapter 10.5 bootstrap failed. See the bootstrap report for safe stage status."
        self.assertEqual(expected_error, payload["lookup_error"])
        self.assertEqual("failed", payload["lookup_status"])
        self.assertEqual("failed", payload["lookup_preflight"])
        self.assertEqual(expected_error, payload["junction_error"])
        self.assertEqual("failed", payload["junction_status"])
        self.assertEqual("failed", payload["junction_preflight"])

    def test_original_bootstrap_error_wins_when_fallback_report_write_also_fails(self):
        payload = self._powershell_payload(
            r'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
function Get-Chapter105PrimaryRepositoryRoot { throw "root lookup leaked detail" }
function Write-Chapter105BootstrapReport { throw "report medium leaked detail" }
$errorMessage = ""
try { Invoke-Chapter105Bootstrap } catch { $errorMessage = $_.Exception.Message }
function Invoke-Chapter105BootstrapStage {
    param($Report, $Name, [scriptblock]$Action)
}
$reportOnlyError = ""
try { Invoke-Chapter105Bootstrap } catch { $reportOnlyError = $_.Exception.Message }
[ordered]@{ error = $errorMessage; report_only_error = $reportOnlyError } | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(
            {
                "error": "Chapter 10.5 bootstrap failed. See the bootstrap report for safe stage status.",
                "report_only_error": "Chapter 10.5 bootstrap report could not be written safely.",
            },
            payload,
        )

    def test_functions_only_preserves_caller_state_and_exposes_only_explicit_interfaces(self):
        payload = self._powershell_payload(
            r'''
Set-StrictMode -Off
$beforeUndefinedThrows = $false
try { $null = $undefinedBefore } catch { $beforeUndefinedThrows = $true }
function Test-DependencyHash { return "caller-private-helper" }
$beforePrivate = (Get-Command Test-DependencyHash).ScriptBlock.ToString()
$beforeAbsent = $null -eq (Get-Command Get-RuntimeDependencyArtifacts -ErrorAction SilentlyContinue)
$beforeModules = @((Get-Module).Name)
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
$afterModules = @((Get-Module).Name)
$afterUndefinedThrows = $false
try { $null = $undefinedAfter } catch { $afterUndefinedThrows = $true }
$afterPrivate = (Get-Command Test-DependencyHash).ScriptBlock.ToString()
$explicit = @(
    "Read-Chapter105EnvFile",
    "Resolve-Chapter105RepositoryPath",
    "Invoke-Chapter105Native",
    "Get-Chapter105FlinkJobDecision",
    "ConvertFrom-Chapter105ComposePsOutput",
    "Invoke-Chapter105Bootstrap"
)
$explicitAvailable = @($explicit | Where-Object { $null -ne (Get-Command $_ -ErrorAction SilentlyContinue) }).Count
[ordered]@{
    modules_equal = (($beforeModules -join "|") -eq ($afterModules -join "|"))
    strict_equal = $beforeUndefinedThrows -eq $afterUndefinedThrows
    private_equal = $beforePrivate -ceq $afterPrivate
    private_result = Test-DependencyHash
    absent_private_equal = $beforeAbsent -and ($null -eq (Get-Command Get-RuntimeDependencyArtifacts -ErrorAction SilentlyContinue))
    installer_hidden = $null -eq (Get-Command Install-RuntimeDependencies -ErrorAction SilentlyContinue)
    explicit_available = $explicitAvailable
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(
            {
                "modules_equal": True,
                "strict_equal": True,
                "private_equal": True,
                "private_result": "caller-private-helper",
                "absent_private_equal": True,
                "installer_hidden": True,
                "explicit_available": 6,
            },
            payload,
        )

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
