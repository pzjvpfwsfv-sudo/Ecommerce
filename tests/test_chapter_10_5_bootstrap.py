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

    def _run_git(self, cwd, *arguments):
        result = subprocess.run(
            ["git", *arguments],
            cwd=cwd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        return result.stdout.strip()

    def _create_linked_worktrees(self, directory, name):
        primary = Path(directory) / f"{name}-primary"
        bootstrap_worktree = Path(directory) / f"{name}-bootstrap"
        task4_worktree = Path(directory) / f"{name}-task4"
        primary.mkdir()
        self._run_git(primary, "init")
        self._run_git(primary, "config", "user.email", "task9@example.invalid")
        self._run_git(primary, "config", "user.name", "Task 9 Test")
        self._run_git(primary, "commit", "--allow-empty", "-m", "fixture")
        self._run_git(primary, "worktree", "add", "-b", "bootstrap", bootstrap_worktree)
        self._run_git(primary, "worktree", "add", "-b", "task4", task4_worktree)
        return primary, bootstrap_worktree, task4_worktree

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

    def test_linked_worktree_defaults_share_primary_production_state_and_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            primary, bootstrap_worktree, task4_worktree = self._create_linked_worktrees(
                directory, "roots"
            )
            for root in (primary, bootstrap_worktree, task4_worktree):
                (root / "tmp" / "chapter-9").mkdir(parents=True)
            payload = self._powershell_payload(
                rf'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
. "scripts/run_chapter_9_production_cutover.ps1" -FunctionsOnly
$bootstrapState = Get-CutoverProductionSubmitStatePath -RepositoryRoot "{bootstrap_worktree}"
$task4State = Get-CutoverProductionSubmitStatePath -RepositoryRoot "{task4_worktree}"
[ordered]@{{
    bootstrap_state = $bootstrapState
    task4_state = $task4State
    bootstrap_lock = "$bootstrapState.lock"
    task4_lock = "$task4State.lock"
}} | ConvertTo-Json -Compress
'''
            )

            expected_state = primary / "tmp" / "chapter-9" / "production-submit-state.json"
            expected_directory = expected_state.parent
            self.assertEqual("production-submit-state.json", Path(payload["bootstrap_state"]).name)
            self.assertEqual("production-submit-state.json", Path(payload["task4_state"]).name)
            self.assertEqual("production-submit-state.json.lock", Path(payload["bootstrap_lock"]).name)
            self.assertEqual("production-submit-state.json.lock", Path(payload["task4_lock"]).name)
            self.assertTrue(
                os.path.samefile(expected_directory, Path(payload["bootstrap_state"]).parent)
            )
            self.assertTrue(
                os.path.samefile(expected_directory, Path(payload["task4_state"]).parent)
            )
            self.assertTrue(
                os.path.samefile(expected_directory, Path(payload["bootstrap_lock"]).parent)
            )
            self.assertTrue(
                os.path.samefile(expected_directory, Path(payload["task4_lock"]).parent)
            )

    def test_explicit_state_root_is_absolute_fixed_and_reparse_safe(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository = root / "repository"
            state_root = root / "isolated-state"
            outside = root / "outside"
            junction = root / "state-junction"
            state_file = root / "state-file"
            for path in (repository, state_root, outside):
                path.mkdir()
            state_file.write_text("not a directory", encoding="utf-8")
            payload = self._powershell_payload(
                rf'''
. "scripts/run_chapter_9_production_cutover.ps1" -FunctionsOnly
New-Item -ItemType Junction -Path "{junction}" -Target "{outside}" | Out-Null
$valid = Get-CutoverProductionSubmitStatePath -RepositoryRoot "{repository}" -StateRoot "{state_root}"
$relativeRejected = $false
$junctionRejected = $false
$nestedJunctionRejected = $false
$fileRejected = $false
try {{
    Get-CutoverProductionSubmitStatePath -RepositoryRoot "{repository}" -StateRoot "relative-state" | Out-Null
}} catch {{ $relativeRejected = $_.Exception.Message -ceq "Production submission recovery state is unsafe." }}
try {{
    Get-CutoverProductionSubmitStatePath -RepositoryRoot "{repository}" -StateRoot "{junction}" | Out-Null
}} catch {{ $junctionRejected = $_.Exception.Message -ceq "Production submission recovery state is unsafe." }}
New-Item -ItemType Junction -Path (Join-Path "{state_root}" "tmp") -Target "{outside}" | Out-Null
try {{
    Get-CutoverProductionSubmitStatePath -RepositoryRoot "{repository}" -StateRoot "{state_root}" | Out-Null
}} catch {{ $nestedJunctionRejected = $_.Exception.Message -ceq "Production submission recovery state is unsafe." }}
try {{
    Get-CutoverProductionSubmitStatePath -RepositoryRoot "{repository}" -StateRoot "{state_file}" | Out-Null
}} catch {{ $fileRejected = $_.Exception.Message -ceq "Production submission recovery state is unsafe." }}
[ordered]@{{
    valid = $valid
    relative_rejected = $relativeRejected
    junction_rejected = $junctionRejected
    nested_junction_rejected = $nestedJunctionRejected
    file_rejected = $fileRejected
}} | ConvertTo-Json -Compress
'''
            )

            valid_path = Path(payload["valid"])
            self.assertEqual(
                ("tmp", "chapter-9", "production-submit-state.json"),
                tuple(valid_path.parts[-3:]),
            )
            self.assertTrue(os.path.samefile(state_root, valid_path.parents[2]))
            self.assertTrue(payload["relative_rejected"])
            self.assertTrue(payload["junction_rejected"])
            self.assertTrue(payload["nested_junction_rejected"])
            self.assertTrue(payload["file_rejected"])

    def test_linked_worktree_production_orchestrators_share_defaults_in_both_orders(self):
        with tempfile.TemporaryDirectory() as directory:
            first_primary, first_bootstrap, first_task4 = self._create_linked_worktrees(
                directory, "bootstrap-first"
            )
            second_primary, second_bootstrap, second_task4 = self._create_linked_worktrees(
                directory, "task4-first"
            )
            payload = self._powershell_payload(
                rf'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
. "scripts/run_chapter_9_production_cutover.ps1" -FunctionsOnly
$name = "chapter-9-datastream-quality-production"
$emptyJobs = [pscustomobject]@{{ jobs = @() }}
$script:mutations = 0
$errors = @()

$bootstrapFirst = Invoke-Chapter105JobsStage -RepositoryRoot "{first_bootstrap}" `
    -Overview $emptyJobs -JobName $name -SavepointUri "s3a://flink-state/savepoints/chapter-9" `
    -SubmitAction {{
        param($savepointPath)
        $script:mutations++
        [pscustomobject]@{{ job_id = "33333333333333333333333333333333" }}
    }}
$firstRunning = [pscustomobject]@{{ jobs = @(
    [pscustomobject]@{{ jid = $bootstrapFirst.job_id; name = $name; state = "RUNNING" }}
) }}
$task4AfterBootstrap = $null
try {{
    $task4AfterBootstrap = Invoke-CutoverProductionSubmitStage -RepositoryRoot "{first_task4}" `
        -Jobs $firstRunning -ExpectedName $name -Action {{
            $script:mutations++
            [pscustomobject]@{{ job_id = "ffffffffffffffffffffffffffffffff" }}
        }}
}} catch {{ $errors += $_.Exception.Message }}

$task4First = Invoke-CutoverProductionSubmitStage -RepositoryRoot "{second_task4}" `
    -Jobs $emptyJobs -ExpectedName $name -Action {{
        $script:mutations++
        [pscustomobject]@{{ job_id = "44444444444444444444444444444444" }}
    }}
$secondRunning = [pscustomobject]@{{ jobs = @(
    [pscustomobject]@{{ jid = $task4First.job_id; name = $name; state = "RUNNING" }}
) }}
$bootstrapAfterTask4 = $null
try {{
    $bootstrapAfterTask4 = Invoke-Chapter105JobsStage -RepositoryRoot "{second_bootstrap}" `
        -Overview $secondRunning -JobName $name -SavepointUri "s3a://flink-state/savepoints/chapter-9" `
        -SubmitAction {{
            param($savepointPath)
            $script:mutations++
            [pscustomobject]@{{ job_id = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" }}
        }}
}} catch {{ $errors += $_.Exception.Message }}

[ordered]@{{
    errors = $errors
    mutations = $script:mutations
    bootstrap_first_action = $bootstrapFirst.action
    task4_after_bootstrap_action = if ($null -ne $task4AfterBootstrap) {{ $task4AfterBootstrap.action }} else {{ $null }}
    task4_first_action = $task4First.action
    bootstrap_after_task4_action = if ($null -ne $bootstrapAfterTask4) {{ $bootstrapAfterTask4.action }} else {{ $null }}
    first_state = Get-CutoverProductionSubmitStatePath -RepositoryRoot "{first_task4}"
    second_state = Get-CutoverProductionSubmitStatePath -RepositoryRoot "{second_bootstrap}"
}} | ConvertTo-Json -Depth 4 -Compress
'''
            )

            self.assertEqual([], payload["errors"])
            self.assertEqual(2, payload["mutations"])
            self.assertEqual("submitted", payload["bootstrap_first_action"])
            self.assertEqual("no_op", payload["task4_after_bootstrap_action"])
            self.assertEqual("submitted", payload["task4_first_action"])
            self.assertEqual("no_op", payload["bootstrap_after_task4_action"])
            self.assertTrue(
                os.path.samefile(
                    first_primary / "tmp" / "chapter-9" / "production-submit-state.json",
                    payload["first_state"],
                )
            )
            self.assertTrue(
                os.path.samefile(
                    second_primary / "tmp" / "chapter-9" / "production-submit-state.json",
                    payload["second_state"],
                )
            )

    def test_bootstrap_orchestrator_passes_its_worktree_to_shared_jobs_boundary(self):
        payload = self._powershell_payload(
            r'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
$primaryRoot = Get-Chapter105PrimaryRepositoryRoot -StartPath (Get-Location)
$worktreeRoot = (Resolve-Path ".").Path
$script:capturedRoot = $null
function Get-Chapter105PrimaryRepositoryRoot { param($StartPath) return $primaryRoot }
function Resolve-Chapter105RepositoryPath {
    param($RepositoryRoot, $Path, $AllowedRelativeRoot)
    return [System.IO.Path]::GetFullPath((Join-Path $RepositoryRoot $Path))
}
function Assert-Chapter105ReportPathWritable { param($Path) }
function Read-Chapter105EnvFile {
    param($Path)
    return [ordered]@{
        FLINK_REST_PORT = "8081"
        API_PORT = "8000"
        FLINK_CHECKPOINT_MAX_AGE_SECONDS = "300"
        CHAPTER9_CHECKPOINT_URI = "s3a://flink-state/checkpoints/chapter-9"
        CHAPTER9_SAVEPOINT_URI = "s3a://flink-state/savepoints/chapter-9"
    }
}
function Get-Chapter105StableMinioDataPath { param($RepositoryRoot) return (Join-Path $RepositoryRoot "infra/compose/minio/data") }
function Assert-Chapter105Preflight { param($RepositoryRoot, $Environment) }
function Invoke-Chapter105Native { param($FilePath, $Arguments, $FailureMessage) return @() }
function Wait-Chapter105ComposeReady { param($ComposePrefix) }
function Invoke-Chapter105FlinkOverview { param($Port) return [pscustomobject]@{ jobs = @() } }
function Invoke-Chapter105JobsStage {
    param($RepositoryRoot, $StateRoot, $Overview, $JobName, $SavepointUri, $SubmitAction)
    $script:capturedRoot = $RepositoryRoot
    throw "stop-after-capture"
}
function Write-Chapter105BootstrapReport { param($Report, $Path) }
$errorMessage = $null
try { Invoke-Chapter105Bootstrap } catch { $errorMessage = $_.Exception.Message }
[ordered]@{
    captured_root = $script:capturedRoot
    primary_root = $primaryRoot
    worktree_root = $worktreeRoot
    error = $errorMessage
} | ConvertTo-Json -Compress
'''
        )

        self.assertTrue(os.path.samefile(ROOT, payload["captured_root"]))
        self.assertTrue(os.path.samefile(ROOT.parent.parent, payload["primary_root"]))
        self.assertNotEqual(
            os.path.normcase(payload["primary_root"]),
            os.path.normcase(payload["worktree_root"]),
        )
        self.assertEqual(
            "Chapter 10.5 bootstrap failed. See the bootstrap report for safe stage status.",
            payload["error"],
        )

    def test_legacy_migration_waits_for_state_lock_and_never_overwrites_result(self):
        with tempfile.TemporaryDirectory() as directory:
            _, _, task4_worktree = self._create_linked_worktrees(directory, "interleaved")
            partial = task4_worktree / "tmp" / "chapter-9" / "cutover-manifest.json.partial"
            started = Path(directory) / "migration-started.txt"
            action_log = Path(directory) / "actions.txt"
            payload = self._powershell_payload(
                rf'''
. "scripts/run_chapter_9_production_cutover.ps1" -FunctionsOnly
$task4Script = (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1").Path
$name = "chapter-9-datastream-quality-production"
$legacyJobId = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
$canonicalJobId = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
$created = "2026-08-25T00:01:00.0000000+00:00"
$completed = "2026-08-25T00:02:00.0000000+00:00"
$partialPath = "{partial}"
$statePath = Get-CutoverProductionSubmitStatePath -RepositoryRoot "{task4_worktree}"
$legacy = [ordered]@{{
    schema_version = 2
    cutover_id = "interleaved-cutover"
    phase = "production_submit"
    created_at = "2026-08-25T00:00:00.0000000+00:00"
    raw_offsets = @("partition:0,offset:42")
    shadow_job_id = "11111111111111111111111111111111"
    savepoint_path = "s3a://flink-state/savepoints/chapter-9/savepoint-fixed"
    production_job_id = $legacyJobId
    doris_job_id = $null
    iceberg_job_id = $null
    mutations = [ordered]@{{
        shadow_stop = [ordered]@{{ status = "result"; intent = @{{}}; result = @{{}} }}
        production_submit = [ordered]@{{
            status = "result"
            intent = [ordered]@{{
                operation = "submit_production_from_savepoint"
                details = [ordered]@{{
                    name = $name
                    savepoint_path = "s3a://flink-state/savepoints/chapter-9/savepoint-fixed"
                }}
                created_at_utc = $created
            }}
            result = [ordered]@{{
                status = "result"
                details = [ordered]@{{ job_id = $legacyJobId }}
                completed_at_utc = $completed
                job_id = $legacyJobId
            }}
        }}
        doris_submit = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
        iceberg_submit = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
        finalization = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
    }}
}}
Write-CutoverStateAtomic -State $legacy -Path $partialPath
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $statePath) | Out-Null
$lock = [System.IO.File]::Open(
    "$statePath.lock",
    [System.IO.FileMode]::OpenOrCreate,
    [System.IO.FileAccess]::ReadWrite,
    [System.IO.FileShare]::None
)
$job = Start-Job -ScriptBlock {{
    param($scriptPath, $repositoryRoot, $manifestPath, $startedPath, $actionPath, $jobName, $runningId)
    . $scriptPath -FunctionsOnly
    [System.IO.File]::WriteAllText($startedPath, "started")
    $manifest = Get-Content -LiteralPath $manifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $running = [pscustomobject]@{{ jobs = @(
        [pscustomobject]@{{ jid = $runningId; name = $jobName; state = "RUNNING" }}
    ) }}
    try {{
        Invoke-CutoverProductionSubmitStage -RepositoryRoot $repositoryRoot -Jobs $running `
            -ExpectedName $jobName -CutoverState $manifest -CutoverStatePath $manifestPath -Action {{
                [System.IO.File]::AppendAllText($actionPath, "mutation`n")
                [pscustomobject]@{{ job_id = "cccccccccccccccccccccccccccccccc" }}
            }} | Out-Null
        "NO_ERROR"
    }} catch {{
        $_.Exception.Message
    }}
}} -ArgumentList $task4Script, "{task4_worktree}", $partialPath, "{started}", "{action_log}", $name, $canonicalJobId

try {{
    for ($attempt = 0; $attempt -lt 100 -and -not (Test-Path -LiteralPath "{started}"); $attempt++) {{
        Start-Sleep -Milliseconds 25
    }}
    if (-not (Test-Path -LiteralPath "{started}")) {{ throw "Migration worker did not start." }}
    for ($attempt = 0; $attempt -lt 40 -and -not (Test-Path -LiteralPath $statePath); $attempt++) {{
        Start-Sleep -Milliseconds 25
    }}
    $stateBeforeRelease = Test-Path -LiteralPath $statePath
    $canonical = [ordered]@{{
        schema_version = 1
        kind = "chapter9_production_submit"
        status = "result"
        intent = [ordered]@{{
            operation = "submit_chapter9_production"
            job_name = $name
            created_at_utc = $created
        }}
        result = [ordered]@{{
            status = "result"
            job_id = $canonicalJobId
            job_name = $name
            completed_at_utc = $completed
        }}
    }}
    Write-CutoverProductionSubmitStateAtomic -State $canonical -Path $statePath
}} finally {{
    $lock.Dispose()
}}
$null = Wait-Job -Job $job -Timeout 10
$childOutcome = @((Receive-Job -Job $job))[-1]
Remove-Job -Job $job -Force
$saved = Get-Content -LiteralPath $statePath -Raw -Encoding UTF8 | ConvertFrom-Json
$legacyAfter = Get-Content -LiteralPath $partialPath -Raw -Encoding UTF8 | ConvertFrom-Json
$actionCount = if (Test-Path -LiteralPath "{action_log}") {{
    @(Get-Content -LiteralPath "{action_log}").Count
}} else {{ 0 }}
[ordered]@{{
    state_existed_before_release = $stateBeforeRelease
    child_outcome = [string]$childOutcome
    canonical_job_id = [string]$saved.result.job_id
    legacy_preserved = $null -ne $legacyAfter.mutations.PSObject.Properties["production_submit"]
    mutations = $actionCount
}} | ConvertTo-Json -Compress
'''
            )

        self.assertFalse(payload["state_existed_before_release"])
        self.assertEqual(
            "Production submission recovery state is unsafe.", payload["child_outcome"]
        )
        self.assertEqual("b" * 32, payload["canonical_job_id"])
        self.assertTrue(payload["legacy_preserved"])
        self.assertLessEqual(payload["mutations"], 1)

    def test_both_entries_migrate_only_an_exact_complete_legacy_result(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for entry in ("bootstrap", "task4"):
                (root / entry).mkdir()
                (root / f"{entry}-state").mkdir()
            payload = self._powershell_payload(
                rf'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
. "scripts/run_chapter_9_production_cutover.ps1" -FunctionsOnly
$name = "chapter-9-datastream-quality-production"
$jobId = "99999999999999999999999999999999"
$created = "2026-08-25T00:01:00.0000000+00:00"
$completed = "2026-08-25T00:02:00.0000000+00:00"
$savepoint = "s3a://flink-state/savepoints/chapter-9/savepoint-fixed"
$script:mutations = 0
$records = @()
$running = [pscustomobject]@{{ jobs = @(
    [pscustomobject]@{{ jid = $jobId; name = $name; state = "RUNNING" }}
) }}
foreach ($entry in @("bootstrap", "task4")) {{
    $repositoryRoot = Join-Path "{root}" $entry
    $stateRoot = Join-Path "{root}" "$entry-state"
    $partialPath = Join-Path $repositoryRoot "tmp/chapter-9/cutover-manifest.json.partial"
    $legacy = [ordered]@{{
        schema_version = 2
        cutover_id = "legacy-$entry"
        phase = "production_submit"
        created_at = "2026-08-25T00:00:00.0000000+00:00"
        raw_offsets = @("partition:0,offset:42")
        shadow_job_id = "11111111111111111111111111111111"
        savepoint_path = $savepoint
        production_job_id = $jobId
        doris_job_id = $null
        iceberg_job_id = $null
        mutations = [ordered]@{{
            shadow_stop = [ordered]@{{ status = "result"; intent = @{{}}; result = @{{}} }}
            production_submit = [ordered]@{{
                status = "result"
                intent = [ordered]@{{
                    operation = "submit_production_from_savepoint"
                    details = [ordered]@{{ name = $name; savepoint_path = $savepoint }}
                    created_at_utc = $created
                }}
                result = [ordered]@{{
                    status = "result"
                    details = [ordered]@{{ job_id = $jobId }}
                    completed_at_utc = $completed
                    job_id = $jobId
                }}
            }}
            doris_submit = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
            iceberg_submit = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
            finalization = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
        }}
    }}
    Write-CutoverStateAtomic -State $legacy -Path $partialPath
    $result = $null
    $errorMessage = $null
    try {{
        if ($entry -ceq "bootstrap") {{
            $result = Invoke-Chapter105JobsStage -RepositoryRoot $repositoryRoot -StateRoot $stateRoot `
                -Overview $running -JobName $name -SavepointUri "s3a://flink-state/savepoints/chapter-9" `
                -SubmitAction {{
                    param($savepointPath)
                    $script:mutations++
                    [pscustomobject]@{{ job_id = "ffffffffffffffffffffffffffffffff" }}
                }}
        }} else {{
            $result = Invoke-CutoverProductionSubmitStage -RepositoryRoot $repositoryRoot -StateRoot $stateRoot `
                -Jobs $running -ExpectedName $name -CutoverState $legacy -CutoverStatePath $partialPath `
                -Action {{
                    $script:mutations++
                    [pscustomobject]@{{ job_id = "ffffffffffffffffffffffffffffffff" }}
                }}
        }}
    }} catch {{ $errorMessage = $_.Exception.Message }}
    $statePath = Join-Path $stateRoot "tmp/chapter-9/production-submit-state.json"
    $canonical = if (Test-Path -LiteralPath $statePath -PathType Leaf) {{
        Get-Content -LiteralPath $statePath -Raw -Encoding UTF8 | ConvertFrom-Json
    }} else {{ $null }}
    $legacyAfter = Get-Content -LiteralPath $partialPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $records += [pscustomobject]@{{
        entry = $entry
        error = $errorMessage
        action = if ($null -ne $result) {{ $result.action }} else {{ $null }}
        job_id = if ($null -ne $result) {{ $result.job_id }} else {{ $null }}
        canonical_status = if ($null -ne $canonical) {{ $canonical.status }} else {{ $null }}
        operation = if ($null -ne $canonical) {{ $canonical.intent.operation }} else {{ $null }}
        detail_names = if ($null -ne $canonical) {{ @($canonical.intent.details.PSObject.Properties.Name) }} else {{ @() }}
        detail_name = if ($null -ne $canonical) {{ $canonical.intent.details.name }} else {{ $null }}
        detail_savepoint = if ($null -ne $canonical) {{ $canonical.intent.details.savepoint_path }} else {{ $null }}
        result_detail_names = if ($null -ne $canonical) {{ @($canonical.result.details.PSObject.Properties.Name) }} else {{ @() }}
        result_detail_job_id = if ($null -ne $canonical) {{ $canonical.result.details.job_id }} else {{ $null }}
        legacy_removed = $null -eq $legacyAfter.mutations.PSObject.Properties["production_submit"]
        lock_exists = Test-Path -LiteralPath "$statePath.lock" -PathType Leaf
    }}
}}
[ordered]@{{ mutations = $script:mutations; records = $records }} | ConvertTo-Json -Depth 8 -Compress
'''
            )

        self.assertEqual(0, payload["mutations"])
        self.assertEqual(2, len(payload["records"]))
        for record in payload["records"]:
            self.assertIsNone(record["error"])
            self.assertEqual("no_op", record["action"])
            self.assertEqual("9" * 32, record["job_id"])
            self.assertEqual("result", record["canonical_status"])
            self.assertEqual("submit_production_from_savepoint", record["operation"])
            self.assertEqual(["name", "savepoint_path"], record["detail_names"])
            self.assertEqual("chapter-9-datastream-quality-production", record["detail_name"])
            self.assertEqual(
                "s3a://flink-state/savepoints/chapter-9/savepoint-fixed",
                record["detail_savepoint"],
            )
            self.assertEqual("job_id", record["result_detail_names"])
            self.assertEqual("9" * 32, record["result_detail_job_id"])
            self.assertTrue(record["legacy_removed"])
            self.assertTrue(record["lock_exists"])

    def test_equivalent_interrupted_migration_ignores_property_order_without_rewrite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository = root / "repository"
            state_root = root / "state"
            repository.mkdir()
            state_root.mkdir()
            payload = self._powershell_payload(
                rf'''
. "scripts/run_chapter_9_production_cutover.ps1" -FunctionsOnly
$name = "chapter-9-datastream-quality-production"
$jobId = "99999999999999999999999999999999"
$savepoint = "s3a://flink-state/savepoints/chapter-9/savepoint-fixed"
$created = "2026-08-25T00:01:00.0000000+00:00"
$completed = "2026-08-25T00:02:00.0000000+00:00"
$partialPath = Join-Path "{repository}" "tmp/chapter-9/cutover-manifest.json.partial"
$statePath = Get-CutoverProductionSubmitStatePath -RepositoryRoot "{repository}" -StateRoot "{state_root}"
$legacy = [ordered]@{{
    schema_version = 2
    cutover_id = "interrupted-migration"
    phase = "production_submit"
    created_at = "2026-08-25T00:00:00.0000000+00:00"
    raw_offsets = @("partition:0,offset:42")
    shadow_job_id = "11111111111111111111111111111111"
    savepoint_path = $savepoint
    production_job_id = $jobId
    doris_job_id = $null
    iceberg_job_id = $null
    mutations = [ordered]@{{
        shadow_stop = [ordered]@{{ status = "result"; intent = @{{}}; result = @{{}} }}
        production_submit = [ordered]@{{
            status = "result"
            intent = [ordered]@{{
                operation = "submit_production_from_savepoint"
                details = [ordered]@{{ name = $name; savepoint_path = $savepoint }}
                created_at_utc = $created
            }}
            result = [ordered]@{{
                status = "result"
                details = [ordered]@{{ job_id = $jobId }}
                completed_at_utc = $completed
                job_id = $jobId
            }}
        }}
        doris_submit = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
        iceberg_submit = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
        finalization = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
    }}
}}
$reorderedCanonical = [ordered]@{{
    result = [ordered]@{{
        job_id = $jobId
        completed_at_utc = $completed
        details = [ordered]@{{ job_id = $jobId }}
        status = "result"
    }}
    intent = [ordered]@{{
        created_at_utc = $created
        details = [ordered]@{{ savepoint_path = $savepoint; name = $name }}
        operation = "submit_production_from_savepoint"
    }}
    status = "result"
    kind = "chapter9_production_submit"
    schema_version = 1
}}
Write-CutoverStateAtomic -State $legacy -Path $partialPath
Write-CutoverProductionSubmitStateAtomic -State $reorderedCanonical -Path $statePath
$canonicalBefore = Get-Content -LiteralPath $statePath -Raw -Encoding UTF8
$script:mutations = 0
$errorMessage = $null
$result = $null
try {{
    $result = Invoke-CutoverProductionSubmitStage -RepositoryRoot "{repository}" -StateRoot "{state_root}" `
        -Jobs ([pscustomobject]@{{ jobs = @(
            [pscustomobject]@{{ jid = $jobId; name = $name; state = "RUNNING" }}
        ) }}) -ExpectedName $name -CutoverState $legacy -CutoverStatePath $partialPath -Action {{
            $script:mutations++
            [pscustomobject]@{{ job_id = "ffffffffffffffffffffffffffffffff" }}
        }}
}} catch {{ $errorMessage = $_.Exception.Message }}
$canonicalAfter = Get-Content -LiteralPath $statePath -Raw -Encoding UTF8
$legacyAfter = Get-Content -LiteralPath $partialPath -Raw -Encoding UTF8 | ConvertFrom-Json
[ordered]@{{
    error = $errorMessage
    action = if ($null -ne $result) {{ $result.action }} else {{ $null }}
    job_id = if ($null -ne $result) {{ $result.job_id }} else {{ $null }}
    canonical_unchanged = $canonicalBefore -ceq $canonicalAfter
    legacy_removed = $null -eq $legacyAfter.mutations.PSObject.Properties["production_submit"]
    mutations = $script:mutations
}} | ConvertTo-Json -Compress
'''
            )

        self.assertIsNone(payload["error"])
        self.assertEqual("no_op", payload["action"])
        self.assertEqual("9" * 32, payload["job_id"])
        self.assertTrue(payload["canonical_unchanged"])
        self.assertTrue(payload["legacy_removed"])
        self.assertEqual(0, payload["mutations"])

    def test_both_entries_preserve_exact_legacy_intent_then_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for entry in ("bootstrap", "task4"):
                (root / entry).mkdir()
                (root / f"{entry}-state").mkdir()
            payload = self._powershell_payload(
                rf'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
. "scripts/run_chapter_9_production_cutover.ps1" -FunctionsOnly
$name = "chapter-9-datastream-quality-production"
$safeError = "Production submission recovery state is unsafe."
$savepoint = "s3a://flink-state/savepoints/chapter-9/savepoint-fixed"
$script:mutations = 0
$records = @()
foreach ($entry in @("bootstrap", "task4")) {{
    $repositoryRoot = Join-Path "{root}" $entry
    $stateRoot = Join-Path "{root}" "$entry-state"
    $partialPath = Join-Path $repositoryRoot "tmp/chapter-9/cutover-manifest.json.partial"
    $legacy = [ordered]@{{
        schema_version = 2
        cutover_id = "legacy-intent-$entry"
        phase = "production_submit"
        created_at = "2026-08-25T00:00:00.0000000+00:00"
        raw_offsets = @("partition:0,offset:42")
        shadow_job_id = "11111111111111111111111111111111"
        savepoint_path = $savepoint
        production_job_id = $null
        doris_job_id = $null
        iceberg_job_id = $null
        mutations = [ordered]@{{
            shadow_stop = [ordered]@{{ status = "result"; intent = @{{}}; result = @{{}} }}
            production_submit = [ordered]@{{
                status = "intent"
                intent = [ordered]@{{
                    operation = "submit_production_from_savepoint"
                    details = [ordered]@{{ name = $name; savepoint_path = $savepoint }}
                    created_at_utc = "2026-08-25T00:01:00.0000000+00:00"
                }}
                result = $null
            }}
            doris_submit = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
            iceberg_submit = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
            finalization = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
        }}
    }}
    Write-CutoverStateAtomic -State $legacy -Path $partialPath
    $errorMessage = $null
    try {{
        if ($entry -ceq "bootstrap") {{
            Invoke-Chapter105JobsStage -RepositoryRoot $repositoryRoot -StateRoot $stateRoot `
                -Overview ([pscustomobject]@{{ jobs = @() }}) -JobName $name `
                -SavepointUri "s3a://flink-state/savepoints/chapter-9" -SubmitAction {{
                    param($savepointPath)
                    $script:mutations++
                    [pscustomobject]@{{ job_id = "ffffffffffffffffffffffffffffffff" }}
                }} | Out-Null
        }} else {{
            Invoke-CutoverProductionSubmitStage -RepositoryRoot $repositoryRoot -StateRoot $stateRoot `
                -Jobs ([pscustomobject]@{{ jobs = @() }}) -ExpectedName $name `
                -CutoverState $legacy -CutoverStatePath $partialPath -Action {{
                    $script:mutations++
                    [pscustomobject]@{{ job_id = "ffffffffffffffffffffffffffffffff" }}
                }} | Out-Null
        }}
    }} catch {{ $errorMessage = $_.Exception.Message }}
    $statePath = Join-Path $stateRoot "tmp/chapter-9/production-submit-state.json"
    $canonical = if (Test-Path -LiteralPath $statePath -PathType Leaf) {{
        Get-Content -LiteralPath $statePath -Raw -Encoding UTF8 | ConvertFrom-Json
    }} else {{ $null }}
    $legacyAfter = Get-Content -LiteralPath $partialPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $records += [pscustomobject]@{{
        entry = $entry
        error = $errorMessage
        canonical_status = if ($null -ne $canonical) {{ $canonical.status }} else {{ $null }}
        operation = if ($null -ne $canonical) {{ $canonical.intent.operation }} else {{ $null }}
        detail_names = if ($null -ne $canonical) {{ @($canonical.intent.details.PSObject.Properties.Name) }} else {{ @() }}
        detail_name = if ($null -ne $canonical) {{ $canonical.intent.details.name }} else {{ $null }}
        detail_savepoint = if ($null -ne $canonical) {{ $canonical.intent.details.savepoint_path }} else {{ $null }}
        legacy_removed = $null -eq $legacyAfter.mutations.PSObject.Properties["production_submit"]
    }}
}}
[ordered]@{{ mutations = $script:mutations; safe_error = $safeError; records = $records }} | ConvertTo-Json -Depth 7 -Compress
'''
            )

        self.assertEqual(0, payload["mutations"])
        for record in payload["records"]:
            self.assertEqual(payload["safe_error"], record["error"])
            self.assertEqual("intent", record["canonical_status"])
            self.assertEqual("submit_production_from_savepoint", record["operation"])
            self.assertEqual(["name", "savepoint_path"], record["detail_names"])
            self.assertEqual("chapter-9-datastream-quality-production", record["detail_name"])
            self.assertEqual(
                "s3a://flink-state/savepoints/chapter-9/savepoint-fixed",
                record["detail_savepoint"],
            )
            self.assertTrue(record["legacy_removed"])

    def test_legacy_schema_matrix_rejects_and_preserves_every_unsafe_tuple(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = self._powershell_payload(
                rf'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
. "scripts/run_chapter_9_production_cutover.ps1" -FunctionsOnly
$name = "chapter-9-datastream-quality-production"
$safeError = "Production submission recovery state is unsafe."
$legacyJobId = "99999999999999999999999999999999"
$canonicalJobId = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
$savepoint = "s3a://flink-state/savepoints/chapter-9/savepoint-fixed"
$created = "2026-08-25T00:01:00.0000000+00:00"
$completed = "2026-08-25T00:02:00.0000000+00:00"
$script:mutations = 0
$records = @()
$cases = @(
    "failed", "malformed_json", "malformed_record", "tuple_extra", "tuple_missing",
    "property_case", "status_case", "operation_case", "details_extra", "details_missing",
    "result_extra", "result_details_extra", "savepoint_conflict", "projection_conflict",
    "time_inversion", "canonical_conflict"
)

function New-TestLegacyManifest {{
    return ([ordered]@{{
        schema_version = 2
        cutover_id = "legacy-matrix"
        phase = "production_submit"
        created_at = "2026-08-25T00:00:00.0000000+00:00"
        raw_offsets = @("partition:0,offset:42")
        shadow_job_id = "11111111111111111111111111111111"
        savepoint_path = $savepoint
        production_job_id = $legacyJobId
        doris_job_id = $null
        iceberg_job_id = $null
        mutations = [ordered]@{{
            shadow_stop = [ordered]@{{ status = "result"; intent = @{{}}; result = @{{}} }}
            production_submit = [ordered]@{{
                status = "result"
                intent = [ordered]@{{
                    operation = "submit_production_from_savepoint"
                    details = [ordered]@{{ name = $name; savepoint_path = $savepoint }}
                    created_at_utc = $created
                }}
                result = [ordered]@{{
                    status = "result"
                    details = [ordered]@{{ job_id = $legacyJobId }}
                    completed_at_utc = $completed
                    job_id = $legacyJobId
                }}
            }}
            doris_submit = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
            iceberg_submit = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
            finalization = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
        }}
    }} | ConvertTo-Json -Depth 12 | ConvertFrom-Json)
}}

foreach ($caseName in $cases) {{
    foreach ($entry in @("bootstrap", "task4")) {{
        $repositoryRoot = Join-Path "{root}" "$caseName-$entry-code"
        $stateRoot = Join-Path "{root}" "$caseName-$entry-state"
        New-Item -ItemType Directory -Force -Path $repositoryRoot, $stateRoot | Out-Null
        $partialPath = Join-Path $repositoryRoot "tmp/chapter-9/cutover-manifest.json.partial"
        $statePath = Join-Path $stateRoot "tmp/chapter-9/production-submit-state.json"
        $legacy = New-TestLegacyManifest
        switch -CaseSensitive ($caseName) {{
            "failed" {{
                $legacy.production_job_id = $null
                $legacy.mutations.production_submit.status = "failed"
                $legacy.mutations.production_submit.result = [pscustomobject][ordered]@{{
                    status = "failed"
                    details = [pscustomobject][ordered]@{{ error = "PASSWORD=legacy-secret" }}
                    completed_at_utc = $completed
                }}
            }}
            "malformed_record" {{ $legacy.mutations.production_submit.intent = "malformed" }}
            "tuple_extra" {{
                $legacy.mutations.production_submit | Add-Member -NotePropertyName unexpected -NotePropertyValue "SECRET=extra"
            }}
            "tuple_missing" {{ $legacy.mutations.production_submit.PSObject.Properties.Remove("result") }}
            "property_case" {{
                $value = $legacy.mutations.production_submit.status
                $legacy.mutations.production_submit.PSObject.Properties.Remove("status")
                $legacy.mutations.production_submit | Add-Member -NotePropertyName Status -NotePropertyValue $value
            }}
            "status_case" {{ $legacy.mutations.production_submit.status = "Result" }}
            "operation_case" {{
                $legacy.mutations.production_submit.intent.operation = "Submit_Production_From_Savepoint"
            }}
            "details_extra" {{
                $legacy.mutations.production_submit.intent.details | Add-Member -NotePropertyName extra -NotePropertyValue "API_KEY=extra"
            }}
            "details_missing" {{
                $legacy.mutations.production_submit.intent.details.PSObject.Properties.Remove("savepoint_path")
            }}
            "result_extra" {{
                $legacy.mutations.production_submit.result | Add-Member -NotePropertyName extra -NotePropertyValue "PASSWORD=extra"
            }}
            "result_details_extra" {{
                $legacy.mutations.production_submit.result.details | Add-Member -NotePropertyName extra -NotePropertyValue "SECRET=extra"
            }}
            "savepoint_conflict" {{
                $legacy.mutations.production_submit.intent.details.savepoint_path = "s3a://flink-state/savepoints/chapter-9/savepoint-other"
            }}
            "projection_conflict" {{ $legacy.production_job_id = "88888888888888888888888888888888" }}
            "time_inversion" {{
                $legacy.mutations.production_submit.intent.created_at_utc = "2026-08-25T00:03:00.0000000+00:00"
            }}
        }}
        if ($caseName -ceq "malformed_json") {{
            New-Item -ItemType Directory -Force -Path (Split-Path -Parent $partialPath) | Out-Null
            [System.IO.File]::WriteAllText($partialPath, '{{"mutations":', (New-Object System.Text.UTF8Encoding($false)))
            $legacyArgument = $null
        }} else {{
            Write-CutoverStateAtomic -State $legacy -Path $partialPath
            $legacyArgument = $legacy
        }}
        if ($caseName -ceq "canonical_conflict") {{
            $canonical = [ordered]@{{
                schema_version = 1
                kind = "chapter9_production_submit"
                status = "result"
                intent = [ordered]@{{
                    operation = "submit_chapter9_production"
                    job_name = $name
                    created_at_utc = $created
                }}
                result = [ordered]@{{
                    status = "result"
                    job_id = $canonicalJobId
                    job_name = $name
                    completed_at_utc = $completed
                }}
            }}
            Write-CutoverProductionSubmitStateAtomic -State $canonical -Path $statePath
        }}
        $legacyBefore = Get-Content -LiteralPath $partialPath -Raw -Encoding UTF8
        $canonicalBefore = if (Test-Path -LiteralPath $statePath -PathType Leaf) {{
            Get-Content -LiteralPath $statePath -Raw -Encoding UTF8
        }} else {{ $null }}
        $runningId = if ($caseName -ceq "canonical_conflict") {{ $canonicalJobId }} else {{ $legacyJobId }}
        $running = [pscustomobject]@{{ jobs = @(
            [pscustomobject]@{{ jid = $runningId; name = $name; state = "RUNNING" }}
        ) }}
        $errorMessage = $null
        try {{
            if ($entry -ceq "bootstrap") {{
                Invoke-Chapter105JobsStage -RepositoryRoot $repositoryRoot -StateRoot $stateRoot `
                    -Overview $running -JobName $name -SavepointUri "s3a://flink-state/savepoints/chapter-9" `
                    -SubmitAction {{
                        param($savepointPath)
                        $script:mutations++
                        [pscustomobject]@{{ job_id = "ffffffffffffffffffffffffffffffff" }}
                    }} | Out-Null
            }} else {{
                $stageArguments = @{{
                    RepositoryRoot = $repositoryRoot
                    StateRoot = $stateRoot
                    Jobs = $running
                    ExpectedName = $name
                    CutoverStatePath = $partialPath
                    Action = {{
                        $script:mutations++
                        [pscustomobject]@{{ job_id = "ffffffffffffffffffffffffffffffff" }}
                    }}
                }}
                if ($null -ne $legacyArgument) {{ $stageArguments["CutoverState"] = $legacyArgument }}
                Invoke-CutoverProductionSubmitStage @stageArguments | Out-Null
            }}
        }} catch {{ $errorMessage = $_.Exception.Message }}
        $legacyAfter = Get-Content -LiteralPath $partialPath -Raw -Encoding UTF8
        $canonicalAfter = if (Test-Path -LiteralPath $statePath -PathType Leaf) {{
            Get-Content -LiteralPath $statePath -Raw -Encoding UTF8
        }} else {{ $null }}
        $records += [pscustomobject]@{{
            case_name = $caseName
            entry = $entry
            error = $errorMessage
            scrubbed = [string]$errorMessage -notmatch "PASSWORD|SECRET|API_KEY|legacy-secret"
            legacy_unchanged = $legacyBefore -ceq $legacyAfter
            canonical_unchanged = $canonicalBefore -ceq $canonicalAfter
            canonical_absent = $null -eq $canonicalAfter
            lock_exists = Test-Path -LiteralPath "$statePath.lock" -PathType Leaf
        }}
    }}
}}
[ordered]@{{
    safe_error = $safeError
    mutations = $script:mutations
    records = $records
}} | ConvertTo-Json -Depth 6 -Compress
'''
            )

        self.assertEqual(0, payload["mutations"])
        self.assertEqual(32, len(payload["records"]))
        for record in payload["records"]:
            self.assertEqual(payload["safe_error"], record["error"], record)
            self.assertTrue(record["scrubbed"], record)
            self.assertTrue(record["legacy_unchanged"], record)
            self.assertTrue(record["canonical_unchanged"], record)
            if record["case_name"] == "canonical_conflict":
                self.assertFalse(record["canonical_absent"], record)
            else:
                self.assertTrue(record["canonical_absent"], record)
            self.assertTrue(record["lock_exists"], record)

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

    def test_job_submission_restores_only_when_validated_cutover_partial_exists(self):
        with tempfile.TemporaryDirectory() as directory:
            recovery_root = Path(directory)
            fresh_root = recovery_root / "fresh"
            (recovery_root / "tmp" / "chapter-9").mkdir(parents=True)
            (recovery_root / "jobs" / "datastream-quality").mkdir(parents=True)
            (fresh_root / "tmp" / "chapter-9").mkdir(parents=True)
            (fresh_root / "jobs" / "datastream-quality").mkdir(parents=True)
            (recovery_root / "tmp" / "chapter-9" / "cutover-manifest.json.partial").write_text(
                json.dumps(
                    {
                        "schema_version": 2,
                        "cutover_id": "cutover-valid",
                        "created_at": "2026-08-25T00:00:00Z",
                        "raw_offsets": ["partition:0,offset:42"],
                        "shadow_job_id": "1" * 32,
                        "savepoint_path": "s3a://flink-state/savepoints/chapter-9/savepoint-fixed",
                        "mutations": {
                            "shadow_stop": {"status": "result", "intent": {}, "result": {}},
                            "doris_submit": {"status": "not_started", "intent": None, "result": None},
                            "iceberg_submit": {"status": "not_started", "intent": None, "result": None},
                            "finalization": {"status": "not_started", "intent": None, "result": None},
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
$fresh = Get-Chapter105Task4RecoveryPlan -RepositoryRoot "{fresh_root}" -SavepointUri "s3a://flink-state/savepoints/chapter-9"
$freshId = Invoke-Chapter105JobSubmission -RepositoryRoot "{fresh_root}" -ComposePrefix @("compose") -CheckpointUri "s3a://flink-state/checkpoints/chapter-9" -SkipBuild
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

    def test_bootstrap_rejects_a_production_only_cutover_partial_without_shadow_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            partial = root / "tmp" / "chapter-9" / "cutover-manifest.json.partial"
            partial.parent.mkdir(parents=True)
            partial.write_text(
                json.dumps(
                    {
                        "schema_version": 2,
                        "cutover_id": "chapter-10-5-bootstrap",
                        "savepoint_path": None,
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
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
. "scripts/run_chapter_9_production_cutover.ps1" -FunctionsOnly
$recoveryError = ""
try {{
    Get-Chapter105Task4RecoveryPlan -RepositoryRoot "{root}" `
        -SavepointUri "s3a://flink-state/savepoints/chapter-9" | Out-Null
}} catch {{ $recoveryError = $_.Exception.Message }}
[ordered]@{{ rejected = $recoveryError -ceq "Task 4 recovery state is invalid." }} | ConvertTo-Json -Compress
'''
            )
        self.assertEqual({"rejected": True}, payload)

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

    def test_bootstrap_and_task4_real_production_flows_submit_once_in_both_orders(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = self._powershell_payload(
                rf'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
. "scripts/run_chapter_9_production_cutover.ps1" -FunctionsOnly
$script:mutations = 0
$name = "chapter-9-datastream-quality-production"
$emptyJobs = [pscustomobject]@{{ jobs = @() }}

$bootstrapFirstRoot = "{root / 'bootstrap-first'}"
New-Item -ItemType Directory -Force -Path $bootstrapFirstRoot | Out-Null
$bootstrapResult = Invoke-Chapter105JobsStage -RepositoryRoot $bootstrapFirstRoot -StateRoot $bootstrapFirstRoot `
    -Overview $emptyJobs -JobName $name -SavepointUri "s3a://flink-state/savepoints/chapter-9" -SubmitAction {{
        param($savepointPath)
        $script:mutations++
        [pscustomobject]@{{ job_id = "33333333333333333333333333333333" }}
    }}
$afterBootstrap = $script:mutations
$bootstrapRunning = [pscustomobject]@{{ jobs = @(
    [pscustomobject]@{{ jid = $bootstrapResult.job_id; name = $name; state = "RUNNING" }}
) }}
$task4AfterBootstrap = Invoke-CutoverProductionSubmitStage -RepositoryRoot $bootstrapFirstRoot -StateRoot $bootstrapFirstRoot `
    -Jobs $bootstrapRunning -ExpectedName $name -Action {{
        $script:mutations++
        [pscustomobject]@{{ job_id = "ffffffffffffffffffffffffffffffff" }}
    }}
$afterTask4 = $script:mutations
$bootstrapFirstPath = Get-CutoverProductionSubmitStatePath -RepositoryRoot $bootstrapFirstRoot -StateRoot $bootstrapFirstRoot
$bootstrapSaved = Get-Content -Raw $bootstrapFirstPath | ConvertFrom-Json

$task4FirstRoot = "{root / 'task4-first'}"
New-Item -ItemType Directory -Force -Path $task4FirstRoot | Out-Null
$task4Result = Invoke-CutoverProductionSubmitStage -RepositoryRoot $task4FirstRoot -StateRoot $task4FirstRoot `
    -Jobs $emptyJobs -ExpectedName $name -Action {{
        $script:mutations++
        [pscustomobject]@{{ job_id = "44444444444444444444444444444444" }}
    }}
$afterTask4First = $script:mutations
$task4Running = [pscustomobject]@{{ jobs = @(
    [pscustomobject]@{{ jid = $task4Result.job_id; name = $name; state = "RUNNING" }}
) }}
$bootstrapAfterTask4 = Invoke-Chapter105JobsStage -RepositoryRoot $task4FirstRoot -StateRoot $task4FirstRoot `
    -Overview $task4Running -JobName $name -SavepointUri "s3a://flink-state/savepoints/chapter-9" -SubmitAction {{
        param($savepointPath)
        $script:mutations++
        [pscustomobject]@{{ job_id = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" }}
    }}
$afterBootstrapSecond = $script:mutations
$task4FirstPath = Get-CutoverProductionSubmitStatePath -RepositoryRoot $task4FirstRoot -StateRoot $task4FirstRoot
$task4Saved = Get-Content -Raw $task4FirstPath | ConvertFrom-Json

[ordered]@{{
    bootstrap_then_task4_mutations = $afterTask4
    task4_then_bootstrap_mutations = $afterBootstrapSecond - $afterTask4
    total_mutations = $script:mutations
    bootstrap_first_action = $bootstrapResult.action
    task4_after_bootstrap_action = $task4AfterBootstrap.action
    task4_first_action = $task4Result.action
    bootstrap_after_task4_action = $bootstrapAfterTask4.action
    bootstrap_job_id = $task4AfterBootstrap.job_id
    task4_job_id = $bootstrapAfterTask4.job_id
    bootstrap_status = $bootstrapSaved.status
    task4_status = $task4Saved.status
    bootstrap_atomic_job_id = $bootstrapSaved.result.job_id
    task4_atomic_job_id = $task4Saved.result.job_id
    bootstrap_kind = $bootstrapSaved.kind
    task4_kind = $task4Saved.kind
    bootstrap_partial_absent = -not (Test-Path (Join-Path $bootstrapFirstRoot "tmp/chapter-9/cutover-manifest.json.partial"))
    task4_partial_absent = -not (Test-Path (Join-Path $task4FirstRoot "tmp/chapter-9/cutover-manifest.json.partial"))
}} | ConvertTo-Json -Compress
'''
            )
        self.assertEqual(1, payload["bootstrap_then_task4_mutations"])
        self.assertEqual(1, payload["task4_then_bootstrap_mutations"])
        self.assertEqual(2, payload["total_mutations"])
        self.assertEqual("submitted", payload["bootstrap_first_action"])
        self.assertEqual("no_op", payload["task4_after_bootstrap_action"])
        self.assertEqual("submitted", payload["task4_first_action"])
        self.assertEqual("no_op", payload["bootstrap_after_task4_action"])
        self.assertEqual("3" * 32, payload["bootstrap_job_id"])
        self.assertEqual("4" * 32, payload["task4_job_id"])
        self.assertEqual("result", payload["bootstrap_status"])
        self.assertEqual("result", payload["task4_status"])
        self.assertEqual("3" * 32, payload["bootstrap_atomic_job_id"])
        self.assertEqual("4" * 32, payload["task4_atomic_job_id"])
        self.assertEqual("chapter9_production_submit", payload["bootstrap_kind"])
        self.assertEqual("chapter9_production_submit", payload["task4_kind"])
        self.assertTrue(payload["bootstrap_partial_absent"])
        self.assertTrue(payload["task4_partial_absent"])

    def test_real_production_flows_require_a_complete_exact_result_before_no_op(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = self._powershell_payload(
                rf'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
. "scripts/run_chapter_9_production_cutover.ps1" -FunctionsOnly
$name = "chapter-9-datastream-quality-production"
$jobId = "55555555555555555555555555555555"
$safeError = "Production submission recovery state is unsafe."
$script:mutations = 0
$errors = @()
$created = "2026-08-25T00:00:00.0000000+00:00"
$completed = "2026-08-25T00:01:00.0000000+00:00"
$validIntent = [ordered]@{{
    operation = "submit_chapter9_production"
    job_name = $name
    created_at_utc = $created
}}
$running = [pscustomobject]@{{ jobs = @(
    [pscustomobject]@{{ jid = $jobId; name = $name; state = "RUNNING" }}
) }}

$completeRoot = "{root / 'complete'}"
New-Item -ItemType Directory -Force -Path $completeRoot | Out-Null
$completePath = Get-CutoverProductionSubmitStatePath -RepositoryRoot $completeRoot -StateRoot $completeRoot
$completeState = [ordered]@{{
    schema_version = 1
    kind = "chapter9_production_submit"
    status = "result"
    intent = $validIntent
    result = [ordered]@{{ status = "result"; job_id = $jobId; job_name = $name; completed_at_utc = $completed }}
}}
Write-CutoverProductionSubmitStateAtomic -State $completeState -Path $completePath
$bootstrapComplete = Invoke-Chapter105JobsStage -RepositoryRoot $completeRoot -StateRoot $completeRoot -Overview $running `
    -JobName $name -SavepointUri "s3a://flink-state/savepoints/chapter-9" -SubmitAction {{
        param($savepointPath)
        $script:mutations++
        [pscustomobject]@{{ job_id = "77777777777777777777777777777777" }}
    }}
$task4Complete = Invoke-CutoverProductionSubmitStage -RepositoryRoot $completeRoot -StateRoot $completeRoot `
    -Jobs $running -ExpectedName $name -Action {{
        $script:mutations++
        [pscustomobject]@{{ job_id = "77777777777777777777777777777777" }}
    }}

$cases = @(
    [pscustomobject]@{{ name = "intent"; state = [ordered]@{{ schema_version = 1; kind = "chapter9_production_submit"; status = "intent"; intent = $validIntent; result = $null }}; jobs = $running }},
    [pscustomobject]@{{ name = "failed"; state = [ordered]@{{ schema_version = 1; kind = "chapter9_production_submit"; status = "failed"; intent = $validIntent; result = [ordered]@{{ status = "failed"; job_name = $name; completed_at_utc = $completed }} }}; jobs = $running }},
    [pscustomobject]@{{ name = "missing_job_id"; state = [ordered]@{{ schema_version = 1; kind = "chapter9_production_submit"; status = "result"; intent = $validIntent; result = [ordered]@{{ status = "result"; job_name = $name; completed_at_utc = $completed }} }}; jobs = $running }},
    [pscustomobject]@{{ name = "wrong_result_name"; state = [ordered]@{{ schema_version = 1; kind = "chapter9_production_submit"; status = "result"; intent = $validIntent; result = [ordered]@{{ status = "result"; job_id = $jobId; job_name = "other"; completed_at_utc = $completed }} }}; jobs = $running }},
    [pscustomobject]@{{ name = "not_started_with_job"; state = [ordered]@{{ schema_version = 1; kind = "chapter9_production_submit"; status = "not_started"; intent = $null; result = $null }}; jobs = $running }}
)
foreach ($case in $cases) {{
    foreach ($entry in @("bootstrap", "task4")) {{
        $caseRoot = Join-Path "{root}" "$($case.name)-$entry"
        New-Item -ItemType Directory -Force -Path $caseRoot | Out-Null
        $path = Get-CutoverProductionSubmitStatePath -RepositoryRoot $caseRoot -StateRoot $caseRoot
        Write-CutoverProductionSubmitStateAtomic -State $case.state -Path $path
        try {{
            if ($entry -eq "bootstrap") {{
                Invoke-Chapter105JobsStage -RepositoryRoot $caseRoot -StateRoot $caseRoot -Overview $case.jobs `
                    -JobName $name -SavepointUri "s3a://flink-state/savepoints/chapter-9" -SubmitAction {{
                        param($savepointPath)
                        $script:mutations++
                        [pscustomobject]@{{ job_id = "77777777777777777777777777777777" }}
                    }} | Out-Null
            }} else {{
                Invoke-CutoverProductionSubmitStage -RepositoryRoot $caseRoot -StateRoot $caseRoot `
                    -Jobs $case.jobs -ExpectedName $name -Action {{
                        $script:mutations++
                        [pscustomobject]@{{ job_id = "77777777777777777777777777777777" }}
                    }} | Out-Null
            }}
            $errors += "NO_ERROR"
        }} catch {{ $errors += $_.Exception.Message }}
    }}
}}

[ordered]@{{
    complete_bootstrap_action = $bootstrapComplete.action
    complete_task4_action = $task4Complete.action
    complete_job_id = $task4Complete.job_id
    rejected = @($errors | Where-Object {{ $_ -ceq $safeError }}).Count
    error_count = $errors.Count
    mutations = $script:mutations
}} | ConvertTo-Json -Compress
'''
            )
        self.assertEqual("no_op", payload["complete_bootstrap_action"])
        self.assertEqual("no_op", payload["complete_task4_action"])
        self.assertEqual("5" * 32, payload["complete_job_id"])
        self.assertEqual(10, payload["rejected"])
        self.assertEqual(10, payload["error_count"])
        self.assertEqual(0, payload["mutations"])

    def test_unknown_submit_outcome_stays_intent_and_never_retries_across_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = self._powershell_payload(
                rf'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
. "scripts/run_chapter_9_production_cutover.ps1" -FunctionsOnly
$name = "chapter-9-datastream-quality-production"
$safeError = "Production submission recovery state is unsafe."
$secret = "PASSWORD=unknown-outcome"
$script:mutations = 0
$errors = @()
$statuses = @()
$emptyJobs = [pscustomobject]@{{ jobs = @() }}
$running = [pscustomobject]@{{ jobs = @(
    [pscustomobject]@{{ jid = "88888888888888888888888888888888"; name = $name; state = "RUNNING" }}
) }}

foreach ($first in @("bootstrap", "task4")) {{
    $caseRoot = Join-Path "{root}" $first
    New-Item -ItemType Directory -Force -Path $caseRoot | Out-Null
    try {{
        if ($first -eq "bootstrap") {{
            Invoke-Chapter105JobsStage -RepositoryRoot $caseRoot -StateRoot $caseRoot -Overview $emptyJobs `
                -JobName $name -SavepointUri "s3a://flink-state/savepoints/chapter-9" -SubmitAction {{
                    param($savepointPath)
                    $script:mutations++
                    throw $secret
                }} | Out-Null
        }} else {{
            Invoke-CutoverProductionSubmitStage -RepositoryRoot $caseRoot -StateRoot $caseRoot `
                -Jobs $emptyJobs -ExpectedName $name -Action {{
                    $script:mutations++
                    throw $secret
                }} | Out-Null
        }}
    }} catch {{ $errors += $_.Exception.Message }}

    try {{
        if ($first -eq "bootstrap") {{
            Invoke-CutoverProductionSubmitStage -RepositoryRoot $caseRoot -StateRoot $caseRoot `
                -Jobs $running -ExpectedName $name -Action {{ $script:mutations++; throw "retried" }} | Out-Null
        }} else {{
            Invoke-Chapter105JobsStage -RepositoryRoot $caseRoot -StateRoot $caseRoot -Overview $running `
                -JobName $name -SavepointUri "s3a://flink-state/savepoints/chapter-9" `
                -SubmitAction {{ param($savepointPath); $script:mutations++; throw "retried" }} | Out-Null
        }}
    }} catch {{ $errors += $_.Exception.Message }}

    $statePath = Get-CutoverProductionSubmitStatePath -RepositoryRoot $caseRoot -StateRoot $caseRoot
    $saved = Get-Content -Raw $statePath | ConvertFrom-Json
    $statuses += [pscustomobject]@{{
        status = $saved.status
        has_intent = $null -ne $saved.intent
        has_result = $null -ne $saved.result
        partial_absent = -not (Test-Path (Join-Path $caseRoot "tmp/chapter-9/cutover-manifest.json.partial"))
    }}
}}

[ordered]@{{
    error_count = $errors.Count
    fixed_errors = @($errors | Where-Object {{ $_ -ceq $safeError }}).Count
    scrubbed = @($errors | Where-Object {{ $_ -match "PASSWORD|unknown-outcome|retried" }}).Count -eq 0
    mutations = $script:mutations
    statuses = $statuses
}} | ConvertTo-Json -Depth 5 -Compress
'''
            )
        self.assertEqual(4, payload["error_count"])
        self.assertEqual(4, payload["fixed_errors"])
        self.assertTrue(payload["scrubbed"])
        self.assertEqual(2, payload["mutations"])
        for state in payload["statuses"]:
            self.assertEqual("intent", state["status"])
            self.assertTrue(state["has_intent"])
            self.assertFalse(state["has_result"])
            self.assertTrue(state["partial_absent"])

    def test_task4_real_production_stage_migrates_one_complete_legacy_tuple(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            partial = root / "tmp" / "chapter-9" / "cutover-manifest.json.partial"
            payload = self._powershell_payload(
                rf'''
. "scripts/run_chapter_9_production_cutover.ps1" -FunctionsOnly
$name = "chapter-9-datastream-quality-production"
$jobId = "99999999999999999999999999999999"
$partialPath = "{partial}"
$manifest = [ordered]@{{
    schema_version = 2
    cutover_id = "legacy-cutover"
    phase = "production_submit"
    created_at = "2026-08-25T00:00:00.0000000+00:00"
    raw_offsets = @("partition:0,offset:42")
    shadow_job_id = "11111111111111111111111111111111"
    savepoint_path = "s3a://flink-state/savepoints/chapter-9/savepoint-fixed"
    production_job_id = $jobId
    doris_job_id = $null
    iceberg_job_id = $null
    mutations = [ordered]@{{
        shadow_stop = [ordered]@{{ status = "result"; intent = @{{}}; result = @{{}} }}
        production_submit = [ordered]@{{
            status = "result"
            intent = [ordered]@{{ operation = "submit_production_from_savepoint"; details = [ordered]@{{ name = $name; savepoint_path = "s3a://flink-state/savepoints/chapter-9/savepoint-fixed" }}; created_at_utc = "2026-08-25T00:01:00.0000000+00:00" }}
            result = [ordered]@{{ status = "result"; job_id = $jobId; details = [ordered]@{{ job_id = $jobId }}; completed_at_utc = "2026-08-25T00:02:00.0000000+00:00" }}
        }}
        doris_submit = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
        iceberg_submit = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
        finalization = [ordered]@{{ status = "not_started"; intent = $null; result = $null }}
    }}
}}
Write-CutoverStateAtomic -State $manifest -Path $partialPath
$script:mutations = 0
$running = [pscustomobject]@{{ jobs = @(
    [pscustomobject]@{{ jid = $jobId; name = $name; state = "RUNNING" }}
) }}
$result = Invoke-CutoverProductionSubmitStage -RepositoryRoot "{root}" -StateRoot "{root}" -Jobs $running `
    -ExpectedName $name -CutoverState $manifest -CutoverStatePath $partialPath -Action {{
        $script:mutations++
        [pscustomobject]@{{ job_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }}
    }}
$statePath = Get-CutoverProductionSubmitStatePath -RepositoryRoot "{root}" -StateRoot "{root}"
$canonical = Get-Content -Raw $statePath | ConvertFrom-Json
$migratedPartial = Get-Content -Raw $partialPath | ConvertFrom-Json
[ordered]@{{
    action = $result.action
    job_id = $result.job_id
    mutations = $script:mutations
    canonical_status = $canonical.status
    canonical_job_id = $canonical.result.job_id
    legacy_removed = $null -eq $migratedPartial.mutations.PSObject.Properties["production_submit"]
    projection_preserved = $migratedPartial.production_job_id
}} | ConvertTo-Json -Compress
'''
            )
        self.assertEqual("no_op", payload["action"])
        self.assertEqual("9" * 32, payload["job_id"])
        self.assertEqual(0, payload["mutations"])
        self.assertEqual("result", payload["canonical_status"])
        self.assertEqual("9" * 32, payload["canonical_job_id"])
        self.assertTrue(payload["legacy_removed"])
        self.assertEqual("9" * 32, payload["projection_preserved"])

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
    schema_version = 2
    cutover_id = "cutover-custom"
    created_at = "2026-08-25T00:00:00Z"
    raw_offsets = @("partition:0,offset:42")
    shadow_job_id = "11111111111111111111111111111111"
    savepoint_path = "$savepoint/savepoint-fixed"
    mutations = [ordered]@{{ shadow_stop = [ordered]@{{ status = "result"; intent = @{{}}; result = @{{}} }} }}
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
            repository_root = Path(directory)
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
$script:submissions = 0
$result = Invoke-Chapter105JobsStage -RepositoryRoot "{repository_root}" -StateRoot "{repository_root}" `
    -Overview ([pscustomobject]@{{ jobs = @() }}) -JobName "chapter-9-datastream-quality-production" `
    -SavepointUri $savepoint -SubmitAction {{
    param($savepointPath)
    $script:submissions++
    [pscustomobject]@{{ job_id = "33333333333333333333333333333333" }}
}}
$running = [pscustomobject]@{{ jobs = @([pscustomobject]@{{
    jid = $result.job_id; name = "chapter-9-datastream-quality-production"; state = "RUNNING"
}}) }}
$replay = Invoke-Chapter105JobsStage -RepositoryRoot "{repository_root}" -StateRoot "{repository_root}" -Overview $running `
    -JobName "chapter-9-datastream-quality-production" -SavepointUri $savepoint -SubmitAction {{
    param($savepointPath)
    $script:submissions++
    throw "must not replay"
}}
$statePath = Get-CutoverProductionSubmitStatePath -RepositoryRoot "{repository_root}" -StateRoot "{repository_root}"
$saved = Get-Content -Raw $statePath | ConvertFrom-Json
[ordered]@{{
    checkpoint = $checkpoint
    savepoint = $savepoint
    job_id = $result.job_id
    status = $saved.status
    operation = $saved.intent.operation
    persisted_job_id = $saved.result.job_id
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
                "operation": "submit_chapter9_production",
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
