import json
from pathlib import Path
import shutil
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent.parent


class Chapter9PhaseBArtifactsTest(unittest.TestCase):
    def _run_powershell(self, command: str) -> subprocess.CompletedProcess[str]:
        executable = shutil.which("pwsh") or shutil.which("powershell")
        if executable is None:
            self.skipTest("PowerShell is required for Chapter 9 behavior coverage")
        return subprocess.run(
            [executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", command],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
            check=False,
        )

    def test_env_enables_four_slots_and_production_namespace(self):
        text = (ROOT / "infra/.env.example").read_text(encoding="utf-8")
        for marker in (
            "FLINK_TASKMANAGER_SLOTS=4",
            "CHAPTER9_CLEAN_TOPIC=user_behavior_clean",
            "CHAPTER9_PRODUCTION_CONSUMER_GROUP=chapter9-quality-production",
            "CHAPTER9_PRODUCTION_TRANSACTION_PREFIX=chapter9-production",
        ):
            self.assertIn(marker, text)

    def test_clean_sources_use_distinct_consumer_groups(self):
        doris = (ROOT / "jobs/sql/13_source_user_behavior_clean_doris.sql").read_text(encoding="utf-8")
        iceberg = (ROOT / "jobs/sql/14_source_user_behavior_clean_iceberg.sql").read_text(encoding="utf-8")
        self.assertIn("'topic' = 'user_behavior_clean'", doris)
        self.assertIn("'topic' = 'user_behavior_clean'", iceberg)
        self.assertIn("'properties.group.id' = 'chapter9-doris-clean-v1'", doris)
        self.assertIn("'properties.group.id' = 'chapter9-iceberg-clean-v1'", iceberg)
        self.assertNotEqual(doris, iceberg)

    def test_rollback_source_requires_recorded_offsets(self):
        text = (ROOT / "jobs/sql/15_source_user_behavior_raw_rollback.sql.template").read_text(encoding="utf-8")
        self.assertIn("'topic' = 'user_behavior_events'", text)
        self.assertIn("'scan.startup.mode' = 'specific-offsets'", text)
        self.assertIn("__ROLLBACK_GROUP_ID__", text)
        self.assertIn("__SPECIFIC_OFFSETS__", text)

    def test_rollback_is_manifest_driven_and_non_destructive(self):
        text = (ROOT / "scripts/rollback_chapter_9_production.ps1").read_text(encoding="utf-8")
        for marker in (
            "[switch]$TrafficPaused",
            "[switch]$DryRun",
            "cutover-manifest.json",
            "15_source_user_behavior_raw_rollback.sql.template",
            "__SPECIFIC_OFFSETS__",
            "chapter9-doris-raw-rollback",
            "chapter9-iceberg-raw-rollback",
            "--savepointPath",
            "[switch]$Resume",
            "rollback-progress.json",
            "Invoke-RollbackMutation",
            "finalization",
        ):
            self.assertIn(marker, text)
        for forbidden in ("kafka-topics --delete", "docker compose down", "DROP TABLE", "Remove-Item"):
            self.assertNotIn(forbidden, text)
        self.assertLess(text.index("if ($DryRun) {"), text.index("$rollbackProgress = $null"))

    def test_rollback_waiters_fail_closed_and_preserve_savepoint_evidence(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/rollback_chapter_9_production.ps1") -FunctionsOnly
$savepoint = Get-RollbackSavepointPath -Lines @(
    "Savepoint completed. Path: file:/workspace/tmp/savepoints/chapter-9/savepoint-new"
)
$missingRejected = $false
try { Get-RollbackSavepointPath -Lines @("stop completed without evidence") | Out-Null } catch { $missingRejected = $true }
$invalidRejected = $false
try { Get-RollbackSavepointPath -Lines @(
    "Savepoint completed. Path: file:/workspace/tmp/savepoints/chapter-9/../bad"
) | Out-Null } catch { $invalidRejected = $true }

$script:jobState = "FAILED"
function Get-FlinkJob {
    param([string]$JobId)
    $name = if ($JobId -match "^a") { "chapter-9-datastream-quality-production" } else { "chapter-9-doris-clean" }
    [pscustomobject]@{ jid = $JobId; name = $name; state = $script:jobState }
}
$productionFailedRejected = $false
try {
    Wait-RollbackProductionFinished -JobId "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" `
        -ExpectedName "chapter-9-datastream-quality-production" -SavepointPath $savepoint `
        -Attempts 1 -SleepSeconds 0 | Out-Null
} catch { $productionFailedRejected = $_.Exception.Message -match "aaaaaaaa.*FAILED.*savepoint" }
$cleanFailedRejected = $false
try {
    Wait-RollbackCleanCanceled -JobId "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" `
        -ExpectedName "chapter-9-doris-clean" -Attempts 1 -SleepSeconds 0 | Out-Null
} catch { $cleanFailedRejected = $_.Exception.Message -match "bbbb.*FAILED" }

function Get-FlinkJob {
    param([string]$JobId)
    [pscustomobject]@{ jid = "cccccccccccccccccccccccccccccccc"; name = "chapter9-doris-raw-rollback-test"; state = "RUNNING" }
}
$wrongIdRejected = $false
try {
    Wait-RollbackJobRunning -JobId "dddddddddddddddddddddddddddddddd" `
        -ExpectedName "chapter9-doris-raw-rollback-test" -Attempts 1 -SleepSeconds 0 | Out-Null
} catch { $wrongIdRejected = $_.Exception.Message -match "requested.*dddd|returned.*cccc" }

[ordered]@{
    savepoint = $savepoint
    missing_rejected = $missingRejected
    invalid_rejected = $invalidRejected
    production_failed_rejected = $productionFailedRejected
    clean_failed_rejected = $cleanFailedRejected
    wrong_id_rejected = $wrongIdRejected
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("file:/workspace/tmp/savepoints/chapter-9/savepoint-new", payload["savepoint"])
        self.assertTrue(payload["missing_rejected"])
        self.assertTrue(payload["invalid_rejected"])
        self.assertTrue(payload["production_failed_rejected"])
        self.assertTrue(payload["clean_failed_rejected"])
        self.assertTrue(payload["wrong_id_rejected"])

    def test_rollback_manifest_schema_rejects_unknown_types_and_bad_offsets(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/rollback_chapter_9_production.ps1") -FunctionsOnly
$base = [pscustomobject]@{
    cutover_id = "85c971e5-1e96-4c21-8cce-35f25402a543"
    created_at = "2026-07-22T09:05:59.8072951+00:00"
    raw_offsets = @("partition:0,offset:212", "partition:2,offset:0")
    shadow_job_id = "6f6e24deea18e22722bfd5e0a83895e4"
    savepoint_path = "file:/workspace/tmp/savepoints/chapter-9/savepoint-new"
    production_job_id = "0d8edd967461402a66e9672d2335ca6d"
    doris_job_id = "bf10b31978af0ae53446535c41120870"
    iceberg_job_id = "ce7ec8a8d04e70f45f6c7806ed1ede28"
}
function Copy-Manifest([object]$Value) { return ($Value | ConvertTo-Json | ConvertFrom-Json) }
function Is-Rejected([object]$Value) {
    try { Assert-RollbackManifest -Manifest $Value | Out-Null; return $false } catch { return $true }
}
$valid = $false
try { Assert-RollbackManifest -Manifest $base | Out-Null; $valid = $true } catch {}
$unknown = Copy-Manifest $base
$unknown | Add-Member -MemberType NoteProperty -Name unexpected -Value "x"
$nonIso = Copy-Manifest $base; $nonIso.created_at = "07/22/2026"
$pathTraversal = Copy-Manifest $base; $pathTraversal.savepoint_path = "file:/workspace/tmp/savepoints/chapter-9/../bad"
$negative = Copy-Manifest $base; $negative.raw_offsets = @("partition:0,offset:-1")
$duplicate = Copy-Manifest $base; $duplicate.raw_offsets = @("partition:0,offset:1", "partition:0,offset:2")
$scalarOffsets = Copy-Manifest $base; $scalarOffsets.raw_offsets = "partition:0,offset:212"
$badClean = Copy-Manifest $base; $badClean | Add-Member -MemberType NoteProperty -Name clean_event_ids -Value @("clean-a", "clean-a")
$badId = Copy-Manifest $base; $badId.production_job_id = "ABC"
[ordered]@{
    valid = $valid
    unknown_rejected = Is-Rejected $unknown
    non_iso_rejected = Is-Rejected $nonIso
    path_traversal_rejected = Is-Rejected $pathTraversal
    negative_rejected = Is-Rejected $negative
    duplicate_rejected = Is-Rejected $duplicate
    scalar_offsets_rejected = Is-Rejected $scalarOffsets
    bad_clean_rejected = Is-Rejected $badClean
    bad_id_rejected = Is-Rejected $badId
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["valid"])
        for key in (
            "unknown_rejected", "non_iso_rejected", "path_traversal_rejected",
            "negative_rejected", "duplicate_rejected", "scalar_offsets_rejected",
            "bad_clean_rejected", "bad_id_rejected"
        ):
            self.assertTrue(payload[key], key)

    def test_rollback_render_and_dry_run_boundaries_cover_multi_partition_offsets(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/rollback_chapter_9_production.ps1") -FunctionsOnly
$root = Join-Path ([IO.Path]::GetTempPath()) ("chapter9-render-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $root | Out-Null
$result = Render-RollbackSql -Template "'properties.group.id' = '__ROLLBACK_GROUP_ID__'; 'scan.startup.specific-offsets' = '__SPECIFIC_OFFSETS__'" `
    -RawOffsets @("partition:0,offset:212", "partition:2,offset:0") -CutoverId "85c971e5-1e96-4c21-8cce-35f25402a543" `
    -DorisPath (Join-Path $root "doris.sql") -IcebergPath (Join-Path $root "iceberg.sql") `
    -DorisSink "DORIS" -DorisInsert "INSERT_DORIS" -IcebergCatalog "ICEBERG" -IcebergInsert "INSERT_ICEBERG"
$doris = Get-Content -Raw (Join-Path $root "doris.sql")
$iceberg = Get-Content -Raw (Join-Path $root "iceberg.sql")
$script:forbiddenCalls = @()
function Invoke-DockerCommand { $script:forbiddenCalls += "docker"; throw "dry-run called docker" }
function Submit-RollbackSqlJob { $script:forbiddenCalls += "submit"; throw "dry-run submitted" }
Write-RollbackDryRunPlan -ProductionJobId "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" -DorisJobId "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" `
    -IcebergJobId "cccccccccccccccccccccccccccccccc" -DorisGroup $result.DorisGroup -IcebergGroup $result.IcebergGroup `
    -DorisPath "doris.sql" -IcebergPath "iceberg.sql"
[ordered]@{
    doris_multi = ($doris -match "partition:0,offset:212;partition:2,offset:0")
    iceberg_multi = ($iceberg -match "partition:0,offset:212;partition:2,offset:0")
    groups_isolated = (($doris -match "chapter9-doris-raw-rollback") -and ($iceberg -match "chapter9-iceberg-raw-rollback") -and ($doris -notmatch "chapter9-iceberg-raw-rollback") -and ($iceberg -notmatch "chapter9-doris-raw-rollback"))
    dry_run_calls = ($script:forbiddenCalls -join ",")
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["doris_multi"])
        self.assertTrue(payload["iceberg_multi"])
        self.assertTrue(payload["groups_isolated"])
        self.assertEqual("", payload["dry_run_calls"])

    def test_rollback_real_orchestration_is_ordered_and_stops_after_failure(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/rollback_chapter_9_production.ps1") -FunctionsOnly
$manifest = [pscustomobject]@{
    production_job_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    doris_job_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    iceberg_job_id = "cccccccccccccccccccccccccccccccc"
}
$script:calls = @()
$script:productionState = "FINISHED"
function Invoke-DockerCommand {
    param([string[]]$Arguments, [string]$FailureMessage)
    if ($Arguments -contains "stop") {
        $script:calls += "stop-production"
        return @("Savepoint completed. Path: file:/workspace/tmp/savepoints/chapter-9/savepoint-new")
    }
    if ($Arguments -contains "cancel") {
        $id = $Arguments[-1]
        $script:calls += "cancel-$id"
        return @("canceled")
    }
    throw "unexpected docker call"
}
function Get-FlinkJob {
    param([string]$JobId)
    $script:calls += "get-$JobId"
    if ($JobId -eq $manifest.production_job_id) { return [pscustomobject]@{ jid = $JobId; name = "chapter-9-datastream-quality-production"; state = $script:productionState } }
    if ($JobId -eq $manifest.doris_job_id) { return [pscustomobject]@{ jid = $JobId; name = "chapter-9-doris-clean"; state = "CANCELED" } }
    if ($JobId -eq $manifest.iceberg_job_id) { return [pscustomobject]@{ jid = $JobId; name = "chapter-9-iceberg-clean"; state = "CANCELED" } }
    if ($JobId -eq "dddddddddddddddddddddddddddddddd") { return [pscustomobject]@{ jid = $JobId; name = "chapter9-doris-raw-rollback-test"; state = "RUNNING" } }
    if ($JobId -eq "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee") { return [pscustomobject]@{ jid = $JobId; name = "chapter9-iceberg-raw-rollback-test"; state = "RUNNING" } }
    throw "unknown job $JobId"
}
function Submit-RollbackSqlJob {
    param([string]$SqlClient, [string]$ContainerSqlPath, [string[]]$ConnectorPaths, [string]$ParentClasspath)
    $script:calls += "submit-$ContainerSqlPath"
    if ($ContainerSqlPath -match "doris") { return @("JobID: dddddddddddddddddddddddddddddddd") }
    return @("JobID: eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
}
$success = Invoke-RollbackRealMode -Manifest $manifest -DorisJobName "chapter9-doris-raw-rollback-test" `
    -IcebergJobName "chapter9-iceberg-raw-rollback-test" -DorisSqlPath "doris.sql" -IcebergSqlPath "iceberg.sql" `
    -DorisConnectors @() -IcebergConnectors @() -IcebergClasspath "" -Attempts 1 -SleepSeconds 0
$successCalls = $script:calls -join ">"
$script:calls = @(); $script:productionState = "FAILED"
$failureRejected = $false
try {
    Invoke-RollbackRealMode -Manifest $manifest -DorisJobName "chapter9-doris-raw-rollback-test" `
        -IcebergJobName "chapter9-iceberg-raw-rollback-test" -DorisSqlPath "doris.sql" -IcebergSqlPath "iceberg.sql" `
        -DorisConnectors @() -IcebergConnectors @() -IcebergClasspath "" -Attempts 1 -SleepSeconds 0 | Out-Null
} catch { $failureRejected = $true }
[ordered]@{
    success_order = $successCalls
    failure_rejected = $failureRejected
    failure_calls = $script:calls -join ">"
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(
            "stop-production>get-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa>cancel-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb>get-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb>cancel-cccccccccccccccccccccccccccccccc>get-cccccccccccccccccccccccccccccccc>submit-doris.sql>submit-iceberg.sql>get-dddddddddddddddddddddddddddddddd>get-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            payload["success_order"]
        )
        self.assertTrue(payload["failure_rejected"])
        self.assertEqual(
            "stop-production>get-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            payload["failure_calls"]
        )

    def test_cutover_requires_traffic_gate_savepoint_manifest_and_three_jobs(self):
        text = (ROOT / "scripts/run_chapter_9_production_cutover.ps1").read_text(encoding="utf-8")
        for marker in (
            "[switch]$TrafficPaused",
            "cutover-manifest.json",
            "cutover-manifest.json.partial",
            "kafka-get-offsets",
            "kafka-consumer-groups",
            "--savepointPath",
            " -s $savepointPath",
            "--mode production",
            "--consumer-group chapter9-quality-production",
            "--transaction-prefix chapter9-production",
            "13_source_user_behavior_clean_doris.sql",
            "14_source_user_behavior_clean_iceberg.sql",
            "chapter-9-datastream-quality-production",
            "chapter-9-doris-clean",
            "chapter-9-iceberg-clean",
            "Move-Item -LiteralPath",
        ):
            self.assertIn(marker, text)
        self.assertNotIn("--allowNonRestoredState", text)
        self.assertNotIn("kafka-topics --delete", text)
        self.assertNotIn("docker compose down", text)

    def test_cutover_pins_connector_hashes_and_uploads_tmp_jars(self):
        path = ROOT / "scripts/run_chapter_9_production_cutover.ps1"
        raw = path.read_bytes()
        text = raw.decode("ascii")
        expected = {
            "flink-sql-connector-kafka-3.3.0-1.19.jar": "F46F69333445C598EBA9E5068B0A58DD2B4BA797738FD0FD3EE4E862FE281691",
            "flink-doris-connector-1.19-25.1.0.jar": "CE1C35B6A16B24F67E61EE95B7DAB9802B1FB654B9DA4FE171C174B2F8B1CA36",
            "flink-sql-connector-hive-3.1.3_2.12-1.19.2.jar": "B7C401F01BF69DD72B052F4B0C548829ABB3528DFAA1DDFF68CD07EB4C552FEF",
            "iceberg-flink-runtime-1.19-1.6.1.jar": "D0B3FC51623E7091B4D5DB96178D8ED79102E51A93F649E3CE82EE4471C080AB",
            "iceberg-aws-bundle-1.6.1.jar": "D14A49CED66A20CBD30F73EBB379646248D784FC5CD49D7295D36524380330E3",
            "hadoop-client-api-3.3.6.jar": "F3D2347A6E1C6885D5BCFD4F60C3AC3810EC11068FC161E04329BAABF412D963",
            "hadoop-client-runtime-3.3.6.jar": "15F01BC804294DF06D2EFFC87DE363A83CF589F50558BDBF48F72541AD8DE854",
            "hadoop-aws-3.3.6.jar": "FBA9EB73E6F0F5458355627FE095F5124705D4048551F4D6AA4084777B824C13",
            "aws-java-sdk-bundle-1.12.262.jar": "873FE7CF495126619997BEC21C44DE5D992544AEA7E632FDC77ADB1A0915BAE5",
        }
        for filename, digest in expected.items():
            self.assertIn(filename, text)
            self.assertIn(digest, text)
            self.assertIn(f'"/workspace/tmp/chapter-9/lib/{filename}"', text)
        self.assertIn("Get-FileHash -Algorithm SHA256", text)
        self.assertIn('"$destination.partial"', text)
        self.assertIn("Invoke-WebRequest", text)
        self.assertIn("Move-Item -LiteralPath $partial", text)
        self.assertIn('$sqlClientArguments += @("-j", $containerPath)', text)
        self.assertIn("HADOOP_CLASSPATH", text)
        self.assertIn("classloader.resolve-order=parent-first", text)
        self.assertNotIn("--force-recreate flink-jobmanager", text)

    def test_cutover_checks_downstream_services_before_stopping_shadow(self):
        text = (ROOT / "scripts/run_chapter_9_production_cutover.ps1").read_text(encoding="ascii")
        for marker in (
            "http://localhost:9000/minio/health/ready",
            "test -d /data",
            'Join-Path $root "infra/compose/minio/data"',
            "/dev/tcp/hive-metastore/9083",
            "/dev/tcp/doris-fe/8030",
            "/dev/tcp/minio/9000",
            "SHOW TABLES FROM analytics LIKE 'realtime_metrics'",
            "doris-preflight.sql",
            "iceberg-preflight.sql",
            "SET 'execution.runtime-mode' = 'batch';",
            "SELECT COUNT(*) FROM lakehouse.analytics.user_behavior_detail;",
        ):
            self.assertIn(marker, text)
            self.assertLess(text.index(marker), text.index("[cutover] stopping shadow job"))

    def test_cutover_rejects_wrong_minio_data_bind_source(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
function Invoke-DockerCommand {
    param([string[]]$Arguments, [string]$FailureMessage)
    return @('{"Mounts":[{"Type":"bind","Source":"C:\\wrong-worktree\\infra\\compose\\minio\\data","Destination":"/data"}]}')
}
$rejected = $false
try {
    Assert-ContainerBindMountSource -Container "ecom-minio" -Destination "/data" `
        -ExpectedSource "C:\current-worktree\infra\compose\minio\data"
} catch {
    $rejected = $_.Exception.Message -match "mount source mismatch"
}
[ordered]@{ rejected = $rejected } | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["rejected"])

    def test_cutover_resume_partial_validates_exact_jobs_and_skips_completed_steps(self):
        text = (ROOT / "scripts/run_chapter_9_production_cutover.ps1").read_text(encoding="ascii")
        self.assertIn("[switch]$ResumePartial", text)
        resume_start = text.index("if ($ResumePartial) {")
        resume_end = text.index("# ResumePartial ends before normal cutover.")
        resume_block = text[resume_start:resume_end]
        self.assertIn("Assert-ResumeManifest", resume_block)
        self.assertIn("iceberg-clean.sql", resume_block)
        self.assertIn("Invoke-CutoverShadowStopStage", resume_block)
        self.assertIn("--mode", resume_block)
        self.assertIn("doris-clean.sql", resume_block)

        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
$productionId = "0123456789abcdef0123456789abcdef"
$dorisId = "fedcba9876543210fedcba9876543210"
$manifest = [pscustomobject]@{
    cutover_id = "cutover-1"
    raw_offsets = @("partition:0,offset:212")
    shadow_job_id = "11111111111111111111111111111111"
    savepoint_path = "file:/workspace/tmp/savepoints/chapter-9/savepoint-1"
    production_job_id = $productionId
    doris_job_id = $dorisId
    iceberg_job_id = $null
}
$jobs = [pscustomobject]@{ jobs = @(
    [pscustomobject]@{ jid = $productionId; name = "chapter-9-datastream-quality-production"; state = "RUNNING" },
    [pscustomobject]@{ jid = $dorisId; name = "chapter-9-doris-clean"; state = "RUNNING" },
    [pscustomobject]@{ jid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; name = "chapter-9-datastream-quality-production"; state = "CANCELED" },
    [pscustomobject]@{ jid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"; name = "chapter-9-doris-clean"; state = "FINISHED" }
) }
Assert-ResumeManifest -Manifest $manifest -Jobs $jobs
$mismatchRejected = $false
try {
    $jobs.jobs[0].jid = "22222222222222222222222222222222"
    Assert-ResumeManifest -Manifest $manifest -Jobs $jobs
} catch { $mismatchRejected = $true }
$jobs.jobs[0].jid = $productionId
$populatedRejected = $false
try {
    $manifest.iceberg_job_id = "33333333333333333333333333333333"
    Assert-ResumeManifest -Manifest $manifest -Jobs $jobs
} catch { $populatedRejected = $true }
$manifest.iceberg_job_id = $null
$existingIcebergId = "33333333333333333333333333333333"
$jobs.jobs += [pscustomobject]@{
    jid = $existingIcebergId
    name = "chapter-9-iceberg-clean"
    state = "RUNNING"
}
$jobs.jobs += [pscustomobject]@{
    jid = "cccccccccccccccccccccccccccccccc"
    name = "chapter-9-iceberg-clean"
    state = "CANCELED"
}
$adoptedIcebergId = Assert-ResumeManifest -Manifest $manifest -Jobs $jobs
[ordered]@{
    mismatch_rejected = $mismatchRejected
    populated_rejected = $populatedRejected
    adopted_iceberg_id = $adoptedIcebergId
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["mismatch_rejected"])
        self.assertTrue(payload["populated_rejected"])
        self.assertEqual("33333333333333333333333333333333", payload["adopted_iceberg_id"])

    def test_cutover_preflight_precedes_partial_and_resume_never_submits_preflight(self):
        text = (ROOT / "scripts/run_chapter_9_production_cutover.ps1").read_text(encoding="ascii")
        preflight_submit = text.index(
            'Submit-SqlJob -SqlClient $sqlClient -ContainerSqlPath "/workspace/tmp/chapter-9/iceberg-preflight.sql"'
        )
        initial_partial = text.index(
            "Write-ManifestPartial -Manifest $manifest -PartialPath $manifestPartialPath"
        )
        resume_start = text.index("if ($ResumePartial) {")
        resume_end = text.index("# ResumePartial ends before normal cutover.")
        self.assertLess(preflight_submit, initial_partial)
        self.assertNotIn("iceberg-preflight.sql", text[resume_start:resume_end])
        normal_guard = text.rfind("if (-not $ResumePartial) {", 0, preflight_submit)
        normal_guard_end = text.index("\n}", normal_guard)
        normal_preflight = text[normal_guard:normal_guard_end]
        self.assertIn("doris-preflight.sql", normal_preflight)
        self.assertIn("iceberg-preflight.sql", normal_preflight)

    def test_cutover_shadow_stop_resume_plans_initial_retry_recovery_and_ambiguity(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
$root = Join-Path ([IO.Path]::GetTempPath()) ("chapter9-shadow-recovery-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $root | Out-Null
try {
    $path = Join-Path $root "cutover.partial"
    $shadowId = "11111111111111111111111111111111"
    $state = New-CutoverRecoveryState -CutoverId "cutover-1" -Path $path
    $state | Add-Member -NotePropertyName shadow_job_id -NotePropertyValue $shadowId
    $running = [pscustomobject]@{ jobs = @(
        [pscustomobject]@{ jid = $shadowId; name = "chapter-9-datastream-quality-shadow"; state = "RUNNING" },
        [pscustomobject]@{ jid = "22222222222222222222222222222222"; name = "chapter-9-datastream-quality-shadow"; state = "CANCELED" }
    ) }
    $initial = Get-CutoverShadowStopResumePlan -State $state -Jobs $running `
        -SavepointCandidates @("savepoint-old")
    Set-CutoverMutationIntent -State $state -Path $path -Stage "shadow_stop" `
        -Operation "stop_shadow_with_savepoint" -Details @{
            job_id = $shadowId
            name = "chapter-9-datastream-quality-shadow"
            savepoint_directory_snapshot = @("savepoint-old")
        }
    $retry = Get-CutoverShadowStopResumePlan -State $state -Jobs $running `
        -SavepointCandidates @("savepoint-old")
    $finished = [pscustomobject]@{ jobs = @(
        [pscustomobject]@{ jid = $shadowId; name = "chapter-9-datastream-quality-shadow"; state = "FINISHED" },
        [pscustomobject]@{ jid = "22222222222222222222222222222222"; name = "chapter-9-datastream-quality-shadow"; state = "CANCELED" }
    ) }
    $recovered = Get-CutoverShadowStopResumePlan -State $state -Jobs $finished `
        -SavepointCandidates @("savepoint-old", "savepoint-new")
    $ambiguousRejected = $false
    try {
        Get-CutoverShadowStopResumePlan -State $state -Jobs $finished `
            -SavepointCandidates @("savepoint-old", "savepoint-a", "savepoint-b") | Out-Null
    } catch { $ambiguousRejected = $true }
    $terminalRejected = $false
    try {
        Get-CutoverShadowStopResumePlan -State $state -Jobs ([pscustomobject]@{ jobs = @(
            [pscustomobject]@{ jid = $shadowId; name = "chapter-9-datastream-quality-shadow"; state = "FAILED" }
        ) }) -SavepointCandidates @("savepoint-old") | Out-Null
    } catch { $terminalRejected = $true }
    [ordered]@{
        initial = $initial.Action
        retry = $retry.Action
        recovered = $recovered.Action
        recovered_path = $recovered.SavepointPath
        ambiguous_rejected = $ambiguousRejected
        terminal_rejected = $terminalRejected
    } | ConvertTo-Json -Compress
} finally { Remove-Item -LiteralPath $root -Recurse -Force }
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("stop", payload["initial"])
        self.assertEqual("retry_stop", payload["retry"])
        self.assertEqual("recover_savepoint", payload["recovered"])
        self.assertEqual(
            "file:/workspace/tmp/savepoints/chapter-9/savepoint-new",
            payload["recovered_path"],
        )
        self.assertTrue(payload["ambiguous_rejected"])
        self.assertTrue(payload["terminal_rejected"])

    def test_cutover_finalization_keeps_partial_resumable_and_writes_independent_final(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("chapter9-finalize-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $tempRoot | Out-Null
try {
    $productionId = "0123456789abcdef0123456789abcdef"
    $dorisId = "fedcba9876543210fedcba9876543210"
    $icebergId = "33333333333333333333333333333333"
    $partial = [ordered]@{
        cutover_id = "cutover-1"
        created_at = "2026-07-22T00:00:00Z"
        raw_offsets = @("partition:0,offset:212")
        shadow_job_id = "11111111111111111111111111111111"
        savepoint_path = "file:/workspace/tmp/savepoints/chapter-9/savepoint-1"
        production_job_id = $productionId
        doris_job_id = $dorisId
        iceberg_job_id = $null
    }
    $checkpoints = [pscustomobject]@{
        counts = [pscustomobject]@{ completed = 1 }
        latest = [pscustomobject]@{ completed = [pscustomobject]@{ status = "COMPLETED" } }
    }
    $script:failIceberg = $false
    function Wait-FlinkJobRunning {
        param([string]$JobId, [string]$ExpectedName)
        if ($script:failIceberg -and $ExpectedName -eq "chapter-9-iceberg-clean") {
            throw "simulated terminal state"
        }
        return [pscustomobject]@{ jid = $JobId; name = $ExpectedName; state = "RUNNING" }
    }

    $successPartialPath = Join-Path $tempRoot "success.partial"
    $successFinalPath = Join-Path $tempRoot "success.json"
    Write-ManifestPartial -Manifest $partial -PartialPath $successPartialPath
    Complete-CutoverManifest -PartialPath $successPartialPath -FinalPath $successFinalPath `
        -ProductionJobId $productionId -DorisJobId $dorisId -IcebergJobId $icebergId `
        -ProductionCheckpoints $checkpoints -DorisCheckpoints $checkpoints -IcebergCheckpoints $checkpoints
    $partialAfter = Get-Content -Raw $successPartialPath | ConvertFrom-Json
    $finalAfter = Get-Content -Raw $successFinalPath | ConvertFrom-Json

    $failurePartialPath = Join-Path $tempRoot "failure.partial"
    $failureFinalPath = Join-Path $tempRoot "failure.json"
    Write-ManifestPartial -Manifest $partial -PartialPath $failurePartialPath
    $script:failIceberg = $true
    try {
        Complete-CutoverManifest -PartialPath $failurePartialPath -FinalPath $failureFinalPath `
            -ProductionJobId $productionId -DorisJobId $dorisId -IcebergJobId $icebergId `
            -ProductionCheckpoints $checkpoints -DorisCheckpoints $checkpoints -IcebergCheckpoints $checkpoints
    } catch {}
    $failurePartial = Get-Content -Raw $failurePartialPath | ConvertFrom-Json

    [ordered]@{
        success_partial_iceberg_is_null = $null -eq $partialAfter.iceberg_job_id
        final_iceberg_id = $finalAfter.iceberg_job_id
        failure_partial_iceberg_is_null = $null -eq $failurePartial.iceberg_job_id
        failure_final_exists = Test-Path $failureFinalPath
    } | ConvertTo-Json -Compress
} finally {
    Remove-Item -LiteralPath $tempRoot -Recurse -Force
}
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["success_partial_iceberg_is_null"])
        self.assertEqual("33333333333333333333333333333333", payload["final_iceberg_id"])
        self.assertTrue(payload["failure_partial_iceberg_is_null"])
        self.assertFalse(payload["failure_final_exists"])

    def test_cutover_uses_one_shared_finalizer_and_reconciles_resume_iceberg_id(self):
        text = (ROOT / "scripts/run_chapter_9_production_cutover.ps1").read_text(encoding="ascii")
        self.assertEqual(3, text.count("Complete-CutoverManifest"))
        self.assertIn('"iceberg_submit" { "iceberg_job_id"; break }', text)
        self.assertIn("Sync-CutoverResolvedJobId", text)
        self.assertIn('if (Test-Path -LiteralPath $manifestPath)', text)
        resume_start = text.index("if ($ResumePartial) {")
        resume_end = text.index("# ResumePartial ends before normal cutover.")
        self.assertEqual(1, text[resume_start:resume_end].count("Complete-CutoverManifest"))
        self.assertEqual(1, text[resume_end:].count("Complete-CutoverManifest"))

    def test_cutover_iceberg_submission_unifies_client_classpath_and_uploads_jars(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
$script:dockerArguments = @()
function docker {
    param([Parameter(ValueFromRemainingArguments = $true)][object[]]$Arguments)
    $script:dockerArguments = @($Arguments)
    $global:LASTEXITCODE = 0
    Write-Output "[INFO] Execute statement succeed."
}
$connectors = @(
    [pscustomobject]@{ ContainerPath = "/workspace/tmp/chapter-9/lib/one.jar" },
    [pscustomobject]@{ ContainerPath = "/workspace/tmp/chapter-9/lib/two.jar" }
)
$parentClasspath = "/workspace/tmp/chapter-9/lib/one.jar:/workspace/tmp/chapter-9/lib/two.jar"
Submit-SqlJob -SqlClient "ecom-flink-sql-client" `
    -ContainerSqlPath "/workspace/tmp/chapter-9/iceberg-clean.sql" `
    -Connectors $connectors -ParentClasspath $parentClasspath | Out-Null
[ordered]@{ arguments = $script:dockerArguments -join "|" } | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        arguments = json.loads(result.stdout.strip().splitlines()[-1])["arguments"]
        self.assertIn("exec|-e|HADOOP_CLASSPATH=", arguments)
        self.assertIn("|-D|classloader.resolve-order=parent-first|", arguments)
        self.assertIn("|-j|/workspace/tmp/chapter-9/lib/one.jar", arguments)
        self.assertIn("|-j|/workspace/tmp/chapter-9/lib/two.jar", arguments)

    def test_cutover_native_wrapper_uses_exit_code_for_successful_stderr(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
function docker {
    param([Parameter(ValueFromRemainingArguments = $true)][object[]]$Arguments)
    $emptyNativeError = [System.Management.Automation.ErrorRecord]::new(
        [System.Management.Automation.RemoteException]::new(""),
        "NativeCommandError",
        [System.Management.Automation.ErrorCategory]::NotSpecified,
        $null
    )
    $warningNativeError = [System.Management.Automation.ErrorRecord]::new(
        [System.Management.Automation.RemoteException]::new("non-fatal native warning"),
        "NativeCommandErrorMessage",
        [System.Management.Automation.ErrorCategory]::NotSpecified,
        "non-fatal native warning"
    )
    Write-Error -ErrorRecord $emptyNativeError
    Write-Error -ErrorRecord $warningNativeError
    Write-Output "payload"
    $global:LASTEXITCODE = 0
}
$output = @(Invoke-DockerCommand -Arguments @("example") -FailureMessage "unexpected failure")
[ordered]@{
    count = $output.Count
    warning = [bool]($output -contains "non-fatal native warning")
    payload = [bool]($output -contains "payload")
    leaked_type = [bool]($output -contains "System.Management.Automation.RemoteException")
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(2, payload["count"])
        self.assertTrue(payload["warning"])
        self.assertTrue(payload["payload"])
        self.assertFalse(payload["leaked_type"])

    def test_cutover_parsers_reject_ambiguous_state_and_format_offsets(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
$offsets = ConvertFrom-KafkaOffsets `
    -Lines @("user_behavior_events:1:43", "user_behavior_events:0:212") `
    -ExpectedTopic "user_behavior_events"
$group = ConvertFrom-KafkaGroupDescription @(
    "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID",
    "chapter9-quality-shadow user_behavior_events 0 212 212 0 - - -"
) -ExpectedGroup "chapter9-quality-shadow" -ExpectedTopic "user_behavior_events"
$jobId = Get-SubmittedJobId @("Job has been submitted with JobID 0123456789abcdef0123456789abcdef")
$savepoint = Get-SavepointPath @("Savepoint completed. Path: file:/workspace/tmp/savepoints/chapter-9/savepoint-1")
$ambiguousRejected = $false
try {
    Get-SubmittedJobId @(
        "JobID 0123456789abcdef0123456789abcdef",
        "JobID fedcba9876543210fedcba9876543210"
    ) | Out-Null
} catch {
    $ambiguousRejected = $true
}
[ordered]@{
    offsets = $offsets -join ";"
    lag = $group.TotalLag
    current_offset = $group.Rows[0].CurrentOffset
    job_id = $jobId
    savepoint = $savepoint
    ambiguous_rejected = $ambiguousRejected
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("partition:0,offset:212;partition:1,offset:43", payload["offsets"])
        self.assertEqual(0, payload["lag"])
        self.assertEqual(212, payload["current_offset"])
        self.assertEqual("0123456789abcdef0123456789abcdef", payload["job_id"])
        self.assertEqual("file:/workspace/tmp/savepoints/chapter-9/savepoint-1", payload["savepoint"])
        self.assertTrue(payload["ambiguous_rejected"])

    def test_cutover_kafka_parsers_accept_exact_single_and_multi_partition_inputs(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
$single = ConvertFrom-KafkaOffsets -Lines @("user_behavior_events:0:212") `
    -ExpectedTopic "user_behavior_events"
$multi = ConvertFrom-KafkaOffsets `
    -Lines @("user_behavior_events:2:9", "user_behavior_events:0:7", "user_behavior_events:1:8") `
    -ExpectedTopic "user_behavior_events"
$group = ConvertFrom-KafkaGroupDescription -Lines @(
    "chapter9-quality-shadow user_behavior_events 1 8 8 0 - - -",
    "chapter9-quality-shadow user_behavior_events 0 7 7 0 - - -"
) -ExpectedGroup "chapter9-quality-shadow" -ExpectedTopic "user_behavior_events"
[ordered]@{
    single = $single -join ";"
    multi = $multi -join ";"
    group_partitions = @($group.Rows | Sort-Object Partition | ForEach-Object { $_.Partition }) -join ","
    lag = $group.TotalLag
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("partition:0,offset:212", payload["single"])
        self.assertEqual(
            "partition:0,offset:7;partition:1,offset:8;partition:2,offset:9",
            payload["multi"],
        )
        self.assertEqual("0,1", payload["group_partitions"])
        self.assertEqual(0, payload["lag"])

    def test_cutover_kafka_parsers_reject_wrong_identity_and_duplicate_partitions(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$wrongOffsetTopic = Test-Rejected {
    ConvertFrom-KafkaOffsets -Lines @("other_topic:0:212") -ExpectedTopic "user_behavior_events"
}
$duplicateOffset = Test-Rejected {
    ConvertFrom-KafkaOffsets `
        -Lines @("user_behavior_events:0:211", "user_behavior_events:0:212") `
        -ExpectedTopic "user_behavior_events"
}
$wrongGroup = Test-Rejected {
    ConvertFrom-KafkaGroupDescription `
        -Lines @("other-group user_behavior_events 0 212 212 0 - - -") `
        -ExpectedGroup "chapter9-quality-shadow" -ExpectedTopic "user_behavior_events"
}
$wrongGroupTopic = Test-Rejected {
    ConvertFrom-KafkaGroupDescription `
        -Lines @("chapter9-quality-shadow other_topic 0 212 212 0 - - -") `
        -ExpectedGroup "chapter9-quality-shadow" -ExpectedTopic "user_behavior_events"
}
$duplicateGroupPartition = Test-Rejected {
    ConvertFrom-KafkaGroupDescription -Lines @(
        "chapter9-quality-shadow user_behavior_events 0 211 212 1 - - -",
        "chapter9-quality-shadow user_behavior_events 0 212 212 0 - - -"
    ) -ExpectedGroup "chapter9-quality-shadow" -ExpectedTopic "user_behavior_events"
}
[ordered]@{
    wrong_offset_topic = $wrongOffsetTopic
    duplicate_offset = $duplicateOffset
    wrong_group = $wrongGroup
    wrong_group_topic = $wrongGroupTopic
    duplicate_group_partition = $duplicateGroupPartition
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(all(payload.values()))

    def test_cutover_kafka_gate_rejects_partition_set_mismatch(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
function Invoke-DockerCommand {
    param([string[]]$Arguments, [string]$FailureMessage)
    $script:describeCalls++
    return @("chapter9-quality-shadow user_behavior_events 0 6 7 1 - - -")
}
$script:describeCalls = 0
$rejected = $false
try {
    Wait-ShadowLagZero `
        -ExpectedOffsets @("partition:0,offset:7", "partition:1,offset:8") `
        -KafkaContainer "ecom-kafka" -Attempts 2 -SleepSeconds 0
} catch {
    $rejected = $_.Exception.Message -match "partition set"
}
[ordered]@{ rejected = $rejected; describe_calls = $script:describeCalls } | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["rejected"])
        self.assertEqual(1, payload["describe_calls"])

    def test_cutover_named_job_wait_allows_nonterminal_registration_states(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
$script:calls = 0
function Get-FlinkJobs {
    $script:calls++
    $state = if ($script:calls -eq 1) { "CREATED" } else { "RUNNING" }
    return [pscustomobject]@{ jobs = @([pscustomobject]@{
        name = "chapter-9-doris-clean"
        jid = "0123456789abcdef0123456789abcdef"
        state = $state
    }) }
}
$jobId = Wait-NewNamedJob -Name "chapter-9-doris-clean" -Attempts 2 -SleepSeconds 0
[ordered]@{ calls = $script:calls; job_id = $jobId } | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(2, payload["calls"])
        self.assertEqual("0123456789abcdef0123456789abcdef", payload["job_id"])

    def test_cutover_named_job_wait_ignores_terminal_history_but_rejects_running_conflicts(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
$name = "chapter-9-doris-clean"
$runningId = "0123456789abcdef0123456789abcdef"
$script:jobs = [pscustomobject]@{ jobs = @(
    [pscustomobject]@{ name = $name; jid = "11111111111111111111111111111111"; state = "CANCELED" },
    [pscustomobject]@{ name = $name; jid = "22222222222222222222222222222222"; state = "FINISHED" },
    [pscustomobject]@{ name = $name; jid = $runningId; state = "RUNNING" }
) }
function Get-FlinkJobs { return $script:jobs }
$accepted = Wait-NewNamedJob -Name $name -Attempts 1 -SleepSeconds 0

$script:jobs = [pscustomobject]@{ jobs = @(
    [pscustomobject]@{ name = $name; jid = $runningId; state = "RUNNING" },
    [pscustomobject]@{ name = $name; jid = "33333333333333333333333333333333"; state = "RUNNING" },
    [pscustomobject]@{ name = $name; jid = "44444444444444444444444444444444"; state = "FAILED" }
) }
$multipleRunningRejected = $false
try { Wait-NewNamedJob -Name $name -Attempts 1 -SleepSeconds 0 | Out-Null } catch { $multipleRunningRejected = $true }

$script:jobs = [pscustomobject]@{ jobs = @(
    [pscustomobject]@{ name = $name; jid = $runningId; state = "RUNNING" },
    [pscustomobject]@{ name = $name; jid = $runningId; state = "CANCELED" }
) }
$terminalSameIdRejected = $false
try { Wait-NewNamedJob -Name $name -Attempts 1 -SleepSeconds 0 | Out-Null } catch { $terminalSameIdRejected = $true }

$script:jobs = [pscustomobject]@{ jobs = @(
    [pscustomobject]@{ name = $name; jid = "not-a-job-id"; state = "RUNNING" }
) }
$invalidIdRejected = $false
try { Wait-NewNamedJob -Name $name -Attempts 1 -SleepSeconds 0 | Out-Null } catch { $invalidIdRejected = $true }

[ordered]@{
    accepted_id = $accepted
    multiple_running_rejected = $multipleRunningRejected
    terminal_same_id_rejected = $terminalSameIdRejected
    invalid_id_rejected = $invalidIdRejected
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("0123456789abcdef0123456789abcdef", payload["accepted_id"])
        self.assertTrue(payload["multiple_running_rejected"])
        self.assertTrue(payload["terminal_same_id_rejected"])
        self.assertTrue(payload["invalid_id_rejected"])

    def test_cutover_checkpoint_waiter_tolerates_failed_startup_checkpoint_while_running(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
$script:jobCalls = 0
$script:checkpointCalls = 0
function Invoke-RestMethod {
    param([string]$Uri, [int]$TimeoutSec)
    if ($Uri -match "/checkpoints$") {
        $script:checkpointCalls++
        if ($script:checkpointCalls -eq 1) {
            return [pscustomobject]@{
                counts = [pscustomobject]@{ completed = 0; failed = 1 }
                latest = [pscustomobject]@{ completed = $null }
            }
        }
        return [pscustomobject]@{
            counts = [pscustomobject]@{ completed = 1; failed = 1 }
            latest = [pscustomobject]@{
                completed = [pscustomobject]@{ status = "COMPLETED" }
            }
        }
    }
    $script:jobCalls++
    return [pscustomobject]@{
        jid = "0123456789abcdef0123456789abcdef"
        name = "chapter-9-iceberg-clean"
        state = "RUNNING"
    }
}
$result = Wait-NewCompletedCheckpoint `
    -JobId "0123456789abcdef0123456789abcdef" `
    -ExpectedName "chapter-9-iceberg-clean" -Attempts 2 -SleepSeconds 0
[ordered]@{
    job_calls = $script:jobCalls
    checkpoint_calls = $script:checkpointCalls
    completed = $result.counts.completed
    failed = $result.counts.failed
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(2, payload["job_calls"])
        self.assertEqual(2, payload["checkpoint_calls"])
        self.assertEqual(1, payload["completed"])
        self.assertEqual(1, payload["failed"])

    def test_cutover_checkpoint_waiter_fails_immediately_on_terminal_job(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
$script:jobCalls = 0
$script:checkpointCalls = 0
function Invoke-RestMethod {
    param([string]$Uri, [int]$TimeoutSec)
    if ($Uri -match "/checkpoints$") {
        $script:checkpointCalls++
        throw "checkpoint endpoint must not be called"
    }
    $script:jobCalls++
    return [pscustomobject]@{
        jid = "0123456789abcdef0123456789abcdef"
        name = "chapter-9-iceberg-clean"
        state = "FAILED"
    }
}
$terminalRejected = $false
try {
    Wait-NewCompletedCheckpoint `
        -JobId "0123456789abcdef0123456789abcdef" `
        -ExpectedName "chapter-9-iceberg-clean" -Attempts 2 -SleepSeconds 0
} catch {
    $terminalRejected = $_.Exception.Message -match "terminal state FAILED"
}
[ordered]@{
    terminal_rejected = $terminalRejected
    job_calls = $script:jobCalls
    checkpoint_calls = $script:checkpointCalls
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["terminal_rejected"])

    def test_cutover_recovery_state_records_intent_and_result_and_fails_closed_on_adoption(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
$root = Join-Path ([IO.Path]::GetTempPath()) ("chapter9-cutover-state-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $root | Out-Null
try {
    $path = Join-Path $root "cutover-manifest.json.partial"
    $state = New-CutoverRecoveryState -CutoverId "cutover-1" -Path $path
    $script:mutationCalls = 0
    $crashRejected = $false
    try {
        Invoke-CutoverMutation -State $state -Path $path -Stage "production_submit" `
            -Operation "submit_production" -Details @{ name = "chapter-9-datastream-quality-production" } `
            -Action { $script:mutationCalls++; throw "simulated REST output loss" } | Out-Null
    } catch { $crashRejected = $true }
    $crashed = Get-Content -Raw $path | ConvertFrom-Json
    $crashedStatus = $crashed.mutations.production_submit.status
    $crashedIntentExists = ($null -ne $crashed.mutations.production_submit.intent)
    $ambiguousState = Get-Content -Raw $path | ConvertFrom-Json
    $jobs = [pscustomobject]@{ jobs = @(
        [pscustomobject]@{ jid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; name = "chapter-9-datastream-quality-production"; state = "RUNNING" },
        [pscustomobject]@{ jid = "cccccccccccccccccccccccccccccccc"; name = "chapter-9-datastream-quality-production"; state = "CANCELED" }
    ) }
    $adopted = Resolve-CutoverJobReference -State $crashed -Stage "production_submit" `
        -ExpectedName "chapter-9-datastream-quality-production" -Jobs $jobs -Path $path
    $adoptedState = Get-Content -Raw $path | ConvertFrom-Json
    $noIntentRejected = $false
    try {
        Resolve-CutoverJobReference -State $adoptedState -Stage "doris_submit" `
            -ExpectedName "chapter-9-doris-clean" -Jobs $jobs -Path $path | Out-Null
    } catch { $noIntentRejected = $true }
    $ambiguousRejected = $false
    try {
        Resolve-CutoverJobReference -State $ambiguousState -Stage "production_submit" `
            -ExpectedName "chapter-9-datastream-quality-production" -Jobs ([pscustomobject]@{ jobs = @(
                [pscustomobject]@{ jid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; name = "chapter-9-datastream-quality-production"; state = "RUNNING" },
                [pscustomobject]@{ jid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"; name = "chapter-9-datastream-quality-production"; state = "RUNNING" }
            ) }) -Path $path | Out-Null
    } catch { $ambiguousRejected = $true }
    [ordered]@{
        crash_rejected = $crashRejected
        mutation_calls = $script:mutationCalls
        intent_status = $crashedStatus
        intent_exists = $crashedIntentExists
        intent_operation = "submit_production"
        result_status = "failed"
        adopted_id = $adopted
        adopted_persisted = $adoptedState.mutations.production_submit.result.job_id
        no_intent_rejected = $noIntentRejected
        ambiguous_rejected = $ambiguousRejected
    } | ConvertTo-Json -Compress
} finally { Remove-Item -LiteralPath $root -Recurse -Force }
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["crash_rejected"])
        self.assertEqual(1, payload["mutation_calls"])
        self.assertEqual("failed", payload["intent_status"])
        self.assertTrue(payload["intent_exists"])
        self.assertEqual("submit_production", payload["intent_operation"])
        self.assertEqual("failed", payload["result_status"])
        self.assertEqual("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", payload["adopted_id"], result.stdout)
        self.assertEqual("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", payload["adopted_persisted"])
        self.assertTrue(payload["no_intent_rejected"])
        self.assertTrue(payload["ambiguous_rejected"])

    def test_cutover_resume_reconciles_all_resolved_job_ids_before_finalization(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_chapter_9_production_cutover.ps1") -FunctionsOnly
$root = Join-Path ([IO.Path]::GetTempPath()) ("chapter9-cutover-reconcile-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $root | Out-Null
try {
    $productionId = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    $dorisId = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    $icebergId = "cccccccccccccccccccccccccccccccc"
    $mappings = @(
        [pscustomobject]@{ Stage = "production_submit"; Field = "production_job_id"; Id = $productionId; Name = "chapter-9-datastream-quality-production" },
        [pscustomobject]@{ Stage = "doris_submit"; Field = "doris_job_id"; Id = $dorisId; Name = "chapter-9-doris-clean" },
        [pscustomobject]@{ Stage = "iceberg_submit"; Field = "iceberg_job_id"; Id = $icebergId; Name = "chapter-9-iceberg-clean" }
    )
    $jobs = [pscustomobject]@{ jobs = @(
        [pscustomobject]@{ jid = $productionId; name = "chapter-9-datastream-quality-production"; state = "RUNNING" },
        [pscustomobject]@{ jid = $dorisId; name = "chapter-9-doris-clean"; state = "RUNNING" },
        [pscustomobject]@{ jid = $icebergId; name = "chapter-9-iceberg-clean"; state = "RUNNING" },
        [pscustomobject]@{ jid = "11111111111111111111111111111111"; name = "chapter-9-datastream-quality-production"; state = "CANCELED" },
        [pscustomobject]@{ jid = "22222222222222222222222222222222"; name = "chapter-9-doris-clean"; state = "FAILED" },
        [pscustomobject]@{ jid = "33333333333333333333333333333333"; name = "chapter-9-iceberg-clean"; state = "FINISHED" }
    ) }
    $checkpoints = [pscustomobject]@{
        counts = [pscustomobject]@{ completed = 1 }
        latest = [pscustomobject]@{ completed = [pscustomobject]@{ status = "COMPLETED" } }
    }
    function Wait-FlinkJobRunning {
        param([string]$JobId, [string]$ExpectedName)
        return [pscustomobject]@{ jid = $JobId; name = $ExpectedName; state = "RUNNING" }
    }
    $script:submitCalls = 0
    $results = [ordered]@{}
    foreach ($target in $mappings) {
        $path = Join-Path $root "$($target.Stage).partial"
        $finalPath = Join-Path $root "$($target.Stage).json"
        $state = [ordered]@{
            schema_version = 2; cutover_id = "cutover-$($target.Stage)"; phase = $target.Stage
            created_at = "2026-07-22T00:00:00Z"; raw_offsets = @("partition:0,offset:212")
            shadow_job_id = "dddddddddddddddddddddddddddddddd"
            savepoint_path = "file:/workspace/tmp/savepoints/chapter-9/savepoint-1"
            production_job_id = $productionId; doris_job_id = $dorisId; iceberg_job_id = $null
            mutations = [ordered]@{
                shadow_stop = [ordered]@{ status = "result"; intent = @{ operation = "stop" }; result = @{ status = "result" } }
                production_submit = [ordered]@{ status = "result"; intent = @{ operation = "submit" }; result = @{ status = "result"; job_id = $productionId } }
                doris_submit = [ordered]@{ status = "result"; intent = @{ operation = "submit" }; result = @{ status = "result"; job_id = $dorisId } }
                iceberg_submit = [ordered]@{ status = "result"; intent = @{ operation = "submit" }; result = @{ status = "result"; job_id = $icebergId } }
                finalization = [ordered]@{ status = "not_started"; intent = $null; result = $null }
            }
        }
        $state[$target.Field] = $null
        Write-CutoverStateAtomic -State $state -Path $path
        $resolved = Resolve-CutoverJobReference -State $state -Stage $target.Stage `
            -ExpectedName $target.Name -Jobs $jobs -Path $path
        if (-not $resolved) { $script:submitCalls++ }
        $persisted = Get-Content -Raw $path | ConvertFrom-Json
        $finalized = $true
        try {
            Complete-CutoverManifest -PartialPath $path -FinalPath $finalPath `
                -ProductionJobId $productionId -DorisJobId $dorisId -IcebergJobId $icebergId `
                -ProductionCheckpoints $checkpoints -DorisCheckpoints $checkpoints `
                -IcebergCheckpoints $checkpoints
        } catch { $finalized = $false }
        $results[$target.Stage] = [ordered]@{
            resolved = $resolved
            persisted = [string]$persisted.($target.Field)
            no_next_file = -not (Test-Path "$path.next")
            finalized = $finalized
            final_exists = Test-Path $finalPath
        }
    }

    $conflicts = [ordered]@{}
    foreach ($target in $mappings) {
        $conflictPath = Join-Path $root "$($target.Stage)-conflict.partial"
        $conflict = [ordered]@{
            mutations = [ordered]@{}
        }
        $conflict[$target.Field] = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        $conflict.mutations[$target.Stage] = [ordered]@{
            status = "result"; intent = @{ operation = "submit" }
            result = @{ status = "result"; job_id = $target.Id }
        }
        Write-CutoverStateAtomic -State $conflict -Path $conflictPath
        $rejected = $false
        try {
            Resolve-CutoverJobReference -State $conflict -Stage $target.Stage `
                -ExpectedName $target.Name -Jobs $jobs -Path $conflictPath | Out-Null
        } catch { $rejected = $true }
        $conflictAfter = Get-Content -Raw $conflictPath | ConvertFrom-Json
        $conflicts[$target.Stage] = [ordered]@{
            rejected = $rejected
            preserved = [string]$conflictAfter.($target.Field)
        }
    }

    [ordered]@{
        submit_calls = $script:submitCalls
        production = $results.production_submit
        doris = $results.doris_submit
        iceberg = $results.iceberg_submit
        conflicts = $conflicts
    } | ConvertTo-Json -Depth 8 -Compress
} finally { Remove-Item -LiteralPath $root -Recurse -Force }
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(0, payload["submit_calls"])
        expected_ids = {
            "production": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "doris": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "iceberg": "cccccccccccccccccccccccccccccccc",
        }
        for stage, expected_id in expected_ids.items():
            self.assertEqual(expected_id, payload[stage]["resolved"])
            self.assertEqual(expected_id, payload[stage]["persisted"])
            self.assertTrue(payload[stage]["no_next_file"])
            self.assertTrue(payload[stage]["finalized"])
            self.assertTrue(payload[stage]["final_exists"])
        for conflict in payload["conflicts"].values():
            self.assertTrue(conflict["rejected"])
            self.assertEqual(
                "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                conflict["preserved"],
            )

    def test_rollback_progress_reconciles_mutation_windows_and_persists_ids(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/rollback_chapter_9_production.ps1") -FunctionsOnly
$root = Join-Path ([IO.Path]::GetTempPath()) ("chapter9-rollback-progress-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $root | Out-Null
try {
    $path = Join-Path $root "rollback-progress.json"
    $progress = New-RollbackProgress -Path $path -ManifestIds @{ production = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; doris = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"; iceberg = "cccccccccccccccccccccccccccccccc" }
    $failed = $false
    try {
        Invoke-RollbackMutation -Progress $progress -Path $path -Stage "production_stop" `
            -Operation "stop_with_savepoint" -Details @{ job_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" } `
            -Action { throw "crash after stop" } | Out-Null
    } catch { $failed = $true }
    $afterStop = Get-Content -Raw $path | ConvertFrom-Json
    $afterStop.stages.doris_cancel.intent = [pscustomobject]@{ operation = "cancel" }
    $afterStop.stages.iceberg_cancel.intent = [pscustomobject]@{ operation = "cancel" }
    $running = [pscustomobject]@{ jid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; name = "chapter-9-datastream-quality-production"; state = "RUNNING" }
    $runningPlan = Get-RollbackResumePlan -Progress $afterStop -Stage "production_stop" -Job $running
    $canceledPlan = Get-RollbackResumePlan -Progress $afterStop -Stage "doris_cancel" `
        -Job ([pscustomobject]@{ jid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"; name = "chapter-9-doris-clean"; state = "CANCELED" })
    $terminalRejected = $false
    try {
        Get-RollbackResumePlan -Progress $afterStop -Stage "iceberg_cancel" `
            -Job ([pscustomobject]@{ jid = "cccccccccccccccccccccccccccccccc"; name = "chapter-9-iceberg-clean"; state = "FAILED" }) | Out-Null
    } catch { $terminalRejected = $true }
    $afterStop.stages.doris_submit.intent = [pscustomobject]@{ operation = "submit_doris_raw_rollback" }
    $adoptedRollback = Resolve-RollbackSubmittedJob -Progress $afterStop -Stage "doris_submit" `
        -ExpectedName "chapter-9-doris-clean" -Jobs ([pscustomobject]@{ jobs = @(
            [pscustomobject]@{ jid = "dddddddddddddddddddddddddddddddd"; name = "chapter-9-doris-clean"; state = "RUNNING" }
        ) }) -Path $path
    $adoptAmbiguousRejected = $false
    $ambiguousRollback = Get-Content -Raw $path | ConvertFrom-Json
    $ambiguousRollback.stages.iceberg_submit.intent = [pscustomobject]@{ operation = "submit_iceberg_raw_rollback" }
    try {
        Resolve-RollbackSubmittedJob -Progress $ambiguousRollback -Stage "iceberg_submit" `
            -ExpectedName "chapter-9-iceberg-clean" -Jobs ([pscustomobject]@{ jobs = @(
                [pscustomobject]@{ jid = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"; name = "chapter-9-iceberg-clean"; state = "RUNNING" },
                [pscustomobject]@{ jid = "ffffffffffffffffffffffffffffffff"; name = "chapter-9-iceberg-clean"; state = "RUNNING" }
            ) }) -Path $path | Out-Null
    } catch { $adoptAmbiguousRejected = $true }
    $progress.rollback_jobs = [ordered]@{ doris = "dddddddddddddddddddddddddddddddd"; iceberg = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" }
    Write-RollbackProgressAtomic -Progress $progress -Path $path
    $final = Get-Content -Raw $path | ConvertFrom-Json
    [ordered]@{
        failed = $failed
        intent = $afterStop.stages.production_stop.status
        intent_exists = ($null -ne $afterStop.stages.production_stop.intent)
        result = $afterStop.stages.production_stop.result.status
        retry_stop = $runningPlan.Action
        canceled_done = $canceledPlan.Action
        terminal_rejected = $terminalRejected
        adopted_id = $adoptedRollback
        adoption_ambiguous_rejected = $adoptAmbiguousRejected
        doris_id = $final.rollback_jobs.doris
        iceberg_id = $final.rollback_jobs.iceberg
    } | ConvertTo-Json -Compress
} finally { Remove-Item -LiteralPath $root -Recurse -Force }
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["failed"])
        self.assertEqual("failed", payload["intent"])
        self.assertTrue(payload["intent_exists"])
        self.assertEqual("failed", payload["result"])
        self.assertEqual("retry_stop", payload["retry_stop"])
        self.assertEqual("complete", payload["canceled_done"])
        self.assertTrue(payload["terminal_rejected"])
        self.assertEqual("dddddddddddddddddddddddddddddddd", payload["adopted_id"])
        self.assertTrue(payload["adoption_ambiguous_rejected"])
        self.assertEqual("dddddddddddddddddddddddddddddddd", payload["doris_id"])
        self.assertEqual("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", payload["iceberg_id"])

    def test_rollback_submission_stage_creates_missing_intent_and_adopts_only_running_history(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/rollback_chapter_9_production.ps1") -FunctionsOnly
$root = Join-Path ([IO.Path]::GetTempPath()) ("chapter9-rollback-submit-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $root | Out-Null
try {
    $path = Join-Path $root "rollback-progress.json"
    $progress = New-RollbackProgress -Path $path -ManifestIds @{
        production = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        doris = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        iceberg = "cccccccccccccccccccccccccccccccc"
    }
    $script:submitCalls = 0
    $doris = Invoke-RollbackSubmissionStage -Progress $progress -Path $path `
        -Stage "doris_submit" -ExpectedName "chapter9-doris-raw-rollback-test" `
        -Operation "submit_doris_raw_rollback" -Jobs ([pscustomobject]@{ jobs = @() }) `
        -SubmitAction {
            $script:submitCalls++
            [pscustomobject]@{ output = @("Job ID: dddddddddddddddddddddddddddddddd"); job_id = "dddddddddddddddddddddddddddddddd" }
        }
    $afterDoris = Get-Content -Raw $path | ConvertFrom-Json
    $afterDoris.stages.iceberg_submit.intent = [pscustomobject]@{
        operation = "submit_iceberg_raw_rollback"
        details = [pscustomobject]@{ name = "chapter9-iceberg-raw-rollback-test" }
    }
    Write-RollbackProgressAtomic -Progress $afterDoris -Path $path
    $iceberg = Invoke-RollbackSubmissionStage -Progress $afterDoris -Path $path `
        -Stage "iceberg_submit" -ExpectedName "chapter9-iceberg-raw-rollback-test" `
        -Operation "submit_iceberg_raw_rollback" -Jobs ([pscustomobject]@{ jobs = @(
            [pscustomobject]@{ jid = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"; name = "chapter9-iceberg-raw-rollback-test"; state = "RUNNING" },
            [pscustomobject]@{ jid = "ffffffffffffffffffffffffffffffff"; name = "chapter9-iceberg-raw-rollback-test"; state = "CANCELED" }
        ) }) -SubmitAction { $script:submitCalls++; throw "must not resubmit" }
    $final = Get-Content -Raw $path | ConvertFrom-Json
    [ordered]@{
        submit_calls = $script:submitCalls
        doris_id = $doris.job_id
        doris_intent = $final.stages.doris_submit.intent.operation
        doris_result = $final.stages.doris_submit.result.status
        iceberg_id = $iceberg.job_id
        iceberg_adopted = $final.stages.iceberg_submit.result.adopted
        persisted_doris = $final.rollback_jobs.doris
        persisted_iceberg = $final.rollback_jobs.iceberg
    } | ConvertTo-Json -Compress
} finally { Remove-Item -LiteralPath $root -Recurse -Force }
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(1, payload["submit_calls"])
        self.assertEqual("dddddddddddddddddddddddddddddddd", payload["doris_id"])
        self.assertEqual("submit_doris_raw_rollback", payload["doris_intent"])
        self.assertEqual("result", payload["doris_result"])
        self.assertEqual("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", payload["iceberg_id"])
        self.assertTrue(payload["iceberg_adopted"])
        self.assertEqual("dddddddddddddddddddddddddddddddd", payload["persisted_doris"])
        self.assertEqual("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", payload["persisted_iceberg"])

    def test_rollback_live_validation_and_evidence_selection_ignore_terminal_history(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/rollback_chapter_9_production.ps1") -FunctionsOnly
$manifest = [pscustomobject]@{
    production_job_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    doris_job_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    iceberg_job_id = "cccccccccccccccccccccccccccccccc"
}
$jobs = [pscustomobject]@{ jobs = @(
    [pscustomobject]@{ jid = $manifest.production_job_id; name = "chapter-9-datastream-quality-production"; state = "RUNNING" },
    [pscustomobject]@{ jid = $manifest.doris_job_id; name = "chapter-9-doris-clean"; state = "RUNNING" },
    [pscustomobject]@{ jid = $manifest.iceberg_job_id; name = "chapter-9-iceberg-clean"; state = "RUNNING" },
    [pscustomobject]@{ jid = "dddddddddddddddddddddddddddddddd"; name = "chapter-9-datastream-quality-production"; state = "CANCELED" },
    [pscustomobject]@{ jid = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"; name = "chapter-9-doris-clean"; state = "FAILED" },
    [pscustomobject]@{ jid = "ffffffffffffffffffffffffffffffff"; name = "unrelated-history"; state = "FINISHED" },
    [pscustomobject]@{ jid = "11111111111111111111111111111111"; name = "chapter9-doris-raw-rollback-test"; state = "RUNNING" }
) }
$validated = @(Assert-RollbackLiveJobs -Manifest $manifest -Jobs $jobs)
$evidence = @(Select-RollbackEvidenceJobs -Manifest $manifest -Jobs $jobs)
[ordered]@{
    validated = @($validated.jid) -join ","
    evidence = @($evidence.jid | Sort-Object) -join ","
    excludes_terminal_history = @($evidence | Where-Object { $_.jid -eq "ffffffffffffffffffffffffffffffff" }).Count -eq 0
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,cccccccccccccccccccccccccccccccc",
            payload["validated"],
        )
        self.assertEqual(
            "11111111111111111111111111111111,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,cccccccccccccccccccccccccccccccc",
            payload["evidence"],
        )
        self.assertTrue(payload["excludes_terminal_history"])

    def test_verifier_durable_baseline_requires_causal_freshness_and_exact_api_values(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$root = Join-Path ([IO.Path]::GetTempPath()) ("chapter9-proof-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $root | Out-Null
try {
    $paths = Initialize-ProductionEvidenceRun -FinalPath (Join-Path $root "final.json") `
        -RunId "chapter9-production-0123456789abcdef0123456789abcdef" -DorisJobId "doris-job"
    $state = [pscustomobject]@{ RunId = "chapter9-production-0123456789abcdef0123456789abcdef"; RunPaths = $paths; DorisJobId = "doris-job"; InitialSent = $false; LateSent = $false }
    $baseline = [ordered]@{
        batch_start_utc = "2026-07-22T10:00:00.0000000+00:00"
        doris = [ordered]@{ pv = 1; uv = 1; pv_updated_at = "2026-07-22T09:59:00.0000000+00:00"; uv_updated_at = "2026-07-22T09:59:00.0000000+00:00" }
        trino = [ordered]@{ event_count = 815; distinct_event_id = 65 }
        checkpoints = [ordered]@{ production = 1 }
    }
    Write-ProductionCausalBaseline -RunState $state -Baseline $baseline
    Write-ProductionStageEvidence -RunState $state -Stage "pre_api" -Evidence @{ output = @{ raw = 8 }; groups = @{ production = @{ readable_data_lag = 0 } }; checkpoints = @{ production = @{ completed = 2 } }; doris_final = @{ pv = 2; uv = 2 }; trino_final = @{ total = 817 } }
    $loaded = Get-ProductionCausalBaseline -RunState $state
    $sameRejected = $false
    try { Assert-ProductionDorisFreshness -Metrics ([pscustomobject]@{ Pv = 2; Uv = 2; PvUpdatedAt = [DateTimeOffset]"2026-07-22T10:00:00Z"; UvUpdatedAt = [DateTimeOffset]"2026-07-22T10:00:00Z" }) -BatchStart ([DateTimeOffset]"2026-07-22T10:00:00Z") -Baseline $loaded.doris | Out-Null } catch { $sameRejected = $true }
    $api = [pscustomobject]@{ generated_at = "2026-07-22T10:01:00Z"; analyzer = "rules"; warnings = @(); evidence = [pscustomobject]@{ realtime = [pscustomobject]@{ pv = 2; uv = 2; updated_at = "2026-07-22T10:00:30Z" }; historical = [pscustomobject]@{ event_count = 817; latest_event_time = "2026-07-22T10:00:30Z" } } }
    $doris = [pscustomobject]@{ Pv = 2; Uv = 2; PvUpdatedAt = [DateTimeOffset]"2026-07-22T10:00:30Z"; UvUpdatedAt = [DateTimeOffset]"2026-07-22T10:00:30Z" }
    $trinoFinal = [pscustomobject]@{ EventCount = 817; LatestEventTime = [DateTimeOffset]"2026-07-22T10:00:30Z" }
    $apiAccepted = Assert-ProductionApiEvidence -Response $api -BatchStart ([DateTimeOffset]"2026-07-22T10:00:00Z") -TrinoBaseline 815 -DorisFinal $doris -TrinoFinal $trinoFinal
    $apiMismatchRejected = $false
    try { $api.evidence.realtime.updated_at = "2026-07-22T10:00:31Z"; Assert-ProductionApiEvidence -Response $api -BatchStart ([DateTimeOffset]"2026-07-22T10:00:00Z") -TrinoBaseline 815 -DorisFinal $doris -TrinoFinal $trinoFinal | Out-Null } catch { $apiMismatchRejected = $true }
    $api.evidence.realtime.updated_at = "2026-07-22T10:00:30Z"
    $api.evidence.historical.latest_event_time = "2026-07-22T10:00:31Z"
    $apiLatestMismatchRejected = $false
    try { Assert-ProductionApiEvidence -Response $api -BatchStart ([DateTimeOffset]"2026-07-22T10:00:00Z") -TrinoBaseline 815 -DorisFinal $doris -TrinoFinal $trinoFinal | Out-Null } catch { $apiLatestMismatchRejected = $true }
    $stageEvidence = Get-ProductionStageEvidence -RunState $state
    [ordered]@{ baseline_source = $loaded.Source; stage = $stageEvidence.pre_api.evidence.output.raw; same_rejected = $sameRejected; api_pv = $apiAccepted.RealtimePv; api_mismatch_rejected = $apiMismatchRejected; api_latest_mismatch_rejected = $apiLatestMismatchRejected } | ConvertTo-Json -Compress
} finally { Remove-Item -LiteralPath $root -Recurse -Force }
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("durable_run_baseline", payload["baseline_source"])
        self.assertEqual(8, payload["stage"], result.stdout)
        self.assertTrue(payload["same_rejected"])
        self.assertEqual(2, payload["api_pv"])
        self.assertTrue(payload["api_mismatch_rejected"])
        self.assertTrue(payload["api_latest_mismatch_rejected"])

    def test_production_resume_initialization_copies_validated_stage_evidence_before_early_failure(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$root = Join-Path ([IO.Path]::GetTempPath()) ("chapter9-resume-stage-copy-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $root | Out-Null
try {
    $runId = "chapter9-production-0123456789abcdef0123456789abcdef"
    $stages = [ordered]@{
        output = @{ evidence = @{ raw = 8 } }
        groups = @{ evidence = @{ production = @{ readable_data_lag = 0 } } }
        checkpoints = @{ evidence = @{ production = @{ completed = 2 } } }
        doris_final = @{ evidence = @{ Pv = 2; Uv = 2; PvUpdatedAt = "2026-07-22T10:00:30Z"; UvUpdatedAt = "2026-07-22T10:00:30Z" } }
        trino_final = @{ evidence = @{ Run = @{ EventCount = 2; DistinctEventId = 2; DistinctUserId = 2; ExcludedEventCount = 0; DuplicateEventCount = 1 }; Total = @{ EventCount = 817; DistinctEventId = 67; LatestEventTime = "2026-07-22T10:00:30Z" } } }
        pre_api = @{ evidence = @{ ready = $true } }
    }
    $stageRecord = [ordered]@{
        schema_version = 1
        proof_source = "durable_stage_evidence"
        run_id = $runId
        stages = $stages
    }
    $sourceRecord = [ordered]@{
        status = "failed"
        run_id = $runId
        doris_job_id = "doris-job"
        events_sent = $true
        failed_at_utc = "2026-07-22T10:01:00Z"
        stage_evidence = $stageRecord
    }
    $source = Join-Path $root "production-verification.$runId.failed.json"
    [IO.File]::WriteAllText($source, ($sourceRecord | ConvertTo-Json -Depth 15))
    $validated = Get-ProductionResumeStageEvidence -SourceFailedEvidence $sourceRecord -RunId $runId
    $paths = Initialize-ProductionResumeEvidenceRun `
        -FinalPath (Join-Path $root "production-verification.json") `
        -RunId $runId -DorisJobId "doris-job" -SourceFailedPath $source
    $copied = Get-Content -Raw $paths.StageEvidencePath | ConvertFrom-Json
    Write-ProductionRunFailure -RunPaths $paths -RunId $runId `
        -ErrorMessage "early resume failure" -EventsSent $true -DorisJobId "doris-job"
    $failed = Get-Content -Raw $paths.FailedPath | ConvertFrom-Json
    $wrongRunRejected = $false
    try {
        Get-ProductionResumeStageEvidence -SourceFailedEvidence $sourceRecord `
            -RunId "chapter9-production-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" | Out-Null
    } catch { $wrongRunRejected = $true }
    $badSchema = $sourceRecord | ConvertTo-Json -Depth 15 | ConvertFrom-Json
    $badSchema.stage_evidence.schema_version = 2
    $badSchemaRejected = $false
    try {
        Get-ProductionResumeStageEvidence -SourceFailedEvidence $badSchema -RunId $runId | Out-Null
    } catch { $badSchemaRejected = $true }
    [ordered]@{
        validated_run = $validated.run_id
        copied_run = $copied.run_id
        copied_stage_count = @($copied.stages.PSObject.Properties).Count
        failed_stage_count = @($failed.stage_evidence.stages.PSObject.Properties).Count
        failed_has_pre_api = $null -ne $failed.stage_evidence.stages.pre_api
        wrong_run_rejected = $wrongRunRejected
        bad_schema_rejected = $badSchemaRejected
    } | ConvertTo-Json -Compress
} finally { Remove-Item -LiteralPath $root -Recurse -Force }
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("chapter9-production-0123456789abcdef0123456789abcdef", payload["validated_run"])
        self.assertEqual(payload["validated_run"], payload["copied_run"])
        self.assertEqual(6, payload["copied_stage_count"])
        self.assertEqual(6, payload["failed_stage_count"])
        self.assertTrue(payload["failed_has_pre_api"])
        self.assertTrue(payload["wrong_run_rejected"])
        self.assertTrue(payload["bad_schema_rejected"])

    def test_production_trino_exact_delta_read_only_and_api_latest_are_fail_closed(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$run = [pscustomobject]@{
    EventCount = 2; DistinctEventId = 2; DistinctUserId = 2
    ExcludedEventCount = 0; DuplicateEventCount = 1
}
$total = [pscustomobject]@{
    EventCount = 817; DistinctEventId = 67
    LatestEventTime = [DateTimeOffset]"2026-07-22T10:00:30Z"
}
$exact = Assert-ProductionTrinoExactFinal -Run $run -Total $total `
    -BaselineEventCount 815 -BaselineDistinctEventId 65
$extraCountRejected = $false
try {
    Assert-ProductionTrinoExactFinal -Run $run `
        -Total ([pscustomobject]@{ EventCount = 818; DistinctEventId = 67; LatestEventTime = $total.LatestEventTime }) `
        -BaselineEventCount 815 -BaselineDistinctEventId 65 | Out-Null
} catch { $extraCountRejected = $true }
$extraDistinctRejected = $false
try {
    Assert-ProductionTrinoExactFinal -Run $run `
        -Total ([pscustomobject]@{ EventCount = 817; DistinctEventId = 68; LatestEventTime = $total.LatestEventTime }) `
        -BaselineEventCount 815 -BaselineDistinctEventId 65 | Out-Null
} catch { $extraDistinctRejected = $true }
$current = [pscustomobject]@{ Run = $run; Total = $total }
$prior = $current | ConvertTo-Json -Depth 8 | ConvertFrom-Json
$matched = Assert-ProductionTrinoMatchesDurableFinal -Current $current -PriorFinal $prior
$changedTotalRejected = $false
$changedTotal = $prior | ConvertTo-Json -Depth 8 | ConvertFrom-Json
$changedTotal.Total.EventCount = 818
try { Assert-ProductionTrinoMatchesDurableFinal -Current $current -PriorFinal $changedTotal | Out-Null } catch { $changedTotalRejected = $true }
$changedLatestRejected = $false
$changedLatest = $prior | ConvertTo-Json -Depth 8 | ConvertFrom-Json
$changedLatest.Total.LatestEventTime = "2026-07-22T10:00:31Z"
try { Assert-ProductionTrinoMatchesDurableFinal -Current $current -PriorFinal $changedLatest | Out-Null } catch { $changedLatestRejected = $true }
$api = [pscustomobject]@{
    generated_at = "2026-07-22T10:01:00Z"; analyzer = "rules"; warnings = @()
    evidence = [pscustomobject]@{
        realtime = [pscustomobject]@{ pv = 2; uv = 2; updated_at = "2026-07-22T10:00:30Z" }
        historical = [pscustomobject]@{ event_count = 817 }
    }
}
$doris = [pscustomobject]@{ Pv = 2; Uv = 2; PvUpdatedAt = [DateTimeOffset]"2026-07-22T10:00:30Z"; UvUpdatedAt = [DateTimeOffset]"2026-07-22T10:00:30Z" }
$missingLatestRejected = $false
try { Assert-ProductionApiEvidence -Response $api -BatchStart ([DateTimeOffset]"2026-07-22T10:00:00Z") -TrinoBaseline 815 -DorisFinal $doris -TrinoFinal $total | Out-Null } catch { $missingLatestRejected = $true }
[ordered]@{
    exact_total = $exact.Total.EventCount
    matched_total = $matched.Total.EventCount
    extra_count_rejected = $extraCountRejected
    extra_distinct_rejected = $extraDistinctRejected
    changed_total_rejected = $changedTotalRejected
    changed_latest_rejected = $changedLatestRejected
    missing_latest_rejected = $missingLatestRejected
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(817, payload["exact_total"])
        self.assertEqual(817, payload["matched_total"])
        self.assertTrue(payload["extra_count_rejected"])
        self.assertTrue(payload["extra_distinct_rejected"])
        self.assertTrue(payload["changed_total_rejected"])
        self.assertTrue(payload["changed_latest_rejected"])
        self.assertTrue(payload["missing_latest_rejected"])

    def test_phase_b_recovery_contracts_cover_resize_bind_abort_and_final_evidence_schema(self):
        resize = (ROOT / "scripts/resize_chapter_9_flink_slots.ps1").read_text(encoding="ascii")
        verifier = (ROOT / "scripts/verify_chapter_9_production.ps1").read_text(encoding="ascii")
        self.assertGreater(resize.count("Assert-ContainerBindMountSource"), 0)
        self.assertIn("post-recreate", resize)
        self.assertIn("ABORT", verifier)
        self.assertIn("stage_evidence", verifier)
        self.assertIn("proof_source", verifier)

    def test_resize_post_recreate_workspace_bind_uses_inspect_behavior(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/resize_chapter_9_flink_slots.ps1") -FunctionsOnly
$script:inspectJson = @([pscustomobject]@{
    Mounts = @([pscustomobject]@{
        Type = "bind"; Source = "C:\repo"; Destination = "/workspace"
    })
}) | ConvertTo-Json -Depth 5
function docker {
    $global:LASTEXITCODE = 0
    return $script:inspectJson
}
Assert-ContainerBindMountSource -Container "ecom-flink-taskmanager" `
    -Destination "/workspace" -ExpectedSource "C:\repo\"
$wrongSourceRejected = $false
try {
    Assert-ContainerBindMountSource -Container "ecom-flink-taskmanager" `
        -Destination "/workspace" -ExpectedSource "C:\other" | Out-Null
} catch { $wrongSourceRejected = $true }
$script:inspectJson = @([pscustomobject]@{
    Mounts = @([pscustomobject]@{
        Type = "volume"; Source = "C:\repo"; Destination = "/workspace"
    })
}) | ConvertTo-Json -Depth 5
$nonBindRejected = $false
try {
    Assert-ContainerBindMountSource -Container "ecom-flink-taskmanager" `
        -Destination "/workspace" -ExpectedSource "C:\repo" | Out-Null
} catch { $nonBindRejected = $true }
[ordered]@{
    wrong_source_rejected = $wrongSourceRejected
    non_bind_rejected = $nonBindRejected
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["wrong_source_rejected"])
        self.assertTrue(payload["non_bind_rejected"])

    def test_production_final_success_payload_schema_round_trips_completely(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$runId = "chapter9-production-0123456789abcdef0123456789abcdef"
$checkpoint = [ordered]@{ Completed = 4; LatestId = "4"; LatestStatus = "COMPLETED" }
$jobs = [ordered]@{}
foreach ($key in @("production", "doris", "iceberg")) {
    $jobs[$key] = [ordered]@{
        id = "$key-job"; name = "$key-name"; state = "RUNNING"
        checkpoint_baseline = $checkpoint; checkpoint_final = $checkpoint
    }
}
$groups = [ordered]@{}
foreach ($key in @("production", "doris", "iceberg")) {
    $groups[$key] = [ordered]@{
        group = "$key-group"; topic = "topic"; cli_lag = 0; readable_data_lag = 0
        partitions = @([ordered]@{
            partition = 0; current_offset = 8; log_end_offset = 8
            cli_lag = 0; readable_data_lag = 0; classifications = @()
        })
    }
}
$eventIds = [ordered]@{}
foreach ($key in @("duplicate", "malformed", "missing_required", "invalid_time", "future", "advancer", "late")) {
    $eventIds[$key] = "$runId-$key"
}
$payload = [ordered]@{
    status = "success"; run_id = $runId; logical_run_resumed = $false
    source_failed_evidence = $null; resume_chain = @(); doris_job_id = "doris-job"
    events_sent = $true; cutover_id = "cutover-1"
    batch_start_utc = "2026-07-22T10:00:00Z"; verified_at_utc = "2026-07-22T10:02:00Z"
    proof = [ordered]@{
        causal_baseline_source = "durable_run_baseline"; causal_baseline_path = "baseline.json"
        stage_evidence_source = "durable_stage_evidence"; stage_evidence_path = "stages.json"
        pre_api_stage = "output/groups/checkpoints/doris_final/trino_final"
    }
    event_ids = $eventIds
    counts = [ordered]@{ raw = 8; clean = 2; dlq = 5; late = 1; duplicate_clean = 1 }
    dlq_reason_counts = [ordered]@{
        DUPLICATE_EVENT = 1; MALFORMED_JSON = 1; MISSING_REQUIRED_FIELD = 1
        INVALID_EVENT_TIME = 1; FUTURE_EVENT_TIME = 1
    }
    resume = $null
    flink = [ordered]@{
        overview = [ordered]@{ taskmanagers = 1; slots_total = 4; slots_available = 1; jobs_running = 3 }
        jobs = $jobs
        watermark_gate = [ordered]@{
            watermark_proof_source = "live_operator_watermark"; vertex_id = "v"; metric_id = "m"
            watermark = 100; late_event_timestamp = 99; current_metric = [ordered]@{ vertex_id = "v"; metric_id = "m"; watermark = 100 }
            late_output_proof = [ordered]@{ EventId = "$runId-late"; LateTopicCount = 1; CleanCount = 0; DlqCount = 0 }
            prior_api_gate_failed_evidence = $null
        }
    }
    kafka_groups = $groups
    doris = [ordered]@{
        baseline = [ordered]@{ pv = 0; uv = 0; pv_updated_at = "2026-07-22T09:59:00Z"; uv_updated_at = "2026-07-22T09:59:00Z" }
        final = [ordered]@{ pv = 2; uv = 2; pv_updated_at = "2026-07-22T10:00:30Z"; uv_updated_at = "2026-07-22T10:00:30Z" }
    }
    trino = [ordered]@{
        baseline = [ordered]@{ EventCount = 815; DistinctEventId = 65; LatestEventTime = "2026-07-22T09:59:00Z" }
        total_final = [ordered]@{ EventCount = 817; DistinctEventId = 67; LatestEventTime = "2026-07-22T10:00:30Z" }
        latest_event_time_final = "2026-07-22T10:00:30Z"
        exact_clean_ids = @("$runId-duplicate", "$runId-advancer")
        exact_counts = [ordered]@{ EventCount = 2; DistinctEventId = 2; DistinctUserId = 2; ExcludedEventCount = 0; DuplicateEventCount = 1 }
        excluded_validation_and_late_ids = @("a", "b", "c", "d", "$runId-late")
        duplicate_event = [ordered]@{ id = "$runId-duplicate"; iceberg_count = 1; assertion = "measured_sql" }
    }
    api = [ordered]@{
        generated_at = "2026-07-22T10:01:00Z"; analyzer = "rules"; warnings = @()
        realtime_pv = 2; realtime_uv = 2; realtime_updated_at = "2026-07-22T10:00:30Z"
        historical_event_count = 817; historical_latest_event_time = "2026-07-22T10:00:30Z"
    }
}
$root = Join-Path ([IO.Path]::GetTempPath()) ("chapter9-final-schema-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $root | Out-Null
try {
    $path = Join-Path $root "evidence.json"
    Write-ProductionJsonAtomic -Value $payload -Path $path -PartialPath "$path.partial"
    $roundTrip = Get-Content -Raw $path | ConvertFrom-Json
    $validated = Assert-ProductionFinalEvidenceSchema -Evidence $roundTrip
    $missingRejected = $false
    $roundTrip.api.PSObject.Properties.Remove("historical_latest_event_time")
    try { Assert-ProductionFinalEvidenceSchema -Evidence $roundTrip | Out-Null } catch { $missingRejected = $true }
    [ordered]@{
        run_id = $validated.run_id
        job_count = @($validated.flink.jobs.PSObject.Properties).Count
        group_count = @($validated.kafka_groups.PSObject.Properties).Count
        missing_rejected = $missingRejected
    } | ConvertTo-Json -Compress
} finally {
    Remove-Item -LiteralPath $root -Recurse -Force
}
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("chapter9-production-0123456789abcdef0123456789abcdef", payload["run_id"])
        self.assertEqual(3, payload["job_count"])
        self.assertEqual(3, payload["group_count"])
        self.assertTrue(payload["missing_rejected"])

    def test_resize_script_recreates_only_taskmanager_and_checks_recovery(self):
        text = (ROOT / "scripts/resize_chapter_9_flink_slots.ps1").read_text(encoding="utf-8")
        for marker in (
            "Get-WorkspaceMountSource",
            "Assert-FlinkCapacity",
            "--no-deps",
            "--force-recreate",
            "flink-taskmanager",
            '"slots-total" -ne 4',
            "/checkpoints",
        ):
            self.assertIn(marker, text)
        self.assertNotIn("docker compose down", text)

    def test_resize_script_retries_checkpoint_rest_failures(self):
        transient_command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/resize_chapter_9_flink_slots.ps1") -FunctionsOnly
$script:checkpointCalls = 0
function Invoke-RestMethod {
    param([string]$Uri)
    $script:checkpointCalls++
    if ($script:checkpointCalls -eq 1) { throw "transient checkpoint REST failure" }
    return [pscustomobject]@{ counts = [pscustomobject]@{ completed = 11 } }
}
$checkpoints = Wait-NewCompletedCheckpoint -JobId "shadow-job" -Baseline 10 -Attempts 2 -SleepSeconds 0
[ordered]@{
    calls = $script:checkpointCalls
    completed = $checkpoints.counts.completed
} | ConvertTo-Json -Compress
'''
        transient_result = self._run_powershell(transient_command)
        self.assertEqual(0, transient_result.returncode, transient_result.stderr or transient_result.stdout)
        transient_payload = json.loads(transient_result.stdout.strip().splitlines()[-1])
        self.assertEqual(2, transient_payload["calls"])
        self.assertEqual(11, transient_payload["completed"])

        permanent_command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/resize_chapter_9_flink_slots.ps1") -FunctionsOnly
$script:checkpointCalls = 0
function Invoke-RestMethod {
    param([string]$Uri)
    $script:checkpointCalls++
    throw "permanent checkpoint REST failure"
}
try {
    Wait-NewCompletedCheckpoint -JobId "shadow-job" -Baseline 10 -Attempts 2 -SleepSeconds 0
    throw "Checkpoint wait unexpectedly succeeded."
} catch {
    [ordered]@{
        calls = $script:checkpointCalls
        message = $_.Exception.Message
    } | ConvertTo-Json -Compress
}
'''
        permanent_result = self._run_powershell(permanent_command)
        self.assertEqual(0, permanent_result.returncode, permanent_result.stderr or permanent_result.stdout)
        permanent_payload = json.loads(permanent_result.stdout.strip().splitlines()[-1])
        self.assertEqual(2, permanent_payload["calls"])
        self.assertIn("Last error: permanent checkpoint REST failure", permanent_payload["message"])

    def test_production_verifier_checks_quality_and_all_downstreams(self):
        path = ROOT / "scripts/verify_chapter_9_production.ps1"
        raw = path.read_bytes()
        text = raw.decode("ascii")
        for marker in (
            "cutover-manifest.json",
            "user_behavior_clean",
            "user_behavior_dlq",
            "user_behavior_late",
            "raw = clean + dlq + late",
            "DUPLICATE_EVENT",
            "MALFORMED_JSON",
            "MISSING_REQUIRED_FIELD",
            "INVALID_EVENT_TIME",
            "FUTURE_EVENT_TIME",
            "analytics.realtime_metrics",
            "lakehouse.analytics.user_behavior_detail",
            "/analysis/realtime",
            "/checkpoints",
            "slots-total",
            "chapter9-quality-production",
            "chapter9-doris-clean-v1",
            "chapter9-iceberg-clean-v1",
            "isolation.level=read_committed",
            "--timeout-ms 5000",
            "kafka-dump-log",
            "route-late-events",
            "currentInputWatermark",
            "StatePartialPath",
            "AS duplicate_event_count",
            'assertion = "measured_sql"',
            "ResumeRunId",
            "logical_run_resumed",
            "pre_resume_counts",
            "send_action = if",
            "resume_action = $resumeState.ResumeAction",
            "initial_events_resent",
            "derived_recovered",
            "read_only_finalize",
            "resume_chain",
            "observed_late_output_after_prior_gate",
            "watermark_proof_source",
        ):
            self.assertIn(marker, text)
        watermark_gate = text.index("$watermarkEvidence = Wait-ProductionWatermarkPast")
        late_send = text.index("Invoke-ProductionSendOnce -RunState $runState -Stage Late")
        post_late_checkpoint = text.index(
            "$productionFinalCheckpoint = Wait-NewProductionCheckpoint", late_send
        )
        self.assertLess(watermark_gate, late_send)
        self.assertLess(late_send, post_late_checkpoint)
        self.assertNotIn("force-recreate", text)
        self.assertNotIn("docker compose down", text)

    def test_production_verifier_validates_the_exact_event_matrix(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$runId = "chapter9-production-0123456789abcdef0123456789abcdef"
$clean = @(
    [pscustomobject]@{ event_id = "$runId-duplicate"; user_id = "$runId-user-1" },
    [pscustomobject]@{ event_id = "$runId-advancer"; user_id = "$runId-user-2" }
)
$dlq = @(
    [pscustomobject]@{ event_id = "$runId-duplicate"; reason_code = "DUPLICATE_EVENT" },
    [pscustomobject]@{ event_id = "$runId-malformed"; reason_code = "MALFORMED_JSON" },
    [pscustomobject]@{ event_id = "$runId-missing"; reason_code = "MISSING_REQUIRED_FIELD" },
    [pscustomobject]@{ event_id = "$runId-invalid-time"; reason_code = "INVALID_EVENT_TIME" },
    [pscustomobject]@{ event_id = "$runId-future"; reason_code = "FUTURE_EVENT_TIME" }
)
$late = @([pscustomobject]@{ event_id = "$runId-late" })
$rawValues = @(
    "duplicate-1-$runId", "duplicate-2-$runId", "malformed-$runId", "missing-$runId",
    "invalid-$runId", "future-$runId", "advancer-$runId", "late-$runId"
)
$valid = Assert-ProductionOutputMatrix -RunId $runId -RawValues $rawValues `
    -CleanRecords $clean -DlqRecords $dlq -LateRecords $late
$wrongUsersRejected = $false
try {
    $clean[1].user_id = $clean[0].user_id
    Assert-ProductionOutputMatrix -RunId $runId -RawValues $rawValues `
        -CleanRecords $clean -DlqRecords $dlq -LateRecords $late | Out-Null
} catch { $wrongUsersRejected = $true }
$emptyRawMessage = $null
try {
    Assert-ProductionOutputMatrix -RunId $runId -RawValues @() `
        -CleanRecords $clean -DlqRecords $dlq -LateRecords $late | Out-Null
} catch { $emptyRawMessage = $_.Exception.Message }
[ordered]@{
    raw = $valid.Raw
    clean = $valid.Clean
    dlq = $valid.Dlq
    late = $valid.Late
    duplicate_clean = $valid.DuplicateClean
    reasons = $valid.Reasons -join ","
    wrong_users_rejected = $wrongUsersRejected
    empty_raw_message = $emptyRawMessage
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual((8, 2, 5, 1, 1), tuple(payload[key] for key in (
            "raw", "clean", "dlq", "late", "duplicate_clean"
        )))
        self.assertEqual(
            "DUPLICATE_EVENT,MALFORMED_JSON,MISSING_REQUIRED_FIELD,INVALID_EVENT_TIME,FUTURE_EVENT_TIME",
            payload["reasons"],
        )
        self.assertTrue(payload["wrong_users_rejected"])
        self.assertIn("raw=0", payload["empty_raw_message"])

    def test_production_verifier_parses_only_exact_kafka_group_rows(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$lines = @(
    "",
    "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID",
    "chapter9-quality-production user_behavior_events 0 220 220 0 - - -"
)
$parsed = ConvertFrom-ProductionKafkaGroupDescription -Lines $lines `
    -ExpectedGroup "chapter9-quality-production" -ExpectedTopic "user_behavior_events" `
    -ExpectedPartitions @(0)
$wrongTopicRejected = $false
try {
    ConvertFrom-ProductionKafkaGroupDescription `
        -Lines @("chapter9-quality-production wrong-topic 0 220 220 0 - - -") `
        -ExpectedGroup "chapter9-quality-production" -ExpectedTopic "user_behavior_events" `
        -ExpectedPartitions @(0) | Out-Null
} catch { $wrongTopicRejected = $true }
[ordered]@{
    lag = $parsed.TotalLag
    partition = $parsed.Rows[0].Partition
    wrong_topic_rejected = $wrongTopicRejected
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(0, payload["lag"])
        self.assertEqual(0, payload["partition"])
        self.assertTrue(payload["wrong_topic_rejected"])

    def test_production_verifier_validates_jobs_overview_and_api_evidence(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$manifest = [pscustomobject]@{
    production_job_id = "0123456789abcdef0123456789abcdef"
    doris_job_id = "fedcba9876543210fedcba9876543210"
    iceberg_job_id = "33333333333333333333333333333333"
}
$jobs = [pscustomobject]@{ jobs = @(
    [pscustomobject]@{ jid = $manifest.production_job_id; name = "chapter-9-datastream-quality-production"; state = "RUNNING" },
    [pscustomobject]@{ jid = $manifest.doris_job_id; name = "chapter-9-doris-clean"; state = "RUNNING" },
    [pscustomobject]@{ jid = $manifest.iceberg_job_id; name = "chapter-9-iceberg-clean"; state = "RUNNING" },
    [pscustomobject]@{ jid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; name = "chapter-9-doris-clean"; state = "CANCELED" }
) }
$overview = [pscustomobject]@{ taskmanagers = 1; "slots-total" = 4; "slots-available" = 1; "jobs-running" = 3 }
$validatedJobs = Assert-ProductionJobsAndCapacity -Manifest $manifest -Jobs $jobs -Overview $overview
$batchStart = [DateTimeOffset]::Parse("2026-07-22T10:00:00Z")
$api = [pscustomobject]@{
    generated_at = "2026-07-22T10:00:01Z"
    analyzer = "rule_based"
    warnings = @()
    evidence = [pscustomobject]@{
        realtime = [pscustomobject]@{
            pv = 2; uv = 2; updated_at = "2026-07-22T10:00:01Z"
        }
        historical = [pscustomobject]@{
            event_count = 815; latest_event_time = "2026-07-22T09:59:00Z"
        }
    }
}
$validatedApi = Assert-ProductionApiEvidence -Response $api -BatchStart $batchStart -TrinoBaseline 813
$oldResponseRejected = $false
try {
    $api.generated_at = "2026-07-22T09:59:59Z"
    Assert-ProductionApiEvidence -Response $api -BatchStart $batchStart -TrinoBaseline 813 | Out-Null
} catch { $oldResponseRejected = $true }
[ordered]@{
    job_count = @($validatedJobs).Count
    generated_at = $validatedApi.GeneratedAt.ToString("o")
    analyzer = $validatedApi.Analyzer
    old_response_rejected = $oldResponseRejected
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(3, payload["job_count"])
        self.assertEqual("rule_based", payload["analyzer"])
        self.assertTrue(payload["old_response_rejected"])

    def test_production_verifier_classifies_control_only_lag_and_rejects_data(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$description = [pscustomobject]@{
    Group = "chapter9-doris-clean-v1"
    Topic = "user_behavior_clean"
    TotalLag = 1
    Rows = @([pscustomobject]@{
        Group = "chapter9-doris-clean-v1"
        Topic = "user_behavior_clean"
        Partition = 0
        CurrentOffset = 3
        LogEndOffset = 4
        Lag = 1
    })
}
$controlDump = @(
    "baseOffset: 3 lastOffset: 3 count: 1 isTransactional: true isControl: true",
    "| offset: 3 endTxnMarker: COMMIT coordinatorEpoch: 0"
)
$control = ConvertFrom-ProductionKafkaDumpLog -Lines $controlDump `
    -Partition 0 -StartOffset 3 -EndOffsetExclusive 4
$accepted = Assert-StableProductionKafkaLag -Before $description -After $description `
    -Classifications $control
$dataRejected = $false
try {
    $data = ConvertFrom-ProductionKafkaDumpLog `
        -Lines @("baseOffset: 3 lastOffset: 3 count: 1 isTransactional: true isControl: false") `
        -Partition 0 -StartOffset 3 -EndOffsetExclusive 4
    Assert-StableProductionKafkaLag -Before $description -After $description `
        -Classifications $data | Out-Null
} catch { $dataRejected = $_.Exception.Message -match "readable data" }
[ordered]@{
    cli_lag = $accepted.CliLag
    readable_lag = $accepted.ReadableDataLag
    offset = $accepted.Classifications[0].Offset
    kind = $accepted.Classifications[0].Kind
    marker = $accepted.Classifications[0].ControlType
    data_rejected = $dataRejected
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(1, payload["cli_lag"])
        self.assertEqual(0, payload["readable_lag"])
        self.assertEqual(3, payload["offset"])
        self.assertEqual("transaction_control", payload["kind"])
        self.assertEqual("COMMIT", payload["marker"])
        self.assertTrue(payload["data_rejected"])

    def test_production_verifier_classifies_abort_control_record(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$before = [pscustomobject]@{ Group = "g"; Topic = "t"; Rows = @([pscustomobject]@{ Partition = 0; CurrentOffset = 4; LogEndOffset = 5; Lag = 1 }); TotalLag = 1 }
$after = [pscustomobject]@{ Group = "g"; Topic = "t"; Rows = @([pscustomobject]@{ Partition = 0; CurrentOffset = 4; LogEndOffset = 5; Lag = 1 }); TotalLag = 1 }
$dump = @("baseOffset: 4 lastOffset: 4 isControl: true", "endTxnMarker: ABORT")
$classified = ConvertFrom-ProductionKafkaDumpLog -Lines $dump -Partition 0 -StartOffset 4 -EndOffsetExclusive 5
$proof = Assert-StableProductionKafkaLag -Before $before -After $after -Classifications $classified
[ordered]@{ kind = $proof.Classifications[0].Kind; control = $proof.Classifications[0].ControlType; readable = $proof.ReadableDataLag } | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("transaction_control", payload["kind"])
        self.assertEqual("ABORT", payload["control"])
        self.assertEqual(0, payload["readable"])

    def test_production_failed_evidence_preserves_post_api_stage_state(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$root = Join-Path ([IO.Path]::GetTempPath()) ("chapter9-failure-chain-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $root | Out-Null
try {
    $paths = Initialize-ProductionEvidenceRun -FinalPath (Join-Path $root "final.json") `
        -RunId "chapter9-production-0123456789abcdef0123456789abcdef" -DorisJobId "doris-job"
    $state = [pscustomobject]@{ RunId = "chapter9-production-0123456789abcdef0123456789abcdef"; RunPaths = $paths; DorisJobId = "doris-job"; InitialSent = $true; LateSent = $true }
    Write-ProductionCausalBaseline -RunState $state -Baseline @{ batch_start_utc = "2026-07-22T10:00:00Z"; doris = @{ pv = 1; uv = 1; pv_updated_at = "2026-07-22T09:59:00Z"; uv_updated_at = "2026-07-22T09:59:00Z" }; trino = @{ event_count = 815; distinct_event_id = 65 }; checkpoints = @{} }
    Write-ProductionStageEvidence -RunState $state -Stage "pre_api" -Evidence @{ output = @{ raw = 8 }; groups = @{ production = @{ readable_data_lag = 0 } }; checkpoints = @{ production = @{ completed = 2 } }; doris_final = @{ pv = 2; uv = 2 }; trino_final = @{ total = 817 } }
    Write-ProductionStageEvidence -RunState $state -Stage "api_gate" -Evidence @{ status = "failed"; error = "api timeout" }
    Write-ProductionRunFailure -RunPaths $paths -RunId $state.RunId -ErrorMessage "late checkpoint timeout" -EventsSent $true -DorisJobId "doris-job"
    $failed = Get-Content -Raw $paths.FailedPath | ConvertFrom-Json
    [ordered]@{ proof_source = $failed.proof_source; has_baseline = ($null -ne $failed.causal_baseline); has_stages = ($null -ne $failed.stage_evidence); has_api_gate = ($null -ne $failed.stage_evidence.stages.api_gate); api_error = $failed.stage_evidence.stages.api_gate.evidence.error } | ConvertTo-Json -Compress
} finally { Remove-Item -LiteralPath $root -Recurse -Force }
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("durable_run_baseline_and_stage_evidence", payload["proof_source"])
        self.assertTrue(payload["has_baseline"])
        self.assertTrue(payload["has_stages"])
        self.assertTrue(payload["has_api_gate"])
        self.assertEqual("api timeout", payload["api_error"])

    def test_production_verifier_requires_stable_complete_lag_classification(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
function New-Description([long]$End) {
    [pscustomobject]@{
        Group = "g"; Topic = "t"; TotalLag = $End - 3
        Rows = @([pscustomobject]@{
            Group = "g"; Topic = "t"; Partition = 0
            CurrentOffset = 3; LogEndOffset = $End; Lag = $End - 3
        })
    }
}
$classification = @([pscustomobject]@{
    Partition = 0; Offset = 3; Kind = "transaction_control"; ControlType = "COMMIT"
})
$unstableRejected = $false
try {
    Assert-StableProductionKafkaLag -Before (New-Description 4) -After (New-Description 5) `
        -Classifications $classification | Out-Null
} catch { $unstableRejected = $_.Exception.Message -match "changed during classification" }
$missingRejected = $false
try {
    Assert-StableProductionKafkaLag -Before (New-Description 5) -After (New-Description 5) `
        -Classifications $classification | Out-Null
} catch { $missingRejected = $_.Exception.Message -match "every lagged offset" }
$badArithmetic = New-Description 4
$badArithmetic.Rows[0].Lag = 0
$badArithmeticRejected = $false
try {
    Assert-StableProductionKafkaLag -Before $badArithmetic -After $badArithmetic `
        -Classifications $classification | Out-Null
} catch { $badArithmeticRejected = $_.Exception.Message -match "offset arithmetic" }
[ordered]@{
    unstable_rejected = $unstableRejected
    missing_rejected = $missingRejected
    bad_arithmetic_rejected = $badArithmeticRejected
} |
    ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["unstable_rejected"])
        self.assertTrue(payload["missing_rejected"])
        self.assertTrue(payload["bad_arithmetic_rejected"])

    def test_production_verifier_waits_for_late_operator_watermark_strictly_past_event(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$script:valueCalls = 0
function Invoke-RestMethod {
    param([string]$Uri, [int]$TimeoutSec)
    if ($Uri -match "/metrics\?get=") {
        $script:valueCalls++
        $value = if ($script:valueCalls -eq 1) { "100" } else { "101" }
        $response = @([pscustomobject]@{
            id = "0.route-late-events.currentInputWatermark"
            value = $value
        })
        Write-Output -NoEnumerate $response
        return
    }
    if ($Uri -match "/vertices/.+/metrics$") {
        $response = @(
            [pscustomobject]@{ id = "0.numRecordsIn" },
            [pscustomobject]@{ id = "0.route-late-events.currentInputWatermark" }
        )
        Write-Output -NoEnumerate $response
        return
    }
    return [pscustomobject]@{
        jid = "0123456789abcdef0123456789abcdef"
        name = "chapter-9-datastream-quality-production"
        state = "RUNNING"
        vertices = @([pscustomobject]@{
            id = "vertex-1"
            name = "source -> route-late-events -> late-sink"
        })
    }
}
$result = Wait-ProductionWatermarkPast `
    -JobId "0123456789abcdef0123456789abcdef" `
    -ExpectedName "chapter-9-datastream-quality-production" `
    -ThresholdEpochMs 100 -Attempts 2 -SleepSeconds 0
[ordered]@{
    calls = $script:valueCalls
    watermark = $result.Watermark
    metric = $result.MetricId
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(2, payload["calls"])
        self.assertEqual(101, payload["watermark"])
        self.assertEqual("0.route-late-events.currentInputWatermark", payload["metric"])

    def test_production_verifier_requires_doris_and_api_strict_freshness(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$batchStart = [DateTimeOffset]::Parse("2026-07-22T10:00:00Z")
$freshTime = [DateTimeOffset]::Parse("2026-07-22T10:00:01Z")
$freshDoris = Assert-ProductionDorisFreshness -Metrics ([pscustomobject]@{
    Pv = 2; Uv = 2; PvUpdatedAt = $freshTime; UvUpdatedAt = $freshTime
}) -BatchStart $batchStart
$staleDorisRejected = $false
try {
    Assert-ProductionDorisFreshness -Metrics ([pscustomobject]@{
        Pv = 2; Uv = 2; PvUpdatedAt = $batchStart; UvUpdatedAt = $batchStart
    }) -BatchStart $batchStart | Out-Null
} catch { $staleDorisRejected = $true }
$apiResponse = [pscustomobject]@{
    generated_at = "2026-07-22T10:00:01Z"; analyzer = "rule_based"; warnings = @()
    evidence = [pscustomobject]@{
        realtime = [pscustomobject]@{ pv = 2; uv = 2; updated_at = "2026-07-22T10:00:01" }
        historical = [pscustomobject]@{ event_count = 815; latest_event_time = "2026-07-22T09:59:00Z" }
    }
}
$freshApi = Assert-ProductionApiEvidence -Response $apiResponse `
    -BatchStart $batchStart -TrinoBaseline 813
$equalGeneratedRejected = $false
try {
    $apiResponse.generated_at = "2026-07-22T10:00:00Z"
    Assert-ProductionApiEvidence -Response $apiResponse `
        -BatchStart $batchStart -TrinoBaseline 813 | Out-Null
} catch { $equalGeneratedRejected = $true }
$apiResponse.generated_at = "2026-07-22T10:00:01Z"
$staleApiRejected = $false
try {
    $apiResponse.evidence.realtime.updated_at = "2026-07-22T10:00:00Z"
    Assert-ProductionApiEvidence -Response $apiResponse `
        -BatchStart $batchStart -TrinoBaseline 813 | Out-Null
} catch { $staleApiRejected = $true }
[ordered]@{
    doris_pv = $freshDoris.Pv
    stale_doris_rejected = $staleDorisRejected
    realtime_updated_at = $freshApi.RealtimeUpdatedAt.ToString("o")
    historical_latest_event_time = $freshApi.HistoricalLatestEventTime.ToString("o")
    equal_generated_rejected = $equalGeneratedRejected
    stale_api_rejected = $staleApiRejected
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(2, payload["doris_pv"])
        self.assertTrue(payload["stale_doris_rejected"])
        self.assertTrue(payload["realtime_updated_at"].startswith("2026-07-22T10:00:01"))
        self.assertTrue(payload["realtime_updated_at"].endswith("+00:00"))
        self.assertTrue(payload["historical_latest_event_time"].startswith("2026-07-22T09:59:00"))
        self.assertTrue(payload["equal_generated_rejected"])
        self.assertTrue(payload["stale_api_rejected"])

    def test_production_verifier_run_lock_and_evidence_lifecycle_are_safe(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$tempRoot = Join-Path ([IO.Path]::GetTempPath()) ("chapter9-run-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $tempRoot | Out-Null
try {
    $lockPath = Join-Path $tempRoot "production-verification.lock"
    $firstLock = Enter-ProductionRunLock -Path $lockPath
    $concurrentRejected = $false
    try { Enter-ProductionRunLock -Path $lockPath | Out-Null } catch { $concurrentRejected = $true }
    $firstLock.Dispose()

    $finalPath = Join-Path $tempRoot "production-verification.json"
    [IO.File]::WriteAllText($finalPath, '{"status":"success","run_id":"old-run"}')
    $run = Initialize-ProductionEvidenceRun -FinalPath $finalPath -RunId "new-run" `
        -DorisJobId "doris-job"
    $oldArchived = Test-Path $run.ArchivedPath
    $fixedRemoved = -not (Test-Path $finalPath)
    $inProgress = Get-Content -Raw $run.InProgressPath | ConvertFrom-Json
    Write-ProductionRunFailure -RunPaths $run -RunId "new-run" `
        -ErrorMessage "simulated" -EventsSent $true -DorisJobId "doris-job"
    $failed = Get-Content -Raw $run.FailedPath | ConvertFrom-Json
    $repeatJobRejected = $false
    try {
        Assert-ProductionRunAllowed -Directory $tempRoot -DorisJobId "doris-job"
    } catch { $repeatJobRejected = $true }

    $successRun = Initialize-ProductionEvidenceRun -FinalPath $finalPath -RunId "success-run" `
        -DorisJobId "success-job"
    Write-ProductionEvidenceAtomic -Evidence ([ordered]@{
        status = "success"; run_id = "success-run"
        proof = [ordered]@{
            causal_baseline_source = "durable_run_baseline"
            stage_evidence_source = "durable_stage_evidence"
            pre_api_stage = "output/groups/checkpoints/doris_final/trino_final"
        }
    }) -RunPaths $successRun
    $success = Get-Content -Raw $finalPath | ConvertFrom-Json

    $crashRun = Initialize-ProductionEvidenceRun -FinalPath $finalPath -RunId "crash-run" `
        -DorisJobId "crash-job"
    $state = [pscustomobject]@{
        InitialSent = $false; LateSent = $false
        RunId = "crash-run"; DorisJobId = "crash-job"; RunPaths = $crashRun
    }
    $script:sendCalls = 0
    try {
        Invoke-ProductionSendOnce -RunState $state -Stage Initial -Action {
            $script:sendCalls++
            throw "simulated process interruption"
        }
    } catch {}
    $interrupted = Get-Content -Raw $crashRun.InProgressPath | ConvertFrom-Json
    $interruptedJobRejected = $false
    try {
        Assert-ProductionRunAllowed -Directory $tempRoot -DorisJobId "crash-job"
    } catch { $interruptedJobRejected = $true }
    $repeatRejected = $false
    try {
        Invoke-ProductionSendOnce -RunState $state -Stage Initial -Action { $script:sendCalls++ }
    } catch { $repeatRejected = $true }

    [ordered]@{
        concurrent_rejected = $concurrentRejected
        old_archived = $oldArchived
        fixed_removed = $fixedRemoved
        in_progress_run = $inProgress.run_id
        failed_run = $failed.run_id
        failed_sent = $failed.events_sent
        repeat_job_rejected = $repeatJobRejected
        partial_is_per_run = $run.PartialPath -like "*new-run*"
        success_run = $success.run_id
        success_proof_source = $success.proof.causal_baseline_source + "/" + $success.proof.stage_evidence_source
        success_pre_api_stage = $success.proof.pre_api_stage
        success_partial_removed = -not (Test-Path $successRun.PartialPath)
        success_in_progress_removed = -not (Test-Path $successRun.InProgressPath)
        interrupted_sent = $interrupted.events_sent
        interrupted_job_rejected = $interruptedJobRejected
        send_calls = $script:sendCalls
        repeat_rejected = $repeatRejected
    } | ConvertTo-Json -Compress
} finally {
    Remove-Item -LiteralPath $tempRoot -Recurse -Force
}
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["concurrent_rejected"])
        self.assertTrue(payload["old_archived"])
        self.assertTrue(payload["fixed_removed"])
        self.assertEqual("new-run", payload["in_progress_run"])
        self.assertEqual("new-run", payload["failed_run"])
        self.assertTrue(payload["failed_sent"])
        self.assertTrue(payload["repeat_job_rejected"])
        self.assertTrue(payload["partial_is_per_run"])
        self.assertEqual("success-run", payload["success_run"])
        self.assertEqual("durable_run_baseline/durable_stage_evidence", payload["success_proof_source"])
        self.assertEqual("output/groups/checkpoints/doris_final/trino_final", payload["success_pre_api_stage"])
        self.assertTrue(payload["success_partial_removed"])
        self.assertTrue(payload["success_in_progress_removed"])
        self.assertTrue(payload["interrupted_sent"])
        self.assertTrue(payload["interrupted_job_rejected"])
        self.assertEqual(1, payload["send_calls"])
        self.assertTrue(payload["repeat_rejected"])

    def test_production_verifier_resume_requires_one_matching_failed_run(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$root = Join-Path ([IO.Path]::GetTempPath()) ("chapter9-resume-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $root | Out-Null
try {
    $runId = "chapter9-production-0123456789abcdef0123456789abcdef"
    $final = Join-Path $root "production-verification.json"
    $failed = Join-Path $root "production-verification.$runId.failed.json"
    [IO.File]::WriteAllText($failed, (@{
        status = "failed"; run_id = $runId; doris_job_id = "doris-job"; events_sent = $true
        failed_at_utc = "2026-07-22T10:00:00Z"
    } | ConvertTo-Json))
    $allowed = Assert-ProductionResumeAllowed -Directory $root -FinalPath $final `
        -RunId $runId -DorisJobId "doris-job"

    $unknownRejected = $false
    try {
        Assert-ProductionResumeAllowed -Directory $root -FinalPath $final `
            -RunId "chapter9-production-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" `
            -DorisJobId "doris-job" | Out-Null
    } catch { $unknownRejected = $true }
    $mismatchRejected = $false
    try {
        Assert-ProductionResumeAllowed -Directory $root -FinalPath $final `
            -RunId $runId -DorisJobId "other-job" | Out-Null
    } catch { $mismatchRejected = $true }

    $record = Get-Content -Raw $failed | ConvertFrom-Json
    $record.events_sent = $false
    [IO.File]::WriteAllText($failed, ($record | ConvertTo-Json))
    $unsentRejected = $false
    try {
        Assert-ProductionResumeAllowed -Directory $root -FinalPath $final `
            -RunId $runId -DorisJobId "doris-job" | Out-Null
    } catch { $unsentRejected = $true }
    $record.events_sent = $true
    [IO.File]::WriteAllText($failed, ($record | ConvertTo-Json))

    $inProgress = Join-Path $root "production-verification.$runId.resume-test.in-progress.json"
    [IO.File]::WriteAllText($inProgress, (@{ status = "resume_in_progress"; run_id = $runId } | ConvertTo-Json))
    $inProgressRejected = $false
    try {
        Assert-ProductionResumeAllowed -Directory $root -FinalPath $final `
            -RunId $runId -DorisJobId "doris-job" | Out-Null
    } catch { $inProgressRejected = $true }
    Remove-Item -LiteralPath $inProgress

    [IO.File]::WriteAllText($final, (@{ status = "success"; run_id = $runId } | ConvertTo-Json))
    $successRejected = $false
    try {
        Assert-ProductionResumeAllowed -Directory $root -FinalPath $final `
            -RunId $runId -DorisJobId "doris-job" | Out-Null
    } catch { $successRejected = $true }
    Remove-Item -LiteralPath $final

    $lock = Enter-ProductionRunLock -Path (Join-Path $root "production-verification.lock")
    $concurrentRejected = $false
    try {
        Enter-ProductionRunLock -Path (Join-Path $root "production-verification.lock") | Out-Null
    } catch { $concurrentRejected = $true }
    $lock.Dispose()

    $resumeFailed = Join-Path $root "production-verification.$runId.resume-attempt.failed.json"
    [IO.File]::WriteAllText($resumeFailed, (@{
        status = "failed"; run_id = $runId; doris_job_id = "doris-job"; events_sent = $true
        failed_at_utc = "2026-07-22T10:01:00Z"
    } | ConvertTo-Json))
    $chain = Assert-ProductionResumeAllowed -Directory $root -FinalPath $final `
        -RunId $runId -DorisJobId "doris-job"

    [ordered]@{
        source = $allowed.SourceFailedPath
        chain_count = $chain.ResumeChainPaths.Count
        chain_latest = $chain.SourceFailedPath
        unknown_rejected = $unknownRejected
        mismatch_rejected = $mismatchRejected
        unsent_rejected = $unsentRejected
        in_progress_rejected = $inProgressRejected
        success_rejected = $successRejected
        concurrent_rejected = $concurrentRejected
    } | ConvertTo-Json -Compress
} finally {
    Remove-Item -LiteralPath $root -Recurse -Force
}
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["source"].endswith(".failed.json"))
        self.assertEqual(2, payload["chain_count"])
        self.assertIn("resume-attempt.failed.json", payload["chain_latest"])
        self.assertTrue(payload["unknown_rejected"])
        self.assertTrue(payload["mismatch_rejected"])
        self.assertTrue(payload["unsent_rejected"])
        self.assertTrue(payload["in_progress_rejected"])
        self.assertTrue(payload["success_rejected"])
        self.assertTrue(payload["concurrent_rejected"])

    def test_production_verifier_resume_reconstructs_exact_partial_run_and_sends_only_late(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$runId = "chapter9-production-0123456789abcdef0123456789abcdef"
$userOne = "$runId-user-1"
$userTwo = "$runId-user-2"
$duplicate = New-ProductionEventJson -EventId "$runId-duplicate" -UserId $userOne `
    -EventTime "2026-07-22T10:00:00Z" -RunId $runId
$advancer = New-ProductionEventJson -EventId "$runId-advancer" -UserId $userTwo `
    -EventTime "2026-07-22T10:00:30Z" -RunId $runId
$missing = New-ProductionEventJson -EventId "$runId-missing" -UserId $userOne `
    -EventTime "2026-07-22T10:00:00Z" -RunId $runId | ConvertFrom-Json
$missing.PSObject.Properties.Remove("user_id")
$raw = @(
    $duplicate, $duplicate, ('{"event_id":"' + $runId + '-malformed"'),
    ($missing | ConvertTo-Json -Compress),
    (New-ProductionEventJson -EventId "$runId-invalid-time" -UserId $userOne `
        -EventTime "2026-07-22 10:00:00" -RunId $runId),
    (New-ProductionEventJson -EventId "$runId-future" -UserId $userOne `
        -EventTime "2026-07-22T10:10:00Z" -RunId $runId),
    $advancer
)
$clean = @(
    [pscustomobject]@{ event_id = "$runId-duplicate"; user_id = $userOne },
    [pscustomobject]@{ event_id = "$runId-advancer"; user_id = $userTwo }
)
$dlq = @(
    [pscustomobject]@{ event_id = "$runId-duplicate"; reason_code = "DUPLICATE_EVENT" },
    [pscustomobject]@{ event_id = "$runId-malformed"; reason_code = "MALFORMED_JSON" },
    [pscustomobject]@{ event_id = "$runId-missing"; reason_code = "MISSING_REQUIRED_FIELD" },
    [pscustomobject]@{ event_id = "$runId-invalid-time"; reason_code = "INVALID_EVENT_TIME" },
    [pscustomobject]@{ event_id = "$runId-future"; reason_code = "FUTURE_EVENT_TIME" }
)
$resume = Assert-ProductionResumeMatrix -RunId $runId -RawValues $raw `
    -CleanRecords $clean -DlqRecords $dlq -LateRecords @()
$missingRejected = $false
try {
    Assert-ProductionResumeMatrix -RunId $runId -RawValues @($raw | Select-Object -Skip 1) `
        -CleanRecords $clean -DlqRecords $dlq -LateRecords @() | Out-Null
} catch { $missingRejected = $true }

$root = Join-Path ([IO.Path]::GetTempPath()) ("chapter9-resume-send-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $root | Out-Null
try {
    $source = Join-Path $root "source.failed.json"
    $stages = [ordered]@{}
    foreach ($stage in @("output", "groups", "checkpoints", "doris_final", "trino_final", "pre_api")) {
        $stages[$stage] = [ordered]@{ evidence = [ordered]@{ retained = $true } }
    }
    [IO.File]::WriteAllText($source, ([ordered]@{
        status = "failed"; run_id = $runId
        stage_evidence = [ordered]@{
            schema_version = 1; proof_source = "durable_stage_evidence"
            run_id = $runId; stages = $stages
        }
    } | ConvertTo-Json -Depth 10))
    $paths = Initialize-ProductionResumeEvidenceRun `
        -FinalPath (Join-Path $root "production-verification.json") `
        -RunId $runId -DorisJobId "doris-job" -SourceFailedPath $source
    $state = [pscustomobject]@{
        InitialSent = $true; LateSent = $false; RunId = $runId
        DorisJobId = "doris-job"; RunPaths = $paths; LogicalRunResumed = $true
        SourceFailedEvidence = $source
    }
    $script:initialCalls = 0
    $initialRejected = $false
    try {
        Invoke-ProductionSendOnce -RunState $state -Stage Initial -Action { $script:initialCalls++ }
    } catch { $initialRejected = $true }
    $script:lateCalls = 0
    Invoke-ProductionSendOnce -RunState $state -Stage Late -Action { $script:lateCalls++ }
    $sourcePreserved = Test-Path $source
} finally {
    Remove-Item -LiteralPath $root -Recurse -Force
}
[ordered]@{
    raw = $resume.Raw
    clean = $resume.Clean
    dlq = $resume.Dlq
    late = $resume.Late
    batch_start = $resume.BatchStart.ToString("o")
    late_time = $resume.LateEventTime.ToString("o")
    late_id = ($resume.LateJson | ConvertFrom-Json).event_id
    missing_rejected = $missingRejected
    initial_rejected = $initialRejected
    initial_calls = $script:initialCalls
    late_calls = $script:lateCalls
    source_preserved = $sourcePreserved
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual((7, 2, 5, 0), tuple(payload[key] for key in ("raw", "clean", "dlq", "late")))
        self.assertTrue(payload["batch_start"].startswith("2026-07-22T10:00:00"))
        self.assertTrue(payload["late_time"].startswith("2026-07-22T09:59:30"))
        self.assertTrue(payload["late_id"].endswith("-late"))
        self.assertTrue(payload["missing_rejected"])
        self.assertTrue(payload["initial_rejected"])
        self.assertEqual(0, payload["initial_calls"])
        self.assertEqual(1, payload["late_calls"])
        self.assertTrue(payload["source_preserved"])

    def test_production_verifier_resume_derives_trino_baseline_with_explicit_basis(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$baseline = Get-ProductionRecoveredTrinoBaseline -CurrentEventCount 817 `
    -CurrentDistinctEventId 67 -RunEventCount 2 -RunDistinctEventId 2
$wrongRunRejected = $false
try {
    Get-ProductionRecoveredTrinoBaseline -CurrentEventCount 817 `
        -CurrentDistinctEventId 67 -RunEventCount 1 -RunDistinctEventId 1 | Out-Null
} catch { $wrongRunRejected = $true }
[ordered]@{
    event_count = $baseline.EventCount
    distinct_event_id = $baseline.DistinctEventId
    sample_type = $baseline.SampleType
    basis = $baseline.Basis
    wrong_run_rejected = $wrongRunRejected
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(815, payload["event_count"])
        self.assertEqual(65, payload["distinct_event_id"])
        self.assertEqual("derived_recovered", payload["sample_type"])
        self.assertIn("current total minus exact run", payload["basis"])
        self.assertTrue(payload["wrong_run_rejected"])

    def test_production_verifier_complete_resume_is_read_only_and_cannot_send_late_again(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_9_production.ps1") -FunctionsOnly
$runId = "chapter9-production-0123456789abcdef0123456789abcdef"
$userOne = "$runId-user-1"
$userTwo = "$runId-user-2"
$duplicate = New-ProductionEventJson -EventId "$runId-duplicate" -UserId $userOne `
    -EventTime "2026-07-22T10:00:00Z" -RunId $runId
$advancer = New-ProductionEventJson -EventId "$runId-advancer" -UserId $userTwo `
    -EventTime "2026-07-22T10:00:30Z" -RunId $runId
$late = New-ProductionEventJson -EventId "$runId-late" -UserId $userOne `
    -EventTime "2026-07-22T09:59:30Z" -RunId $runId
$missing = New-ProductionEventJson -EventId "$runId-missing" -UserId $userOne `
    -EventTime "2026-07-22T10:00:00Z" -RunId $runId | ConvertFrom-Json
$missing.PSObject.Properties.Remove("user_id")
$raw = @(
    $duplicate, $duplicate, ('{"event_id":"' + $runId + '-malformed"'),
    ($missing | ConvertTo-Json -Compress),
    (New-ProductionEventJson -EventId "$runId-invalid-time" -UserId $userOne `
        -EventTime "2026-07-22 10:00:00" -RunId $runId),
    (New-ProductionEventJson -EventId "$runId-future" -UserId $userOne `
        -EventTime "2026-07-22T10:10:00Z" -RunId $runId),
    $advancer, $late
)
$clean = @(
    [pscustomobject]@{ event_id = "$runId-duplicate"; user_id = $userOne },
    [pscustomobject]@{ event_id = "$runId-advancer"; user_id = $userTwo }
)
$dlq = @(
    [pscustomobject]@{ event_id = "$runId-duplicate"; reason_code = "DUPLICATE_EVENT" },
    [pscustomobject]@{ event_id = "$runId-malformed"; reason_code = "MALFORMED_JSON" },
    [pscustomobject]@{ event_id = "$runId-missing"; reason_code = "MISSING_REQUIRED_FIELD" },
    [pscustomobject]@{ event_id = "$runId-invalid-time"; reason_code = "INVALID_EVENT_TIME" },
    [pscustomobject]@{ event_id = "$runId-future"; reason_code = "FUTURE_EVENT_TIME" }
)
$lateRecord = [pscustomobject]@{ event_id = "$runId-late" }
$resume = Assert-ProductionResumeMatrix -RunId $runId -RawValues $raw `
    -CleanRecords $clean -DlqRecords $dlq -LateRecords @($lateRecord)
$resumeChain = @([pscustomobject]@{
    path = "production-verification.$runId.resume-0123456789abcdef0123456789abcdef.failed.json"
    failed_at_utc = "2026-07-22T10:05:00Z"
    events_sent = $true
    error = "Chapter 8 API production evidence did not converge. Last error: timeout"
})
function Get-ProductionCurrentWatermarkMetric {
    return [pscustomobject]@{
        VertexId = "vertex-1"
        MetricId = "0.route-late-events.currentInputWatermark"
        Watermark = [int64]::MinValue
    }
}
$proof = Get-ProductionReadOnlyWatermarkProof -ResumeState $resume `
    -ResumeChain $resumeChain -JobId "0123456789abcdef0123456789abcdef" `
    -ExpectedName "chapter-9-datastream-quality-production" `
    -ThresholdEpochMs $resume.LateEventTime.ToUnixTimeMilliseconds()
$partialRejected = $false
try {
    Get-ProductionReadOnlyWatermarkProof -ResumeState ([pscustomobject]@{
        ResumeAction = "send_late"; Raw = 7; Clean = 2; Dlq = 5; Late = 0
        LateOutputProof = $resume.LateOutputProof
    }) -ResumeChain $resumeChain -JobId "0123456789abcdef0123456789abcdef" `
        -ExpectedName "chapter-9-datastream-quality-production" `
        -ThresholdEpochMs $resume.LateEventTime.ToUnixTimeMilliseconds() | Out-Null
} catch { $partialRejected = $true }
$root = Join-Path ([IO.Path]::GetTempPath()) ("chapter9-read-only-resume-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $root | Out-Null
try {
    $source = Join-Path $root "source.failed.json"
    $stages = [ordered]@{}
    foreach ($stage in @("output", "groups", "checkpoints", "doris_final", "trino_final", "pre_api")) {
        $stages[$stage] = [ordered]@{ evidence = [ordered]@{ retained = $true } }
    }
    [IO.File]::WriteAllText($source, ([ordered]@{
        status = "failed"; run_id = $runId
        stage_evidence = [ordered]@{
            schema_version = 1; proof_source = "durable_stage_evidence"
            run_id = $runId; stages = $stages
        }
    } | ConvertTo-Json -Depth 10))
    $paths = Initialize-ProductionResumeEvidenceRun `
        -FinalPath (Join-Path $root "production-verification.json") `
        -RunId $runId -DorisJobId "doris-job" -SourceFailedPath $source
    $state = [pscustomobject]@{
        InitialSent = $true; LateSent = $false; RunId = $runId
        DorisJobId = "doris-job"; RunPaths = $paths; LogicalRunResumed = $true
        SourceFailedEvidence = $source
    }
    Set-ProductionResumeRunState -RunState $state -ResumeState $resume
    $script:lateCalls = 0
    $lateRejected = $false
    try {
        Invoke-ProductionSendOnce -RunState $state -Stage Late -Action { $script:lateCalls++ }
    } catch { $lateRejected = $true }
} finally {
    Remove-Item -LiteralPath $root -Recurse -Force
}
[ordered]@{
    action = $resume.ResumeAction
    raw = $resume.Raw
    late = $resume.Late
    late_sent_state = $state.LateSent
    late_rejected = $lateRejected
    late_calls = $script:lateCalls
    proof_source = $proof.WatermarkProofSource
    current_metric = $proof.CurrentMetric
    prior_gate_error = $proof.PriorGateEvidence.error
    late_topic_count = $resume.LateOutputProof.LateTopicCount
    late_clean_count = $resume.LateOutputProof.CleanCount
    late_dlq_count = $resume.LateOutputProof.DlqCount
    partial_rejected = $partialRejected
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("read_only_finalize", payload["action"])
        self.assertEqual((8, 1), (payload["raw"], payload["late"]))
        self.assertTrue(payload["late_sent_state"])
        self.assertTrue(payload["late_rejected"])
        self.assertEqual(0, payload["late_calls"])
        self.assertEqual("observed_late_output_after_prior_gate", payload["proof_source"])
        self.assertIsNone(payload["current_metric"])
        self.assertIn("Chapter 8 API production evidence did not converge", payload["prior_gate_error"])
        self.assertEqual((1, 0, 0), tuple(payload[key] for key in (
            "late_topic_count", "late_clean_count", "late_dlq_count"
        )))
        self.assertTrue(payload["partial_rejected"])
