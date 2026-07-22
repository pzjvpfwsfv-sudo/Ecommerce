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
        self.assertNotIn('"stop"', resume_block)
        self.assertNotIn("--mode", resume_block)
        self.assertNotIn("doris-clean.sql", resume_block)

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
    [pscustomobject]@{ jid = $dorisId; name = "chapter-9-doris-clean"; state = "RUNNING" }
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

    def test_cutover_uses_one_shared_finalizer_per_path_and_never_populates_partial_iceberg(self):
        text = (ROOT / "scripts/run_chapter_9_production_cutover.ps1").read_text(encoding="ascii")
        self.assertEqual(3, text.count("Complete-CutoverManifest"))
        self.assertNotIn('$manifest["iceberg_job_id"] = $icebergJobId', text)
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
        self.assertEqual(1, payload["job_calls"])
        self.assertEqual(0, payload["checkpoint_calls"])

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
        ):
            self.assertIn(marker, text)
        watermark_gate = text.index("$watermarkEvidence = Wait-ProductionWatermarkPast")
        late_send = text.index("Invoke-ProductionSendOnce -RunState $runState -Stage Late")
        post_late_checkpoint = text.index(
            "$productionFinalCheckpoint = Wait-NewProductionCheckpoint"
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
    [pscustomobject]@{ jid = $manifest.iceberg_job_id; name = "chapter-9-iceberg-clean"; state = "RUNNING" }
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
        return @([pscustomobject]@{
            id = "0.route-late-events.currentInputWatermark"
            value = $value
        })
    }
    if ($Uri -match "/vertices/.+/metrics$") {
        return @([pscustomobject]@{ id = "0.route-late-events.currentInputWatermark" })
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
        realtime = [pscustomobject]@{ pv = 2; uv = 2; updated_at = "2026-07-22T10:00:01Z" }
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
        self.assertTrue(payload["success_partial_removed"])
        self.assertTrue(payload["success_in_progress_removed"])
        self.assertTrue(payload["interrupted_sent"])
        self.assertTrue(payload["interrupted_job_rejected"])
        self.assertEqual(1, payload["send_calls"])
        self.assertTrue(payload["repeat_rejected"])
