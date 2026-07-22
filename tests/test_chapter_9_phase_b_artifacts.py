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
            "/dev/tcp/hive-metastore/9083",
            "/dev/tcp/doris-fe/8030",
            "/dev/tcp/minio/9000",
            "SHOW TABLES FROM analytics LIKE 'realtime_metrics'",
            "doris-preflight.sql",
            "iceberg-preflight.sql",
        ):
            self.assertIn(marker, text)
            self.assertLess(text.index(marker), text.index("[cutover] stopping shadow job"))

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
$offsets = ConvertFrom-KafkaOffsets @("user_behavior_events:1:43", "user_behavior_events:0:212")
$group = ConvertFrom-KafkaGroupDescription @(
    "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID",
    "chapter9-quality-shadow user_behavior_events 0 212 212 0 - - -"
)
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
