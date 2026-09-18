import json
from pathlib import Path
import shutil
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent.parent


class PowerShellTestCase(unittest.TestCase):
    def run_powershell(self, command: str) -> subprocess.CompletedProcess[str]:
        executable = shutil.which("pwsh") or shutil.which("powershell")
        if executable is None:
            self.skipTest("PowerShell is required for G2-C behavior coverage")
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


class G2cRunnerTests(PowerShellTestCase):
    def test_derives_isolated_names_and_renders_complete_sql(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_g2c_real_event_lakehouse.ps1") -FunctionsOnly
$deployment = Get-G2cDeployment -RunId "g2c-20260918a" -CleanTopic "" -LateTopic "" -TableName ""
$template = Get-Content -LiteralPath "jobs/sql/16_real_behavior_to_iceberg.sql.template" -Raw -Encoding UTF8
$sql = Render-G2cSql -Template $template -Deployment $deployment
Assert-G2cRenderedSql -Sql $sql -Deployment $deployment
[ordered]@{
    raw_topic = $deployment.RawTopic
    clean_topic = $deployment.CleanTopic
    late_topic = $deployment.LateTopic
    table_name = $deployment.TableName
    consumer_group = $deployment.ConsumerGroup
    pipeline_name = $deployment.PipelineName
    unresolved = [bool]($sql -match "__[A-Z0-9_]+__")
} | ConvertTo-Json -Compress
'''
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(
            "real_behavior_events_v1_g2c-20260918a", payload["raw_topic"]
        )
        self.assertEqual(
            "real_behavior_clean_v1_g2c-20260918a", payload["clean_topic"]
        )
        self.assertEqual(
            "real_behavior_late_v1_g2c-20260918a", payload["late_topic"]
        )
        self.assertEqual(
            "real_behavior_detail_v1_g2c_20260918a", payload["table_name"]
        )
        self.assertEqual(
            "graduation-g2c-g2c-20260918a", payload["consumer_group"]
        )
        self.assertEqual(
            "graduation-g2c-g2c-20260918a", payload["pipeline_name"]
        )
        self.assertFalse(payload["unresolved"])

    def test_rejects_unsafe_or_mismatched_resource_identity(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_g2c_real_event_lakehouse.ps1") -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
[ordered]@{
    uppercase_run = Test-Rejected { Get-G2cDeployment -RunId "G2C-bad" }
    quoted_run = Test-Rejected { Get-G2cDeployment -RunId "g2c'bad" }
    underscore_run = Test-Rejected { Get-G2cDeployment -RunId "g2c_bad" }
    long_run = Test-Rejected { Get-G2cDeployment -RunId ("a" * 33) }
    clean_mismatch = Test-Rejected {
        Get-G2cDeployment -RunId "g2c-safe" -CleanTopic "real_behavior_clean_v1_other"
    }
    late_mismatch = Test-Rejected {
        Get-G2cDeployment -RunId "g2c-safe" -LateTopic "real_behavior_late_v1_other"
    }
    table_escape = Test-Rejected {
        Get-G2cDeployment -RunId "g2c-safe" -TableName "other_table"
    }
} | ConvertTo-Json -Compress
'''
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(all(payload.values()), payload)


class G2cVerifierTests(PowerShellTestCase):
    def test_job_lock_ignores_terminal_history_but_rejects_active_duplicates(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_g2c_real_event_lakehouse.ps1") -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$pipeline = "graduation-g2c-g2c-repeat"
$historyAndRunning = @(
    [pscustomobject]@{ jid = "old-canceled"; name = $pipeline; state = "CANCELED" },
    [pscustomobject]@{ jid = "current-running"; name = $pipeline; state = "RUNNING" }
)
$selected = Get-G2cSingleRunningJob -PipelineName $pipeline -JobId "current-running" -Jobs $historyAndRunning
[ordered]@{
    selected_jid = $selected.jid
    no_running = Test-Rejected {
        Get-G2cSingleRunningJob -PipelineName $pipeline -JobId "old-canceled" -Jobs @($historyAndRunning[0])
    }
    wrong_job_id = Test-Rejected {
        Get-G2cSingleRunningJob -PipelineName $pipeline -JobId "another-job" -Jobs $historyAndRunning
    }
    duplicate_active = Test-Rejected {
        Get-G2cSingleRunningJob -PipelineName $pipeline -JobId "current-running" -Jobs @(
            $historyAndRunning[1],
            [pscustomobject]@{ jid = "second-running"; name = $pipeline; state = "RUNNING" }
        )
    }
    non_running_active = Test-Rejected {
        Get-G2cSingleRunningJob -PipelineName $pipeline -JobId "restarting" -Jobs @(
            [pscustomobject]@{ jid = "restarting"; name = $pipeline; state = "RESTARTING" }
        )
    }
} | ConvertTo-Json -Compress
'''
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("current-running", payload["selected_jid"])
        self.assertTrue(payload["no_running"])
        self.assertTrue(payload["duplicate_active"])
        self.assertTrue(payload["non_running_active"])

    def test_summary_and_quality_assertions_fail_closed(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_g2c_real_event_lakehouse.ps1") -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$validSummary = [pscustomobject]@{
    total_count = 12
    clean_count = 10
    late_count = 2
    distinct_event_count = 12
    invalid_route_count = 0
    event_route_sha256 = ("a" * 64)
}
$expectedDigest = "a" * 64
$validQuality = [pscustomobject]@{
    invalid_original_count = 0
    invalid_derived_count = 0
    invalid_landing_count = 0
    invalid_clean_diagnostic_count = 0
    invalid_late_diagnostic_count = 0
}
[ordered]@{
    valid_summary = -not (Test-Rejected {
        Assert-G2cSummary -Summary $validSummary -ExpectedCleanCount 10 -ExpectedLateCount 2 `
            -ExpectedEventRouteSha256 $expectedDigest
    })
    valid_quality = -not (Test-Rejected { Assert-G2cQuality -Quality $validQuality })
    route_sum = Test-Rejected {
        Assert-G2cSummary -Summary ([pscustomobject]@{
            total_count=13; clean_count=10; late_count=2; distinct_event_count=13; invalid_route_count=0;
            event_route_sha256=$expectedDigest
        }) -ExpectedCleanCount 10 -ExpectedLateCount 2 -ExpectedEventRouteSha256 $expectedDigest
    }
    duplicate_id = Test-Rejected {
        Assert-G2cSummary -Summary ([pscustomobject]@{
            total_count=12; clean_count=10; late_count=2; distinct_event_count=11; invalid_route_count=0;
            event_route_sha256=$expectedDigest
        }) -ExpectedCleanCount 10 -ExpectedLateCount 2 -ExpectedEventRouteSha256 $expectedDigest
    }
    invalid_route = Test-Rejected {
        Assert-G2cSummary -Summary ([pscustomobject]@{
            total_count=12; clean_count=10; late_count=2; distinct_event_count=12; invalid_route_count=1;
            event_route_sha256=$expectedDigest
        }) -ExpectedCleanCount 10 -ExpectedLateCount 2 -ExpectedEventRouteSha256 $expectedDigest
    }
    wrong_expected = Test-Rejected {
        Assert-G2cSummary -Summary $validSummary -ExpectedCleanCount 9 -ExpectedLateCount 2 `
            -ExpectedEventRouteSha256 $expectedDigest
    }
    wrong_event_route_digest = Test-Rejected {
        Assert-G2cSummary -Summary $validSummary -ExpectedCleanCount 10 -ExpectedLateCount 2 `
            -ExpectedEventRouteSha256 ("b" * 64)
    }
    invalid_quality = Test-Rejected {
        Assert-G2cQuality -Quality ([pscustomobject]@{
            invalid_original_count=0; invalid_derived_count=0; invalid_landing_count=0;
            invalid_clean_diagnostic_count=0; invalid_late_diagnostic_count=1
        })
    }
} | ConvertTo-Json -Compress
'''
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(all(payload.values()), payload)

    def test_trino_template_and_csv_results_are_strictly_consumable(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_g2c_real_event_lakehouse.ps1") -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$template = Get-Content -LiteralPath "jobs/sql/17_trino_verify_real_behavior.sql.template" -Raw -Encoding UTF8
$deployment = [pscustomobject]@{
    TableName = "real_behavior_detail_v1_g2c_safe"
    CleanTopic = "real_behavior_clean_v1_g2c-safe"
    LateTopic = "real_behavior_late_v1_g2c-safe"
    RawTopic = "real_behavior_events_v1_g2c-safe"
}
$sql = Render-G2cTrinoSql -Template $template -Deployment $deployment
$statements = @(Split-G2cSqlStatements -Sql $sql)
$summary = ConvertFrom-G2cCsvResult -Lines @(
    "total_count,clean_count,late_count,distinct_event_count,invalid_route_count,event_route_sha256",
    "12,10,2,12,0,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)
[ordered]@{
    statement_count = $statements.Count
    total = [long]$summary.total_count
    unresolved = [bool]($sql -match "__[A-Z0-9_]+__")
    snapshot_query = [bool]($sql -match 'real_behavior_detail_v1_g2c_safe\$snapshots')
    event_route_digest = [bool]($sql -match 'event_route_sha256')
    exact_topics = [bool](
        $sql.Contains("'real_behavior_clean_v1_g2c-safe'") -and
        $sql.Contains("'real_behavior_late_v1_g2c-safe'") -and
        $sql.Contains("'real_behavior_events_v1_g2c-safe'")
    )
    unsafe_table = Test-Rejected {
        $bad = $deployment.PSObject.Copy()
        $bad.TableName = 'real_behavior_detail_v1"; DROP TABLE x; --'
        Render-G2cTrinoSql -Template $template -Deployment $bad
    }
    unresolved_template = Test-Rejected {
        Render-G2cTrinoSql -Template ($template + "`n__UNKNOWN__") -Deployment $deployment
    }
    no_row = Test-Rejected { ConvertFrom-G2cCsvResult -Lines @("total_count") }
    multiple_rows = Test-Rejected {
        ConvertFrom-G2cCsvResult -Lines @("total_count", "1", "2")
    }
} | ConvertTo-Json -Compress
'''
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(3, payload["statement_count"])
        self.assertEqual(12, payload["total"])
        self.assertFalse(payload["unresolved"])
        self.assertTrue(payload["snapshot_query"])
        self.assertTrue(payload["event_route_digest"])
        self.assertTrue(payload["exact_topics"])
        self.assertTrue(payload["unsafe_table"])
        self.assertTrue(payload["unresolved_template"])
        self.assertTrue(payload["no_row"])
        self.assertTrue(payload["multiple_rows"])

    def test_snapshot_and_checkpoint_evidence_are_required(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_g2c_real_event_lakehouse.ps1") -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$completed = [pscustomobject]@{
    counts = [pscustomobject]@{ completed = 2 }
    latest = [pscustomobject]@{
        completed = [pscustomobject]@{ id = 7; status = "COMPLETED"; latest_ack_timestamp = 1789700000000 }
    }
}
$notBefore = [DateTimeOffset]::Parse("2026-09-18T11:59:00Z").ToUnixTimeMilliseconds()
[ordered]@{
    snapshot_present = -not (Test-Rejected {
        Assert-G2cSnapshot -Snapshot ([pscustomobject]@{
            snapshot_count = 1
            latest_snapshot_id = 918273645
            latest_snapshot_committed_at = "2026-09-18 12:00:00.000 UTC"
        }) -NotBeforeEpochMs $notBefore
    })
    snapshot_missing = Test-Rejected {
        Assert-G2cSnapshot -Snapshot ([pscustomobject]@{ snapshot_count = 0 }) `
            -NotBeforeEpochMs $notBefore
    }
    snapshot_identity_missing = Test-Rejected {
        Assert-G2cSnapshot -Snapshot ([pscustomobject]@{
            snapshot_count = 1
            latest_snapshot_id = $null
            latest_snapshot_committed_at = $null
        }) -NotBeforeEpochMs $notBefore
    }
    snapshot_stale = Test-Rejected {
        Assert-G2cSnapshot -Snapshot ([pscustomobject]@{
            snapshot_count = 1
            latest_snapshot_id = 918273645
            latest_snapshot_committed_at = "2026-09-18 11:58:59.000 UTC"
        }) -NotBeforeEpochMs $notBefore
    }
    checkpoint_completed = -not (Test-Rejected {
        Get-G2cCheckpointEvidence -Checkpoints $completed
    })
    checkpoint_missing = Test-Rejected {
        Get-G2cCheckpointEvidence -Checkpoints ([pscustomobject]@{
            counts = [pscustomobject]@{ completed = 0 }
            latest = [pscustomobject]@{ completed = $null }
        })
    }
    checkpoint_bad_status = Test-Rejected {
        Get-G2cCheckpointEvidence -Checkpoints ([pscustomobject]@{
            counts = [pscustomobject]@{ completed = 1 }
            latest = [pscustomobject]@{ completed = [pscustomobject]@{ id = 8; status = "FAILED" } }
        })
    }
} | ConvertTo-Json -Compress
'''
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(all(payload.values()), payload)


class G2cRunnerSafetyTests(PowerShellTestCase):
    def test_submission_rejects_prior_job_history_or_nonempty_target(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_g2c_real_event_lakehouse.ps1") -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$pipeline = "graduation-g2c-g2c-once"
[ordered]@{
    fresh_empty = -not (Test-Rejected {
        Assert-G2cSubmissionSafety -PipelineName $pipeline -Jobs @() -TargetRowCount 0
    })
    terminal_history = Test-Rejected {
        Assert-G2cSubmissionSafety -PipelineName $pipeline -Jobs @(
            [pscustomobject]@{ jid = "old"; name = $pipeline; state = "CANCELED" }
        ) -TargetRowCount 0
    }
    active_job = Test-Rejected {
        Assert-G2cSubmissionSafety -PipelineName $pipeline -Jobs @(
            [pscustomobject]@{ jid = "current"; name = $pipeline; state = "RUNNING" }
        ) -TargetRowCount 0
    }
    nonempty_target = Test-Rejected {
        Assert-G2cSubmissionSafety -PipelineName $pipeline -Jobs @() -TargetRowCount 1
    }
} | ConvertTo-Json -Compress
'''
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(all(payload.values()), payload)

    def test_running_job_lookup_ignores_terminal_history(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_g2c_real_event_lakehouse.ps1") -FunctionsOnly
$pipeline = "graduation-g2c-g2c-repeat"
$matches = @(Get-G2cActivePipelineJobs -PipelineName $pipeline -Jobs @(
    [pscustomobject]@{ jid = "old-canceled"; name = $pipeline; state = "CANCELED" },
    [pscustomobject]@{ jid = "old-finished"; name = $pipeline; state = "FINISHED" },
    [pscustomobject]@{ jid = "current-running"; name = $pipeline; state = "RUNNING" },
    [pscustomobject]@{ jid = "other-running"; name = "another-pipeline"; state = "RUNNING" }
))
[ordered]@{
    count = $matches.Count
    jid = $matches[0].jid
    state = $matches[0].state
} | ConvertTo-Json -Compress
'''
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(1, payload["count"])
        self.assertEqual("current-running", payload["jid"])
        self.assertEqual("RUNNING", payload["state"])

    def test_uses_migrated_docker_cli_as_process_local_fallback(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_g2c_real_event_lakehouse.ps1") -FunctionsOnly
$env:PATH = "$env:SystemRoot\System32"
$resolved = Enable-G2cDockerCli -FallbackBin "D:\DockerProgram\Docker\resources\bin"
[ordered]@{
    resolved = $resolved
    command_source = (Get-Command docker -ErrorAction Stop).Source
    path_contains_fallback = $env:PATH.StartsWith("D:\DockerProgram\Docker\resources\bin;")
} | ConvertTo-Json -Compress
'''
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(
            r"D:\DockerProgram\Docker\resources\bin\docker.exe".lower(),
            payload["resolved"].lower(),
        )
        self.assertEqual(payload["resolved"].lower(), payload["command_source"].lower())
        self.assertTrue(payload["path_contains_fallback"])

    def test_rendering_fails_closed_when_sql_contract_is_mutated(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_g2c_real_event_lakehouse.ps1") -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$deployment = Get-G2cDeployment -RunId "g2c-safe"
$template = Get-Content -LiteralPath "jobs/sql/16_real_behavior_to_iceberg.sql.template" -Raw -Encoding UTF8
$sql = Render-G2cSql -Template $template -Deployment $deployment
$target = "lakehouse.analytics.$($deployment.TableName)"
[ordered]@{
    valid_contract = -not (Test-Rejected { Assert-G2cRenderedSql -Sql $sql -Deployment $deployment })
    unresolved = Test-Rejected {
        Render-G2cSql -Template ($template + "`n__UNKNOWN_PLACEHOLDER__") -Deployment $deployment
    }
    second_writer = Test-Rejected {
        Assert-G2cRenderedSql -Sql ($sql + "`nINSERT INTO $target`nSELECT * FROM real_behavior_normalized;") -Deployment $deployment
    }
    dlq_reference = Test-Rejected {
        Assert-G2cRenderedSql -Sql ($sql.Replace($deployment.LateTopic, "real_behavior_dlq_v1_g2c-safe")) -Deployment $deployment
    }
    missing_union = Test-Rejected {
        Assert-G2cRenderedSql -Sql ($sql.Replace("UNION ALL", "UNION")) -Deployment $deployment
    }
    permissive_json = Test-Rejected {
        Assert-G2cRenderedSql -Sql ($sql.Replace("'json.ignore-parse-errors' = 'false'", "'json.ignore-parse-errors' = 'true'")) -Deployment $deployment
    }
} | ConvertTo-Json -Compress
'''
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(all(payload.values()), payload)

    def test_plan_only_renders_identity_without_docker(self):
        executable = shutil.which("pwsh") or shutil.which("powershell")
        if executable is None:
            self.skipTest("PowerShell is required for G2-C behavior coverage")
        result = subprocess.run(
            [
                executable,
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-File",
                str(ROOT / "scripts" / "run_g2c_real_event_lakehouse.ps1"),
                "-RunId",
                "g2c-plan",
                "-PlanOnly",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
            check=False,
            env={"PATH": str(Path(executable).parent)},
        )
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("planned", payload["status"])
        self.assertEqual("real_behavior_clean_v1_g2c-plan", payload["clean_topic"])
        self.assertEqual("real_behavior_late_v1_g2c-plan", payload["late_topic"])
        self.assertEqual("real_behavior_detail_v1_g2c_plan", payload["table_name"])
        self.assertRegex(payload["sql_sha256"], r"^[0-9a-f]{64}$")

    def test_topic_preflight_requires_both_exact_sources(self):
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/run_g2c_real_event_lakehouse.ps1") -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$deployment = Get-G2cDeployment -RunId "g2c-topics"
[ordered]@{
    exact_topics = -not (Test-Rejected {
        Assert-G2cSourceTopics -Deployment $deployment -AvailableTopics @(
            "unrelated", $deployment.CleanTopic, $deployment.LateTopic
        )
    })
    missing_clean = Test-Rejected {
        Assert-G2cSourceTopics -Deployment $deployment -AvailableTopics @($deployment.LateTopic)
    }
    missing_late = Test-Rejected {
        Assert-G2cSourceTopics -Deployment $deployment -AvailableTopics @($deployment.CleanTopic)
    }
    empty = Test-Rejected {
        Assert-G2cSourceTopics -Deployment $deployment -AvailableTopics @()
    }
} | ConvertTo-Json -Compress
'''
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(all(payload.values()), payload)


if __name__ == "__main__":
    unittest.main()
