import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = ROOT / "infra" / "docker-compose.yml"
ENV_FILE = ROOT / "infra" / ".env.example"
BOOTSTRAP = ROOT / "scripts" / "bootstrap_chapter_10_5.ps1"


class Chapter105ColdStartVerifierTest(unittest.TestCase):
    def _powershell(self, body, check=True):
        body = '[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false);' + body
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", body], cwd=ROOT,
            capture_output=True, text=True, encoding="utf-8", errors="replace", check=False,
            timeout=30,
        )
        if check and result.returncode != 0:
            self.fail(result.stderr or result.stdout)
        return result

    def _payload(self, body):
        result = self._powershell(body)
        return json.loads(result.stdout.strip().splitlines()[-1])

    def test_real_bootstrap_entry_accepts_empty_catalog_switch(self):
        result = subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(BOOTSTRAP),
             "-EnvFile", "missing.env", "-ReportPath", "tmp/chapter-10-5/bootstrap-entry-test.json",
             "-ComposeProjectName", "chapter105-acceptance-111111111111", "-IsolatedAcceptance",
             "-InitializeEmptyCatalog"],
            cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False,
            timeout=30,
        )
        combined = result.stdout + result.stderr
        self.assertNotEqual(0, result.returncode)
        self.assertNotIn("InitializeEmptyCatalog", combined)
        self.assertIn("bootstrap failed", combined.lower())

    def test_functions_only_loads_without_native_command_side_effects(self):
        payload = self._payload(r'''
$originalPath=$env:PATH
try {
  $env:PATH=''
  . "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
  [ordered]@{loaded=($null -ne (Get-Command Invoke-Chapter105ColdStartVerification -ErrorAction SilentlyContinue))}|ConvertTo-Json -Compress
} finally { $env:PATH=$originalPath }
''')
        self.assertTrue(payload["loaded"])

    def test_isolation_context_couples_project_run_env_report_minio_and_state(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tmp" / "chapter-10-5") as directory:
            temp = Path(directory)
            run_id = "123456789abc"
            run_root = temp / "acceptance" / run_id
            run_root.mkdir(parents=True)
            env_path = run_root / "isolated.env"
            report_path = run_root / "bootstrap-first.json"
            minio_path = run_root / "minio-data"
            env_path.write_text(
                f"PROJECT_NAME=chapter105-acceptance-{run_id}\nMINIO_DATA_DIR={minio_path}\n",
                encoding="utf-8",
            )
            payload = self._payload(f'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
$ok = Resolve-Chapter105IsolationContext -RepositoryRoot "{temp}" `
    -ProjectName "chapter105-acceptance-{run_id}" -EnvPath "{env_path}" `
    -ReportPath "{report_path}" -MinioDataPath "{minio_path}" -InitializeEmptyCatalog
$wrong = $null
try {{ Resolve-Chapter105IsolationContext -RepositoryRoot "{temp}" `
    -ProjectName "chapter105-acceptance-aaaaaaaaaaaa" -EnvPath "{env_path}" `
    -ReportPath "{report_path}" -MinioDataPath "{minio_path}" }} catch {{ $wrong = $_.Exception.Message }}
[ordered]@{{ run_id=$ok.run_id; state_root=$ok.state_root; wrong=$wrong }} | ConvertTo-Json -Compress
''')
            self.assertEqual(run_id, payload["run_id"])
            self.assertEqual(str(run_root), payload["state_root"])
            self.assertEqual("Isolated acceptance identity is unsafe.", payload["wrong"])

    def test_isolation_context_rejects_env_content_and_unrecognized_report_name(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tmp" / "chapter-10-5") as directory:
            temp = Path(directory)
            run_id = "123456789abc"
            run_root = temp / "acceptance" / run_id
            run_root.mkdir(parents=True)
            env_path = run_root / "isolated.env"
            minio_path = run_root / "minio-data"
            env_path.write_text(
                f"PROJECT_NAME=ecommerce-lakehouse-ai\nMINIO_DATA_DIR={minio_path}\n",
                encoding="utf-8",
            )
            payload = self._payload(f'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
$envCaught=$null; $reportCaught=$null
try {{ Resolve-Chapter105IsolationContext -RepositoryRoot "{temp}" `
    -ProjectName "chapter105-acceptance-{run_id}" -EnvPath "{env_path}" `
    -ReportPath "{run_root / 'bootstrap-first.json'}" -MinioDataPath "{minio_path}" }} catch {{ $envCaught=$_.Exception.Message }}
Set-Content -LiteralPath "{env_path}" -Value @("PROJECT_NAME=chapter105-acceptance-{run_id}","MINIO_DATA_DIR={minio_path}") -Encoding UTF8
try {{ Resolve-Chapter105IsolationContext -RepositoryRoot "{temp}" `
    -ProjectName "chapter105-acceptance-{run_id}" -EnvPath "{env_path}" `
    -ReportPath "{run_root / 'arbitrary.json'}" -MinioDataPath "{minio_path}" }} catch {{ $reportCaught=$_.Exception.Message }}
[ordered]@{{ env=$envCaught; report=$reportCaught }} | ConvertTo-Json -Compress
''')
            self.assertEqual("Isolated acceptance identity is unsafe.", payload["env"])
            self.assertEqual("Isolated acceptance identity is unsafe.", payload["report"])

    def test_empty_catalog_initialization_is_rejected_outside_isolation(self):
        payload = self._payload(r'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
$caught = $null
try { Assert-Chapter105EmptyCatalogGate -InitializeEmptyCatalog -IsolatedAcceptance:$false } catch { $caught = $_.Exception.Message }
[ordered]@{ error=$caught } | ConvertTo-Json -Compress
''')
        self.assertEqual("Empty catalog initialization requires isolated acceptance.", payload["error"])

    def test_checkpoint_parser_reads_latest_completed_and_enforces_freshness(self):
        payload = self._payload(r'''
. "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
$now = [DateTimeOffset]::Parse("2026-08-27T10:00:00Z")
$overview = '{"jobs":[{"jid":"11111111111111111111111111111111","name":"chapter-9-datastream-quality-production","state":"RUNNING"}]}' | ConvertFrom-Json
$fresh = '{"counts":{"completed":4},"latest":{"completed":{"id":4,"status":"COMPLETED","latest_ack_timestamp":1787824790000,"external_path":"s3a://flink-state/checkpoints/chapter-9/111/chk-4"}}}' | ConvertFrom-Json
$stale = '{"counts":{"completed":4},"latest":{"completed":{"id":4,"status":"COMPLETED","latest_ack_timestamp":1787820000000,"external_path":"s3a://flink-state/checkpoints/chapter-9/111/chk-4"}}}' | ConvertFrom-Json
$evidence = Get-AcceptanceCheckpointEvidence -Overview $overview -Checkpoints $fresh -Now $now -MaxAgeSeconds 120
$caught = $null
try { Get-AcceptanceCheckpointEvidence -Overview $overview -Checkpoints $stale -Now $now -MaxAgeSeconds 120 } catch { $caught = $_.Exception.Message }
[ordered]@{ id=$evidence.checkpoint_id; path=$evidence.external_path; error=$caught } | ConvertTo-Json -Compress
''')
        self.assertEqual(4, payload["id"])
        self.assertEqual("s3a://flink-state/checkpoints/chapter-9/111/chk-4", payload["path"])
        self.assertEqual("Isolated Flink checkpoint is not fresh.", payload["error"])

    def test_restart_transition_is_run_local_and_requires_matching_result_job(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tmp" / "chapter-10-5") as directory:
            run_root = Path(directory) / "acceptance" / "abcdef123456"
            state_dir = run_root / "tmp" / "chapter-9"
            state_dir.mkdir(parents=True)
            state_path = state_dir / "production-submit-state.json"
            state_path.write_text(json.dumps({
                "schema_version": 1, "kind": "chapter9_production_submit", "status": "result",
                "intent": {"operation": "submit_chapter9_production", "job_name": "chapter-9-datastream-quality-production", "created_at_utc": "2026-08-27T00:00:00Z"},
                "result": {"status": "result", "job_id": "1" * 32, "job_name": "chapter-9-datastream-quality-production", "completed_at_utc": "2026-08-27T00:01:00Z"},
            }), encoding="utf-8")
            payload = self._payload(f'''
. "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
$transition = Set-AcceptanceRestartRecoveryState -StateRoot "{run_root}" -RunId "abcdef123456" `
    -ExpectedJobId "{'1' * 32}" -JobName "chapter-9-datastream-quality-production" `
    -SavepointPath "s3a://flink-state/savepoints/chapter-9/savepoint-abc"
$saved = Get-Content -Raw "{state_path}" | ConvertFrom-Json
$wrong = $null
try {{ Set-AcceptanceRestartRecoveryState -StateRoot "{run_root}" -RunId "abcdef123456" `
    -ExpectedJobId "{'2' * 32}" -JobName "chapter-9-datastream-quality-production" `
    -SavepointPath "s3a://flink-state/savepoints/chapter-9/savepoint-def" }} catch {{ $wrong=$_.Exception.Message }}
[ordered]@{{ status=$saved.status; recovery=$transition.recovery_path; wrong=$wrong }} | ConvertTo-Json -Compress
''')
            self.assertEqual("not_started", payload["status"])
            self.assertEqual(str(state_dir / "acceptance-restart-recovery.json"), payload["recovery"])
            self.assertEqual("Isolated restart recovery state is unsafe.", payload["wrong"])

    def test_recursive_cleanup_path_rejects_junction_and_run_id_mismatch(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tmp" / "chapter-10-5") as directory:
            base = Path(directory)
            acceptance, target = base / "acceptance", base / "target"
            acceptance.mkdir(); target.mkdir()
            payload = self._payload(f'''
. "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
$junction = Join-Path "{acceptance}" "abcdef123456"
New-Item -ItemType Junction -Path $junction -Target "{target}" | Out-Null
$junctionError=$null; $idError=$null
try {{ Assert-AcceptanceRunRoot -AcceptanceRoot "{acceptance}" -RunRoot $junction -RunId "abcdef123456" }} catch {{ $junctionError=$_.Exception.Message }}
try {{ Assert-AcceptanceRunRoot -AcceptanceRoot "{acceptance}" -RunRoot (Join-Path "{acceptance}" "abcdef123456") -RunId "111111111111" }} catch {{ $idError=$_.Exception.Message }}
[ordered]@{{ junction=$junctionError; id=$idError }} | ConvertTo-Json -Compress
''')
            self.assertEqual("Acceptance cleanup path is unsafe.", payload["junction"])
            self.assertEqual("Acceptance cleanup path is unsafe.", payload["id"])

    def test_absolute_deadline_is_shared_and_expiry_fails_closed(self):
        payload = self._payload(r'''
. "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
$deadline=[DateTimeOffset]::UtcNow.AddSeconds(10)
$first=Get-AcceptanceRemainingMilliseconds -Deadline $deadline
$second=Get-AcceptanceRemainingMilliseconds -Deadline $deadline
$caught=$null
try { Get-AcceptanceRemainingMilliseconds -Deadline ([DateTimeOffset]::UtcNow.AddSeconds(-1)) } catch { $caught=$_.Exception.Message }
[ordered]@{ first=$first; second=$second; error=$caught } | ConvertTo-Json -Compress
''')
        self.assertGreater(payload["first"], 0)
        self.assertLessEqual(payload["second"], payload["first"])
        self.assertEqual("Chapter 10.5 acceptance deadline expired.", payload["error"])

    def test_native_process_preserves_argv_and_kills_at_absolute_deadline(self):
        payload = self._payload(r'''
. "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
$output=@(Invoke-AcceptanceProcess -FilePath "powershell" -Arguments @("-NoProfile","-Command","Write-Output 'native argument ok'") -Deadline ([DateTimeOffset]::UtcNow.AddSeconds(10)) -FailureMessage "native failed")
$caught=$null
try { Invoke-AcceptanceProcess -FilePath "powershell" -Arguments @("-NoProfile","-Command","Start-Sleep -Seconds 5") -Deadline ([DateTimeOffset]::UtcNow.AddMilliseconds(300)) -FailureMessage "native failed" } catch { $caught=$_.Exception.Message }
[ordered]@{output=$output;caught=$caught}|ConvertTo-Json -Compress
''')
        self.assertEqual(["native argument ok"], payload["output"])
        self.assertEqual("Chapter 10.5 acceptance deadline expired.", payload["caught"])

    def test_frozen_environment_overrides_ambient_values_and_restores_them(self):
        payload = self._payload(r'''
. "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
[Environment]::SetEnvironmentVariable("PROJECT_NAME", "ambient-default", "Process")
$previous=Set-AcceptanceEnvironment -Values ([ordered]@{ PROJECT_NAME="chapter105-acceptance-123456789abc"; API_PORT="25001" })
$during=[Environment]::GetEnvironmentVariable("PROJECT_NAME", "Process")
Restore-AcceptanceEnvironment -Previous $previous
$after=[Environment]::GetEnvironmentVariable("PROJECT_NAME", "Process")
[ordered]@{ during=$during; after=$after } | ConvertTo-Json -Compress
''')
        self.assertEqual("chapter105-acceptance-123456789abc", payload["during"])
        self.assertEqual("ambient-default", payload["after"])

    def test_rendered_compose_preflight_rejects_redirected_identity(self):
        payload = self._payload(r'''
. "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
$valid='{"name":"chapter105-acceptance-123456789abc","networks":{"platform-net":{"name":"chapter105-acceptance-123456789abc-net"}},"services":{"minio":{"container_name":"chapter105-acceptance-123456789abc-minio","volumes":[{"type":"bind","source":"C:\\safe\\minio-data","target":"/data"}]},"api":{"environment":{"DORIS_PORT":"9030"}}}}'
$ok=Assert-AcceptanceRenderedConfig -ConfigJson $valid -ProjectName "chapter105-acceptance-123456789abc" -MinioContainerName "chapter105-acceptance-123456789abc-minio" -MinioDataPath "C:\safe\minio-data"
$bad=$valid.Replace("chapter105-acceptance-123456789abc-net", "ecommerce-lakehouse-ai-net")
$caught=$null
try { Assert-AcceptanceRenderedConfig -ConfigJson $bad -ProjectName "chapter105-acceptance-123456789abc" -MinioContainerName "chapter105-acceptance-123456789abc-minio" -MinioDataPath "C:\safe\minio-data" } catch { $caught=$_.Exception.Message }
[ordered]@{ ok=$ok; error=$caught } | ConvertTo-Json -Compress
''')
        self.assertEqual("validated", payload["ok"])
        self.assertEqual("Rendered Compose isolation is unsafe.", payload["error"])

    def test_rendered_compose_preflight_validates_all_isolation_values(self):
        payload = self._payload(r'''
. "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
$project="chapter105-acceptance-123456789abc"
$expected=[ordered]@{
  PROJECT_NAME=$project; MINIO_CONTAINER_NAME="$project-minio"; MINIO_DATA_DIR="C:\safe\minio-data"
  DORIS_INTERNAL_QUERY_PORT="9030"; DORIS_NETWORK_SUBNET="172.30.40.0/24"; DORIS_NETWORK_IP_RANGE="172.30.40.128/25"
  DORIS_FE_STATIC_IP="172.30.40.2"; DORIS_BE_STATIC_IP="172.30.40.3"; API_PORT="25001"; DORIS_FE_QUERY_PORT="25002"
}
$config=[ordered]@{
  name=$project
  networks=[ordered]@{
    'platform-net'=[ordered]@{name="$project-net"}
    custom_network=[ordered]@{ipam=[ordered]@{config=@([ordered]@{subnet="172.30.40.0/24";ip_range="172.30.40.128/25"})}}
  }
  services=[ordered]@{
    minio=[ordered]@{container_name="$project-minio";volumes=@([ordered]@{type="bind";source="C:\safe\minio-data";target="/data"})}
    api=[ordered]@{container_name="$project-api";environment=[ordered]@{DORIS_PORT="9030"};ports=@([ordered]@{target=8000;published="25001"})}
    'doris-fe'=[ordered]@{container_name="$project-doris-fe";ports=@([ordered]@{target=9030;published="25002"});networks=[ordered]@{custom_network=[ordered]@{ipv4_address="172.30.40.2"}}}
    'doris-be'=[ordered]@{container_name="$project-doris-be";networks=[ordered]@{custom_network=[ordered]@{ipv4_address="172.30.40.3"}}}
  }
}
$ok=Assert-AcceptanceRenderedConfig -ConfigJson ($config|ConvertTo-Json -Depth 20 -Compress) -ExpectedValues $expected
$config.services.api.ports[0].published="8000"
$caught=$null
try { Assert-AcceptanceRenderedConfig -ConfigJson ($config|ConvertTo-Json -Depth 20 -Compress) -ExpectedValues $expected } catch { $caught=$_.Exception.Message }
[ordered]@{ok=$ok;caught=$caught}|ConvertTo-Json -Compress
''')
        self.assertEqual("validated", payload["ok"])
        self.assertEqual("Rendered Compose isolation is unsafe.", payload["caught"])

    def test_real_compose_render_passes_full_isolation_preflight(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tmp" / "chapter-10-5") as directory:
            run_root = Path(directory) / "acceptance" / "123456789abc"
            run_root.mkdir(parents=True)
            (run_root / "minio-data").mkdir()
            project = "chapter105-acceptance-123456789abc"
            overrides = {
                "PROJECT_NAME": project,
                "MINIO_DATA_DIR": str(run_root / "minio-data"),
                "DORIS_INTERNAL_QUERY_PORT": "9030",
                "DORIS_NETWORK_SUBNET": "172.30.40.0/24",
                "DORIS_NETWORK_IP_RANGE": "172.30.40.128/25",
                "DORIS_FE_STATIC_IP": "172.30.40.2",
                "DORIS_BE_STATIC_IP": "172.30.40.3",
                "KAFKA_PORT": "25101", "API_PORT": "25102", "FLINK_REST_PORT": "25103",
                "DORIS_FE_HTTP_PORT": "25104", "DORIS_FE_QUERY_PORT": "25105",
                "DORIS_FE_EDIT_LOG_PORT": "25106", "DORIS_BE_HTTP_PORT": "25107",
                "DORIS_BE_HEARTBEAT_PORT": "25108", "MINIO_API_PORT": "25109",
                "MINIO_CONSOLE_PORT": "25110", "TRINO_PORT": "25111",
            }
            container_keys = {
                "KAFKA_CONTROLLER_CONTAINER_NAME": "kafka-controller", "KAFKA_CONTAINER_NAME": "kafka",
                "API_CONTAINER_NAME": "api", "FLINK_JOBMANAGER_CONTAINER_NAME": "jobmanager",
                "FLINK_TASKMANAGER_CONTAINER_NAME": "taskmanager", "FLINK_SQL_CLIENT_CONTAINER_NAME": "sql-client",
                "DORIS_FE_CONTAINER_NAME": "doris-fe", "DORIS_BE_CONTAINER_NAME": "doris-be",
                "MINIO_CONTAINER_NAME": "minio", "MINIO_INIT_CONTAINER_NAME": "minio-init",
                "METASTORE_POSTGRES_CONTAINER_NAME": "postgres", "HIVE_METASTORE_CONTAINER_NAME": "hive",
                "TRINO_CONTAINER_NAME": "trino",
            }
            overrides.update({key: f"{project}-{suffix}" for key, suffix in container_keys.items()})
            values = {}
            for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    values[key] = value
            values.update(overrides)
            env_path = run_root / "isolated.env"
            env_path.write_text("".join(f"{key}={value}\n" for key, value in values.items()), encoding="utf-8")
            process_env = os.environ.copy()
            process_env.update(overrides)
            rendered = subprocess.run(
                ["docker", "compose", "--project-name", project, "--env-file", str(env_path), "-f", str(COMPOSE_FILE),
                 "--profile", "flink", "--profile", "serving", "--profile", "lakehouse", "config", "--format", "json"],
                cwd=ROOT, env=process_env, capture_output=True, text=True, encoding="utf-8", errors="replace", check=True,
                timeout=30,
            ).stdout
            config_path = run_root / "rendered.json"
            expected_path = run_root / "expected.json"
            config_path.write_text(rendered, encoding="utf-8")
            expected_path.write_text(json.dumps(overrides), encoding="utf-8")
            payload = self._payload(f'''
. "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
$record=Get-Content -Raw -Encoding UTF8 "{expected_path}"|ConvertFrom-Json
$expected=[ordered]@{{}}; foreach($property in $record.PSObject.Properties){{$expected[$property.Name]=[string]$property.Value}}
$result=Assert-AcceptanceRenderedConfig -ConfigJson (Get-Content -Raw -Encoding UTF8 "{config_path}") -ExpectedValues $expected
[ordered]@{{result=$result}}|ConvertTo-Json -Compress
''')
            self.assertEqual("validated", payload["result"])

    def test_report_contract_starts_with_all_required_pending_gates(self):
        payload = self._payload(r'''
. "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
$report=New-AcceptanceReport -RunId "123456789abc" -ProjectName "chapter105-acceptance-123456789abc" -Deadline ([DateTimeOffset]::Parse("2026-08-27T12:34:56Z"))
[ordered]@{keys=@($report.Keys);values=@($report.cold_start,$report.idempotent_second_run,$report.restart_recovery,$report.data_continuity,$report.readiness,$report.tool_analysis)}|ConvertTo-Json -Compress
''')
        for key in ("cold_start", "idempotent_second_run", "restart_recovery", "data_continuity", "readiness", "tool_analysis"):
            self.assertIn(key, payload["keys"])
        self.assertEqual(["pending"] * 6, payload["values"])

    def test_bootstrap_native_argv_carries_exact_isolation_identity_and_deadline(self):
        payload = self._payload(r'''
. "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
$script:call=$null
function Invoke-AcceptanceProcess { param($FilePath,$Arguments,$Deadline,$FailureMessage); $script:call=[ordered]@{file=$FilePath;argv=@($Arguments);deadline=$Deadline.ToString('o');failure=$FailureMessage} }
$deadline=[DateTimeOffset]::Parse("2026-08-27T12:34:56Z")
Invoke-AcceptanceBootstrap -RepositoryRoot "C:\repo" -EnvFile "C:\run\isolated.env" `
  -ProjectName "chapter105-acceptance-123456789abc" -ReportPath "C:\run\bootstrap-first.json" `
  -Deadline $deadline -InitializeEmptyCatalog
$script:call | ConvertTo-Json -Depth 5 -Compress
''')
        self.assertEqual("powershell", payload["file"])
        self.assertEqual("2026-08-27T12:34:56.0000000+00:00", payload["deadline"])
        self.assertEqual(1, payload["argv"].count("-IsolatedAcceptance"))
        self.assertEqual(1, payload["argv"].count("-InitializeEmptyCatalog"))
        self.assertEqual("chapter105-acceptance-123456789abc", payload["argv"][payload["argv"].index("-ComposeProjectName") + 1])
        self.assertEqual("C:\\run\\isolated.env", payload["argv"][payload["argv"].index("-EnvFile") + 1])

    def test_clean_checkout_artifact_build_uses_clean_package_before_preflight(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "jobs" / "datastream-quality").mkdir(parents=True)
            payload = self._payload(f'''
. "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
$script:call=$null
function Invoke-AcceptanceProcess {{ param($FilePath,$Arguments,$Deadline,$FailureMessage)
  $script:call=[ordered]@{{file=$FilePath;argv=@($Arguments);deadline=$Deadline.ToString('o')}}
  $target=Join-Path "{root}" 'jobs/datastream-quality/target'
  [IO.Directory]::CreateDirectory($target)|Out-Null
  [IO.File]::WriteAllBytes((Join-Path $target 'datastream-quality-1.0.0.jar'),[byte[]](1,2,3))
}}
$deadline=[DateTimeOffset]::Parse("2026-08-27T12:34:56Z")
$jar=Invoke-AcceptanceArtifactBuild -RepositoryRoot "{root}" -Deadline $deadline
[ordered]@{{jar=$jar;call=$script:call}}|ConvertTo-Json -Depth 5 -Compress
''')
            self.assertEqual("mvn", payload["call"]["file"])
            self.assertIn("clean", payload["call"]["argv"])
            self.assertIn("package", payload["call"]["argv"])
            self.assertEqual("2026-08-27T12:34:56.0000000+00:00", payload["call"]["deadline"])
            self.assertTrue(payload["jar"].endswith("datastream-quality-1.0.0.jar"))

    def test_continuity_evidence_requires_insert_and_exact_restart_preservation(self):
        payload = self._payload(r'''
. "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
$ok=Assert-AcceptanceContinuity -RowCountInitial 10 -RowCountBeforeRestart 11 -RowCountAfterRestart 11 -SnapshotBeforeRestart "9001" -SnapshotAfterRestart "9001"
$caught=$null
try { Assert-AcceptanceContinuity -RowCountInitial 10 -RowCountBeforeRestart 10 -RowCountAfterRestart 10 -SnapshotBeforeRestart "9001" -SnapshotAfterRestart "9001" } catch { $caught=$_.Exception.Message }
[ordered]@{inserted=$ok.inserted_rows;after=$ok.row_count_after_restart;caught=$caught}|ConvertTo-Json -Compress
''')
        self.assertEqual(1, payload["inserted"])
        self.assertEqual(11, payload["after"])
        self.assertEqual("Isolated data continuity evidence is invalid.", payload["caught"])

    def test_default_snapshot_uses_rendered_minio_path_and_bounded_git(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            minio = root / "actual-minio"
            minio.mkdir()
            (minio / "object.bin").write_bytes(b"actual")
            (root / ".git").mkdir()
            payload = self._payload(f'''
. "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
$script:process=$null
function Invoke-AcceptanceDocker {{ param($Arguments,$Deadline,$FailureMessage)
  if ($Arguments -contains 'config') {{
    $config=[ordered]@{{name='real-default';services=[ordered]@{{minio=[ordered]@{{volumes=@([ordered]@{{type='bind';source='{minio}';target='/data'}})}}}}}}
    return @(($config|ConvertTo-Json -Depth 10 -Compress))
  }}
  return @()
}}
function Invoke-AcceptanceProcess {{ param($FilePath,$Arguments,$Deadline,$FailureMessage); $script:process=[ordered]@{{file=$FilePath;deadline=$Deadline.ToString('o');argv=@($Arguments)}}; return @("{root / '.git'}") }}
$deadline=[DateTimeOffset]::Parse("2026-08-27T12:34:56Z")
$state=Get-AcceptanceDefaultProjectState -RepositoryRoot "{root}" -DefaultEnvPath "{root / 'infra.env'}" -Deadline $deadline
[ordered]@{{minio=$state.minio_data_path;files=@($state.minio_data.files).Count;process=$script:process}}|ConvertTo-Json -Depth 10 -Compress
''')
            self.assertTrue(Path(payload["minio"]).samefile(minio))
            self.assertEqual(1, payload["files"])
            self.assertEqual("git", payload["process"]["file"])
            self.assertEqual("2026-08-27T12:34:56.0000000+00:00", payload["process"]["deadline"])

    def test_success_cleanup_uses_exact_project_argv_and_owned_path(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tmp" / "chapter-10-5") as directory:
            acceptance = Path(directory) / "acceptance"
            run_id = "abcdef123456"
            run_root = acceptance / run_id
            run_root.mkdir(parents=True)
            payload = self._payload(f'''
. "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
$script:calls=@()
function Invoke-AcceptanceDocker {{ param($Arguments,$Deadline,$FailureMessage); $script:calls += ,@($Arguments); return @() }}
$prefix=@('compose','--project-name','chapter105-acceptance-{run_id}','--env-file','{run_root / 'isolated.env'}','-f','compose.yml')
Remove-AcceptanceOwnedResources -ComposePrefix $prefix -ProjectName 'chapter105-acceptance-{run_id}' `
  -AcceptanceRoot '{acceptance}' -RunRoot '{run_root}' -RunId '{run_id}' -Deadline ([DateTimeOffset]::UtcNow.AddSeconds(10))
[ordered]@{{exists=(Test-Path -LiteralPath '{run_root}');calls=$script:calls}}|ConvertTo-Json -Depth 10 -Compress
''')
            self.assertFalse(payload["exists"])
            calls = payload["calls"]
            self.assertIn("down", calls[0])
            self.assertNotIn("-v", calls[0])
            self.assertIn(f"label=com.docker.compose.project=chapter105-acceptance-{run_id}", calls[1])

    def test_bootstrap_report_evidence_requires_expected_job_action(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "bootstrap.json"
            report.write_text(json.dumps({
                "status": "passed", "started_at": "2026-08-27T00:00:00Z", "completed_at": "2026-08-27T00:01:00Z",
                "stages": {name: {"status": "passed", "started_at": "2026-08-27T00:00:00Z", "completed_at": "2026-08-27T00:01:00Z",
                    "details": ({"action": "no_op", "job_id": "1" * 32} if name == "jobs" else ({"action": "already_registered"} if name == "catalog" else {}))}
                    for name in ("preflight", "dependencies", "infrastructure", "initialization", "catalog", "jobs", "acceptance")},
            }), encoding="utf-8")
            payload = self._payload(f'''
. "scripts/verify_chapter_10_5_cold_start.ps1" -FunctionsOnly
$ok=Get-AcceptanceBootstrapEvidence -Path "{report}" -ExpectedJobAction "no_op" -ExpectedCatalogAction "already_registered"
$caught=$null
try {{ Get-AcceptanceBootstrapEvidence -Path "{report}" -ExpectedJobAction "submitted" }} catch {{ $caught=$_.Exception.Message }}
[ordered]@{{ job=$ok.job_id; action=$ok.job_action; catalog=$ok.catalog_action; error=$caught }} | ConvertTo-Json -Compress
''')
            self.assertEqual("1" * 32, payload["job"])
            self.assertEqual("no_op", payload["action"])
            self.assertEqual("already_registered", payload["catalog"])
            self.assertEqual("Bootstrap phase report is invalid.", payload["error"])

    def test_fixed_catalog_initialization_and_existing_table_validation_use_fixed_location(self):
        payload = self._payload(r'''
. "scripts/bootstrap_chapter_10_5.ps1" -FunctionsOnly
$script:sql=@()
function Invoke-Chapter105Native { param($FilePath,$Arguments,$FailureMessage); $script:sql += [string]$Arguments[-1]; return @() }
Initialize-Chapter105EmptyCatalog -ComposePrefix @("compose")
. "scripts/restore_chapter_10_5_catalog.ps1" -FunctionsOnly
function Invoke-Chapter105Trino { param($Sql,$FailureMessage); return @('Create Table','CREATE TABLE lakehouse.analytics.user_behavior_detail WITH ( location = ''s3a://wrong'' )') }
$caught=$null
try { Assert-Chapter105FixedTableLocation } catch { $caught=$_.Exception.Message }
[ordered]@{ sql=$script:sql; error=$caught } | ConvertTo-Json -Compress
''')
        self.assertTrue(any("s3a://warehouse/iceberg/analytics.db" in sql for sql in payload["sql"]))
        self.assertTrue(any("s3a://warehouse/iceberg/analytics.db/user_behavior_detail" in sql for sql in payload["sql"]))
        self.assertEqual("Trino fixed table location is invalid.", payload["error"])

    def test_compose_keeps_legacy_defaults_and_splits_doris_internal_port(self):
        result = subprocess.run(
            ["docker", "compose", "--env-file", str(ENV_FILE), "-f", str(COMPOSE_FILE), "--profile", "serving", "--profile", "lakehouse", "config", "--format", "json"],
            cwd=ROOT, capture_output=True, text=True, check=True,
        )
        config = json.loads(result.stdout)
        self.assertEqual("9030", config["services"]["api"]["environment"]["DORIS_PORT"])
        self.assertEqual("9030", config["services"]["doris-fe"]["ports"][1]["published"])
        with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False, encoding="utf-8") as handle:
            legacy_env = Path(handle.name)
            prefixes = ("DORIS_INTERNAL_QUERY_PORT=", "DORIS_NETWORK_", "DORIS_FE_STATIC_IP=", "DORIS_BE_STATIC_IP=", "DORIS_FE_EDIT_LOG_PORT=", "DORIS_BE_HTTP_PORT=", "MINIO_CONTAINER_NAME=", "MINIO_INIT_CONTAINER_NAME=", "METASTORE_POSTGRES_CONTAINER_NAME=")
            for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
                if not line.startswith(prefixes):
                    handle.write(line + "\n")
        try:
            legacy = subprocess.run(
                ["docker", "compose", "--env-file", str(legacy_env), "-f", str(COMPOSE_FILE), "--profile", "serving", "--profile", "lakehouse", "config", "--format", "json"],
                cwd=ROOT, capture_output=True, text=True, check=True,
            )
            legacy_config = json.loads(legacy.stdout)
            self.assertEqual("ecom-minio", legacy_config["services"]["minio"]["container_name"])
            self.assertEqual("172.21.80.0/24", legacy_config["networks"]["custom_network"]["ipam"]["config"][0]["subnet"])
        finally:
            legacy_env.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
