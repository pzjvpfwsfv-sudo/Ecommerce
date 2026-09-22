import json
from pathlib import Path
import re
import shutil
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent.parent
BUNDLE = "a" * 64


def _compose_service_block(text: str, service: str) -> str:
    lines = text.splitlines()
    start = lines.index(f"  {service}:")
    end = len(lines)
    for index in range(start + 1, len(lines)):
        if re.fullmatch(r"  [a-zA-Z0-9_-]+:", lines[index]):
            end = index
            break
    return "\n".join(lines[start:end])


POWERSHELL_FIXTURE = r'''
function New-TestManifest {
    $specs = [ordered]@{
        orders = 'olist_orders_dataset.csv'
        order_items = 'olist_order_items_dataset.csv'
        order_payments = 'olist_order_payments_dataset.csv'
        order_reviews = 'olist_order_reviews_dataset.csv'
        customers = 'olist_customers_dataset.csv'
        products = 'olist_products_dataset.csv'
        sellers = 'olist_sellers_dataset.csv'
        geolocation = 'olist_geolocation_dataset.csv'
        category_translation = 'product_category_name_translation.csv'
    }
    $files = foreach ($entry in $specs.GetEnumerator()) {
        [pscustomobject][ordered]@{
            entity = $entry.Key
            name = $entry.Value
            sha256 = ('d' * 64)
            bytes = 100
            row_count = 3
            header = @('fixture')
            normalized = [pscustomobject][ordered]@{
                name = "normalized/$($entry.Key).jsonl"
                sha256 = ('b' * 64)
                bytes = 200
                row_count = 3
                source_row_id_sequence_sha256 = ('c' * 64)
            }
        }
    }
    [pscustomobject][ordered]@{
        schema_version = 1
        dataset_id = 'olist-brazilian-ecommerce-v2'
        source_bundle_sha256 = ('a' * 64)
        files = @($files)
    }
}

function New-ObservedState {
    [pscustomobject][ordered]@{
        table_exists = 1
        row_count = 3
        distinct_source_row_id_count = 3
        min_source_row_number = 1
        max_source_row_number = 3
        source_row_id_sequence_sha256 = ('c' * 64)
        bundle_count = 1
        source_bundle_sha256 = ('a' * 64)
        source_file_count = 1
        source_file = 'olist_orders_dataset.csv'
        duplicate_key_count = 0
        snapshot_count = 1
        latest_snapshot_id = '123456789'
        latest_snapshot_committed_at = '2026-09-22 08:00:00.000 UTC'
    }
}

function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
'''


class PowerShellTestCase(unittest.TestCase):
    def run_powershell(self, command: str) -> subprocess.CompletedProcess[str]:
        executable = shutil.which("pwsh") or shutil.which("powershell")
        if executable is None:
            self.skipTest("PowerShell is required for G2-E source coverage")
        return subprocess.run(
            [executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", command],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
            check=False,
        )


class OlistFlinkSqlTest(unittest.TestCase):
    def test_all_registry_entities_render_one_bounded_strict_insert(self):
        from generators.olist_data.flink_sql import render_flink_ingest_sql
        from generators.olist_data.schemas import TABLE_SPECS

        for entity, spec in TABLE_SPECS.items():
            with self.subTest(entity=entity):
                sql = render_flink_ingest_sql(entity, BUNDLE)
                self.assertEqual(1, len(re.findall(r"(?im)^INSERT INTO ", sql)))
                self.assertIn(f"lakehouse.olist.{spec.target_table}", sql)
                self.assertIn(
                    f"/data/olist/prepared/{BUNDLE}/normalized/{entity}.jsonl",
                    sql,
                )
                self.assertIn("SET 'execution.runtime-mode' = 'batch';", sql)
                self.assertIn("SET 'parallelism.default' = '1';", sql)
                self.assertIn("'connector' = 'filesystem'", sql)
                self.assertIn("'json.fail-on-missing-field' = 'true'", sql)
                self.assertIn("'json.ignore-parse-errors' = 'false'", sql)
                self.assertIn(f"source_bundle_sha256 = '{BUNDLE}'", sql)
                self.assertNotRegex(sql, r"__[A-Z0-9_]+__")
                self.assertNotIn("'connector' = 'kafka'", sql)
                self.assertNotIn("PARTITIONED BY", sql.upper())
                for coordinate in (
                    "source_file",
                    "source_row_number",
                    "source_row_id",
                    "source_bundle_sha256",
                    "schema_version",
                ):
                    self.assertIn(coordinate, sql)

    def test_renderer_rejects_unregistered_entities_and_noncanonical_bundle_ids(self):
        from generators.olist_data.flink_sql import render_flink_ingest_sql

        for entity in ("unknown", "orders; DROP TABLE x", "Orders", ""):
            with self.subTest(entity=entity):
                with self.assertRaises(ValueError):
                    render_flink_ingest_sql(entity, BUNDLE)
        for bundle in ("A" * 64, "a" * 63, "a" * 65, "g" * 64, ""):
            with self.subTest(bundle=bundle):
                with self.assertRaises(ValueError):
                    render_flink_ingest_sql("orders", bundle)

    def test_renderer_types_money_timestamps_and_unsigned_values_without_user_sql(self):
        from generators.olist_data.flink_sql import render_flink_ingest_sql

        item_sql = render_flink_ingest_sql("order_items", BUNDLE)
        self.assertIn("price_raw VARCHAR", item_sql)
        self.assertIn("price DECIMAL(38, 2)", item_sql)
        self.assertIn("CAST(price_decimal AS DECIMAL(38, 2)) AS price", item_sql)
        self.assertIn("CAST(order_item_id AS BIGINT) AS order_item_id", item_sql)
        self.assertIn(
            "TO_TIMESTAMP(shipping_limit_date, 'yyyy-MM-dd HH:mm:ss') AS shipping_limit_date",
            item_sql,
        )
        product_sql = render_flink_ingest_sql("products", BUNDLE)
        self.assertIn("product_weight_g DECIMAL(38, 6)", product_sql)
        geo_sql = render_flink_ingest_sql("geolocation", BUNDLE)
        self.assertIn("geolocation_lat DECIMAL(38, 12)", geo_sql)

    def test_ingest_template_has_only_the_closed_token_set(self):
        template = (ROOT / "jobs/sql/19_olist_source_ingest.sql.template").read_text(
            encoding="utf-8"
        )
        self.assertEqual(
            {
                "__PIPELINE_NAME__",
                "__SOURCE_PATH__",
                "__SOURCE_COLUMNS__",
                "__TARGET_TABLE__",
                "__TARGET_COLUMNS__",
                "__SELECT_COLUMNS__",
                "__SOURCE_BUNDLE_SHA256__",
            },
            set(re.findall(r"__[A-Z0-9_]+__", template)),
        )


class OlistComposeContractTest(unittest.TestCase):
    def test_three_flink_services_share_the_same_read_only_olist_mount(self):
        compose = (ROOT / "infra/docker-compose.yml").read_text(encoding="utf-8")
        mount = re.compile(
            r"(?m)^      - type: bind\n"
            r"        source: \$\{OLIST_DATA_DIR\}\n"
            r"        target: /data/olist\n"
            r"        read_only: true$"
        )
        for service in ("flink-jobmanager", "flink-taskmanager", "flink-sql-client"):
            with self.subTest(service=service):
                block = _compose_service_block(compose, service)
                self.assertEqual(1, len(mount.findall(block)))

    def test_default_olist_data_root_is_on_d_drive(self):
        env_lines = (ROOT / "infra/.env.example").read_text(encoding="utf-8").splitlines()
        self.assertIn("OLIST_DATA_DIR=D:/EcommerceData/olist", env_lines)


class OlistPowerShellContractTest(PowerShellTestCase):
    def test_powershell_registry_matches_the_python_source_of_truth(self):
        from generators.olist_data.schemas import TABLE_SPECS

        command = r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path 'scripts/run_g2e_olist_source.ps1') -FunctionsOnly
$registry = Get-G2eRegistry
$rows = foreach ($name in $registry.Keys) {
    [ordered]@{
        entity = $name
        source_file = $registry[$name].SourceFile
        target_table = $registry[$name].TargetTable
        key_fields = @($registry[$name].KeyColumns)
    }
}
@($rows) | ConvertTo-Json -Depth 4 -Compress
'''
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        rows = json.loads(result.stdout.strip().splitlines()[-1])
        actual = {row["entity"]: row for row in rows}
        self.assertEqual(set(TABLE_SPECS), set(actual))
        for entity, spec in TABLE_SPECS.items():
            with self.subTest(entity=entity):
                self.assertEqual(spec.source_file, actual[entity]["source_file"])
                self.assertEqual(spec.target_table, actual[entity]["target_table"])
                self.assertEqual(list(spec.key_fields), actual[entity]["key_fields"])

    def test_startup_plan_preserves_lakehouse_health_dependencies_without_kafka(self):
        command = r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path 'scripts/run_g2e_olist_source.ps1') -FunctionsOnly
$plan = Get-G2eServiceStartupPlan
[ordered]@{
    lakehouse = @($plan.Lakehouse) -join ','
    flink = @($plan.Flink) -join ','
    contains_kafka = @($plan.Lakehouse + $plan.Flink) -contains 'kafka-broker'
} | ConvertTo-Json -Compress
'''
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(
            "minio,minio-init,metastore-postgres,hive-metastore,trino",
            payload["lakehouse"],
        )
        self.assertEqual(
            "flink-jobmanager,flink-taskmanager,flink-sql-client",
            payload["flink"],
        )
        self.assertFalse(payload["contains_kafka"])

    def test_deployment_and_exact_state_machine_are_registry_bound(self):
        command = (
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path 'scripts/verify_g2e_olist_source.ps1') -FunctionsOnly
'''
            + POWERSHELL_FIXTURE
            + r'''
$deployment = Get-G2eSourceDeployment -Manifest (New-TestManifest) -Entity 'orders'
$verified = Assert-G2eSourceState -Observed (New-ObservedState) -Expected $deployment
$absent = Assert-G2eSourceState -Observed ([pscustomobject]@{ table_exists = 0 }) -Expected $deployment
$empty = Assert-G2eSourceState -Observed ([pscustomobject]@{
    table_exists = 1; row_count = 0; distinct_source_row_id_count = 0
    min_source_row_number = $null; max_source_row_number = $null
    source_row_id_sequence_sha256 = $null; bundle_count = 0; source_bundle_sha256 = $null
    source_file_count = 0; source_file = $null; duplicate_key_count = 0
    snapshot_count = 0; latest_snapshot_id = $null; latest_snapshot_committed_at = $null
}) -Expected $deployment
[ordered]@{
    entity = $deployment.Entity
    table = $deployment.TargetTable
    pipeline = $deployment.PipelineName
    source_path = $deployment.ContainerSourcePath
    expected_rows = $deployment.ExpectedRowCount
    verified = $verified.Kind
    absent = $absent.Kind
    empty = $empty.Kind
} | ConvertTo-Json -Compress
'''
        )
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("orders", payload["entity"])
        self.assertEqual("orders_src_v1", payload["table"])
        self.assertEqual(f"graduation-g2e-orders-{BUNDLE}", payload["pipeline"])
        self.assertEqual(
            f"/data/olist/prepared/{BUNDLE}/normalized/orders.jsonl",
            payload["source_path"],
        )
        self.assertEqual(3, payload["expected_rows"])
        self.assertEqual("Verified", payload["verified"])
        self.assertEqual("Absent", payload["absent"])
        self.assertEqual("EmptyWithoutSnapshot", payload["empty"])

    def test_every_contradictory_existing_state_is_rejected(self):
        command = (
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path 'scripts/verify_g2e_olist_source.ps1') -FunctionsOnly
'''
            + POWERSHELL_FIXTURE
            + r'''
$deployment = Get-G2eSourceDeployment -Manifest (New-TestManifest) -Entity 'orders'
function Test-StateMutation([string]$Name, $Value) {
    $observed = New-ObservedState
    $observed.$Name = $Value
    return Test-Rejected { Assert-G2eSourceState -Observed $observed -Expected $deployment }
}
[ordered]@{
    foreign_bundle = Test-StateMutation 'source_bundle_sha256' ('f' * 64)
    partial_rows = Test-StateMutation 'row_count' 2
    duplicate_ids = Test-StateMutation 'distinct_source_row_id_count' 2
    row_gap = Test-StateMutation 'min_source_row_number' 2
    wrong_file = Test-StateMutation 'source_file' 'other.csv'
    wrong_digest = Test-StateMutation 'source_row_id_sequence_sha256' ('e' * 64)
    duplicate_key = Test-StateMutation 'duplicate_key_count' 1
    no_snapshot = Test-StateMutation 'snapshot_count' 0
    zero_snapshot_id = Test-StateMutation 'latest_snapshot_id' '0'
} | ConvertTo-Json -Compress
'''
        )
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(all(payload.values()), payload)

    def test_paths_file_hash_sql_tokens_and_active_jobs_fail_closed(self):
        command = (
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path 'scripts/run_g2e_olist_source.ps1') -FunctionsOnly
'''
            + POWERSHELL_FIXTURE
            + r'''
$deployment = Get-G2eSourceDeployment -Manifest (New-TestManifest) -Entity 'orders'
$tempFile = Join-Path $env:TEMP ('g2e-hash-' + [guid]::NewGuid().ToString('N') + '.jsonl')
[IO.File]::WriteAllText($tempFile, "fixture`n", [Text.UTF8Encoding]::new($false))
try {
    [ordered]@{
        non_d_manifest = Test-Rejected {
            Assert-G2eManifestPath -ManifestPath 'C:\data\source-bundle.json' -DataRoot 'D:\EcommerceData\olist'
        }
        outside_root = Test-Rejected {
            Assert-G2eManifestPath -ManifestPath 'D:\other\source-bundle.json' -DataRoot 'D:\EcommerceData\olist'
        }
        altered_hash = Test-Rejected {
            Assert-G2eFileSha256 -Path $tempFile -ExpectedSha256 ('0' * 64)
        }
        unresolved_sql = Test-Rejected {
            Assert-G2eRenderedSql -Sql '__UNKNOWN_TOKEN__' -Deployment $deployment
        }
        unknown_entity = Test-Rejected {
            Get-G2eSourceDeployment -Manifest (New-TestManifest) -Entity 'unknown'
        }
        active_duplicate = Test-Rejected {
            Assert-G2eNoActivePipelineJob -PipelineName $deployment.PipelineName -Jobs @(
                [pscustomobject]@{ jid = ('1' * 32); name = $deployment.PipelineName; state = 'RUNNING' }
            )
        }
        terminal_history_ok = -not (Test-Rejected {
            Assert-G2eNoActivePipelineJob -PipelineName $deployment.PipelineName -Jobs @(
                [pscustomobject]@{ jid = ('2' * 32); name = $deployment.PipelineName; state = 'FAILED' }
            )
        })
    } | ConvertTo-Json -Compress
} finally {
    Remove-Item -LiteralPath $tempFile -Force -ErrorAction SilentlyContinue
}
'''
        )
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(all(payload.values()), payload)

    def test_verification_sql_and_finished_job_are_exact(self):
        command = (
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path 'scripts/verify_g2e_olist_source.ps1') -FunctionsOnly
'''
            + POWERSHELL_FIXTURE
            + r'''
$deployment = Get-G2eSourceDeployment -Manifest (New-TestManifest) -Entity 'orders'
$template = Get-Content -LiteralPath 'jobs/sql/20_olist_source_verify.sql.template' -Raw -Encoding UTF8
$sql = Render-G2eVerificationSql -Template $template -Deployment $deployment
$parts = @(Split-G2eNamedSql -Sql $sql)
$jobs = @(
    [pscustomobject]@{ jid = ('1' * 32); name = $deployment.PipelineName; state = 'FAILED' },
    [pscustomobject]@{ jid = ('2' * 32); name = $deployment.PipelineName; state = 'FINISHED' }
)
$finished = Assert-G2eFinishedJob -PipelineName $deployment.PipelineName -JobId ('2' * 32) -Jobs $jobs
[ordered]@{
    names = @($parts.Name)
    jid = $finished.jid
    exact_digest = [bool]($sql -match 'array_agg\(source_row_id ORDER BY source_row_number\)')
    snapshot_table = [bool]($sql -match 'orders_src_v1\$snapshots')
    unresolved = [bool]($sql -match '__[A-Z0-9_]+__')
    wrong_job = Test-Rejected {
        Assert-G2eFinishedJob -PipelineName $deployment.PipelineName -JobId ('3' * 32) -Jobs $jobs
    }
    unfinished_job = Test-Rejected {
        Assert-G2eFinishedJob -PipelineName $deployment.PipelineName -JobId ('1' * 32) -Jobs $jobs
    }
    unknown_token = Test-Rejected {
        Render-G2eVerificationSql -Template ($template + "`n__UNKNOWN__") -Deployment $deployment
    }
} | ConvertTo-Json -Depth 4 -Compress
'''
        )
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(
            ["table_exists", "row_summary", "identity", "key_uniqueness", "snapshot"],
            payload["names"],
        )
        self.assertEqual("2" * 32, payload["jid"])
        self.assertTrue(payload["exact_digest"])
        self.assertTrue(payload["snapshot_table"])
        self.assertFalse(payload["unresolved"])
        self.assertTrue(payload["wrong_job"])
        self.assertTrue(payload["unfinished_job"])
        self.assertTrue(payload["unknown_token"])

    def test_checkpoint_is_optional_but_contradictory_evidence_is_rejected(self):
        command = (
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path 'scripts/verify_g2e_olist_source.ps1') -FunctionsOnly
'''
            + POWERSHELL_FIXTURE
            + r'''
$valid = Get-G2eCheckpointEvidence ([pscustomobject]@{
    counts = [pscustomobject]@{ completed = 1 }
    latest = [pscustomobject]@{
        completed = [pscustomobject]@{
            id = 7; status = 'COMPLETED'; latest_ack_timestamp = 1780000000000
        }
    }
})
$missing = Get-G2eCheckpointEvidence $null
[ordered]@{
    valid = $valid.status
    missing = $missing.status
    contradictory = Test-Rejected {
        Get-G2eCheckpointEvidence ([pscustomobject]@{
            counts = [pscustomobject]@{ completed = 1 }
            latest = [pscustomobject]@{
                completed = [pscustomobject]@{
                    id = 8; status = 'FAILED'; latest_ack_timestamp = 1780000000001
                }
            }
        })
    }
} | ConvertTo-Json -Compress
'''
        )
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("completed", payload["valid"])
        self.assertEqual("not_observed", payload["missing"])
        self.assertTrue(payload["contradictory"])

    def test_verification_report_is_atomic_idempotent_and_never_overwritten(self):
        command = (
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path 'scripts/verify_g2e_olist_source.ps1') -FunctionsOnly
'''
            + POWERSHELL_FIXTURE
            + r'''
$directory = Join-Path $env:TEMP ('g2e-report-' + [guid]::NewGuid().ToString('N'))
$path = Join-Path $directory 'orders.json'
$report = [ordered]@{
    status = 'PASS'; verified_at = '2026-09-22T08:00:00Z'; verification_mode = 'finished_bounded_job'
    entity = 'orders'; source_bundle_sha256 = ('a' * 64); expected_row_count = 3
    job_id = ('1' * 32); checkpoint = [ordered]@{ status = 'completed'; completed_count = 1 }
    iceberg = [ordered]@{ row_count = 3; snapshot_id = '123' }
}
$safeReplay = [ordered]@{
    status = 'PASS'; verified_at = '2026-09-22T09:00:00Z'; verification_mode = 'already_ingested'
    entity = 'orders'; source_bundle_sha256 = ('a' * 64); expected_row_count = 3
    job_id = $null; checkpoint = [ordered]@{ status = 'not_requested'; completed_count = 0 }
    iceberg = [ordered]@{ row_count = 3; snapshot_id = '123' }
}
try {
    $first = Write-G2eSourceReport -Path $path -Report $report
    $second = Write-G2eSourceReport -Path $path -Report $safeReplay
    $before = Get-Content -LiteralPath $path -Raw -Encoding UTF8
    $contradiction = Test-Rejected {
        Write-G2eSourceReport -Path $path -Report ([ordered]@{
            status = 'PASS'; verified_at = '2026-09-22T09:00:00Z'; verification_mode = 'already_ingested'
            entity = 'orders'; source_bundle_sha256 = ('a' * 64); expected_row_count = 3
            job_id = $null; checkpoint = [ordered]@{ status = 'not_requested'; completed_count = 0 }
            iceberg = [ordered]@{ row_count = 3; snapshot_id = '999' }
        })
    }
    $after = Get-Content -LiteralPath $path -Raw -Encoding UTF8
    [ordered]@{
        same_path = $first -ceq $second
        contradiction = $contradiction
        unchanged = $before -ceq $after
        part_count = @(Get-ChildItem -LiteralPath $directory -Filter '*.part').Count
    } | ConvertTo-Json -Compress
} finally {
    Remove-Item -LiteralPath $directory -Recurse -Force -ErrorAction SilentlyContinue
}
'''
        )
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["same_path"])
        self.assertTrue(payload["contradiction"])
        self.assertTrue(payload["unchanged"])
        self.assertEqual(0, payload["part_count"])


if __name__ == "__main__":
    unittest.main()
