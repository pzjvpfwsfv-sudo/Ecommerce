from hashlib import sha256
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
BUNDLE = "a" * 64
RUN_ID = f"orders-v1-b{BUNDLE}"

SOURCE_TABLES = {
    "orders": "orders_src_v1",
    "order_items": "order_items_src_v1",
    "order_payments": "order_payments_src_v1",
    "order_reviews": "order_reviews_src_v1",
    "customers": "customers_src_v1",
    "products": "products_src_v1",
    "sellers": "sellers_src_v1",
    "geolocation": "geolocation_src_v1",
    "category_translation": "category_translation_src_v1",
}
CURATED_TABLES = [
    "customer_dim_v1",
    "category_dim_v1",
    "product_dim_v1",
    "seller_dim_v1",
    "geolocation_dim_v1",
    "order_fact_v1",
    "order_item_fact_v1",
    "payment_fact_v1",
    "review_fact_v1",
]
CURATED_TOKENS = {
    f"__{name.removesuffix('_v1').upper()}_SNAPSHOT__" for name in CURATED_TABLES
}
FAMILIES = ["overview", "delivery", "payment", "ranking", "review", "quality"]

REQUIRED_ORDER_METRICS = {
    "order_count", "delivered_order_count", "canceled_order_count",
    "unavailable_order_count", "status_eligible_order_count", "status_excluded_order_count",
    "delivered_rate", "canceled_rate", "unique_customer_count", "repeat_customer_count",
    "repeat_customer_rate", "item_value_sum", "freight_value_sum",
    "payment_value_sum", "items_per_order_avg", "delivery_eligible_order_count",
    "delivery_days_avg", "delivery_days_p50", "delivery_days_p90",
    "late_delivery_order_count", "late_delivery_rate", "payment_row_count",
    "payment_order_count", "installment_order_count", "payment_type_order_count",
    "payment_type_value_sum", "ranking_order_count", "ranking_item_row_count",
    "ranking_customer_count", "ranking_item_value_sum", "ranking_freight_value_sum",
    "ranking_payment_value_sum", "ranking_late_delivery_order_count",
    "ranking_late_delivery_rate", "review_row_count", "reviewed_order_count",
    "review_coverage_rate", "review_score_avg", "low_score_order_count",
    "low_score_rate", "multi_review_order_count", "source_row_count",
    "iceberg_row_count", "duplicate_key_count", "orphan_key_count",
    "invalid_value_count", "temporal_anomaly_count", "amount_comparable_order_count",
    "amount_reconciled_order_count", "amount_mismatch_order_count",
    "amount_reconciliation_rate", "payment_item_freight_abs_difference_avg",
    "payment_item_freight_abs_difference_p50", "payment_item_freight_abs_difference_p90",
}


POWERSHELL_FIXTURE = r'''
$ErrorActionPreference = 'Stop'
Import-Module (Resolve-Path './scripts/lib/G2e.OrderMetrics.psm1') -Force

function New-TestManifest {
    $counts = [ordered]@{
        orders=2; order_items=2; order_payments=2; order_reviews=3; customers=2
        products=2; sellers=2; geolocation=2; category_translation=2
    }
    $files = foreach ($entity in $counts.Keys) {
        [pscustomobject][ordered]@{
            entity=$entity; name=("$entity.csv"); sha256=('d' * 64); bytes=100
            row_count=$counts[$entity]; header=@('fixture')
            normalized=[pscustomobject][ordered]@{
                name="normalized/$entity.jsonl"; sha256=('b' * 64); bytes=200
                row_count=$counts[$entity]; source_row_id_sequence_sha256=('c' * 64)
            }
        }
    }
    [pscustomobject][ordered]@{
        schema_version=1; dataset_id='olist-brazilian-ecommerce-v2'
        source_bundle_sha256=('a' * 64); files=@($files)
    }
}

function New-SourceReports {
    $tableNames = [ordered]@{
        orders='orders_src_v1'; order_items='order_items_src_v1'
        order_payments='order_payments_src_v1'; order_reviews='order_reviews_src_v1'
        customers='customers_src_v1'; products='products_src_v1'; sellers='sellers_src_v1'
        geolocation='geolocation_src_v1'; category_translation='category_translation_src_v1'
    }
    $counts = [ordered]@{
        orders=2; order_items=2; order_payments=2; order_reviews=3; customers=2
        products=2; sellers=2; geolocation=2; category_translation=2
    }
    $result = @(); $snapshot = 101
    foreach ($entity in $tableNames.Keys) {
        $result += [pscustomobject][ordered]@{
            status='PASS'; dataset_id='olist-brazilian-ecommerce-v2'; entity=$entity
            source_bundle_sha256=('a' * 64); expected_row_count=$counts[$entity]
            normalized_sha256=('b' * 64); source_row_id_sequence_sha256=('c' * 64)
            target_table=$tableNames[$entity]; source_file=("$entity.csv")
            iceberg=[pscustomobject][ordered]@{
                row_count=$counts[$entity]; snapshot_id=[string]$snapshot
                snapshot_committed_at='2026-09-22 08:00:00.000 UTC'
            }
        }
        $snapshot++
    }
    return $result
}

function New-CuratedReport {
    $source = [ordered]@{}; $snapshot = 101
    foreach ($table in @(
        'orders_src_v1','order_items_src_v1','order_payments_src_v1','order_reviews_src_v1',
        'customers_src_v1','products_src_v1','sellers_src_v1','geolocation_src_v1',
        'category_translation_src_v1')) { $source[$table] = [string]$snapshot; $snapshot++ }
    $curated = [ordered]@{}; $snapshot = 201
    foreach ($table in @(
        'customer_dim_v1','category_dim_v1','product_dim_v1','seller_dim_v1',
        'geolocation_dim_v1','order_fact_v1','order_item_fact_v1','payment_fact_v1',
        'review_fact_v1')) { $curated[$table] = [string]$snapshot; $snapshot++ }
    [pscustomobject][ordered]@{
        status='PASS'; dataset_id='olist-brazilian-ecommerce-v2'
        source_bundle_sha256=('a' * 64); source_snapshot_set_sha256=('e' * 64)
        source_snapshots=$source; curated_snapshots=$curated
        hard_gate=[pscustomobject][ordered]@{ duplicate_order_key_count=0; orphan_item_order_count=0 }
        reportable_quality=[pscustomobject][ordered]@{
            unknown_order_status_count=1; unknown_payment_type_count=0
            multi_review_order_count=1; lifecycle_order_anomaly_count=0
            payment_item_total_mismatch_count=1
        }
        grain_reconciliation=[pscustomobject][ordered]@{
            order_fact_expected_count=2; order_fact_row_count=2
            order_item_fact_expected_count=2; order_item_fact_row_count=2
            payment_fact_expected_count=2; payment_fact_row_count=2
            review_fact_expected_count=3; review_fact_row_count=3
        }
        anti_fanout=[pscustomobject][ordered]@{
            source_order_count=2; fact_order_count=2
            source_item_count=2; fact_item_count=2
            source_payment_count=2; fact_payment_count=2
            source_review_count=3; fact_review_count=3
            source_item_value_sum='30.00'; fact_item_value_sum='30.00'
            source_freight_value_sum='0.00'; fact_freight_value_sum='0.00'
            source_payment_value_sum='35.00'; fact_payment_value_sum='35.00'
        }
    }
}

function New-Identity {
    Get-G2eMetricIdentity -Manifest (New-TestManifest) -SourceReports (New-SourceReports) `
        -CuratedReport (New-CuratedReport) -WindowStart '2017-01-01' -WindowEnd '2017-01-01' `
        -CalculatedAt '2026-09-22T08:00:00Z' -ImplementationRevision ('1' * 40)
}

function New-QualityRow {
    [pscustomobject][ordered]@{
        window_type='FULL'; window_start='2017-01-01'; window_end='2017-01-01'
        source_row_count='2'; iceberg_row_count='2'; duplicate_key_count='0'
        orphan_key_count='0'; invalid_value_count='1'; temporal_anomaly_count='0'
        amount_comparable_order_count='1'; amount_reconciled_order_count='0'
        amount_mismatch_order_count='1'; amount_reconciliation_rate='0.000000'
        payment_item_freight_abs_difference_avg='5.000000'
        payment_item_freight_abs_difference_p50='5.000000'
        payment_item_freight_abs_difference_p90='5.000000'
        reconciliation_status='PASS'
    }
}

function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
'''


REFRESH_POWERSHELL_FIXTURE = POWERSHELL_FIXTURE + r'''
. (Resolve-Path './scripts/refresh_g2e_order_metrics.ps1') -FunctionsOnly

function New-TestEvidence {
    $result = [ordered]@{}; $index = 1
    foreach ($name in @('overview','delivery','payment','ranking','review','quality')) {
        $result[$name] = [pscustomobject][ordered]@{
            RowCount = [long]$index
            Sha256 = ([string]$index * 64)
        }
        $index++
    }
    return $result
}

function New-StoredOverviewRows {
    $identity = New-Identity
    return @([pscustomobject][ordered]@{
        metric_run_id=$identity.MetricRunId; dataset_id=$identity.DatasetId
        metric_version=$identity.MetricVersion; window_type='FULL'
        window_start='2017-01-01'; window_end='2017-01-01'; order_count='2'
        delivered_order_count='2'; canceled_order_count='0'; unavailable_order_count='0'
        status_eligible_order_count='2'; status_excluded_order_count='0'
        delivered_rate='1.000000'; canceled_rate='0.000000'; unique_customer_count='1'
        repeat_customer_count='1'; repeat_customer_rate='1.000000'; item_row_count='2'
        item_value_sum='30.00'; freight_value_sum='0.00'; payment_value_sum='35.00'
        items_per_order_avg='1.000000'
    })
}
'''


class PowerShellTestCase(unittest.TestCase):
    def run_powershell(
        self, command: str, timeout: int = 30, executable: str | None = None
    ):
        executable = executable or shutil.which("pwsh") or shutil.which("powershell")
        if executable is None:
            self.skipTest("PowerShell is required for G2-E metric coverage")
        return subprocess.run(
            [executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", command],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
        )

    def payload(self, body: str):
        result = self.run_powershell(POWERSHELL_FIXTURE + body)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        return json.loads(result.stdout.strip().splitlines()[-1])


class OrderMetricArtifactTests(unittest.TestCase):
    def test_catalog_pins_the_exact_public_metric_set_and_semantics(self):
        catalog = json.loads(
            (ROOT / "configs/metrics/orders-v1.json").read_text(encoding="utf-8")
        )
        self.assertEqual("orders", catalog["domain"])
        self.assertEqual("olist-brazilian-ecommerce-v2", catalog["dataset_id"])
        self.assertEqual("orders-v1", catalog["metric_version"])
        definitions = catalog["definitions"]
        self.assertEqual(REQUIRED_ORDER_METRICS, {item["metric_name"] for item in definitions})
        self.assertEqual(len(REQUIRED_ORDER_METRICS), len(definitions))
        required_fields = {
            "metric_name", "display_name", "formula", "numerator", "denominator",
            "source_fields", "allowed_windows", "additive", "null_policy", "exclusions",
            "limitations", "forbidden_claims",
        }
        for item in definitions:
            self.assertEqual(required_fields, set(item))
            self.assertRegex(item["display_name"], r"[\u4e00-\u9fff]")
            self.assertTrue(set(item["allowed_windows"]) <= {"DAY", "MONTH", "FULL"})
            self.assertTrue(item["limitations"])
        amount_names = {
            "item_value_sum", "freight_value_sum", "payment_value_sum",
            "payment_type_value_sum", "ranking_item_value_sum",
            "ranking_freight_value_sum", "ranking_payment_value_sum",
            "payment_item_freight_abs_difference_avg",
            "payment_item_freight_abs_difference_p50",
            "payment_item_freight_abs_difference_p90",
        }
        amount_definitions = [item for item in definitions if item["metric_name"] in amount_names]
        for item in amount_definitions:
            claims = " ".join(item["forbidden_claims"])
            for claim in ("利润", "净收入", "审计 GMV", "退款后收入", "币种换算"):
                self.assertIn(claim, claims)
        ranking_payment = next(
            item for item in definitions if item["metric_name"] == "ranking_payment_value_sum"
        )
        self.assertFalse(ranking_payment["additive"])
        self.assertIn("seller_state", " ".join(ranking_payment["limitations"]))

    def test_sql_has_six_fixed_snapshot_pinned_result_families(self):
        sql = (ROOT / "jobs/sql/23_g2e_order_metrics.sql.template").read_text(
            encoding="utf-8"
        )
        self.assertEqual(FAMILIES, re.findall(r"(?m)^-- result:([a-z_]+)$", sql))
        self.assertEqual(CURATED_TOKENS, set(re.findall(r"__[A-Z_]+_SNAPSHOT__", sql)))
        self.assertNotRegex(sql, r"(?im)^\s*(DROP|DELETE|TRUNCATE|CREATE|INSERT|UPDATE)\b")
        self.assertNotIn("__SOURCE_TABLE__", sql)
        for table in CURATED_TABLES:
            token = f"__{table.removesuffix('_v1').upper()}_SNAPSHOT__"
            for match in re.finditer(rf"lakehouse\.olist\.{table}\b", sql):
                suffix = sql[match.end(): match.end() + 80]
                self.assertRegex(suffix, rf"FOR VERSION AS OF\s+{token}")
        for cte in ("item_per_order", "payment_per_order", "review_per_order"):
            self.assertIn(f"{cte} AS (", sql)
        self.assertIn("PARTITION BY order_id", sql)
        self.assertRegex(
            sql,
            r"(?s)all_payments AS \(.*?FROM window_totals totals\s+LEFT JOIN payment_rows rows",
        )
        self.assertIn("seller_state_order", sql)
        self.assertIn("payment_value_is_additive", sql)

    def test_payment_rollup_does_not_count_a_left_join_placeholder_as_a_payment(self):
        sql = (ROOT / "jobs/sql/23_g2e_order_metrics.sql.template").read_text(
            encoding="utf-8"
        )
        all_payments = re.search(
            r"(?s)all_payments AS \((.*?)\),\s*typed_payments AS \(", sql
        )
        self.assertIsNotNone(all_payments)
        self.assertRegex(
            all_payments.group(1),
            r"count\(rows\.order_id\)\s+AS payment_row_count",
        )
        self.assertNotRegex(
            all_payments.group(1), r"count\(\*\)\s+AS payment_row_count"
        )

    def test_doris_ddl_defines_seven_immutable_run_scoped_tables(self):
        ddl = (ROOT / "infra/compose/doris/init/03_create_order_metrics.sql").read_text(
            encoding="utf-8"
        )
        names = re.findall(r"(?im)^CREATE TABLE IF NOT EXISTS\s+([a-z_]+)", ddl)
        self.assertEqual(
            [
                "order_metric_publications", "order_metric_overview", "order_metric_delivery",
                "order_metric_payment", "order_metric_ranking", "order_metric_review",
                "order_metric_quality",
            ],
            names,
        )
        self.assertEqual(7, ddl.count("BUCKETS 1"))
        self.assertNotRegex(ddl, r"(?im)^\s*(DROP|DELETE|TRUNCATE|CREATE OR REPLACE)\b")
        self.assertRegex(ddl, r"metric_run_id\s+VARCHAR\(128\)")
        publication = ddl.split("CREATE TABLE IF NOT EXISTS order_metric_overview", 1)[0]
        for family in FAMILIES:
            self.assertRegex(publication, rf"{family}_row_count\s+BIGINT")
            self.assertRegex(publication, rf"{family}_sha256\s+CHAR\(64\)")
        keys = re.findall(r"UNIQUE KEY\(([^)]+)\)", ddl)
        self.assertEqual(7, len(keys))
        self.assertEqual("metric_run_id", keys[0].strip())
        self.assertIn("dimension_type, dimension_id", keys[4])
        self.assertIn("DECIMAL(38,2)", ddl)
        self.assertIn("DECIMAL(18,6)", ddl)


class OrderMetricPureFunctionTests(PowerShellTestCase):
    def test_identity_requires_exact_bundle_snapshots_counts_range_and_revision(self):
        payload = self.payload(
            r'''
$manifest = New-TestManifest; $reports = New-SourceReports; $curated = New-CuratedReport
$identity = New-Identity
$badMap = New-CuratedReport; $badMap.curated_snapshots.Remove('review_fact_v1')
$wrongBundle = New-CuratedReport; $wrongBundle.source_bundle_sha256 = ('f' * 64)
$wrongCount = New-TestManifest; ($wrongCount.files | Where-Object entity -ceq 'orders').row_count = 3
[ordered]@{
    run_id=$identity.MetricRunId; source_count=$identity.SourceOrderCount
    source_keys=@($identity.SourceSnapshots.Keys); curated_keys=@($identity.CuratedSnapshots.Keys)
    missing_snapshot=Test-Rejected { Get-G2eMetricIdentity -Manifest $manifest -SourceReports $reports -CuratedReport $badMap -WindowStart '2017-01-01' -WindowEnd '2017-01-01' -CalculatedAt '2026-09-22T08:00:00Z' -ImplementationRevision ('1' * 40) }
    wrong_bundle=Test-Rejected { Get-G2eMetricIdentity -Manifest $manifest -SourceReports $reports -CuratedReport $wrongBundle -WindowStart '2017-01-01' -WindowEnd '2017-01-01' -CalculatedAt '2026-09-22T08:00:00Z' -ImplementationRevision ('1' * 40) }
    wrong_count=Test-Rejected { Get-G2eMetricIdentity -Manifest $wrongCount -SourceReports $reports -CuratedReport $curated -WindowStart '2017-01-01' -WindowEnd '2017-01-01' -CalculatedAt '2026-09-22T08:00:00Z' -ImplementationRevision ('1' * 40) }
    dirty_revision=Test-Rejected { Get-G2eMetricIdentity -Manifest $manifest -SourceReports $reports -CuratedReport $curated -WindowStart '2017-01-01' -WindowEnd '2017-01-01' -CalculatedAt '2026-09-22T08:00:00Z' -ImplementationRevision (('1' * 40) + '-dirty') }
    wrong_run=Test-Rejected { Get-G2eMetricIdentity -Manifest $manifest -SourceReports $reports -CuratedReport $curated -WindowStart '2017-01-01' -WindowEnd '2017-01-01' -CalculatedAt '2026-09-22T08:00:00Z' -ImplementationRevision ('1' * 40) -MetricRunId 'orders-v1-bbad' }
    reversed=Test-Rejected { Get-G2eMetricIdentity -Manifest $manifest -SourceReports $reports -CuratedReport $curated -WindowStart '2017-01-02' -WindowEnd '2017-01-01' -CalculatedAt '2026-09-22T08:00:00Z' -ImplementationRevision ('1' * 40) }
} | ConvertTo-Json -Depth 8 -Compress
'''
        )
        self.assertEqual(RUN_ID, payload["run_id"])
        self.assertEqual(2, payload["source_count"])
        self.assertEqual(sorted(payload["source_keys"]), payload["source_keys"])
        self.assertEqual(sorted(payload["curated_keys"]), payload["curated_keys"])
        self.assertTrue(all(payload[key] for key in (
            "missing_snapshot", "wrong_bundle", "wrong_count", "dirty_revision",
            "wrong_run", "reversed",
        )))

    def test_named_sql_renderer_requires_exact_positive_snapshot_map(self):
        payload = self.payload(
            r'''
$template = Get-Content './jobs/sql/23_g2e_order_metrics.sql.template' -Raw -Encoding UTF8
$snapshots = (New-CuratedReport).curated_snapshots
$parts = Split-G2eNamedSql -Sql $template -CuratedSnapshots $snapshots
$missing = [ordered]@{}; foreach ($key in $snapshots.Keys) { if ($key -cne 'review_fact_v1') { $missing[$key]=$snapshots[$key] } }
$extra = [ordered]@{}; foreach ($key in $snapshots.Keys) { $extra[$key]=$snapshots[$key] }; $extra.other='999'
$zero = [ordered]@{}; foreach ($key in $snapshots.Keys) { $zero[$key]=$snapshots[$key] }; $zero.order_fact_v1='0'
[ordered]@{
    names=@($parts.Keys); no_tokens=(($parts.Values -join "`n") -notmatch '__[A-Z_]+_SNAPSHOT__')
    numeric_pins=(($parts.Values -join "`n") -match 'FOR VERSION AS OF 20[1-9]')
    missing=Test-Rejected { Split-G2eNamedSql -Sql $template -CuratedSnapshots $missing }
    extra=Test-Rejected { Split-G2eNamedSql -Sql $template -CuratedSnapshots $extra }
    zero=Test-Rejected { Split-G2eNamedSql -Sql $template -CuratedSnapshots $zero }
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(FAMILIES, payload["names"])
        self.assertTrue(payload["no_tokens"])
        self.assertTrue(payload["numeric_pins"])
        self.assertTrue(payload["missing"] and payload["extra"] and payload["zero"])

    def test_csv_parser_preserves_quotes_nulls_and_rejects_ambiguous_input(self):
        payload = self.payload(
            r'''
$rows = ConvertFrom-G2eCsv "id,note,nullable`r`n1,`"a,b`",`r`n2,`"`",x`r`n"
[ordered]@{
    count=$rows.Count; comma=$rows[0].note; null_value=($null -eq $rows[0].nullable)
    quoted_empty=($rows[1].note -ceq '')
    duplicate_header=Test-Rejected { ConvertFrom-G2eCsv "id,ID`n1,2`n" }
    short_row=Test-Rejected { ConvertFrom-G2eCsv "a,b`n1`n" }
    bad_quote=Test-Rejected { ConvertFrom-G2eCsv "a`n`"unterminated`n" }
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(2, payload["count"])
        self.assertEqual("a,b", payload["comma"])
        self.assertTrue(payload["null_value"])
        self.assertTrue(payload["quoted_empty"])
        self.assertTrue(payload["duplicate_header"])
        self.assertTrue(payload["short_row"])
        self.assertTrue(payload["bad_quote"])

    def test_csv_parser_rejects_an_invalid_header_before_scanning_the_body(self):
        payload = self.payload(
            r'''
$message = ''
try { $null = ConvertFrom-G2eCsv ",value`n1,`"unterminated" }
catch { $message = $_.Exception.Message }
[ordered]@{ message=$message } | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(
            "G2-E CSV headers must be nonempty and unique.", payload["message"]
        )

    def test_quality_merge_reconciles_manifest_source_and_curated_evidence(self):
        payload = self.payload(
            r'''
$quality = Merge-G2eQualityEvidence -SqlQuality @((New-QualityRow)) `
    -Manifest (New-TestManifest) -SourceReports (New-SourceReports) `
    -CuratedReport (New-CuratedReport)
$badSql = New-QualityRow; $badSql.source_row_count='3'
$badReport = New-CuratedReport; $badReport.anti_fanout.fact_payment_count=9
[ordered]@{
    source=$quality.source_row_count; status=$quality.reconciliation_status
    raw=($quality.raw_row_counts_json | ConvertFrom-Json).orders
    iceberg=($quality.iceberg_row_counts_json | ConvertFrom-Json).order_payments_src_v1
    normalized_hash=($quality.normalized_sha256_json | ConvertFrom-Json).orders
    curated_snapshot=($quality.curated_snapshots_json | ConvertFrom-Json).order_fact_v1
    quality_evidence=($quality.reportable_quality_json | ConvertFrom-Json).multi_review_order_count
    bad_sql=Test-Rejected { Merge-G2eQualityEvidence -SqlQuality @($badSql) -Manifest (New-TestManifest) -SourceReports (New-SourceReports) -CuratedReport (New-CuratedReport) }
    bad_report=Test-Rejected { Merge-G2eQualityEvidence -SqlQuality @((New-QualityRow)) -Manifest (New-TestManifest) -SourceReports (New-SourceReports) -CuratedReport $badReport }
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(2, payload["source"])
        self.assertEqual("PASS", payload["status"])
        self.assertEqual(2, payload["raw"])
        self.assertEqual(2, payload["iceberg"])
        self.assertEqual("b" * 64, payload["normalized_hash"])
        self.assertEqual("206", payload["curated_snapshot"])
        self.assertEqual(1, payload["quality_evidence"])
        self.assertTrue(payload["bad_sql"] and payload["bad_report"])

    def test_digest_and_export_are_canonical_lf_utf8_and_key_safe(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tmp") as temporary:
            output_dir = Path(temporary)
            output_path = output_dir / "overview.csv"
            ps_dir = str(output_dir).replace("'", "''")
            ps_path = str(output_path).replace("'", "''")
            payload = self.payload(
                rf'''
$identity = New-Identity
$rowA = [pscustomobject][ordered]@{{ metric_run_id=$identity.MetricRunId; window_type='FULL'; window_start='2017-01-01'; item_value_sum='30.0'; is_all=$true; note=$null }}
$rowB = [pscustomobject][ordered]@{{ metric_run_id=$identity.MetricRunId; window_type='DAY'; window_start='2017-01-01'; item_value_sum='30.00'; is_all='1'; note='x' }}
$columns=@('metric_run_id','window_type','window_start','item_value_sum','is_all','note')
$keys=@('metric_run_id','window_type','window_start')
$first=Get-G2eCanonicalDigest -Rows @($rowA,$rowB) -Columns $columns -UniqueKeyColumns $keys
$second=Get-G2eCanonicalDigest -Rows @($rowB,$rowA) -Columns $columns -UniqueKeyColumns $keys
$changed=[pscustomobject][ordered]@{{ metric_run_id=$identity.MetricRunId; window_type='FULL'; window_start='2017-01-01'; item_value_sum='31.00'; is_all=$true; note=$null }}
$third=Get-G2eCanonicalDigest -Rows @($changed,$rowB) -Columns $columns -UniqueKeyColumns $keys
$overview=@([pscustomobject][ordered]@{{
    window_type='FULL'; window_start='2017-01-01'; window_end='2017-01-01'
    order_count='2'; delivered_order_count='2'; canceled_order_count='0'; unavailable_order_count='0'
    status_eligible_order_count='2'; status_excluded_order_count='0'; delivered_rate='1.000000'
    canceled_rate='0.000000'; unique_customer_count='1'; repeat_customer_count='1'
    repeat_customer_rate='1.000000'; item_row_count='2'; item_value_sum='30.00'
    freight_value_sum='0.00'; payment_value_sum='35.00'; items_per_order_avg='1.000000'
}})
$saved=Export-G2eCandidateCsv -Target overview -Rows $overview -Identity $identity `
    -OutputDirectory '{ps_dir}' -OutputPath '{ps_path}'
$bytes=[IO.File]::ReadAllBytes($saved); $text=[Text.Encoding]::UTF8.GetString($bytes)
[ordered]@{{
    first=$first; stable=($first -ceq $second); changed=($first -cne $third)
    no_bom=(-not ($bytes.Length -ge 3 -and $bytes[0]-eq 239 -and $bytes[1]-eq 187 -and $bytes[2]-eq 191))
    lf_only=($text.Contains("`n") -and -not $text.Contains("`r")); ends_lf=$text.EndsWith("`n")
    header=($text.Split("`n")[0]); escaped=($text -match [regex]::Escape($identity.MetricRunId))
    duplicate=Test-Rejected {{ Get-G2eCanonicalDigest -Rows @($rowA,$rowA) -Columns $columns -UniqueKeyColumns $keys }}
    escape=Test-Rejected {{ Export-G2eCandidateCsv -Target overview -Rows $overview -Identity $identity -OutputDirectory '{ps_dir}' -OutputPath (Join-Path '{ps_dir}' '../escape.csv') }}
}} | ConvertTo-Json -Compress
'''
            )
            self.assertRegex(payload["first"], r"^[0-9a-f]{64}$")
            self.assertTrue(payload["stable"] and payload["changed"])
            self.assertTrue(payload["no_bom"] and payload["lf_only"] and payload["ends_lf"])
            self.assertTrue(payload["header"].startswith("metric_run_id,dataset_id,metric_version"))
            self.assertTrue(payload["escaped"])
            self.assertTrue(payload["duplicate"] and payload["escape"])

    def test_metric_bundle_accepts_anti_fanout_review_and_nonadditive_state_semantics(self):
        payload = self.payload(
            r'''
$identity=New-Identity
$overview=@(); $delivery=@(); $payment=@(); $ranking=@(); $review=@()
foreach ($window in @('DAY','MONTH','FULL')) {
    $overview += [pscustomobject][ordered]@{
        window_type=$window; window_start='2017-01-01'; window_end='2017-01-01'
        order_count='2'; delivered_order_count='2'; canceled_order_count='0'; unavailable_order_count='0'
        status_eligible_order_count='2'; status_excluded_order_count='0'; delivered_rate='1.000000'
        canceled_rate='0.000000'; unique_customer_count='1'; repeat_customer_count='1'
        repeat_customer_rate='1.000000'; item_row_count='2'; item_value_sum='30.00'
        freight_value_sum='0.00'; payment_value_sum='35.00'; items_per_order_avg='1.000000'
    }
    $delivery += [pscustomobject][ordered]@{
        window_type=$window; window_start='2017-01-01'; window_end='2017-01-01'
        delivery_eligible_order_count='2'; delivery_excluded_order_count='0'
        delivery_days_avg='3.000000'; delivery_days_p50='3.000000'; delivery_days_p90='4.000000'
        late_delivery_order_count='1'; late_delivery_eligible_order_count='2'
        late_delivery_excluded_order_count='0'; late_delivery_rate='0.500000'
    }
    $payment += [pscustomobject][ordered]@{
        window_type=$window; window_start='2017-01-01'; window_end='2017-01-01'
        payment_type='__ALL__'; is_all='true'; global_order_count='2'; payment_order_count='1'
        payment_row_count='2'; installment_order_count='1'; payment_type_order_count='1'
        payment_type_value_sum='35.00'
    }, [pscustomobject][ordered]@{
        window_type=$window; window_start='2017-01-01'; window_end='2017-01-01'
        payment_type='credit_card'; is_all='false'; global_order_count='2'; payment_order_count='1'
        payment_row_count='1'; installment_order_count='1'; payment_type_order_count='1'
        payment_type_value_sum='20.00'
    }, [pscustomobject][ordered]@{
        window_type=$window; window_start='2017-01-01'; window_end='2017-01-01'
        payment_type='voucher'; is_all='false'; global_order_count='2'; payment_order_count='1'
        payment_row_count='1'; installment_order_count='0'; payment_type_order_count='1'
        payment_type_value_sum='15.00'
    }
    $ranking += [pscustomobject][ordered]@{
        window_type=$window; window_start='2017-01-01'; window_end='2017-01-01'
        dimension_type='customer_state'; dimension_id='SP'; dimension_name='SP'; is_unknown='false'
        ranking_order_count='2'; ranking_item_row_count=$null; ranking_customer_count='1'
        ranking_item_value_sum=$null; ranking_freight_value_sum=$null
        ranking_payment_value_sum='35.00'; ranking_late_delivery_order_count='1'
        ranking_late_delivery_eligible_order_count='2'; ranking_late_delivery_rate='0.500000'
        payment_value_is_additive='true'
    }, [pscustomobject][ordered]@{
        window_type=$window; window_start='2017-01-01'; window_end='2017-01-01'
        dimension_type='seller_state'; dimension_id='MG'; dimension_name='MG'; is_unknown='false'
        ranking_order_count='1'; ranking_item_row_count='1'; ranking_customer_count='1'
        ranking_item_value_sum='10.00'; ranking_freight_value_sum='0.00'
        ranking_payment_value_sum='35.00'; ranking_late_delivery_order_count='1'
        ranking_late_delivery_eligible_order_count='1'; ranking_late_delivery_rate='1.000000'
        payment_value_is_additive='false'
    }, [pscustomobject][ordered]@{
        window_type=$window; window_start='2017-01-01'; window_end='2017-01-01'
        dimension_type='seller_state'; dimension_id='SP'; dimension_name='SP'; is_unknown='false'
        ranking_order_count='1'; ranking_item_row_count='1'; ranking_customer_count='1'
        ranking_item_value_sum='20.00'; ranking_freight_value_sum='0.00'
        ranking_payment_value_sum='35.00'; ranking_late_delivery_order_count='1'
        ranking_late_delivery_eligible_order_count='1'; ranking_late_delivery_rate='1.000000'
        payment_value_is_additive='false'
    }
    $review += [pscustomobject][ordered]@{
        window_type=$window; window_start='2017-01-01'; window_end='2017-01-01'
        review_row_count='3'; reviewed_order_count='2'; all_order_count='2'
        review_coverage_rate='1.000000'; review_score_avg='4.000000'
        low_score_order_count='0'; low_score_rate='0.000000'; multi_review_order_count='1'
    }
}
$quality=Merge-G2eQualityEvidence -SqlQuality @((New-QualityRow)) -Manifest (New-TestManifest) `
    -SourceReports (New-SourceReports) -CuratedReport (New-CuratedReport)
$result=Assert-G2eMetricBundle -Identity $identity -Overview $overview -Delivery $delivery `
    -Payment $payment -Ranking $ranking -Review $review -Quality @($quality)
$wrongOverview=@($overview | ForEach-Object { $_.PSObject.Copy() }); $wrongOverview[-1].payment_value_sum='70.00'
$wrongDelivery=@($delivery | ForEach-Object { $_.PSObject.Copy() }); $wrongDelivery[-1].delivery_eligible_order_count='1'; $wrongDelivery[-1].delivery_excluded_order_count='1'
$wrongReview=@($review | ForEach-Object { $_.PSObject.Copy() }); $wrongReview[-1].review_row_count='4'
$duplicatePayment=@($payment + $payment[0])
$wrongSeller=@($ranking | ForEach-Object { $_.PSObject.Copy() }); ($wrongSeller | Where-Object { $_.dimension_type -ceq 'seller_state' })[0].payment_value_is_additive='true'
[ordered]@{
    item_sum=$overview[-1].item_value_sum; payment_sum=$overview[-1].payment_value_sum
    review_average=$review[-1].review_score_avg
    grouped_order_sum=(($payment | Where-Object { $_.window_type -ceq 'FULL' -and -not [bool]::Parse($_.is_all) } | Measure-Object payment_order_count -Sum).Sum)
    global_order_count=($payment | Where-Object { $_.window_type -ceq 'FULL' -and [bool]::Parse($_.is_all) }).global_order_count
    seller_payment_sum=(($ranking | Where-Object { $_.window_type -ceq 'FULL' -and $_.dimension_type -ceq 'seller_state' } | Measure-Object ranking_payment_value_sum -Sum).Sum)
    status=$result.Status
    inflated=Test-Rejected { Assert-G2eMetricBundle -Identity $identity -Overview $wrongOverview -Delivery $delivery -Payment $payment -Ranking $ranking -Review $review -Quality @($quality) }
    delivery_rollup=Test-Rejected { Assert-G2eMetricBundle -Identity $identity -Overview $overview -Delivery $wrongDelivery -Payment $payment -Ranking $ranking -Review $review -Quality @($quality) }
    review_rollup=Test-Rejected { Assert-G2eMetricBundle -Identity $identity -Overview $overview -Delivery $delivery -Payment $payment -Ranking $ranking -Review $wrongReview -Quality @($quality) }
    duplicate=Test-Rejected { Assert-G2eMetricBundle -Identity $identity -Overview $overview -Delivery $delivery -Payment $duplicatePayment -Ranking $ranking -Review $review -Quality @($quality) }
    additive_seller=Test-Rejected { Assert-G2eMetricBundle -Identity $identity -Overview $overview -Delivery $delivery -Payment $payment -Ranking $wrongSeller -Review $review -Quality @($quality) }
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual("30.00", payload["item_sum"])
        self.assertEqual("35.00", payload["payment_sum"])
        self.assertEqual("4.000000", payload["review_average"])
        self.assertEqual(2, payload["grouped_order_sum"])
        self.assertEqual("2", payload["global_order_count"])
        self.assertEqual(70, payload["seller_payment_sum"])
        self.assertEqual("PASS", payload["status"])
        self.assertTrue(payload["inflated"] and payload["delivery_rollup"])
        self.assertTrue(payload["review_rollup"] and payload["duplicate"])
        self.assertTrue(payload["additive_seller"])


class G2eRefreshTests(PowerShellTestCase):
    def refresh_payload(self, body: str, executable: str | None = None):
        result = self.run_powershell(
            REFRESH_POWERSHELL_FIXTURE + body, executable=executable
        )
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        return json.loads(result.stdout.strip().splitlines()[-1])

    def test_refresh_plan_and_publication_sequence_are_fixed_and_fail_closed(self):
        payload = self.refresh_payload(
            r'''
$identity = New-Identity
$plan = @(Get-G2eRefreshPlan -MetricRunId $identity.MetricRunId)
$events = [Collections.Generic.List[string]]::new()
$verify = {
    param($Name)
    $events.Add($Name)
    return [pscustomobject]@{ RowCount=1; Sha256=('a' * 64) }
}
$publish = { param($Evidence) $events.Add('publication'); return $Evidence }
$null = Invoke-G2ePublicationSequence -VerifyAction $verify -PublishAction $publish
$failures = [ordered]@{}
foreach ($family in @('overview','delivery','payment','ranking','review','quality')) {
    $failedFamily = $family
    $seen = [Collections.Generic.List[string]]::new()
    $verifyFailure = {
        param($Name)
        $seen.Add($Name)
        if ($Name -ceq $failedFamily) { throw "simulated $Name failure" }
        return [pscustomobject]@{ RowCount=1; Sha256=('b' * 64) }
    }.GetNewClosure()
    $publishFailure = { param($Evidence) $seen.Add('publication') }.GetNewClosure()
    try {
        $null = Invoke-G2ePublicationSequence -VerifyAction $verifyFailure `
            -PublishAction $publishFailure
    } catch {}
    $failures[$family] = @($seen)
}
[ordered]@{
    names=@($plan.Name); targets=@($plan.Target); success=@($events)
    failures=$failures
    unsafe=Test-Rejected { Get-G2eRefreshPlan -MetricRunId 'orders-v1-bad' }
} | ConvertTo-Json -Depth 8 -Compress
'''
        )
        expected = FAMILIES + ["publication"]
        self.assertEqual(expected, payload["names"])
        self.assertEqual(
            [
                "order_metric_overview", "order_metric_delivery", "order_metric_payment",
                "order_metric_ranking", "order_metric_review", "order_metric_quality",
                "order_metric_publications",
            ],
            payload["targets"],
        )
        self.assertEqual(expected, payload["success"])
        for family, events in payload["failures"].items():
            self.assertIn(family, events)
            self.assertNotIn("publication", events)
        self.assertTrue(payload["unsafe"])

    def test_run_lock_is_exclusive_until_the_owner_releases_it(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tmp") as temporary:
            lock_path = str(Path(temporary) / ".refresh.lock").replace("'", "''")
            payload = self.refresh_payload(
                rf'''
$path = '{lock_path}'
$first = Enter-G2eRunLock -Path $path
$blocked = Test-Rejected {{ Enter-G2eRunLock -Path $path }}
$first.Dispose(); [IO.File]::Delete($path)
$second = Enter-G2eRunLock -Path $path
$reacquired = $null -ne $second
$second.Dispose(); [IO.File]::Delete($path)
[ordered]@{{ blocked=$blocked; reacquired=$reacquired }} | ConvertTo-Json -Compress
'''
            )
            self.assertTrue(payload["blocked"])
            self.assertTrue(payload["reacquired"])

    def test_candidate_readback_and_stream_load_response_are_exact(self):
        payload = self.refresh_payload(
            r'''
$identity = New-Identity
$candidate = @(New-StoredOverviewRows)
$stored = @($candidate | ForEach-Object { $_.PSObject.Copy() })
$evidence = Assert-G2eStoredCandidate -Family overview -Identity $identity `
    -CandidateRows $candidate -StoredRows $stored
$changed = @($stored | ForEach-Object { $_.PSObject.Copy() }); $changed[0].order_count='3'
$wrongIdentity = @($stored | ForEach-Object { $_.PSObject.Copy() }); $wrongIdentity[0].metric_run_id='orders-v1-bwrong'
$success = [pscustomobject]@{ Status='Success'; NumberLoadedRows=1; NumberFilteredRows=0 }
[ordered]@{
    rows=$evidence.RowCount; hash=$evidence.Sha256
    changed=Test-Rejected { Assert-G2eStoredCandidate -Family overview -Identity $identity -CandidateRows $candidate -StoredRows $changed }
    identity=Test-Rejected { Assert-G2eStoredCandidate -Family overview -Identity $identity -CandidateRows $candidate -StoredRows $wrongIdentity }
    stream_ok=(-not (Test-Rejected { Assert-G2eStreamLoadResponse -Response $success -ExpectedRows 1 -TableName 'order_metric_overview' }))
    filtered=Test-Rejected { Assert-G2eStreamLoadResponse -Response ([pscustomobject]@{ Status='Success'; NumberLoadedRows=1; NumberFilteredRows=1 }) -ExpectedRows 1 -TableName 'order_metric_overview' }
    short=Test-Rejected { Assert-G2eStreamLoadResponse -Response ([pscustomobject]@{ Status='Success'; NumberLoadedRows=0; NumberFilteredRows=0 }) -ExpectedRows 1 -TableName 'order_metric_overview' }
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(1, payload["rows"])
        self.assertRegex(payload["hash"], r"^[0-9a-f]{64}$")
        self.assertTrue(payload["changed"] and payload["identity"])
        self.assertTrue(payload["stream_ok"] and payload["filtered"] and payload["short"])

    def test_unpublished_partial_candidates_require_exact_evidence(self):
        payload = self.refresh_payload(
            r'''
$expected = New-TestEvidence
$partial = [ordered]@{}
foreach ($name in @('overview','delivery','payment','ranking','review','quality')) {
    $partial[$name] = if ($name -in @('overview','payment')) { $expected[$name] } else { $null }
}
$state = Assert-G2eUnpublishedCandidateState -ExpectedEvidence $expected -StoredEvidence $partial
$bad = [ordered]@{}; foreach ($name in $partial.Keys) { $bad[$name]=$partial[$name] }
$bad.overview = [pscustomobject]@{ RowCount=1; Sha256=('f' * 64) }
$unknown = [ordered]@{}; foreach ($name in $partial.Keys) { $unknown[$name]=$partial[$name] }; $unknown.other=$null
[ordered]@{
    status=$state.status; reuse=@($state.reuse); load=@($state.load)
    mismatch=Test-Rejected { Assert-G2eUnpublishedCandidateState -ExpectedEvidence $expected -StoredEvidence $bad }
    unknown=Test-Rejected { Assert-G2eUnpublishedCandidateState -ExpectedEvidence $expected -StoredEvidence $unknown }
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual("continue", payload["status"])
        self.assertEqual(["overview", "payment"], payload["reuse"])
        self.assertEqual(["delivery", "ranking", "review", "quality"], payload["load"])
        self.assertTrue(payload["mismatch"] and payload["unknown"])

    def test_publication_record_retry_and_insert_are_fail_closed(self):
        payload = self.refresh_payload(
            r'''
$identity = New-Identity; $evidence = New-TestEvidence
$record = New-G2ePublicationRecord -Identity $identity -Evidence $evidence `
    -PublishedAt '2026-09-22T08:01:00Z'
$candidates = [ordered]@{}
foreach ($name in $evidence.Keys) {
    $candidates[$name] = [pscustomobject]@{
        RowCount=$evidence[$name].RowCount; CanonicalSha256=$evidence[$name].Sha256
    }
}
$candidateRecord = New-G2ePublicationRecord -Identity $identity -Candidates $candidates `
    -PublishedAt '2026-09-22T08:01:00Z'
$already = Assert-G2eExistingPublication -Identity $identity -Publication $record `
    -StoredEvidence $evidence
$badHash = $record.PSObject.Copy(); $badHash.ranking_sha256=('f' * 64)
$badRevision = $record.PSObject.Copy(); $badRevision.implementation_revision=('2' * 40)
$badRange = $record.PSObject.Copy(); $badRange.window_end='2017-01-02'
$sql = New-G2ePublicationInsertSql -Publication $record
[ordered]@{
    status=$already.status; publication_status=$record.status
    candidates_alias=($candidateRecord.ranking_sha256 -ceq $record.ranking_sha256)
    source_keys=@(($record.source_snapshots_json | ConvertFrom-Json).PSObject.Properties.Name)
    curated_keys=@(($record.curated_snapshots_json | ConvertFrom-Json).PSObject.Properties.Name)
    hash=Test-Rejected { Assert-G2eExistingPublication -Identity $identity -Publication $badHash -StoredEvidence $evidence }
    revision=Test-Rejected { Assert-G2eExistingPublication -Identity $identity -Publication $badRevision -StoredEvidence $evidence }
    range=Test-Rejected { Assert-G2eExistingPublication -Identity $identity -Publication $badRange -StoredEvidence $evidence }
    insert=($sql -match '^INSERT INTO analytics\.order_metric_publications \(')
    unsafe=($sql -match '(?im)\b(UPDATE|DELETE|DROP|TRUNCATE|REPLACE)\b')
} | ConvertTo-Json -Depth 5 -Compress
'''
        )
        self.assertEqual("already_published", payload["status"])
        self.assertEqual("PUBLISHED", payload["publication_status"])
        self.assertTrue(payload["candidates_alias"])
        self.assertEqual(sorted(SOURCE_TABLES.values()), sorted(payload["source_keys"]))
        self.assertEqual(sorted(CURATED_TABLES), sorted(payload["curated_keys"]))
        self.assertTrue(payload["hash"] and payload["revision"] and payload["range"])
        self.assertTrue(payload["insert"])
        self.assertFalse(payload["unsafe"])

    def test_metrics_paths_and_report_path_are_fixed_to_the_d_drive_workspace(self):
        payload = self.refresh_payload(
            r'''
$paths = Get-G2eMetricsPaths -SourceBundleSha256 ('a' * 64)
$accepted = Assert-G2eReportPath -SourceBundleSha256 ('a' * 64) -Path $paths.ReportPath
[ordered]@{
    directory=$paths.MetricsDirectory; report=$paths.ReportPath; accepted=$accepted
    c_drive=Test-Rejected { Assert-G2eReportPath -SourceBundleSha256 ('a' * 64) -Path 'C:\temp\refresh.json' }
    outside=Test-Rejected { Assert-G2eReportPath -SourceBundleSha256 ('a' * 64) -Path (Join-Path $script:G2eProjectRoot 'tmp\refresh.json') }
    wrong_name=Test-Rejected { Assert-G2eReportPath -SourceBundleSha256 ('a' * 64) -Path (Join-Path $paths.MetricsDirectory 'other.json') }
    unresolved=Test-Rejected { Assert-G2eRenderedMetricSql -Sql 'SELECT * FROM x FOR VERSION AS OF __ORDER_FACT_SNAPSHOT__;' }
    sentinels=(-not (Test-Rejected { Assert-G2eRenderedMetricSql -Sql "SELECT '__ALL__', '__UNKNOWN__';" }))
    unquoted_sentinel=Test-Rejected { Assert-G2eRenderedMetricSql -Sql 'SELECT __ALL__;' }
} | ConvertTo-Json -Compress
'''
        )
        self.assertTrue(payload["directory"].lower().startswith("d:\\"))
        self.assertTrue(payload["report"].endswith("metrics\\refresh.json"))
        self.assertEqual(payload["report"].lower(), payload["accepted"].lower())
        self.assertTrue(payload["c_drive"] and payload["outside"])
        self.assertTrue(payload["wrong_name"] and payload["unresolved"])
        self.assertTrue(payload["sentinels"] and payload["unquoted_sentinel"])

    def test_refresh_transport_is_fixed_strict_and_process_local(self):
        script = (ROOT / "scripts/refresh_g2e_order_metrics.ps1").read_text(
            encoding="utf-8"
        )
        for function_name in (
            "Get-G2eRefreshPlan", "Enter-G2eRunLock", "Invoke-G2ePublicationSequence",
            "New-G2ePublicationRecord", "Invoke-G2eRefresh",
        ):
            self.assertRegex(script, rf"(?m)^function {function_name} \{{$")
        for required in (
            "TERM=dumb", "Expect:100-continue",
            "strict_mode:true", "max_filter_ratio:0", "skip_lines:1",
            'enclose:"', "trim_double_quotes:true",
        ):
            self.assertIn(required, script)
        self.assertNotIn("$env:PATH =", script)
        self.assertNotIn("setx", script.lower())

    def test_trino_transport_preserves_a_dimension_name_containing_a_comma(self):
        payload = self.refresh_payload(
            r'''
$script:G2eDockerExecutable = 'docker-fixture'
$capturedFormat = ''
function Invoke-G2eComposeCommand {
    param([string]$EnvFile, [string[]]$Arguments)
    $formatIndex = [Array]::IndexOf($Arguments, '--output-format')
    $script:capturedFormat = $Arguments[$formatIndex + 1]
    if ($script:capturedFormat -ceq 'CSV_HEADER') {
        return @('"dimension_name"', '"sao paulo, sp"')
    }
    return @('dimension_name', 'sao paulo", sp')
}
$rows = @(Invoke-G2eMetricTrinoStatement -Name ranking -Sql 'SELECT 1;' -EnvFile 'fixture.env')
[ordered]@{ format=$script:capturedFormat; value=$rows[0].dimension_name } |
    ConvertTo-Json -Compress
'''
        )
        self.assertEqual("CSV_HEADER", payload["format"])
        self.assertEqual("sao paulo, sp", payload["value"])

    def test_trino_transport_keeps_success_warnings_out_of_csv_rows(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tmp") as temporary:
            executable = Path(temporary) / "docker-fixture.cmd"
            executable.write_text(
                '@echo off\n'
                '>&2 echo WARNING: diagnostic only\n'
                'echo "dimension_name"\n'
                'echo "sao paulo, sp"\n'
                'exit /b 0\n',
                encoding="ascii",
            )
            escaped = str(executable).replace("'", "''")
            payload = self.refresh_payload(
                rf'''
$script:G2eDockerExecutable = '{escaped}'
$rows = @(Invoke-G2eMetricTrinoStatement -Name ranking -Sql 'SELECT 1;' -EnvFile 'fixture.env')
[ordered]@{{ count=$rows.Count; value=$rows[0].dimension_name }} |
    ConvertTo-Json -Compress
'''
            )
            self.assertEqual(1, payload["count"])
            self.assertEqual("sao paulo, sp", payload["value"])

    def test_compose_success_stderr_is_nonterminating_in_windows_powershell(self):
        executable = shutil.which("powershell")
        if executable is None:
            self.skipTest("Windows PowerShell is unavailable")
        with tempfile.TemporaryDirectory(dir=ROOT / "tmp") as temporary:
            fixture = Path(temporary) / "docker-fixture.cmd"
            failure_fixture = Path(temporary) / "docker-failure-fixture.cmd"
            fixture.write_text(
                '@echo off\n'
                '>&2 echo WARNING: diagnostic only\n'
                'echo ready\n'
                'exit /b 0\n',
                encoding="ascii",
            )
            failure_fixture.write_text(
                '@echo off\n'
                'echo partial output\n'
                '>&2 echo ERROR: real failure\n'
                'exit /b 9\n',
                encoding="ascii",
            )
            escaped = str(fixture).replace("'", "''")
            failure_escaped = str(failure_fixture).replace("'", "''")
            payload = self.refresh_payload(
                rf'''
$script:G2eDockerExecutable = '{escaped}'
$lines = @(Invoke-G2eComposeCommand -EnvFile 'fixture.env' -Arguments @('config'))
$script:G2eDockerExecutable = '{failure_escaped}'
$failure = ''
try {{ $null = Invoke-G2eComposeCommand -EnvFile 'fixture.env' -Arguments @('config') }}
catch {{ $failure = $_.Exception.Message }}
[ordered]@{{
    lines=$lines
    preference=[string]$ErrorActionPreference
    failure=$failure
}} |
    ConvertTo-Json -Compress
''',
                executable=executable,
            )
            self.assertEqual(["ready"], payload["lines"])
            self.assertEqual("Stop", payload["preference"])
            self.assertIn("partial output", payload["failure"])
            self.assertIn("ERROR: real failure", payload["failure"])

    def test_compose_preserves_literal_quotes_in_windows_powershell(self):
        executable = shutil.which("powershell")
        if executable is None:
            self.skipTest("Windows PowerShell is unavailable")
        with tempfile.TemporaryDirectory(dir=ROOT / "tmp") as temporary:
            fixture = Path(temporary) / "compose"
            fixture.write_text("import sys\nprint(sys.argv[-1])\n", encoding="ascii")
            python_executable = sys.executable.replace("'", "''")
            temporary_path = str(temporary).replace("'", "''")
            payload = self.refresh_payload(
                rf'''
$script:G2eDockerExecutable = '{python_executable}'
$sql = 'SELECT * FROM lakehouse.olist."orders_src_v1$snapshots"'
Push-Location '{temporary_path}'
try {{
    $lines = @(Invoke-G2eComposeCommand -EnvFile 'fixture.env' -Arguments @(
        'exec', '-T', '--execute', $sql
    ))
}} finally {{ Pop-Location }}
[ordered]@{{ sql=$lines[0] }} | ConvertTo-Json -Compress
''',
                executable=executable,
            )
            self.assertEqual(
                'SELECT * FROM lakehouse.olist."orders_src_v1$snapshots"',
                payload["sql"],
            )

    def test_trino_transport_maps_its_quoted_empty_null_encoding_to_null(self):
        payload = self.refresh_payload(
            r'''
$script:G2eDockerExecutable = 'docker-fixture'
function Invoke-G2eComposeCommand {
    param([string]$EnvFile, [string[]]$Arguments)
    return @(
        '"dimension_name","ranking_payment_value_sum"',
        '"seller",""'
    )
}
$rows = @(Invoke-G2eMetricTrinoStatement -Name ranking -Sql 'SELECT 1;' -EnvFile 'fixture.env')
[ordered]@{
    name=$rows[0].dimension_name
    value_is_null=($null -eq $rows[0].ranking_payment_value_sum)
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual("seller", payload["name"])
        self.assertTrue(payload["value_is_null"])

    def test_trino_transport_maps_its_backslash_n_null_encoding_to_null(self):
        payload = self.refresh_payload(
            r'''
$script:G2eDockerExecutable = 'docker-fixture'
function Invoke-G2eComposeCommand {
    param([string]$EnvFile, [string[]]$Arguments)
    return @(
        'window_type,delivery_days_avg',
        'DAY,\N'
    )
}
$raw = @(ConvertFrom-G2eCsv "window_type,delivery_days_avg`nDAY,\N`n")
$rows = @(Invoke-G2eMetricTrinoStatement -Name delivery -Sql 'SELECT 1;' -EnvFile 'fixture.env')
[ordered]@{
    generic_value=$raw[0].delivery_days_avg
    metric_value_is_null=($null -eq $rows[0].delivery_days_avg)
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(r"\N", payload["generic_value"])
        self.assertTrue(payload["metric_value_is_null"])

    def test_trino_readiness_requires_a_successful_query_not_only_http_info(self):
        payload = self.refresh_payload(
            r'''
$script:attempts = 0
function Invoke-RestMethod { return [pscustomobject]@{ starting=$true } }
function Invoke-G2eMetricTrinoStatement {
    param([string]$Name, [string]$Sql, [string]$EnvFile)
    $script:attempts++
    if ($script:attempts -eq 1) { throw 'Trino server is still initializing' }
    return @([pscustomobject]@{ ready='1' })
}
Wait-G2eTrinoDependency -EnvFile 'fixture.env' -TimeoutSeconds 5
[ordered]@{ attempts=$script:attempts } | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(2, payload["attempts"])

    def test_metric_output_rejects_physical_link_escape_when_supported(self):
        payload = self.refresh_payload(
            r'''
$base = Join-Path ([IO.Path]::GetTempPath()) ('g2e-link-' + [guid]::NewGuid().ToString('N'))
$project = Join-Path $base 'project'; $outside = Join-Path $base 'outside'
$link = Join-Path $project 'tmp'; $supported = $true; $rejected = $false
$null = New-Item -ItemType Directory -Path $project
$null = New-Item -ItemType Directory -Path $outside
try {
    try { $null = New-Item -ItemType SymbolicLink -Path $link -Target $outside -ErrorAction Stop }
    catch { $supported = $false }
    if ($supported) {
        $candidate = Join-Path $link 'graduation/g2e/file.txt'
        $rejected = Test-Rejected {
            Assert-G2eRefreshNoReparsePoint -RootPath $project -CandidatePath $candidate
        }
    }
    [ordered]@{ supported=$supported; rejected=$rejected } | ConvertTo-Json -Compress
} finally {
    if (Test-Path -LiteralPath $link) { Remove-Item -LiteralPath $link -Force }
    if (Test-Path -LiteralPath $base) {
        $tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
        $safeBase = [IO.Path]::GetFullPath($base)
        if (-not $safeBase.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase)) {
            throw 'Refusing unsafe test cleanup.'
        }
        Remove-Item -LiteralPath $base -Recurse -Force
    }
}
'''
        )
        if not payload["supported"]:
            self.skipTest("Symbolic links are unavailable on this Windows host")
        self.assertTrue(payload["rejected"])


class IndependentBusinessSemanticsTests(unittest.TestCase):
    def test_two_payments_do_not_multiply_two_items(self):
        items = [("o1", 10), ("o1", 20)]
        payments = [("o1", "credit_card", 20), ("o1", "voucher", 15)]
        item_per_order = {"o1": sum(value for _, value in items)}
        payment_per_order = {"o1": sum(value for _, _, value in payments)}
        metrics = {
            "item_value_sum": f"{sum(item_per_order.values()):.2f}",
            "payment_value_sum": f"{sum(payment_per_order.values()):.2f}",
        }
        self.assertEqual("30.00", metrics["item_value_sum"])
        self.assertEqual("35.00", metrics["payment_value_sum"])

    def test_order_level_review_average_gives_each_order_one_vote(self):
        reviews = {"A": [1, 5], "B": [5]}
        order_scores = [sum(scores) / len(scores) for scores in reviews.values()]
        metrics = {"review_score_avg": f"{sum(order_scores) / len(order_scores):.6f}"}
        self.assertEqual("4.000000", metrics["review_score_avg"])


if __name__ == "__main__":
    unittest.main()
