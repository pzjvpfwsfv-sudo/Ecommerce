from hashlib import sha256
import json
from pathlib import Path
import re
import shutil
import subprocess
import unittest

from generators.olist_data.schemas import TABLE_SPECS


ROOT = Path(__file__).resolve().parent.parent
BUNDLE = "a" * 64

STAGES = [
    "customer_dim",
    "category_dim",
    "product_dim",
    "seller_dim",
    "geolocation_dim",
    "order_fact",
    "order_item_fact",
    "payment_fact",
    "review_fact",
]

SOURCE_TOKENS = {
    f"__{entity.upper()}_SRC_SNAPSHOT__" for entity in TABLE_SPECS
}
CURATED_TOKENS = {
    "__CUSTOMER_DIM_SNAPSHOT__",
    "__CATEGORY_DIM_SNAPSHOT__",
    "__PRODUCT_DIM_SNAPSHOT__",
    "__SELLER_DIM_SNAPSHOT__",
    "__GEOLOCATION_DIM_SNAPSHOT__",
    "__ORDER_FACT_SNAPSHOT__",
    "__ORDER_ITEM_FACT_SNAPSHOT__",
    "__PAYMENT_FACT_SNAPSHOT__",
    "__REVIEW_FACT_SNAPSHOT__",
}


POWERSHELL_FIXTURE = r'''
function New-SourceReports {
    $counts = [ordered]@{
        orders = 1; order_items = 2; order_payments = 2; order_reviews = 2
        customers = 1; products = 2; sellers = 2; geolocation = 3; category_translation = 2
    }
    $reports = @()
    $index = 100
    foreach ($entity in (Get-G2eRegistry).Keys) {
        $spec = (Get-G2eRegistry)[$entity]
        $reports += [pscustomobject][ordered]@{
            status = 'PASS'
            dataset_id = 'olist-brazilian-ecommerce-v2'
            entity = $entity
            source_bundle_sha256 = ('a' * 64)
            normalized_sha256 = ('b' * 64)
            source_row_id_sequence_sha256 = ('c' * 64)
            expected_row_count = $counts[$entity]
            target_table = $spec.TargetTable
            source_file = $spec.SourceFile
            iceberg = [pscustomobject][ordered]@{
                row_count = $counts[$entity]
                snapshot_id = [string]$index
                snapshot_committed_at = '2026-09-22 08:00:00.000 UTC'
            }
        }
        $index++
    }
    return $reports
}

function New-CuratedSnapshots {
    return [ordered]@{
        customer_dim_v1 = '201'
        category_dim_v1 = '202'
        product_dim_v1 = '203'
        seller_dim_v1 = '204'
        geolocation_dim_v1 = '205'
        order_fact_v1 = '206'
        order_item_fact_v1 = '207'
        payment_fact_v1 = '208'
        review_fact_v1 = '209'
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
            self.skipTest("PowerShell is required for G2-E curated coverage")
        return subprocess.run(
            [executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", command],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=25,
            check=False,
        )


class OlistCuratedSqlContractTest(unittest.TestCase):
    def test_model_has_nine_fixed_non_destructive_snapshot_pinned_stages(self):
        sql = (ROOT / "jobs/sql/21_olist_curated_model.sql.template").read_text(
            encoding="utf-8"
        )
        self.assertEqual(STAGES, re.findall(r"(?m)^-- stage:([a-z_]+)$", sql))
        self.assertEqual(9, len(re.findall(r"(?im)^CREATE TABLE IF NOT EXISTS ", sql)))
        self.assertNotRegex(sql, r"(?im)^\s*(DROP|DELETE|TRUNCATE|CREATE OR REPLACE)\b")
        for entity, spec in TABLE_SPECS.items():
            token = f"__{entity.upper()}_SRC_SNAPSHOT__"
            self.assertRegex(
                sql,
                re.escape(f"lakehouse.olist.{spec.target_table} FOR VERSION AS OF {token}"),
            )
        tokens = set(re.findall(r"__[A-Z0-9_]+__", sql))
        self.assertTrue(SOURCE_TOKENS <= tokens)
        self.assertIn("__SOURCE_BUNDLE_SHA256__", tokens)
        self.assertIn("__SOURCE_SNAPSHOT_SET_SHA256__", tokens)

    def test_curated_grains_columns_and_anti_fanout_shape_are_explicit(self):
        sql = (ROOT / "jobs/sql/21_olist_curated_model.sql.template").read_text(
            encoding="utf-8"
        )
        for table in (
            "customer_dim_v1",
            "category_dim_v1",
            "product_dim_v1",
            "seller_dim_v1",
            "geolocation_dim_v1",
            "order_fact_v1",
            "order_item_fact_v1",
            "payment_fact_v1",
            "review_fact_v1",
        ):
            self.assertIn(f"lakehouse.olist.{table}", sql)
        self.assertIn("row_number() OVER", sql)
        self.assertIn("ORDER BY city_state_count DESC, geolocation_city, geolocation_state", sql)
        self.assertIn("avg(geolocation_lat)", sql.lower())
        self.assertIn("source_point_count", sql)
        self.assertIn("source_snapshot_set_sha256", sql)

        item_block = sql.split("-- stage:order_item_fact", 1)[1].split(
            "-- stage:payment_fact", 1
        )[0]
        self.assertIn("order_items_src_v1", item_block)
        self.assertIn("product_dim_v1", item_block)
        self.assertNotIn("order_payments_src_v1", item_block)
        self.assertNotIn("order_reviews_src_v1", item_block)
        self.assertNotIn("payment_fact_v1", item_block)
        self.assertNotIn("review_fact_v1", item_block)

        review_block = sql.split("-- stage:review_fact", 1)[1]
        self.assertNotIn("review_comment_title", review_block)
        self.assertNotIn("review_comment_message", review_block)
        self.assertIn("valid_review_score", review_block)

    def test_verification_template_separates_hard_and_reportable_quality(self):
        sql = (ROOT / "jobs/sql/22_olist_curated_verify.sql.template").read_text(
            encoding="utf-8"
        )
        self.assertEqual(
            [
                "hard_gate",
                "reportable_quality",
                "grain_reconciliation",
                "anti_fanout",
                "snapshot_identity",
            ],
            re.findall(r"(?m)^-- result:([a-z_]+)$", sql),
        )
        for alias in (
            "duplicate_order_key_count",
            "duplicate_customer_key_count",
            "duplicate_product_key_count",
            "duplicate_seller_key_count",
            "duplicate_category_key_count",
            "duplicate_item_key_count",
            "duplicate_payment_key_count",
            "orphan_order_customer_count",
            "orphan_item_order_count",
            "orphan_item_product_count",
            "orphan_item_seller_count",
            "orphan_payment_order_count",
            "orphan_review_order_count",
            "blank_required_id_count",
            "mixed_bundle_table_count",
            "negative_amount_count",
            "invalid_purchase_time_count",
            "invalid_optional_time_count",
        ):
            self.assertIn(f"AS {alias}", sql)
        for alias in (
            "duplicate_review_id_count",
            "unknown_order_status_count",
            "unknown_payment_type_count",
            "missing_product_category_count",
            "missing_category_translation_count",
            "multi_review_order_count",
            "missing_optional_time_count",
            "lifecycle_order_anomaly_count",
            "payment_item_total_mismatch_count",
        ):
            self.assertIn(f"AS {alias}", sql)
        self.assertIn(
            "array_agg(source_row_id ORDER BY source_row_number)",
            sql,
        )
        self.assertNotRegex(sql, r"(?im)^\s*(DROP|DELETE|TRUNCATE|CREATE OR REPLACE)\b")


class OlistCuratedIdentityTest(PowerShellTestCase):
    def test_stage_report_requires_the_complete_source_snapshot_map(self):
        command = (
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path 'scripts/build_g2e_olist_curated.ps1') -FunctionsOnly
'''
            + POWERSHELL_FIXTURE
            + r'''
$identity = Get-G2eSourceSnapshotIdentity -Reports (New-SourceReports)
$report = [pscustomobject][ordered]@{
    status = 'PASS'; stage = 'order_fact'; target_table = 'order_fact_v1'
    source_bundle_sha256 = $identity.SourceBundleSha256
    source_snapshot_set_sha256 = $identity.SourceSnapshotSetSha256
    source_snapshots = $identity.SourceSnapshots
    row_count = 1; snapshot_id = '700'
}
$valid = Assert-G2eCuratedStageReport $report 'order_fact' $identity
$wrongMap = $report.PSObject.Copy()
$changed = [ordered]@{}
foreach ($key in $identity.SourceSnapshots.Keys) { $changed[$key] = $identity.SourceSnapshots[$key] }
$changed['orders_src_v1'] = '999'
$wrongMap.source_snapshots = $changed
$missingMap = $report.PSObject.Copy()
$missingMap.PSObject.Properties.Remove('source_snapshots')
[ordered]@{
    snapshot = $valid.snapshot_id
    wrong_map = Test-Rejected { Assert-G2eCuratedStageReport $wrongMap 'order_fact' $identity }
    missing_map = Test-Rejected { Assert-G2eCuratedStageReport $missingMap 'order_fact' $identity }
} | ConvertTo-Json -Compress
'''
        )
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("700", payload["snapshot"])
        self.assertTrue(payload["wrong_map"])
        self.assertTrue(payload["missing_map"])

    def test_source_reports_create_one_canonical_snapshot_identity(self):
        command = (
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path 'scripts/build_g2e_olist_curated.ps1') -FunctionsOnly
'''
            + POWERSHELL_FIXTURE
            + r'''
$identity = Get-G2eSourceSnapshotIdentity -Reports (New-SourceReports)
[ordered]@{
    bundle = $identity.SourceBundleSha256
    digest = $identity.SourceSnapshotSetSha256
    snapshots = $identity.SourceSnapshots
    counts = $identity.SourceRowCounts
} | ConvertTo-Json -Depth 6 -Compress
'''
        )
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        expected_snapshots = {
            spec.target_table: str(100 + index)
            for index, spec in enumerate(TABLE_SPECS.values())
        }
        canonical = json.dumps(
            dict(sorted(expected_snapshots.items())),
            ensure_ascii=False,
            separators=(",", ":"),
        )
        self.assertEqual(BUNDLE, payload["bundle"])
        self.assertEqual(expected_snapshots, payload["snapshots"])
        self.assertEqual(sha256(canonical.encode()).hexdigest(), payload["digest"])
        self.assertEqual("1", str(payload["counts"]["orders_src_v1"]))
        self.assertEqual("2", str(payload["counts"]["order_items_src_v1"]))

    def test_source_report_set_rejects_every_identity_gap(self):
        command = (
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path 'scripts/build_g2e_olist_curated.ps1') -FunctionsOnly
'''
            + POWERSHELL_FIXTURE
            + r'''
function Test-Mutated([scriptblock]$Mutation) {
    $reports = @(New-SourceReports)
    & $Mutation $reports
    return Test-Rejected { Get-G2eSourceSnapshotIdentity -Reports $reports }
}
[ordered]@{
    missing = Test-Rejected { Get-G2eSourceSnapshotIdentity -Reports @((New-SourceReports)[0..7]) }
    duplicate = Test-Rejected {
        $r = @(New-SourceReports); Get-G2eSourceSnapshotIdentity -Reports @($r + $r[0])
    }
    wrong_status = Test-Mutated { param($r) $r[0].status = 'FAIL' }
    mixed_bundle = Test-Mutated { param($r) $r[1].source_bundle_sha256 = ('f' * 64) }
    zero_snapshot = Test-Mutated { param($r) $r[2].iceberg.snapshot_id = '0' }
    wrong_table = Test-Mutated { param($r) $r[3].target_table = 'other_table' }
    wrong_rows = Test-Mutated { param($r) $r[4].iceberg.row_count = 999 }
    unknown_entity = Test-Mutated { param($r) $r[5].entity = 'unknown' }
} | ConvertTo-Json -Compress
'''
        )
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(all(payload.values()), payload)

    def test_renderer_accepts_only_exact_positive_snapshot_maps(self):
        command = (
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path 'scripts/build_g2e_olist_curated.ps1') -FunctionsOnly
'''
            + POWERSHELL_FIXTURE
            + r'''
$identity = Get-G2eSourceSnapshotIdentity -Reports (New-SourceReports)
$template = Get-Content -LiteralPath 'jobs/sql/21_olist_curated_model.sql.template' -Raw -Encoding UTF8
$snapshots = New-CuratedSnapshots
$sql = Render-G2eCuratedModel -Template $template -Identity $identity -CuratedSnapshots $snapshots
$stages = @(Split-G2eCuratedStages -Sql $sql)
[ordered]@{
    stage_names = @($stages.Name)
    unresolved = [bool]($sql -match '__[A-Z0-9_]+__')
    pinned_count = ([regex]::Matches($sql, 'FOR VERSION AS OF [1-9][0-9]*')).Count
    missing_curated = Test-Rejected {
        $bad = New-CuratedSnapshots; $bad.Remove('product_dim_v1')
        Render-G2eCuratedModel $template $identity $bad
    }
    zero_curated = Test-Rejected {
        $bad = New-CuratedSnapshots; $bad['product_dim_v1'] = '0'
        Render-G2eCuratedModel $template $identity $bad
    }
    extra_curated = Test-Rejected {
        $bad = New-CuratedSnapshots; $bad['other'] = '999'
        Render-G2eCuratedModel $template $identity $bad
    }
    bad_digest = Test-Rejected {
        $copy = $identity.PSObject.Copy(); $copy.SourceSnapshotSetSha256 = ('A' * 64)
        Render-G2eCuratedModel $template $copy $snapshots
    }
    unknown_token = Test-Rejected {
        Render-G2eCuratedModel ($template + "`n__UNKNOWN__") $identity $snapshots
    }
} | ConvertTo-Json -Depth 5 -Compress
'''
        )
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(STAGES, payload["stage_names"])
        self.assertFalse(payload["unresolved"])
        self.assertGreaterEqual(payload["pinned_count"], 12)
        self.assertTrue(payload["missing_curated"])
        self.assertTrue(payload["zero_curated"])
        self.assertTrue(payload["extra_curated"])
        self.assertTrue(payload["bad_digest"])
        self.assertTrue(payload["unknown_token"])


class OlistCuratedGateTest(PowerShellTestCase):
    def test_every_hard_gate_is_zero_while_reportable_counts_may_be_nonzero(self):
        command = (
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path 'scripts/verify_g2e_olist_curated.ps1') -FunctionsOnly
'''
            + POWERSHELL_FIXTURE
            + r'''
$hard = [ordered]@{}
foreach ($name in Get-G2eHardGateFields) { $hard[$name] = 0 }
$hardPass = -not (Test-Rejected { Assert-G2eZeroHardGate ([pscustomobject]$hard) })
$rejected = 0
foreach ($name in Get-G2eHardGateFields) {
    $mutated = [ordered]@{}
    foreach ($field in Get-G2eHardGateFields) { $mutated[$field] = 0 }
    $mutated[$name] = 1
    if (Test-Rejected { Assert-G2eZeroHardGate ([pscustomobject]$mutated) }) { $rejected++ }
}
$quality = [ordered]@{}
$value = 1
foreach ($name in Get-G2eReportableQualityFields) { $quality[$name] = $value; $value++ }
$qualityResult = Assert-G2eReportableQuality ([pscustomobject]$quality)
$badQuality = [ordered]@{}
foreach ($name in Get-G2eReportableQualityFields) { $badQuality[$name] = 0 }
$badQuality[(Get-G2eReportableQualityFields)[0]] = -1
[ordered]@{
    hard_pass = $hardPass
    hard_field_count = @(Get-G2eHardGateFields).Count
    hard_rejected = $rejected
    quality_sum = $qualityResult.TotalReportableCount
    negative_quality = Test-Rejected { Assert-G2eReportableQuality ([pscustomobject]$badQuality) }
} | ConvertTo-Json -Compress
'''
        )
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["hard_pass"])
        self.assertEqual(payload["hard_field_count"], payload["hard_rejected"])
        self.assertEqual(sum(range(1, 10)), payload["quality_sum"])
        self.assertTrue(payload["negative_quality"])

    def test_synthetic_multi_fact_order_does_not_fan_out(self):
        command = (
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path 'scripts/verify_g2e_olist_curated.ps1') -FunctionsOnly
'''
            + POWERSHELL_FIXTURE
            + r'''
$valid = [pscustomobject][ordered]@{
    source_order_count = 1; fact_order_count = 1
    source_item_count = 2; fact_item_count = 2
    source_payment_count = 2; fact_payment_count = 2
    source_review_count = 2; fact_review_count = 2
    source_item_value_sum = '30.00'; fact_item_value_sum = '30.00'
    source_freight_value_sum = '5.00'; fact_freight_value_sum = '5.00'
    source_payment_value_sum = '35.00'; fact_payment_value_sum = '35.00'
}
$evidence = Assert-G2eAntiFanout $valid
$multiplied = $valid.PSObject.Copy(); $multiplied.fact_item_value_sum = '120.00'
$lostReview = $valid.PSObject.Copy(); $lostReview.fact_review_count = 1
[ordered]@{
    order_count = $evidence.OrderCount
    item_row_count = $evidence.ItemRowCount
    payment_row_count = $evidence.PaymentRowCount
    review_row_count = $evidence.ReviewRowCount
    item_value_sum = $evidence.ItemValueSum
    payment_value_sum = $evidence.PaymentValueSum
    multiplied_rejected = Test-Rejected { Assert-G2eAntiFanout $multiplied }
    lost_review_rejected = Test-Rejected { Assert-G2eAntiFanout $lostReview }
} | ConvertTo-Json -Compress
'''
        )
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(1, payload["order_count"])
        self.assertEqual(2, payload["item_row_count"])
        self.assertEqual(2, payload["payment_row_count"])
        self.assertEqual(2, payload["review_row_count"])
        self.assertEqual("30.00", payload["item_value_sum"])
        self.assertEqual("35.00", payload["payment_value_sum"])
        self.assertTrue(payload["multiplied_rejected"])
        self.assertTrue(payload["lost_review_rejected"])

    def test_grain_and_resume_require_exact_counts_identity_and_snapshots(self):
        command = (
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path 'scripts/verify_g2e_olist_curated.ps1') -FunctionsOnly
'''
            + POWERSHELL_FIXTURE
            + r'''
$grain = [ordered]@{}
foreach ($stage in Get-G2eCuratedStageOrder) {
    $grain["${stage}_expected_count"] = 2
    $grain["${stage}_row_count"] = 2
    $grain["${stage}_distinct_key_count"] = 2
}
$grainPass = -not (Test-Rejected { Assert-G2eGrainReconciliation ([pscustomobject]$grain) })
$badGrain = [ordered]@{}
foreach ($key in $grain.Keys) { $badGrain[$key] = $grain[$key] }
$badGrain['payment_fact_distinct_key_count'] = 1
$expected = [pscustomobject][ordered]@{
    Stage = 'order_fact'; ExpectedRowCount = 1; SourceBundleSha256 = ('a' * 64)
    SourceSnapshotSetSha256 = ('d' * 64); RecordedSnapshotId = '700'
}
$observed = [pscustomobject][ordered]@{
    table_exists = 1; row_count = 1; distinct_key_count = 1
    bundle_count = 1; source_bundle_sha256 = ('a' * 64)
    snapshot_set_count = 1; source_snapshot_set_sha256 = ('d' * 64)
    snapshot_count = 1; latest_snapshot_id = '700'
}
$verified = Assert-G2eCuratedStageState $observed $expected
$absent = Assert-G2eCuratedStageState ([pscustomobject]@{ table_exists = 0 }) $expected
$empty = Assert-G2eCuratedStageState ([pscustomobject]@{
    table_exists = 1; row_count = 0; distinct_key_count = 0
    bundle_count = 0; source_bundle_sha256 = $null
    snapshot_set_count = 0; source_snapshot_set_sha256 = $null
    snapshot_count = 0; latest_snapshot_id = $null
}) $expected
function Test-StateMutation([string]$Name, $Value) {
    $copy = $observed.PSObject.Copy(); $copy.$Name = $Value
    return Test-Rejected { Assert-G2eCuratedStageState $copy $expected }
}
[ordered]@{
    grain_pass = $grainPass
    grain_conflict = Test-Rejected { Assert-G2eGrainReconciliation ([pscustomobject]$badGrain) }
    verified = $verified.Kind
    absent = $absent.Kind
    empty = $empty.Kind
    wrong_rows = Test-StateMutation 'row_count' 2
    duplicate_key = Test-StateMutation 'distinct_key_count' 0
    wrong_bundle = Test-StateMutation 'source_bundle_sha256' ('f' * 64)
    wrong_source_set = Test-StateMutation 'source_snapshot_set_sha256' ('e' * 64)
    wrong_snapshot = Test-StateMutation 'latest_snapshot_id' '701'
    no_snapshot = Test-StateMutation 'snapshot_count' 0
} | ConvertTo-Json -Compress
'''
        )
        result = self.run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["grain_pass"])
        self.assertTrue(payload["grain_conflict"])
        self.assertEqual("Verified", payload["verified"])
        self.assertEqual("Absent", payload["absent"])
        self.assertEqual("EmptyWithoutSnapshot", payload["empty"])
        for key in (
            "wrong_rows",
            "duplicate_key",
            "wrong_bundle",
            "wrong_source_set",
            "wrong_snapshot",
            "no_snapshot",
        ):
            self.assertTrue(payload[key], key)


if __name__ == "__main__":
    unittest.main()
