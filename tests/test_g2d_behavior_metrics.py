import json
from pathlib import Path
import re
import shutil
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent.parent
DEFINITIONS = ROOT / "configs" / "metrics" / "behavior-v1.json"
DORIS_SCHEMA = ROOT / "infra" / "compose" / "doris" / "init" / "02_create_behavior_metrics.sql"
TRINO_TEMPLATE = ROOT / "jobs" / "sql" / "18_g2d_behavior_metrics.sql.template"

EXPECTED_METRICS = {
    "event_count",
    "view_count",
    "cart_count",
    "purchase_count",
    "unique_user_count",
    "session_count",
    "product_count",
    "purchase_amount_proxy",
    "view_sessions",
    "view_to_cart_sessions",
    "completed_sessions",
    "view_to_cart_rate",
    "cart_to_purchase_rate",
    "full_conversion_rate",
    "clean_event_count",
    "late_event_count",
    "distinct_event_count",
    "duplicate_event_count",
    "missing_session_count",
    "unknown_category_count",
    "unknown_brand_count",
    "invalid_event_type_count",
    "empty_key_id_count",
    "invalid_price_count",
    "invalid_derived_date_count",
}
EXPECTED_ITEM_KEYS = {
    "metric_name",
    "display_name",
    "formula",
    "numerator",
    "denominator",
    "source_fields",
    "allowed_windows",
    "additive",
    "null_policy",
    "limitations",
    "forbidden_claims",
}
EXPECTED_DORIS_TABLES = (
    "behavior_metric_publications",
    "behavior_overview_metrics",
    "behavior_funnel_metrics",
    "behavior_dimension_metrics",
    "behavior_quality_metrics",
)


class G2dArtifactContractTests(unittest.TestCase):
    def test_definition_catalog_declares_fixed_identity_and_proxy_warning(self):
        """Protect consumers from catalog identity or public metric drift."""
        payload = json.loads(DEFINITIONS.read_text(encoding="utf-8"))
        self.assertEqual("behavior", payload["domain"])
        self.assertEqual("rees46-multicategory", payload["dataset_id"])
        self.assertEqual("behavior-v1", payload["metric_version"])
        by_name = {item["metric_name"]: item for item in payload["definitions"]}
        self.assertEqual(EXPECTED_METRICS, set(by_name))
        self.assertEqual(len(payload["definitions"]), len(by_name))
        for item in payload["definitions"]:
            self.assertEqual(EXPECTED_ITEM_KEYS, set(item))
        self.assertIn("purchase_amount_proxy", by_name)
        self.assertEqual(["DAY", "FULL"], by_name["unique_user_count"]["allowed_windows"])
        self.assertIn("GMV", by_name["purchase_amount_proxy"]["forbidden_claims"])
        self.assertIn("收入", by_name["purchase_amount_proxy"]["forbidden_claims"])

    def test_doris_schema_uses_run_scoped_tables_without_destructive_sql(self):
        """Protect published runs from destructive or non-idempotent schema changes."""
        sql = DORIS_SCHEMA.read_text(encoding="utf-8")
        for table in EXPECTED_DORIS_TABLES:
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", sql)
            self.assertIn(f"DISTRIBUTED BY HASH(metric_run_id) BUCKETS 1", sql)
        self.assertNotRegex(sql.upper(), r"\b(?:DROP|TRUNCATE|DELETE)\b")
        self.assertNotIn("CURRENT_TIMESTAMP", sql.upper())
        self.assertIn("CREATE DATABASE IF NOT EXISTS analytics", sql)
        self.assertEqual(1, len(re.findall(r"(?mi)^USE analytics;$", sql)))
        self.assertIn("metric_run_id", sql)
        self.assertIn("source_snapshot_id", sql)
        self.assertIn("UNIQUE KEY(metric_run_id, window_type, window_start)", sql)
        self.assertIn(
            "UNIQUE KEY(metric_run_id, window_type, window_start, dimension_type, dimension_id)",
            sql,
        )
        self.assertIn("purchase_amount_proxy DECIMAL(38,2)", sql)

    def test_trino_template_has_five_fixed_results_and_no_dynamic_source(self):
        """Protect every result family from current-table reads or caller-controlled SQL."""
        sql = TRINO_TEMPLATE.read_text(encoding="utf-8")
        result_names = re.findall(r"(?m)^-- result:([a-z_]+)$", sql)
        self.assertEqual(
            ["source_identity", "overview", "funnel", "dimension", "quality"], result_names
        )
        self.assertEqual(5, len([part for part in sql.split(";") if part.strip()]))
        self.assertIn("lakehouse.analytics.real_behavior_detail_v1", sql)
        self.assertEqual({"__SNAPSHOT_ID__"}, set(re.findall(r"__[A-Z0-9_]+__", sql)))
        self.assertIn("FOR VERSION AS OF __SNAPSHOT_ID__", sql)
        self.assertIn("source_row_number", sql)
        self.assertIn("purchase_amount_proxy", sql)
        self.assertEqual(5, sql.count("WITH base AS"))
        self.assertEqual(5, sql.count("FOR VERSION AS OF __SNAPSHOT_ID__"))
        self.assertIn("event_ts, source_file, source_row_number, event_id", sql)
        self.assertIn("category_id", sql)
        self.assertIn("category_code", sql)
        self.assertIn("is_unknown", sql)
        self.assertIn("reconciliation_status", sql)
        self.assertIn('"real_behavior_detail_v1$snapshots"', sql)

    def test_quality_preserves_valid_removals_in_total_reconciliation(self):
        """Protect valid remove-from-cart facts from forcing an unreconciled publication."""
        quality_sql = TRINO_TEMPLATE.read_text(encoding="utf-8").split(
            "-- result:quality\n", 1
        )[1]
        self.assertIn(
            "event_type NOT IN ('view', 'cart', 'remove_from_cart', 'purchase')",
            quality_sql,
        )
        self.assertIn("count(*) AS overview_event_count", quality_sql)
        self.assertIn("source_event_count = overview_event_count", quality_sql)

    def test_quality_emits_exact_pass_status_contract(self):
        """Protect the quality result consumed by Task 2 and later publication gates."""
        quality_sql = TRINO_TEMPLATE.read_text(encoding="utf-8").split(
            "-- result:quality\n", 1
        )[1]
        self.assertRegex(
            quality_sql,
            r"THEN\s+'PASS'\s+ELSE\s+'FAIL'\s+END AS reconciliation_status",
        )
        self.assertNotRegex(quality_sql, r"'(?:UN)?RECONCILED'")


class G2dPowerShellContractTests(unittest.TestCase):
    def _run_powershell(self, command):
        executable = shutil.which("pwsh") or shutil.which("powershell")
        if executable is None:
            self.skipTest("PowerShell is required for G2-D behavior coverage")
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

    def _payload(self, command):
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        return json.loads(result.stdout.strip().splitlines()[-1])

    def test_identity_rejects_invalid_values_and_publication_must_be_last(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
Import-Module ./scripts/lib/G2d.BehaviorMetrics.psm1 -Force
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$identity = Get-G2dMetricIdentity -SnapshotId 3854376992136224865 `
    -DataScope g2c-correctness-subset -SourceEventCount 1002
[ordered]@{
    run_id = $identity.MetricRunId
    dataset_id = $identity.DatasetId
    rejected_scope_count = Test-Rejected {
        Get-G2dMetricIdentity -SnapshotId 3854376992136224865 `
            -DataScope stable-user-2pct-full -SourceEventCount 1002
    }
    rejected_unknown_scope = Test-Rejected {
        Get-G2dMetricIdentity -SnapshotId 1 -DataScope unknown -SourceEventCount 1002
    }
    rejected_zero = Test-Rejected {
        Get-G2dMetricIdentity -SnapshotId 0 -DataScope g2c-correctness-subset -SourceEventCount 1002
    }
    rejected_negative = Test-Rejected {
        Get-G2dMetricIdentity -SnapshotId -1 -DataScope g2c-correctness-subset -SourceEventCount 1002
    }
    rejected_fraction = Test-Rejected {
        Get-G2dMetricIdentity -SnapshotId '1.5' -DataScope g2c-correctness-subset -SourceEventCount 1002
    }
    publication_order = @(Get-G2dPublicationOrder)
    rejected_publication_not_last = Test-Rejected {
        Get-G2dPublicationOrder -Order @('overview', 'publication', 'funnel', 'dimension', 'quality')
    }
} | ConvertTo-Json -Depth 5 -Compress
'''
        )
        self.assertEqual("behavior-v1-s3854376992136224865", payload["run_id"])
        self.assertEqual("rees46-multicategory", payload["dataset_id"])
        self.assertTrue(payload["rejected_scope_count"])
        self.assertTrue(payload["rejected_unknown_scope"])
        self.assertTrue(payload["rejected_zero"])
        self.assertTrue(payload["rejected_negative"])
        self.assertTrue(payload["rejected_fraction"])
        self.assertEqual(
            ["overview", "funnel", "dimension", "quality", "publication"],
            payload["publication_order"],
        )
        self.assertTrue(payload["rejected_publication_not_last"])

    def test_sql_splitter_renders_only_five_strict_named_statements(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
Import-Module ./scripts/lib/G2d.BehaviorMetrics.psm1 -Force
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$sql = Get-Content jobs/sql/18_g2d_behavior_metrics.sql.template -Raw -Encoding UTF8
$parts = Split-G2dNamedSql -Sql $sql -SnapshotId 3854376992136224865
[ordered]@{
    names = @($parts.Keys)
    all_rendered = -not [bool](($parts.Values -join "`n") -match '__[A-Z0-9_]+__')
    snapshot_occurrences = ([regex]::Matches(($parts.Values -join "`n"), '3854376992136224865')).Count
    duplicate = Test-Rejected { Split-G2dNamedSql -Sql ($sql + "`n-- result:overview`nSELECT 1;") -SnapshotId 1 }
    missing = Test-Rejected { Split-G2dNamedSql -Sql ($sql -replace '-- result:quality', '-- omitted:quality') -SnapshotId 1 }
    unknown_placeholder = Test-Rejected { Split-G2dNamedSql -Sql ($sql + "`n-- __OTHER__") -SnapshotId 1 }
    extra_statement = Test-Rejected { Split-G2dNamedSql -Sql ($sql + "`nSELECT 1;") -SnapshotId 1 }
    zero_snapshot = Test-Rejected { Split-G2dNamedSql -Sql $sql -SnapshotId 0 }
} | ConvertTo-Json -Depth 5 -Compress
'''
        )
        self.assertEqual(
            ["source_identity", "overview", "funnel", "dimension", "quality"],
            payload["names"],
        )
        self.assertTrue(payload["all_rendered"])
        self.assertEqual(7, payload["snapshot_occurrences"])
        self.assertTrue(payload["duplicate"])
        self.assertTrue(payload["missing"])
        self.assertTrue(payload["unknown_placeholder"])
        self.assertTrue(payload["extra_statement"])
        self.assertTrue(payload["zero_snapshot"])

    def test_csv_parser_accepts_real_csv_and_rejects_ambiguous_input(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
Import-Module ./scripts/lib/G2d.BehaviorMetrics.psm1 -Force
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$rows = @(ConvertFrom-G2dCsv -CsvText "id,name`n1,`"alpha,beta`"")
[ordered]@{
    count = $rows.Count
    name = $rows[0].name
    duplicate_header = Test-Rejected { ConvertFrom-G2dCsv -CsvText "id,id`n1,2" }
    header_only = Test-Rejected { ConvertFrom-G2dCsv -CsvText 'id,name' }
    malformed_quote = Test-Rejected { ConvertFrom-G2dCsv -CsvText "id,name`n1,`"unterminated" }
    inconsistent_columns = Test-Rejected { ConvertFrom-G2dCsv -CsvText "id,name`n1" }
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(1, payload["count"])
        self.assertEqual("alpha,beta", payload["name"])
        self.assertTrue(payload["duplicate_header"])
        self.assertTrue(payload["header_only"])
        self.assertTrue(payload["malformed_quote"])
        self.assertTrue(payload["inconsistent_columns"])

    def test_bundle_accepts_removal_gap_and_rejects_core_reconciliation_failures(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
Import-Module ./scripts/lib/G2d.BehaviorMetrics.psm1 -Force
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
function Copy-Rows([object[]]$Rows) {
    return @(($Rows | ConvertTo-Json -Depth 8) | ConvertFrom-Json)
}
function Test-Bundle([object[]]$Overview, [object[]]$Funnel, [object[]]$Dimension, [object[]]$Quality) {
    Assert-G2dMetricBundle -Identity $identity -SourceIdentity $sourceIdentity `
        -Overview $Overview -Funnel $Funnel -Dimension $Dimension -Quality $Quality
}
$identity = Get-G2dMetricIdentity -SnapshotId 3854376992136224865 `
    -DataScope g2c-correctness-subset -SourceEventCount 1002
$sourceIdentity = @([pscustomobject]@{
    source_event_count = '1002'; window_start = '2024-01-01'; window_end = '2024-01-02'
    distinct_event_count = '1002'; source_snapshot_id = '3854376992136224865'
    snapshot_committed_at = '2024-01-03T00:00:00Z'
})
$overview = @(
    [pscustomobject]@{ window_type='DAY'; window_start='2024-01-01'; window_end='2024-01-01'; event_count='500'; view_count='350'; cart_count='100'; purchase_count='49'; unique_user_count='300'; session_count='320'; product_count='200'; purchase_amount_proxy='490.00' },
    [pscustomobject]@{ window_type='DAY'; window_start='2024-01-02'; window_end='2024-01-02'; event_count='502'; view_count='350'; cart_count='100'; purchase_count='51'; unique_user_count='301'; session_count='321'; product_count='201'; purchase_amount_proxy='510.00' },
    [pscustomobject]@{ window_type='FULL'; window_start='2024-01-01'; window_end='2024-01-02'; event_count='1002'; view_count='700'; cart_count='200'; purchase_count='100'; unique_user_count='501'; session_count='600'; product_count='350'; purchase_amount_proxy='1000.00' }
)
$funnel = @(
    [pscustomobject]@{ window_type='DAY'; window_start='2024-01-01'; window_end='2024-01-01'; missing_session_event_count='0'; view_sessions='300'; view_to_cart_sessions='100'; completed_sessions='40' },
    [pscustomobject]@{ window_type='DAY'; window_start='2024-01-02'; window_end='2024-01-02'; missing_session_event_count='0'; view_sessions='301'; view_to_cart_sessions='101'; completed_sessions='41' },
    [pscustomobject]@{ window_type='FULL'; window_start='2024-01-01'; window_end='2024-01-02'; missing_session_event_count='0'; view_sessions='600'; view_to_cart_sessions='200'; completed_sessions='80' }
)
$dimension = @(
    [pscustomobject]@{ window_type='FULL'; window_start='2024-01-01'; window_end='2024-01-02'; dimension_type='product'; dimension_id='p1'; dimension_name='p1'; is_unknown='false'; view_count='10'; cart_count='2'; purchase_count='1'; unique_user_count='8'; purchase_amount_proxy='12.00' },
    [pscustomobject]@{ window_type='FULL'; window_start='2024-01-01'; window_end='2024-01-02'; dimension_type='category'; dimension_id='c1'; dimension_name='cat'; is_unknown='0'; view_count='10'; cart_count='2'; purchase_count='1'; unique_user_count='8'; purchase_amount_proxy='12.00' },
    [pscustomobject]@{ window_type='FULL'; window_start='2024-01-01'; window_end='2024-01-02'; dimension_type='brand'; dimension_id='b1'; dimension_name='brand'; is_unknown='1'; view_count='10'; cart_count='2'; purchase_count='1'; unique_user_count='8'; purchase_amount_proxy='12.00' }
)
$quality = @([pscustomobject]@{
    window_type='FULL'; window_start='2024-01-01'; window_end='2024-01-02'
    source_event_count='1002'; clean_event_count='1000'; late_event_count='2'
    clean_event_rate='0.99800399'; late_event_rate='0.00199601'
    distinct_event_count='1002'; duplicate_event_count='0'; missing_session_count='0'
    unknown_category_count='0'; unknown_brand_count='0'; invalid_event_type_count='0'
    empty_key_id_count='0'; invalid_price_count='0'; invalid_derived_date_count='0'
    overview_event_count='1002'; reconciliation_status='PASS'
})
$validGap = $true
try { Test-Bundle $overview $funnel $dimension $quality | Out-Null } catch { $validGap = $false }
$above = Copy-Rows $overview; ($above | Where-Object window_type -ceq 'FULL').purchase_count = '103'
$dayAbove = Copy-Rows $overview; $dayAbove[0].purchase_count = '51'
$dayMismatch = Copy-Rows $overview; $dayMismatch[0].event_count = '499'
$duplicateIds = Copy-Rows $quality; $duplicateIds[0].distinct_event_count = '1001'
$duplicateCount = Copy-Rows $quality; $duplicateCount[0].duplicate_event_count = '1'
$badFunnel = Copy-Rows $funnel; ($badFunnel | Where-Object window_type -ceq 'FULL').completed_sessions = '201'
$badDayFunnel = Copy-Rows $funnel; $badDayFunnel[0].completed_sessions = '101'
$badStatus = Copy-Rows $quality; $badStatus[0].reconciliation_status = 'RECONCILED'
$invalidType = Copy-Rows $quality; $invalidType[0].invalid_event_type_count = '1'
$extraDayQuality = @(Copy-Rows $quality)
$dayQuality = Copy-Rows $quality
$dayQuality[0].window_type = 'DAY'; $dayQuality[0].window_end = '2024-01-01'
$extraDayQuality += @($dayQuality)
[ordered]@{
    valid_removal_gap = $validGap
    sum_above_total = Test-Rejected { Test-Bundle $above $funnel $dimension $quality }
    day_sum_above_total = Test-Rejected { Test-Bundle $dayAbove $funnel $dimension $quality }
    day_total_mismatch = Test-Rejected { Test-Bundle $dayMismatch $funnel $dimension $quality }
    duplicate_ids = Test-Rejected { Test-Bundle $overview $funnel $dimension $duplicateIds }
    duplicate_count = Test-Rejected { Test-Bundle $overview $funnel $dimension $duplicateCount }
    funnel_order = Test-Rejected { Test-Bundle $overview $badFunnel $dimension $quality }
    day_funnel_order = Test-Rejected { Test-Bundle $overview $badDayFunnel $dimension $quality }
    status_not_pass = Test-Rejected { Test-Bundle $overview $funnel $dimension $badStatus }
    invalid_event_type = Test-Rejected { Test-Bundle $overview $funnel $dimension $invalidType }
    extra_day_quality = Test-Rejected { Test-Bundle $overview $funnel $dimension $extraDayQuality }
} | ConvertTo-Json -Compress
'''
        )
        self.assertTrue(payload["valid_removal_gap"])
        self.assertTrue(payload["sum_above_total"])
        self.assertTrue(payload["day_sum_above_total"])
        self.assertTrue(payload["day_total_mismatch"])
        self.assertTrue(payload["duplicate_ids"])
        self.assertTrue(payload["duplicate_count"])
        self.assertTrue(payload["funnel_order"])
        self.assertTrue(payload["day_funnel_order"])
        self.assertTrue(payload["status_not_pass"])
        self.assertTrue(payload["invalid_event_type"])
        self.assertTrue(payload["extra_day_quality"])

    def test_bundle_rejects_invalid_shapes_windows_values_and_keys(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
Import-Module ./scripts/lib/G2d.BehaviorMetrics.psm1 -Force
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
function Copy-Rows([object[]]$Rows) { return @(($Rows | ConvertTo-Json -Depth 8) | ConvertFrom-Json) }
function Test-Bundle([object[]]$Overview, [object[]]$Funnel, [object[]]$Dimension, [object[]]$Quality) {
    Assert-G2dMetricBundle -Identity $identity -SourceIdentity $sourceIdentity `
        -Overview $Overview -Funnel $Funnel -Dimension $Dimension -Quality $Quality
}
$identity = Get-G2dMetricIdentity -SnapshotId 9 -DataScope g2c-correctness-subset -SourceEventCount 1002
$sourceIdentity = @([pscustomobject]@{ source_event_count='1002'; window_start='2024-01-01'; window_end='2024-01-01'; distinct_event_count='1002'; source_snapshot_id='9'; snapshot_committed_at='2024-01-02T00:00:00Z' })
$overview = @(
    [pscustomobject]@{ window_type='DAY'; window_start='2024-01-01'; window_end='2024-01-01'; event_count='1002'; view_count='700'; cart_count='200'; purchase_count='100'; unique_user_count='500'; session_count='600'; product_count='300'; purchase_amount_proxy='1.00' },
    [pscustomobject]@{ window_type='FULL'; window_start='2024-01-01'; window_end='2024-01-01'; event_count='1002'; view_count='700'; cart_count='200'; purchase_count='100'; unique_user_count='500'; session_count='600'; product_count='300'; purchase_amount_proxy='1.00' }
)
$funnel = @([pscustomobject]@{ window_type='FULL'; window_start='2024-01-01'; window_end='2024-01-01'; missing_session_event_count='0'; view_sessions='600'; view_to_cart_sessions='200'; completed_sessions='100' })
$dimension = @([pscustomobject]@{ window_type='FULL'; window_start='2024-01-01'; window_end='2024-01-01'; dimension_type='product'; dimension_id='p1'; dimension_name='p1'; is_unknown='false'; view_count='1'; cart_count='0'; purchase_count='0'; unique_user_count='1'; purchase_amount_proxy='0.00' })
$quality = @([pscustomobject]@{ window_type='FULL'; window_start='2024-01-01'; window_end='2024-01-01'; source_event_count='1002'; clean_event_count='1002'; late_event_count='0'; clean_event_rate='1'; late_event_rate='0'; distinct_event_count='1002'; duplicate_event_count='0'; missing_session_count='0'; unknown_category_count='0'; unknown_brand_count='0'; invalid_event_type_count='0'; empty_key_id_count='0'; invalid_price_count='0'; invalid_derived_date_count='0'; overview_event_count='1002'; reconciliation_status='PASS' })
$twoFull = Copy-Rows $overview; $twoFull += Copy-Rows @($overview[1])
$badWindow = Copy-Rows $funnel; $badWindow[0].window_end = '2024-01-02'
$negative = Copy-Rows $overview; $negative[0].view_count = '-1'
$badAmount = Copy-Rows $dimension; $badAmount[0].purchase_amount_proxy = '0.0'
$badDimension = Copy-Rows $dimension; $badDimension[0].dimension_type = 'campaign'
$duplicateDimension = @(Copy-Rows $dimension); $duplicateDimension += @(Copy-Rows $dimension)
[ordered]@{
    duplicate_full = Test-Rejected { Test-Bundle $twoFull $funnel $dimension $quality }
    mismatched_window = Test-Rejected { Test-Bundle $overview $badWindow $dimension $quality }
    negative_integer = Test-Rejected { Test-Bundle $negative $funnel $dimension $quality }
    bad_amount = Test-Rejected { Test-Bundle $overview $funnel $badAmount $quality }
    unknown_dimension = Test-Rejected { Test-Bundle $overview $funnel $badDimension $quality }
    duplicate_key = Test-Rejected { Test-Bundle $overview $funnel $duplicateDimension $quality }
} | ConvertTo-Json -Compress
'''
        )
        self.assertTrue(all(payload.values()), payload)

    def test_canonical_digest_is_order_independent_and_cell_sensitive(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
Import-Module ./scripts/lib/G2d.BehaviorMetrics.psm1 -Force
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$columns = @('dimension_id', 'window_start', 'is_unknown', 'event_count', 'purchase_amount_proxy', 'calculated_at', 'optional_value')
$keys = @('dimension_id')
$a = [pscustomobject]@{ dimension_id='a'; window_start='2024-01-01'; is_unknown=$false; event_count='1'; purchase_amount_proxy='2.5'; calculated_at='2024-01-01T08:00:00+08:00'; optional_value=$null }
$b = [pscustomobject]@{ dimension_id='b'; window_start='2024-01-02'; is_unknown='1'; event_count=2; purchase_amount_proxy='3.50'; calculated_at='2024-01-02T00:00:00Z'; optional_value=$null }
$digest1 = Get-G2dCanonicalDigest -Rows @($b, $a) -Columns $columns -UniqueKeyColumns $keys
$digest2 = Get-G2dCanonicalDigest -Rows @($a, $b) -Columns $columns -UniqueKeyColumns $keys
$changed = [pscustomobject]@{ dimension_id='b'; window_start='2024-01-02'; is_unknown='1'; event_count=3; purchase_amount_proxy='3.50'; calculated_at='2024-01-02T00:00:00Z'; optional_value=$null }
$digest3 = Get-G2dCanonicalDigest -Rows @($a, $changed) -Columns $columns -UniqueKeyColumns $keys
$upper = [pscustomobject]@{ dimension_id='A'; window_start='2024-01-03'; is_unknown=$false; event_count='4'; purchase_amount_proxy='4.50'; calculated_at='2024-01-03T00:00:00Z'; optional_value=$null }
$caseForward = Get-G2dCanonicalDigest -Rows @($a, $upper) -Columns $columns -UniqueKeyColumns $keys
$caseReverse = Get-G2dCanonicalDigest -Rows @($upper, $a) -Columns $columns -UniqueKeyColumns $keys
[ordered]@{
    first = $digest1
    reordered = $digest2
    changed = $digest3
    case_forward = $caseForward
    case_reverse = $caseReverse
    duplicate_key = Test-Rejected { Get-G2dCanonicalDigest -Rows @($a, $a) -Columns $columns -UniqueKeyColumns $keys }
} | ConvertTo-Json -Compress
'''
        )
        self.assertRegex(payload["first"], r"^[0-9a-f]{64}$")
        self.assertEqual(payload["first"], payload["reordered"])
        self.assertNotEqual(payload["first"], payload["changed"])
        self.assertEqual(payload["case_forward"], payload["case_reverse"])
        self.assertTrue(payload["duplicate_key"])

    def test_candidate_export_uses_fixed_columns_utf8_no_bom_and_safe_paths(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
Import-Module ./scripts/lib/G2d.BehaviorMetrics.psm1 -Force
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$identity = Get-G2dMetricIdentity -SnapshotId 9 -DataScope g2c-correctness-subset -SourceEventCount 1002
$root = Join-Path ([IO.Path]::GetTempPath()) ('g2d-export-' + [guid]::NewGuid().ToString('N'))
$null = New-Item -ItemType Directory -Path $root
try {
    $row = [pscustomobject]@{ window_type='FULL'; window_start='2024-01-01'; window_end='2024-01-01'; dimension_type='product'; dimension_id='p1'; dimension_name='cafe'; is_unknown=$true; view_count='1'; cart_count='0'; purchase_count='0'; unique_user_count='1'; purchase_amount_proxy='0.00' }
    $path = Join-Path $root 'dimension.csv'
    $written = Export-G2dCandidateCsv -Target dimension -Rows @($row) -Identity $identity -OutputDirectory $root -OutputPath $path
    $bytes = [IO.File]::ReadAllBytes($path)
    $lines = [IO.File]::ReadAllLines($path, [Text.UTF8Encoding]::new($false))
    $outside = Join-Path (Split-Path $root -Parent) 'escaped.csv'
    [ordered]@{
        returned_path = [IO.Path]::GetFullPath($written)
        expected_path = [IO.Path]::GetFullPath($path)
        header = $lines[0]
        boolean_as_one = $lines[1] -match ',1,1,0,0,1,0.00$'
        has_bom = $bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF
        outside_rejected = Test-Rejected { Export-G2dCandidateCsv -Target dimension -Rows @($row) -Identity $identity -OutputDirectory $root -OutputPath $outside }
        publication_rejected = Test-Rejected { Export-G2dCandidateCsv -Target publication -Rows @($row) -Identity $identity -OutputDirectory $root -OutputPath (Join-Path $root 'publication.csv') }
    } | ConvertTo-Json -Compress
} finally {
    Remove-Item -LiteralPath $root -Recurse -Force
}
'''
        )
        self.assertEqual(payload["expected_path"], payload["returned_path"])
        self.assertEqual(
            "metric_run_id,dataset_id,metric_version,window_type,window_start,window_end,dimension_type,dimension_id,dimension_name,is_unknown,view_count,cart_count,purchase_count,unique_user_count,purchase_amount_proxy",
            payload["header"],
        )
        self.assertTrue(payload["boolean_as_one"])
        self.assertFalse(payload["has_bom"])
        self.assertTrue(payload["outside_rejected"])
        self.assertTrue(payload["publication_rejected"])

    def test_candidate_export_rejects_physical_link_escape_when_supported(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
Import-Module ./scripts/lib/G2d.BehaviorMetrics.psm1 -Force
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$identity = Get-G2dMetricIdentity -SnapshotId 9 -DataScope g2c-correctness-subset -SourceEventCount 1002
$base = Join-Path ([IO.Path]::GetTempPath()) ('g2d-link-' + [guid]::NewGuid().ToString('N'))
$root = Join-Path $base 'root'
$outside = Join-Path $base 'outside'
$link = Join-Path $root 'linked'
$null = New-Item -ItemType Directory -Path $root
$null = New-Item -ItemType Directory -Path $outside
$marker = Join-Path $outside 'marker.txt'
[IO.File]::WriteAllText($marker, 'safe', [Text.UTF8Encoding]::new($false))
$linkCreated = $false
try {
    try {
        $linkType = if ([IO.Path]::DirectorySeparatorChar -eq '\') { 'Junction' } else { 'SymbolicLink' }
        $null = New-Item -ItemType $linkType -Path $link -Target $outside -ErrorAction Stop
        $linkCreated = $true
    } catch {
        $linkCreated = $false
    }
    $row = [pscustomobject]@{ window_type='FULL'; window_start='2024-01-01'; window_end='2024-01-01'; dimension_type='product'; dimension_id='p1'; dimension_name='p1'; is_unknown=$false; view_count='1'; cart_count='0'; purchase_count='0'; unique_user_count='1'; purchase_amount_proxy='0.00' }
    $lexicalOutside = Join-Path $outside 'lexical.csv'
    $lexicalRejected = Test-Rejected { Export-G2dCandidateCsv -Target dimension -Rows @($row) -Identity $identity -OutputDirectory $root -OutputPath $lexicalOutside }
    $linkResult = 'unsupported'
    if ($linkCreated) {
        $linkedOutput = Join-Path $link 'escaped.csv'
        $linkResult = if (Test-Rejected { Export-G2dCandidateCsv -Target dimension -Rows @($row) -Identity $identity -OutputDirectory $root -OutputPath $linkedOutput }) { 'rejected' } else { 'accepted' }
    }
    [ordered]@{
        lexical_rejected = $lexicalRejected
        link_result = $linkResult
        marker_unchanged = (Test-Path -LiteralPath $marker) -and ([IO.File]::ReadAllText($marker) -ceq 'safe')
        escaped_absent = -not (Test-Path -LiteralPath (Join-Path $outside 'escaped.csv'))
    } | ConvertTo-Json -Compress
} finally {
    if ($linkCreated -and (Test-Path -LiteralPath $link)) { Remove-Item -LiteralPath $link -Force }
    if (Test-Path -LiteralPath $base) { Remove-Item -LiteralPath $base -Recurse -Force }
}
'''
        )
        self.assertTrue(payload["lexical_rejected"])
        self.assertIn(payload["link_result"], {"rejected", "unsupported"})
        self.assertTrue(payload["marker_unchanged"])
        self.assertTrue(payload["escaped_absent"])


class G2dRefreshSafetyTests(unittest.TestCase):
    def _run_powershell(self, command):
        executable = shutil.which("pwsh") or shutil.which("powershell")
        if executable is None:
            self.skipTest("PowerShell is required for G2-D refresh coverage")
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

    def _payload(self, command):
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        return json.loads(result.stdout.strip().splitlines()[-1])

    def test_refresh_plan_is_fixed_publication_last_and_plan_only_is_offline(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path './scripts/refresh_g2d_behavior_metrics.ps1') -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$plan = Get-G2dRefreshPlan -MetricRunId 'behavior-v1-s3854376992136224865'
[ordered]@{
    tables = @($plan | ForEach-Object { $_.Name })
    targets = @($plan | ForEach-Object { $_.Target })
    publication_last = $plan[-1].Name -ceq 'publication'
    plan_only = Invoke-G2dRefresh -DataScope g2c-correctness-subset -PlanOnly
    unsafe_run_id = Test-Rejected {
        Get-G2dRefreshPlan -MetricRunId "behavior-v1-s1'; DROP TABLE analytics.x; --"
    }
    zero_run_id = Test-Rejected { Get-G2dRefreshPlan -MetricRunId 'behavior-v1-s0' }
} | ConvertTo-Json -Depth 8 -Compress
'''
        )
        self.assertEqual(
            ["overview", "funnel", "dimension", "quality", "publication"],
            payload["tables"],
        )
        self.assertEqual(
            [
                "behavior_overview_metrics",
                "behavior_funnel_metrics",
                "behavior_dimension_metrics",
                "behavior_quality_metrics",
                "behavior_metric_publications",
            ],
            payload["targets"],
        )
        self.assertTrue(payload["publication_last"])
        self.assertEqual("planned", payload["plan_only"]["status"])
        self.assertEqual("g2c-correctness-subset", payload["plan_only"]["data_scope"])
        self.assertTrue(payload["unsafe_run_id"])
        self.assertTrue(payload["zero_run_id"])

    def test_trino_result_set_requires_exactly_five_fixed_names(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path './scripts/refresh_g2d_behavior_metrics.ps1') -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$valid = [ordered]@{
    source_identity = @([pscustomobject]@{ source_snapshot_id = '9' })
    overview = @([pscustomobject]@{ value = '1' })
    funnel = @([pscustomobject]@{ value = '1' })
    dimension = @([pscustomobject]@{ value = '1' })
    quality = @([pscustomobject]@{ value = '1' })
}
$missing = [ordered]@{
    source_identity = $valid.source_identity; overview = $valid.overview
    funnel = $valid.funnel; dimension = $valid.dimension
}
$extra = [ordered]@{}
foreach ($entry in $valid.GetEnumerator()) { $extra[$entry.Key] = $entry.Value }
$extra['other'] = @([pscustomobject]@{ value = '1' })
$wrong = [ordered]@{
    source_identity = $valid.source_identity; overview = $valid.overview
    funnel = $valid.funnel; dimension = $valid.dimension; other = $valid.quality
}
[ordered]@{
    valid = -not (Test-Rejected { Assert-G2dTrinoResultSet -Results $valid })
    four_results = Test-Rejected { Assert-G2dTrinoResultSet -Results $missing }
    six_results = Test-Rejected { Assert-G2dTrinoResultSet -Results $extra }
    wrong_name = Test-Rejected { Assert-G2dTrinoResultSet -Results $wrong }
} | ConvertTo-Json -Compress
'''
        )
        self.assertTrue(all(payload.values()), payload)

    def test_stream_load_accepts_only_exact_success_and_exact_counts(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path './scripts/refresh_g2d_behavior_metrics.ps1') -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$success = [pscustomobject]@{
    Status = 'Success'; NumberLoadedRows = 2; NumberFilteredRows = 0
}
[ordered]@{
    exact = -not (Test-Rejected {
        Assert-G2dStreamLoadResponse -Response $success -ExpectedRows 2 `
            -TableName behavior_overview_metrics
    })
    publish_timeout = Test-Rejected {
        Assert-G2dStreamLoadResponse -Response ([pscustomobject]@{
            Status='Publish Timeout'; NumberLoadedRows=2; NumberFilteredRows=0
        }) -ExpectedRows 2 -TableName behavior_overview_metrics
    }
    lowercase_success = Test-Rejected {
        Assert-G2dStreamLoadResponse -Response ([pscustomobject]@{
            Status='success'; NumberLoadedRows=2; NumberFilteredRows=0
        }) -ExpectedRows 2 -TableName behavior_overview_metrics
    }
    wrong_loaded = Test-Rejected {
        Assert-G2dStreamLoadResponse -Response ([pscustomobject]@{
            Status='Success'; NumberLoadedRows=1; NumberFilteredRows=0
        }) -ExpectedRows 2 -TableName behavior_overview_metrics
    }
    filtered = Test-Rejected {
        Assert-G2dStreamLoadResponse -Response ([pscustomobject]@{
            Status='Success'; NumberLoadedRows=2; NumberFilteredRows=1
        }) -ExpectedRows 2 -TableName behavior_overview_metrics
    }
} | ConvertTo-Json -Compress
'''
        )
        self.assertTrue(all(payload.values()), payload)

    def test_candidate_verification_rejects_missing_changed_or_stale_rows(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path './scripts/refresh_g2d_behavior_metrics.ps1') -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$row = [pscustomobject]@{
    metric_run_id='behavior-v1-s9'; dataset_id='rees46-multicategory'
    metric_version='behavior-v1'; window_type='FULL'; window_start='2024-01-01'
    window_end='2024-01-01'; event_count='1002'; view_count='700'; cart_count='200'
    purchase_count='100'; unique_user_count='500'; session_count='600'
    product_count='300'; purchase_amount_proxy='1000.00'
}
$same = ($row | ConvertTo-Json | ConvertFrom-Json)
$changed = ($row | ConvertTo-Json | ConvertFrom-Json); $changed.view_count = '699'
$extra = ($row | ConvertTo-Json | ConvertFrom-Json); $extra.window_type = 'DAY'
[ordered]@{
    exact = -not (Test-Rejected {
        Assert-G2dStoredCandidate -Name overview -CandidateRows @($row) -StoredRows @($same)
    })
    missing = Test-Rejected {
        Assert-G2dStoredCandidate -Name overview -CandidateRows @($row) -StoredRows @()
    }
    changed = Test-Rejected {
        Assert-G2dStoredCandidate -Name overview -CandidateRows @($row) -StoredRows @($changed)
    }
    stale_extra = Test-Rejected {
        Assert-G2dStoredCandidate -Name overview -CandidateRows @($row) `
            -StoredRows @($same, $extra)
    }
} | ConvertTo-Json -Compress
'''
        )
        self.assertTrue(all(payload.values()), payload)

    def test_doris_batch_parser_decodes_escaped_cells_and_nulls(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path './scripts/refresh_g2d_behavior_metrics.ps1') -FunctionsOnly
$rows = @(ConvertFrom-G2dMysqlBatch -Lines @(
    "name`tnote",
    ('alpha\tbeta' + "`t" + 'NULL'),
    ('path\\leaf' + "`t" + 'plain')
))
[ordered]@{
    count = $rows.Count
    decoded_tab = $rows[0].name -ceq "alpha`tbeta"
    null_value = $null -eq $rows[0].note
    decoded_backslash = $rows[1].name -ceq 'path\leaf'
    plain = $rows[1].note
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(2, payload["count"])
        self.assertTrue(payload["decoded_tab"])
        self.assertTrue(payload["null_value"])
        self.assertTrue(payload["decoded_backslash"])
        self.assertEqual("plain", payload["plain"])

    def test_latest_snapshot_query_orders_by_commit_time_not_numeric_max(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path './scripts/refresh_g2d_behavior_metrics.ps1') -FunctionsOnly
$sql = Get-G2dLatestSnapshotSql
[ordered]@{
    fixed_metadata_table = $sql.Contains(
        'lakehouse.analytics."real_behavior_detail_v1$snapshots"'
    )
    positive_only = [bool]($sql -match '(?i)WHERE\s+snapshot_id\s*>\s*0')
    chronological = [bool]($sql -match (
        '(?is)ORDER\s+BY\s+committed_at\s+DESC\s*,\s*' +
        'snapshot_id\s+DESC\s+LIMIT\s+1'
    ))
    numeric_max_absent = -not [bool]($sql -match '(?i)\bmax\s*\(\s*snapshot_id')
} | ConvertTo-Json -Compress
'''
        )
        self.assertTrue(all(payload.values()), payload)

    def test_output_path_rejects_physical_link_escape_without_touching_outside(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path './scripts/refresh_g2d_behavior_metrics.ps1') -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$base = Join-Path ([IO.Path]::GetTempPath()) ('g2d-refresh-path-' + [guid]::NewGuid().ToString('N'))
$project = Join-Path $base 'project'
$tmp = Join-Path $project 'tmp'
$outside = Join-Path $base 'outside'
$outsideGraduation = Join-Path $outside 'graduation'
$outsideRun = Join-Path $outsideGraduation 'g2d/behavior-v1-s9'
$link = Join-Path $tmp 'graduation'
$marker = Join-Path $outside 'marker.txt'
$null = New-Item -ItemType Directory -Path $tmp
$null = New-Item -ItemType Directory -Path $outsideRun
[IO.File]::WriteAllText($marker, 'safe', [Text.UTF8Encoding]::new($false))
$linkCreated = $false
try {
    try {
        $linkType = if ([IO.Path]::DirectorySeparatorChar -eq '\') { 'Junction' } else { 'SymbolicLink' }
        $null = New-Item -ItemType $linkType -Path $link -Target $outsideGraduation -ErrorAction Stop
        $linkCreated = $true
    } catch {
        $linkCreated = $false
    }
    $script:G2dProjectRoot = $project
    $script:G2dOutputRoot = Join-Path $link 'g2d'
    $deterministicEscape = Test-Rejected {
        Assert-G2dPhysicalContainment -RootPath $project -CandidatePath $outsideRun `
            -Description 'deterministic outside path'
    }
    $linkResult = 'unsupported'
    if ($linkCreated) {
        $linkResult = if (Test-Rejected {
            Write-G2dRefreshReport -OutputDirectory (Join-Path $script:G2dOutputRoot 'behavior-v1-s9') `
                -Report ([ordered]@{ metric_run_id='behavior-v1-s9'; status='FAILED' })
        }) { 'rejected' } else { 'accepted' }
    }
    [ordered]@{
        deterministic_escape = $deterministicEscape
        link_result = $linkResult
        marker_unchanged = (Test-Path -LiteralPath $marker) -and
            ([IO.File]::ReadAllText($marker) -ceq 'safe')
        escaped_report_absent = -not (Test-Path -LiteralPath (Join-Path $outsideRun 'refresh-report.json'))
    } | ConvertTo-Json -Compress
} finally {
    if ($linkCreated -and (Test-Path -LiteralPath $link)) {
        Remove-Item -LiteralPath $link -Force
    }
    if (Test-Path -LiteralPath $base) { Remove-Item -LiteralPath $base -Recurse -Force }
}
'''
        )
        self.assertTrue(payload["deterministic_escape"])
        self.assertIn(payload["link_result"], {"rejected", "unsupported"})
        self.assertTrue(payload["marker_unchanged"])
        self.assertTrue(payload["escaped_report_absent"])

    def test_report_write_rejects_final_file_link_without_touching_outside(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path './scripts/refresh_g2d_behavior_metrics.ps1') -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$base = Join-Path ([IO.Path]::GetTempPath()) ('g2d-report-link-' + [guid]::NewGuid().ToString('N'))
$project = Join-Path $base 'project'
$outputRoot = Join-Path $project 'tmp/graduation/g2d'
$run = Join-Path $outputRoot 'behavior-v1-s9'
$outside = Join-Path $base 'outside'
$marker = Join-Path $outside 'marker.txt'
$reportLink = Join-Path $run 'refresh-report.json'
$null = New-Item -ItemType Directory -Path $run
$null = New-Item -ItemType Directory -Path $outside
[IO.File]::WriteAllText($marker, 'safe', [Text.UTF8Encoding]::new($false))
$script:G2dProjectRoot = $project
$script:G2dOutputRoot = $outputRoot
$deterministicSafe = -not (Test-Rejected {
    Assert-G2dReportFilePath -RunDirectory $run `
        -ReportPath (Join-Path $run 'refresh-report.json')
})
$deterministicEscape = Test-Rejected {
    Assert-G2dReportFilePath -RunDirectory $run -ReportPath $marker
}
$linkCreated = $false
try {
    try {
        $null = New-Item -ItemType SymbolicLink -Path $reportLink -Target $marker -ErrorAction Stop
        $linkCreated = $true
    } catch {
        $linkCreated = $false
    }
    $linkResult = 'unsupported'
    if ($linkCreated) {
        $linkResult = if (Test-Rejected {
            Write-G2dRefreshReport -OutputDirectory $run `
                -Report ([ordered]@{ metric_run_id='behavior-v1-s9'; status='FAILED' })
        }) { 'rejected' } else { 'accepted' }
    }
    [ordered]@{
        deterministic_safe = $deterministicSafe
        deterministic_escape = $deterministicEscape
        link_result = $linkResult
        link_preserved = -not $linkCreated -or (Test-Path -LiteralPath $reportLink)
        marker_unchanged = (Test-Path -LiteralPath $marker) -and
            ([IO.File]::ReadAllText($marker) -ceq 'safe')
    } | ConvertTo-Json -Compress
} finally {
    if ($linkCreated -and (Test-Path -LiteralPath $reportLink)) {
        Remove-Item -LiteralPath $reportLink -Force
    }
    if (Test-Path -LiteralPath $base) { Remove-Item -LiteralPath $base -Recurse -Force }
}
'''
        )
        self.assertTrue(payload["deterministic_safe"])
        self.assertTrue(payload["deterministic_escape"])
        self.assertIn(payload["link_result"], {"rejected", "unsupported"})
        self.assertTrue(payload["link_preserved"])
        self.assertTrue(payload["marker_unchanged"])

    def test_existing_publication_requires_exact_identity_counts_hashes_and_status(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path './scripts/refresh_g2d_behavior_metrics.ps1') -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$identity = [pscustomobject]@{
    MetricRunId='behavior-v1-s9'; DatasetId='rees46-multicategory'
    MetricVersion='behavior-v1'; DataScope='g2c-correctness-subset'
    SourceSnapshotId=9; SourceEventCount=1002
}
$source = [pscustomobject]@{
    window_start='2024-01-01'; window_end='2024-01-02'
}
$evidence = [ordered]@{
    overview=[pscustomobject]@{ RowCount=3; Sha256=('a' * 64) }
    funnel=[pscustomobject]@{ RowCount=3; Sha256=('b' * 64) }
    dimension=[pscustomobject]@{ RowCount=30; Sha256=('c' * 64) }
    quality=[pscustomobject]@{ RowCount=1; Sha256=('d' * 64) }
}
$publication = [pscustomobject]@{
    metric_run_id='behavior-v1-s9'; dataset_id='rees46-multicategory'
    metric_version='behavior-v1'; data_scope='g2c-correctness-subset'
    source_snapshot_id='9'; source_event_count='1002'; window_start='2024-01-01'
    window_end='2024-01-02'; calculated_at='2026-09-18T00:00:00.000Z'
    published_at='2026-09-18T00:00:01.000Z'; overview_row_count='3'
    overview_sha256=('a' * 64); funnel_row_count='3'; funnel_sha256=('b' * 64)
    dimension_row_count='30'; dimension_sha256=('c' * 64); quality_row_count='1'
    quality_sha256=('d' * 64); status='PUBLISHED'
}
$wrongScope = ($publication | ConvertTo-Json | ConvertFrom-Json)
$wrongScope.data_scope = 'stable-user-2pct-full'
$wrongHash = ($publication | ConvertTo-Json | ConvertFrom-Json)
$wrongHash.quality_sha256 = 'e' * 64
$wrongStatus = ($publication | ConvertTo-Json | ConvertFrom-Json)
$wrongStatus.status = 'PASS'
$emptyDigest = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
$zeroEvidence = [ordered]@{}
foreach ($entry in $evidence.GetEnumerator()) { $zeroEvidence[$entry.Key] = $entry.Value }
$zeroEvidence.quality = [pscustomobject]@{ RowCount=0; Sha256=$emptyDigest }
$zeroPublication = ($publication | ConvertTo-Json | ConvertFrom-Json)
$zeroPublication.quality_row_count = '0'; $zeroPublication.quality_sha256 = $emptyDigest
$storedIdentity = [pscustomobject]@{
    metric_run_id='behavior-v1-s9'; dataset_id='rees46-multicategory'; metric_version='behavior-v1'
}
$wrongStoredIdentity = ($storedIdentity | ConvertTo-Json | ConvertFrom-Json)
$wrongStoredIdentity.dataset_id = 'other-dataset'
$exact = Assert-G2dExistingPublication -Identity $identity -SourceIdentity $source `
    -Publication $publication -StoredEvidence $evidence
$insertSql = New-G2dPublicationInsertSql -Publication $publication
[ordered]@{
    exact_status = $exact.status
    fixed_insert_target = $insertSql.StartsWith(
        'INSERT INTO analytics.behavior_metric_publications ('
    )
    exact_published_literal = $insertSql.Contains("'PUBLISHED'")
    valid_stored_identity = -not (Test-Rejected {
        Assert-G2dStoredIdentity -Identity $identity -Rows @($storedIdentity) -Name overview
    })
    mismatched_stored_identity = Test-Rejected {
        Assert-G2dStoredIdentity -Identity $identity -Rows @($wrongStoredIdentity) -Name overview
    }
    empty_metric_table = Test-Rejected {
        Assert-G2dExistingPublication -Identity $identity -SourceIdentity $source `
            -Publication $zeroPublication -StoredEvidence $zeroEvidence
    }
    mismatched_identity = Test-Rejected {
        Assert-G2dExistingPublication -Identity $identity -SourceIdentity $source `
            -Publication $wrongScope -StoredEvidence $evidence
    }
    mismatched_digest = Test-Rejected {
        Assert-G2dExistingPublication -Identity $identity -SourceIdentity $source `
            -Publication $wrongHash -StoredEvidence $evidence
    }
    wrong_status = Test-Rejected {
        Assert-G2dExistingPublication -Identity $identity -SourceIdentity $source `
            -Publication $wrongStatus -StoredEvidence $evidence
    }
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual("already_published", payload["exact_status"])
        self.assertTrue(payload["fixed_insert_target"])
        self.assertTrue(payload["exact_published_literal"])
        self.assertTrue(payload["valid_stored_identity"])
        self.assertTrue(payload["mismatched_stored_identity"])
        self.assertTrue(payload["empty_metric_table"])
        self.assertTrue(payload["mismatched_identity"])
        self.assertTrue(payload["mismatched_digest"])
        self.assertTrue(payload["wrong_status"])

    def test_unpublished_partial_candidates_continue_with_fresh_attempt_identity(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path './scripts/refresh_g2d_behavior_metrics.ps1') -FunctionsOnly
$first = New-G2dAttemptId
$second = New-G2dAttemptId
$state = Get-G2dUnpublishedCandidateState -Counts ([ordered]@{
    overview=3; funnel=0; dimension=12; quality=1
}) -AttemptId $first
[ordered]@{
    first = $first
    second = $second
    distinct = $first -cne $second
    continued = $state.status -ceq 'continue'
    preserved_counts = $state.counts.overview -eq 3 -and $state.counts.dimension -eq 12
} | ConvertTo-Json -Depth 5 -Compress
'''
        )
        self.assertRegex(payload["first"], r"^[0-9a-f]{32}$")
        self.assertRegex(payload["second"], r"^[0-9a-f]{32}$")
        self.assertTrue(payload["distinct"])
        self.assertTrue(payload["continued"])
        self.assertTrue(payload["preserved_counts"])

    def test_publication_action_is_not_called_until_all_four_checks_pass(self):
        payload = self._payload(
            r'''
$ErrorActionPreference = 'Stop'
. (Resolve-Path './scripts/refresh_g2d_behavior_metrics.ps1') -FunctionsOnly
function Test-Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$script:checks = [Collections.Generic.List[string]]::new()
$script:published = 0
$failed = Test-Rejected {
    Invoke-G2dPublicationSequence `
        -VerifyAction {
            param($Name)
            $script:checks.Add($Name)
            if ($Name -ceq 'quality') { throw 'quality mismatch' }
            [pscustomobject]@{ RowCount=1; Sha256=('a' * 64) }
        } `
        -PublishAction { $script:published++; [pscustomobject]@{ status='PUBLISHED' } }
}
$beforeSuccess = @($script:checks)
$script:checks.Clear()
$succeeded = Invoke-G2dPublicationSequence `
    -VerifyAction {
        param($Name)
        $script:checks.Add($Name)
        [pscustomobject]@{ RowCount=1; Sha256=('a' * 64) }
    } `
    -PublishAction { $script:published++; [pscustomobject]@{ status='PUBLISHED' } }
[ordered]@{
    failed = $failed
    failed_checks = $beforeSuccess
    not_published_on_failure = $script:published -eq 1
    success_checks = @($script:checks)
    success_status = $succeeded.status
} | ConvertTo-Json -Depth 5 -Compress
'''
        )
        self.assertTrue(payload["failed"])
        self.assertEqual(
            ["overview", "funnel", "dimension", "quality"], payload["failed_checks"]
        )
        self.assertTrue(payload["not_published_on_failure"])
        self.assertEqual(
            ["overview", "funnel", "dimension", "quality"], payload["success_checks"]
        )
        self.assertEqual("PUBLISHED", payload["success_status"])


if __name__ == "__main__":
    unittest.main()
