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
$dayMismatch = Copy-Rows $overview; $dayMismatch[0].event_count = '499'
$duplicateIds = Copy-Rows $quality; $duplicateIds[0].distinct_event_count = '1001'
$duplicateCount = Copy-Rows $quality; $duplicateCount[0].duplicate_event_count = '1'
$badFunnel = Copy-Rows $funnel; ($badFunnel | Where-Object window_type -ceq 'FULL').completed_sessions = '201'
$badStatus = Copy-Rows $quality; $badStatus[0].reconciliation_status = 'RECONCILED'
$invalidType = Copy-Rows $quality; $invalidType[0].invalid_event_type_count = '1'
[ordered]@{
    valid_removal_gap = $validGap
    sum_above_total = Test-Rejected { Test-Bundle $above $funnel $dimension $quality }
    day_total_mismatch = Test-Rejected { Test-Bundle $dayMismatch $funnel $dimension $quality }
    duplicate_ids = Test-Rejected { Test-Bundle $overview $funnel $dimension $duplicateIds }
    duplicate_count = Test-Rejected { Test-Bundle $overview $funnel $dimension $duplicateCount }
    funnel_order = Test-Rejected { Test-Bundle $overview $badFunnel $dimension $quality }
    status_not_pass = Test-Rejected { Test-Bundle $overview $funnel $dimension $badStatus }
    invalid_event_type = Test-Rejected { Test-Bundle $overview $funnel $dimension $invalidType }
} | ConvertTo-Json -Compress
'''
        )
        self.assertTrue(payload["valid_removal_gap"])
        self.assertTrue(payload["sum_above_total"])
        self.assertTrue(payload["day_total_mismatch"])
        self.assertTrue(payload["duplicate_ids"])
        self.assertTrue(payload["duplicate_count"])
        self.assertTrue(payload["funnel_order"])
        self.assertTrue(payload["status_not_pass"])
        self.assertTrue(payload["invalid_event_type"])

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
[ordered]@{
    first = $digest1
    reordered = $digest2
    changed = $digest3
    duplicate_key = Test-Rejected { Get-G2dCanonicalDigest -Rows @($a, $a) -Columns $columns -UniqueKeyColumns $keys }
} | ConvertTo-Json -Compress
'''
        )
        self.assertRegex(payload["first"], r"^[0-9a-f]{64}$")
        self.assertEqual(payload["first"], payload["reordered"])
        self.assertNotEqual(payload["first"], payload["changed"])
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


if __name__ == "__main__":
    unittest.main()
