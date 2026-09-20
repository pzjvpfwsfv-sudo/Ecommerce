# G2-D Real Behavior Metrics API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a snapshot-bound REES46 behavior metrics layer in Doris and expose it through stable `/api/v1/behavior/*` FastAPI contracts without misrepresenting the 1,002-row correctness subset as the full sample.

**Architecture:** Fixed Trino SQL computes DAY and FULL metrics from `lakehouse.analytics.real_behavior_detail_v1`. A PowerShell refresh pipeline validates every metric family, loads candidate rows into fixed Doris tables, and inserts the publication row last; FastAPI only reads the latest published run. Metric semantics live in one Git-versioned JSON catalog shared by the API and future RAG ingestion.

**Tech Stack:** Trino 458 SQL, Apache Doris 2.1.9, PowerShell 7, FastAPI, Pydantic v2, PyMySQL, Python `unittest`, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-09-18-graduation-behavior-metrics-api-design.md`

## Global Constraints

- Source table is fixed to `lakehouse.analytics.real_behavior_detail_v1`; no caller-supplied SQL, catalog, schema, or table.
- Dataset and metric versions are exactly `rees46-multicategory` and `behavior-v1`.
- `metric_run_id` is exactly `behavior-v1-s<positive Iceberg snapshot id>`.
- `g2c-correctness-subset` requires exactly 1,002 source rows; `stable-user-2pct-full` requires exactly 2,199,938 source rows.
- `purchase_amount_proxy` is a decimal-string analysis proxy and must never be presented as GMV, revenue, or sales.
- DAY distinct counts and funnels are not additive; FULL values are recomputed from source facts.
- Missing category names and brands remain explicit unknown groups.
- Candidate metric rows are invisible until the publication row is written last.
- Existing `/metrics/*`, `/analysis/realtime`, and `/analysis/tools` contracts remain backward compatible.
- Runtime reports and extracted metric files stay under ignored `tmp/graduation/g2d/`; real data is never committed.
- Docker CLI fallback may add `D:\DockerProgram\Docker\resources\bin` to the current process only; no C-drive migration reversal.
- No Docker volume, Iceberg table, published metric run, or user file may be deleted by the implementation or verifier.

---

### Task 1: Define Metric, SQL, and Doris Contracts

**Files:**
- Create: `tests/test_g2d_behavior_metrics.py`
- Create: `configs/metrics/behavior-v1.json`
- Create: `infra/compose/doris/init/02_create_behavior_metrics.sql`
- Create: `jobs/sql/18_g2d_behavior_metrics.sql.template`

**Interfaces:**
- Consumes: fixed G2-C columns from `lakehouse.analytics.real_behavior_detail_v1`.
- Produces: `behavior-v1.json`; five fixed Doris table definitions; five ordered Trino result statements named `source_identity`, `overview`, `funnel`, `dimension`, and `quality` by `-- result:<name>` comments; the only SQL placeholder is numeric `__SNAPSHOT_ID__`.

- [ ] **Step 1: Write failing artifact-contract tests**

Create `G2dArtifactContractTests` with executable assertions, not only existence checks:

```python
class G2dArtifactContractTests(unittest.TestCase):
    def test_definition_catalog_declares_fixed_identity_and_proxy_warning(self):
        payload = json.loads(DEFINITIONS.read_text(encoding="utf-8"))
        self.assertEqual("behavior", payload["domain"])
        self.assertEqual("behavior-v1", payload["metric_version"])
        by_name = {item["metric_name"]: item for item in payload["definitions"]}
        self.assertIn("purchase_amount_proxy", by_name)
        self.assertEqual(["DAY", "FULL"], by_name["unique_user_count"]["allowed_windows"])
        self.assertIn("GMV", by_name["purchase_amount_proxy"]["forbidden_claims"])
        self.assertIn("收入", by_name["purchase_amount_proxy"]["forbidden_claims"])

    def test_doris_schema_uses_run_scoped_tables_without_destructive_sql(self):
        sql = DORIS_SCHEMA.read_text(encoding="utf-8")
        for table in EXPECTED_DORIS_TABLES:
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", sql)
        self.assertNotRegex(sql.upper(), r"\b(?:DROP|TRUNCATE|DELETE)\b")
        self.assertIn("metric_run_id", sql)
        self.assertIn("source_snapshot_id", sql)

    def test_trino_template_has_five_fixed_results_and_no_dynamic_source(self):
        sql = TRINO_TEMPLATE.read_text(encoding="utf-8")
        self.assertEqual(5, len(re.findall(r"(?m)^-- result:[a-z_]+$", sql)))
        self.assertEqual(5, len([part for part in sql.split(";") if part.strip()]))
        self.assertIn("lakehouse.analytics.real_behavior_detail_v1", sql)
        self.assertEqual({"__SNAPSHOT_ID__"}, set(re.findall(r"__[A-Z0-9_]+__", sql)))
        self.assertIn("FOR VERSION AS OF __SNAPSHOT_ID__", sql)
        self.assertIn("source_row_number", sql)
        self.assertIn("purchase_amount_proxy", sql)
```

- [ ] **Step 2: Run the contract tests and confirm RED**

Run:

```powershell
python -m unittest tests.test_g2d_behavior_metrics.G2dArtifactContractTests -v
```

Expected: FAIL because the catalog, Doris schema, and Trino template do not exist.

- [ ] **Step 3: Create the versioned metric-definition catalog**

Use this top-level shape and require every entry to include all shown keys:

```json
{
  "domain": "behavior",
  "dataset_id": "rees46-multicategory",
  "metric_version": "behavior-v1",
  "definitions": [
    {
      "metric_name": "event_count",
      "display_name": "有效行为事件数",
      "formula": "count(*)",
      "numerator": "all valid facts",
      "denominator": null,
      "source_fields": ["event_id"],
      "allowed_windows": ["DAY", "FULL"],
      "additive": true,
      "null_policy": "event_id must be non-empty",
      "limitations": ["The correctness subset is not the full sampled population."],
      "forbidden_claims": []
    },
    {
      "metric_name": "purchase_amount_proxy",
      "display_name": "购买事件金额合计（分析代理值）",
      "formula": "sum(price_decimal) where event_type = 'purchase'",
      "numerator": "purchase event price_decimal",
      "denominator": null,
      "source_fields": ["event_type", "price_decimal"],
      "allowed_windows": ["DAY", "FULL"],
      "additive": true,
      "null_policy": "invalid price fails publication",
      "limitations": ["No quantity, currency, discount, refund, cancellation, or payment state."],
      "forbidden_claims": ["GMV", "销售额", "收入"]
    }
  ]
}
```

Add definitions for exactly these 25 public metrics: `event_count`, `view_count`, `cart_count`, `purchase_count`, `unique_user_count`, `session_count`, `product_count`, `purchase_amount_proxy`, `view_sessions`, `view_to_cart_sessions`, `completed_sessions`, `view_to_cart_rate`, `cart_to_purchase_rate`, `full_conversion_rate`, `clean_event_count`, `late_event_count`, `distinct_event_count`, `duplicate_event_count`, `missing_session_count`, `unknown_category_count`, `unknown_brand_count`, `invalid_event_type_count`, `empty_key_id_count`, `invalid_price_count`, and `invalid_derived_date_count`.

- [ ] **Step 4: Create the non-destructive Doris schema**

Define these fixed tables in database `analytics`:

```sql
CREATE TABLE IF NOT EXISTS behavior_metric_publications (
    metric_run_id VARCHAR(96) NOT NULL,
    dataset_id VARCHAR(64) NOT NULL,
    metric_version VARCHAR(32) NOT NULL,
    data_scope VARCHAR(64) NOT NULL,
    source_snapshot_id LARGEINT NOT NULL,
    source_event_count BIGINT NOT NULL,
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    calculated_at DATETIME(3) NOT NULL,
    published_at DATETIME(3) NOT NULL,
    overview_row_count BIGINT NOT NULL,
    overview_sha256 CHAR(64) NOT NULL,
    funnel_row_count BIGINT NOT NULL,
    funnel_sha256 CHAR(64) NOT NULL,
    dimension_row_count BIGINT NOT NULL,
    dimension_sha256 CHAR(64) NOT NULL,
    quality_row_count BIGINT NOT NULL,
    quality_sha256 CHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL
)
UNIQUE KEY(metric_run_id)
DISTRIBUTED BY HASH(metric_run_id) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");
```

The other four tables use one bucket for local development and these exact keys/business columns:

- `behavior_overview_metrics`: `UNIQUE KEY(metric_run_id, window_type, window_start)`; window end, the seven integer counts, and `DECIMAL(38,2) purchase_amount_proxy`.
- `behavior_funnel_metrics`: `UNIQUE KEY(metric_run_id, window_type, window_start)`; window end, missing-session event count, `view_sessions`, `view_to_cart_sessions`, and `completed_sessions`.
- `behavior_dimension_metrics`: `UNIQUE KEY(metric_run_id, window_type, window_start, dimension_type, dimension_id)`; window end, nullable `dimension_name`, `is_unknown`, three event counts, unique users, and amount proxy.
- `behavior_quality_metrics`: `UNIQUE KEY(metric_run_id, window_type, window_start)`; window end, route counts, distinct IDs, duplicate difference, missing/unknown/invalid counts, and reconciliation status. The first release emits exactly one FULL row.

Do not add `DROP`, `TRUNCATE`, `DELETE`, auto-generated timestamps, or a second database.

- [ ] **Step 5: Create the five-statement Trino template**

Use one fixed `base` CTE per statement. The overview result must use DAY rows plus an independently recomputed FULL row:

```sql
-- result:overview
WITH base AS (
    SELECT event_date, event_type, user_id, user_session, product_id,
           price_decimal, event_id
    FROM lakehouse.analytics.real_behavior_detail_v1
        FOR VERSION AS OF __SNAPSHOT_ID__
),
day_rows AS (
    SELECT 'DAY' AS window_type, event_date AS window_start, event_date AS window_end,
           count(*) AS event_count,
           count_if(event_type = 'view') AS view_count,
           count_if(event_type = 'cart') AS cart_count,
           count_if(event_type = 'purchase') AS purchase_count,
           count(DISTINCT user_id) FILTER (WHERE user_id IS NOT NULL) AS unique_user_count,
           count(DISTINCT user_session) FILTER (WHERE user_session IS NOT NULL) AS session_count,
           count(DISTINCT product_id) FILTER (WHERE product_id IS NOT NULL) AS product_count,
           coalesce(sum(price_decimal) FILTER (WHERE event_type = 'purchase'), DECIMAL '0.00')
               AS purchase_amount_proxy
    FROM base GROUP BY event_date
),
full_row AS (
    SELECT 'FULL' AS window_type, min(event_date) AS window_start, max(event_date) AS window_end,
           count(*) AS event_count,
           count_if(event_type = 'view') AS view_count,
           count_if(event_type = 'cart') AS cart_count,
           count_if(event_type = 'purchase') AS purchase_count,
           count(DISTINCT user_id) FILTER (WHERE user_id IS NOT NULL) AS unique_user_count,
           count(DISTINCT user_session) FILTER (WHERE user_session IS NOT NULL) AS session_count,
           count(DISTINCT product_id) FILTER (WHERE product_id IS NOT NULL) AS product_count,
           coalesce(sum(price_decimal) FILTER (WHERE event_type = 'purchase'), DECIMAL '0.00')
               AS purchase_amount_proxy
    FROM base
)
SELECT * FROM day_rows
UNION ALL
SELECT * FROM full_row
ORDER BY window_type, window_start;
```

For `funnel`, calculate DAY and FULL independently through three stages: first view ordering key, first cart strictly at or after that key, and first purchase strictly at or after the cart key. Build the ordering key from `event_ts`, `source_file`, `source_row_number`, and `event_id`; do not infer FULL by summing DAY rows.

For `dimension`, emit DAY and FULL for `product`, `category`, and `brand`. Category key is `category_id`; category display is `category_code`; null category/brand display values use `is_unknown=true` rather than disappearing.

For `source_identity`, return one row with total, min/max date, distinct event IDs, the literal selected Snapshot identity, and its commit time from `$snapshots`. For `quality`, return FULL counts needed by the design and a deterministic reconciliation flag. Every read of the fact table in all five statements must contain `FOR VERSION AS OF __SNAPSHOT_ID__`; no statement may read the unpinned current table.

- [ ] **Step 6: Run Task 1 tests and confirm GREEN**

Run:

```powershell
python -m unittest tests.test_g2d_behavior_metrics.G2dArtifactContractTests -v
```

Expected: all Task 1 tests PASS.

- [ ] **Step 7: Commit the metric contracts**

```powershell
git add tests/test_g2d_behavior_metrics.py configs/metrics/behavior-v1.json infra/compose/doris/init/02_create_behavior_metrics.sql jobs/sql/18_g2d_behavior_metrics.sql.template
git commit -m "feat: define G2-D behavior metric contracts"
```

### Task 2: Build Pure Identity, Parsing, and Reconciliation Functions

**Files:**
- Create: `scripts/lib/G2d.BehaviorMetrics.psm1`
- Modify: `tests/test_g2d_behavior_metrics.py`

**Interfaces:**
- Consumes: five named CSV result sets from Task 1 and `DataScope`.
- Produces: `Get-G2dMetricIdentity`, `Split-G2dNamedSql`, `ConvertFrom-G2dCsv`, `Assert-G2dMetricBundle`, `Get-G2dCanonicalDigest`, `Get-G2dPublicationOrder`, and `Export-G2dCandidateCsv`.

- [ ] **Step 1: Add failing PowerShell behavior tests**

Invoke the module from Python and assert a compact JSON result:

```powershell
Import-Module ./scripts/lib/G2d.BehaviorMetrics.psm1 -Force
$identity = Get-G2dMetricIdentity -SnapshotId 3854376992136224865 `
    -DataScope g2c-correctness-subset -SourceEventCount 1002
$rejectedScope = $false
try {
    Get-G2dMetricIdentity -SnapshotId 3854376992136224865 `
        -DataScope stable-user-2pct-full -SourceEventCount 1002 | Out-Null
} catch { $rejectedScope = $true }
[ordered]@{
    run_id = $identity.MetricRunId
    rejected_scope = $rejectedScope
    publication_order = @(Get-G2dPublicationOrder)
} | ConvertTo-Json -Compress
```

Expected assertions:

```python
self.assertEqual("behavior-v1-s3854376992136224865", payload["run_id"])
self.assertTrue(payload["rejected_scope"])
self.assertEqual(
    ["overview", "funnel", "dimension", "quality", "publication"],
    payload["publication_order"],
)
```

Add negative cases for zero/negative/non-integer Snapshot identity, unknown scope, duplicate SQL result markers, missing result markers, malformed CSV, DAY total mismatch, event-type sum mismatch, duplicate event IDs, invalid funnel ordering, and publication not last. Add digest cases proving row order does not change the digest while a changed cell does.

- [ ] **Step 2: Run the module tests and confirm RED**

Run:

```powershell
python -m unittest tests.test_g2d_behavior_metrics.G2dPowerShellContractTests -v
```

Expected: FAIL because the module does not exist.

- [ ] **Step 3: Implement strict metric identity and SQL splitting**

Use exact scope/count rules:

```powershell
function Get-G2dMetricIdentity {
    param(
        [Parameter(Mandatory = $true)][long]$SnapshotId,
        [Parameter(Mandatory = $true)][ValidateSet(
            'g2c-correctness-subset', 'stable-user-2pct-full'
        )][string]$DataScope,
        [Parameter(Mandatory = $true)][long]$SourceEventCount
    )
    if ($SnapshotId -le 0) { throw 'G2-D snapshot ID must be positive.' }
    $expected = if ($DataScope -ceq 'g2c-correctness-subset') { 1002L } else { 2199938L }
    if ($SourceEventCount -ne $expected) { throw 'G2-D data scope does not match source count.' }
    [pscustomobject][ordered]@{
        MetricRunId = "behavior-v1-s$SnapshotId"
        DatasetId = 'rees46-multicategory'
        MetricVersion = 'behavior-v1'
        DataScope = $DataScope
        SourceSnapshotId = $SnapshotId
        SourceEventCount = $SourceEventCount
    }
}
```

`Split-G2dNamedSql` must validate a positive Snapshot ID, replace every exact `__SNAPSHOT_ID__` token with its invariant decimal representation, reject every other placeholder, and return an ordered dictionary containing exactly the five result names from Task 1. It rejects duplicate/missing markers and extra semicolon statements and never evaluates SQL as PowerShell.

- [ ] **Step 4: Implement CSV parsing and bundle reconciliation**

`ConvertFrom-G2dCsv` accepts CSV text with one header and at least one data row, rejects duplicate headers, and returns `PSCustomObject[]`. `Assert-G2dMetricBundle` must enforce:

```powershell
if ($fullOverview.event_count -ne $Identity.SourceEventCount) { throw 'Source total mismatch.' }
if ($fullOverview.view_count + $fullOverview.cart_count + $fullOverview.purchase_count -gt
        $fullOverview.event_count) {
    throw 'Named event type totals exceed the valid event total.'
}
if (($dayOverview | Measure-Object event_count -Sum).Sum -ne $fullOverview.event_count) {
    throw 'DAY and FULL totals do not reconcile.'
}
if ($fullQuality.distinct_event_count -ne $fullOverview.event_count) {
    throw 'Duplicate event IDs detected.'
}
if ($fullFunnel.completed_sessions -gt $fullFunnel.view_to_cart_sessions -or
        $fullFunnel.view_to_cart_sessions -gt $fullFunnel.view_sessions) {
    throw 'Funnel stages are not monotonic.'
}
```

The named overview counts intentionally omit valid `remove_from_cart` events, so their sum may be lower than `event_count`; only a sum greater than the valid event total is contradictory. Also require exactly one FULL row per overview/funnel/quality family; identical DAY/FULL window sets; nonnegative integers; two-decimal nonnegative amount strings; known dimension types; unique composite keys; and `quality.reconciliation_status='PASS'`. Reconcile source, overview, quality, distinct-event, clean/late, duplicate, invalid-field, missing-session, and rate fields. Require DAY additive overview totals to equal FULL, funnel session bounds to agree with overview/quality totals, every dimension family to cover every metric window, and each FULL dimension ID/value to equal the union/sum of its DAY rows. If stored rows include identity columns, require the complete identity tuple and exact equality with the requested run.

- [ ] **Step 5: Implement deterministic candidate CSV export**

`Export-G2dCandidateCsv` writes UTF-8 without BOM under an explicitly supplied directory, prepends identity columns to every metric row, uses one fixed column list per target, formats booleans as `0/1`, and rejects paths outside that directory. Do not write publication metadata here.

`Get-G2dCanonicalDigest` receives rows, a fixed column list, and fixed unique-key columns. Normalize null as JSON `null`, integers as invariant base-10, booleans as `0/1`, decimals with two digits, dates as `yyyy-MM-dd`, and timestamps as UTC ISO-8601. Sort by the normalized unique-key tuple, serialize each row as a compact JSON array, join rows with LF, then hash UTF-8 bytes with SHA-256. Reject duplicate normalized keys.

- [ ] **Step 6: Run Task 2 tests and confirm GREEN**

```powershell
python -m unittest tests.test_g2d_behavior_metrics.G2dPowerShellContractTests -v
```

Expected: all Task 2 tests PASS.

- [ ] **Step 7: Commit the pure pipeline module**

```powershell
git add tests/test_g2d_behavior_metrics.py scripts/lib/G2d.BehaviorMetrics.psm1
git commit -m "feat: validate G2-D metric bundles"
```

### Task 3: Implement Fail-Closed Refresh and Publication

**Files:**
- Create: `scripts/refresh_g2d_behavior_metrics.ps1`
- Modify: `tests/test_g2d_behavior_metrics.py`

**Interfaces:**
- Consumes: `-DataScope`, fixed SQL/DDL artifacts, running Trino and Doris dependencies.
- Produces: candidate files and `refresh-report.json` under `tmp/graduation/g2d/<metric_run_id>/`; a Doris publication visible only after all four metric loads pass.

- [ ] **Step 1: Add failing refresh safety tests**

Dot-source with `-FunctionsOnly` and verify:

```powershell
$plan = Get-G2dRefreshPlan -MetricRunId 'behavior-v1-s3854376992136224865'
[ordered]@{
    tables = @($plan | ForEach-Object { $_.Name })
    publication_last = $plan[-1].Name -ceq 'publication'
    plan_only = Invoke-G2dRefresh -DataScope g2c-correctness-subset -PlanOnly
} | ConvertTo-Json -Depth 6 -Compress
```

Tests must reject unsafe run IDs, an already published run with mismatched identity, Trino result count other than five, a Stream Load response other than exact `Success`, candidate row-count mismatch, stale extra candidate rows, and publication SQL attempted before all four metric-table checks pass. An already published run with the exact same identity returns `already_published` without loading. A partially loaded but unpublished run is allowed to continue with a new attempt identity.

- [ ] **Step 2: Run refresh tests and confirm RED**

```powershell
python -m unittest tests.test_g2d_behavior_metrics.G2dRefreshSafetyTests -v
```

Expected: FAIL because the refresh script does not exist.

- [ ] **Step 3: Implement dependency and preflight checks**

The script must:

```powershell
$dockerFallback = 'D:\DockerProgram\Docker\resources\bin'
if (-not (Get-Command docker -ErrorAction SilentlyContinue) -and
        (Test-Path (Join-Path $dockerFallback 'docker.exe'))) {
    $env:PATH = "$dockerFallback;$env:PATH"
}
```

Then verify Docker, Trino `/v1/info`, Doris `SELECT 1`, fixed source table existence, and the latest positive Snapshot ID. It must not start or remove containers automatically; on missing dependencies it exits with a command pointing to the existing project startup/runbook. This keeps container lifecycle separate from metric publication and avoids deleting or recreating persisted state.

Do not inspect publication or candidate rows until the fixed idempotent Doris DDL has run in Step 5.

- [ ] **Step 4: Implement fixed Trino execution and candidate generation**

Read the latest positive Snapshot ID from the fixed `$snapshots` metadata table, render the sole numeric placeholder, and execute each named SQL statement separately with the Trino CLI in `ecom-trino` using `CSV_HEADER_UNQUOTED`. Parse the source identity first, require it to echo the selected Snapshot, derive the metric identity, execute the remaining four snapshot-pinned statements, call `Assert-G2dMetricBundle`, and export candidate CSVs.

Do not accept caller-provided SQL paths or statement text. Store SHA-256 for the SQL template and every candidate file in the report.

- [ ] **Step 5: Implement Doris initialization and Stream Load**

Initialize the fixed DDL through the Doris FE MySQL client, then require all five tables to exist. If a publication exists, require every identity, row-count and digest field to match the currently stored metric rows before returning `already_published`; any mismatch fails. Record counts for unpublished candidate rows, but do not delete them and do not reject a partial candidate solely because it exists.

Generate one safe `attempt_id` with `[guid]::NewGuid().ToString('N')` per invocation and use label `<metric_run_id>-<table>-<attempt_id>` with fixed column mapping. Unique-key loading makes an interrupted run idempotently refill the same candidate keys. Accept only this response:

```powershell
if ([string]$response.Status -cne 'Success' -or
        [long]$response.NumberLoadedRows -ne $ExpectedRows -or
        [long]$response.NumberFilteredRows -ne 0) {
    throw "G2-D Stream Load failed for $TableName."
}
```

Do not accept `Publish Timeout` as success because its visibility is uncertain. Query every fixed column back from Doris ordered by the table's exact unique key, normalize through `Get-G2dCanonicalDigest`, and require the candidate row count and SHA-256 to match; stale extra or altered rows therefore fail instead of being silently published.

- [ ] **Step 6: Publish metadata last**

After all four tables pass, insert exactly one `PUBLISHED` row via a fixed parameter-safe SQL value builder, including each table's row count and SHA-256. Immediately read it back and require identity, Snapshot, scope, source count, dates, four row counts, four hashes and status to match. `Get-G2dRefreshPlan` must always return:

```powershell
@(
    [pscustomobject]@{ Name = 'overview'; Target = 'behavior_overview_metrics' },
    [pscustomobject]@{ Name = 'funnel'; Target = 'behavior_funnel_metrics' },
    [pscustomobject]@{ Name = 'dimension'; Target = 'behavior_dimension_metrics' },
    [pscustomobject]@{ Name = 'quality'; Target = 'behavior_quality_metrics' },
    [pscustomobject]@{ Name = 'publication'; Target = 'behavior_metric_publications' }
)
```

- [ ] **Step 7: Run Task 3 tests and confirm GREEN**

```powershell
python -m unittest tests.test_g2d_behavior_metrics.G2dRefreshSafetyTests -v
python -m unittest tests.test_g2d_behavior_metrics -v
```

Expected: all G2-D offline pipeline tests PASS without Docker.

- [ ] **Step 8: Commit the refresh pipeline**

```powershell
git add tests/test_g2d_behavior_metrics.py scripts/refresh_g2d_behavior_metrics.ps1
git commit -m "feat: publish G2-D metric snapshots safely"
```

### Task 4: Add Typed API Models and Metric Definition Loading

**Files:**
- Create: `services/api/app/behavior_models.py`
- Create: `services/api/app/metric_definitions.py`
- Create: `tests/test_behavior_metrics_api.py`
- Modify: `services/api/app/config.py`
- Modify: `infra/docker-compose.yml`
- Modify: `infra/.env.example`

**Interfaces:**
- Consumes: `configs/metrics/behavior-v1.json`.
- Produces: strict Pydantic response types, `MetricDefinitionCatalog.load(path)`, and `ApiSettings.behavior_metric_definitions_path`.

- [ ] **Step 1: Write failing model and catalog tests**

Test exact identities and serialization:

```python
def test_metric_meta_preserves_snapshot_and_subset_warning(self):
    meta = BehaviorMetricMeta(
        dataset_id="rees46-multicategory",
        metric_version="behavior-v1",
        metric_run_id="behavior-v1-s3854376992136224865",
        source_snapshot_id="3854376992136224865",
        window_start=date(2019, 10, 1),
        window_end=date(2019, 11, 30),
        calculated_at=datetime(2026, 9, 18, tzinfo=UTC),
        data_scope="g2c-correctness-subset",
        source_event_count=1002,
        warnings=["correctness subset; not the full 2% user sample"],
    )
    self.assertEqual("3854376992136224865", meta.model_dump(mode="json")["source_snapshot_id"])

def test_catalog_requires_proxy_forbidden_claims(self):
    catalog = MetricDefinitionCatalog.load(DEFINITIONS)
    proxy = catalog.get("purchase_amount_proxy")
    self.assertEqual({"GMV", "销售额", "收入"}, set(proxy.forbidden_claims))
```

Add malformed-catalog cases for duplicate metric names, wrong domain/version/dataset, missing public metric, unknown source field, missing limitations, missing proxy forbidden claims, and extra JSON keys.

- [ ] **Step 2: Run Task 4 tests and confirm RED**

```powershell
python -m unittest tests.test_behavior_metrics_api.BehaviorModelsAndCatalogTests -v
```

Expected: FAIL because model and catalog modules do not exist.

- [ ] **Step 3: Implement strict models**

Create models with `ConfigDict(extra="forbid")`:

```python
class BehaviorMetricMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")
    dataset_id: Literal["rees46-multicategory"]
    metric_version: Literal["behavior-v1"]
    metric_run_id: str = Field(pattern=r"^behavior-v1-s[1-9][0-9]*$")
    source_snapshot_id: str = Field(pattern=r"^[1-9][0-9]*$")
    window_start: date
    window_end: date
    calculated_at: datetime
    data_scope: Literal["g2c-correctness-subset", "stable-user-2pct-full"]
    source_event_count: int = Field(gt=0)
    warnings: list[str]
```

Add `OverviewPoint`, `FunnelPoint`, `DimensionRanking`, `QualityMetrics`, `PublicationResponse`, `OverviewResponse`, `FunnelResponse`, `RankingsResponse`, `QualityResponse`, `MetricDefinition`, and `MetricDefinitionsResponse`. Monetary and rate fields are strings matching `^[0-9]+\.[0-9]{2,6}$` or `None`; do not serialize binary floats.

- [ ] **Step 4: Implement strict catalog loading**

Use `json.loads`, Pydantic validation, a duplicate-name check before dictionary construction, and an exact set comparison against the 25 required public metrics from Task 1. `source_fields` entries are limited to `event_id`, `event_time`, `event_type`, `user_id`, `user_session`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `price_decimal`, `quality_route`, `event_date`, `event_ts`, `source_file`, `source_row_number`, and `lateness_ms`. `get(name)` returns a validated `MetricDefinition`; `all()` preserves JSON order.

- [ ] **Step 5: Wire the read-only catalog path**

Add `BEHAVIOR_METRIC_DEFINITIONS_PATH`. For host-side tests, derive the absolute default from `Path(__file__).resolve().parents[3] / "configs" / "metrics" / "behavior-v1.json"`, never from the caller's current directory. Set `/app/configs/metrics/behavior-v1.json` in `.env.example` and mount the repository directory read-only in the API container:

```dotenv
BEHAVIOR_METRIC_DEFINITIONS_PATH=/app/configs/metrics/behavior-v1.json
```

```yaml
environment:
  BEHAVIOR_METRIC_DEFINITIONS_PATH: ${BEHAVIOR_METRIC_DEFINITIONS_PATH}
volumes:
  - ../services/api:/app
  - ../configs/metrics:/app/configs/metrics:ro
```

Reject an empty path in `ApiSettings.__post_init__`.

- [ ] **Step 6: Run Task 4 tests and existing API tests**

```powershell
python -m unittest tests.test_behavior_metrics_api.BehaviorModelsAndCatalogTests -v
python -m unittest tests.test_api_service tests.test_analysis_api tests.test_tool_analysis_api -q
```

Expected: all listed tests PASS.

- [ ] **Step 7: Commit models and definitions**

```powershell
git add services/api/app/behavior_models.py services/api/app/metric_definitions.py services/api/app/config.py infra/docker-compose.yml infra/.env.example tests/test_behavior_metrics_api.py
git commit -m "feat: add G2-D metric API contracts"
```

### Task 5: Implement Doris Repository, Service, and Versioned Routes

**Files:**
- Create: `services/api/app/behavior_repository.py`
- Create: `services/api/app/behavior_service.py`
- Modify: `services/api/app/dependencies.py`
- Modify: `services/api/app/main.py`
- Modify: `tests/test_behavior_metrics_api.py`
- Modify: `tests/test_api_service.py`

**Interfaces:**
- Consumes: latest PUBLISHED Doris run and validated metric catalog.
- Produces: `BehaviorMetricsRepository`, `BehaviorMetricsService`, `BehaviorMetricsUnavailableError`, and six `/api/v1` GET routes.

- [ ] **Step 1: Write failing repository security tests**

Use a recording fake connection and assert parameterization:

```python
def test_rankings_use_whitelisted_order_expression_and_bound_values(self):
    repository, cursor = make_repository(rows=[])
    repository.fetch_rankings(
        metric_run_id="behavior-v1-s1",
        dimension="category",
        window="FULL",
        sort_by="purchases",
        limit=20,
    )
    sql, parameters = cursor.execute.call_args.args
    self.assertIn("ORDER BY purchase_count DESC", sql)
    self.assertNotIn("category", sql.replace("dimension_type = %s", ""))
    self.assertEqual(("behavior-v1-s1", "category", "FULL", 20), parameters)
```

Reject unknown dimension/window/sort values before opening a connection. Verify every data query binds `metric_run_id` from the publication row and never independently selects a different run.

- [ ] **Step 2: Write failing service and route tests**

Inject a fake service into `create_app` and test all routes. Required cases:

```python
response = client.get(
    "/api/v1/behavior/rankings",
    params={"dimension": "brand", "window": "full", "sort_by": "purchases", "limit": 20},
)
self.assertEqual(200, response.status_code)
self.assertEqual("behavior-v1", response.json()["meta"]["metric_version"])

self.assertEqual(422, client.get(
    "/api/v1/behavior/rankings",
    params={"dimension": "brand;drop table x", "limit": 101},
).status_code)
```

Also test no publication -> 503, database error -> safe 503, empty ranking -> 200 empty list, DAY responses ordered by date, null funnel rates on zero denominators, two-decimal amount strings, definitions endpoint, and all old API tests unchanged.

- [ ] **Step 3: Run Task 5 tests and confirm RED**

```powershell
python -m unittest tests.test_behavior_metrics_api -v
```

Expected: FAIL because repository, service, and routes do not exist.

- [ ] **Step 4: Implement the repository**

Use the existing PyMySQL connection pattern and fixed mappings:

```python
_SORT_COLUMNS = {
    "views": "view_count",
    "carts": "cart_count",
    "purchases": "purchase_count",
    "users": "unique_user_count",
    "amount": "purchase_amount_proxy",
}
_DIMENSIONS = frozenset({"product", "category", "brand"})
_WINDOWS = {"day": "DAY", "full": "FULL"}
```

`fetch_latest_publication()` selects only `status='PUBLISHED'`, ordered by `published_at DESC, metric_run_id DESC`, limit 1. Expose fixed methods `fetch_overview`, `fetch_funnel`, `fetch_rankings`, and `fetch_quality`; do not expose a generic query method.

- [ ] **Step 5: Implement the service**

`BehaviorMetricsService` loads the publication once per request, converts Doris `LARGEINT source_snapshot_id` to a base-10 string for JSON safety, treats Doris `DATETIME(3)` values as UTC, builds one `BehaviorMetricMeta`, and passes that exact `metric_run_id` to the data query. It validates publication identity and scope/count consistency again.

Compute funnel rates with `Decimal`:

```python
def _ratio(numerator: int, denominator: int) -> str | None:
    if denominator == 0:
        return None
    return format((Decimal(numerator) / Decimal(denominator)).quantize(
        Decimal("0.000001"), rounding=ROUND_HALF_UP
    ), "f")
```

For `g2c-correctness-subset`, always include `correctness subset; not the full 2% user sample` in `meta.warnings`. For the full scope, require 2,199,938 source rows and omit that warning.

- [ ] **Step 6: Wire dependencies and routes**

Add optional `behavior_service` injection to `create_app` so existing tests do not need live Doris. Overview and funnel default to `window=full`; rankings require `dimension`, default to `window=full`, `sort_by=purchases`, and `Query(20, ge=1, le=100)`. Use `Literal` for every enum. Quality and publication accept no filtering parameters. Map only `BehaviorMetricsUnavailableError` to a generic 503 and log only route stage plus exception type.

Definitions route reads the already validated in-memory catalog and never queries Doris.

- [ ] **Step 7: Run API regressions and full G2-D offline tests**

```powershell
python -m unittest tests.test_behavior_metrics_api tests.test_api_service -v
python -m unittest tests.test_g2d_behavior_metrics -v
python -m unittest tests.test_analysis_api tests.test_tool_analysis_api tests.test_trino_repository -q
```

Expected: all listed tests PASS.

- [ ] **Step 8: Commit the versioned behavior API**

```powershell
git add services/api/app/behavior_repository.py services/api/app/behavior_service.py services/api/app/dependencies.py services/api/app/main.py tests/test_behavior_metrics_api.py tests/test_api_service.py
git commit -m "feat: serve published G2-D behavior metrics"
```

### Task 6: Verify the Real Snapshot and Close G2-D

**Files:**
- Create: `scripts/verify_g2d_behavior_metrics.ps1`
- Create: `docs/graduation/behavior-metrics-api-runbook.md`
- Modify: `tests/test_g2d_behavior_metrics.py`
- Modify: `README.md`
- Modify: `docs/graduation/data-readiness.md`
- Modify: `docs/superpowers/plans/2026-09-18-graduation-behavior-metrics-api.md`

**Interfaces:**
- Consumes: the formal 1,002-row Iceberg table, a published Doris run, and running API.
- Produces: ignored `verification.json`, exact Trino/Doris/API reconciliation, measured runbook evidence, and the next-stage marker G2-E.

- [x] **Step 1: Write failing verifier-function tests**

Dot-source with `-FunctionsOnly` and test:

```powershell
$evidence = Assert-G2dVerificationEvidence `
    -ExpectedRunId 'behavior-v1-s3854376992136224865' `
    -ExpectedSnapshotId 3854376992136224865 `
    -SourceTotal 1002 -DayTotal 1002 -FullTotal 1002 `
    -CleanCount 1001 -LateCount 1 -DistinctEventCount 1002 `
    -ApiRunId 'behavior-v1-s3854376992136224865' `
    -ApiSourceTotal 1002
$evidence.Status
```

Add negative cases for wrong run/Snapshot, DAY/FULL mismatch, clean+late mismatch, duplicate IDs, API/Doris identity mismatch, missing subset warning, missing proxy limitation, definitions that permit GMV wording, and any unvalidated metric table.

- [x] **Step 2: Run verifier tests and confirm RED**

```powershell
python -m unittest tests.test_g2d_behavior_metrics.G2dVerifierTests -v
```

Expected: FAIL because the verifier does not exist.

- [x] **Step 3: Implement fail-closed three-layer verification**

The verifier must check dependency readiness without deleting or recreating services, then:

1. Read the latest PUBLISHED run from Doris and require scope `g2c-correctness-subset`, plus four nonzero/expected row counts and four lowercase SHA-256 values.
2. Query Trino source identity and require Snapshot, total 1,002 and distinct IDs 1,002.
3. Query all four Doris metric tables by the exact run ID and recompute each canonical digest.
4. Require the complete bundle contract: DAY additive totals sum to FULL, named event-type counts never exceed the event total (preserving `remove_from_cart`), source/quality/distinct/clean/late/duplicate/invalid/missing-session totals and rates reconcile, funnel stages are monotonic and bounded by overview sessions, all metric windows agree, and dimension DAY/FULL identity and additive totals reconcile.
5. Call all six API endpoints and compare every metadata and payload field exactly with the PUBLISHED Doris row and all four stored metric families. Recompute funnel and quality rates, require exact ranking order and unique rows, and require the correctness-subset warning only for subset scope (with no such warning for full scope).
6. Require the definition endpoint to return the exact ordered set of 25 unique definitions, including every field and array from the validated local catalog, nonblank limitations, and the amount-proxy forbidden claims.
7. Write a compact report only after every assertion passes.

- [x] **Step 4: Run verifier tests and confirm GREEN**

```powershell
python -m unittest tests.test_g2d_behavior_metrics.G2dVerifierTests -v
python -m unittest tests.test_g2d_behavior_metrics tests.test_behavior_metrics_api -v
```

Expected: all G2-D tests PASS offline.

- [x] **Step 5: Run the real refresh and dynamic verification**

Start only the already-defined lakehouse, Doris and API dependencies. First make the migrated Docker CLI visible to this PowerShell process and start services that do not have the persisted-schema resume issue:

```powershell
$env:PATH='D:\DockerProgram\Docker\resources\bin;' + $env:PATH
docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile lakehouse --profile serving up -d minio minio-init metastore-postgres doris-fe doris-be
```

If `ecom-hive-metastore` is already running, keep it. If it is absent, create it with resume mode. If an exited container holds that exact name, inspect its logs, remove only that stopped container (never its PostgreSQL/MinIO volumes), then run:

```powershell
docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile lakehouse run -d --name ecom-hive-metastore -e IS_RESUME=true hive-metastore
docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile lakehouse --profile serving up -d trino api
```

Require Trino `/v1/info`, Doris `SELECT 1`, and API `/health` before refreshing.

Run:

```powershell
./scripts/refresh_g2d_behavior_metrics.ps1 -DataScope g2c-correctness-subset
./scripts/verify_g2d_behavior_metrics.ps1 -ExpectedDataScope g2c-correctness-subset
```

Expected final verifier JSON includes `"status":"PASS"`, the measured Snapshot/run identity, source/overview total 1,002, clean 1,001, late 1, and all six API contracts.

- [x] **Step 6: Run complete regressions**

Ensure the migrated Docker CLI is visible only in this process, then run:

```powershell
$env:PATH='D:\DockerProgram\Docker\resources\bin;' + $env:PATH
python -m unittest discover -s tests -q
```

Expected: all tests PASS with an exact count recorded in the runbook. Run `git diff --check` and confirm `tmp/`, `data/`, Maven `target/`, secrets and verification output are not staged.

- [x] **Step 7: Document only measured results**

The runbook must record commands, source table, data scope, metric run, Snapshot, row counts per metric family, funnel counts, amount-proxy caveat, API responses, elapsed time, failures encountered, and the explicit 1,002-row capacity boundary. README and data-readiness move the next step to G2-E only after dynamic verification passes.

- [x] **Step 8: Commit locally and hand off independent review/push**

```powershell
git add scripts/verify_g2d_behavior_metrics.ps1 scripts/refresh_g2d_behavior_metrics.ps1 scripts/lib/G2d.BehaviorMetrics.psm1 infra/compose/doris/init/02_create_behavior_metrics.sql services/api/app/config.py tests/test_g2d_behavior_metrics.py tests/test_behavior_metrics_api.py docs/graduation/behavior-metrics-api-runbook.md README.md docs/graduation/data-readiness.md docs/superpowers/plans/2026-09-18-graduation-behavior-metrics-api.md
git commit -m "docs: record G2-D behavior metrics acceptance"
```

The Task 6 owner stops after the local commit. The controller owns the required broad independent whole-branch review, final verification, push, and local/remote hash comparison; do not merge `main`.

## Task 6 Acceptance Record (2026-09-19)

- Dynamic status: `PASS` for Snapshot `881836466779140976` and run `behavior-v1-s881836466779140976` at scope `g2c-correctness-subset`.
- Reconciliation: source/DAY/FULL `1,002`; distinct events `1,002`; clean `1,001`; late `1`; ordered FULL funnel `275 -> 11 -> 5`.
- Doris evidence: overview/funnel/dimension/quality rows `2/2/1,780/1`; all four recomputed SHA-256 values match the PUBLISHED row.
- API evidence: all six contracts returned HTTP 200 with the published identity and required correctness-subset warning; definitions used `GET /api/v1/metrics/definitions?domain=behavior&version=behavior-v1`, and the catalog retained the amount-proxy limitation and forbidden claims.
- Timing: successful refresh `34,791 ms`; Fix Round 1 verifier rerun `22,824 ms` internally and `23,401 ms` wall time.
- Regression: the eight focused acceptance-fix tests passed in `5.368 s`; `python -m unittest discover -s tests -q` passed `552` tests in `302.152 s`.
- Boundary: the 1,002-row subset is correctness-only, explicitly not capacity evidence or the full 2% user sample. G2-E is next.
- Detailed commands, failures, recovery actions, values and digests are recorded in `docs/graduation/behavior-metrics-api-runbook.md`.
- Final hardening note (2026-09-21): this dynamic result is retained as historical pre-hardening evidence. The published run was not refreshed or overwritten after SQL identity/date semantics and verifier strictness changed; a naturally new source Snapshot is required for a post-hardening publication and live acceptance run.

## Self-Review

- Spec coverage: Tasks 1-6 cover metric semantics, DAY/FULL windows, ordered funnel, unknown dimensions, quality, snapshot publication, definition catalog, typed API, error handling, dynamic acceptance, resource limits and future handoff.
- Placeholder scan: the plan contains no unfinished marker, generic “handle errors” step, omitted public metric list, or undefined neighboring interface.
- Type consistency: `metric_run_id`, `source_snapshot_id`, `data_scope`, `source_event_count`, window names and route enums remain identical from SQL output through PowerShell, Doris, Service and API.
- Scope: Olist ingestion, React pages, RAG, arbitrary date-range distinct counts and real-time 5-minute aggregation remain outside G2-D.
