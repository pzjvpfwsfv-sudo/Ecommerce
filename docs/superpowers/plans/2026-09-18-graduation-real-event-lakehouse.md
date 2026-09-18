# G2-C 真实事件湖仓落地实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 G2-B 的真实 clean/late 有效事件统一写入独立 Iceberg 事实表，并由 Trino 完成可重复的计数、唯一性和字段质量对账。

**Architecture:** 两个严格 Kafka JSON source 规范化成相同列后经一个 `UNION ALL` 视图进入单一 Iceberg sink。目标表保留 17 个原始字段，增加类型化查询列、质量路由、Kafka 落地坐标和迟到诊断；DLQ 不参与落湖。

**Tech Stack:** Flink SQL 1.19.2、Kafka connector 3.3.0-1.19、Iceberg 1.6.1、Hive Metastore、MinIO、Trino 458、PowerShell 7、Python 3.12 `unittest`。

**Spec:** `docs/superpowers/specs/2026-09-18-graduation-real-event-lakehouse-design.md`

## Global Constraints

- 只在 `codex/chapter-10-controlled-tools` 工作树修改；不得纳入主工作区未提交文件。
- 新表必须位于 `lakehouse.analytics`，名称只能是 `real_behavior_detail_v1` 或带已校验 run-id 后缀的隔离表；旧 `user_behavior_detail` 不变。
- clean/late Topic 必须同属一个 G2-B run-id；不得读取 DLQ、旧 `user_behavior_*` 或自动创建 Topic。
- 保留 17 个原始字段；金额新增 `DECIMAL(20,2)` 查询列但原价格字符串不丢失，业务 ID 继续使用字符串。
- 只允许一个 Iceberg `INSERT INTO`；不能用两个独立 writer 同时写同一张表。
- 动态验收只使用真实数据；人为改变到达顺序可以验证 late，但不得改写业务字段。
- 临时文件、测试目录和依赖缓存位于 D 盘；不得停止 Docker Desktop、清空旧卷或删除正式表。
- 本轮不修改 G2-B Java 业务逻辑，不实现 Doris 指标、API、前端或 RAG。

---

### Task 1: SQL contract and normalized Iceberg sink

**Files:**
- Create: `tests/test_g2c_real_event_lakehouse.py`
- Create: `jobs/sql/16_real_behavior_to_iceberg.sql.template`
- Create: `jobs/sql/17_trino_verify_real_behavior.sql.template`

**Interfaces:**
- Consumes: G2-B clean top-level 17-field JSON and late envelope JSON.
- Produces: placeholders `__RUN_ID__`, `__CLEAN_TOPIC__`, `__LATE_TOPIC__`, `__TABLE_NAME__`; one target table and one `INSERT INTO`; Trino verification rows with fixed aliases.

- [ ] **Step 1: Write failing artifact tests**

Create `tests/test_g2c_real_event_lakehouse.py` with assertions that both templates exist and that the Flink template contains all 17 source fields, Kafka metadata, strict JSON options, `read_committed`, `UNION ALL`, one `INSERT INTO`, `PARTITIONED BY (event_date)`, no DLQ source, and exact placeholder names. Assert the Trino template checks total/routes/distinct IDs/derived columns/late diagnostics.

```python
def test_flink_template_uses_one_sink_for_clean_and_late(self):
    sql = FLINK_TEMPLATE.read_text(encoding="utf-8")
    self.assertEqual(1, sql.upper().count("INSERT INTO"))
    self.assertIn("UNION ALL", sql.upper())
    self.assertNotIn("real_behavior_dlq", sql)
```

- [ ] **Step 2: Run the test and confirm RED**

Run: `$env:TEMP='D:\EcommerceDev\temp'; $env:TMP=$env:TEMP; python -m unittest tests.test_g2c_real_event_lakehouse -v`

Expected: FAIL because `16_real_behavior_to_iceberg.sql.template` and `17_trino_verify_real_behavior.sql.template` do not exist.

- [ ] **Step 3: Implement the Flink SQL template**

Define strict sources and a single normalized sink. Use UTC and deterministic helpers:

```sql
SET 'table.local-time-zone' = 'UTC';
SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

CREATE TEMPORARY VIEW real_behavior_normalized AS
SELECT event_time,
       TO_TIMESTAMP_LTZ(
         UNIX_TIMESTAMP(REPLACE(SUBSTRING(event_time, 1, 19), 'T', ' '), 'yyyy-MM-dd HH:mm:ss') * 1000,
         3) AS event_ts,
       CAST(SUBSTRING(event_time, 1, 10) AS DATE) AS event_date,
       event_type, product_id, category_id, category_code, brand, price,
       CAST(price AS DECIMAL(20, 2)) AS price_decimal,
       user_id, user_session, schema_version, event_id, dataset_id, source_file,
       source_row_number, replay_schema_version, replay_id, replayed_at,
       'clean' AS quality_route,
       landing_topic, landing_partition, landing_offset, landing_timestamp,
       CAST(NULL AS STRING) AS origin_topic,
       CAST(NULL AS INT) AS origin_partition,
       CAST(NULL AS BIGINT) AS origin_offset,
       CAST(NULL AS STRING) AS quality_job_version,
       CAST(NULL AS STRING) AS quality_observed_at,
       CAST(NULL AS BIGINT) AS watermark_ms,
       CAST(NULL AS BIGINT) AS lateness_ms
FROM real_behavior_clean_source
UNION ALL
SELECT event.event_time,
       TO_TIMESTAMP_LTZ(
         UNIX_TIMESTAMP(REPLACE(SUBSTRING(event.event_time, 1, 19), 'T', ' '), 'yyyy-MM-dd HH:mm:ss') * 1000,
         3) AS event_ts,
       CAST(SUBSTRING(event.event_time, 1, 10) AS DATE) AS event_date,
       event.event_type, event.product_id, event.category_id, event.category_code,
       event.brand, event.price, CAST(event.price AS DECIMAL(20, 2)) AS price_decimal,
       event.user_id, event.user_session, event.schema_version, event.event_id,
       event.dataset_id, event.source_file, event.source_row_number,
       event.replay_schema_version, event.replay_id, event.replayed_at,
       'late' AS quality_route,
       landing_topic, landing_partition, landing_offset, landing_timestamp,
       source_topic, source_partition, source_offset, job_version, observed_at,
       `watermark` AS watermark_ms, lateness_ms
FROM real_behavior_late_source;

INSERT INTO lakehouse.analytics.__TABLE_NAME__
SELECT event_time, event_ts, event_date, event_type, product_id, category_id,
       category_code, brand, price, price_decimal, user_id, user_session,
       schema_version, event_id, dataset_id, source_file, source_row_number,
       replay_schema_version, replay_id, replayed_at, quality_route,
       landing_topic, landing_partition, landing_offset, landing_timestamp,
       origin_topic, origin_partition, origin_offset, quality_job_version,
       quality_observed_at, watermark_ms, lateness_ms
FROM real_behavior_normalized;
```

Both Kafka sources use `scan.startup.mode='earliest-offset'`, `properties.isolation.level='read_committed'`, `json.fail-on-missing-field='true'`, and `json.ignore-parse-errors='false'`. The target stores original and typed fields and is partitioned by `event_date`.

- [ ] **Step 4: Implement the Trino verification template**

Use fixed aliases so PowerShell can parse results without relying on column order from ad hoc queries:

```sql
SELECT COUNT(*) AS total_count,
       count_if(quality_route = 'clean') AS clean_count,
       count_if(quality_route = 'late') AS late_count,
       COUNT(DISTINCT event_id) AS distinct_event_count,
       count_if(quality_route NOT IN ('clean', 'late')) AS invalid_route_count
FROM lakehouse.analytics.__TABLE_NAME__;
```

Add this quality query so every violation is an explicit count:

```sql
SELECT count_if(event_ts IS NULL OR event_date IS NULL OR price_decimal IS NULL) AS invalid_derived_count,
       count_if(quality_route = 'clean' AND
         (origin_topic IS NOT NULL OR origin_partition IS NOT NULL OR origin_offset IS NOT NULL OR
          quality_job_version IS NOT NULL OR quality_observed_at IS NOT NULL OR
          watermark_ms IS NOT NULL OR lateness_ms IS NOT NULL)) AS invalid_clean_diagnostic_count,
       count_if(quality_route = 'late' AND
         (origin_topic IS NULL OR origin_partition IS NULL OR origin_offset IS NULL OR
          quality_job_version IS NULL OR quality_observed_at IS NULL OR
          watermark_ms IS NULL OR lateness_ms IS NULL OR lateness_ms < 0)) AS invalid_late_diagnostic_count
FROM lakehouse.analytics.__TABLE_NAME__;
```

- [ ] **Step 5: Run tests and confirm GREEN**

Run: `$env:TEMP='D:\EcommerceDev\temp'; $env:TMP=$env:TEMP; python -m unittest tests.test_g2c_real_event_lakehouse -v`

Expected: SQL contract tests PASS.

- [ ] **Step 6: Commit**

```powershell
git add tests/test_g2c_real_event_lakehouse.py jobs/sql/16_real_behavior_to_iceberg.sql.template jobs/sql/17_trino_verify_real_behavior.sql.template
git commit -m "feat: define G2-C real event lakehouse contract"
```

### Task 2: Safe SQL rendering and submission

**Files:**
- Modify: `tests/test_g2c_real_event_lakehouse.py`
- Create: `scripts/run_g2c_real_event_lakehouse.ps1`

**Interfaces:**
- Consumes: `-RunId`, optional exact matching `-CleanTopic/-LateTopic`, and optional safe `-TableName`.
- Produces: `Get-G2cDeployment`, `Render-G2cSql`, a D-drive rendered SQL file, and a submitted Flink SQL job named `graduation-g2c-<run-id>`.

- [ ] **Step 1: Write failing PowerShell behavior tests**

From Python, dot-source the script with `-FunctionsOnly` and verify:

```powershell
$d = Get-G2cDeployment -RunId 'g2c-20260918a' -CleanTopic '' -LateTopic '' -TableName ''
$sql = Render-G2cSql -Template (Get-Content -Raw 'jobs/sql/16_real_behavior_to_iceberg.sql.template') -Deployment $d
$d | ConvertTo-Json -Compress
```

Expected derived names are `real_behavior_clean_v1_g2c-20260918a`, `real_behavior_late_v1_g2c-20260918a`, table `real_behavior_detail_v1_g2c_20260918a`, and group/job `graduation-g2c-g2c-20260918a`. Add rejection cases for path separators, quotes, old namespaces, topic/run mismatch, unsafe table names and unreplaced placeholders.

- [ ] **Step 2: Run the targeted test and confirm RED**

Run: `$env:TEMP='D:\EcommerceDev\temp'; $env:TMP=$env:TEMP; python -m unittest tests.test_g2c_real_event_lakehouse.G2cRunnerTests -v`

Expected: FAIL because the runner and functions do not exist.

- [ ] **Step 3: Implement validation and rendering**

Create a function-only guard and use literal replacement, never `Invoke-Expression`:

```powershell
function Render-G2cSql {
    param([string]$Template, $Deployment)
    $rendered = $Template.Replace('__RUN_ID__', $Deployment.RunId)
    $rendered = $rendered.Replace('__CLEAN_TOPIC__', $Deployment.CleanTopic)
    $rendered = $rendered.Replace('__LATE_TOPIC__', $Deployment.LateTopic)
    $rendered = $rendered.Replace('__TABLE_NAME__', $Deployment.TableName)
    if ($rendered -match '__[A-Z0-9_]+__') { throw 'Unresolved G2-C SQL placeholder.' }
    return $rendered
}

if ($FunctionsOnly) { return }
```

Validate run-id against `[a-z0-9][a-z0-9_-]{0,31}`; topics must exactly equal the derived clean/late names; table must be the formal name or the normalized run-specific name.

- [ ] **Step 4: Implement minimal dependency startup and SQL submission**

Check Docker, existing connector JARs and source Topic existence before rendering. Start only Kafka/Flink/MinIO/Hive Metastore, wait for ports, write rendered SQL under `tmp/graduation/g2c/<run-id>/`, submit through the existing SQL client and fail on nonzero exit or `[ERROR]`. Do not create source Topic or delete tables.

- [ ] **Step 5: Run tests and confirm GREEN**

Run: `$env:TEMP='D:\EcommerceDev\temp'; $env:TMP=$env:TEMP; python -m unittest tests.test_g2c_real_event_lakehouse -v`

Expected: artifact and runner tests PASS without starting Docker.

- [ ] **Step 6: Commit**

```powershell
git add tests/test_g2c_real_event_lakehouse.py scripts/run_g2c_real_event_lakehouse.ps1
git commit -m "feat: add safe G2-C lakehouse runner"
```

### Task 3: Trino acceptance verifier

**Files:**
- Modify: `tests/test_g2c_real_event_lakehouse.py`
- Create: `scripts/verify_g2c_real_event_lakehouse.ps1`

**Interfaces:**
- Consumes: `-RunId`, `-ExpectedCleanCount`, `-ExpectedLateCount`, optional table name; the SQL template and an already submitted G2-C job.
- Produces: nonzero failure on timeout, duplicate IDs, bad routes, invalid derived fields or bad diagnostics; success JSON with table/counts/snapshot evidence.

- [ ] **Step 1: Write failing verifier tests**

Dot-source with `-FunctionsOnly`. Test `ConvertFrom-G2cCsvRow`, `Assert-G2cSummary`, `Assert-G2cQuality`, and render-time table validation using synthetic command output. Include negative cases for `total != clean + late`, `total != distinct`, unexpected route, nonzero quality violations and wrong expected counts.

- [ ] **Step 2: Run the verifier test and confirm RED**

Run: `$env:TEMP='D:\EcommerceDev\temp'; $env:TMP=$env:TEMP; python -m unittest tests.test_g2c_real_event_lakehouse.G2cVerifierTests -v`

Expected: FAIL because verifier functions do not exist.

- [ ] **Step 3: Implement Trino polling and fail-closed assertions**

Start only Hive Metastore and Trino if needed, render `17_trino_verify_real_behavior.sql.template`, execute each statement using the Trino CLI with `CSV_HEADER_UNQUOTED`, and poll until expected rows are committed or timeout. Parse named columns and fail closed:

```powershell
if ($Summary.total_count -ne ($Summary.clean_count + $Summary.late_count)) { throw 'Route reconciliation failed.' }
if ($Summary.total_count -ne $Summary.distinct_event_count) { throw 'Duplicate event_id detected.' }
if ($Quality.Values | Where-Object { $_ -ne 0 }) { throw 'Lakehouse quality assertion failed.' }
```

Query Iceberg metadata table `"<table>$snapshots"` and require at least one snapshot. Emit a compact JSON report under ignored `tmp/graduation/g2c/<run-id>/`.

- [ ] **Step 4: Run tests and confirm GREEN**

Run: `$env:TEMP='D:\EcommerceDev\temp'; $env:TMP=$env:TEMP; python -m unittest tests.test_g2c_real_event_lakehouse -v`

Expected: all G2-C offline tests PASS.

- [ ] **Step 5: Commit**

```powershell
git add tests/test_g2c_real_event_lakehouse.py scripts/verify_g2c_real_event_lakehouse.ps1
git commit -m "feat: verify G2-C Iceberg facts through Trino"
```

### Task 4: Real-data dynamic acceptance and documentation

**Files:**
- Create: `docs/graduation/real-event-lakehouse-runbook.md`
- Modify: `README.md`
- Modify: `docs/graduation/data-readiness.md`
- Modify: `docs/superpowers/plans/2026-09-18-graduation-real-event-lakehouse.md`

**Interfaces:**
- Consumes: verified G2-B real topics, runner and verifier.
- Produces: real dynamic evidence, accurate runbook, updated project progress and explicit next stage G2-D.

- [ ] **Step 1: Run focused regressions**

Run G2-C Python tests, G2-B Java tests/package in the existing Docker Java 17 environment, and real-data Python tests. Record exact counts and elapsed times; do not rerun unrelated UI/Agent suites.

- [ ] **Step 2: Produce isolated real clean and late events**

Use a fresh G2-B run-id and actual normalized events from the verified Oct/Nov sample. Replay a small chronological baseline, then send two different valid real events more than ten seconds apart in newest-then-oldest arrival order so the second routes to late. Confirm `read_committed` clean/late counts and capture event IDs; do not edit payload business fields.

- [ ] **Step 3: Submit G2-C and verify through Trino**

Run `run_g2c_real_event_lakehouse.ps1` with that exact run-id, then call `verify_g2c_real_event_lakehouse.ps1` with observed clean/late counts. Require one running SQL job, completed checkpoint, Iceberg snapshot, exact route reconciliation, unique event IDs, zero quality violations and expected sample IDs returned by Trino.

- [ ] **Step 4: Document only measured results**

Write commands, resource names, source hashes, event IDs, counts, job/checkpoint/snapshot identity, elapsed time and remaining capacity boundary. Update README from “下一步落湖” to “G2-C 已完成，下一步 G2-D 指标与 API” only if dynamic acceptance really passes.

- [ ] **Step 5: Final verification and commit**

Run focused tests again, `git diff --check`, inspect ignored/untracked files, and confirm no real data or runtime report is staged. Commit documentation and plan checkboxes:

```powershell
git add README.md docs/graduation/data-readiness.md docs/graduation/real-event-lakehouse-runbook.md docs/superpowers/plans/2026-09-18-graduation-real-event-lakehouse.md
git commit -m "docs: record G2-C real lakehouse acceptance"
```

- [ ] **Step 6: Push and verify backup**

Push `codex/chapter-10-controlled-tools` using the current proxy only if needed, then compare `git rev-parse HEAD` with `git ls-remote origin refs/heads/codex/chapter-10-controlled-tools`. Do not merge `main`.

## Self-Review

- Spec coverage: table contract, clean/late merge, DLQ exclusion, parameter isolation, one sink, Trino reconciliation, real late event, local resource constraints and documentation each map to a task.
- Placeholder scan: template placeholders are intentional executable interface names; the plan contains no unfinished implementation placeholders.
- Type consistency: run-id derives both Topic names, consumer group, pipeline name and default table; SQL and verifier use the same target table and fixed result aliases.
- Scope: G2-C remains one independently testable subsystem. Doris metrics and versioned API begin only after this plan passes.

用户已选择在当前会话按整体计划继续，因此完成计划后使用 `superpowers:executing-plans` 直接实施，不再重复询问执行方式。
