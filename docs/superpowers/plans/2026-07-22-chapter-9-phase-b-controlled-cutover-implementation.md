# 第 9 章 Phase B 受控切流实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 Java DataStream 数据质量作业切换为正式入口，让 Doris 与 Iceberg 分别消费 `user_behavior_clean`，并完成可验证、可回滚的端到端迁移。

**Architecture:** 单 TaskManager 扩为 4 slots，DataStream、Doris SQL、Iceberg SQL 各使用 1 个 slot。切流脚本先确认流量已暂停并记录 Kafka offset，再通过 Stop-with-Savepoint 将影子状态交接给 production 作业；两个 SQL 下游使用不同 Consumer Group，防止 Kafka 将数据在下游之间错误分摊。

**Tech Stack:** PowerShell 5、Docker Compose、Flink 1.19.2、Java 17 DataStream API、Flink SQL、Kafka KRaft、Doris 2.1.9、Iceberg 1.6.1、Hive Metastore、MinIO、Trino 458、Python `unittest`。

## Global Constraints

- 执行实现前使用 `superpowers:using-git-worktrees`；复用 `.worktrees/chapter-9-datastream-quality`，先将其分支快进到当前 `main`。
- 运行 Compose 的仓库根目录必须与 JobManager、TaskManager 的 `/workspace` 宿主机挂载一致，否则立即失败。
- 只允许使用 `--no-deps --force-recreate flink-taskmanager` 扩容 TaskManager；不得级联重建 JobManager、Kafka、Doris、MinIO 或 Hive Metastore。
- `FLINK_TASKMANAGER_SLOTS=4`，TaskManager 数量仍为 1。
- DataStream production 使用 `chapter9-quality-production`、`chapter9-production` 和 `user_behavior_clean`。
- Doris clean Group 与 Iceberg clean Group 必须不同；不得让两个独立 Sink 共享 Kafka Consumer Group。
- 原始、影子、clean、DLQ、late Topic 以及 Checkpoint、Savepoint 均不得自动删除或清空。
- 正式作业必须从影子 Savepoint 恢复；不得使用 `--allowNonRestoredState`。
- PowerShell 运维脚本仅输出 ASCII，避免 Windows PowerShell 5 对 UTF-8 无 BOM 中文脚本的解析问题。
- 保留 `.superpowers/sdd/task-1-report.md` 的用户修改，不暂存、不覆盖、不回退。

---

## 文件结构

| 文件 | 职责 |
| --- | --- |
| `tests/test_chapter_9_phase_b_artifacts.py` | Phase B 静态契约与 PowerShell 纯函数测试 |
| `infra/.env.example` | 4 slots、正式 Topic、Group、事务前缀配置 |
| `jobs/sql/13_source_user_behavior_clean_doris.sql` | Doris 独立 clean Consumer Group Source |
| `jobs/sql/14_source_user_behavior_clean_iceberg.sql` | Iceberg 独立 clean Consumer Group Source |
| `jobs/sql/15_source_user_behavior_raw_rollback.sql.template` | 按 cutover offset 渲染的 raw 回滚 Source |
| `scripts/resize_chapter_9_flink_slots.ps1` | 安全重建单个 TaskManager 并验证恢复 |
| `scripts/run_chapter_9_production_cutover.ps1` | 记录边界、Savepoint 状态交接、提交正式三作业 |
| `scripts/verify_chapter_9_production.ps1` | 质量分流、Doris、Iceberg、Trino、API 联合验收 |
| `scripts/rollback_chapter_9_production.ps1` | 按 manifest 停止正式链路并恢复 raw SQL 下游 |
| `docs/chapter-9-datastream-quality-runbook.md` | Phase B 命令、证据和真实排障记录 |
| `docs/superpowers/specs/2026-07-22-chapter-9-java-datastream-data-quality-design.md` | 第 9 章最终状态与验收结论 |
| `docs/superpowers/plans/2026-07-22-chapter-9-java-datastream-data-quality-implementation.md` | 勾选 Phase B 最终门禁 |

---

### Task 1: 固化 Phase B 配置与 Source 消费契约

**Files:**
- Create: `tests/test_chapter_9_phase_b_artifacts.py`
- Modify: `infra/.env.example`
- Create: `jobs/sql/13_source_user_behavior_clean_doris.sql`
- Create: `jobs/sql/14_source_user_behavior_clean_iceberg.sql`
- Create: `jobs/sql/15_source_user_behavior_raw_rollback.sql.template`

**Interfaces:**
- Produces: `CHAPTER9_CLEAN_TOPIC`、`CHAPTER9_PRODUCTION_CONSUMER_GROUP`、`CHAPTER9_PRODUCTION_TRANSACTION_PREFIX`。
- Produces: SQL 表名 `user_behavior_source`，供现有 `05_pv_uv_to_doris.sql` 与 `07_sink_user_behavior_to_iceberg.sql` 直接复用。
- Produces: 回滚模板占位符 `__ROLLBACK_GROUP_ID__` 与 `__SPECIFIC_OFFSETS__`。

- [x] **Step 1: 写失败的配置与 SQL 契约测试**

```python
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent


class Chapter9PhaseBArtifactsTest(unittest.TestCase):
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
```

- [x] **Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_chapter_9_phase_b_artifacts -v`

Expected: FAIL，提示 Phase B SQL 文件不存在或 `FLINK_TASKMANAGER_SLOTS=4` 缺失。

- [x] **Step 3: 添加最小配置并创建三个 Source 文件**

两个 clean Source 均保持现有八字段 Schema 和 JSON 格式，但分别使用 `chapter9-doris-clean-v1` 与 `chapter9-iceberg-clean-v1`；startup mode 使用 `earliest-offset`，因为正式 clean Topic 在切流前不被下游消费。

回滚模板使用：

```sql
'topic' = 'user_behavior_events',
'properties.group.id' = '__ROLLBACK_GROUP_ID__',
'scan.startup.mode' = 'specific-offsets',
'scan.startup.specific-offsets' = '__SPECIFIC_OFFSETS__',
'format' = 'json'
```

- [x] **Step 4: 运行配置测试与仓库基线**

Run: `python -m unittest tests.test_chapter_9_phase_b_artifacts -v`

Expected: PASS。

Run: `python -m unittest discover -s tests -v`

Expected: 现有 110 项加新增测试全部 PASS。

- [x] **Step 5: 提交**

```powershell
git add -- infra/.env.example jobs/sql/13_source_user_behavior_clean_doris.sql jobs/sql/14_source_user_behavior_clean_iceberg.sql jobs/sql/15_source_user_behavior_raw_rollback.sql.template tests/test_chapter_9_phase_b_artifacts.py
git commit -m "feat: define chapter 9 production sources"
```

---

### Task 2: 安全扩容 TaskManager 到 4 slots

**Files:**
- Create: `scripts/resize_chapter_9_flink_slots.ps1`
- Modify: `tests/test_chapter_9_phase_b_artifacts.py`

**Interfaces:**
- Produces: `Get-WorkspaceMountSource([string]$Container) -> string`。
- Produces: `Assert-FlinkCapacity([object]$Overview) -> void`。
- Consumes: `infra/.env.example` 中的 `FLINK_TASKMANAGER_SLOTS=4`。

- [x] **Step 1: 写失败的安全边界测试**

在 Python 测试中断言脚本包含以下标记：

```python
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
```

- [x] **Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_chapter_9_phase_b_artifacts.Chapter9PhaseBArtifactsTest.test_resize_script_recreates_only_taskmanager_and_checks_recovery -v`

Expected: FAIL，提示扩容脚本不存在。

- [x] **Step 3: 实现扩容脚本**

脚本按以下顺序执行：

1. 解析当前仓库根目录绝对路径。
2. 从 `docker inspect` 的 Mounts 中读取 JobManager 与 TaskManager 的 `/workspace` Source。
3. 要求两个 Source 与当前根目录完全一致；不一致时输出三个路径并退出。
4. 记录影子作业 ID 与成功 Checkpoint 数。
5. 执行唯一允许的重建命令：

```powershell
docker compose --env-file $envFile -f $compose --profile flink up -d `
    --no-deps --force-recreate flink-taskmanager
```

6. 轮询 `/overview`，要求 `taskmanagers=1`、`slots-total=4`。
7. 轮询原 Job ID，要求恢复为 `RUNNING` 且成功 Checkpoint 数增加。

脚本增加 `-FunctionsOnly`，便于加载纯函数测试，不执行 Docker 命令。

- [x] **Step 4: 测试 PowerShell 语法和纯函数**

Run:

```powershell
$tokens=$null; $errors=$null
[System.Management.Automation.Language.Parser]::ParseFile(
  (Resolve-Path 'scripts/resize_chapter_9_flink_slots.ps1'),
  [ref]$tokens,
  [ref]$errors
)
if ($errors) { throw $errors[0] }
. ./scripts/resize_chapter_9_flink_slots.ps1 -FunctionsOnly
Assert-FlinkCapacity ([pscustomobject]@{ taskmanagers=1; 'slots-total'=4 })
```

Expected: 无语法错误、无异常。

- [x] **Step 5: 运行扩容并记录证据**

Run: `./scripts/resize_chapter_9_flink_slots.ps1`

Expected: 同一影子 Job ID 恢复 `RUNNING`，`slots-total=4`，出现新的成功 Checkpoint。

- [x] **Step 6: 提交**

```powershell
git add -- scripts/resize_chapter_9_flink_slots.ps1 tests/test_chapter_9_phase_b_artifacts.py
git commit -m "ops: resize chapter 9 flink capacity"
```

---

### Task 3: 实现受控 Savepoint 切流与正式 SQL 提交

**Files:**
- Create: `scripts/run_chapter_9_production_cutover.ps1`
- Modify: `tests/test_chapter_9_phase_b_artifacts.py`

**Interfaces:**
- Produces: `tmp/chapter-9/cutover-manifest.json`，字段为 `cutover_id`、`created_at`、`raw_offsets`、`shadow_job_id`、`savepoint_path`、`production_job_id`、`doris_job_id`、`iceberg_job_id`。
- Produces: 三个作业名 `chapter-9-datastream-quality-production`、`chapter-9-doris-clean`、`chapter-9-iceberg-clean`。
- Consumes: Task 1 的两个 clean Source、现有 Doris/Iceberg Sink SQL 和 DataStream Fat JAR。

- [x] **Step 1: 写失败的切流脚本契约测试**

```python
def test_cutover_requires_traffic_gate_savepoint_manifest_and_three_jobs(self):
    text = (ROOT / "scripts/run_chapter_9_production_cutover.ps1").read_text(encoding="utf-8")
    for marker in (
        "[switch]$TrafficPaused",
        "cutover-manifest.json",
        "kafka-get-offsets",
        "kafka-consumer-groups",
        "--savepointPath",
        " -s $savepointPath",
        "--mode production",
        "--consumer-group chapter9-quality-production",
        "--transaction-prefix chapter9-production",
        "13_source_user_behavior_clean_doris.sql",
        "14_source_user_behavior_clean_iceberg.sql",
        "chapter-9-doris-clean",
        "chapter-9-iceberg-clean",
    ):
        self.assertIn(marker, text)
    self.assertNotIn("--allowNonRestoredState", text)
    self.assertNotIn("kafka-topics --delete", text)
```

- [x] **Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_chapter_9_phase_b_artifacts.Chapter9PhaseBArtifactsTest.test_cutover_requires_traffic_gate_savepoint_manifest_and_three_jobs -v`

Expected: FAIL，提示切流脚本不存在。

- [x] **Step 3: 实现 fail-fast 预检和 offset manifest**

脚本必须要求显式 `-TrafficPaused`。预检包括：Docker 服务可用、`slots-total=4`、只有一个影子 DataStream 作业、最近 Checkpoint 成功、正式作业名尚不存在、`user_behavior_clean` 通过 `--if-not-exists` 创建。

使用 `kafka-get-offsets --topic user_behavior_events` 记录每个分区的 log-end offset；使用 `kafka-consumer-groups --describe --group chapter9-quality-shadow` 轮询总 lag 为 0。将 offset 以 Kafka SQL 格式保存，例如 `partition:0,offset:42`。

- [x] **Step 4: 实现 Savepoint 状态交接**

执行：

```powershell
docker exec $jobManager /opt/flink/bin/flink stop `
  --savepointPath file:///workspace/tmp/savepoints/chapter-9 $shadowJobId

docker exec $jobManager /opt/flink/bin/flink run -d -s $savepointPath `
  -c com.ecommerce.quality.DataQualityJob /tmp/datastream-quality-1.0.0.jar `
  --bootstrap-servers kafka:29092 `
  --input-topic user_behavior_events `
  --mode production `
  --consumer-group chapter9-quality-production `
  --checkpoint-uri file:///workspace/tmp/checkpoints/chapter-9-production `
  --transaction-prefix chapter9-production `
  --job-version chapter-9-v1
```

要求解析新 Job ID、等待 `RUNNING` 并等待首个成功 Checkpoint。任何恢复错误立即退出，不提交 SQL 下游。

- [x] **Step 5: 分别提交 Doris 与 Iceberg SQL 作业**

生成两个 UTF-8 无 BOM 临时 SQL：

```text
tmp/chapter-9/doris-clean.sql
  SET pipeline.name + 13_source + 04_sink + 05_insert

tmp/chapter-9/iceberg-clean.sql
  SET checkpoint + SET pipeline.name + 14_source + 06_catalog + 07_insert
```

通过 `ecom-flink-sql-client` 提交，并从 Flink REST 按精确作业名获取 Job ID。要求三个作业均为 `RUNNING` 后，原子写入 `cutover-manifest.json`；先写 `.partial`，再使用 `Move-Item -LiteralPath` 替换。

- [x] **Step 6: 运行静态测试和语法测试**

Run: `python -m unittest tests.test_chapter_9_phase_b_artifacts -v`

Expected: PASS。

Run: 使用 PowerShell Parser 解析 `scripts/run_chapter_9_production_cutover.ps1`。

Expected: `$errors.Count = 0`。

- [x] **Step 7: 提交**

```powershell
git add -- scripts/run_chapter_9_production_cutover.ps1 tests/test_chapter_9_phase_b_artifacts.py
git commit -m "feat: orchestrate chapter 9 production cutover"
```

---

### Task 4: 实现正式链路端到端严格验收

**Files:**
- Create: `scripts/verify_chapter_9_production.ps1`
- Modify: `tests/test_chapter_9_phase_b_artifacts.py`

**Interfaces:**
- Consumes: `tmp/chapter-9/cutover-manifest.json` 和三个正式 Job ID。
- Produces: 唯一批次 ID `chapter9-production-<guid>` 以及 raw/clean/DLQ/late、Doris、Iceberg、Trino、API 证据。

- [x] **Step 1: 写失败的验收契约测试**

```python
def test_production_verifier_checks_quality_and_all_downstreams(self):
    text = (ROOT / "scripts/verify_chapter_9_production.ps1").read_text(encoding="utf-8")
    for marker in (
        "cutover-manifest.json",
        "user_behavior_clean",
        "user_behavior_dlq",
        "user_behavior_late",
        "raw = clean + dlq + late",
        "DUPLICATE_EVENT",
        "analytics.realtime_metrics",
        "lakehouse.analytics.user_behavior_detail",
        "/analysis/realtime",
        "/checkpoints",
        "slots-total",
    ):
        self.assertIn(marker, text)
```

- [x] **Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_chapter_9_phase_b_artifacts.Chapter9PhaseBArtifactsTest.test_production_verifier_checks_quality_and_all_downstreams -v`

Expected: FAIL，提示验收脚本不存在。

- [x] **Step 3: 实现八条事件质量矩阵**

复用影子验收的 8 条矩阵，但使用 `chapter9-production-<guid>` 嵌入 `event_id/user_id/product_id`。两条 clean 事件使用不同 user ID，使预期为：

```text
raw=8
clean=2
dlq=5
late=1
duplicate_clean=1
```

读取 Kafka 输出时使用 `isolation.level=read_committed`，等待至少一个新 production Checkpoint 后再断言结果。

- [x] **Step 4: 验证三个 Flink 作业和 Kafka Group**

从 manifest 读取 Job ID，要求三个作业均为 `RUNNING`；Flink overview 要求 `taskmanagers=1`、`slots-total=4`、`jobs-running=3`。production Group 的 CLI/readable lag 为 `0/0`；Doris 与 Iceberg clean Group 的 CLI/readable lag 为 `1/0`，未读 offset 均为 `COMMIT` control record。

- [x] **Step 5: 验证 Doris、Iceberg、Trino 与 API**

轮询 Doris `analytics.realtime_metrics`，要求本次 clean 作业状态最终产生 `pv=2`、`uv=2`。通过 Trino 对两个精确 event ID 查询，要求 Iceberg `event_count=2`、`distinct_event_id=2`、`distinct_user_id=2`，且五条 DLQ 与一条 late 事件均不存在于明细表。

调用 `POST http://localhost:8000/analysis/realtime`，要求 HTTP 成功、`evidence` 非空，并且证据中的 Doris/Trino 查询时间晚于本次切流批次开始时间。这里验证 API 可用和证据落地，不要求模型模式，规则模式或自动降级均可。

- [x] **Step 6: 运行静态、语法和纯函数测试**

Run: `python -m unittest tests.test_chapter_9_phase_b_artifacts -v`

Expected: PASS。

Run: PowerShell Parser 解析验收脚本，并以 `-FunctionsOnly` 加载。

Expected: 无语法错误且加载过程不发送 Kafka 数据。

- [x] **Step 7: 提交**

```powershell
git add -- scripts/verify_chapter_9_production.ps1 tests/test_chapter_9_phase_b_artifacts.py
git commit -m "test: verify chapter 9 production pipeline"
```

---

### Task 5: 实现 manifest 驱动的回滚入口

**Files:**
- Create: `scripts/rollback_chapter_9_production.ps1`
- Modify: `tests/test_chapter_9_phase_b_artifacts.py`

**Interfaces:**
- Consumes: `tmp/chapter-9/cutover-manifest.json` 中的 raw partition offsets 和正式 Job ID。
- Produces: `tmp/chapter-9/rollback-doris-raw.sql`、`tmp/chapter-9/rollback-iceberg-raw.sql` 与两个 raw 回滚 SQL 作业。

- [x] **Step 1: 写失败的回滚安全测试**

```python
def test_rollback_is_manifest_driven_and_non_destructive(self):
    text = (ROOT / "scripts/rollback_chapter_9_production.ps1").read_text(encoding="utf-8")
    for marker in (
        "[switch]$TrafficPaused",
        "[switch]$DryRun",
        "cutover-manifest.json",
        "15_source_user_behavior_raw_rollback.sql.template",
        "__SPECIFIC_OFFSETS__",
        "chapter9-doris-raw-rollback",
        "chapter9-iceberg-raw-rollback",
        "--savepointPath",
    ):
        self.assertIn(marker, text)
    for forbidden in ("kafka-topics --delete", "docker compose down", "DROP TABLE", "Remove-Item"):
        self.assertNotIn(forbidden, text)
```

- [x] **Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_chapter_9_phase_b_artifacts.Chapter9PhaseBArtifactsTest.test_rollback_is_manifest_driven_and_non_destructive -v`

Expected: FAIL，提示回滚脚本不存在。

- [x] **Step 3: 实现 dry-run 与模板渲染**

脚本要求 `-TrafficPaused`，读取 manifest 并校验全部字段。将模板分别渲染为两个 SQL 文件，Group 为 `chapter9-doris-raw-rollback-<cutover_id>` 和 `chapter9-iceberg-raw-rollback-<cutover_id>`，specific offsets 使用 manifest 的原始边界。

`-DryRun` 只验证 manifest、渲染 SQL 并输出将停止和启动的精确 Job ID/作业名，不调用 Flink stop/cancel，也不提交 SQL。

- [x] **Step 4: 实现真实回滚顺序**

非 DryRun 模式：

1. 再次确认三个正式 Job ID 与当前运行作业一致。
2. production DataStream 使用 Stop-with-Savepoint 保留状态。
3. 精确停止 Doris clean 和 Iceberg clean 作业。
4. 分别提交 raw Doris 与 raw Iceberg SQL 作业。
5. 要求两个回滚作业为 `RUNNING`，输出“可以恢复生成器”，但脚本本身不启动生成器。

脚本不自动处理已经写入 Doris/Iceberg 的切流批次；输出批次 ID 和两个 clean event ID，供显式补偿或审计。

- [x] **Step 5: 验证 dry-run**

Run: `./scripts/rollback_chapter_9_production.ps1 -TrafficPaused -DryRun`

Expected: 成功渲染 specific-offset Source，输出精确回滚计划，当前三个正式作业状态不变。

- [x] **Step 6: 提交**

```powershell
git add -- scripts/rollback_chapter_9_production.ps1 tests/test_chapter_9_phase_b_artifacts.py
git commit -m "ops: add chapter 9 manifest rollback"
```

---

### Task 6: 执行正式切流、完成全量验证并收尾文档

**Files:**
- Modify: `docs/chapter-9-datastream-quality-runbook.md`
- Modify: `docs/superpowers/specs/2026-07-22-chapter-9-java-datastream-data-quality-design.md`
- Modify: `docs/superpowers/plans/2026-07-22-chapter-9-java-datastream-data-quality-implementation.md`
- Modify: `docs/superpowers/specs/2026-07-22-chapter-9-phase-b-controlled-cutover-design.md`
- Modify: `docs/superpowers/plans/2026-07-22-chapter-9-phase-b-controlled-cutover-implementation.md`

**Interfaces:**
- Consumes: Tasks 1-5 的脚本、SQL 和 manifest。
- Produces: Phase B 最终 Job ID、Savepoint、offset、Checkpoint、对账和下游回归证据。

- [x] **Step 1: 运行全部自动化测试**

Run: `python -m unittest discover -s tests -v`

实际结果：全量 Python 回归最终为 165/165 PASS；Java 17 Maven JUnit 为 15/15 PASS。

Run:

```powershell
docker run --rm -v "${PWD}:/workspace" -w /workspace/jobs/datastream-quality `
  maven:3.9.9-eclipse-temurin-17 mvn -q test
```

实际结果：15 项 JUnit 全部 PASS。

- [x] **Step 2: 扩容并执行正式切流**

确认没有正在运行的 `generators/run_generator.py` 进程后执行：

```powershell
./scripts/resize_chapter_9_flink_slots.ps1
./scripts/run_chapter_9_production_cutover.ps1 -TrafficPaused
```

实际结果：slots 由 2 扩为 4；影子作业通过 Savepoint 停止，production、Doris clean、Iceberg clean
三作业均为 `RUNNING`。正式切流使用 manifest raw offset `partition:0,offset:212`，保存影子
Job `6f6e24deea18e22722bfd5e0a83895e4` 的 Savepoint 后，三个最终 Job ID 分别为
`0d8edd967461402a66e9672d2335ca6d`、`bf10b31978af0ae53446535c41120870`、
`ce7ec8a8d04e70f45f6c7806ed1ede28`。

- [x] **Step 3: 执行正式验收与回滚 dry-run**

Run:

```powershell
./scripts/rollback_chapter_9_production.ps1 -TrafficPaused -DryRun
```

原始单次验收命令按真实恢复流程调整：逻辑 run `ab626...` 先经历 7 条发送的 watermark
失败、late-only resume 的 API 超时，再由 `read_only_finalize` 以零发送完成最终 JSON；未声称首次一次成功，
也未重发前 7 条。最终为 `raw=8 clean=2 dlq=5 late=1`、Doris `2/2`、Trino `817`、API
historical `817`。rollback dry-run 使用 offset `212` 渲染两个 raw SQL，前后运行作业身份不变，
未调用 stop/cancel/submit。

- [x] **Step 4: 记录真实证据和排障，不伪造结果**

在 runbook 中记录实际：

- 扩容前后 slots；
- 影子 Job ID、Savepoint 路径、production Job ID；
- Doris 与 Iceberg SQL Job ID；
- cutover offset manifest；
- Checkpoint 数；
- Kafka 质量对账；
- Doris、Trino、API 结果；
- 回滚 dry-run 输出；
- 执行过程中真实发生的错误、根因和修复。

将两个设计文档状态更新为“Phase B 正式切流已完成”，勾选原第 9 章计划的 Phase B 门禁和本计划所有已完成步骤。
真实排障已记录：空 connector 挂载及 SHA-256 校验 JAR、旧 worktree MinIO 无损恢复与 Hive metadata
指针修复、Flink 历史同名 job、Doris earliest 重放后 specific offset 4、Watermark REST 数组和
idle `Int64.MinValue`、Kafka `COMMIT` control offset、Trino OOM/旧 Chapter 8 挂载、API offset-free
时间按 UTC，以及 rollback I1-I4 修复与复审。

- [x] **Step 5: 最终回归与状态检查**

Run:

```powershell
python -m unittest discover -s tests -v
git diff --check
git status --short
```

实际结果：165/165 Python 测试、`git diff --check` 通过；变更范围审计只允许本任务五份文档，另有
`.superpowers/sdd/chapter9-phase-b-task-6-report.md` 作为忽略文件，不进入提交。

- [x] **Step 6: 请求代码审查并修复 P0/P1/P2 问题**

重点审查：Compose 重建范围、挂载路径一致性、Consumer Group 隔离、Savepoint UID 兼容、offset manifest、回滚幂等性、SQL 重复写入和验收证据真实性。
实际结果：Task 3/4/5 review 与 rereview 已完成；Task 5 I1-I4 已修复并以 0 Critical/Important
复审通过。合并前全分支审查进一步补齐 cutover partial、rollback progress/`-Resume`、verifier
durable stage evidence 和历史同名 Job 过滤；最终定点复审为 P0/P1/P2 全部 0。

- [x] **Step 7: 提交五份文档**

```powershell
git add -- docs/chapter-9-datastream-quality-runbook.md docs/superpowers/specs/2026-07-22-chapter-9-java-datastream-data-quality-design.md docs/superpowers/specs/2026-07-22-chapter-9-phase-b-controlled-cutover-design.md docs/superpowers/plans/2026-07-22-chapter-9-java-datastream-data-quality-implementation.md docs/superpowers/plans/2026-07-22-chapter-9-phase-b-controlled-cutover-implementation.md
git commit -m "docs: close chapter 9 production cutover"
```

本 Task 只提交五份文档，不在本工作流中 merge 或 push。提交后仍不得删除
`.worktrees/chapter-9-datastream-quality`，因为当前 Flink 容器仍将其挂载为 `/workspace`。
`.superpowers/sdd/chapter9-phase-b-task-6-report.md` 仅作忽略的本地收尾记录。

---

## 最终验收门禁

- [x] 单 TaskManager 提供 4 slots，未级联重建其他有状态服务。
- [x] 影子作业通过 Savepoint 成功交接到 production，去重状态未被绕过或丢弃。
- [x] DataStream、Doris SQL、Iceberg SQL 三作业同时稳定运行并产生成功 Checkpoint。
- [x] Doris 与 Iceberg 使用不同 Kafka Consumer Group。
- [x] 正式质量矩阵满足 `raw=8 clean=2 dlq=5 late=1`。
- [x] Doris、Iceberg、Trino 和第 8 章 API 回归通过。
- [x] 回滚脚本能从 manifest 的 raw offset 渲染并 dry-run，且不删除数据或状态。
- [x] Python、JUnit、PowerShell 语法和最终仓库检查全部通过。

**执行边界实录（2026-07-22）：本计划所述 Phase B 已完成。原定“一次 verify 后直接
dry-run”的命令因真实 watermark 数组、API 超时和 idle `Int64.MinValue` 证据，按恢复流程调整为
失败证据保留、同逻辑 run 的 late-only resume、最终 `read_only_finalize` 零发送；最终 JSON 通过且
回滚 dry-run 无 mutation。当前 Flink 仍挂载此 worktree，未完成挂载迁移前不得删除。**
