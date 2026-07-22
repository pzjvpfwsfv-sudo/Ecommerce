# 第 9 章 Java DataStream 数据质量治理实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 先实现并验证 Java 17 + Flink 1.19.2 DataStream 影子数据质量作业，再完成受控 Phase B 切流，将正式 Doris/Iceberg 主链路切换到 clean、DLQ 和 late 数据契约。

**Architecture:** 新 Maven 模块把纯 Java 的解析校验、Flink 事件时间/状态处理和 Kafka 拓扑装配分离。作业以独立 Consumer Group 消费 `user_behavior_events`，使用 Side Output 输出拒绝与迟到事件，使用带 24 小时 TTL 的 `ValueState` 去重，并通过三个 EXACTLY_ONCE Kafka Sink 写入 shadow 或 production Topic；Phase B 完成后，Flink SQL 使用独立 group 消费 `user_behavior_clean`。

**Tech Stack:** Java 17、Apache Flink 1.19.2 DataStream API、Flink Kafka Connector 3.3.0-1.19、Jackson 2.17.2、Maven Shade Plugin、JUnit 5、Python unittest、PowerShell、Docker Compose、Kafka KRaft

## Global Constraints

- Phase A 实施阶段先不得修改现有 Flink SQL Source；Phase B 已在独立人工确认和受控门禁下完成正式切流，最终状态以本计划末尾的 Phase B 门禁为准。
- 作业使用 Java 17 和 Flink 1.19.2；宿主机 Maven 绑定 Java 8，因此编译测试统一在 `maven:3.9.9-eclipse-temurin-17` 容器中执行。
- 输入事件固定为八字段契约；未知字段忽略，标准输出只保留八个已知字段。
- `event_type` 只允许 `view/click/cart/purchase/order/pay`；`channel` 与 `device_type` 只校验非空和长度。
- 未来超过 5 分钟的事件拒绝；Watermark 最大乱序 10 秒、空闲分区 30 秒。
- `event_id` 去重状态 TTL 为 24 小时，使用 `OnCreateAndWrite`，重复读取不得延长寿命。
- clean、DLQ、late 三个 Kafka Sink 均使用 `DeliveryGuarantee.EXACTLY_ONCE` 和互不相同的事务前缀。
- Checkpoint 间隔 10 秒、超时 60 秒、最小间隔 5 秒、最大并发数 1；取消时保留外部 Checkpoint；固定延迟最多重启 3 次。
- 默认使用 HashMap State Backend；本章不引入 RocksDB、Flink HA、Schema Registry、CEP 或监控平台。
- 所有密钥、凭证、异常堆栈不得写入 DLQ、日志或仓库。
- Phase A 的影子和恢复证据必须保留；最终文档必须明确写出 Phase B 正式切流已完成，并保留原始 Topic、旧 Source、Checkpoint 和 Savepoint 回滚边界。

---

### Task 1: Maven 模块与事件契约

**Files:**
- Create: `jobs/datastream-quality/pom.xml`
- Create: `jobs/datastream-quality/src/main/java/com/ecommerce/quality/model/UserBehaviorEvent.java`
- Create: `jobs/datastream-quality/src/main/java/com/ecommerce/quality/model/RejectedEvent.java`
- Create: `jobs/datastream-quality/src/main/java/com/ecommerce/quality/model/LateEvent.java`
- Create: `jobs/datastream-quality/src/main/java/com/ecommerce/quality/model/ValidationResult.java`
- Create: `jobs/datastream-quality/src/test/java/com/ecommerce/quality/model/EventModelTest.java`
- Create: `tests/test_chapter_9_artifacts.py`

**Interfaces:**
- Produces: immutable-style Java beans with public no-argument constructors for Flink/Jackson, getters, setters, `equals`, and `hashCode`.
- Produces: `ValidationResult.valid(UserBehaviorEvent)` and `ValidationResult.invalid(String reasonCode, String reasonMessage)`.

- [x] **Step 1: Write failing artifact and model tests**

Add Python assertions for the Maven module, Java 17, Flink `1.19.2`, Kafka connector `3.3.0-1.19`, Shade plugin, and main class. Add JUnit assertions proving event round-trip equality and mutually exclusive valid/invalid `ValidationResult` states.

- [x] **Step 2: Verify the tests fail**

Run: `python -m unittest tests.test_chapter_9_artifacts -v`

Expected: FAIL because `jobs/datastream-quality/pom.xml` and Java sources do not exist.

- [x] **Step 3: Implement the Maven module and model classes**

Pin compiler release to 17. Mark Flink runtime dependencies as `provided`, include Kafka connector and Jackson in the shaded JAR, configure `com.ecommerce.quality.DataQualityJob` as the manifest main class, and exclude signature metadata from shading.

- [x] **Step 4: Run model and artifact tests in Java 17**

Run: `docker run --rm -v "${PWD}:/workspace" -v ecommerce-maven-cache:/root/.m2 -w /workspace/jobs/datastream-quality maven:3.9.9-eclipse-temurin-17 mvn -q test`

Expected: all model and artifact tests PASS.

- [x] **Step 5: Commit**

```powershell
git add jobs/datastream-quality tests/test_chapter_9_artifacts.py
git commit -m "build: scaffold chapter 9 datastream job"
```

### Task 2: JSON 解析与纯业务校验

**Files:**
- Create: `jobs/datastream-quality/src/main/java/com/ecommerce/quality/validation/EventValidator.java`
- Create: `jobs/datastream-quality/src/main/java/com/ecommerce/quality/serde/EventJsonCodec.java`
- Create: `jobs/datastream-quality/src/main/java/com/ecommerce/quality/process/ParseAndValidateFunction.java`
- Create: `jobs/datastream-quality/src/test/java/com/ecommerce/quality/validation/EventValidatorTest.java`
- Create: `jobs/datastream-quality/src/test/java/com/ecommerce/quality/serde/EventJsonCodecTest.java`

**Interfaces:**
- Consumes: `UserBehaviorEvent`, `RejectedEvent`, and `ValidationResult` from Task 1.
- Produces: `EventValidator.validate(UserBehaviorEvent event, Instant observedAt): ValidationResult`.
- Produces: `EventJsonCodec.parseAndValidate(String payload, Instant observedAt): ValidationResult` and JSON serializers for all three output models.
- Produces: `ParseAndValidateFunction` main output `UserBehaviorEvent` and `OutputTag<RejectedEvent> REJECTED_TAG`.

- [x] **Step 1: Write the failing validator tests**

Cover a valid event, malformed JSON, non-object JSON, missing/blank fields, each field length limit, invalid `event_type`, invalid timezone-bearing ISO-8601 time, exactly 5-minute future boundary, more-than-5-minute future rejection, unknown-field normalization, and stable reason codes.

- [x] **Step 2: Verify the tests fail**

Run: `mvn -q -Dtest=EventValidatorTest,EventJsonCodecTest test`

Expected: compilation FAIL because validator and codec classes do not exist.

- [x] **Step 3: Implement minimal parsing and validation**

Use a strict `OffsetDateTime.parse` check for timezone presence, inject `observedAt` into pure methods for deterministic tests, and build sanitized error messages without exception text. Use reason codes `MALFORMED_JSON`, `MISSING_REQUIRED_FIELD`, `FIELD_TOO_LONG`, `INVALID_EVENT_TYPE`, `INVALID_EVENT_TIME`, and `FUTURE_EVENT_TIME`.

- [x] **Step 4: Add Flink process adapter and counters**

`ParseAndValidateFunction` delegates all business rules to `EventJsonCodec`, emits valid events to the main stream, rejected records to `REJECTED_TAG`, and increments `parse_errors_total` or `validation_errors_total` based on the reason code.

- [x] **Step 5: Run tests and commit**

Run: `mvn -q test`

Expected: all Java tests PASS.

```powershell
git add jobs/datastream-quality
git commit -m "feat: validate chapter 9 behavior events"
```

### Task 3: 迟到分流、状态去重与指标

**Files:**
- Create: `jobs/datastream-quality/src/main/java/com/ecommerce/quality/process/LateEventFunction.java`
- Create: `jobs/datastream-quality/src/main/java/com/ecommerce/quality/process/DeduplicateFunction.java`
- Create: `jobs/datastream-quality/src/test/java/com/ecommerce/quality/process/LateEventFunctionTest.java`
- Create: `jobs/datastream-quality/src/test/java/com/ecommerce/quality/process/DeduplicateFunctionTest.java`

**Interfaces:**
- Consumes: parsed `UserBehaviorEvent` stream with assigned timestamps/watermarks.
- Produces: `LateEventFunction` main output on-time event and `OutputTag<LateEvent> LATE_TAG`.
- Produces: `DeduplicateFunction` main output first-seen event and `OutputTag<RejectedEvent> DUPLICATE_TAG`.
- Produces metrics: `valid_events_total`, `late_events_total`, and `duplicate_events_total`.

- [x] **Step 1: Write failing operator tests**

Using Flink operator test harnesses, assert that an event at or behind the current watermark becomes `LateEvent`, an on-time event stays on the main output, the first event ID passes, a duplicate within TTL enters DLQ with `DUPLICATE_EVENT`, and an event after TTL passes again.

- [x] **Step 2: Verify the tests fail**

Run: `mvn -q -Dtest=LateEventFunctionTest,DeduplicateFunctionTest test`

Expected: compilation FAIL because both process functions do not exist.

- [x] **Step 3: Implement late-event routing**

Compare the assigned event timestamp with `Context.timerService().currentWatermark()`. Preserve the full event and record `watermark`, `lateness_ms`, and `observed_at` in `LateEvent`; do not emit late records to the clean stream.

- [x] **Step 4: Implement keyed state deduplication**

Configure `ValueStateDescriptor<Boolean>` with 24-hour TTL, `OnCreateAndWrite`, and `NeverReturnExpired`. Emit duplicates to `DUPLICATE_TAG` without updating state; increment the valid counter only after first-seen events pass.

- [x] **Step 5: Run tests and commit**

Run: `mvn -q test`

Expected: parsing, validation, watermark, and TTL tests all PASS.

```powershell
git add jobs/datastream-quality
git commit -m "feat: route late and duplicate events"
```

### Task 4: DataStream 作业拓扑与可靠性配置

**Files:**
- Create: `jobs/datastream-quality/src/main/java/com/ecommerce/quality/config/JobConfig.java`
- Create: `jobs/datastream-quality/src/main/java/com/ecommerce/quality/DataQualityJob.java`
- Create: `jobs/datastream-quality/src/test/java/com/ecommerce/quality/config/JobConfigTest.java`
- Modify: `tests/test_chapter_9_artifacts.py`

**Interfaces:**
- Produces: `JobConfig.fromArgs(String[] args): JobConfig` with explicit getters for bootstrap server, input/output topics, group ID, mode, checkpoint URI, transaction prefixes, watermark, idleness, state TTL, and job version.
- Produces: `DataQualityJob.build(StreamExecutionEnvironment env, JobConfig config): void` and executable `main`.

- [x] **Step 1: Write failing configuration tests**

Assert shadow defaults, production clean-topic selection, duration parsing, unique transaction prefixes, required bootstrap/topic checks, rejection of unsupported mode, and rejection of identical output topics.

- [x] **Step 2: Verify the tests fail**

Run: `mvn -q -Dtest=JobConfigTest test`

Expected: compilation FAIL because `JobConfig` does not exist.

- [x] **Step 3: Implement validated runtime configuration**

Parse Flink `ParameterTool` arguments. Default to shadow mode and `user_behavior_clean_shadow`, but fail before submission for blank Kafka bootstrap servers, conflicting topics, non-positive durations, or missing checkpoint URI.

- [x] **Step 4: Assemble source, watermarks, side outputs, and sinks**

Use `KafkaSource<String>` with committed offsets or earliest fallback. Assign 10-second bounded out-of-orderness and 30-second idleness. Connect rejected outputs from parse and dedup, serialize each envelope with `EventJsonCodec`, and attach three `KafkaSink` instances using EXACTLY_ONCE plus unique transaction prefixes.

- [x] **Step 5: Configure checkpoint and restart behavior**

Set 10-second EXACTLY_ONCE checkpointing, 60-second timeout, 5-second minimum pause, one concurrent checkpoint, externalized checkpoint retention, HashMap State Backend, and fixed-delay restart with three attempts.

- [x] **Step 6: Package and inspect the fat JAR**

Run: `mvn -q clean package`

Expected: `target/datastream-quality-1.0.0.jar` exists, includes Kafka/Jackson classes, excludes Flink runtime classes, and manifest main class is `com.ecommerce.quality.DataQualityJob`.

- [x] **Step 7: Commit**

```powershell
git add jobs/datastream-quality tests/test_chapter_9_artifacts.py
git commit -m "feat: assemble chapter 9 quality topology"
```

### Task 5: 可重复构建、提交与影子数据验证

**Files:**
- Create: `scripts/build_chapter_9_datastream.ps1`
- Create: `scripts/run_chapter_9_shadow.ps1`
- Create: `scripts/verify_chapter_9_shadow.ps1`
- Modify: `infra/.env.example`
- Modify: `tests/test_chapter_9_artifacts.py`

**Interfaces:**
- Produces: `build_chapter_9_datastream.ps1` using a Maven 3.9 + Java 17 Docker image and writing the fat JAR under the module `target` directory.
- Produces: `run_chapter_9_shadow.ps1` that creates four non-destructive Topics, starts the Flink profile, submits exactly one named shadow job, and prints its job ID.
- Produces: `verify_chapter_9_shadow.ps1` that sends run-ID-scoped fixtures, consumes matching outputs, queries Flink REST metrics, and fails unless the expected matrix and count reconciliation pass.

- [x] **Step 1: Extend failing artifact tests**

Assert scripts use `docker compose`, create Topics without deleting them, submit the shaded JAR with shadow mode, retain the original raw Topic, include deterministic valid/duplicate/malformed/missing-field/invalid-time/future/late fixtures, and check REST checkpoint/metric evidence.

- [x] **Step 2: Verify the artifact tests fail**

Run: `python -m unittest tests.test_chapter_9_artifacts -v`

Expected: FAIL because Chapter 9 scripts and environment variables are absent.

- [x] **Step 3: Implement the Java 17 build script**

Use `maven:3.9.9-eclipse-temurin-17` with a named Maven cache volume. Run `mvn -q clean test package`; fail on non-zero exit or missing fat JAR.

- [x] **Step 4: Implement idempotent shadow submission**

Create `user_behavior_events`, `user_behavior_clean_shadow`, `user_behavior_dlq`, and `user_behavior_late` with `--if-not-exists`. Start only Kafka/Flink profiles, wait for Kafka and Flink REST readiness, build the JAR, copy it to JobManager, and submit with `--mode shadow` and a unique shadow group/transaction namespace.

- [x] **Step 5: Implement deterministic end-to-end verification**

Generate a run ID, publish the seven-case matrix, advance the watermark before publishing the late fixture, and consume outputs with temporary groups while filtering by run ID/event IDs. Assert exactly one clean event for the duplicated ID, expected DLQ reason codes, one late envelope, `raw = clean + dlq + late`, at least one successful checkpoint, job state `RUNNING`, and all six counters present.

- [x] **Step 6: Run artifact and full repository tests**

Run: `python -m unittest discover -s tests -v`

Expected: existing 105 tests plus Chapter 9 artifact tests PASS.

- [x] **Step 7: Run real shadow verification**

Run: `./scripts/verify_chapter_9_shadow.ps1`

Expected: script reports one clean first-seen event, duplicate and invalid records in DLQ, one late record, successful reconciliation/checkpoint, and a RUNNING job; existing SQL jobs and downstream tables remain unchanged.

- [x] **Step 8: Commit**

```powershell
git add infra/.env.example scripts/build_chapter_9_datastream.ps1 scripts/run_chapter_9_shadow.ps1 scripts/verify_chapter_9_shadow.ps1 tests/test_chapter_9_artifacts.py
git commit -m "test: verify chapter 9 shadow quality pipeline"
```

### Task 6: TaskManager 恢复、Savepoint 与中文收尾文档

**Files:**
- Create: `scripts/verify_chapter_9_recovery.ps1`
- Create: `docs/chapter-9-datastream-quality-runbook.md`
- Modify: `jobs/README.md`
- Modify: `docs/superpowers/specs/2026-07-22-chapter-9-java-datastream-data-quality-design.md`
- Modify: `docs/superpowers/plans/2026-07-22-chapter-9-java-datastream-data-quality-implementation.md`
- Modify: `tests/test_chapter_9_artifacts.py`

**Interfaces:**
- Produces: recovery script that records the current job/checkpoint/output state, restarts only TaskManager, waits for recovery, creates a Savepoint, resumes from it, and checks no duplicate committed output for a run-specific event.
- Produces: Chinese runbook with architecture, commands, reason-code table, evidence, rollback boundary, troubleshooting record, and interview narrative.

- [x] **Step 1: Write failing recovery/documentation artifact tests**

Assert TaskManager-only restart, checkpoint wait, stop-with-savepoint, `-s` restore, no destructive Topic/checkpoint deletion, and preservation of the Phase A shadow evidence alongside the Phase B final status.

- [x] **Step 2: Verify tests fail**

Run: `python -m unittest tests.test_chapter_9_artifacts -v`

Expected: FAIL because recovery script and runbook are missing.

- [x] **Step 3: Implement and execute TaskManager recovery test**

Use Flink REST to select only the Chapter 9 job, wait for a completed checkpoint, run `docker restart ecom-flink-taskmanager`, wait until the same job returns to `RUNNING`, then validate output uniqueness.

- [x] **Step 4: Implement and execute Savepoint restore test**

Use `flink stop --savepointPath file:///tmp/flink-savepoints/chapter-9 <job-id>`, verify the path exists, submit the same JAR with `-s <savepoint-path>` and the same operator UIDs, then verify stateful duplicate suppression still works.

- [x] **Step 5: Record evidence and operational boundaries**

Write actual job ID, Flink/Java versions, Maven test count, repository test count, Topic result counts, reason-code distribution, checkpoint ID/path, restart result, Savepoint path, and any real troubleshooting. Do not claim full-cluster HA or downstream exactly-once.

- [x] **Step 6: Run final verification**

Run: `python -m unittest discover -s tests -v`

Run: `./scripts/build_chapter_9_datastream.ps1`

Run: `./scripts/verify_chapter_9_shadow.ps1`

Run: `./scripts/verify_chapter_9_recovery.ps1`

Expected: all commands exit 0; shadow job is RUNNING after restore; main SQL source files are unchanged.

- [x] **Step 7: Inspect scope and commit**

Run: `git diff --check`

Run: `git diff --name-only 438b35c..HEAD`

Expected: only Chapter 9 module/scripts/tests/docs and intentional `.env.example` additions appear; `.superpowers/sdd/task-1-report.md` is absent.

```powershell
git add jobs/README.md docs/chapter-9-datastream-quality-runbook.md docs/superpowers/specs/2026-07-22-chapter-9-java-datastream-data-quality-design.md docs/superpowers/plans/2026-07-22-chapter-9-java-datastream-data-quality-implementation.md scripts/verify_chapter_9_recovery.ps1 tests/test_chapter_9_artifacts.py
git commit -m "docs: close chapter 9 shadow quality phase"
```

## Phase A 完成门禁

- [x] Java 与仓库自动化测试全部通过，Fat JAR 可重复构建。
- [x] 真实 Kafka 三路分流矩阵、去重、Watermark 和数量对账通过。
- [x] Flink REST 证明作业 RUNNING、Checkpoint 成功、六个 Counter 可读取。
- [x] TaskManager 重启和 Savepoint 恢复均有真实证据。
- [x] Phase A 阶段现有 SQL Source 与 Doris/Iceberg/Trino/第 8 章 API 保持未切流；Phase B 已切换正式 Source，原始 Source 仍保留作回滚入口。
- [x] 文档已明确记录 Phase B 正式切流完成，并保留 Phase A 影子与恢复证据。
- [x] 用户已确认 Phase B；受控切流计划已编写、执行并完成文档收尾。

## Phase A 执行记录（2026-07-22）

- Maven Java 17 测试、Fat JAR 检查和 110 项仓库测试通过。
- Kafka 三路输出与五种 DLQ 原因码通过，`raw=8, clean=2, dlq=5, late=1`。
- Checkpoint、六个 Counter、TaskManager 重启和 Savepoint 状态恢复通过。
- 实际排障包括 Jackson 版本对齐、PowerShell 编码、容器状态去换行、指标延迟发现、Savepoint 权限和重启窗口调整。
- 详细证据见 `docs/chapter-9-datastream-quality-runbook.md`。

**执行边界实录：Phase A 影子与恢复已完成；随后经人工确认完成 Phase B 正式切流。最终 production、Doris clean、Iceberg clean 均保持 RUNNING，验收逻辑 run `ab626...` 通过分阶段恢复，最终 `read_only_finalize` 零发送；Doris/Iceberg 端到端 Exactly-Once 不超出 connector 实际语义。**
