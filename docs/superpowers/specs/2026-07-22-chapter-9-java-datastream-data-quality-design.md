# 第 9 章：Java DataStream API 数据质量治理设计

## 1. 背景

前 8 章已经跑通两条主链路：

- 实时指标链路：`Kafka -> Flink SQL -> Doris -> FastAPI`
- 湖仓分析链路：`Kafka -> Flink SQL -> Iceberg/MinIO -> Trino -> AI 分析助手`

现有 Flink SQL 适合表达标准聚合和明细落湖，但原始 Kafka 事件仍缺少统一的数据质量入口。非法 JSON、字段缺失、重复事件和严重迟到数据可能直接进入下游，影响指标可信度。

第 9 章补充 Java DataStream API，不替换 Flink SQL 的全部职责。DataStream 负责复杂事件治理，Flink SQL 继续负责清晰、稳定的指标与落湖逻辑，从而形成“SQL 与 DataStream 各司其职”的架构演进。

## 2. 设计原则

1. **先旁路、后切流**：先运行影子清洗链路，对账通过后再切换下游。
2. **职责单一**：DataStream 做解析、校验、事件时间、去重和分流；SQL 做聚合和存储。
3. **失败可解释**：每条拒绝记录都有稳定原因码，不依赖异常堆栈排障。
4. **状态有边界**：去重状态配置 TTL，避免状态无限增长。
5. **切流可回退**：原始 Topic 和旧 SQL Source 在验证期保留，不做破坏性迁移。
6. **控制难度**：本章只完成一个可运行的数据质量闭环，不一次引入所有生产组件。

## 3. 方案选择

### 3.1 采用方案

采用 Java 17 + Flink 1.19.2 DataStream API，新增独立 Maven 作业：

```text
user_behavior_events
        |
        v
Java DataStream Quality Job
  |          |          |
  v          v          v
clean       dlq        late
  |
  v
Flink SQL -> Doris / Iceberg
```

选择 Java 的原因：

- 与当前 `flink:1.19.2-scala_2.12-java17` 镜像一致。
- DataStream API 的生产使用和面试认可度更高。
- 能完整展示 Watermark、Keyed State、TTL、Side Output、Checkpoint 和 Savepoint。

### 3.2 不采用的方案

- **PyFlink**：上手较快，但依赖部署和生产使用面不如 Java 稳定。
- **全部改写为 DataStream**：会丢失现有 SQL 资产并扩大回归范围。
- **仅做 Java Demo**：不进入真实 Kafka 和下游，无法证明工程价值。
- **直接切换主链路**：缺少影子对账和回滚边界，风险过高。

## 4. 范围与难度控制

### 4.1 Phase A：核心闭环，必做

- Java Maven 工程与可提交 Fat JAR。
- Kafka 原始事件读取。
- JSON 解析和字段校验。
- 事件时间与 Watermark。
- 基于 `event_id` 的状态去重和 TTL。
- 正常、DLQ、迟到三路 Kafka 输出。
- Checkpoint 和固定延迟重启策略。
- 影子运行、数据对账和真实 Topic 验证。

### 4.2 Phase B：受控切流，二次确认后执行

- 暂停生成器并等待 Kafka lag 清零。
- 启动正式 clean Topic 作业。
- 将 Flink SQL Source 切换到 clean Topic。
- 回归 Doris、Iceberg、Trino 和第 8 章 API。
- 验证失败时恢复原始 Topic Source。

Phase A 完成后必须先展示对账证据，再由用户确认是否进入 Phase B，不自动连续执行。

### 4.3 后续章节再做

- Flink HA 和 Kubernetes 部署。
- RocksDB State Backend 与大状态调优。
- CEP、CDC 和复杂漏斗。
- Schema Registry 和 Avro/Protobuf。
- 自动补数平台和 DLQ 管理页面。
- Prometheus/Grafana 完整监控栈。

主链路切换必须在 Phase A 验证通过后单独执行，不能为了赶进度跳过门禁。

## 5. Topic 设计

| Topic | 用途 |
| --- | --- |
| `user_behavior_events` | 现有原始事件入口，切流期间继续保留 |
| `user_behavior_clean_shadow` | 影子阶段正常数据输出 |
| `user_behavior_clean` | 正式切流后的正常数据输出 |
| `user_behavior_dlq` | 解析失败、字段非法和重复事件 |
| `user_behavior_late` | 合法但超过 Watermark 的迟到事件 |

影子和正式 Topic 分离，避免影子历史数据在切流后被下游重复消费。

## 6. 数据契约

### 6.1 正常事件

沿用现有八字段契约：

```json
{
  "event_id": "evt_000001",
  "user_id": "u_1001",
  "product_id": "p_2001",
  "event_type": "view",
  "event_time": "2026-07-22T10:00:00Z",
  "channel": "app",
  "device_type": "android",
  "page_id": "home"
}
```

所有字段不能为空，并配置合理长度上限。`event_type` 允许：

- `view`
- `click`
- `cart`
- `purchase`
- `order`
- `pay`

`channel` 和 `device_type` 在本章只校验非空与长度，不做严格枚举，兼容前面章节已有的 `mobile/desktop/ios/android/pc` 等值。

### 6.2 拒绝事件

DLQ 使用稳定信封：

```json
{
  "reason_code": "MISSING_REQUIRED_FIELD",
  "reason_message": "user_id is required",
  "raw_payload": "{...}",
  "observed_at": "2026-07-22T10:00:01Z",
  "job_version": "chapter-9-v1"
}
```

原因消息只描述校验结论，不写 Java 堆栈、Kafka 凭证或内部配置。

### 6.3 迟到事件

迟到事件保留完整标准事件，并补充 Watermark 和延迟量，便于后续补数。本章不自动回灌，避免重复计算。

## 7. 处理流程

### 7.1 JSON 解析

Kafka Source 先读取原始字符串：

- 无法解析为 JSON：`MALFORMED_JSON`
- JSON 不是对象：`MALFORMED_JSON`
- 未知额外字段：第一版忽略，但标准输出只保留八个已知字段

### 7.2 字段校验

依次校验：

1. 八个字段存在且非空。
2. 字符串未超过长度上限。
3. `event_type` 属于允许集合。
4. `event_time` 是带时区的 ISO-8601 时间。
5. 事件时间不超过当前处理时间 5 分钟。

对应原因码：

- `MISSING_REQUIRED_FIELD`
- `FIELD_TOO_LONG`
- `INVALID_EVENT_TYPE`
- `INVALID_EVENT_TIME`
- `FUTURE_EVENT_TIME`

### 7.3 Watermark 与迟到处理

使用：

- 最大乱序时间：10 秒
- 空闲分区检测：30 秒

事件时间不大于当前 Watermark 时，事件进入 `user_behavior_late`，不进入实时 clean 流。迟到数据被保留，后续通过独立补数流程处理。

### 7.4 去重与状态 TTL

合法且未迟到的事件按 `event_id` 执行 `keyBy`：

- 第一次出现：写入 clean 流，并在 `ValueState` 中记录。
- TTL 内再次出现：写入 DLQ，原因码为 `DUPLICATE_EVENT`。
- TTL 到期后再次出现：按新事件处理。

默认 TTL 为 24 小时，采用 `OnCreateAndWrite` 更新策略，不因重复读取延长状态寿命。

## 8. Java 工程结构

```text
jobs/datastream-quality/
├── pom.xml
├── src/main/java/com/ecommerce/quality/
│   ├── DataQualityJob.java
│   ├── config/JobConfig.java
│   ├── model/UserBehaviorEvent.java
│   ├── model/RejectedEvent.java
│   ├── model/LateEvent.java
│   ├── validation/EventValidator.java
│   ├── process/ParseAndValidateFunction.java
│   ├── process/DeduplicateFunction.java
│   └── serde/
└── src/test/java/com/ecommerce/quality/
```

边界要求：

- `EventValidator` 是无 Flink 依赖的纯业务校验器。
- `process` 包只处理流分支、状态和计时语义。
- `serde` 负责 Kafka 输入输出契约。
- `DataQualityJob` 只负责组装拓扑和运行参数。

## 9. 作业配置

所有易变项通过命令行参数或环境变量注入：

- Bootstrap Server
- 输入和输出 Topic
- Consumer Group
- 运行模式：`shadow` 或 `production`
- Watermark 乱序时间
- 状态 TTL
- Checkpoint 目录
- Kafka 事务前缀
- 作业版本

非法参数在作业提交前直接失败，不静默使用危险默认值。

## 10. Checkpoint、Kafka 事务与恢复

Phase A 配置：

- Checkpoint 间隔：10 秒
- Checkpoint 超时：60 秒
- 最小间隔：5 秒
- 并发 Checkpoint：1
- 取消作业时保留外部 Checkpoint
- 固定延迟重启：最多 3 次
- Kafka Sink：`DeliveryGuarantee.EXACTLY_ONCE`
- clean、DLQ、late 使用不同事务前缀

Checkpoint 保存在宿主机挂载的 `tmp/checkpoints/chapter-9`。本地环境先使用默认 HashMap State Backend；RocksDB 和大状态调优放到后续压测章节。

这里的 Exactly-Once 边界只覆盖 Flink 状态与 Kafka Sink 事务。后续 Doris/Iceberg 是否端到端 Exactly-Once，仍取决于对应 Connector 和写入配置，文档不得扩大声明。

## 11. 影子运行与切流

### 11.1 影子阶段

1. DataStream 使用独立 Consumer Group 消费原始 Topic。
2. 正常数据写入 `user_behavior_clean_shadow`。
3. 现有 Flink SQL 主链路继续消费原始 Topic。
4. 对账：`raw = clean_shadow + dlq + late`，并检查原因码分布。
5. 验证重复事件只在 clean 中出现一次。

### 11.2 正式切流

正式切流必须人工确认：

1. 暂停生成器。
2. 等待旧链路和影子链路 lag 清零。
3. 停止影子作业并保留 Savepoint。
4. 以 production 模式启动正式 clean Topic 作业。
5. 将 Flink SQL Source 切换到 `user_behavior_clean`。
6. 恢复生成器。
7. 验证 Doris PV/UV、Iceberg 明细、Trino 和第 8 章分析 API。

正式 clean Topic 不复用影子 Topic，避免重放影子历史导致下游重复。

### 11.3 回滚

若切流验证失败：

- 再次暂停生成器。
- 停止 clean SQL 作业和 DataStream production 作业。
- 恢复原始 Topic SQL Source。
- 从保留的 Checkpoint/Savepoint 和 Kafka offset 证据定位边界。
- 验证旧链路恢复后再重新放量。

脚本不能自动删除 Topic、Checkpoint、Savepoint 或旧作业状态。

## 12. 测试设计

### 12.1 单元测试

- 正常 JSON 解析。
- 非法 JSON 和非对象 JSON。
- 缺字段、空字段和超长字段。
- 合法与非法 `event_type`。
- 非法、未来和边界事件时间。
- DLQ 原因码稳定。

### 12.2 状态测试

- 同一 `event_id` 在 TTL 内只进入 clean 一次。
- 重复事件进入 DLQ。
- TTL 到期后事件可再次通过。
- Watermark 前后的事件分流正确。

### 12.3 真实 Kafka 集成测试

验证以下输入：

| 输入 | 预期输出 |
| --- | --- |
| 正常事件 | clean |
| 重复 `event_id` | DLQ：`DUPLICATE_EVENT` |
| 非法 JSON | DLQ：`MALFORMED_JSON` |
| 缺少字段 | DLQ：`MISSING_REQUIRED_FIELD` |
| 非法时间 | DLQ：`INVALID_EVENT_TIME` |
| 未来超过 5 分钟 | DLQ：`FUTURE_EVENT_TIME` |
| 超过 Watermark | late |

### 12.4 故障恢复测试

1. 等待成功 Checkpoint。
2. 记录输出 Topic 数量。
3. 重启 TaskManager。
4. 验证作业自动恢复。
5. 验证事务 Sink 没有产生已提交数据重复。
6. 从 Savepoint 停止并重新提交，再验证状态恢复。

本章故障演练只做 TaskManager 和 Savepoint 两条路径，不扩展到完整集群 HA。

## 13. 指标与可观测性

作业至少注册以下 Counter：

- `valid_events_total`
- `dlq_events_total`
- `late_events_total`
- `duplicate_events_total`
- `parse_errors_total`
- `validation_errors_total`

验证脚本同时检查 Kafka Topic 数据和 Flink REST 指标，不能只以容器 `running` 作为成功标准。

完整 Prometheus/Grafana 接入放到后续可观测性章节。

## 14. 验收标准

Phase A 完成需同时满足：

1. Maven 测试通过并生成可提交 Fat JAR。
2. Java 作业在 Flink 1.19.2 / Java 17 中为 `RUNNING`。
3. 正常、DLQ、迟到三路输出符合测试矩阵。
4. 去重和 TTL 行为有自动化证据。
5. Checkpoint 成功，TaskManager 重启后作业恢复。
6. 影子阶段满足数量对账。

完成以上 6 项后，文档状态必须写成“影子链路已完成、主链路尚未切换”，不能把旁路验证描述成正式上线。Phase B 的独立验收标准是：正式切流经过人工确认、回滚入口可用，并且 Doris、Iceberg、Trino 和第 8 章 API 回归通过。

## 15. 面试表达

“项目早期我先用 Flink SQL 跑通 Kafka、Doris 和 Iceberg，因为标准聚合用 SQL 表达更清晰。链路稳定后，我没有为了展示 API 而把所有 SQL 重写成 Java，而是针对 SQL 不擅长的数据质量场景新增 DataStream 作业：原始事件先经过解析、校验、Watermark、状态 TTL 去重和 Side Output 分流，再进入 clean Topic。迁移不是直接替换，我先做影子对账，确认 raw 等于 clean、DLQ 和 late 的总和，再暂停流量、保存状态并切换 SQL Source。这样既保留了 SQL 的开发效率，也展示了 DataStream 的状态管理、Exactly-Once、故障恢复和可回滚切流能力。”
