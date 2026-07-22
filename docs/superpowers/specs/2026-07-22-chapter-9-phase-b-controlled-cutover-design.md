# 第 9 章 Phase B：DataStream 主链路受控切流设计

## 1. 背景与目标

第 9 章 Phase A 已完成 Java DataStream 数据质量作业的影子验证，包括字段校验、Watermark、状态 TTL 去重、Side Output 分流、Exactly-Once、Checkpoint、TaskManager 故障恢复和 Savepoint 恢复。

Phase B 的目标是将正式下游从原始 Topic `user_behavior_events` 切换到清洗 Topic `user_behavior_clean`，并完成 Doris、Iceberg、Trino 和第 8 章 AI 分析 API 的端到端回归。

本阶段不重写现有 Flink SQL 聚合逻辑。Java DataStream 负责数据质量，Flink SQL 继续负责指标聚合和湖仓写入，形成职责清晰的混合架构。

## 2. 实施原则

1. **先扩容、后切流**：先解决本地 Flink 槽位不足，再启动完整链路。
2. **先停流、再换源**：暂停生成器并等待 lag 清零，避免切换窗口内产生不确定边界。
3. **状态可恢复**：停止影子作业前创建 Savepoint，正式作业从该 Savepoint 恢复。
4. **新旧入口并存**：保留原始 Topic 和旧 SQL Source 文件，不删除历史数据。
5. **失败可回滚**：切流脚本和回滚脚本分离，不自动删除 Topic、Checkpoint、Savepoint 或业务表。
6. **证据驱动验收**：不能只以容器或作业显示 `RUNNING` 作为成功标准。

## 3. 资源方案

### 3.1 选择

本地环境使用一个 TaskManager，将 `taskmanager.numberOfTaskSlots` 从 2 调整为 4。

预计资源分配：

| 作业 | 最低 slot 数 |
| --- | ---: |
| DataStream 数据质量正式作业 | 1 |
| Doris 实时指标 SQL 作业 | 1 |
| Iceberg 明细 SQL 作业 | 1 |
| 故障恢复与调度余量 | 1 |

该方案比增加第二个 TaskManager 更适合当前本地教学环境：它能展示完整链路并发运行，同时避免引入多 TaskManager 编排、容器命名和额外内存管理复杂度。生产环境应使用多个 TaskManager 提供故障域隔离，本章不把本地扩容描述为生产高可用。

### 3.2 扩容边界

只允许重建 `flink-taskmanager`，并使用 `--no-deps` 防止 Compose 级联重建 JobManager、Kafka、Doris、MinIO 或 Hive Metastore。

扩容前记录当前运行作业、Checkpoint 和 Savepoint 信息；扩容后必须确认：

- Flink REST 返回 `slots-total = 4`；
- TaskManager 数量仍为 1；
- 影子作业恢复为 `RUNNING`；
- 影子作业产生新的成功 Checkpoint。

## 4. 目标架构与数据流

```text
数据生成器
    |
    v
user_behavior_events
    |
    v
Java DataStream 数据质量作业
    |----------------------|--------------------|
    v                      v                    v
user_behavior_clean   user_behavior_dlq   user_behavior_late
    |
    |-------------------------------|
    v                               v
Flink SQL -> Doris             Flink SQL -> Iceberg
    |                               |
    v                               v
第 8 章 AI API                    Trino 查询
```

正式作业使用独立且稳定的 Consumer Group、作业名和 Kafka 事务前缀，不能复用影子环境标识。影子 Topic `user_behavior_clean_shadow` 保留为历史验证证据，但正式下游不得消费该 Topic。

## 5. 受控切流流程

### 5.1 切流前检查

1. 确认 Kafka、Flink、Doris、MinIO、Hive Metastore、Trino 和 API 服务可用。
2. 确认影子作业处于 `RUNNING` 且最近 Checkpoint 成功。
3. 确认 `user_behavior_clean` 存在；只允许 `--if-not-exists` 创建，不清空 Topic。
4. 记录原始 SQL Source 文件校验值、当前作业 ID、Consumer Group offset 和目标表基线值。
5. 将 TaskManager 扩为 4 slots，并完成扩容验收。

### 5.2 停流与状态交接

1. 暂停数据生成器。
2. 等待影子 Consumer Group lag 清零，并记录 offset 证据。
3. 使用 Flink Stop-with-Savepoint 停止影子作业。
4. 确认 Savepoint 路径存在且影子作业不再运行。
5. 从 Savepoint 以 `production` 模式启动正式作业，输出到 `user_behavior_clean`。

正式作业必须使用与影子作业一致的算子 UID。若 Savepoint 恢复发生状态不兼容错误，立即停止切流，不允许使用 `--allowNonRestoredState` 绕过状态丢失。

### 5.3 SQL Source 切换

为 Doris 和 Iceberg 创建使用 `user_behavior_clean` 的正式 Source 定义。旧的原始 Source 文件继续保留，作为明确的回滚入口。

停止旧 SQL 作业后再提交正式 SQL 作业，避免新旧作业同时向同一业务 Sink 写入造成重复。提交后确认三个核心作业同时处于 `RUNNING`：

- DataStream 数据质量正式作业；
- Doris 实时指标 SQL 作业；
- Iceberg 明细 SQL 作业。

### 5.4 恢复流量

恢复生成器后发送带唯一批次标记的受控测试矩阵，其中包括正常、重复、非法和迟到事件。批次标记嵌入 `event_id`、`user_id` 和 `product_id`，不增加第九个业务字段。验证结束前不扩大测试流量。

## 6. 验收标准

### 6.1 Flink 与 Kafka

- Flink REST 显示 1 个 TaskManager、4 个总 slots 和 3 个核心运行作业。
- DataStream 作业在恢复流量后产生新的成功 Checkpoint。
- 正常事件进入 `user_behavior_clean`，非法和重复事件进入 `user_behavior_dlq`，迟到事件进入 `user_behavior_late`。
- 受控测试满足 `raw = clean + dlq + late`，重复事件只在 clean 中保留一次。
- 正式 Consumer Group lag 最终归零。

### 6.2 下游回归

- Doris PV/UV 指标包含本次批次标记对应事件，数值与输入矩阵一致。
- Iceberg 明细表只包含通过质量校验的正常事件。
- Trino 能读取本次写入的 Iceberg 明细。
- 第 8 章 API 返回 Doris 和 Trino 的可信证据，并保持规则分析或模型降级能力可用。

### 6.3 安全检查

- 原始 Topic、影子 Topic、DLQ、late Topic、Checkpoint 和 Savepoint 均未删除。
- 旧 SQL Source 仍可用于回滚。
- 自动化测试、Java 测试和切流验证脚本全部通过。

## 7. 回滚设计

出现以下任一情况立即回滚：

- 正式 DataStream 作业无法从 Savepoint 恢复；
- Checkpoint 连续失败或 Kafka 事务提交异常；
- 三个核心 Flink 作业无法同时稳定运行；
- Doris、Iceberg、Trino 或第 8 章 API 回归失败；
- 对账结果不满足守恒关系或出现非预期重复。

回滚顺序：

1. 暂停生成器。
2. 记录失败作业日志、Job ID、Checkpoint、offset 和目标表证据。
3. 停止正式 SQL 作业和 DataStream production 作业。
4. 使用保留的原始 Topic Source 重新提交旧 SQL 作业。
5. 确认旧链路恢复后再启动生成器。
6. 保留正式 clean Topic 和失败现场，修复后重新执行完整切流，不在原现场上继续追加变更。

回滚不承诺自动撤销切流窗口内已经写入 Doris 或 Iceberg 的数据。受控测试必须在现有字段中嵌入唯一批次标记，以便识别、核对和必要时执行显式补偿。

## 8. 自动化边界

Phase B 提供以下独立脚本：

- 资源扩容与检查脚本；
- 正式切流脚本；
- 端到端验收脚本；
- 回滚脚本。

脚本默认采用 fail-fast，并在每个破坏性边界前验证容器、作业和 Topic 身份。脚本不得自动清空 Topic、删除表、删除状态目录或强制忽略 Savepoint 不兼容。

## 9. 面试表达

“影子验证通过后，我没有直接把 SQL Source 改到 clean Topic，而是先核算资源，发现本地 Flink 的 2 个 slots 无法同时承载 DataStream、Doris SQL 和 Iceberg SQL。于是我先将单 TaskManager 扩为 4 slots，并限制 Compose 只重建 TaskManager。切流时暂停生成器、等待 lag 清零，再通过 Stop-with-Savepoint 把影子作业状态交接给 production 作业，最后切换两个 SQL 下游。验收不仅看 RUNNING，还检查 Checkpoint、Kafka 守恒对账、Doris PV/UV、Iceberg 明细、Trino 和 AI API。整个过程保留原始 Topic、旧 Source 和 Savepoint，因此可以按明确步骤回滚。”

## 10. 状态

本设计已确认采用“单 TaskManager 4 slots”方案。当前仅完成 Phase B 设计，尚未执行资源扩容或正式切流。
