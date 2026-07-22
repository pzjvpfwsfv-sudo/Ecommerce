# 第 9 章 Java DataStream 数据质量治理运行手册

## 1. 当前状态

**影子链路已完成、主链路尚未切换。**

```text
user_behavior_events
        |
        v
Java DataStream Quality Job
  |          |          |
  v          v          v
clean       dlq        late
shadow
```

现有 Doris/Iceberg Flink SQL Source 文件没有修改，仍指向原始 `user_behavior_events`。只有用户再次确认 Phase B 后，才允许把下游 Source 切换到正式 `user_behavior_clean`。

## 2. 核心能力

- Java 17 + Flink 1.19.2 DataStream API Fat JAR。
- 八字段 JSON 解析与稳定原因码校验。
- 最大乱序 10 秒、空闲分区 30 秒的 Watermark。
- 基于 `event_id` 的 `ValueState` 去重，TTL 24 小时。
- clean、DLQ、late 三路 Kafka 输出。
- 三个 Kafka Sink 使用独立事务前缀和 EXACTLY_ONCE。
- 10 秒 Checkpoint、60 秒超时、5 秒最小间隔、单并发 Checkpoint。
- HashMap State Backend、最多 3 次、每次间隔 15 秒的固定延迟重启。
- TaskManager 重启和 Savepoint 恢复验证。

## 3. 常用命令

```powershell
./scripts/build_chapter_9_datastream.ps1
./scripts/run_chapter_9_shadow.ps1
./scripts/verify_chapter_9_shadow.ps1
./scripts/verify_chapter_9_recovery.ps1
python -m unittest discover -s tests -v
```

## 4. DLQ 原因码

| 原因码 | 含义 |
| --- | --- |
| `MALFORMED_JSON` | 输入不是合法 JSON 对象 |
| `MISSING_REQUIRED_FIELD` | 八个必填字段存在缺失或空白 |
| `FIELD_TOO_LONG` | 字段超过 256 字符 |
| `INVALID_EVENT_TYPE` | 事件类型不在允许集合中 |
| `INVALID_EVENT_TIME` | 时间不是带时区的 ISO-8601 |
| `FUTURE_EVENT_TIME` | 事件时间比处理时间晚超过 5 分钟 |
| `DUPLICATE_EVENT` | 相同 `event_id` 在 24 小时 TTL 内重复出现 |

## 5. 真实验收证据

### 自动化测试

- 原有仓库基线：105 项通过。
- 加入第 9 章后：110 项 Python 测试通过。
- Maven Java 17 容器执行全部 15 项 JUnit 测试通过。
- Fat JAR 包含 Kafka Connector/Jackson，不包含 Flink 核心运行时类。

### 影子三路分流

- run ID：`chapter9-914670168b5c49439fcc215d9a04eaf2`。
- 作业 ID：`826791363f10d34aa05e88f0722cc45f`。
- `raw=8`、`clean=2`、`dlq=5`、`late=1`。
- 对账结果：`raw = clean + dlq + late`。
- 同一重复事件在 clean 中恰好出现 1 次。
- 五种预期 DLQ 原因码各出现 1 次。
- 验证时已完成 24 个 Checkpoint，六个自定义 Counter 均可查询。

### 故障恢复

- TaskManager 重启测试作业 ID：`e5dec34fc70c34a276640241f110a3b0`。
- 重启后同一 Job 恢复为 RUNNING，并完成新 Checkpoint。
- Savepoint：`file:/workspace/tmp/savepoints/chapter-9/savepoint-e5dec3-482c0e9b60b9`。
- 恢复作业 ID：`6f6e24deea18e22722bfd5e0a83895e4`。
- 恢复后重放相同 `event_id`：`clean=1`、`duplicate_dlq=1`。

## 6. 真实排障记录

### Jackson 版本冲突

只固定 `jackson-databind` 时，Flink 传递依赖带入不同版本的 `jackson-core`，触发 `NoSuchMethodError`。修复方式是统一锁定 core、annotations、databind 为 2.17.2。

### PowerShell 5 编码

UTF-8 无 BOM 的中文脚本字符串会被 Windows PowerShell 5 错误解码。运维脚本改为 ASCII 输出，中文说明集中在 Markdown 中。

### 容器状态误判

最初没有清理 `docker inspect` 输出换行，脚本误判容器缺失并执行 Compose 重建。修复后逐个容器读取、`Trim()` 并检查退出码；脚本明确不使用 `force-recreate`。

误重建会中断 Flink 运行中作业，但没有删除 Kafka Topic、Doris、Iceberg 或 MinIO 数据。旧 SQL Source 文件未修改；第 4/5 章原启动脚本本身带 `--force-recreate`，因此没有在 2-slot 环境中盲目重提未知旧 Job。

### 指标发现时序

Flink REST 指标带算子作用域前缀，例如 `0.deduplicate-event-id.valid_events_total`，且启动初期不一定立即出现。脚本改为按后缀匹配并有限轮询。

### Savepoint 权限

目录最初由 root 创建，Flink 用户无法写入。脚本只为 `tmp/savepoints/chapter-9` 创建共享目录并授权，不删除历史 Savepoint。

### 重启窗口不足

TaskManager 重启第一次失败的根因是 `3 次 x 5 秒` 的窗口在 TaskManager 注册前耗尽，最终出现 `NoResourceAvailableException`。保持最多 3 次不变，将间隔调整为 15 秒后，真实恢复通过。

## 7. 运行边界

- 这里只证明 Flink 状态与 Kafka Sink 事务范围内的 Exactly-Once。
- 这里只验证单 TaskManager 重启，不代表完整集群 HA。
- Savepoint、Checkpoint 和 Topic 都保留，脚本不自动删除。
- 影子 Topic 不能直接复用为正式 Topic。
- Phase B 必须暂停流量、等待 lag 清零、保存状态、人工切流并完成下游回归。

## 8. 面试表达

项目先用 Flink SQL 建立 Doris 实时指标和 Iceberg 湖仓链路，随后没有重写全部 SQL，而是把解析、校验、Watermark、状态 TTL 去重和 Side Output 交给 Java DataStream。迁移采用影子 Topic 对账，再用 TaskManager 重启和 Savepoint 证明状态可恢复。实际排障还覆盖依赖冲突、容器冷启动窗口、文件权限和 REST 指标作用域，体现的是可验证、可回滚的架构演进，而不是单纯增加一个 Java Demo。
