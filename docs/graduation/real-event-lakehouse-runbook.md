# G2-C 真实事件湖仓运行手册

## 目标与结论

G2-C 将 G2-B 的真实 `clean` 与 `late` 有效事件统一规范化后，通过一个 Flink SQL writer 写入 `lakehouse.analytics.real_behavior_detail_v1`，再由 Trino 对数量、唯一性、字段质量、Checkpoint 和 Iceberg Snapshot 做失败即停的验收。DLQ 只用于问题追踪，不进入事实表。

2026-09-18 的动态验收已通过。正式表包含 1,002 条真实事件，其中 `clean=1,001`、`late=1`；`event_id` 去重后仍为 1,002，五类质量违规均为 0。验收作业已取消以释放本机资源，Iceberg 表与 Snapshot 保留，后续 G2-D 可直接读取。

## 数据与资源身份

| 项目 | 实测值 |
| --- | --- |
| 数据集 | REES46 Multi-category Store，真实匿名化历史行为数据 |
| 标准化文件 | `data/rees46/oct-nov-users-2pct-verified.jsonl` |
| 文件大小 / 事件数 | 1,045,479,893 字节 / 2,199,938 条 |
| 文件 SHA-256 | `18a3202d380c8f6c53ad767ec2043d0717df1d3604d2211639bb0fc4699a5437` |
| 验收 run-id | `g2c-20260918a` |
| 原始 / clean / late Topic | `real_behavior_events_v1_g2c-20260918a` / `real_behavior_clean_v1_g2c-20260918a` / `real_behavior_late_v1_g2c-20260918a` |
| G2-B 作业 | `999f5fb239549a20b2b7aea2fc7f1bb7`，完成 28 次 Checkpoint 后取消 |
| G2-C 正式作业 | `2350b9398e755f609457ba94bae8786c`，完成 27 次 Checkpoint 后取消 |
| 正式表 | `lakehouse.analytics.real_behavior_detail_v1` |
| 最新验收 Snapshot | `3854376992136224865`，提交于 `2026-09-18 07:50:17.775 UTC` |

真实数据的下载、全扫描和稳定用户抽样证据见 [数据准备说明](data-readiness.md)，G2-B 的 Java DataStream 入口与恢复边界见 [真实事件质量运行手册](real-event-quality-runbook.md)。

## 链路职责

```text
真实历史 JSONL
  -> G2-A 按业务时间限速回放
  -> Kafka raw Topic
  -> G2-B Java DataStream 校验、去重、迟到分流
  -> clean + late Topic
  -> G2-C Flink SQL 统一字段与单 writer
  -> MinIO + Iceberg 真实明细表
  -> Trino 对账与后续 G2-D 指标计算
```

目标表保留 17 个原始字段，同时增加 `event_ts`、`event_date`、`price_decimal`、`quality_route`、Kafka 落地坐标及 late 诊断字段。业务 ID 仍按字符串保存，原始 `price` 不被类型化字段替代。

## 运行命令

先做不连接 Docker 的渲染检查。下列 run-id 是历史验收身份，用于说明命令，不要对现有正式表再次提交：

```powershell
./scripts/run_g2c_real_event_lakehouse.ps1 `
  -RunId g2c-20260918a `
  -TableName real_behavior_detail_v1 `
  -PlanOnly
```

首次提交时，G2-B 对应 `clean`/`late` Topic 必须已存在；runner 还要求该 run-id 从未提交过且目标表为空：

```powershell
./scripts/run_g2c_real_event_lakehouse.ps1 `
  -RunId g2c-20260918a `
  -TableName real_behavior_detail_v1
```

验收固定数量、唯一性、字段质量、Checkpoint 和 Snapshot：

```powershell
./scripts/verify_g2c_real_event_lakehouse.ps1 `
  -RunId g2c-20260918a `
  -TableName real_behavior_detail_v1 `
  -ExpectedCleanCount 1001 `
  -ExpectedLateCount 1 `
  -ExpectedEventRouteSha256 0f7a0fdff6a2c3eefdcba4a8b07a797583a3659921a8b085c6e9f388694c1be8 `
  -JobId 2350b9398e755f609457ba94bae8786c `
  -TimeoutSeconds 180
```

上面的 verifier 命令只在该 Job 仍为 `RUNNING` 时有效；当前验收作业已经取消，所以它是证据记录而不是可重复执行指令。同一个 run-id 与目标表不能当作“重放按钮”重复提交。当前 Source 使用 `earliest-offset` 保证隔离验收可复现，但无状态重启到已有表会再次读取历史消息；runner 因此会拒绝任何同名历史作业或非空目标表。需重新验收时使用新 run-id 和隔离表，不能直接向正式表重复写入。

## 动态验收证据

先回放前 1,000 条真实事件，再发送两个未改写业务字段的真实事件：较新的事件推进 watermark，较旧事件验证 late 路由。最终 G2-B 输入为 1,002 条，输出 `clean=1,001`、`late=1`、`dlq=0`。

| 事件 | event_id | 路由与证据 |
| --- | --- | --- |
| 推进 watermark 的真实事件 | `real_e775e9f87a4354e98874842c1b517421949531d9d285aa7e61764c046e479e71` | `clean`；落地 offset `1001` |
| 较旧的真实事件 | `real_1130161eed9ee309c34b25cbfcb5d4233daa67d9ce68d572a0bd8ff50a893ee6` | `late`；原始 offset `1001`，`watermark=1569922803999`，`lateness_ms=22387999` |

Trino 正式表验收结果：

| 断言 | 实测值 |
| --- | --- |
| 总数 / clean / late | `1002 / 1001 / 1` |
| 不同 event_id | `1002` |
| event_id:route 集合 SHA-256 | `0f7a0fdff6a2c3eefdcba4a8b07a797583a3659921a8b085c6e9f388694c1be8` |
| 非法 route | `0` |
| 原始字段 / 派生字段 / 落地坐标违规 | `0 / 0 / 0` |
| clean 诊断字段违规 / late 诊断字段违规 | `0 / 0` |
| 完成 Checkpoint / 最新 ID | `27 / 27` |
| Snapshot 数 / 最新 ID | `3 / 3854376992136224865` |

另对验收隔离表导出 Kafka clean/late 与 Trino 结果，按 `event_id` 逐条比较 17 个源字段：共 17,034 次字段比较，缺失 ID、额外 ID 和字段差异均为 0；事件 ID 集合 SHA-256 为 `1e04bf0eb2c6f072e7dd571fd9b48570563dac20a87e216bff173d615a810342`。原始导出和报告位于被 Git 忽略的 `tmp/graduation/g2c/g2c-20260918a/`，不会上传真实数据。

## 排障记录

- 空 Topic 烟雾测试会创建表但没有 Snapshot，严格验收按预期失败为“表没有已提交 Snapshot”，证明不会把空链路误报为成功。
- 本机旧版 `kafka-python` 不接受 `isolation_level` 参数，动态验收改用 Kafka 容器内置 CLI 以 `read_committed` 核对；已发送的真实 watermark 事件没有重发，也没有构造业务值。
- Flink REST 会长期保留已取消作业。首次提交正式表时，等待逻辑曾把同名历史作业误认为第二个活动 writer；现在等待逻辑只识别活动状态，而提交前门禁会拒绝任何同名历史，避免 `earliest-offset` 重提污染已有表。
- 独立审查进一步补齐 run-id 表名防碰撞、非空目标表拒绝、精确 Job ID、Snapshot 新鲜度、Topic/run-id 一致性和 `event_id:route` 摘要门禁；正式表复查得到 1,002 行且新摘要精确匹配。
- Docker CLI 已迁移到 D 盘；runner 只在当前进程找不到 `docker.exe` 时加入 `D:\DockerProgram\Docker\resources\bin`，不会把依赖重新写回 C 盘。

## 自动化回归与边界

- G2-C PowerShell 行为与失败边界：12 项通过。
- G2-B Java DataStream：30 项通过，并生成 Java 17 fat JAR。
- 真实数据下载、抽样、回放与 Kafka 契约：63 项通过。
- 仓库全量 Python 回归：484 项通过，耗时 170.464 秒。

全量回归首轮有一个第 10.5 章测试超时：在线 Flink 的 `8081` 让原本依赖快速连接失败的 mock 测试误入后续恢复流程。取消验收作业并只停止项目容器后，该单测 4.725 秒通过；安全加固完成后的最终 484 项全量回归通过。没有修改第 10.5 章代码，也没有删除容器、卷或湖表数据。

本轮证明的是 1,002 条真实事件的端到端正确性，不是 2,199,938 条全量吞吐、长期稳定性或跨故障 exactly-once 容量验收。G2-D 可以基于正式明细表实现分层指标与 API；在性能章节再按本机资源分批回放全量数据并记录吞吐、反压、Checkpoint 和存储增长。
