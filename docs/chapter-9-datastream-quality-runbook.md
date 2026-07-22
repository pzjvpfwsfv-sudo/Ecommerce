# 第 9 章 Java DataStream 数据质量治理运行手册

## 1. 当前状态

**Phase B 正式切流已完成，当前三条正式 Flink 作业保持 RUNNING。**

```text
user_behavior_events
        |
        v
Java DataStream Quality Job (production)
  |          |          |
  v          v          v
clean      dlq        late
  |
  +--> Flink SQL -> Doris
  +--> Flink SQL -> Iceberg/MinIO -> Trino -> 第 8 章 API
```

正式 Doris/Iceberg SQL Source 已消费 `user_behavior_clean`。原始 Topic、影子 Topic、旧
raw Source、Checkpoint、Savepoint 和回滚现场均保留。当前 Flink 容器仍把本 worktree
挂载为 `/workspace`，因此在切换容器挂载前不得删除
`.worktrees/chapter-9-datastream-quality`。

## 2. 核心能力与 Exactly-Once 边界

- Java 17 + Flink 1.19.2 DataStream API Fat JAR，负责八字段 JSON 解析、校验、Watermark、24 小时 `event_id` TTL 去重和 clean/DLQ/late 三路输出。
- 三个 Kafka Sink 使用独立事务前缀和 `EXACTLY_ONCE`；本章可以证明 Flink 状态与 Kafka Sink 事务边界内的 Exactly-Once。
- Doris 与 Iceberg 的 SQL 作业使用不同 Kafka Consumer Group：`chapter9-doris-clean-v1` 与 `chapter9-iceberg-clean-v1`。
- Doris Connector 的 2PC 配置、Iceberg 提交和 MinIO/Hive 元数据链路不等同于跨 Flink 状态、Kafka、Doris、Iceberg 的端到端 Exactly-Once；本文只记录实际 connector 与读回证据，不扩大语义声明。
- Watermark 最大乱序 10 秒、空闲分区 30 秒；Checkpoint 间隔 10 秒、超时 60 秒、最小间隔 5 秒、单并发；HashMap State Backend 和固定延迟最多 3 次重启保持不变。

## 3. 常用命令

```powershell
./scripts/build_chapter_9_datastream.ps1
./scripts/run_chapter_9_shadow.ps1
./scripts/verify_chapter_9_shadow.ps1
./scripts/verify_chapter_9_recovery.ps1
./scripts/resize_chapter_9_flink_slots.ps1
./scripts/run_chapter_9_production_cutover.ps1 -TrafficPaused
./scripts/verify_chapter_9_production.ps1
./scripts/rollback_chapter_9_production.ps1 -TrafficPaused -DryRun
python -m unittest discover -s tests -v
```

真实切流后的验收或回滚命令仍需暂停流量；回滚脚本的 `-DryRun` 只渲染 SQL 和审查身份，
不停止、取消或提交任何作业。

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

## 5. Phase A 与 Phase B 真实证据

### 5.1 Phase A 影子与恢复

- Phase A 当时的门禁原文为“影子链路已完成、主链路尚未切换”；这只是历史阶段记录，当前状态已由
  下文 Phase B 正式切流完成记录覆盖，不表示当前仍未切流。
- 影子对账 run：`chapter9-914670168b5c49439fcc215d9a04eaf2`；作业
  `826791363f10d34aa05e88f0722cc45f`；`raw=8`、`clean=2`、`dlq=5`、`late=1`。
- Phase A 还验证了五种 DLQ reason 各 1 次、重复事件在 clean 恰好 1 次、TaskManager
  重启和 Savepoint 恢复后的去重状态。
- TaskManager 恢复测试作业为 `e5dec34fc70c34a276640241f110a3b0`；Savepoint 恢复作业为
  `6f6e24deea18e22722bfd5e0a83895e4`，其后作为正式切流的影子作业。

### 5.2 受控切流 manifest

- 扩容前 Flink 为 2 slots；Task 2 仅使用 `--no-deps --force-recreate flink-taskmanager`，
  扩容后为 1 个 TaskManager、4 个总 slots，保留 1 个可用 slot。
- Cutover ID：`85c971e5-1e96-4c21-8cce-35f25402a543`。
- 影子 Job：`6f6e24deea18e22722bfd5e0a83895e4`，Stop-with-Savepoint 路径：
  `file:/workspace/tmp/savepoints/chapter-9/savepoint-6f6e24-cb1178e80c05`。
- Production DataStream Job：`0d8edd967461402a66e9672d2335ca6d`。
- Doris clean SQL Job：`bf10b31978af0ae53446535c41120870`。
- Iceberg clean SQL Job：`ce7ec8a8d04e70f45f6c7806ed1ede28`。
- 原始 Topic 停流边界为 `partition:0,offset:212`；manifest、回滚 SQL 和 dry-run 使用同一边界。

### 5.3 最终 production 验收

最终文件为 `tmp/chapter-9/production-verification.json`，对应逻辑 run
`chapter9-production-ab626f6106d5462c8212cc15369e9255`。这不是首次一次成功，而是同一
逻辑 run 的分阶段恢复：

1. 初次验证发送前 7 条，watermark REST 数组形状导致 late gate 失败，未发送 late。
2. 修复 REST 数组解包后，`-ResumeRunId` 只发送 1 条 late；随后 API 历史证据超时，保留失败证据。
3. `read_only_finalize` 继续使用已存在的 8/2/5/1 结果，不再发送 Initial 或 Late；idle source 的
   `Int64.MinValue` watermark 以 null 记录，以持久化 late 输出和此前 API gate 失败证据完成收尾，
   该阶段发送次数为 0，最终退出码为 0。

最终质量矩阵：

| 项目 | 结果 |
| --- | ---: |
| raw / clean / DLQ / late | `8 / 2 / 5 / 1` |
| duplicate clean | `1` |
| `DUPLICATE_EVENT` | `1` |
| `MALFORMED_JSON` | `1` |
| `MISSING_REQUIRED_FIELD` | `1` |
| `INVALID_EVENT_TIME` | `1` |
| `FUTURE_EVENT_TIME` | `1` |

最终 Flink Checkpoint baseline -> final：

| 作业 | baseline | final |
| --- | --- | --- |
| production `0d8edd...` | completed `1177`，latest `3570` | completed `1181`，latest `3574` |
| Doris `bf10b3...` | completed `403`，latest `404` | completed `412`，latest `413` |
| Iceberg `ce7ec8...` | completed `1045`，latest `1046` | completed `1055`，latest `1056` |

最终 Kafka Group 证据：production `228/228`、CLI lag `0`、readable lag `0`；Doris clean
`6/7`、CLI lag `1`、readable lag `0`；Iceberg clean `6/7`、CLI lag `1`、readable lag `0`。
两个 clean group 的 offset `6` 都是 `COMMIT` transaction-control record，不是可读业务数据，
因此不能把 CLI lag 1 误报为数据积压，也不能把它改写成 CLI lag 0。

重复事件的同一 `event_id` 在 clean 保留 1 次、在 duplicate DLQ 保留 1 次；Trino 契约要求
四个 validation ID 加 late ID 在 Iceberg 中 absent，并单独测得 duplicate Iceberg count 为 1。
下游最终读回：Doris baseline/final 均为 `pv=2, uv=2`；Trino recovered baseline 为 815，
最终 event_count 为 817，精确 clean 事件为 `2/2/2`（event/distinct-event/distinct-user），
四个 validation ID 加 late ID 排除数为 0；第 8 章 API 返回
historical event_count `817`、realtime `pv/uv=2/2`、`rule_based`、空 warnings。
API 的 offset-free `updated_at` 按 UTC 解释。

## 6. 真实排障记录

### 6.1 切流前基础设施与 connector

- 初次 SQL Client 是空 connector 挂载，随后补齐并逐个加载 Kafka、Doris、Hive、Iceberg、Hadoop
  和 AWS JAR；每个文件使用 SHA-256 校验、`.partial` 临时文件和原子移动，最终从已验证的
  `tmp/chapter-9/lib` 缓存装载。缺失 Hadoop 类和 HiveCatalog classloader 冲突通过补齐依赖及
  `HADOOP_CLASSPATH`、`classloader.resolve-order=parent-first` 修复。
- 活跃 MinIO 曾挂载旧 worktree 的 Chapter 8 数据，Chapter 9 对象缺失。恢复过程复制
  2,355 个文件、153,733,809 bytes，逐项保留并核验数据后只重建 MinIO；没有删除业务数据。
- Hive 表指针曾指向缺失的 `00009`/`00008` metadata。先保留原表参数，再恢复到存在 manifest list
  的 `00336`/`00335` 指针；Trino 随后恢复到 813 行、63 个 distinct event ID 的可读基线。

### 6.2 Flink 与验收过程

- Flink `/jobs/overview` 保留历史同名 job，旧 CANCELED Doris job 与新 job 同名；验收器改为只用
  RUNNING job 做唯一性判断，同时仍要求 manifest Job ID/name 精确匹配。
- Doris clean 初次使用 `earliest-offset` 重放既有两条 clean 记录；取消后用 runtime-specific
  offset `partition:0,offset:4` 重新生成 SQL 并启动 `bf10b319...`，将累计结果刷新回精确 `2/2`。
- Watermark REST 返回数组时 PowerShell 5.1 把响应包装成嵌套 `Object[]`；修复为先保存响应再枚举
  321 条 metric。后续 idle source 返回 `Int64.MinValue`，不伪造 live watermark，改以 late topic
  持久化 `late/clean/DLQ=1/0/0` 证据完成只读收尾。
- Kafka 的 exactly-once control offset 被显式分类为 `COMMIT`，故两个 clean group 保留 CLI lag 1
  但 readable lag 0。Trino 曾因 OOM 退出且旧 Chapter 8 worktree 挂载失效；只重建无状态 Trino
  容器并使用当前 catalog，未重建 Kafka、Flink、Doris、Iceberg 或生产 DataStream。
- API 曾返回无 offset 的 Doris `updated_at`；验收器按 UTC 解释，保留严格 freshness 判断。

### 6.3 回滚审查与 dry-run

Task 5 的 I1-I4 review debt 已修复并复审通过：I1 要求 production stop 为 `FINISHED` 且解析唯一
Savepoint；I2 严格校验 manifest schema、ISO 时间、Job ID、路径和 offset；I3 要求提交后 REST `jid`
与请求 Job ID 相等；I4 增加 fail-closed waiter、顺序和零 mutation 行为测试。

回滚 dry-run 使用 manifest 的 `partition:0,offset:212`，生成 Doris/Iceberg raw SQL，使用隔离的
rollback group；没有调用 stop、cancel 或 submit。dry-run 前后 production
`0d8edd...`、Doris `bf10b3...`、Iceberg `ce7ec8...` 均保持 RUNNING，说明回滚入口可审查但未执行真实回滚。

### 6.4 合并前恢复性加固

- cutover partial manifest 升级为阶段化状态：每个 Stop-with-Savepoint 和 SQL Job submission 都先
  原子写 intent，再写结果；`-ResumePartial` 可从 savepoint-only、production-only、Doris-only 和
  提交结果丢失窗口继续，并且只能领养唯一 exact-name RUNNING Job。历史 terminal 同名作业不再阻塞，
  多个 RUNNING 候选或顶层 ID 冲突仍 fail closed。
- rollback 使用 `tmp/chapter-9/rollback-progress.json` 持久化 stop、cancel、submit 阶段，真实回滚中断后
  必须显式 `-Resume`；已完成阶段不会重复提交，缺失阶段会先写 intent 再执行。dry-run 不创建 progress，
  也不调用 stop、cancel 或 submit。
- verifier 在发送前持久化原始 batch start、Doris/Trino/Checkpoint baseline，并逐阶段保存 output、group、
  checkpoint、Doris、Trino 和 API 证据。read-only finalize 必须复用同一 run 的 durable evidence，
  Doris/API 必须与直接查询完全一致，Trino 总量和 distinct event ID 必须严格为 baseline `+2`。
- 合并前最终回归为 Phase B `55/55`、全量 Python `165/165`、Java JUnit `15/15`；四个 PowerShell
  脚本 Parser/ASCII 与 `git diff --check` 均通过，最终审查 P0/P1/P2 为 0。加固阶段没有再次执行
  resize、cutover、verifier、真实 rollback 或事件发送。

## 7. 运行边界与回滚

- 任何真实回滚先暂停流量，按 manifest 精确核验 Job ID/name/state，再执行 production
  Stop-with-Savepoint、clean 作业取消和 raw SQL 提交；不删除 Topic、表、状态、Checkpoint、Savepoint
  或容器，不自动启动生成器。
- 回滚只恢复 raw Source 消费入口，不承诺自动撤销切流窗口已经写入 Doris/Iceberg 的数据；需要单独补偿和审计。
- 影子 Topic 不复用为正式 Topic；原始 Topic 和旧 SQL Source 仍是显式回滚入口。

## 8. 面试表达

“影子对账通过后，我先发现本地 Flink 只有 2 个 slots，先只重建 TaskManager 扩到 4 slots，再暂停流量、核对 raw offset 212、保存影子状态并恢复 production。过程中处理了错误 connector 挂载、旧 worktree 的 MinIO/Hive 指针、Flink 历史同名 job、Doris earliest 重放、Watermark REST 数组和 idle watermark、Kafka control offset、Trino OOM 以及 API UTC 时间。最终同一逻辑 run 通过 late-only 恢复和 read-only finalize 得到 8/2/5/1，Doris 2/2、Trino 817 和 API 证据；回滚 dry-run 验证了可回退边界，但没有把 Doris/Iceberg 宣称成跨系统端到端 Exactly-Once。”
