# G5-A 真实行为全样本分级验收

## 当前状态

截至 2026-10-08，2,199,938 条 REES46 历史事件的源文件已在本机重新核验：1,045,479,893 字节，SHA-256 为 `18a3202d380c8f6c53ad767ec2043d0717df1d3604d2211639bb0fc4699a5437`。**1 万轮真实 Kafka → Flink → Iceberg → Trino 对账通过；10 万轮只发送 50,000 条后因主机可用物理内存降至约 0.61 GiB 停止，未通过；全样本未启动。**没有发布 `stable-user-2pct-full`，旧 1,002 条 Doris 发布和表未删除或覆盖。试验结束后项目容器已停止，Topic、湖表、断点和卷保留。

### 本次实测

| 阶段 | 结果 | 可核对证据 |
| --- | --- | --- |
| 10,000，`g5-a10k-01` | `PASS` | 回放确认和 raw offset 均为 10,000，G2-B lag 0；Kafka `read_committed` clean 10,000，late/DLQ 0；Trino 总数与不同 `event_id` 均为 10,000，Kafka/Trino `event_id:route` 摘要一致，五类字段违规均为 0；验收时 G2-C 已完成 29 次 Checkpoint，Iceberg 有 15 个 Snapshot。详见本机 `tmp/graduation/g5/pilot-10000.json` 与 `tmp/graduation/g2c/g5-a10k-01/verification.json`。 |
| 100,000，`g5-a100k-01` | `BLOCKED` | 独立身份预检通过，MinIO 断点前缀为空；第一段按断点确认 50,000，raw offset 50,000、G2-B lag 0。Windows 可用物理内存最低观测约 0.61 GiB，D 盘最低观测余量约 32.59 GiB。未执行后 50,000 条，也未在停机前做该轮 Trino 表对账；两个作业已取消，不允许无状态重提写入非空表。本机报告为 `tmp/graduation/g5/pilot-100000.json`。 |
| 2,199,938，`g5-full-01` | `NOT_RUN` | 10 万轮未通过，缺少实际 `d_delta_bytes`，容量门禁不能推算；Doris/API/六模块的全样本验收与性能 P95 均未验证。 |

本机 Windows 保留了宿主端口 8081、8088，试验分别使用 `FLINK_REST_PORT=8334`、`TRINO_PORT=8333`；不要改容器内部端口。现有 Hive PostgreSQL 已有 3.1.0 schema，重建 Metastore 时必须在当前 PowerShell 进程设置 `HIVE_METASTORE_IS_RESUME=true`，否则镜像会重复执行建表并退出。G2-C runner/verifier 已改为不自动重建运行中的服务；仍须在启动前核对 Compose 参数。Doris BE 默认宿主端口 8040 也落在本机保留范围，本轮未启动 Doris，后续发布前须单独处理并验证。

## 三轮身份与门禁

| 阶段 | 默认 run ID | 用途 | 发布限制 |
| --- | --- | --- | --- |
| 10,000 | `g5-a10k-01` | 字段、路由、窗口和 Snapshot 核验 | 不发布全样本指标 |
| 100,000 | `g5-a100k-01` | 稳态、暂停恢复、磁盘增长测量 | 不发布全样本指标 |
| 2,199,938 | `g5-full-01` | 完整 61 天入湖、Trino/Doris/API/页面验收 | 全部门禁通过后才发布 |

每轮必须使用未使用过的 run ID、`real_behavior_events_v1_<run>` 原始 Topic、对应 clean/late/DLQ Topic、`real_behavior_detail_v1_<run 下划线形式>` Iceberg 表、独立回放断点和 `s3a://flink-state/checkpoints/graduation-g2b/<run>`。不要把旧 run ID、Topic、Checkpoint 或已非空的表当作“重试按钮”。回放脚本保证已确认位置续传，但 Kafka 仍可能至少一次投递，Flink 去重状态默认处理时间 TTL 为 24 小时，不是永久跨重放幂等或端到端 exactly-once。

## 操作顺序

1. 在当前 PowerShell 进程把 `TEMP`/`TMP` 设为 `D:\EcommerceDev\temp`，`PIP_CACHE_DIR` 设为 `D:\EcommerceDev\cache\pip`；前端运行时将 `npm_config_cache` 设为 `D:\EcommerceDev\cache\npm`。复现本机运行还需设置 `TRINO_PORT=8333`、`FLINK_REST_PORT=8334`，复用现有 Hive 元数据库时设置 `HIVE_METASTORE_IS_RESUME=true`。原始数据、运行报告和 Checkpoint 均留在 D 盘。发现服务端口时使用 `docker port <容器名> <容器端口>/tcp`，不要假定上次的宿主端口仍有效。
2. 只启动本轮必要的 Kafka、Flink、MinIO、Hive/Trino 服务，核对现有卷和数据不被重建。对目标 run ID 运行 `pwsh -File scripts/g5_behavior_preflight.ps1 -Stage 10000 -RunId g5-a10k-01`（后两轮改阶段和 run ID）。`BLOCKED` 时先按 `stop_reasons` 排障，不发送数据。首次提交前还须只读检查 MinIO 中对应的 Flink Checkpoint 前缀不存在；预检脚本只自动检查本地回放断点、Kafka Topic、Iceberg 表和 Flink Job 历史，**不把远端 S3 Checkpoint 标成已核验**。
3. 分别创建本轮四个隔离 Topic，核对 Kafka 内部地址和事务副本/ISR。通过 `scripts/build_chapter_9_datastream.ps1` 的 Java 17 Docker 环境构建 JAR 后，以 `com.ecommerce.quality.real.RealDataQualityJob` 启动 G2-B，参数含本轮 `--run-id`、`--input-topic`、`--checkpoint-uri` 和容器内 Kafka 地址。确认 Job `RUNNING`、Checkpoint 可用后，用 `scripts/run_g2c_real_event_lakehouse.ps1 -RunId <run>` 启动 G2-C 的单 writer；该脚本在提交前拒绝历史 Job 和非空目标表。先查 `-PlanOnly` 输出，不使用旧表覆盖参数。
4. 用 `D:\EcommerceDev\venv\Scripts\python.exe -m generators.real_data.replay --input data/rees46/oct-nov-users-2pct-verified.jsonl --checkpoint tmp/graduation/g5/replay-<run>.json --mode kafka --bootstrap-servers <实际宿主地址> --topic real_behavior_events_v1_<run> --max-events <本轮条数> --rate <已测稳定速率>` 回放真实事件。暂停后只用同一身份/断点续传；不要从 earliest 重新写入非空湖表。100 条/秒是工具默认值，不是容量承诺。
5. 记录 raw Topic 确认和 offset，等待 Kafka 消费 lag 归零，再等待 Flink 的完成 Checkpoint 与 Iceberg 新 Snapshot。发送结束不等于落湖完成。用 `scripts/verify_g2c_real_event_lakehouse.ps1` 输入实际 clean/late 计数、`event_id:route` 集合 SHA、Job ID 验证；不能填预期值冒充实测。Trino 另外查总数、`count(DISTINCT event_id)`、业务日期 `2019-10-01` 至 `2019-11-30`。如果重复、缺失、DLQ 非法业务事件、TTL 超时或恢复点丢失，停止正式轮次并调查，不删表/卷/Topic 凑通过。
6. 1 万轮通过后，按实测结果在忽略目录 `tmp/graduation/g5/pilot-10000.json` 记录 `status`、`stage`、`run_id`、`source_sha256`、核验计数与资源指标；10 万轮同理写 `pilot-100000.json`，并记录 Docker 所在 D 盘的真实增量字节 `d_delta_bytes`。只有实际证据为 `PASS` 才能继续。全样本门禁要求 C≥5 GiB、D≥20 GiB，且 D 余量≥`1.5 × 10万轮增量 × (2,199,938/100,000) + 5 GiB`。若不足，停机，不通过删旧卷或旧发布绕过。
7. 只有完整样本入湖对账均为 2,199,938，才运行 `scripts/refresh_g2d_behavior_metrics.ps1 -DataScope stable-user-2pct-full -SourceRunId g5-full-01`。核对四张 Doris 指标表及 publication 回读、`source_table`、Snapshot、回放时间；运行 `scripts/verify_g2d_behavior_metrics.ps1 -ExpectedDataScope stable-user-2pct-full -ExpectedSourceRunId g5-full-01`。旧发布 `behavior-v1-s881836466779140976` 应仍为 1,002 条并绑定 `real_behavior_detail_v1`。最后检查 FastAPI 四类行为响应、六模块桌面/手机页面、证据抽屉和正式 Compose API/身份库。性能报告记录 P95、任务时长、CPU/内存/磁盘峰值；未实测一律写“未验证”。

## 证据边界

原始 Topic offset、Flink Job/Checkpoint、clean/late/DLQ、Trino Snapshot、Doris 指标哈希、容量与浏览器截图保存在被忽略的 `tmp/graduation/g5/`，Git 只保存不含原始业务记录的汇总。业务时间是 2019 年历史事件，`replayed_at` 是本机回放时间，`calculated_at` 是指标计算时间，三者不能混用。Olist 订单域独立，不能按两源 ID 拼接；没有订单支付、退款、库存等源字段的 REES46 分析不得编造。

下一次 10 万轮需使用新的 run ID，并先解决主机内存余量，再从零提交隔离作业。现有 `g5-a100k-01` 的 50,000 条及其湖表不能当作新 run 的基础，更不能用同名无状态 writer 继续追加。只有新的 10 万轮完成对账并量得真实 D 盘增量，才能判断 Docker 数据仍在 D 盘时是否足以承载 2,199,938 条；E 盘有空闲空间不会自动扩大 D 盘 Docker 数据容量。

## 回归与未验证项

本次 G2-C/G2-D/G5 与行为 API 聚焦回归 100 项通过；前端 Vitest 40 项通过，类型检查和生产构建通过（仍有现存的大包提示）。完整 Python 回归在最后一条独立加载回归测试加入前运行 705 项，3 个失败、1 个错误、1 个跳过；4 个不通过项仍是未改动的第 10.5 章 Windows 冷启动/子进程超时测试，与本阶段开始前的基线相同，不能称全量测试通过。全样本只读预检返回 `BLOCKED`，原因为 10 万轮容量证据失败且没有 `d_delta_bytes`。正式 Doris/API/浏览器六模块的全样本验收和 P95 均未执行。
