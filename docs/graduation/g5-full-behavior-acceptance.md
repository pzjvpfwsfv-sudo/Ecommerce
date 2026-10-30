# G5-A 真实行为全样本分级验收

## 当前状态

原始源文件于 2026-10-10 核验：1,045,479,893 字节，SHA-256 为 `18a3202d380c8f6c53ad767ec2043d0717df1d3604d2211639bb0fc4699a5437`。截至 2026-10-30，**1 万轮、10 万轮和 2,199,938 条全样本的数据链路对账通过；全样本 Doris 指标已发布，正式 Compose 的真实登录与桌面/手机六模块浏览器验收通过。**并发压测、RAG/Agent 的真实浏览器验收仍未执行，不能称整体验收完成。旧 1,002 条 Doris 发布保留。各轮已取消的 writer 不可无状态重提；Topic、湖表、断点和卷保留。

### 本次实测

| 阶段 | 结果 | 可核对证据 |
| --- | --- | --- |
| 10,000，`g5-a10k-01` | `PASS` | 回放确认和 raw offset 均为 10,000，G2-B lag 0；Kafka `read_committed` clean 10,000，late/DLQ 0；Trino 总数与不同 `event_id` 均为 10,000，Kafka/Trino `event_id:route` 摘要一致，五类字段违规均为 0；验收时 G2-C 已完成 29 次 Checkpoint，Iceberg 有 15 个 Snapshot。详见本机 `tmp/graduation/g5/pilot-10000.json` 与 `tmp/graduation/g2c/g5-a10k-01/verification.json`。 |
| 100,000，`g5-a100k-01` | `BLOCKED` | 首次并行尝试只确认 50,000 条；内存降至约 0.61 GiB 后取消 G2-B/G2-C。该 run 的 writer 已提交，不能无状态重提到非空表。本机报告为 `tmp/graduation/g5/pilot-100000-attempt-01.json`。 |
| 100,000，`g5-a100k-02` | `PASS` | 原 100,000 条 raw/clean 及 G2-B 证据未变。10 月 10 日先核对目标表/MinIO 前缀不存在、Flink writer 历史为空、clean 最早 offset 为 0，再**不重放 raw**提交单 writer。Trino 总数与不同 `event_id` 均为 100,000，日期为 `2019-10-01` 至 `2019-10-04`，Kafka/Trino `event_id:route` 摘要一致，五类字段违规均为 0；G2-C 已完成 18 次 Checkpoint，Iceberg 有 3 个 Snapshot。本机报告为 `tmp/graduation/g5/pilot-100000.json` 和 `tmp/graduation/g2c/g5-a100k-02/verification.json`。 |
| 2,199,938，`g5-full-01` | 数据链路、Doris 发布、真实登录与六模块浏览器 `PASS` | 原始回放确认 2,199,938；因暂停续传出现 179 条重复原始投递，raw 末尾 offset 2,200,117。G2-B `read_committed` clean 2,199,938、late 0、DLQ 179（均为 `DUPLICATE_EVENT`），原始消费 lag 0。Trino 湖表总数与不同 `event_id` 均为 2,199,938，业务日期为 `2019-10-01` 至 `2019-11-30`，Kafka/湖表 `event_id:route` 流式摘要同为 `9b998a5c8b18653e7ab123a5628fe3d6c812cb76370b956405835c22720b9696`，五类字段违规均为 0。Iceberg 当前 Snapshot `5556722256660191067` 已发布为 `behavior-v1-s5556722256660191067`；Doris 行数为概览 62、漏斗 62、维度 832,012、质量 1。正式 Compose API 登录、质量、证据抽屉、注销及桌面/手机六模块 Playwright 6/6 通过。 |

全样本 G2-B 初次 Job `be7e114322fcf12b24da8beebf89609a` 在约 206.5 万条时因默认 TaskManager Java 堆不足失败；保留外部 Checkpoint `chk-1410`，仅将 TaskManager 进程内存临时调至 3072m 后，从该状态恢复为 Job `c126352f9952d20cf8e663a87d608e5d`。恢复后有 4 次启动期 Checkpoint 失败，随后稳定完成并消费至 lag 0；不能写成“0 次失败”。G2-C 单 writer Job `7b97bb1c21e1ab71e034ae3dbb670a70` 完成 23 次 Checkpoint、失败 0 次，核验后取消。Trino 与 Doris 同时空载时 Windows 可用物理内存已降至约 0.8 GiB，因此没有启动全量指标计算。只读基数探测约为 110,239 个商品、614,811 个“日期 × 商品”组合，说明原脚本的内存路径不适用于此规模，不能把近似基数当精确发布行数。

G2-D 先前只读探测选中了 Snapshot `5556722256660191067`，来源身份返回总数/去重数均 2,199,938、窗口为 `2019-10-01` 至 `2019-11-30`、回放时间为 `2026-10-10T03:28:51.807Z` 至 `2026-10-10T07:42:16.420Z`。概览 SQL 返回 62 行（61 个 DAY、1 个 FULL），FULL 计数为 view 2,086,366、cart 80,023、purchase 33,549、去重用户 107,000、会话 460,894、商品 107,443，购买金额仅为数据集价格代理值 10,262,860.60，不能称真实成交额。原漏斗 SQL 曾因 `PagesToPagesIndexes=1.43GB` 触及 1.85GB 单节点内存上限；这是优化前故障记录，不是本次发布结果。

## 2026-10-30 全样本分阶段发布

将漏斗的逐事件 `row_number()` 改为按会话分组取首个有序事件，使用同一 10 万条 Snapshot 对比原 SQL 的 4 个 DAY 和 1 个 FULL 结果，五行完全一致。优化后全样本漏斗返回 62 行；FULL 为缺失会话 0、浏览会话 460,659、浏览后加购 46,113、完成购买 18,746。维度 SQL 本身可流式输出 832,012 行，但不能交给旧 PowerShell 刷新器的全量内存数组。新增的 `scripts/g2d_full_stream.py` 逐行审计、在 D 盘 SQLite 临时索引检查重复键及 DAY/FULL 明细对账，计算与旧发布口径一致的规范摘要；`scripts/refresh_g2d_full_staged.ps1` 将 Trino 计算与 Doris 写入错峰执行。

`Prepare` 对原 Snapshot 校验 2,199,938 条身份后，生成概览 62、漏斗 62、维度 832,012、质量 1 的候选文件，跨维度汇总与概览一致，写入忽略目录 `tmp/graduation/g2d/behavior-v1-s5556722256660191067/staged-full.json`，随后停止 Trino。`Publish` 校验候选文件 SHA，逐表 Stream Load 并回读对照行数和规范摘要，最后才插入正式元数据。正式 `source_table=real_behavior_detail_v1_g5_full_01`、Snapshot `5556722256660191067`、源事件数 2,199,938；四表摘要为概览 `b0cd3d83620adf6d68fb9eb4744357a5ccb85137209693193f3a96a674ec18aa`、漏斗 `78b98351b5d6a62c140f387cb3d1fc9710a39e8ada734a517e4279a89c7d9f1e`、维度 `e689ccd8a1cd3882e50d9fcd33cafe3e269b7d31707501847fde0e39bd4e2f73`、质量 `0dee07e6a39c48b24e6d5ebcc05ab4e279fe3f53ddf5c4943c86410d93a1041e`。独立 Doris 查询确认新旧两个 `PUBLISHED` run 并存，旧 run 仍为 1,002 条源事件。完整本机报告为同目录 `refresh-report.json`。

首次准备因 Trino `/v1/info` 已响应但查询引擎尚未初始化而失败，改用实际 `SELECT 1` 就绪探针后成功。首次发布在前三张表写入后，维度回读命令被 PowerShell 将裸 `-h127.0.0.1` 误传为主机 `127`，故正式元数据仍为 0；修正为 `--host=127.0.0.1` 后只读核对前三表行数，续跑时逐表校验并跳过已匹配候选，补质量表后发布。该故障不是 Doris 进程退出或数据丢失。Docker Desktop 启动前另有残留 `sailor-ingest.sock`，仅在 Desktop/backend 完全停止后隔离 0 字节运行时 Socket 目录；没有执行 factory reset 或删除卷。

测试身份覆盖认证依赖的本机 FastAPI `TestClient` 连真实 Doris，`publication`、FULL 概览/漏斗、商品购买排行、质量、定义目录六端点均为 HTTP 200，前五端点均指向新 run；不带身份时为 401。每端点预热后顺序请求 20 次的本机数据路径 P95 分别为 46.2、64.6、69.6、81.8、64.5、2.0 ms；不是浏览器、网络或并发负载 P95。

同日补做正式 Compose：建立独立持久的 `app-postgres-g4-data` 卷，确认 `pgvector 0.8.7`、身份表和知识库表，应用 `003_agent_reports.sql`，创建本机管理员。真实 HTTP 检查未登录为 401、管理员登录与 `/auth/me` 成功、Olist 与 REES46 均返回已发布 run、带 CSRF 注销后为 401。容器首次启动暴露知识库模块的项目根目录假设不适用于 `/app`，已修正并加测试；管理员初始化命令须带 `PYTHONPATH=/app`，脚本挂载已移至 `/opt` 避免在 API 源码目录生成占位文件。重建新容器后再次核验真实登录，FULL 来源事件 2,199,938 与 61 个 DAY 窗口属于同一 run。Playwright 在 1440×900 与 390×844 下完成登录、Olist 全量切换、证据抽屉、质量页、注销和六个已发布指标模块的无横向溢出检查，重建后再次 6/6 通过。REES46 全量漏斗仍取 FULL 汇总，走势单独取同一 run 的 61 个 DAY 点；延时截图确认曲线可见。Python 聚焦回归 62 项通过、1 项跳过；前端 Vitest 50 项通过，类型检查与生产构建通过，仍有单包超过 500 kB 的提示。以上不包含知识库检索/Agent 的真实浏览器流程或并发性能结论。

本机 Docker 卷在 D 盘，但 Windows 自动管理的 C 盘分页文件在查询与服务运行时扩容，C 盘曾短时降至约 0.68 GiB。按已核对路径把旧下载、索引、网页缓存原样归档到 `D:\EcommerceDev\c-drive-cache-archive\2026-10-30`；未移动 Docker 数据卷、用户文档或运行中的 Codex 缓存。停止计算/服务容器并归档后，观察到 C 盘约 6.39 GiB、D 盘约 38.4 GiB 空闲；未来运行前必须重新测量，不将此值视为永久保证。

10 万轮的 run 专属持久化量为 234,617,269 字节：Kafka 四个 Topic 目录 `du -sb` 合计 216,519,449 字节，MinIO 中 G2-B Checkpoint 为 11,110,533 字节，Iceberg 表为 6,987,287 字节。这个逐项实测值用作 `d_delta_bytes` 容量输入，**不是** Windows D 盘空闲量的净变化；只续跑 G2-C 时，D 盘空闲减少 74,133,504 字节，不能拿它冒充全链路增量。按计划的 1.5 倍线性外推、另加 5 GiB 余量及最低 20 GiB 门槛计算，全样本启动前需保留 20 GiB；当时实测 D 盘约 34.82 GiB、C 盘约 5.91 GiB，静态磁盘门禁通过。全样本数据链路验收后 D 盘约 31.12 GiB、C 盘约 5.84 GiB；发布前仍须重新检查。C 盘安全余量不足 1 GiB；Trino 与 Doris 同时空载时 Windows 可用物理内存约 0.8 GiB，不能直接运行全量指标计算。

本机 Windows 保留了宿主端口 8081、8088，试验分别使用 `FLINK_REST_PORT=8334`、`TRINO_PORT=8333`；不要改容器内部端口。现有 Hive PostgreSQL 已有 3.1.0 schema，重建 Metastore 时必须在当前 PowerShell 进程设置 `HIVE_METASTORE_IS_RESUME=true`，否则镜像会重复执行建表并退出。G2-C runner/verifier 已改为不自动重建运行中的服务；仍须在启动前核对 Compose 参数。本次 Doris BE 的宿主 8040 端口可用，FE/BE 均可启动；发布前仍须重新发现端口及容量。

## 本机演示入口

网页地址为 `http://127.0.0.1:8000/app/`。仅在本机 C/D 盘余量足够时，按顺序启动现有持久容器（不会重建卷）：

```powershell
docker start ecommerce-lakehouse-ai-app-postgres ecom-doris-fe ecom-doris-be
docker start ecom-api
```

管理员账号及口令在本机 `D:\EcommerceDev\secrets\ecommerce-graduation-credentials.json`，由当前 Windows 账号的 DPAPI 加密，**不在 Git 中**。以下命令只在自己的 PowerShell 终端显示登录信息，不要分享终端截图或把口令写入文档：

```powershell
$login = Get-Content -LiteralPath 'D:\EcommerceDev\secrets\ecommerce-graduation-credentials.json' -Raw | ConvertFrom-Json
$login.username
[System.Net.NetworkCredential]::new('', (ConvertTo-SecureString $login.admin_password_dpapi)).Password
```

若将来需要 `docker compose` **重建** API/数据库容器，先按 G3 手册设置原有数据库口令，不能把新口令当作已有卷的密码。当前容器已使用 `WEB_DIST_DIR=/web`，静态页面来自 `apps/web/dist`；修改前端后需重新执行 `npm run build`。演示结束可 `docker stop ecom-api ecom-doris-be ecom-doris-fe ecommerce-lakehouse-ai-app-postgres`，保留数据卷。

## 三轮身份与门禁

| 阶段 | 本次 run ID（均已使用） | 用途 | 发布限制 |
| --- | --- | --- | --- |
| 10,000 | `g5-a10k-01` | 字段、路由、窗口和 Snapshot 核验 | 不发布全样本指标 |
| 100,000 | `g5-a100k-02` | 稳态、暂停恢复、磁盘增长测量；该 ID 已用 | 不发布全样本指标 |
| 2,199,938 | `g5-full-01` | 完整 61 天入湖、Trino/Doris/API/页面验收 | 全部门禁通过后才发布 |

每轮必须使用未使用过的 run ID、`real_behavior_events_v1_<run>` 原始 Topic、对应 clean/late/DLQ Topic、`real_behavior_detail_v1_<run 下划线形式>` Iceberg 表、独立回放断点和 `s3a://flink-state/checkpoints/graduation-g2b/<run>`。不要把旧 run ID、Topic、Checkpoint 或已非空的表当作“重试按钮”。回放脚本保证已确认位置续传，但 Kafka 仍可能至少一次投递，Flink 去重状态默认处理时间 TTL 为 24 小时，不是永久跨重放幂等或端到端 exactly-once。

## 操作顺序

1. 在当前 PowerShell 进程把 `TEMP`/`TMP` 设为 `D:\EcommerceDev\temp`，`PIP_CACHE_DIR` 设为 `D:\EcommerceDev\cache\pip`；前端运行时将 `npm_config_cache` 设为 `D:\EcommerceDev\cache\npm`。复现本机运行还需设置 `TRINO_PORT=8333`、`FLINK_REST_PORT=8334`，复用现有 Hive 元数据库时设置 `HIVE_METASTORE_IS_RESUME=true`。原始数据、运行报告和 Checkpoint 均留在 D 盘。发现服务端口时使用 `docker port <容器名> <容器端口>/tcp`，不要假定上次的宿主端口仍有效。
2. 只启动本轮必要的 Kafka、Flink、MinIO、Hive/Trino 服务，核对现有卷和数据不被重建。对**新**目标 run ID 运行 `pwsh -File scripts/g5_behavior_preflight.ps1 -Stage 10000 -RunId <new-run-id>`（后两轮改阶段）。上表三个 ID 均已使用，不可当作新执行命令。`BLOCKED` 时先按 `stop_reasons` 排障，不发送数据。首次提交前还须只读检查 MinIO 中对应的 Flink Checkpoint 前缀不存在；预检脚本只自动检查本地回放断点、Kafka Topic、Iceberg 表和 Flink Job 历史，**不把远端 S3 Checkpoint 标成已核验**。
3. 分别创建本轮四个隔离 Topic，核对 Kafka 内部地址和事务副本/ISR。通过 `scripts/build_chapter_9_datastream.ps1` 的 Java 17 Docker 环境构建 JAR 后，以 `com.ecommerce.quality.real.RealDataQualityJob` 启动 G2-B，参数含本轮 `--run-id`、`--input-topic`、`--checkpoint-uri` 和容器内 Kafka 地址。确认 Job `RUNNING`、Checkpoint 可用后，视内存余量选择先回放并核验 G2-B，再错峰启动 G2-C 单 writer；也可在资源足够时并行。G2-C 脚本在提交前拒绝历史 Job 和非空目标表；先查 `-PlanOnly` 输出，不使用旧表覆盖参数。
4. 用 `D:\EcommerceDev\venv\Scripts\python.exe -m generators.real_data.replay --input data/rees46/oct-nov-users-2pct-verified.jsonl --checkpoint tmp/graduation/g5/replay-<run>.json --mode kafka --bootstrap-servers <实际宿主地址> --topic real_behavior_events_v1_<run> --max-events <本轮条数> --rate <已测稳定速率>` 回放真实事件。暂停后只用同一身份/断点续传；不要从 earliest 重新写入非空湖表。100 条/秒是工具默认值，不是容量承诺。
5. 记录 raw Topic 确认和 offset，等待 Kafka 消费 lag 归零，再等待 Flink 的完成 Checkpoint 与 Iceberg 新 Snapshot。发送结束不等于落湖完成。用 `scripts/verify_g2c_real_event_lakehouse.ps1` 输入实际 clean/late 计数、`event_id:route` 集合 SHA、Job ID 验证；不能填预期值冒充实测。Trino 另外查总数、`count(DISTINCT event_id)`、业务日期 `2019-10-01` 至 `2019-11-30`。如果重复、缺失、DLQ 非法业务事件、TTL 超时或恢复点丢失，停止正式轮次并调查，不删表/卷/Topic 凑通过。
6. 1 万轮通过后，按实测结果在忽略目录 `tmp/graduation/g5/pilot-10000.json` 记录 `status`、`stage`、`run_id`、`source_sha256`、核验计数与资源指标；10 万轮同理写 `pilot-100000.json`，并记录容量输入 `d_delta_bytes` 的测量方法。若有同一轮开始与结束的 D 盘空闲量，记录净变化；若因中断续跑缺少全程基线，应逐项量取该 run 在 Kafka 与 MinIO 的持久化字节数，并把局部 D 盘变化单列，不混称为全程净增长。只有数据链路实际证据为 `PASS` 才能继续。全样本门禁要求 C≥5 GiB、D≥20 GiB，且 D 余量≥`1.5 × 10万轮容量输入 × (2,199,938/100,000) + 5 GiB`。若不足，停机，不通过删旧卷或旧发布绕过。
7. 完整样本已入湖并通过分阶段发布，**不要直接运行**旧的 `scripts/refresh_g2d_behavior_metrics.ps1 -DataScope stable-user-2pct-full -SourceRunId g5-full-01`，也不要把仍会全量读取维度表的旧 `verify_g2d_behavior_metrics.ps1` 用于该 run。复现时先核对 C≥5 GiB、D≥20 GiB，保持 MinIO/Hive Metastore 可用，运行 `pwsh -File scripts/refresh_g2d_full_staged.ps1 -Phase Prepare -SourceRunId g5-full-01 -PythonExecutable <Python 路径>`；确认 Trino 已停、候选清单为 `PREPARED`，释放计算依赖，再运行同脚本的 `-Phase Publish`。已发布的 run 不得重复覆盖。正式 Compose 登录与指标页面已验收；后续并发性能、RAG/Agent 真实浏览器链路未实测项仍应明确标为“未验证”。

## 证据边界

原始 Topic offset、Flink Job/Checkpoint、clean/late/DLQ、Trino Snapshot、Doris 指标哈希和容量报告保存在被忽略的 `tmp/graduation/g5/`；浏览器截图在 D 盘 `EcommerceDev/temp/g3-playwright/` 和 `EcommerceDev/temp/behavior-daily-trend-verified.png`。Git 只保存不含原始业务记录的汇总。业务时间是 2019 年历史事件，`replayed_at` 是本机回放时间，`calculated_at` 是指标计算时间，三者不能混用。Olist 订单域独立，不能按两源 ID 拼接；没有订单支付、退款、库存等源字段的 REES46 分析不得编造。

`g5-a100k-01` 和 `g5-a100k-02` 的 writer 都已提交过且取消，不可重提；`g5-a100k-02` 的表已非空。全样本必须使用新 run ID、Topic、Checkpoint 和湖表，先只读预检，再按内存余量分阶段运行：质量作业与回放完成并保存 Kafka/Checkpoint 证据后，释放不再需要的作业；G2-C 提交前短时启动 Trino 核对空表，提交后可停 Trino，待 Checkpoint/Snapshot 稳定再短时启动 Trino 对账。每阶段重新检查 C≥5 GiB、D≥20 GiB 和内存，不删旧表/卷/Topic 绕过门禁。E 盘空闲不会自动扩大 D 盘 Docker 数据容量。

## 回归与未验证项

截至 10 月 8 日，G2-C/G2-D/G5 与行为 API 聚焦回归 100 项通过；当时完整 Python 回归 705 项中有 3 个失败、1 个错误、1 个跳过，未通过项为第 10.5 章 Windows 冷启动/子进程超时基线，不能称全量测试通过。10 月 30 日本轮流式/指标/行为 API 聚焦测试 89 项通过；本轮前端 Vitest 50 项、类型检查、生产构建及正式 Compose 下的浏览器 6 项通过。并发压测和知识库/Agent 的真实浏览器端到端尚未执行；不要把局部聚焦测试或本机顺序 P95 写成整体验收 PASS。
