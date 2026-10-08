# G5-A 真实行为全样本闭环实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 2,199,938 条已核验 REES46 历史事件经过隔离的 Kafka/Flink/Iceberg/Trino/Doris/API/页面链路，并形成可复核的容量与口径证据。

**Architecture:** 复用 G2-A 至 G3；每级回放用独立 run ID、Topic、Checkpoint 和 Iceberg 表。G2-D 只接受由合法 run ID 推导的来源表，发布记录绑定来源表和 Snapshot；旧 1,002 条发布及表不动。代码先离线验收，再在 C/D 盘门禁下做 1 万、10 万、全样本三轮，不把中间轮次发布为正式业务指标。

**Tech Stack:** PowerShell 7、Python 3.12、Kafka、Java Flink DataStream/Flink SQL、Iceberg/Trino、Doris、FastAPI/Pydantic、React/TypeScript、unittest/Vitest/Playwright。

**Spec:** [G5-A 设计](../specs/2026-10-08-graduation-g5-full-behavior-acceptance-design.md)。后续 G4 只消费已验收发布；本计划不实现 RAG/Agent。

## Global Constraints

- 源文件固定为 `data/rees46/oct-nov-users-2pct-verified.jsonl`，1,045,479,893 字节、2,199,938 条，SHA-256 `18a3202d380c8f6c53ad767ec2043d0717df1d3604d2211639bb0fc4699a5437`；重跑前重新校验文件和来源清单。
- `source_table` 只能是旧 `real_behavior_detail_v1`，或由 G2-C 合法 `run_id` 推导的 `real_behavior_detail_v1_<run_id 下划线形式>`；不得接收自由 SQL 标识符。
- `behavior-v1-s<snapshot>` 规则和指标公式不变。同 run ID 对应不同来源表时拒绝发布；旧 run 绑定旧表，不能静默重算、覆盖或删除。
- 只有来源数 2,199,938、`event_id` 精确去重数相同、窗口 `2019-10-01` 至 `2019-11-30`、跨层对账通过，才允许 `stable-user-2pct-full` 正式发布。
- C 盘至少 5 GiB；D 盘至少 20 GiB，且高于 10 万轮磁盘增量外推的 1.5 倍再加 5 GiB。门禁不满足立即停止，不能通过删镜像、卷或旧表绕过。
- 新缓存、Checkpoint、构建产物和报告留在 D 盘；执行 Python 命令前仅在当前进程设置 `TEMP/TMP=D:\EcommerceDev\temp`、`PIP_CACHE_DIR=D:\EcommerceDev\cache\pip`。前端 npm 缓存和临时目录也显式放 D 盘。
- 历史 `event_time`、本次 `replayed_at`、指标 `calculated_at` 分开呈现；不把历史回放称为企业当前实时流，也不宣称端到端 exactly-once。
- Olist 订单域独立，不与 REES46 按用户、商品或订单 ID 拼接；无源字段的业务功能不补造。

## Review Focus

- 非法 run ID、SQL 注入、表名与 run ID 不匹配：刷新和验证必须在查询/写入前拒绝，见 Task 1 测试。
- 两张 Iceberg 表偶然出现同一 Snapshot ID：旧发布的 `source_table` 不同必须失败，不可当作幂等成功，见 Task 2 测试。
- 旧 Doris 表没有新列或迁移被中断：迁移须可重试、等 Schema Change 完成并核对旧行仍指向旧表，见 Task 2 测试。
- 较新的 1 万条测试发布晚于全样本发布：API 仍选择完整样本；不合法完整发布则失败关闭，见 Task 3 测试。
- 暂停续传超过 24 小时状态 TTL、Kafka 重发或 writer 非空后从 earliest 重读：停止正式轮次并报告重复/缺失，不删除状态强行过关，见 Task 4/5 门禁。

## File Map

- `scripts/lib/G2d.BehaviorMetrics.psm1`：来源表解析和 SQL 模板安全替换。
- `jobs/sql/18_g2d_behavior_metrics.sql.template`：绑定隔离 Iceberg 表及其 Snapshot；来源身份增加回放时间范围。
- `scripts/refresh_g2d_behavior_metrics.ps1`：来源选择、Doris schema 迁移、发布和回读；`scripts/verify_g2d_behavior_metrics.ps1`：独立按发布来源表对账。
- `infra/compose/doris/init/02_create_behavior_metrics.sql`：新安装的 publication 结构；既有库由刷新脚本显式迁移，不依赖 `CREATE TABLE IF NOT EXISTS` 修改旧表。
- `services/api/app/behavior_repository.py`、`behavior_service.py`、`behavior_models.py`：完整样本优先读取、来源身份与时间契约。
- `apps/web/src/lib/types.ts`、`lib/api.ts`、`components/EvidenceDrawer.tsx`、`modules/Behavior.tsx`、`modules/Overview.tsx`：来源表、历史回放和时间证据。
- `scripts/g5_behavior_preflight.ps1`：分级只读门禁；`docs/graduation/g5-full-behavior-acceptance.md`：执行与失败证据。运行期复用 `generators/real_data/replay.py`、`jobs/datastream-quality/.../RealDataQualityJob.java`、`scripts/run_g2c_real_event_lakehouse.ps1` 和 `scripts/verify_g2c_real_event_lakehouse.ps1`。
- 对应测试扩展 `tests/test_g2d_behavior_metrics.py`、`tests/test_behavior_metrics_api.py`、`tests/test_g5_behavior_preflight.py` 和 `apps/web/src` 邻近测试；不写假业务数据充当验收结果。

---

### Task 1: 安全绑定独立来源表

**Interfaces:** `Resolve-G2dSourceTable -RunId <string>` 返回旧表或由合法 G2-C run ID 推导的表；`Split-G2dNamedSql -Sql <string> -SnapshotId <long> -SourceTable <string>` 返回五条绑定同表同 Snapshot 的查询。刷新增加 `-SourceRunId`，空值仅表示旧表；验证增加 `-ExpectedSourceRunId`。

- [ ] 在 `tests/test_g2d_behavior_metrics.py` 加失败测试：旧表默认保持不变；`g5-a10k-01` 推导 `real_behavior_detail_v1_g5_a10k_01`；非法字符、超长 ID、任意表名被拒绝；五条 SQL 都绑定相同来源表及 Snapshot 表。
- [ ] 运行 `python -m unittest tests.test_g2d_behavior_metrics -q`，确认新增断言先失败。
- [ ] 修改模块、SQL 模板、刷新器和验证器：来源只由 run ID 产生；`Get-G2dLatestSnapshotId`、命名 SQL、验证均使用同一来源；`-PlanOnly` 显示表但不连接服务。`stable-user-2pct-full` 必须指定独立 run ID，并检查精确来源数及 61 天窗口。
- [ ] 重跑聚焦测试，确认通过；旧默认 `-PlanOnly` 的行为和来源表保持兼容。提交独立改动。

### Task 2: 迁移并锁定发布来源身份

**Interfaces:** publication 增 `source_table VARCHAR(128) NOT NULL`、可空 `replay_first_at`/`replay_last_at`；旧记录迁移回填旧表。`New-G2dPublicationRecord`、插入、回读和 `Assert-G2dExistingPublication` 均比较 `source_table`；报告写入同一字段。回放时间从选定 Iceberg Snapshot 的 `replayed_at` 获取，不用当前时间冒充。

- [ ] 加失败测试：已存在旧行迁移后仍绑定旧表；重复迁移不改旧指标；同 Snapshot/不同表拒绝；新发布和回读缺失/错配 `source_table` 拒绝；回放时间缺失时不得捏造。
- [ ] 运行 `python -m unittest tests.test_g2d_behavior_metrics -q`，确认新增断言先失败。
- [ ] 更新 Doris 新建 DDL；在刷新器中检测旧 schema，按 [Doris Schema Change 文档](https://doris.apache.org/docs/3.x/table-design/schema-change/)做有界、可重试的 `ALTER TABLE ADD COLUMN` 并等待完成，再核对旧行和新列。更新 SQL 来源身份、发布/候选/回读及验证器；旧 run 不重新计算。
- [ ] 重跑聚焦测试；用只读 Doris 查询确认原 `behavior-v1-s881836466779140976` 仍是 1,002 条且来源表为旧表。若数据库不可用，标记动态迁移未验收，不把模拟测试写成真实迁移。提交独立改动。

### Task 3: API 与页面保留可追溯性

**Interfaces:** `BehaviorMetricMeta` 增 `source_table: str`、可空回放开始/结束时间；API 只读 PUBLISHED，优先完整样本再按发布时间排序。网页证据抽屉显示来源表/Snapshot、业务时间、回放时间和计算时间。

- [ ] 在 `tests/test_behavior_metrics_api.py` 加失败测试：完整样本优先于较新子集；非法来源表/计数/日期拒绝；旧发布显示旧表且回放时间未知；四类响应共享相同 meta。前端测试覆盖来源表和“历史回放”，不伪造当前生产时间。
- [ ] 运行聚焦 Python 与 Vitest，确认新增断言先失败。
- [ ] 修改 repository、service、Pydantic 模型和前端类型/解析/展示；保留现有订单页、日期/粒度筛选和空态。未来 G4 工具通过行为服务读取同一 meta，不另造来源字段。
- [ ] 重跑 Python 聚焦测试、`npm run test -- --run`、`npm run typecheck`；在真实发布后再运行浏览器流程，离线测试不代替动态验收。提交独立改动。

### Task 4: 分级门禁和可重复执行手册

**Interfaces:** `scripts/g5_behavior_preflight.ps1 -Stage 10000|100000|2199938 -RunId <id> [-FunctionsOnly]` 只读输出源身份、C/D 余量、隔离 Topic/表/Checkpoint 状态与停止原因；不创建或删除容器、Topic、表、卷。

- [ ] 在 `tests/test_g5_behavior_preflight.py` 加失败测试：SHA/字节数错误、C<5 GiB、D<20 GiB、目标非空、run ID 复用、100000 轮缺失时试图全样本均拒绝；测试以临时夹具/命令替身运行，不启动 Docker。
- [ ] 运行 `python -m unittest tests.test_g5_behavior_preflight -q`，确认新增断言先失败。
- [ ] 实现只读预检，并在运行手册写清三轮不同身份、实际端口发现、Checkpoint 续传、Kafka lag 归零后等待 Flink Checkpoint/Iceberg Snapshot、10 万轮磁盘增量外推和故障停机规则。回放使用 `python -m generators.real_data.replay`，质量作业使用 `com.ecommerce.quality.real.RealDataQualityJob`，落湖与验证使用现有 G2-C 两个脚本；不做新的全栈编排器。
- [ ] 重跑聚焦测试和全量 Python 回归；所有测试临时文件与缓存固定 D 盘。提交独立改动。

### Task 5: 真实三轮验收与证据发布

**Interfaces:** 运行报告保存在被忽略的 `tmp/graduation/g5/`，Git 只提交聚合结论和可复现命令。每轮记录 SHA、run ID、Topic offset、Kafka 确认、Flink Job/Checkpoint、clean/late/DLQ、Iceberg Snapshot、Trino `event_id` 去重、Doris run、吞吐/延迟/资源峰值。

- [ ] 只读预检并发现现有端口/容器状态；最小化启动必需服务。1 万轮用独立 Topic/Checkpoint/湖表核对字段、日期和数量；不发布 `stable-user-2pct-full`。
- [ ] 10 万轮用新身份验证持续处理、暂停恢复和容量；测量 Docker D 盘增量，计算全样本预测，先判断 C/D 门禁再决定是否继续。
- [ ] 门禁通过才对完整 2,199,938 条做新 run；等待 Kafka lag、Flink Checkpoint 和 Iceberg Snapshot 对齐，Trino 总数与 `DISTINCT event_id` 都须精确为 2,199,938；异常先诊断，不清表重跑伪造成功。
- [ ] 从该来源表刷新 Doris；核对四表行数/SHA、API、六模块桌面/手机浏览器和正式 Compose API/持久身份库。记录 API P95、任务耗时与局限；只在真实证据齐全时写 `PASS`，否则写停机点与未验证项。
- [ ] 运行聚焦/全量回归、前端类型/构建与浏览器测试；复核 `git diff --check`，仅提交代码和汇总证据到当前 `codex/` 分支并核对远端 SHA，保留原始数据/运行报告在本机。
