# 第 4 章：Doris + FastAPI 最小查询链路设计

## 1. 背景

前 3 章已经完成了 Kafka 事件流、Flink SQL 明细清洗与实时聚合的基础链路，但这时项目还停留在“数据在流里跑”，没有真正形成一个可以给业务方、面试官和后续 AI 助手消费的查询出口。

因此，第 4 章的目标不是继续堆新技术，而是先把“实时指标真正查出来”这件事做通：让 Flink 聚合结果落到分析型存储中，再通过一个轻量 API 暴露给外部系统调用。

这一章选用 Doris + FastAPI，原因很明确：

- Doris 负责承接实时聚合结果，提供低延迟查询能力。
- FastAPI 负责暴露一个最小可用查询接口，形成“实时数仓指标服务化”的闭环。
- 这一章强调的是链路打通、排障落地、工程闭环，而不是一次性把服务层做重。

## 2. 本章目标

本章需要完成以下结果：

- 新增 Doris 到本地 Docker Compose 环境。
- 让 Flink SQL 将实时 PV/UV 聚合结果写入 Doris。
- 初始化 `realtime_metrics` 表，并保证表结构可反复初始化。
- 提供 FastAPI 查询接口，对外返回实时 PV/UV 指标。
- 补齐一份真实可复述的排障记录，让本章不仅“能跑”，还“能讲”。

## 3. 范围

本章包含：

- Doris FE/BE 最小单机开发环境。
- Flink 到 Doris 的实时 sink。
- FastAPI 指标查询接口。
- 基础单元测试与文档校验。
- 真实排障经验沉淀。

本章不包含：

- 多副本 Doris 高可用部署。
- 指标服务鉴权、限流、缓存和多租户治理。
- 完整 BI 看板。
- Iceberg 湖仓明细查询。

这些内容放到后续章节逐步演进，避免在本章过度设计。

## 4. 设计原则

### 4.1 先跑通，再演进

本章采用最小闭环原则，先让一条“生成数据 -> Kafka -> Flink -> Doris -> FastAPI”链路稳定可见，再为后续湖仓、AI 助手和架构演进留扩展点。

### 4.2 优先保留可讲述的工程痕迹

很多项目只记录“最终正确答案”，但这类项目在面试中会显得很薄。本章明确保留真实排障记录，让项目具有“做过、踩过、修过、总结过”的可信度。

### 4.3 开发环境强调可重复初始化

所有组件都以本地开发为目标，初始化脚本要支持重复执行，避免因为“第一次成功、第二次坏掉”而降低项目可维护性。

### 4.4 服务层保持轻量

FastAPI 在本章只承担查询出口职责，不提前引入复杂分层、权限系统和缓存系统，保证重点聚焦在实时链路闭环。

## 5. 架构设计

链路如下：

1. 数据生成器向 Kafka 持续写入电商行为事件。
2. Flink SQL 读取 Kafka 主题，计算实时 PV/UV 聚合。
3. Flink 将聚合结果写入 Doris `realtime_metrics` 表。
4. FastAPI 从 Doris 查询实时指标。
5. 后续 AI 指标助手或前端页面调用 FastAPI 接口获取最新值。

本章形成的是“实时指标服务层”的第一版，而不是最终分析平台终态。

## 6. Doris 表设计

目标表：`ecommerce.realtime_metrics`

核心字段：

- `metric_name`：指标名称，如 `pv`、`uv`
- `metric_value`：指标值
- `updated_at`：最新更新时间

设计思路：

- 本章只保留宽度极小的实时聚合结果表，避免过早引入复杂明细模型。
- 通过主键语义保留每个指标的最新值，适合实时看板与 API 拉取。
- 由于本章关注的是“最新指标”，所以不做长时间序列沉淀；后续如需历史趋势，再落到 Iceberg 或补充 Doris 明细/汇总模型。

## 7. Flink 落库设计

Flink 任务负责将实时聚合结果写入 Doris。

实际落地时，本章采用 Doris Connector 的 batch mode，而不是最初想象中的逐条流式提交，原因来自真实排障结果：

- 逐条或过于频繁的 stream load 在本地单机 Doris 环境中容易出现“写入日志成功但查询不可见”的现象。
- 将 sink 切换为批量刷新后，能够显著提升本地开发环境的稳定性。
- Doris Connector 对 `sink.buffer-flush.max-rows` 存在下限要求，最终使用 `10000` 作为有效配置值。

最终关键配置思路：

- 开启 `sink.enable.batch-mode = true`
- 关闭两阶段提交 `sink.enable-2pc = false`
- 设置批量刷新条数与刷新间隔，兼顾可见性与稳定性

这套配置是本章“真实可跑通”的版本，不是纸面上的理想化版本。

## 8. FastAPI 接口设计

本章 API 只提供最小必要能力：

- `GET /health`：健康检查
- `GET /metrics/realtime`：一次返回当前 PV/UV
- `GET /metrics/pv`：单指标查询
- `GET /metrics/uv`：单指标查询

返回结构尽量直观，方便：

- 本地 curl 调试
- 后续前端接入
- AI 助手调用后做自然语言解释

本章 API 层不做复杂 DTO 抽象，保持“查询即返回”的轻量实现。

## 9. 文件与目录规划

本章核心文件包括：

- `infra/docker-compose.yml`
- `jobs/sql/04_sink_doris_metrics.sql`
- `scripts/init_doris_realtime_metrics.ps1`
- `api/app.py`
- `tests/test_chapter_4_artifacts.py`
- `tests/test_api_service.py`

文档文件包括：

- `docs/superpowers/specs/2026-07-07-chapter-4-doris-fastapi-design.md`
- `docs/superpowers/plans/2026-07-07-chapter-4-doris-fastapi-implementation.md`

## 10. 真实排障记录沉淀

这一节是本章最重要的附加价值之一。项目不是一次写对，而是在真实运行中逐步修正。

### 10.1 Docker Desktop 后端未稳定，导致容器无法正常拉起

最开始并不是 Doris 或 Flink 配置有问题，而是 Docker Desktop 自身状态不稳定，导致容器启动过程异常、网络初始化不完整、后续诊断信息也不可靠。

这类问题说明一个真实工程经验：

- 遇到中间件故障时，不要默认是业务配置错了。
- 先确认宿主环境、Docker daemon、网络驱动是否正常。

### 10.2 Doris 自定义网段与现有网络冲突

`docker-compose.yml` 中给 Doris 预设了自定义网段，但实际环境里该网段已经被占用，导致 Doris 相关容器无法稳定启动。

最终修复：

- 将 Doris 使用的自定义子网从冲突网段调整到新的可用网段。

这一步的经验是：

- 本地多项目并行开发时，自定义 bridge subnet 很容易冲突。
- Docker 网络问题如果不先处理，后面的服务排障都会失真。

### 10.3 Doris 初始化脚本“看起来成功”，实际上没有真正建表

初始化脚本最初存在两个问题：

- Doris FE 虽然进程启动，但未真正 ready 时就开始执行建库建表。
- SQL 文件路径按容器内 `/workspace` 处理，但实际 FE 容器里并不存在该路径。

这导致表面上脚本似乎执行过，但目标表并没有可靠创建。

最终修复：

- 在 PowerShell 脚本中增加更严格的 Doris ready 轮询。
- 改为把本地 SQL 内容通过 stdin 输送给容器内 mysql 客户端执行，而不是依赖容器内文件路径。

这一步是非常典型的“脚本假成功”问题，面试里很值得讲。

### 10.4 Flink 执行临时 SQL 时遇到 UTF-8 BOM 词法错误

在把 SQL 内容拼接成临时执行文件时，文件头部带入了 BOM，导致 Flink SQL Client 报词法错误，表现为开头出现不可见字符引发解析失败。

最终修复：

- 确保生成的临时 SQL 文件采用无 BOM 的 UTF-8 编码。

这类问题很细，但正因为细，才更能体现真实工程排障能力。

### 10.5 Kafka ZooKeeper 模式残留 znode，触发 `NodeExistsException`

Kafka 在 ZooKeeper 模式下，broker 重启或异常退出后可能遗留临时 znode，导致下一次启动时报 `NodeExistsException`，看起来像是 Kafka 本身坏了。

最终处理方式：

- 清理残留状态后重建 broker 运行环境。
- 重新确认 topic 与 broker 注册状态。

这也正是后续要升级到 KRaft 的一个重要铺垫：

- ZooKeeper 模式更容易在本地教学环境里出现额外状态管理问题。
- 后续架构演进时可以明确讲出“为什么要从 ZooKeeper 迁移到 KRaft”。

### 10.6 Kafka 重建后 topic 状态丢失，需要重建主题

在修复 Kafka broker 问题后，原有 topic 不一定继续存在，因此数据生成器虽然能跑，但消费者侧没有读到预期数据。

最终修复：

- 显式重建主题。
- 再重新启动数据生成器与 Flink 作业验证全链路。

说明：

- 中间件恢复后，不能想当然地认为业务状态也自动恢复。
- 排障时必须把“基础设施恢复”和“业务链路恢复”分开验证。

### 10.7 Doris sink 有 stream load 日志，但查询结果始终不可见

这是本章最关键、也最有价值的一次问题。现象是：

- Flink 作业看起来在跑。
- Doris Connector 日志里能看到 stream load 请求。
- 但在 Doris 表中查询不到最终结果。

这类问题最容易让人误判成：

- SQL 写错了
- 表结构不匹配
- FastAPI 查询错库

但真实根因是本地单机环境下的 sink 提交策略不稳定，导致“请求发出”和“结果对查询可见”之间没有形成预期闭环。

最终修复：

- 启用 Doris sink batch mode。
- 设置合理的刷新间隔。
- 将 `sink.buffer-flush.max-rows` 提高到 Doris Connector 接受的最小有效阈值 `10000`。
- 关闭 `sink.enable-2pc`，降低本地单机验证复杂度。

修复后，PV/UV 数据可以稳定查到。

### 10.8 最终链路验证成功

最终得到的有效结果是：

- Flink 作业状态为 `RUNNING`
- Doris 查询结果返回：
  - `pv = 3`
  - `uv = 2`
- FastAPI 接口可正常返回：
  - `/metrics/realtime`
  - `/metrics/pv`

这意味着本章真正完成了“流式计算结果服务化”的闭环。

## 11. 验证标准

本章完成的判定标准如下：

- Docker Compose 中相关服务能正常启动。
- Doris `realtime_metrics` 表可被初始化脚本稳定创建。
- Flink 作业可持续运行，不是一次性失败退出。
- Doris 中可查询到 `pv`、`uv` 实时值。
- FastAPI 接口能返回与 Doris 一致的指标结果。
- 对应测试用例通过。
- 文档中保留真实排障记录。

## 12. 本章价值

这一章的价值不只是“加了 Doris 和 API”，而是把项目从“会算”推进到“能查、能展示、能讲清楚为什么这样做”。

它为后续章节提供了三个直接收益：

- 为 MinIO + Iceberg 湖仓层提供并行演进基线。
- 为 AI 指标分析助手提供稳定的实时指标输入。
- 为面试中的“工程排障与架构演进故事”提供真实素材。

## 13. 面试表达

这一章可以这样概括：

“我先用 Doris 和 FastAPI 做了一条最小实时查询链路，把 Flink 聚合结果真正服务化。过程中不是一次跑通的，我连续解决了 Docker 网络冲突、Doris 初始化假成功、Flink SQL BOM 编码、Kafka ZooKeeper 残留状态，以及 Doris sink 可见性问题。最后把链路稳定在本地跑起来，API 能直接查到实时 PV/UV。后面我会继续把明细沉到 MinIO + Iceberg，再把 Kafka 升级到 KRaft，形成完整的架构演进故事。”

## 14. 后续演进回写

从后续章节的真实推进结果来看，第 4 章当时沉淀下来的排障方法是有延续价值的。

后面在第 5/6 章推进 MinIO + Iceberg + Trino 时，又复用了同样的工程思路：

- 先把写入链路和查询链路拆开验证
- 先确认容器与中间件状态，再判断业务配置
- 失败时优先找“架构级根因”，而不是盲调参数

尤其是两段后续演进，和第 4 章形成了很自然的故事衔接：

- Kafka 一侧，ZooKeeper 残留状态问题进一步强化了后续升级 KRaft 的必要性
- 湖仓一侧，Trino 无法直接共享 HadoopCatalog，推动项目升级到共享 Hive Metastore

这意味着第 4 章不只是一个“实时查询小闭环”，它还是后面整条架构演进叙事的起点。

## 15. 结合本次 KRaft 迁移的回看补充

到了第 7 章真正动手把 Kafka 从 ZooKeeper 迁移到 KRaft 时，第 4 章里记录下来的那些“当时看起来像偶发故障”的现象，反而变成了很有说服力的演进依据。

这次回看最值得补进设计文档的有三点：

- 第 4 章出现的 `NodeExistsException` 不是一次孤立报错，而是 ZooKeeper 模式下 broker 状态与外部协调状态分离后，在本地开发环境里很容易放大的典型问题。
- 当项目逐步接入 Doris、Iceberg、Hive Metastore、Trino 之后，Kafka 已经不再只是一个“能跑就行”的消息队列，而是整条链路的前置基础设施，因此它的可解释性和可维护性必须提升。
- 采用 `controller + broker` 分角色的最小 KRaft 拓扑，不只是为了“升级新版本”，而是为了给后续继续演进到多 controller、多 broker 形态保留清晰路径。

换句话说，第 4 章的真实排障记录并没有随着章节结束而失效，而是直接成为了后续架构演进设计输入的一部分。这种“先记录问题，再用后续架构方案回应问题”的写法，也会让整套项目材料更像真实工程，而不是事后拼接的技术清单。
