# 第 4 章：Doris + FastAPI 实现记录

## 1. 实现目标

第 4 章的目标是把前面已经完成的 Kafka + Flink SQL 实时计算链路，真正接到一个可以查询的下游系统里，并通过轻量 API 暴露出来。

最终要求不是“配置写完”，而是满足下面这几个落地结果：

- Doris 在本地开发环境中可稳定启动。
- Flink 聚合结果能真实写入 Doris。
- FastAPI 能查到实时 PV/UV。
- 整条链路的真实排障过程被记录下来，变成项目资产。

## 2. 最终交付结果

本章最终交付如下：

- Docker Compose 中补齐 Doris FE / BE 运行环境。
- 新增 Doris 初始化脚本，创建 `ecommerce.realtime_metrics` 表。
- 完成 `jobs/sql/04_sink_doris_metrics.sql`，将 Flink 聚合结果落入 Doris。
- 提供 `api/app.py` 查询接口。
- 补齐 `tests/test_chapter_4_artifacts.py` 与 `tests/test_api_service.py`。
- 验证本地接口可返回实时指标值。
- 将真实排障记录沉淀进设计和实现文档。

## 3. 关键实现文件

核心落地文件如下：

- `infra/docker-compose.yml`
- `jobs/sql/04_sink_doris_metrics.sql`
- `scripts/init_doris_realtime_metrics.ps1`
- `api/app.py`
- `tests/test_chapter_4_artifacts.py`
- `tests/test_api_service.py`

这些文件共同组成了本章的最小闭环。

## 4. 与原计划相比的实际落地差异

原始计划更偏“标准流程”视角，但真实实现过程中出现了不少环境与中间件层面的偏差，因此本章最终不是机械照着计划执行，而是边跑边修。

最关键的实际差异有三点：

### 4.1 Doris sink 最终采用 batch mode

原本更自然的想法是直接用流式方式持续写入 Doris，但本地单机环境下可见性与稳定性并不理想。最终采用 batch mode，并配合刷新参数，使结果能够稳定写入并可查询。

### 4.2 Doris 初始化改成“严格 ready 检查 + stdin 执行 SQL”

最初脚本对 Doris ready 判断不够严格，而且错误假设了容器内存在 `/workspace` 路径。最终改成等待 Doris 真正 ready 后，再通过 stdin 将本地 SQL 输入容器内 mysql 客户端执行。

### 4.3 本章新增了“真实排障沉淀”作为正式交付物

这不是补充说明，而是本章正式成果的一部分。因为这个项目不只是为了跑通，还要服务于后续复盘、讲解和面试表达。

## 5. 真实排障过程

以下内容按实际问题暴露顺序记录。

### 5.1 Docker Desktop 状态异常，先影响容器启动可靠性

在进入 Doris 与 Flink 细节之前，首先暴露的是 Docker Desktop 后端状态不稳定问题。这个阶段如果直接盯业务配置，会浪费很多时间。

实际结论：

- 容器编排问题不一定来自 compose 文件本身。
- 排障第一步应该先确认 Docker daemon、网络与容器运行时状态。

### 5.2 Doris 网络子网冲突，导致服务无法稳定拉起

`infra/docker-compose.yml` 中 Doris 所使用的自定义网段与当前机器已有 Docker 网络发生冲突。

实际修复：

- 把 Doris 的自定义子网改到新的可用网段。

这一步解决后，后续 Doris 组件才具备稳定启动基础。

### 5.3 Doris 初始化脚本出现“假成功”

虽然脚本执行完成，但库表并没有可靠创建。排查后发现有两个原因：

- Doris FE 尚未真正可用。
- SQL 文件路径错误地按容器内路径处理。

实际修复：

- 在脚本中增加 Doris ready 轮询。
- 改为本地读取 SQL，并通过 stdin 喂给容器内 mysql 客户端执行。

这一步非常关键，因为如果初始化脚本不可信，后续所有问题都会被误导。

### 5.4 Flink SQL Client 执行时遇到 BOM 编码问题

临时 SQL 文件生成后，Flink 在解析时遇到词法错误。最终确认是文件头带了 UTF-8 BOM。

实际修复：

- 确保写出的临时 SQL 文件采用无 BOM 的 UTF-8 编码。

这类问题虽然小，但真实项目里非常常见，尤其是在 Windows 环境下。

### 5.5 Kafka ZooKeeper 模式残留状态导致 broker 启动异常

Kafka 在本项目当前阶段仍是 ZooKeeper 模式，运行中出现了 `NodeExistsException`，本质上是 broker 状态残留造成的启动冲突。

实际处理：

- 清理残留状态。
- 重新确认 broker 注册与运行状态。

这一步为后续升级 KRaft 埋下了很好的“为什么要演进”的背景。

### 5.6 Kafka 恢复后，需要重新确认 topic 是否存在

broker 恢复不等于业务状态自动恢复。重建 Kafka 后，topic 不一定还在，因此需要重新创建并再次验证数据链路。

实际处理：

- 重建主题。
- 重启数据生成器。
- 再次提交 Flink 作业验证。

### 5.7 Doris sink 有写入动作，但表里查不到数据

这是本章最核心的问题。

现象：

- Flink 作业处于运行态。
- Doris Connector 日志可见 stream load 请求。
- Doris 查询结果仍为空。

实际定位结论：

- 问题不在 SQL 字段映射。
- 问题不在 FastAPI 查询逻辑。
- 问题主要出在本地单机环境下 Doris sink 的提交与可见性策略。

最终修复：

- 开启 `sink.enable.batch-mode = true`
- 关闭 `sink.enable-2pc = false`
- 设置 `sink.buffer-flush.interval = 3s`
- 设置 `sink.buffer-flush.max-rows = 10000`

这里还踩到了一个 connector 约束：

- `sink.buffer-flush.max-rows` 不能设置得过小，最终 `10000` 才满足要求。

修复后，数据终于在 Doris 中稳定可见。

## 6. 最终验证记录

最终验证结果如下：

### 6.1 Flink 作业状态

- Flink Job 状态为 `RUNNING`

### 6.2 Doris 查询结果

最终查询到：

- `pv = 3`
- `uv = 2`

### 6.3 FastAPI 接口结果

`GET /metrics/realtime` 返回：

```json
{"updated_at":"2026-07-07T16:02:43","pv":3,"uv":2}
```

`GET /metrics/pv` 返回：

```json
{"metric_name":"pv","metric_value":3,"updated_at":"2026-07-07T16:02:43"}
```

### 6.4 测试结果

执行：

```powershell
python -m unittest tests.test_chapter_4_artifacts tests.test_api_service -v
```

结果：

- 15/15 通过

## 7. 本章产出能怎么讲

如果从面试或项目复盘角度来讲，第 4 章的重点不是“接了一个 API”，而是：

- 我把流式聚合结果真正落到了分析型存储。
- 我把实时指标封装成了服务接口。
- 我在本地单机环境中完整解决了一轮真实中间件排障问题。
- 我知道哪些问题是环境问题、哪些是配置问题、哪些是组件机制问题。

一个比较自然的表达方式是：

“第 4 章我把 Flink 的实时聚合结果落到 Doris，并通过 FastAPI 暴露成指标查询接口。过程中我处理过 Docker 网络冲突、Doris 初始化假成功、Flink BOM 编码问题、Kafka ZooKeeper 残留状态，以及 Doris sink 可见性问题。最后不仅链路跑通了，还把排障过程沉淀成文档，后面继续往湖仓和架构演进方向扩展。”

## 8. 对下一章的承接

第 4 章解决的是“实时聚合结果如何对外查询”，但项目还缺少一层真正的湖仓明细沉淀能力。

因此第 5 章的自然承接方向是：

- 引入 MinIO 作为对象存储
- 引入 Iceberg 作为湖表格式
- 让行为明细形成可回放、可追溯、可离线分析的数据底座

同时，后续还要继续保留 Kafka 从 ZooKeeper 向 KRaft 演进的路线，这样整个项目才会形成一条完整的“先跑通，再升级”的架构故事线。

## 9. 后续章节补充验证

后续真实推进已经证明，第 4 章的排障资产并没有过时，而是被继续复用了。

### 9.1 对 Kafka 侧的启发

第 4 章里 ZooKeeper 模式下的残留状态问题，在后续章节里依然是一个明确痛点。这让“后面一定要升级到 KRaft”不再只是规划，而是有真实问题支撑的演进方向。

### 9.2 对湖仓侧的启发

第 6 章最开始尝试让 Trino 直接读取第 5 章 HadoopCatalog 时失败，最终推动项目升级到共享 Hive Metastore。这和第 4 章里的经验完全一致：

- 不能把单组件局部成功误判成整条链路成功
- 不能把架构不兼容误判成脚本或参数小问题

### 9.3 对项目故事线的价值

回头看，第 4 章之后的项目已经形成两条很完整的演进线：

- 实时查询层：`Flink -> Doris -> FastAPI`
- 湖仓分析层：`Flink -> Iceberg/MinIO -> Hive Metastore -> Trino`

再往后接上 `ZooKeeper -> KRaft`，整个项目就不再只是“做了很多组件”，而是能讲出连续、真实、可解释的工程演进过程。

## 10. 本次回补的真实排障记录价值

结合后续真正推进到第 7 章 KRaft 迁移的结果，可以把第 4 章的实现记录再落得更实一点：

- 当时 Kafka 在 ZooKeeper 模式下出现的残留状态、topic 需要重建、broker 恢复不等于业务恢复，这些都不是“临时运气不好”，而是后续架构升级的直接动因。
- 这次迁移到 `controller + broker` 分角色的 KRaft Compose 形态后，前面那段排障历史终于形成了一个完整闭环：先遇到状态管理痛点，再实施去 ZooKeeper 化改造。
- 从实现角度看，第 4 章留下的排障方法论也被证明是可复用的：先验证容器状态，再验证中间件可用性，最后验证业务链路与数据可见性，而不是一开始就盲改 SQL 或接口代码。

因此，这次回补不是单纯“补文档”，而是把第 4 章的现场故障、后续第 5/6 章的链路扩展、以及第 7 章的 KRaft 架构迁移真正串成一条能复述、能追问、也经得起追问的实现故事线。
