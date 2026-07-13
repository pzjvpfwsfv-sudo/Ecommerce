# 第 3 章Flink SQL 实时指标计算设计

## 1. 设计目标

第 3 章的目标是先把“实时计算闭环”跑通，而不是一上来就做复杂流处理能力，具体包括：

- 从 Kafka 的 `user_behavior_events` topic 读取行为事件
- 用 Flink SQL 先完成最小指标计算
- 把结果输出到 `print` sink 做可视化验证
- 为第 4 章接入 Doris 做好结构和脚本准备

## 2. 方案选择

本章优先采用 `Flink SQL`，而不是直接进入 `DataStream API`。

### 当前阶段计算内容

- `PV`：累计事件数
- `UV`：累计去重用户数

### 当前阶段链路

- Kafka Source
- Flink SQL 聚合
- Print Sink

## 3. 为什么先选 Flink SQL

Flink SQL 更适合这一章的目标，因为我们现在最需要的是：

- 快速验证 Kafka 到 Flink 的消费链路是否通
- 用最少代码把指标逻辑表达清楚
- 为后续 Doris 落库和面试展示保留清晰的 SQL 资产

如果第 3 章直接上 DataStream API，虽然灵活性更强，但会过早引入：

- 更重的工程代码
- 更高的调试成本
- 与当前“先跑通主链路”的目标不完全匹配的复杂度

因此当前推荐路线是：

- 第 3 章先用 Flink SQL 跑通
- 后续补一版 DataStream API，专门承接更复杂的事件处理和工程能力展示

这样后面就能自然形成一段很完整的“为什么先 SQL、什么时候升级 DataStream”的面试故事。

## 4. 作业组织方式

本章把 SQL 作业拆成三段：

- Source 表定义
- Sink 表定义
- 指标插入逻辑

脚本层再把三段 SQL 合并后一次性提交给 SQL Client。

这样设计的原因是：

- 文件职责清晰，便于阅读和修改
- 不会因为分多次提交导致临时表作用域丢失
- 更适合后面扩展 Doris sink、更多指标 SQL 和多环境运行脚本

## 5. 运行环境设计

本章在 Compose 中新增最小 Flink 运行时：

- `flink-jobmanager`
- `flink-taskmanager`
- `flink-sql-client`

同时显式挂载 Kafka SQL Connector，并在脚本里补上两类 readiness 检查：

- Kafka broker ready
- Flink Web UI ready

这是因为第 3 章的真实风险并不在 SQL 本身，而在“作业提交时外部依赖是否真的已经就绪”。

## 6. 本章边界

### 本章要完成的内容

- Kafka source 表
- print sink 表
- 累计型 PV / UV 指标 SQL
- 一键提交流式作业脚本
- 最小自动化测试与运行验证说明

### 本章暂时不做的内容

- Doris sink 落库
- 事件时间窗口、水位线和迟到处理
- 复杂漏斗、转化率、留存等业务指标
- DataStream API 版本作业
- 作业 HA、checkpoint、savepoint 等生产级治理能力

## 7. 这章要沉淀的工程经验

这一章除了要跑通作业，还要沉淀几个后面能讲得很加分的工程点：

- SQL 分段管理与单次提交策略
- PowerShell 写文件 BOM 对 Flink SQL 的影响
- 官方镜像不自带 Kafka connector 的兼容处理
- “容器已启动”不等于“服务已 ready”的链路认知

这些都能支撑后续把项目讲成“真实排障过、理解系统边界”的作品，而不是只会照着教程搭环境。

## 8. 与后续章节的衔接

第 3 章的 print sink 不是终点，而是一个故意保守的中间站。

接下来会按下面顺序继续演进：

- 第 4 章把指标结果写入 Doris
- 第 5 章通过 FastAPI 暴露实时指标查询接口
- 后续补充 DataStream API 版本
- 再安排一次 Kafka 从 ZooKeeper 到 KRaft 的架构升级

这样整个项目会形成很顺的演进路径：先跑通、再沉淀、再升级。

## 9. 预期结果

第 3 章结束后，仓库应该具备“Kafka 消息被 Flink SQL 持续消费，并实时输出 PV / UV 更新结果”的能力。

我们应该能够在本地明确验证到：

- Flink Web UI 中有运行中的 SQL 作业
- REST 接口能看到 `RUNNING` 状态
- TaskManager 日志持续输出 `+I / -U / +U` 形式的流式聚合结果

这意味着项目第一次真正具备了“从事件输入到实时指标输出”的核心闭环。
