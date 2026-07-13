# 第 7 章：Kafka 从 ZooKeeper 到 KRaft 演进设计

## 1. 背景

前面的章节已经把项目主链路逐步搭了起来：

- 第 1 章先用分阶段 Compose 搭好基础设施骨架
- 第 2 章跑通了 `数据生成器 -> Kafka`
- 第 3 章跑通了 `Kafka -> Flink SQL`
- 第 4 章补上了 `Flink -> Doris -> FastAPI`
- 第 5/6 章又补上了 `Flink -> Iceberg/MinIO -> Hive Metastore -> Trino`

在这个过程中，Kafka 一直沿用的是 `ZooKeeper + broker` 的起步方案。这个选择在项目初期是合理的，因为它能帮助我们尽快把消息入口搭起来、尽快验证 Flink 主链路。

但随着项目反复执行端到端验证，ZooKeeper 模式已经暴露出真实问题：

- broker 重建后可能残留 znode
- 本地反复 `docker compose up -d --force-recreate` 时可能触发 `NodeExistsException`
- Kafka 恢复后还要额外确认 topic 状态是否还在
- 验证脚本不得不在基础设施层做越来越多兜底

这说明第 7 章的 KRaft 迁移不是“为了升级而升级”，而是有明确工程动机的架构演进。

## 2. 本章目标

第 7 章需要完成以下结果：

- 去掉 ZooKeeper 服务
- 把 Kafka 升级为 KRaft 模式
- 采用 `1 controller + 1 broker` 的最小双角色拓扑
- 保持上层业务入口基本不变
- 让数据生成器、Flink SQL、验证脚本继续正常工作
- 沉淀一份完整、可复述的迁移设计和验证记录

## 3. 为什么不做更重的“伪生产集群”

本章不做：

- `3 controller + 1 broker`
- `3 controller + 3 broker`
- 多 broker 副本容灾

原因很明确：

- 原始项目计划里并没有把第 7 章定义成生产级 Kafka 集群模拟
- 当前项目目标是“真实教学型本地演进”，不是“强行堆集群规模”
- 如果现在拔高到多 controller 集群，会显著增加配置、验证、资源和排障复杂度，反而冲淡主线

因此本章最合适的范围是：

**用最小双角色 KRaft 拓扑，把控制面升级讲清楚，同时不破坏前面已经跑通的业务主链路。**

## 4. 目标架构

本章迁移后的 Kafka 拓扑如下：

- `kafka-controller`
  - 只承担 controller 角色
  - 只参与 KRaft quorum
  - 不对宿主机暴露端口
- `kafka-broker`
  - 只承担 broker 角色
  - 对宿主机暴露 `9092`
  - 对容器内继续提供 `kafka:29092`

这意味着项目从：

- `ZooKeeper + 单 broker`

演进为：

- `KRaft controller + KRaft broker`

## 5. 设计原则

### 5.1 控制面升级，业务入口尽量不变

这次迁移最重要的原则是：

- 外部使用者仍然连 `localhost:9092`
- 容器内使用者仍然连 `kafka:29092`

也就是说，KRaft 的变化主要发生在 Kafka 基础设施内部，而不是强行把所有上层调用方式一起推翻。

### 5.2 用角色拆分体现 KRaft 的含金量

本章不采用单进程 `broker,controller` 混合模式作为最终形态，而是采用双角色拆分：

- 这样更容易讲清楚 KRaft 的角色边界
- 更容易解释 `process.roles`、`controller.quorum.voters`、listener 分工
- 更像一次真实的架构演进，而不是单纯“把 ZooKeeper 配置删掉”

### 5.3 优先保证现有章节兼容

迁移不能只看 Kafka 自己是否启动，还必须保证：

- 第 2 章数据生成器还能发消息
- 第 3 章 Flink SQL 还能消费
- 第 5/6 章依赖 Kafka 的脚本还能跑

如果只做成“KRaft 能启动”，但把上层链路打断，这次迁移就不能算成功。

## 6. Compose 与配置设计

### 6.1 服务拆分

`infra/docker-compose.yml` 中将发生以下变化：

- 删除 `zookeeper`
- 删除旧的单 `kafka` 服务定义
- 新增 `kafka-controller`
- 新增 `kafka-broker`

其中：

- `kafka-controller` 只用于控制面
- `kafka-broker` 保留当前业务侧需要的 broker 地址语义

### 6.2 命名兼容

为了尽量减少对现有脚本的破坏，本章建议：

- `kafka-broker` 使用 `container_name=ecom-kafka`
- `kafka-broker` 使用 `hostname=kafka`

这样可以最大程度兼容已有脚本中的：

- `ecom-kafka`
- `kafka:29092`
- `localhost:9092`

控制器则单独使用新的容器名，例如：

- `ecom-kafka-controller`

### 6.3 环境变量设计

`infra/.env.example` 需要从 ZooKeeper 参数迁移到 KRaft 参数，例如：

- 删除：
  - `ZOOKEEPER_CONTAINER_NAME`
  - `ZOOKEEPER_PORT`
- 新增：
  - `KAFKA_CONTROLLER_CONTAINER_NAME`
  - `KAFKA_CONTROLLER_PORT`
  - `KAFKA_BROKER_ID`
  - `KAFKA_CONTROLLER_NODE_ID`
  - `KAFKA_CLUSTER_ID`

这样第 7 章的配置不只是“写在 Compose 里”，而是具备清晰可解释的参数边界。

### 6.4 KRaft 核心配置语义

本章文档与配置中需要明确体现以下关键概念：

- `process.roles`
  - controller 只做 controller
  - broker 只做 broker
- `node.id`
  - controller 和 broker 使用不同节点 ID
- `controller.quorum.voters`
  - 定义 controller quorum 成员
- `listeners`
  - controller listener 与 broker listener 分离
- `inter.broker.listener.name`
  - broker 间通信 listener
- `controller.listener.names`
  - controller 使用的 listener 名称

本章不追求一次引入所有生产参数，而是要把这些最关键的 KRaft 语义讲清楚、配置正确。

## 7. 对现有脚本和代码的影响

### 7.1 生成器与业务侧

Python 数据生成器原则上不需要理解 controller，它仍然只连接 broker：

- 本地继续走 `localhost:9092`

### 7.2 Flink 侧

Flink SQL source 继续保持：

```sql
'properties.bootstrap.servers' = 'kafka:29092'
```

也就是说，Flink 不需要因为 KRaft 迁移而改动其业务语义。

### 7.3 PowerShell 验证脚本

现有脚本中与 Kafka 相关的检查逻辑会继续聚焦 broker，而不是 controller：

- `run_flink_sql_job.ps1`
- `verify_chapter_5_end_to_end.ps1`
- `run_chapter_5_iceberg_pipeline.ps1`

这些脚本可以继续通过：

```powershell
docker exec ecom-kafka kafka-topics --bootstrap-server kafka:29092 --list
```

来判断 broker 是否可用。

这符合真实工程中的职责边界：业务脚本只验证业务入口，不直接操作控制面。

## 8. 验证设计

### 8.1 第 1 层：KRaft 基础设施验证

本章首先要确认：

- `kafka-controller` 能启动
- `kafka-broker` 能启动
- broker 日志明确体现 KRaft 模式，而不是 ZooKeeper 逻辑
- `kafka-topics --bootstrap-server kafka:29092 --list` 能正常返回

### 8.2 第 2 层：主链路兼容性验证

迁移后还必须验证：

- 数据生成器还能往 `user_behavior_events` 发消息
- Flink SQL 还能继续消费 Kafka source
- 前面章节依赖 Kafka 的脚本不被打断

这里的验证重点不是重新设计上层业务，而是证明：

**控制面升级之后，业务入口保持稳定。**

### 8.3 第 3 层：文档与叙事验证

文档需要明确记录：

- 为什么前期先用 ZooKeeper
- 为什么后来必须升级到 KRaft
- 真实遇到过哪些 ZooKeeper 模式问题
- KRaft 迁移后解决了什么
- 哪些对外行为保持不变

这部分同样是本章正式交付的一部分。

## 9. 文件范围

本章预计会涉及：

- `infra/.env.example`
- `infra/docker-compose.yml`
- `infra/compose/kafka/README.md`
- `README.md`
- `scripts/run_flink_sql_job.ps1`
- `scripts/run_chapter_5_iceberg_pipeline.ps1`
- `scripts/verify_chapter_5_end_to_end.ps1`
- `tests/test_flink_sql_job.py`
- 与 Kafka/Compose 相关的产物测试
- 第 7 章设计与实现文档

## 10. 风险与边界

### 10.1 风险

本章主要风险包括：

- KRaft listener 配置写错，导致 controller 与 broker 互相不可见
- 迁移时如果直接改掉 broker 名称，可能打断现有脚本和 Flink 配置
- 本地环境中 KRaft 存储初始化步骤如果处理不好，容易出现重复启动问题

### 10.2 边界

本章不解决：

- 多 broker 扩容
- 副本高可用
- 分区级容灾
- 生产级 controller quorum

这些内容可以作为未来扩展，但不应挤进当前章节。

## 11. 完成标准

第 7 章完成后，至少应满足：

- Compose 中不再存在 ZooKeeper
- Kafka 成功切换到 KRaft 模式
- `controller + broker` 双角色拓扑成立
- `localhost:9092` 和 `kafka:29092` 使用方式保持可用
- 数据生成器可继续发消息
- Flink 可继续消费
- 文档完整记录这次迁移的工程动机、改造方案和验证结果

## 12. 面试表达

这一章非常适合形成一段高质量面试故事：

“项目初期我先用 ZooKeeper 模式快速搭起 Kafka 消息入口，目的是尽快跑通 `生成器 -> Kafka -> Flink` 主链路。随着后面 Chapter 4、5、6 反复做端到端验证，我真实遇到了 ZooKeeper 模式下 broker 重建残留状态、`NodeExistsException`、topic 状态恢复不稳定这类问题。到第 7 章，我没有继续只在脚本层兜底，而是把 Kafka 控制面升级到 KRaft。迁移时我没有改业务入口，外部还是只连 broker 的 `9092`，内部则拆成 `controller + broker` 双角色。这样既保住了上层 Flink 和验证脚本的兼容性，也让我能讲清楚从 ZooKeeper 到 KRaft 的架构演进思路。”

## 13. 最终落地与回归证据

截至 2026-07-14，本章设计中的迁移已经完成，不再是未来方案：

- Compose 已删除 ZooKeeper 服务，运行 `kafka-controller` 与 `kafka-broker` 两个独立角色。
- controller 使用节点 ID 1 维护 KRaft metadata log，broker 使用节点 ID 2 提供消息读写。
- broker 继续保留容器名 `ecom-kafka` 和主机名 `kafka`，因此宿主机入口 `localhost:9092`、容器内入口 `kafka:29092` 均未改变。
- controller 端口不暴露给宿主机，业务脚本仍然只探测 broker，保持控制面与数据面职责分离。

### 13.1 真实启动时序

在 KRaft 容器被强制重建时，controller 会先恢复 metadata log 并成为 leader；broker 随后注册、追平 controller 高水位并解除 fenced 状态。这个过程可能出现短暂的 `DuplicateBrokerRegistrationException` 或 broker 尚未接受连接，但最终会进入 `RUNNING`。

因此验证脚本不能只依赖容器 Running，而应通过 `kafka-topics --bootstrap-server kafka:29092` 探测业务就绪，并使用 `--create --if-not-exists` 幂等保障 topic。

### 13.2 上层兼容性回归

迁移后已重新验证前置链路：

- 第 7 章 KRaft 产物测试与 Flink runner 契约测试通过。
- 数据生成器与 Flink SQL 仍使用原有 broker 地址，不需要理解 controller。
- 第 5 章 filesystem Iceberg 回归脚本可在 KRaft 环境下重新拉起 Kafka 与 Flink。
- filesystem Iceberg 作业 `f211e3f7b4a82c491d01057e1bd59623` 成功提交并保持 `RUNNING`。

这证明本次演进不仅替换了 Kafka 控制面，也保持了 `Kafka -> Flink -> Iceberg` 业务链路兼容。

### 13.3 当前边界

当前 `1 controller + 1 broker` 是教学与本地开发拓扑，用来讲清角色拆分、listener 和 quorum 语义，不具备生产高可用。后续若演进到 `3 controller + 3 broker`，还需要补充副本因子、故障转移、滚动升级、容量与压测设计；这些不属于本章当前交付范围。
