# 第 7 章 KRaft 提交与面试指南

## 1. 当前工作区判断

当前工作区里同时混着几类改动：

- 第 7 章 KRaft 迁移与运行时修复
- 第 7 章相关文档回写
- 第 6 章 Trino / Hive Metastore 方向的改动
- 大量历史未提交的新增文件
- `.superpowers/sdd/` 下的排障与 review 临时产物

因此不建议“一把梭”直接提交全部改动。最稳妥的做法是按主题拆提交，并且明确排除 `.superpowers/sdd/*.diff` 这类临时文件。

## 2. 推荐提交策略

### 方案 A：只先收口第 7 章

如果你现在最想先把 `ZooKeeper -> KRaft` 这一章干净落地，建议拆成 3 个提交。

#### Commit 1：KRaft 基础设施与运行时修复

目标：先把最核心的可运行改动收进去。

建议包含：

- `infra/docker-compose.yml`
- `scripts/run_flink_sql_job.ps1`
- `scripts/run_chapter_5_iceberg_pipeline.ps1`
- `tests/test_flink_sql_job.py`

建议命令：

```powershell
git add infra/docker-compose.yml
git add scripts/run_flink_sql_job.ps1
git add scripts/run_chapter_5_iceberg_pipeline.ps1
git add tests/test_flink_sql_job.py
git commit -m "fix: validate chapter 7 kraft runtime"
```

这个提交对应的价值是：

- 修复 broker 在 KRaft 模式下的 listener protocol 映射问题
- 修复 Flink runner 假成功问题
- 修复 Chapter 5 runner 在干净工作区缺少 `tmp` 目录的问题
- 把这些运行时约束补进测试

#### Commit 2：第 7 章文档与项目叙事回写

目标：把“已经落地的 KRaft 演进”写回项目文档。

建议包含：

- `README.md`
- `docs/superpowers/specs/2026-07-07-chapter-1-compose-design.md`
- `docs/superpowers/plans/2026-07-07-chapter-1-compose-implementation.md`
- `docs/superpowers/specs/2026-07-07-chapter-4-doris-fastapi-design.md`
- `docs/superpowers/plans/2026-07-07-chapter-4-doris-fastapi-implementation.md`
- `docs/superpowers/plans/2026-07-10-chapter-7-kraft-migration-implementation.md`

建议命令：

```powershell
git add README.md
git add docs/superpowers/specs/2026-07-07-chapter-1-compose-design.md
git add docs/superpowers/plans/2026-07-07-chapter-1-compose-implementation.md
git add docs/superpowers/specs/2026-07-07-chapter-4-doris-fastapi-design.md
git add docs/superpowers/plans/2026-07-07-chapter-4-doris-fastapi-implementation.md
git add docs/superpowers/plans/2026-07-10-chapter-7-kraft-migration-implementation.md
git commit -m "docs: record chapter 7 kraft evolution"
```

这个提交对应的价值是：

- 把第 1 章的“未来要迁 KRaft”改成“第 7 章已真实落地”
- 把第 4 章的 ZooKeeper 痛点和第 7 章 KRaft 迁移因果关系写实
- 把第 7 章运行时排障证据沉淀成正式文档

#### Commit 3：面试与交付辅助文档

目标：把你对外表达会用到的材料单独收好。

建议包含：

- `docs/CHAPTER_7_KRAFT_COMMIT_AND_INTERVIEW_GUIDE.md`

建议命令：

```powershell
git add docs/CHAPTER_7_KRAFT_COMMIT_AND_INTERVIEW_GUIDE.md
git commit -m "docs: add chapter 7 commit and interview guide"
```

这样做的好处是：代码修复、项目文档、面试材料三层边界清晰，后面回看非常舒服。

### 方案 B：先做一个最小第 7 章交付提交

如果你现在只想快速形成一个“第 7 章收尾”提交，也可以把上面 Commit 1 和 Commit 2 合并。

建议命令：

```powershell
git add infra/docker-compose.yml
git add scripts/run_flink_sql_job.ps1
git add scripts/run_chapter_5_iceberg_pipeline.ps1
git add tests/test_flink_sql_job.py
git add README.md
git add docs/superpowers/specs/2026-07-07-chapter-1-compose-design.md
git add docs/superpowers/plans/2026-07-07-chapter-1-compose-implementation.md
git add docs/superpowers/specs/2026-07-07-chapter-4-doris-fastapi-design.md
git add docs/superpowers/plans/2026-07-07-chapter-4-doris-fastapi-implementation.md
git add docs/superpowers/plans/2026-07-10-chapter-7-kraft-migration-implementation.md
git commit -m "feat: complete chapter 7 kraft migration"
```

这个方案更快，但不如 3 提交方案那么利于后续复盘。

## 3. 当前不建议一起提交的内容

下面这些内容建议暂时不要和第 7 章混提：

- `infra/compose/trino/catalog/lakehouse.properties`
- `jobs/README.md`
- `scripts/verify_chapter_6_trino_queries.ps1`
- `tests/test_chapter_6_trino_artifacts.py`
- `.superpowers/sdd/*.diff`
- `.superpowers/sdd/task-*-brief.md`
- `.superpowers/sdd/task-*-review-package*.diff`

原因很简单：

- 它们不是第 7 章 KRaft 的核心交付
- 混进去以后会把“架构迁移”这条线讲散
- 尤其 `.superpowers/sdd/` 下很多是过程产物，不适合进入正式提交历史

## 4. 提交前最后检查

推荐每次 commit 前都跑：

```powershell
python -m unittest tests.test_chapter_7_kraft_artifacts tests.test_flink_sql_job -v
```

如果你要提交 Chapter 5 兼容性证据，再补跑：

```powershell
$env:DOCKER_HOST='npipe:////./pipe/dockerDesktopLinuxEngine'
./scripts/verify_chapter_5_end_to_end.ps1
```

## 5. 1 分钟面试口述稿

“我这个项目前期先用 ZooKeeper 模式快速把 Kafka 搭起来，目的是尽快跑通 `数据生成器 -> Kafka -> Flink` 这条主链路。后面随着 Doris、Iceberg、Hive Metastore、Trino 一步步接进来，ZooKeeper 模式开始暴露真实问题，比如 broker 重建后的状态残留、`NodeExistsException`，以及 topic 恢复不稳定。到第 7 章我没有继续只在脚本层打补丁，而是把 Kafka 迁移到了 KRaft，并且不是最简单的单进程混合模式，而是拆成了 `controller + broker` 双角色。迁移时我保留了 `ecom-kafka`、`localhost:9092`、`kafka:29092` 这些既有入口，所以 Flink 和验证脚本基本不用重写。最后我又通过真实运行验证解决了 broker listener 映射和 Docker Desktop context 这类问题，最终把 Chapter 5 端到端链路重新跑通，这样整段架构演进故事就比较完整。”

## 6. 3 分钟面试口述稿

“这个项目一开始我没有直接堆最复杂的生产级架构，而是先用 ZooKeeper 模式把 Kafka 跑起来，目的是尽快验证 `生成器 -> Kafka -> Flink SQL` 这一段最小主链路。这一步在项目早期是合理的，因为它能降低起步复杂度，让我先把主流程做通。

但后面项目不断扩展，接入了 Doris、FastAPI、MinIO、Iceberg、Hive Metastore 和 Trino 之后，Kafka 已经不再只是一个单独能启动的中间件，而是整条实时链路的前置基础设施。这个阶段 ZooKeeper 模式开始暴露真实问题，比如本地反复 `docker compose up --force-recreate` 时 broker 残留状态、`NodeExistsException`，以及 Kafka 恢复之后 topic 和业务链路不一定自动恢复。这些问题让我意识到，不能再只在脚本层做兜底，而是应该做一次真正有工程动机的基础设施演进。

所以我在第 7 章把 Kafka 迁移到 KRaft。为了让这个演进更有含金量，我没有采用最简单的单进程 `broker,controller` 混合模式，而是做成了 `1 controller + 1 broker` 的双角色拓扑。这样我可以把 `process.roles`、`controller.quorum.voters`、listener 分工这些 KRaft 的核心概念讲清楚。同时我又刻意保持了业务入口不变，对外还是 `localhost:9092`，容器内还是 `kafka:29092`，broker 容器名也继续保留 `ecom-kafka`，这样上层 Flink SQL、生成器和验证脚本都不需要大改。

真正有价值的是迁移过程中的运行时排障。比如我一开始发现 Docker Desktop 进程都在，但 `docker version` 会卡，最后定位到是默认 context 走的 pipe 不稳定，显式切到 `desktop-linux` context 才恢复。再比如第一次拉起 KRaft broker 时，不是网络问题，而是旧的 `ecom-kafka`、`ecom-zookeeper` 先占住了名字；清掉旧容器后，controller 很快就起来了，但 broker 又因为缺少 `CONTROLLER` listener 的 protocol map 启动失败。最后我补齐了 broker 和 controller 两边的 listener 映射，broker 才真正稳定起来。

最后我不是只看容器状态，而是做了两层验证。第一层是 KRaft 自身验证，看 controller 和 broker 日志里都出现了明确的 `KafkaRaftServer`、`QuorumController` 和 broker 监听端口信息。第二层是业务兼容验证，我重新跑了第 5 章的端到端脚本，确认 Flink SQL Iceberg 作业还能继续消费 `kafka:29092` 并把数据写到 MinIO/Iceberg。这样这次迁移就不只是‘升级了 Kafka’，而是形成了一段从 ZooKeeper 到 KRaft、从脚本兜底到控制面升级的完整架构演进故事。”

## 7. 追问时可以补的亮点

如果面试官继续追问，你可以顺着补这几句：

- “我故意保留了 broker 入口不变，这是为了让控制面升级不把上层业务调用一起推翻。”
- “我没有直接做 3 controller 或 3 broker，是因为当前项目目标是教学型本地演进，先把双角色 KRaft 跑实，比硬堆伪生产拓扑更有含金量。”
- “这次迁移最关键的不是删掉 ZooKeeper，而是把前面 Chapter 4 遇到的 ZooKeeper 状态问题，变成一次有动机、有证据的架构升级。”
