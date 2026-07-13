# 第 1 章 Compose 基础设施骨架实现计划

> **给执行型智能体的说明：** 实现本计划时，建议使用 `superpowers:subagent-driven-development` 或 `superpowers:executing-plans` 子技能按任务逐步推进。任务使用 `- [ ]` 复选框格式跟踪。

**目标：** 搭建一个分阶段的 Docker Compose 基础设施骨架，先引入 Kafka 和 API 占位服务，同时保留后续章节扩展到 Flink、Doris、MinIO 和 KRaft 演进的清晰路径。

**架构说明：** 以 `infra/docker-compose.yml` 作为唯一编排入口，把组件说明拆到 `infra/compose/` 子目录，用环境变量模板统一管理端口和服务名。当前阶段只启用 ZooKeeper、Kafka 和占位 HTTP 服务，后续再逐章扩展。

**技术栈：** Docker Compose、Kafka、ZooKeeper、Nginx 占位服务、Markdown 文档

---

### 任务 1：新增 Compose 主入口文件

**文件：**
- 新增：`infra/docker-compose.yml`
- 新增：`infra/.env.example`
- 验证：`docker compose --env-file infra/.env.example -f infra/docker-compose.yml config`

- [ ] **步骤 1：创建 Compose 主文件**

```yaml
name: ${PROJECT_NAME}
```

- [ ] **步骤 2：创建环境变量模板**

```dotenv
PROJECT_NAME=ecommerce-lakehouse-ai
```

- [ ] **步骤 3：执行 Compose 静态校验**

执行：`docker compose --env-file infra/.env.example -f infra/docker-compose.yml config`  
预期：Compose 文件能被正确展开，不出现语法或变量插值错误。

### 任务 2：新增组件占位目录和配置

**文件：**
- 新增：`infra/compose/kafka/README.md`
- 新增：`infra/compose/app/README.md`
- 新增：`infra/compose/flink/README.md`
- 新增：`infra/compose/doris/README.md`
- 新增：`infra/compose/minio/README.md`
- 新增：`infra/compose/app/default.conf`
- 新增：`infra/compose/app/index.html`

- [ ] **步骤 1：创建组件说明文件**

```markdown
# Kafka Compose Notes
```

- [ ] **步骤 2：添加 API 占位配置**

```nginx
location = /health {
    return 200 "api-placeholder-ok\n";
}
```

- [ ] **步骤 3：确认 Compose 已引用占位文件**

执行：`docker compose --env-file infra/.env.example -f infra/docker-compose.yml config`  
预期：`api` 服务中能看到 `default.conf` 和 `index.html` 的挂载配置。

### 任务 3：更新第 1 章文档说明

**文件：**
- 修改：`README.md`
- 新增：`docs/superpowers/specs/2026-07-07-chapter-1-compose-design.md`
- 新增：`docs/superpowers/plans/2026-07-07-chapter-1-compose-implementation.md`

- [ ] **步骤 1：补充分阶段 Compose 策略说明**

```markdown
## 第 1 章基础设施初始化
```

- [ ] **步骤 2：补充未来 KRaft 演进说明**

```markdown
### 为什么现在先不用 KRaft
```

- [ ] **步骤 3：回读文档检查术语一致性**

执行：`rg "KRaft|Compose|ZooKeeper" README.md docs/superpowers/specs/2026-07-07-chapter-1-compose-design.md`  
预期：README 与设计文档中的分阶段策略、ZooKeeper 起步和 KRaft 演进表述保持一致。

---

## 后续演进结果回写

后续章节已经把这里预留的演进路线真正走完了一段关键升级：

- 第 1 章起步时使用的是 `ZooKeeper + Kafka`
- 第 7 章已经迁移为 KRaft `controller + broker` 双角色拓扑
- 对业务侧入口保持 `ecom-kafka`、`kafka:29092`、`localhost:9092` 不变

这意味着第 1 章当初的实现计划不是一次性脚手架，而是成功支撑了后续真实的基础设施重构。对项目讲述来说，这一点很重要，因为它能体现“先做最小可用，再做有动机的架构升级”，而不是一开始就堆一个过重的伪生产方案。
