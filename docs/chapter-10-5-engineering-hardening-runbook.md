# 第 10.5 章：工程可靠性加固运行手册

本章把历史现场才能运行的链路收敛为可重复启动、可判断就绪、可受控迁移的本地工程环境。所有命令都在仓库根目录执行。本方案仍是本地单机演示，不代表生产 HA，也不能直接暴露到公网。

## 全新克隆

1. 确认 Docker Desktop 已启动，`docker version` 能连接 daemon。
2. 从示例生成本地配置：

```powershell
Copy-Item infra/.env.example infra/.env
```

3. 使用唯一推荐入口完成依赖校验、Compose 启动、初始化、Catalog/Job 恢复和严格验收：

```powershell
./scripts/bootstrap_chapter_10_5.ps1
```

Bootstrap 默认非破坏且可重复执行。完整隔离验收使用：

```powershell
./scripts/verify_chapter_10_5_cold_start.ps1 -TimeoutMinutes 45
```

## 已有环境迁移

首次从旧环境迁移前必须先暂停 Generator 和其他写入流量。确认流量已停后，显式提供两个确认参数：

```powershell
./scripts/migrate_chapter_10_5.ps1 -TrafficPaused -ConfirmRealtimeReset
```

迁移将保留 MinIO 湖明细，受控重建实时状态并切换到 PostgreSQL Metastore。报告写入 `tmp/chapter-10-5/migration-report.json`。这不承诺 Kafka offset、Doris 实时状态或 Flink 去重状态无损延续；失败时保留现场，不自动回滚。

## 日常幂等启动

日常只执行同一个入口；不要先删除卷或 MinIO 数据：

```powershell
./scripts/bootstrap_chapter_10_5.ps1
```

脚本会复用 Topic、固定 Iceberg 表和唯一 RUNNING 的第 9 章生产 Job。报告位于 `tmp/chapter-10-5/bootstrap-report.json`。只有需要跳过 Java 构建且已有正确产物时才使用 `-SkipBuild`。

## 实时层 Reset

仅在确认可以丢弃并重建实时层状态时执行：

```powershell
./scripts/reset_chapter_10_5_realtime.ps1 -ConfirmReset
```

脚本只允许处理五个命名卷：`kafka-controller-data`、`kafka-broker-data`、`doris-fe-meta`、`doris-be-storage`、`metastore-postgres-data`，以及 `flink-state` bucket 中第 9 章固定 checkpoint/savepoint 前缀。**不得删除 warehouse**，也不得手工扩大卷名或对象前缀。报告位于 `tmp/chapter-10-5/realtime-reset-report.json`。

MinIO 的两个 bucket 职责固定：`warehouse` 保存 Iceberg 湖表，`flink-state` 保存 Flink checkpoint/savepoint。

## Catalog 恢复

PostgreSQL Metastore 已启动但固定表缺失时，执行：

```powershell
./scripts/restore_chapter_10_5_catalog.ps1 -EnvFile infra/.env
```

恢复脚本只注册 `lakehouse.analytics.user_behavior_detail`。metadata 缺失、冲突或 location 不一致时会 fail closed，不会猜测或批量注册其他表。

## Readiness 诊断

`/health` 只证明 FastAPI 进程存活；`/ready` 还会检查 Doris、Trino 固定表、唯一生产 Flink Job 和新鲜 checkpoint，任一不满足均返回安全的 503。

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8000/health
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8000/ready
docker compose --env-file infra/.env -f infra/docker-compose.yml --profile flink --profile serving --profile lakehouse ps
```

先看 `tmp/chapter-10-5/bootstrap-report.json` 的失败阶段，再检查对应服务；不要用“容器 running”替代 `/ready`。

## 线程池饱和诊断

`POST /analysis/tools` 的 planner、repository、工具执行和 narrative 共用一个统一 deadline。同步阻塞调用由有界线程池隔离：`AI_TOOL_EXECUTOR_MAX_WORKERS` 允许 `1..3`，默认 `3`；`AI_TOOL_TOTAL_TIMEOUT_SECONDS` 默认 `20`。

满载或总预算耗尽时接口应快速返回 503，且不会继续创建线程。排查时先核对 `infra/.env` 的这两个值，再查看 API 普通日志；不要通过增大 worker 数掩盖下游 Doris、Trino 或 Flink 阻塞。

