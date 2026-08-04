# 第 10 章：受控工具调用运行手册

## 目标与架构

`POST /analysis/tools` 只允许后端注册的三个只读工具：

- 实时指标：`get_realtime_metrics` 从 Doris 读取 PV、UV 和更新时间。
- 历史行为：`get_historical_behavior_summary` 从 Trino + Iceberg 读取事件汇总。
- 数据质量：`get_data_quality_health` 通过固定的 Flink REST 地址读取生产 Job、checkpoint 和六个质量计数器。

请求先由 planner 选择工具，再由后端 Registry 按固定顺序执行，最后由受控的 claim 模板生成叙事。响应包含每个工具的 `evidence`、调用摘要、`degraded` 和唯一 `audit_id`。`POST /analysis/realtime` 仍是第 8 章接口，和本章接口独立回归。

## 安全边界

- 三个工具没有请求参数；调用方和模型都不能指定 SQL、表、URL、Flink Job、metric ID 或连接地址。
- Flink 仅允许读取固定 REST 地址上的 Job 概览、checkpoint 和指标；不修改 Job、不触发 savepoint、不重启容器。
- 验收脚本只向 `/analysis/tools` 发出 5 次 POST 请求，不发送 Kafka 数据，不启动或停止服务，也不执行数据库写操作。
- 工具 ID、执行顺序、调用上限、超时和 evidence 模型均由后端控制。失败时不放宽白名单或 evidence 校验。
- 第 10 章不是 NL2SQL：模型不能生成或执行 SQL，SQL 模板仍由后端固定实现。

## 配置与前提

默认配置在 `infra/.env.example` 中：

```text
AI_TOOL_PLANNER_MODE=rule_based
AI_ANALYZER_MODE=rule_based
AI_TOOL_MAX_CALLS=3
AI_TOOL_TOTAL_TIMEOUT_SECONDS=20
AI_TOOL_MAX_EVENT_TYPES=20
FLINK_REST_URL=http://flink-jobmanager:8081
CHAPTER9_PRODUCTION_JOB_NAME=chapter-9-datastream-quality-production
```

默认 `rule_based` 模式不需要 API Key，适合真实验收。可选模型模式使用 `openai_compatible`：将 `AI_TOOL_PLANNER_MODE` 设置为 `openai_compatible` 可让模型选择三个固定工具，将 `AI_ANALYZER_MODE` 设置为 `openai_compatible` 可让模型选择受控 claim；两者都需要 `AI_API_KEY`、`AI_BASE_URL` 和 `AI_MODEL`。无论是否启用模型，模型都不能传入工具参数或生成 SQL。

验收前，已存在的 FastAPI、Doris、Trino、Flink REST 和唯一的第 9 章生产质量 Job 必须可用，且该 Job 为 `RUNNING` 并有成功 checkpoint。按项目既有方式启动所需服务后，确认接口可访问：

```powershell
Invoke-WebRequest -UseBasicParsing http://localhost:8000/health
```

## API 示例

```powershell
$body = @{ question = "做一次综合分析" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://localhost:8000/analysis/tools `
  -ContentType "application/json" -Body $body
```

四类问题及预期工具顺序如下：

| 问题类型 | 预期工具 |
| --- | --- |
| 实时指标 | `get_realtime_metrics` |
| 历史行为 | `get_historical_behavior_summary` |
| 数据质量 | `get_data_quality_health` |
| 综合分析 | 按实时、历史、质量的固定顺序调用三个工具 |

每个成功响应都必须满足：`audit_id` 是非空 UUID 且与本轮其他请求不同；全部 `tool_calls.status` 为 `success`；每个已调用工具有对应的非空 `evidence`；`degraded=false`。提示注入请求也只能使用三个白名单工具，响应 JSON 不得包含 SQL 关键字或 URL。

## 审计与降级

结构化日志用 `audit_id` 串联请求。允许的审计字段包括 planner/analyzer 名称、工具 ID、调用状态、阶段、耗时、`degraded` 和异常类型；不得记录 API Key、密码、Authorization、完整 prompt、SQL、URL、原始模型输出、异常消息或 stack。

单工具不可用时，服务可以返回其余 evidence 并设置 `degraded=true`。主模型 planner 返回非法结构、未知/重复工具或额外字段时，主计划整体作废，不执行其中任何工具，并降级到规则 planner；只有规则 fallback 计划也失败时才返回固定 `503`。所有工具不可用或响应无法安全校验时同样返回固定 `503`：`analysis tools are temporarily unavailable`。真实验收不接受任何降级响应，必须修复依赖后重跑，不得跳过断言。

## 严格验收

在仓库根目录运行：

```powershell
./scripts/verify_chapter_10_tool_analysis.ps1
```

脚本依次请求实时、历史、质量、综合和提示注入五个问题，只读验证 HTTP 200、工具白名单和顺序、对应 evidence、唯一 `audit_id`、`degraded=false`，以及注入响应不泄露 SQL 或 URL。通过时最后一行是：

```json
{"status":"PASS","requests":5,"tools_verified":3,"prompt_injection_blocked":true}
```

## 失败排障

- 收到固定 `503`：检查 FastAPI、Doris、Trino、Flink REST 是否可用，以及生产质量 Job 是否唯一、`RUNNING` 且存在成功 checkpoint；不要把 `degraded=true` 当作通过。
- 工具顺序或 evidence 不符：确认使用默认 `rule_based` 配置，检查接口返回的 `tool_calls` 和 evidence 分区；不要改变脚本期望来掩盖问题。
- 注入断言失败：检查 planner、Registry、响应序列化和日志边界，确保未注册工具、SQL 和 URL 都不进入响应。
- Docker 引擎不可用：修复宿主机虚拟化或 Docker 引擎后，直接重跑上面的原始严格命令；不要改为模拟响应或降低断言。

## 面试叙事

第 8 章把查询权收回后端并输出可信 evidence；第 10 章再增加受控工具选择，但工具、数据源、预算、超时、输出模型和审计仍由后端掌握。这样能展示 Agent 的工具编排能力，同时不把数据库或运维权限交给模型；受控 NL2SQL 留到第 11 章。
