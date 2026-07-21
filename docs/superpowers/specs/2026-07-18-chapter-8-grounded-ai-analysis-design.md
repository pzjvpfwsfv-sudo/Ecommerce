# 第 8 章：基于可信指标上下文的 AI 分析助手设计

## 1. 背景

前 7 章已经形成两条可运行的数据链路：

- 实时指标链路：`Kafka -> Flink -> Doris -> FastAPI`
- 湖仓分析链路：`Kafka -> Flink -> Iceberg/MinIO -> Hive Metastore -> Trino`

项目已经具备实时 PV/UV、历史行为明细和多引擎查询能力，但这些能力目前仍以接口、SQL 和中间件结果为主。第 8 章需要把已有数据能力转化为业务人员能直接理解的分析结论。

本章不让模型直接生成 SQL，而是由后端先查询可信数据，再让模型只基于这些证据生成解读。这样既能体现 AI 能力，也能控制幻觉、权限和查询风险。

## 2. 本章目标

本章实现一个最小、可信、可测试的 AI 指标分析闭环：

1. 接收用户的业务分析问题。
2. 从 Doris 获取实时 PV/UV。
3. 从 Trino 获取历史事件分布等预定义统计结果。
4. 组装统一的 `AnalysisContext`。
5. 调用分析器生成结构化业务解读。
6. 返回结论、发现、风险、建议和原始证据。

本章的核心价值不是“接入一个聊天接口”，而是建立以下工程边界：

- 数据查询由后端控制。
- 模型只负责解释，不负责创造事实。
- 没有模型凭证时仍可演示和测试。
- 后续可以自然演进到工具调用和受控 NL2SQL。

## 3. 方案选择

### 3.1 采用方案

本章采用“可信指标上下文 + 模型解读”方案：

```text
用户问题
  -> 后端执行预定义查询
  -> 组装可信指标上下文
  -> 分析器生成结构化解读
  -> 返回分析结果与证据
```

### 3.2 暂不采用的方案

本章暂不采用以下方案：

- 模型自由选择任意数据库工具。
- 模型直接生成并执行 SQL。
- 多轮自主 Agent。
- 向量数据库和 RAG 知识库。
- 复杂前端聊天页面。

这些能力会增加权限、审计、成本和错误恢复复杂度，不适合作为 AI 主线的第一步。

## 4. 总体架构

第 8 章复用现有 FastAPI 服务，在其中增加分析应用层，不另起一个重复的 API 服务。

```text
POST /analysis/realtime
        |
        v
AnalysisService
  |              |
  v              v
DorisMetrics   TrinoAnalytics
Repository     Repository
  |              |
  +-------> AnalysisContext
                  |
                  v
             MetricAnalyzer
              |         |
              v         v
       RuleBased      OpenAICompatible
       Analyzer       Analyzer
                  |
                  v
            AnalysisResponse
```

组件职责保持单一：

- Repository 只负责查询和数据类型转换。
- `AnalysisService` 只负责流程编排与证据组装。
- `MetricAnalyzer` 只负责根据上下文生成分析。
- FastAPI 路由只负责请求校验、依赖注入和响应映射。

## 5. 数据来源设计

### 5.1 Doris 实时指标

第一版复用现有 `analytics.realtime_metrics` 表，至少提供：

- `pv`
- `uv`
- 指标更新时间

实时指标用于回答“现在流量怎么样”“当前访问规模如何”等问题。

### 5.2 Trino 历史统计

第一版只执行后端预定义的只读聚合查询，例如：

- 总行为事件数
- 按 `event_type` 聚合的事件数量
- 数据集中最近的事件时间

Trino 查询不接受模型生成的 SQL，也不直接拼接用户输入。

### 5.3 AnalysisContext

统一上下文建议包含：

```json
{
  "question": "当前用户活跃情况如何？",
  "generated_at": "2026-07-18T12:00:00+08:00",
  "realtime": {
    "pv": 120,
    "uv": 80,
    "updated_at": "2026-07-18T11:59:55"
  },
  "historical": {
    "event_count": 1000,
    "event_type_counts": {
      "view": 700,
      "click": 200,
      "cart": 70,
      "purchase": 30
    }
  },
  "warnings": []
}
```

所有交给模型的业务数字都必须来自该上下文。

## 6. API 契约

### 6.1 请求

新增接口：

```http
POST /analysis/realtime
Content-Type: application/json
```

请求体：

```json
{
  "question": "当前用户活跃情况如何？"
}
```

约束：

- `question` 必填。
- 去除首尾空白后不能为空。
- 第一版限制合理长度，避免无界 Prompt。

### 6.2 响应

成功响应：

```json
{
  "summary": "当前累计访问 120 次，覆盖 80 名用户。",
  "insights": [
    "人均访问次数约为 1.5 次。",
    "历史行为以浏览为主。"
  ],
  "risks": [
    "当前数据为累计指标，不能直接代表分钟级趋势。"
  ],
  "actions": [
    "下一步补充时间窗口指标，再判断流量变化方向。"
  ],
  "evidence": {
    "realtime": {
      "pv": 120,
      "uv": 80
    },
    "historical": {
      "event_count": 1000,
      "event_type_counts": {
        "view": 700,
        "click": 200
      }
    }
  },
  "analyzer": "rule_based",
  "generated_at": "2026-07-18T12:00:00+08:00"
}
```

响应必须始终携带 `evidence`，让用户可以核对分析依据。

## 7. 分析器设计

### 7.1 MetricAnalyzer 接口

应用层依赖抽象接口，而不是直接依赖某个模型 SDK：

```python
class MetricAnalyzer(Protocol):
    def analyze(self, context: AnalysisContext) -> AnalysisResult:
        ...
```

这样可以在不修改路由和查询逻辑的情况下替换分析器。

### 7.2 RuleBasedAnalyzer

规则分析器是第一版必须具备的基础实现，用于：

- 无 API Key 时完整演示。
- 单元测试和端到端测试。
- 模型服务不可用时降级。
- 给模型输出建立可比较的基线。

第一版规则可以稳定计算：

- `PV / UV` 人均访问次数。
- UV 为 0 时的数据不足提示。
- 各事件类型占比。
- 浏览到点击、加购、购买的粗粒度行为分布提示。

规则输出不能把累计指标描述成实时趋势，也不能在没有时间对照数据时声称“上涨”或“下降”。

### 7.3 OpenAICompatibleAnalyzer

模型分析器通过配置启用，输入为系统约束、用户问题和序列化后的 `AnalysisContext`。

模型提示必须要求：

- 只能使用上下文中的事实和数字。
- 不得生成 SQL。
- 不得声称访问了上下文之外的数据。
- 数据不足时明确说明限制。
- 按固定结构返回结果。

模型输出需要经过结构校验；缺字段、类型错误或解析失败时视为模型调用失败。

### 7.4 降级策略

默认策略：

1. 未配置模型时直接使用规则分析器。
2. 已配置模型但调用失败时，记录错误并降级到规则分析器。
3. 响应中的 `analyzer` 明确标记实际使用的分析器。
4. 不因为外部模型不可用而让可信指标查询整体失败。

## 8. 配置设计

建议新增以下配置：

- `AI_ANALYZER_MODE=rule_based|openai_compatible`
- `AI_API_KEY`
- `AI_BASE_URL`
- `AI_MODEL`
- `AI_REQUEST_TIMEOUT_SECONDS`
- `AI_MAX_QUESTION_LENGTH`

安全要求：

- `.env.example` 只保留空值或示例值。
- 真实 API Key 不进入 Git。
- 日志不得打印 API Key 或完整认证头。

## 9. 错误处理

### 9.1 请求错误

- 问题为空或超长：返回 `422`。

### 9.2 数据源错误

- Doris 查询失败：返回 `503`，因为实时证据是第一版必需数据。
- Trino 查询失败：保留 Doris 实时证据，在 `warnings` 中标记历史数据暂不可用，继续生成降级分析。
- 指标缺失：不伪造 0，明确标记数据不足。

### 9.3 模型错误

- 超时、网络错误、限流、结构解析失败：降级到规则分析器。
- 降级信息进入服务日志，但响应不暴露密钥、内部堆栈或供应商敏感信息。

## 10. 可观测性

第一版至少记录：

- 请求 ID。
- 实际使用的 analyzer。
- Doris、Trino 和模型调用耗时。
- 是否发生降级。
- 错误类型，不记录密钥和完整 Prompt。

第一版不引入额外可观测性平台，先使用结构化应用日志。后续再接入指标系统和调用追踪。

## 11. 测试设计

### 11.1 单元测试

- `AnalysisContext` 组装正确。
- PV/UV 比率计算正确。
- UV 为 0 或指标缺失时不除零、不编造结论。
- 事件类型占比计算正确。
- Trino 失败时生成 warning 并保留 Doris 分析。
- 模型失败时降级规则分析器。

### 11.2 API 测试

- 合法问题返回固定结构。
- 空问题和超长问题返回 `422`。
- 响应始终包含 `evidence` 和 `analyzer`。
- Repository 和模型调用通过 mock 隔离，不依赖真实外部模型。

### 11.3 集成验证

- 启动 Doris、Trino 和 FastAPI。
- 查询真实 PV/UV 与 Iceberg 历史聚合。
- 在规则模式下验证完整接口。
- 配置模型后再做一次可选真实模型验证。

自动化测试不得要求真实 API Key，保证仓库克隆后可重复运行。

## 12. 文件规划

预计新增或调整：

- `services/api/app/analysis_models.py`
- `services/api/app/analysis_service.py`
- `services/api/app/analyzers.py`
- `services/api/app/trino_repository.py`
- `services/api/app/config.py`
- `services/api/app/main.py`
- `services/api/requirements.txt`
- `infra/.env.example`
- `infra/docker-compose.yml`
- `tests/test_analysis_service.py`
- `tests/test_analysis_api.py`
- `README.md`

同时删除已经被 FastAPI 替代的第 1 章 Nginx 占位目录：

- `infra/compose/app/default.conf`
- `infra/compose/app/index.html`
- `infra/compose/app/README.md`

## 13. 范围边界

本章包含：

- Doris 实时指标查询。
- Trino 预定义历史聚合查询。
- 规则分析器。
- 可选的 OpenAI-compatible 模型分析器。
- 模型失败自动降级。
- 结构化分析 API。

本章不包含：

- 模型自由生成 SQL。
- 任意表查询。
- 多轮会话记忆。
- RAG 知识库。
- 自动执行运营动作。
- 完整聊天前端。

## 14. 后续演进路线

### 14.1 第 8.2 阶段：趋势与异常

- 增加分钟级、小时级窗口指标。
- 增加环比、同比或基线对照。
- 增加可解释的异常检测。
- 让模型可以描述“上涨、下降、异常”，但仍必须引用计算证据。

### 14.2 第 9 章：受控工具调用

- 把预定义查询暴露为只读工具。
- 模型可以在有限工具集合中选择查询。
- 增加调用次数、超时和结果行数限制。
- 记录工具调用审计日志。

### 14.3 第 10 章：受控 NL2SQL

- 只允许 `SELECT`。
- 限定 catalog、schema、表和字段白名单。
- 禁止 DDL、DML、多语句和危险函数。
- 在执行前进行 SQL 解析、成本限制和超时控制。
- 返回生成 SQL、查询证据和审计 ID。

### 14.4 第 11 章：产品化与评测

- 增加分析问答界面。
- 展示证据、SQL、数据时间和降级状态。
- 建立问题集、准确性评测和回归基线。
- 接入调用链追踪、成本与延迟指标。

## 15. 完成标准

第 8 章完成后应满足：

- `POST /analysis/realtime` 可用。
- 规则模式不依赖外部模型即可完整运行。
- 接口可以读取 Doris 实时指标。
- Trino 可用时能补充历史行为统计。
- Trino 或模型失败时能按设计降级。
- 响应始终包含可信证据。
- 自动化测试不依赖真实 API Key。
- 文档能清楚解释为什么第一版不直接做 NL2SQL。

## 15.1 严格可信模式加固记录

- 数字只允许来自 `AnalysisContext.evidence` 或后端预定义派生值，禁止分析器引入无法追溯的新数字。
- 所有叙事先经过 NFKC 归一化，再由数字来源守卫按 fail-closed 策略校验；遇到未知字符、不可见格式或无法确认的分隔方式时直接拒绝，不猜测其含义。
- 当前只支持可见中文/英文数字分隔语义，明确不把任意 Unicode 字母、控制字符或混合书写自动视为安全自然语言分隔。
- 主分析器与回退分析器执行同一个数字来源守卫，避免降级路径绕过可信约束。
- 可观测性使用结构化 `LogRecord.extra` 字段记录 request ID、analyzer、阶段与耗时，不把凭证、Prompt 或原始异常文本拼进消息。
- 异常边界的实际保证是固定安全响应、普通日志不含异常消息或 stack，并以 `from None` 抑制默认 traceback context。Python `__context__` 对象仍可能存在，因此不宣称递归擦除 `__cause__` 或 `__context__` 对象；调用方也不应把捕获到的内部异常对象直接序列化或记录。
- 模型输出要求 `summary`、`insights`、`risks`、`actions` 四字段显式完整，缺字段或依赖默认值补齐均视为模型结果无效并触发降级。
- 安全边界是数值可追溯不等于整句语义正确：守卫不能证明因果关系、趋势判断或运营建议合理，后续仍需问题集评测、回归基线与结构化 claim 校验。

## 16. 面试表达

“前面的章节已经把 Kafka、Flink、Doris、Iceberg 和 Trino 链路跑通了。第 8 章我没有直接让大模型自由生成 SQL，因为那样很容易出现幻觉、越权和高成本查询。我先由后端执行受控查询，把 Doris 实时指标和 Trino 历史聚合组装成统一证据上下文，再让模型只负责解释。接口会同时返回分析结论和原始 evidence，而且没有模型凭证时可以使用规则分析器，模型超时或结构错误也会自动降级。后续再从预定义工具调用演进到带 SQL 白名单、解析和审计的 NL2SQL，这样 AI 能力是逐步增强的，而不是一开始就把数据库权限交给模型。”

## 最终审查加固

- 主分析器与回退分析器共用 SQL/代码输出守卫：拒绝代码围栏及结构化识别到的 SELECT、DDL、DML 语句；模型路径违规时安全降级，回退路径违规时返回固定安全失败。模型仍不得生成或执行 SQL，系统也没有引入 NL2SQL。
- 输入和叙事在 NFKC 后全局拒绝 Cc/Cf 控制或格式字符；数字 token 只接受 ASCII 0-9 的明确格式，并拒绝残留 Unicode 数字、中英文数字词或数量词及无穷等数值符号。
- 历史 evidence 由单条 Trino statement 返回总数、事件类型计数和最新时间；`try(from_iso8601_timestamp(event_time))` 使无效时间变为 NULL 后不参与 MAX。构造 evidence 前校验计数非负且分组和等于总数，否则按 Trino 不可用降级。
- `TRINO_CATALOG` 与 `TRINO_SCHEMA` 只接受严格 ASCII 标识符白名单，查询表名逐段安全双引号，statement 与 Trino header 使用同一配置。
- 真实脚本是隔离验证：先要求 PV、UV、updated_at 连续 3 次稳定，再发布两个唯一用户，并只接受 PV/UV 精确增长 2 且 updated_at 推进；并发或 backlog 导致 overshoot 会失败。该验证不是按 runId 从 Doris 做明细审计，不能据此宣称完成了事件级归因。
