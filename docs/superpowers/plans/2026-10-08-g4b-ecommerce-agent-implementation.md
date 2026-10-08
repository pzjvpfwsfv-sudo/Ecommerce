# G4-B E-commerce Grounded Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 交付代码化单 Agent，能结合知识库与 Olist/REES46 已发布指标回答电商问题，显示工具轨迹、可核对证据和可保存报告；无模型或模型失败仍有确定性摘要。

**Architecture:** 在 [G4-A](2026-10-08-g4a-knowledge-retrieval-implementation.md) 的 `KnowledgeSearchService` 上加一层 LangChain `create_agent`，模型仅从三个只读工具选择；工具由后端闭包绑定当前会话身份。模型产生带证据 ID 的结构化观点，数值、窗口和 run ID 由后端证据卡渲染，引用在报告读取时重新核验。供应商显式配置，默认关闭付费 API；远程兼容接口与本地 Ollama 共用工具/验证逻辑。

**Tech Stack:** FastAPI/Pydantic、LangChain、`langchain-openai`、`langchain-ollama`、现有 Doris 服务方法、PostgreSQL 应用库、React/Vitest/Playwright、Python unittest。

**Spec:** [G4 知识库与有据 AI 分析设计](../specs/2026-10-08-graduation-g4-knowledge-rag-design.md)。必须先完成 [G4-A 计划](2026-10-08-g4a-knowledge-retrieval-implementation.md)，不直接把旧 `/analysis/tools` 执行器的模拟实时指标当业务证据。

## Global Constraints

- 单 Agent、三个工具：`search_knowledge`、`get_published_behavior_metrics`、`get_published_order_metrics`；不得提供任意 SQL、网络、文件系统、写入或新增工具。
- 只读指标来自现有 `BehaviorMetricsService`/`OrderMetricsService` 的已发布响应，返回原始 `dataset_id`、`metric_version`、`metric_run_id`、窗口、`calculated_at`/发布时间及局限；不得跨 Olist/REES46 关联个人或合并漏斗。
- 默认 `G4_AGENT_PROVIDER=off`；`openai_compatible` 和 `ollama` 由本机环境显式选择。没有密钥不发远程请求；远程故障不自动冷启本地模型，转确定性证据摘要。
- 远程仅发送用户问题、最多五段有权短证据、聚合指标与最近三轮摘要；不上传原始 PDF/整库/明细/凭据。密钥只放未跟踪的本地环境配置，用户在官方平台自行登录并设消费限额。
- 单请求最大 3 次模型调用、5 次工具调用、800 输出 token、45 秒总 deadline；每个后端/模型请求也用剩余时间设置传输超时。不得用隐藏思维链或模型自报置信度做验收。
- 模型文本不能直接承担数字/日期/百分比事实；后端渲染指标证据卡。每条模型观点必须引用本轮可见证据 ID；无效 ID、注入、超时、空证据均拒绝或降级。
- 报告仅创建者/admin 可读；历史 `metric_run_id` 不静默替换；被撤回/删除的文档引用使报告失效并隐藏原回答正文。
- 本机 16 GB 内存、4 GB 显存，Ollama Qwen3 实测冷启动单句 22.07 秒，不预先声称全栈可用。下载、临时文件、模型/前端缓存和报告在 D 盘；Python 测试设置 `TEMP/TMP/PIP_CACHE_DIR` 为 D 盘指定目录。

## Review Focus

- 模型先调用某工具又试图调用不存在的 SQL/网络工具：第二次调用须被白名单和限次拦下，审计不泄漏参数原文；归 Task 2/3 测试。
- 订单/月窗口、行为/日窗口及非法维度/排序组合：返回 422 或工具错误，不由模型补造数字；归 Task 2 测试。
- 报告保存后新指标 run 发布或知识来源被撤回：旧 run 明确标历史，撤回文档使报告正文失效；归 Task 4 测试。
- 远程 API 429/超时或本地模型不可达：请求受 deadline 约束，显示真实降级状态，不自动换付费/本地服务；归 Task 1/3/5 测试。
- 用户问题或检索文档诱导改规则/泄密：模型输出必须经过证据 ID、数值/日期、角色和工具名检查；归 Task 3/6 测试。

## File Map

- `services/api/app/config.py`、`infra/.env.example`：新增隔离的 G4 provider/限次配置；不复用第 8 章 `AI_*` 开关，避免意外付费。
- `services/api/app/agent_provider.py`：`off`/OpenAI 兼容/Ollama 模型工厂及配置前置验证。
- `services/api/app/agent_models.py`：工具参数、证据、结构化答案、状态/轨迹与报告契约。
- `services/api/app/agent_tools.py`：三个只读 LangChain 工具和当前 `Principal` 的服务端闭包。
- `services/api/app/agent_service.py`：`create_agent` 循环、deadline/调用预算、证据核验和确定性降级。
- `services/api/app/agent_store.py`：最近三轮会话、报告与引用状态重校验；迁移 `infra/compose/app-postgres/migrations/003_agent_reports.sql`。
- `services/api/app/agent_api.py`：`/api/v1/agent` 路由；`main.py` 仅注册。
- `apps/web/src/lib/agent.ts`、`apps/web/src/modules/Agent.tsx`、`apps/web/src/styles/agent.css`：问题模板、步骤轨迹、证据卡、引用跳转、报告列表与独立视觉语言；改 `App.tsx`/`Shell.tsx`。
- `scripts/evaluate_g4_agent.py`、`tests/fixtures/g4_agent_questions.json`：冻结的任务/拒答/工具选择评测，合并 G4-A 的标注达到至少 50 题。

---

### Task 1: 模型接入与显式成本边界

**Files:** Create `services/api/app/agent_provider.py`; modify `services/api/app/config.py`, `services/api/requirements.txt`, `infra/.env.example`; test `tests/test_g4_agent_provider.py`。

**Interfaces:** Produces `AgentProviderSettings` from `G4_AGENT_PROVIDER=off|openai_compatible|ollama`（默认 `off`）、`G4_AGENT_BASE_URL`、`G4_AGENT_MODEL`、`G4_AGENT_API_KEY`、`G4_AGENT_TIMEOUT_SECONDS=30`；`build_model(settings: AgentProviderSettings) -> BaseChatModel | None`。`off` 返回 `None`；兼容 API 用 `ChatOpenAI`，Ollama 用 `ChatOllama`；首版远程 host 仅接受 `dashscope.aliyuncs.com` 或 `api.deepseek.com` 的 HTTPS，Ollama 仅 `127.0.0.1/localhost` loopback；上限固定，不从客户端输入模型/URL/密钥。

- [ ] **Step 1: 写失败测试。** 默认 off 无模型/无网络；远程缺密钥、非 HTTPS/非允许域、空模型拒绝；Ollama 仅 loopback；每模型配置最多 800 输出 token，超时受 45 秒总预算约束；日志/异常字符串无密钥。
- [ ] **Step 2: 跑 `python -m unittest tests.test_g4_agent_provider -v`，预期失败。**
- [ ] **Step 3: 实现配置工厂，固定推荐候选为官方北京地域 `qwen-plus` OpenAI 兼容端点，但允许同协议的明确配置；密钥只由进程环境注入，不能在 UI 管理。成本主要由供应商控制台限额加请求 token/调用次数双层约束，不把动态单价硬编码为可靠账单。**
- [ ] **Step 4: 重跑单测；用假客户端验证 off、远程、本地切换不改工具契约。执行真实 API 验收时只请用户走官方平台创建密钥，不要求在聊天中粘贴。**
- [ ] **Step 5: 仅提交本任务文件，提交信息 `feat: configure opt-in agent model providers`。**

### Task 2: 电商场景白名单指标工具

**Files:** Create `services/api/app/agent_models.py`, `services/api/app/agent_tools.py`; test `tests/test_g4_agent_tools.py`。

**Interfaces:** Produces `build_tools(principal: Principal, knowledge: KnowledgeSearchService, behavior: BehaviorMetricsService, orders: OrderMetricsService, budget: ToolBudget) -> list[BaseTool]`；工具名严格为 `search_knowledge`、`get_published_behavior_metrics`、`get_published_order_metrics`。`ToolEvidence` 具有唯一 `evidence_id`、类型、资料块/指标 `meta`、受限正文或最多 10 条聚合行、界面跳转目标；`ToolBudget.consume()` 不允许超过 5 次。

- [ ] **Step 1: 写失败测试。** 直接调用知识工具仅调用受权检索；行为漏斗工具只调 `get_funnel(day|full)`；订单配送/支付/评价/排行仅用对应已发布服务方法。非法日期、`full`+日期、排行维度/排序错配、limit>10、未发布服务返回 503、模型参数伪造 `role`/`user_id` 均失败。工具输出保留 run ID/窗口/来源限制，不包含明细、任意 SQL 或跨源转化率。
- [ ] **Step 2: 跑 `python -m unittest tests.test_g4_agent_tools -v`，预期失败。**
- [ ] **Step 3: 定义 Pydantic 工具参数：行为 view=`overview|funnel|rankings|quality`、window=`day|full`；订单 view=`overview|delivery|payments|rankings|reviews|quality`、window=`day|month|full`；日期和排序沿用现有 API 校验。`Principal` 由闭包传入，所有指标通过 `BehaviorMetricsService`/`OrderMetricsService`，行数与字段白名单裁剪。**
- [ ] **Step 4: 重跑单测；用真实已发布响应作一条行为、一条订单只读工具动态验证，记录 `dataset_id` 和 `metric_run_id`。**
- [ ] **Step 5: 仅提交本任务文件，提交信息 `feat: expose published commerce metrics as safe tools`。**

### Task 3: 单 Agent 循环、证据校验与降级

**Files:** Create `services/api/app/agent_service.py`; test `tests/test_g4_agent_service.py`。

**Interfaces:** Produces `AgentService.ask(question: str, principal: Principal, history: list[ConversationTurn], template_id: str | None = None) -> AgentAnswer`。五个模板 ID 固定为 `orders_payments`、`delivery_reviews`、`rankings`、`behavior_funnel`、`definitions_quality`，仅当问题与服务端模板原文一致才走模板降级。`AgentAnswer` 含 `status: 'answered'|'evidence_only'|'refused'`、最多 3 条 `Insight(text, evidence_ids)`、文档引用、后端渲染的指标卡、`ToolTrace(name,status,elapsed_ms,evidence_ids)`、模型/检索版本、降级原因；任何精确数字都来自 `ToolEvidence`，不是模型自由文本。

- [ ] **Step 1: 写失败测试。** 假模型真实走 `create_agent`，分别选择知识、行为、订单、知识+指标；工具/模型超过 5/3 次、总 45 秒、空证据、伪造引用 ID、模型写数字/日期、检索注入指令、同条观点混用两源证据均返回拒答/确定性摘要。`off`/超时对五类原文匹配的模板按确定性路由出受权证据卡，篡改模板 ID/修改原文及意图不清的自由问题只返回知识命中，不猜指标；失败不把隐藏思维链或敏感原文写入轨迹。
- [ ] **Step 2: 跑 `python -m unittest tests.test_g4_agent_service -v`，预期失败。**
- [ ] **Step 3: 以 LangChain `create_agent` 和限次中间件运行唯一工具循环；后端外层用 `use_tool_deadline`、传输超时和预算计数封闭，捕获超时/429/异常。结构化观点只允许引用本轮 `evidence_id`，同条观点不可混两源，且不允许数字/日期/百分比自由文本；指标值/窗口由后端证据卡填充。`off` 或失败时按五类显式模板的固定工具参数取证，其他问题只做知识检索；未取到证据则拒答。**
- [ ] **Step 4: 重跑单测；用假模型确认步骤轨迹是工具事实，不是隐藏思考，并测模型 off 时仍能返回确定性证据摘要。**
- [ ] **Step 5: 仅提交本任务文件，提交信息 `feat: run a bounded grounded commerce agent`。**

### Task 4: API、短会话与报告安全

**Files:** Create `services/api/app/agent_api.py`, `services/api/app/agent_store.py`, `infra/compose/app-postgres/migrations/003_agent_reports.sql`; modify `services/api/app/main.py`; test `tests/test_g4_agent_api.py`, `tests/test_g4_agent_store.py`。

**Interfaces:** `POST /api/v1/agent/ask` 需 analyst/admin + CSRF，请求为 `question`（长度 1..500）和可选 `template_id`，响应含服务端 `answer_id`；`GET /api/v1/agent/reports`、`GET /api/v1/agent/reports/{id}` 只见本人或 admin；`POST /api/v1/agent/reports` 只接收当前用户的 `answer_id`，不接收客户端答案正文。`AgentStore.last_turns(user_id: int, limit: int=3) -> list[ConversationTurn]`，`save_answer(owner_id: int, answer: AgentAnswer) -> UUID`，`save_report(owner_id: int, answer_id: UUID) -> UUID`，`load_report(report_id: UUID, principal: Principal) -> SavedReport`；读取引用时调用 G4-A `resolve_citation` 并核对当前可见性。

- [ ] **Step 1: 写失败测试。** 匿名 401、viewer 提问 403、缺 CSRF 403、>500 字 422、其他用户报告 403、伪造/别人的 `answer_id` 不可保存、浏览器不能直接提交证据正文；报告里不存密钥/隐藏思维链/原始 PDF；旧 run 标历史、撤回文档使报告 `invalidated` 并遮住旧回答正文，三轮外历史不送模型。
- [ ] **Step 2: 跑 `python -m unittest tests.test_g4_agent_api tests.test_g4_agent_store -v`，预期失败。**
- [ ] **Step 3: 实现可重入迁移和薄 API，先由 `ask` 在服务端保存回答并返回 ID，报告按钮只用这个 ID；报告只存结构化引用/结果/受限文本。读取时对文档引用重新鉴权，当前发布 run 与保存 run 不同则标历史快照，不用新指标悄悄替换。`off` 不报“AI 已回答”，返回 `evidence_only`。**
- [ ] **Step 4: 重跑单测；在隔离应用库以两账户动态验证创建、读报告、撤回来源和旧 run 显示。**
- [ ] **Step 5: 仅提交本任务文件，提交信息 `feat: secure agent answers and reports`。**

### Task 5: 面向分析的独立交互页

**Files:** Create `apps/web/src/lib/agent.ts`, `apps/web/src/modules/Agent.tsx`, `apps/web/src/styles/agent.css`; modify `apps/web/src/App.tsx`, `apps/web/src/components/Shell.tsx`; test `apps/web/src/modules/Agent.test.tsx`, `apps/web/e2e/agent.spec.ts`。

**Interfaces:** 页面分开显示“问题与建议提问”“工具步骤”“知识引用”“指标证据卡”“分析结论/限制”“报告”；引用跳 `#/knowledge?...`，指标卡跳现有六页相应窗口。服务状态明确区分 `answered`、`evidence_only`、`refused`、API 失败，报告标历史/失效。

- [ ] **Step 1: 写失败前端测试。** 五类问题模板能填充但不自动调用付费模型，用户改写模板即清除 `template_id`；loading/超时/降级/越权/无证据有不同状态；run ID、来源、时间窗口和限制可读；数值从 API 卡片而非前端假数据；桌面/移动端仍可操作。
- [ ] **Step 2: 跑 `npm --prefix apps/web test -- --run Agent.test.tsx`，预期失败。**
- [ ] **Step 3: 实现有别于 G3 图表页的“分析工作台”布局，用步骤时间线 + 双栏证据/结论 + 可展开来源，而非套用通用图表卡片；不在 UI 提供密钥输入，不展示模型隐藏思维链。**
- [ ] **Step 4: 重跑 Vitest、`npm --prefix apps/web run build`、Playwright；人工审视两种屏宽、引用跳转、报告读取和失效提示。**
- [ ] **Step 5: 仅提交本任务文件，提交信息 `feat: add grounded agent analysis workbench`。**

### Task 6: 真实链路与独立问题集验收

**Files:** Create `scripts/evaluate_g4_agent.py`, `tests/fixtures/g4_agent_questions.json`, `tests/test_g4_agent_eval.py`; update `docs/superpowers/specs/2026-10-08-graduation-g4-knowledge-rag-design.md` 只写实测结论和限制。

**Interfaces:** `evaluate_agent(cases: list[AgentCase], client: AgentClient) -> AgentEvaluation` 记录工具选择、任务成功/拒答、引用有效性、越权/注入失败数、检索/生成 P95、token 使用与粗略费用估算（按当时官方价格，不当账单）；报告写 ignored `tmp/g4/agent-evaluation.json`。G4-A 冻结 30 题 + 此任务冻结至少 20 题，合计至少 50 题且不用于反复调参。

- [ ] **Step 1: 写失败测试。** 至少 20 条基于真实 Olist/REES46 已发布指标的订单趋势、支付、履约评价、排行、行为漏斗、混合知识+指标、无答案/跨源诱导/旧 run/提示注入题；`test_eval_distinguishes_model_from_fallback` 确保 off/超时不计为真实模型成功。
- [ ] **Step 2: 跑 `python -m unittest tests.test_g4_agent_eval -v`，预期失败。**
- [ ] **Step 3: 实现评测器和固定问题集，不将受测输出反写标注；完整链路先用假模型回归，再由用户在官方平台自行开通/设限并配置本机密钥，选择远程 Qwen Plus 或显式本地 Ollama 至少跑通一个真实模型案例。**
- [ ] **Step 4: 执行真实资料+真实已发布指标案例，记录模型提供商/版本、P95、CPU/内存、API token/估算费用、run ID；若未获密钥或机器不足，标为未验收而非通过。跑全量后端/前端测试、构建、Playwright、`git diff --check`，再提交真实证据。**
- [ ] **Step 5: 仅提交本任务文件与实际验收记录，提交信息 `test: validate grounded g4 agent end to end`。**
