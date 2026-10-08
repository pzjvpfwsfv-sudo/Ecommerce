# G4-A Knowledge Retrieval Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 交付可独立演示的电商项目知识库：真实资料版本管理、角色隔离、混合检索、可定位引用和前端页面，不依赖生成模型。

**Architecture:** 沿用 G3 应用 PostgreSQL、FastAPI 会话和 React 工作台。PostgreSQL 16 的新 pgvector 镜像和新数据卷承载 `knowledge` schema；文档、版本、分块和向量在同库，发布事务切换唯一可见版本。FastEmbed 只在准备/导入阶段生成向量，查询可退到明确标记的关键词模式；后端先过滤权限再融合结果。

**Tech Stack:** PostgreSQL 16 + pgvector、psycopg、FastAPI、FastEmbed `BAAI/bge-small-zh-v1.5`、pypdf、React/TypeScript、Vitest、Playwright、Python unittest。

**Spec:** [G4 知识库与有据 AI 分析设计](../specs/2026-10-08-graduation-g4-knowledge-rag-design.md)。后续 [G4-B 单 Agent 计划](2026-10-08-g4b-ecommerce-agent-implementation.md) 消费本计划的检索接口。

## Global Constraints

- 仅知识文档入库；Olist/REES46 明细仍在 Iceberg/Doris，绝不逐行向量化；两源身份不能关联。
- 只接受 MD、TXT、带文本层的 PDF；单文件 `10 MiB`、PDF `200` 页；扫描件/加密件/空文本拒绝。
- 角色沿用 `admin`、`analyst`、`viewer`；仅管理员写；所有写接口走现有会话和 CSRF；未发布或无权限的版本不得出现在候选集合。
- 每份文档最多一个已发布版本；发布失败保留旧版；删除原文件、抽取文本、分块和向量；引用失效后不得暴露旧正文。
- 向量模型固定 `BAAI/bge-small-zh-v1.5`、`512` 维；缓存、下载、临时文件和报告全部放 D 盘；请求期不得自动下载。
- 首版向量精确检索、中文字符二元组/英文技术词关键词检索、RRF 融合；返回最多 `5` 条，每文档最多 `2` 条；实际 Recall@5 与 P95 要测量，不预填通过。
- 不覆盖旧 `app-postgres-data` 卷，不删除数据；如执行前发现正式旧卷，先逻辑备份到 D 并验证恢复后再切换；不碰 `metastore-postgres`。
- 所有 Python 测试/下载前在当前进程设置 `TEMP`、`TMP=D:\EcommerceDev\temp`、`PIP_CACHE_DIR=D:\EcommerceDev\cache\pip`；测试临时目录在 `.worktrees` 外。只显式提交相关文件，忽略 `.superpowers/brainstorm/`。

## Review Focus

- 同名文档新草稿导入/向量失败：旧已发布版本及其搜索结果必须不变；归 Task 3 的事务测试。
- 用户原本可见的文档被撤回或删除：搜索、预览和旧引用解析均不得返回正文；归 Task 3/4 的测试。
- PDF 扩展名伪装、扫描件或超限文件：导入返回明确 422，不产生半成品版本；归 Task 2/5 的测试。
- 中文口径词与 `metric_run_id` 等英文标识混合查询：关键词路径都应命中正确块，不依赖模型；归 Task 4 的测试。
- FastEmbed 模型缺失、数据库向量扩展不可用：请求不触发下载，不伪称混合检索；前者明确退关键词，后者迁移/就绪失败；归 Task 1/4 的测试。

## File Map

- `infra/docker-compose.yml`：只替换应用库为 pgvector PG16、新命名卷；旧卷保留。
- `infra/compose/app-postgres/migrations/002_knowledge.sql`：可重入 schema/表/约束/索引及扩展检查；不能只挂在首次初始化目录。
- `scripts/prepare_g4_pgvector.ps1`：只读预检、D 盘逻辑备份/恢复到新卷、校验认证行数与扩展；任何不匹配停止，不自动删旧卷。
- `services/api/app/knowledge_models.py`：文档/版本/块/搜索响应契约。
- `services/api/app/knowledge_ingest.py`：格式解析、逐节分块、指标定义受控导入和 FastEmbed 适配。
- `configs/knowledge/g4_sources.json`：经人工核对的仓库资料白名单及来源标签，不自动爬取整个 `docs/`。
- `services/api/app/knowledge_store.py`：PostgreSQL 读写与发布/撤回事务，所有角色过滤 SQL。
- `services/api/app/knowledge_search.py`：关键词、精确向量、RRF 与可引用结果。
- `services/api/app/knowledge_api.py`：鉴权 API 路由；`services/api/app/main.py` 只注册路由和依赖。
- `apps/web/src/lib/knowledge.ts`、`apps/web/src/modules/Knowledge.tsx`、`apps/web/src/styles/knowledge.css`：知识库协议、页面与独立视觉样式；改 `App.tsx`/`Shell.tsx` 接导航。
- `scripts/evaluate_g4_knowledge.py`、`tests/fixtures/g4_knowledge_questions.json`：人工标注问题、召回/引用/延迟报告，输出到 D 盘 ignored `tmp/`。

---

### Task 1: pgvector 应用库安全启用

**Files:** Modify `infra/docker-compose.yml`; create `infra/compose/app-postgres/migrations/002_knowledge.sql`, `scripts/prepare_g4_pgvector.ps1`; test `tests/test_g4_pgvector_config.py`。

**Interfaces:** Produces `knowledge.documents`、`knowledge.versions`、`knowledge.chunks`，`chunks.embedding vector(512)`；`versions` 对一个 `document_id` 只允许一个 `status='published'` 的部分唯一索引。数据库扩展名为 `vector`；新卷名 `app-postgres-g4-data`。`documents.visibility_roles` 为受限角色数组；`versions` 持有原文件 `bytea` 与抽取文本；`chunks` 持有页/章节/词元/向量和分块 SHA-256。

- [ ] **Step 1: 写失败测试。** `test_new_volume_does_not_reuse_old_volume` 断言 Compose 的应用库镜像为官方固定 `pgvector/pgvector:0.8.7-pg16-bookworm`、挂载新卷且原 `metastore-postgres` 不变；`test_schema_has_vector_and_single_published_version` 断言 SQL 有 512 维和已发布唯一约束；`test_preflight_refuses_unverified_old_data` 模拟旧卷有数据但无备份校验时报停止。
- [ ] **Step 2: 运行 `python -m unittest tests.test_g4_pgvector_config -v`，预期新增测试失败。**
- [ ] **Step 3: 实现 Compose、迁移 SQL 与预检脚本。** SQL 使用 `CREATE EXTENSION IF NOT EXISTS vector` 和可重复执行 DDL；PowerShell 脚本先列卷和可用空间，若旧正式卷存在则将 `pg_dump` 逻辑备份写 D、恢复至新卷、核对认证行数/登录；随后对新卷显式执行迁移 SQL 两次并验证扩展，才准切换。旧卷及 `g3-auth-verify` 卷始终保留。当前环境虽只观测到验证卷，执行时仍重新预检。
- [ ] **Step 4: 重跑单测、`docker compose --env-file infra/.env.example -f infra/docker-compose.yml config`；在隔离新卷上执行迁移两次，确认 `SELECT extversion FROM pg_extension WHERE extname='vector'` 和现有登录链路通过。** 若镜像/空间不足，只记录阻塞，不启动对旧卷的危险操作。
- [ ] **Step 5: 仅提交本任务文件，提交信息 `feat: prepare isolated pgvector app database`。**

### Task 2: 可核对的解析、分块与本地向量化

**Files:** Create `services/api/app/knowledge_models.py`, `services/api/app/knowledge_ingest.py`, `configs/knowledge/g4_sources.json`; modify `services/api/requirements.txt`; test `tests/test_g4_knowledge_ingest.py`。

**Interfaces:** Produces `parse_document(filename: str, data: bytes) -> ParsedDocument`、`chunk_document(parsed: ParsedDocument) -> list[KnowledgeChunkInput]`、`Embedder.embed_many(texts: list[str]) -> list[list[float]]`；`EmbeddedChunk` 为 `KnowledgeChunkInput` 加 `embedding: list[float]`，每块含 `section`、`page`、`text`、`sha256`、`terms`，向量必须恰好 512 个有限数。`DocumentMetadata` 含可选 `document_id`（已有文档新版本）、`title`、`category`、`source_type: 'project_doc'|'external'`、`source_ref`（仓库相对路径或 HTTPS URL）、`visibility_roles`。`metric_definition_sections(domain: Literal['behavior','orders'], settings: ApiSettings) -> ParsedDocument` 只读两份受控 JSON。

- [ ] **Step 1: 写失败测试。** MD/TXT 标题和段落定位、PDF 页码、每个指标 JSON 独立可引用、相同内容块保留不同版本身份；白名单仅含两份指标 JSON 与已核对的 `docs/graduation/olist-order-domain-runbook.md`、`docs/graduation/behavior-metrics-api-runbook.md`、`docs/graduation/real-event-quality-runbook.md`，标签为“项目文档”。10 MiB+1 字节、201 页、伪 PDF、扫描件、加密件均拒绝且不写库。`test_embedder_rejects_wrong_dimensions` 验证 511 维/NaN 失败；模型文件缺失时不下载。
- [ ] **Step 2: 运行 `python -m unittest tests.test_g4_knowledge_ingest -v`，预期失败。**
- [ ] **Step 3: 用 `pypdf` 做文本层解析；按章节/页分块，最长 400 字符、重叠不超过 50 字符；英文技术词保留、中文生成字符二元组。FastEmbed 固定模型与 D 盘缓存，导入前的显式准备命令才允许下载，查询请求禁止下载。**
- [ ] **Step 4: 重跑单测并用白名单中的两份真实指标 JSON 和三份运行手册演示块与来源定位，记录模型缓存的实际 D 盘路径。**
- [ ] **Step 5: 仅提交本任务文件，提交信息 `feat: parse and embed project knowledge`。**

### Task 3: 文档版本与发布生命周期

**Files:** Create `services/api/app/knowledge_store.py`; test `tests/test_g4_knowledge_store.py`。

**Interfaces:** Produces `KnowledgeStore.create_draft(owner_id: int, metadata: DocumentMetadata, parsed: ParsedDocument, chunks: list[EmbeddedChunk]) -> DocumentVersion`、`publish(document_id: UUID, version_id: UUID) -> DocumentVersion`、`withdraw(document_id: UUID) -> None`、`delete(document_id: UUID) -> None`、`list_documents(role: Role) -> list[KnowledgeDocument]`、`preview(document_id: UUID, version_id: UUID, principal: Principal) -> DocumentPreview`、`resolve_citation(chunk_id: UUID, principal: Principal) -> Citation | RevokedCitation`。参数化 SQL，不信任客户端 role/owner。

- [ ] **Step 1: 写失败测试。** 新草稿不可被普通搜索、草稿失败旧发布不变、事务发布切换唯一版本、来源类型/链接/角色字段受验证、不同角色能见集合不同、撤回/删除后搜索与引用解析无正文、删除 `bytea` 与向量级联清理；真实 PG 测试以隔离新卷验证上述事务。
- [ ] **Step 2: 运行 `python -m unittest tests.test_g4_knowledge_store -v`，预期失败。**
- [ ] **Step 3: 实现文档/版本/块的事务 CRUD；写入前校验所有向量/块数，发布时先撤旧后启新但同一事务提交；所有读取 SQL 在候选生成前执行已发布、未删除和角色过滤。保留撤回引用的 ID 状态，不保留旧正文。**
- [ ] **Step 4: 重跑单测；在隔离数据库执行“导入 v1 -> 发布 -> 导入坏 v2 -> v1 仍可查 -> 发布好 v2 -> 删除 -> 旧引用仅显示撤回”动态验证。**
- [ ] **Step 5: 仅提交本任务文件，提交信息 `feat: version and publish knowledge documents`。**

### Task 4: 权限优先的混合检索

**Files:** Create `services/api/app/knowledge_search.py`; test `tests/test_g4_knowledge_search.py`。

**Interfaces:** Produces `KnowledgeSearchService.search(query: str, principal: Principal, limit: int = 5) -> SearchResult`，其中 `SearchResult.hits: list[KnowledgeHit]`、`mode: Literal['hybrid','keyword_only']`、`elapsed_ms: float`；每个 `KnowledgeHit` 含 `chunk_id`、`document_id`、`version_id`、章节/页、短正文、来源标签、两个排名和可点击定位。该契约供 G4-B 的 `search_knowledge` 复用。

- [ ] **Step 1: 写失败测试。** 中文二元词、英文 `metric_run_id` 精确词、无命中、不同角色、撤回旧版、向量缺失降级、恶意文档中的工具指令只作为文本；融合前两路都只含授权发布块，RRF 结果最多 5 条且每文档最多 2 条。
- [ ] **Step 2: 运行 `python -m unittest tests.test_g4_knowledge_search -v`，预期失败。**
- [ ] **Step 3: 关键词与 `embedding <=> query_vector` 各取受权 Top 20，按 `1/(60+rank)` 融合并稳定排序；若已准备的 embedding 不可用，显式返回 `keyword_only`，不得在请求期间下载。证据不足阈值基于独立标注集调校，未校准前优先返回命中/未命中而非伪称高置信。**
- [ ] **Step 4: 重跑单测；在真实已导入资料上分别测关键词/向量/融合 Top 5 与请求延迟，记录实际值。**
- [ ] **Step 5: 仅提交本任务文件，提交信息 `feat: retrieve authorized knowledge evidence`。**

### Task 5: 知识 API 与专用页面

**Files:** Create `services/api/app/knowledge_api.py`, `apps/web/src/lib/knowledge.ts`, `apps/web/src/modules/Knowledge.tsx`, `apps/web/src/styles/knowledge.css`; modify `services/api/app/main.py`, `apps/web/src/App.tsx`, `apps/web/src/components/Shell.tsx`, `apps/web/src/lib/http.ts`（FormData 不设 JSON Content-Type）；test `tests/test_g4_knowledge_api.py`, `apps/web/src/modules/Knowledge.test.tsx`, `apps/web/e2e/knowledge.spec.ts`。

**Interfaces:** `POST /api/v1/knowledge/documents` multipart、`POST /api/v1/knowledge/import-metrics`、`POST /api/v1/knowledge/documents/{id}/versions/{version_id}/publish`、`POST /api/v1/knowledge/documents/{id}/withdraw`、`DELETE /api/v1/knowledge/documents/{id}` 仅 admin + CSRF；`GET /api/v1/knowledge/documents`、`GET /api/v1/knowledge/documents/{id}/versions/{version_id}`、`GET /api/v1/knowledge/search?q=` 为会话用户，预览按发布/角色裁剪。所有响应使用 Task 2/4 模型，422 对非法输入，403 对越权，503 对数据库/扩展不可用。

- [ ] **Step 1: 写失败 API/UI 测试。** 匿名 401、viewer 写 403、admin 缺 CSRF 403、伪 PDF/超限 422、`javascript:` 来源链接拒绝；已发布列表/引用定位可见而草稿仅 admin 可预览；撤回引用不返正文。页面把上传 Markdown 当文本而非可执行 HTML 渲染，验证来源/版本/检索模式、上传发布、空态/错误态、移动导航；不出现假指标。
- [ ] **Step 2: 跑 `python -m unittest tests.test_g4_knowledge_api -v`、`npm --prefix apps/web test -- --run Knowledge.test.tsx`，预期新增测试失败。**
- [ ] **Step 3: 注册薄路由并注入 Store/Search；前端用独立知识库布局呈现资料目录、版本时间线和命中位置，admin 才显示管理动作。API 同源请求带已有 CSRF；原文预览只提供可见版本的文本，不直接暴露 `bytea`。**
- [ ] **Step 4: 重跑单测、前端测试、构建与 Playwright；人工检验桌面/移动布局，分别以 admin/analyst/viewer 验证权限。**
- [ ] **Step 5: 仅提交本任务文件，提交信息 `feat: expose and visualize knowledge library`。**

### Task 6: 检索验收与交接证据

**Files:** Create `scripts/evaluate_g4_knowledge.py`, `tests/fixtures/g4_knowledge_questions.json`, `tests/test_g4_knowledge_eval.py`; update `docs/superpowers/specs/2026-10-08-graduation-g4-knowledge-rag-design.md` 仅写真实测量结果和限制。

**Interfaces:** `evaluate(cases: list[RetrievalCase], search: KnowledgeSearchService) -> EvaluationReport`，按关键词/向量/融合分别报告 Recall@5、引用定位成功率、越权/撤回泄漏数、P95 检索耗时、模型缓存与内存观察；输出 JSON 到 `tmp/g4/knowledge-evaluation.json`，原题与标注随仓库提交，原文和密钥不进入报告。

- [ ] **Step 1: 写失败测试。** 另备 10 条调参题，并冻结至少 30 条基于真实资料人工标注的知识/口径/旧版本/越权/无答案保留题；`test_eval_denominator_and_no_leak` 检查分母与越权零泄漏、缺失向量时不会伪报融合分数。
- [ ] **Step 2: 跑 `python -m unittest tests.test_g4_knowledge_eval -v`，预期失败。**
- [ ] **Step 3: 实现不自动调参的评测器；只用 10 条开发题设定拒答阈值，最终 30 条保留题在评测前冻结，负例与无答案题不计入正例召回分母，单列拒答/泄漏指标。**
- [ ] **Step 4: 用真实导入资料运行评测，记录是否达到融合 Recall@5 不低于最佳单路、引用 100% 可定位、泄漏 0、检索 P95 ≤2 秒；全量 `python -m unittest discover -s tests -q`、前端测试/构建、`git diff --check` 后只写实测结论。**
- [ ] **Step 5: 仅提交本任务文件与真实验收记录，提交信息 `test: verify g4 knowledge retrieval on real corpus`。**
