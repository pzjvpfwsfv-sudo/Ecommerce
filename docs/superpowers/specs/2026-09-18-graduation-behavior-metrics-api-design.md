# G2-D 真实行为分层指标与版本化 API 设计

## 1. 目标与阶段边界

G2-D 把 G2-C 已验收的真实行为明细表 `lakehouse.analytics.real_behavior_detail_v1` 转换为可供业务页面和后续 RAG/Agent 稳定消费的指标产品。首版只处理 REES46 行为域，不在同一阶段并行接入 Olist 交易域。

本阶段交付：

- 从 Iceberg 明细计算可复现的行为指标快照。
- 将已验证快照发布到 Doris，供低延迟查询。
- 提供 `/api/v1/behavior/*` 版本化 FastAPI 接口。
- 将指标名称、公式、分母、粒度、字段来源和限制固化为 Git 版本化定义。
- 使用当前 1,002 条真实事件完成正确性验收。

本阶段不交付：

- 不把 1,002 条验收样本描述为正式业务趋势或容量结果。
- 不完成 2,199,938 条全量回放、压测或长期调度；这些属于 G5。
- 不增加 Olist 订单、支付、物流和评价表；这些属于 G2-E。
- 不实现 React 页面、知识库或 RAG；G3/G4 只消费本阶段稳定契约。
- 不把 `purchase` 事件中的商品价格称为 GMV、销售额或收入。

## 2. 方案选择

采用“长周期真相在 Iceberg，历史计算在 Trino，高频服务在 Doris，统一契约在 FastAPI”的混合分层方案。

未采用以下方案：

1. FastAPI 每次直接扫描 Trino：实现简单，但页面延迟和资源占用不可控，也绕过 Doris 服务层。
2. 首版全部改成 Flink 实时聚合：实时性高，但复杂漏斗、历史重算和口径升级成本过高，容易在当前工期内形成两套不一致指标。

G2-D 首版以 Iceberg Snapshot 为刷新边界，提供可重复的微批指标。后续如需 5 分钟实时窗口，单独增加 `behavior_live_5m` 表和接口，不能把实时近似值覆盖历史精确指标。

## 3. 架构与数据流

```text
lakehouse.analytics.real_behavior_detail_v1
  -> Trino 固定 SQL（按 Iceberg Snapshot 计算）
  -> 本地忽略目录中的 CSV/JSON 临时结果
  -> 数量、分母、时间范围、金额与质量对账
  -> Doris 带 metric_run_id 的候选指标行
  -> 最后写入 PUBLISHED 发布记录
  -> FastAPI 只读取最新 PUBLISHED 批次
  -> G3 页面 / G4 指标工具与知识库
```

刷新入口固定为 `scripts/refresh_g2d_behavior_metrics.ps1`，验收入口固定为 `scripts/verify_g2d_behavior_metrics.ps1`。脚本只允许读取固定 Iceberg 表并写入固定 Doris 指标表，不接受任意 SQL、任意 catalog/schema 或任意目标表名。

每次刷新生成 `metric_run_id`，并绑定：

- `dataset_id=rees46-multicategory`
- `metric_version=behavior-v1`
- `data_scope=g2c-correctness-subset` 或 `stable-user-2pct-full`
- Iceberg `source_snapshot_id`
- 源数据起止日期和总行数
- 计算时间、发布时间和发布状态

`metric_run_id` 固定为 `behavior-v1-s<source_snapshot_id>`，Snapshot ID 只接受正整数。当前 1,002 条验收表只能发布为 `g2c-correctness-subset`；只有源总数精确等于 2,199,938 且完整范围对账通过时才能发布为 `stable-user-2pct-full`。候选指标全部装载并通过验证后，才最后写入 `PUBLISHED` 元数据。API 不读取没有发布记录的候选行，因此任一表装载失败都不会暴露半套结果。重复发布同一 `metric_run_id` 必须失败或返回已经发布的相同身份，不能静默叠加。

## 4. 指标窗口与可加性

首版只发布两个明确窗口：

- `DAY`：按 UTC 业务日期独立计算，供趋势图和单日分析。
- `FULL`：对当前 Iceberg Snapshot 的完整观察窗口重新计算，供全窗口总览和排行。

不允许把每日 UV、每日会话数或每日漏斗人数简单相加后冒充任意日期范围的精确去重结果。任意日期范围接口首版只返回可加的事件计数与逐日序列；精确去重指标只在单日或 `FULL` 窗口返回。未来若增加滚动 7/30 日窗口，必须在 Trino 中对每个窗口重新去重并发布为新粒度。

## 5. 指标口径

### 5.1 运营总览

`behavior_overview_metrics` 按 `metric_run_id + window_type + window_start + window_end` 保存：

- `event_count`：有效事实总数。
- `view_count`、`cart_count`、`purchase_count`：对应事件类型数量，三者之和必须等于总数。
- `unique_user_count`：窗口内不同非空 `user_id`。
- `session_count`：窗口内不同非空 `user_session`。
- `product_count`：窗口内不同非空 `product_id`。
- `purchase_amount_proxy`：仅对 `purchase` 事件求 `price_decimal` 之和，使用 `DECIMAL`，API 以字符串返回；名称固定为“购买事件金额合计（分析代理值）”。

`purchase_amount_proxy` 不考虑购买数量、币种、折扣、退款、取消或支付状态，禁止在代码、接口和页面中重命名为 GMV、收入或销售额。

### 5.2 会话顺序漏斗

`behavior_funnel_metrics` 只统计非空 `user_session`，同时发布被排除的缺失会话事件数。

事件顺序使用 `(event_ts, source_file, source_row_number, event_id)`，避免同秒事件随机排序。漏斗必须满足同一窗口、同一会话内的先后关系：

1. `view_sessions`：至少存在一次 view。
2. `view_to_cart_sessions`：在一次 view 之后存在 cart。
3. `completed_sessions`：在上述 cart 之后存在 purchase。

比率由整数分子和分母计算：

- `view_to_cart_rate = view_to_cart_sessions / view_sessions`
- `cart_to_purchase_rate = completed_sessions / view_to_cart_sessions`
- `full_conversion_rate = completed_sessions / view_sessions`

分母为 0 时返回 `null`，不能返回 0 或无穷。`DAY` 将跨 UTC 日期的会话按日期分别观察；`FULL` 对完整窗口重新计算，不能由每日漏斗相加得到。

### 5.3 商品、品类与品牌排行

`behavior_dimension_metrics` 支持 `product`、`category`、`brand` 三类维度，并保存：

- view/cart/purchase 事件数。
- 不同用户数。
- 购买事件金额代理值。
- 维度 ID、展示名称和 `is_unknown`。

商品使用 `product_id`；品类使用 `category_id` 作为稳定键、`category_code` 作为可空展示名；品牌缺失进入明确的未知组。不能猜测补齐缺失品类或品牌。API 排行只允许后端白名单排序字段和 `1..100` 的 limit，不能把客户端字段直接拼进 SQL。

### 5.4 数据质量

`behavior_quality_metrics` 至少发布：

- clean/late 数量及占比。
- 不同 `event_id` 数和重复差值。
- 缺失会话、品类名称、品牌的数量及占比。
- 非法事件类型、空关键 ID、非法金额、派生日期不一致数量。
- 源总数、质量总数与 overview 总数对账结果。

质量问题和业务未知维度必须保留，不能通过过滤让指标“看起来更干净”。DLQ 不进入事实指标，其统计仍属于 G2-B 质量证据。

## 6. Doris 发布模型

新建以下固定表，不改写旧模拟链路的 `realtime_metrics`：

1. `behavior_metric_publications`
2. `behavior_overview_metrics`
3. `behavior_funnel_metrics`
4. `behavior_dimension_metrics`
5. `behavior_quality_metrics`

所有指标表都包含 `metric_run_id`、`dataset_id` 和 `metric_version`。发布表使用 `metric_run_id` 唯一键，指标表的唯一键包含运行身份和业务粒度。初始化 SQL 必须可重复执行，但不得删除已有发布批次。

加载使用 Doris Stream Load 或等价的受控批量入口，label 包含 `metric_run_id` 和表名。每张表装载后核对行数；overview/funnel/quality 还需核对源总数、窗口范围及关键分子分母。只有全部核对通过才发布元数据。

## 7. 指标定义文件

新增 `configs/metrics/behavior-v1.json`，作为指标语义的 Git 版本化事实来源。每个指标包含：

- `metric_name` 与中文展示名
- `metric_version` 和所属数据域
- 公式、分子、分母与聚合粒度
- 使用的源字段
- 可加性和允许窗口
- 空值/未知维度处理
- 业务限制与禁止表述

应用启动时严格校验该文件；重复名称、未知字段、版本不匹配或缺少限制说明必须失败。G4 知识库以后导入同一文件或其渲染文档，避免 RAG 口径和 API 代码各写一份。

## 8. API 契约

保留现有 `/metrics/realtime`、`/metrics/{metric_name}` 和第 8/10 章分析接口，不做破坏性修改。新增：

- `GET /api/v1/behavior/publication`
- `GET /api/v1/behavior/overview?window=day|full`
- `GET /api/v1/behavior/funnel?window=day|full`
- `GET /api/v1/behavior/rankings?dimension=product|category|brand&window=day|full&sort_by=views|carts|purchases|users|amount&limit=20`
- `GET /api/v1/behavior/quality`
- `GET /api/v1/metrics/definitions?domain=behavior&version=behavior-v1`

`window=day` 返回逐日序列；`window=full` 返回完整观察窗口。首版不接受任意 SQL、不接受任意表名，也不把自由文本转换为排序列。

所有行为响应统一包含：

```json
{
  "meta": {
    "dataset_id": "rees46-multicategory",
    "metric_version": "behavior-v1",
    "metric_run_id": "...",
    "source_snapshot_id": "...",
    "window_start": "2019-10-01",
    "window_end": "2019-11-30",
    "calculated_at": "...",
    "data_scope": "g2c-correctness-subset",
    "source_event_count": 1002,
    "warnings": ["correctness subset; not the full 2% user sample"]
  },
  "data": {}
}
```

没有已发布批次时返回 503；非法枚举和 limit 返回 422；请求有效但排行为空时返回 200 和空列表。数据库异常只记录安全的阶段和异常类型，对外统一返回 503，不泄露 SQL、连接串或内部地址。

## 9. 代码边界

预计新增或修改：

- `configs/metrics/behavior-v1.json`
- `infra/compose/doris/init/02_create_behavior_metrics.sql`
- `jobs/sql/18_g2d_behavior_metrics.sql.template`
- `scripts/refresh_g2d_behavior_metrics.ps1`
- `scripts/verify_g2d_behavior_metrics.ps1`
- `services/api/app/behavior_models.py`
- `services/api/app/behavior_repository.py`
- `services/api/app/behavior_service.py`
- `services/api/app/main.py`
- `services/api/app/dependencies.py`
- 对应测试、运行手册和 README 状态

Repository 只负责参数化 Doris 查询，Service 负责响应组装、比率计算和定义文件校验，路由只负责输入验证和错误映射。不得把 SQL、业务口径和 HTTP 处理堆进 `main.py`。

## 10. 测试与动态验收

按 TDD 实施：

1. SQL/配置契约测试先失败，再实现固定表、固定占位符和指标定义校验。
2. 漏斗使用人工边界夹具验证乱序、同秒、跨日、缺会话和零分母。
3. Repository 测试验证参数绑定和排序白名单，禁止 SQL 注入。
4. Service/API 测试验证统一 meta、Decimal 字符串、422/503、空排行和旧接口兼容。
5. 刷新脚本测试验证“候选装载失败时不发布”、重复 run、错误 Snapshot 和对账失败。
6. 动态验收从正式 Iceberg 表计算，再核对 Trino、Doris 和 FastAPI 三层结果。

当前 1,002 条正式表必须至少满足：

- FULL `event_count=1002`，事件类型之和等于 1,002。
- quality `clean=1001`、`late=1`、不同 `event_id=1002`。
- DAY 总事件数之和等于 FULL 总事件数。
- 四张指标表的 `metric_run_id`、版本、Snapshot、数据范围和窗口完全一致。
- API 与 Doris 返回值一致，定义接口包含金额代理值的禁止表述。

动态验收只证明小规模正确性，不证明 220 万条刷新时长、Doris 并发能力或长期运行稳定性。

## 11. 资源与运行边界

本机 16 GB 环境仍按需启动服务。刷新时只启动 MinIO、Metastore、Trino、Doris 和 API 所需依赖；不同时启动无关 AI 模型或前端构建。临时 CSV/JSON、Stream Load 响应和验收报告写入 `tmp/graduation/g2d/<metric_run_id>/` 并由 Git 忽略。

Docker CLI 继续使用已迁移的 D 盘安装位置；脚本只做当前进程 PATH fallback，不把程序、镜像或数据迁回 C 盘。刷新和验证不得删除 Docker 卷、Iceberg 表或历史发布批次。

## 12. 后续演进

- G2-E 在独立 `dataset_id` 下接入 Olist 订单域，使用独立事实表、指标版本和 API 前缀。
- G3 统一页面框架，但行为域和交易域分区展示，不制造跨来源用户关联。
- G4 将 `behavior-v1` 和后续交易定义导入知识库；Agent 回答必须同时返回指标版本、数据证据和文档引用。
- G5 对 2,199,938 条真实行为事件执行分批回放和刷新，记录吞吐、反压、Checkpoint、Snapshot、Trino 计算时间、Doris 装载时间与 API 延迟。

## 13. 成功标准

- 正确：所有指标能回溯到同一 Iceberg Snapshot，并通过三层对账。
- 稳定：失败批次不对 API 可见，旧已发布批次继续可读。
- 清楚：行为事件金额不冒充财务指标，未知维度和样本边界不被隐藏。
- 可扩展：Olist、前端和 RAG 通过新版本/新数据域接入，不修改 `behavior-v1` 历史语义。
- 可面试：能够解释为什么选择 Iceberg、Trino、Doris、快照发布和版本化指标，而不是只展示若干查询页面。
