# G2-E Olist 真实订单域与版本化交易指标设计

日期：2026-09-21  
状态：待书面确认后编写实现计划

## 1. 目标与阶段边界

G2-E 为毕业设计增加一个与 REES46 行为域完全隔离的真实订单域，解决当前项目只能分析浏览、加购和购买事件，却不能严谨回答订单金额、支付、履约、客户复购和评价问题的缺口。

本阶段使用 Olist 发布的 Brazilian E-Commerce Public Dataset。官方页面说明该数据包含约 10 万笔匿名化真实商业订单，并以九个 CSV 提供订单、订单明细、支付、评价、客户、商品、卖家、地理位置和品类翻译信息：

- 官方主页：<https://www.kaggle.com/olistbr/brazilian-ecommerce/home>
- 官方元数据：<https://www.kaggle.com/olistbr/brazilian-ecommerce/metadata>
- 发布组织：<https://www.kaggle.com/organizations/olistbr>

真实性陈述以发布方说明为依据。本项目不声称审计过 Olist 的生产数据库，也不把匿名化历史数据描述为正在连接的企业生产系统。

本阶段交付：

- 数据集登记、下载后校验、九文件画像和可重建来源清单。
- 流式 CSV 校验与规范化，不依赖把全部文件一次性载入内存。
- 有界 Flink SQL 入湖和独立 Iceberg Olist schema。
- 防止多事实表连接扇出的订单分析模型。
- Trino 可重算指标、Doris 不可变发布和 FastAPI `orders-v1` 契约。
- 订单总览、履约、支付、商品/品类、卖家、客户复购、评价和质量指标。
- 面向后续 G3 页面和 G4 RAG 的版本化指标定义。

本阶段不交付：

- 不把 Olist 历史快照伪装成 Kafka CDC 或企业实时订单流。
- 不把 Olist 和 REES46 的用户、商品或订单 ID 强行关联。
- 不实现前端页面、知识库、RAG 或模型调用；它们只消费本阶段稳定契约。
- 不增加库存、退款、利润、营销归因、预测模型或虚构人口画像。
- 不把测试夹具、第三方清洗版或自行扩增记录冒充正式业务数据。
- 不完成百万级行为回放和容量压测；这些仍属于 G5。

## 2. 方案选择

采用“真实历史订单批处理湖仓域”方案：

`Olist 九个 CSV -> Python 流式校验/规范化 -> 有界 Flink SQL -> Iceberg -> Trino 建模与指标 -> Doris 发布 -> FastAPI`

该方案符合源数据本身是关系型历史快照的事实，同时复用现有湖仓、计算、服务和发布基础设施。

未采用以下方案：

1. **时间戳拆分后伪造订单 CDC**：原始文件没有变更日志、事务序列或可靠到达时间。按状态时间拆成消息只能称为教学回放，不能称为真实 CDC，首版不使用。
2. **直接导入 Doris**：实现更快，但会绕过 Iceberg Snapshot、Trino 重算和湖仓血缘，削弱项目的架构价值与可审计性。
3. **把九表直接宽连接**：订单明细、支付和评价均可能一对多，直接连接会产生乘法扇出并重复累计金额。
4. **跨 REES46/Olist 映射身份**：两个数据源没有共同业务主键，任何映射都是编造关系。

## 3. 身份与版本

固定身份如下：

| 名称 | 值 | 含义 |
| --- | --- | --- |
| 数据集 ID | `olist-brazilian-ecommerce-v2` | Olist 官方 Kaggle Version 2 来源身份 |
| 数据域 | `orders` | 与 `behavior` 行为域隔离 |
| 数据 schema | `lakehouse.olist` | Iceberg 独立 schema |
| 指标版本 | `orders-v1` | 首版交易指标语义 |
| API 前缀 | `/api/v1/orders` | 与行为 API 隔离 |

源数据包身份 `source_bundle_sha256` 按文件名升序，对每个文件的 `name + NUL + bytes + NUL + row_count + NUL + sha256` 做 UTF-8 串联后计算 SHA-256。任何文件内容、行数或文件集合变化都产生新数据包身份。

`metric_run_id` 使用完整数据包摘要：

```text
orders-v1-b<source_bundle_sha256>
```

不得截断摘要作为唯一身份。显示层可以显示短前缀，但数据库和 API 必须保留完整值；Doris/FastAPI 对应字段至少允许 96 个 ASCII 字符。

## 4. 数据文件与来源登记

正式包必须恰好包含以下九个文件：

| 文件 | 角色 | 首版用途 |
| --- | --- | --- |
| `olist_orders_dataset.csv` | 订单主表 | 状态、购买与履约时间 |
| `olist_order_items_dataset.csv` | 订单明细 | 商品、卖家、价格、运费 |
| `olist_order_payments_dataset.csv` | 支付事实 | 支付方式、分期、支付值 |
| `olist_order_reviews_dataset.csv` | 评价事实 | 评分和评价时间 |
| `olist_customers_dataset.csv` | 客户维度 | 订单客户、稳定客户身份、地区 |
| `olist_products_dataset.csv` | 商品维度 | 品类及商品属性 |
| `olist_sellers_dataset.csv` | 卖家维度 | 卖家地区 |
| `olist_geolocation_dataset.csv` | 邮编地理参考 | 州/城市地图扩展，不作为精确地址 |
| `product_category_name_translation.csv` | 品类翻译 | 葡萄牙语品类到英文标签 |

下载和登记边界：

- 数据、压缩包、规范化中间文件和运行报告只放 D 盘配置目录，默认建议 `D:\EcommerceData\olist\`。
- Git 只保存登记模板、下载说明、校验工具、聚合证据和来源链接，不保存原始数据。
- 不在仓库、日志或命令历史中保存 Kaggle 密钥。需要认证时由用户在本机完成 Kaggle 登录或提供环境级凭据。
- 下载后记录 Kaggle 数据版本、来源 URL、获取时间、压缩包摘要、九文件摘要、字节数、逻辑记录数和当时页面展示的许可信息。
- 许可或文件集合无法核验时失败停止，不自动改用未经确认的镜像或二次加工版。
- 输入目录出现缺文件、额外 CSV、重复文件名、符号链接越界或与清单不一致时拒绝发布清单。

## 5. 流式校验与规范化

新增独立 `generators/olist_data` 包，不把订单逻辑塞进 REES46 的 `generators.real_data`。

校验器使用 Python 标准库逐条读取 CSV：

- 保留 `source_file` 和从 1 开始的数据记录号 `source_row_number`。
- 限制单条逻辑 CSV 记录长度，避免异常超宽或多行字段撑大内存。
- 校验表头必须与版本化 schema 完全一致，不允许静默接受未知列或缺列。
- 业务 ID 全部保留为字符串，邮编前缀不转为会丢前导零的整数。
- 金额保留原始文本并严格解析为非负定点小数，不经二进制浮点往返。
- 空字符串规范为 null，但不生成替代客户、商品、卖家或订单 ID。
- 时间戳保留发布文件中的无时区文本，不擅自追加 `Z` 或改写成 UTC。
- 每条规范化记录生成稳定 `source_row_id`，算法为 `SHA-256(dataset_id + NUL + source_file + NUL + source_row_number + NUL + canonical_row)`。
- 测试夹具可以构造非法边界；正式画像和指标只能读取带有效来源清单的真实文件。

规范化结果按源表写成逐行 JSON，并附带清单。输出先写临时文件，完成行数、摘要和源文件对账后原子发布；已有正式输出不覆盖。

## 6. 有界入湖

Olist 是历史关系快照，因此采用有界 Flink SQL 文件 Source，不经过 Kafka：

1. 每个规范化 JSON 文件作为一个 bounded source。
2. 单次只提交一个来源表，降低 16 GB 本机峰值资源。
3. 每个来源表写入 `lakehouse.olist.<entity>_src_v1`。
4. Source 表保留业务列、`source_file`、`source_row_number`、`source_row_id` 和 `source_bundle_sha256`。
5. 每张表独立等待成功 Checkpoint/作业完成并执行 Trino 行数、唯一性和摘要核对。
6. 九张表全部验收后才允许构建 curated 表；中途失败不产生可发布指标。

首版来源表：

```text
orders_src_v1
order_items_src_v1
order_payments_src_v1
order_reviews_src_v1
customers_src_v1
products_src_v1
sellers_src_v1
geolocation_src_v1
category_translation_src_v1
```

每个来源表必须绑定同一个 `source_bundle_sha256`。不得把不同下载版本的文件混在一次运行中。

## 7. Curated 数据模型

Trino 从九张 Snapshot 固定的来源表构建以下独立事实与维度表：

### 7.1 事实表

`order_fact_v1`，一行一个 `order_id`：

- 客户外键、订单状态和五个订单时间字段。
- 购买日期与月份派生列。
- 不在该表直接连接明细、支付或评价的多行事实。

`order_item_fact_v1`，一行一个 `(order_id, order_item_id)`：

- 商品、卖家、价格、运费和发货期限。
- 商品品类与翻译标签通过一对一维度预处理后补充。

`payment_fact_v1`，一行一个 `(order_id, payment_sequential)`：

- 支付类型、分期数和 `payment_value`。

`review_fact_v1`，一行一个稳定评价身份：

- `review_id`、`order_id`、评分和时间。
- 首版指标不把评价文本导入 RAG，也不向 API 返回原始评价文本。

### 7.2 维度表

`customer_dim_v1`：

- `customer_id` 是订单级客户键。
- `customer_unique_id` 才用于观察窗口内复购分析。
- 首次观测不等于注册时间，复购不等于终身价值。

`product_dim_v1`、`seller_dim_v1`、`category_dim_v1`：

- 保留未知属性和翻译缺失，不猜测填充。
- 品类翻译出现重复时必须失败或按明确唯一规则处理，不能任意选取。

`geolocation_dim_v1`：

- 邮编前缀可能对应多行坐标，只能通过明确聚合得到展示中心点。
- 不把邮编前缀或聚合坐标称为客户精确地址。

### 7.3 防扇出规则

任何订单级指标必须先分别聚合：

```text
order_items -> one row per order
payments    -> one row per order
reviews     -> one row per order
```

随后才能与 `order_fact_v1` 一对一连接。禁止将原始订单明细、支付和评价直接三表连接后求和。

商品、品类和卖家排名只能从订单明细事实计算；支付方式从支付事实计算；评价指标从评价事实计算。不同事实表的金额或计数不强制相等，只做有业务依据的独立对账。

## 8. 时间、状态与金额语义

### 8.1 时间

- Olist 源时间没有时区标记，Iceberg 使用无时区 `TIMESTAMP(3)` 保存。
- API 返回时间时必须标记 `source_timezone: "unspecified"`，不得添加 `Z`。
- 聚合窗口以 `order_purchase_timestamp` 为准。
- 支持 `DAY`、`MONTH` 和 `FULL` 三种固定粒度。
- 履约耗时只在购买时间和客户签收时间均存在时计算。
- 延迟交付只在实际签收和预计交付时间均存在的已交付订单中计算。

### 8.2 状态

- 状态保留源枚举，不把未知状态映射为已交付。
- `delivered_rate` 分子为 `status = delivered`，分母为观察窗口内全部有效订单。
- `canceled_rate` 分子为 `status = canceled`，分母相同。
- 每个比率同时返回分子、分母和排除规则。

### 8.3 金额

- `item_value` 为订单明细 `price` 合计。
- `freight_value` 为订单明细运费合计。
- `payment_value` 为支付事实金额合计。
- 三者都不得称为利润、净收入、退款后收入或财务审计 GMV。
- `payment_value` 与 `item_value + freight_value` 不要求逐单严格相等；系统报告差异分布和可对账订单比例，不通过强制相等隐藏源数据语义。
- 币种展示须以正式数据说明核验结果为准；未核验前 API 的 `source_currency` 返回 null，并附带口径警告，不擅自显示货币符号或执行汇率换算。

## 9. 首版指标产品

指标定义写入 `configs/metrics/orders-v1.json`，每项包含公式、分子、分母、来源字段、粒度、可加性、null 规则、限制和禁止宣称。

### 9.1 订单总览

- `order_count`
- `delivered_order_count`
- `canceled_order_count`
- `unavailable_order_count`
- `unique_customer_count`
- `repeat_customer_count`
- `repeat_customer_rate`
- `item_value_sum`
- `freight_value_sum`
- `payment_value_sum`
- `items_per_order_avg`

复购率定义为观察窗口内订单数至少为 2 的 `customer_unique_id` 数量，除以窗口内至少有 1 单的 `customer_unique_id` 数量。它不是客户终身复购率。

### 9.2 履约

- `delivery_eligible_order_count`
- `delivery_days_avg`
- `delivery_days_p50`
- `delivery_days_p90`
- `late_delivery_order_count`
- `late_delivery_rate`

### 9.3 支付

- `payment_row_count`
- `payment_order_count`
- `payment_value_sum`
- `installment_order_count`
- 按 `payment_type` 的订单数与支付值

多支付方式订单可以出现在多个支付方式组中；API 必须返回组内订单数与全局订单数，不能把分组订单数简单相加当总订单数。

### 9.4 商品、品类、卖家和地区

- 订单数、明细数、商品件数代理、`item_value`、运费和客户数。
- 商品件数代理只等于订单明细行数，不代表源数据存在真实 `quantity` 字段。
- 未知品类、未知翻译和未知地区保留独立组。

### 9.5 评价

- `reviewed_order_count`
- `review_coverage_rate`
- `review_score_avg`
- `low_score_order_count`
- `low_score_rate`

低分首版固定为订单级平均评分小于等于 2。一个订单存在多条有效评价时，先计算该订单所有有效评分的算术平均值；`review_score_avg` 再对已评价订单的订单级平均值求平均，避免多评价订单获得更高权重。系统同时报告评价行数、多评价订单数和已评价订单数。

### 9.6 质量

- 九表源行数和 Iceberg 行数。
- 主键重复、必填键为空、外键孤儿和未知枚举数量。
- 非法金额、非法时间、状态时间顺序违规数量。
- 每张来源表 Snapshot ID、规范化摘要和 source bundle 身份。
- 订单、明细、支付、评价的独立金额与数量对账。
- 指标候选行数、SHA-256 和发布状态。

## 10. Doris 发布模型

首版 Doris 表：

```text
order_metric_publications
order_metric_overview
order_metric_delivery
order_metric_payment
order_metric_ranking
order_metric_review
order_metric_quality
```

每行包含 `metric_run_id`、`dataset_id`、`metric_version`、窗口和业务键。发布流程为：

1. 验证九文件 bundle 和九张来源表 Snapshot 身份。
2. 固定所有输入 Snapshot，生成候选指标。
3. 执行跨表语义门禁与唯一键检查。
4. 计算每张候选表的规范化 SHA-256。
5. 按 `metric_run_id` 幂等写入指标表并读回核验。
6. 所有表读回行数和摘要一致后，最后写一行 `PUBLISHED`。

API 只读取最新 `PUBLISHED`。失败运行不可见；已有发布不可更新、删除或覆盖。相同 bundle 再次运行时，只有全部行数、摘要、Snapshot 集合和范围一致才返回 `already_published`，否则失败停止。

发布记录分别保存九张来源表的 `source_snapshots`、本次指标实际读取的事实/维度表 `curated_snapshots` 和 `implementation_revision`。两个 Snapshot 集合均使用按表名排序的对象，Snapshot ID 在 JSON 边界作为十进制字符串返回。相同源 bundle 因语义修改产生不同指标时必须升级指标版本，不能借覆盖 `orders-v1` 解决摘要冲突。

## 11. FastAPI 契约

新增端点：

```text
GET /api/v1/orders/publication
GET /api/v1/orders/overview
GET /api/v1/orders/delivery
GET /api/v1/orders/payments
GET /api/v1/orders/rankings
GET /api/v1/orders/reviews
GET /api/v1/orders/quality
GET /api/v1/metrics/definitions?domain=orders&version=orders-v1
```

统一查询参数：

- `window=day|month|full`
- `start_date`、`end_date` 必须成对出现、包含于发布窗口，且只允许用于 `day` 或 `month`；`full` 禁止携带日期范围。
- 排名端点允许 `dimension=product|category|seller|customer_state|seller_state`。
- 商品、品类和卖家允许 `sort_by=order_count|item_value|freight_value`；客户州和卖家州允许 `sort_by=order_count|payment_value|late_rate`。
- `limit` 必须为 1 至 100 的整数，默认 20；所有并列项再按维度 ID 升序确定稳定顺序。

响应元数据至少包含：

```text
dataset_id
metric_version
metric_run_id
source_bundle_sha256
source_snapshots
curated_snapshots
window_start
window_end
source_timezone
source_currency
source_order_count
calculated_at
implementation_revision
warnings
```

所有数据库值通过参数化查询获取。API 不接收 SQL、列名片段或任意排序表达式。没有有效发布、发布与指标表不一致或定义目录无效时返回安全 `503`，不回退到未发布候选。

## 12. 数据质量与失败边界

以下属于结构性硬门禁，任一计数非零时必须失败停止：

- 九文件集合、摘要、行数或版本清单不一致。
- 任一来源表不属于同一 bundle。
- 订单 `order_id`、客户 `customer_id`、商品 `product_id`、卖家 `seller_id` 或品类翻译源键重复。
- 订单明细 `(order_id, order_item_id)` 或支付 `(order_id, payment_sequential)` 复合键重复。
- 订单明细、支付或评价引用不存在的订单。
- 订单引用不存在的客户。
- 明细引用不存在的商品或卖家。
- 必填 ID 为空、金额为负或不能精确解析。
- 必填购买时间为空或不能解析；任一非空时间字段不能解析。
- DAY/MONTH/FULL 汇总与事实总量不一致。
- 同一发布唯一键重复、候选与读回摘要不一致。

以下属于可报告的数据质量问题，不单独阻断整个订单域发布：

- 产品品类、翻译、评价或可选履约时间缺失。
- 已存在的审批、交承运商或签收时间早于购买时间。
- 实际签收早于交承运商时间，或预计日期早于购买日期。
- 未知订单状态、未知支付类型和多评价订单。
- 重复 `review_id`；评价事实仍使用唯一 `source_row_id` 保留每条源记录。
- 支付值与明细价格加运费不一致。

这些记录必须进入质量计数，并从依赖相应字段的履约、状态或评价指标分母中排除；总订单数和不依赖异常字段的指标仍保留。API 同时返回排除数量和规则。若真实画像表明某类异常会破坏事实粒度或使核心指标不可解释，实施计划必须把该类升级为硬门禁并记录理由，不能静默丢弃。

## 13. 资源与运行边界

- 本机 16 GB 环境一次只处理一个来源表或一个指标阶段。
- 原始校验和 JSON 规范化采用流式 I/O，不使用全表 Pandas DataFrame 作为正式流水线。
- Geolocation 不与每个订单明细提前宽连接；地图查询使用聚合维度。
- Trino、Flink、Doris 按阶段启动，不同时运行前端构建和本地大模型。
- Docker CLI 和数据继续使用 D 盘路径，不把镜像、程序、临时文件或数据迁回 C 盘。
- 脚本不得清理 Docker 卷、删除 Iceberg 表、截断 Doris 表或覆盖历史发布。

## 14. 隐私、安全与 RAG 边界

- 匿名 ID 仍按不透明业务标识处理，不尝试反向识别个人。
- 城市、州和邮编前缀用于聚合，不暴露或推断精确地址。
- 原始评价文本不进入首版 API 和 RAG，避免把用户文本作为可信指令或无必要传播内容。
- 后续知识库只导入指标定义、数据字典、运行手册和经过选择的分析规则，不向量化订单明细或客户记录。
- Agent 同时查询行为域和订单域时必须分区展示证据，不生成跨来源用户旅程。

## 15. 测试与真实验收

### 15.1 自动化测试

- 下载清单、路径边界、文件集合和摘要校验。
- 九表表头、CSV 多行/超宽记录、金额、时间、null 和稳定行身份。
- 主外键、重复键、未知维度和时间顺序质量规则。
- 事实表防扇出和订单粒度聚合。
- 指标公式、分子分母、DAY/MONTH/FULL 对账。
- 发布顺序、幂等、摘要不匹配失败和不可变历史。
- FastAPI 模型、白名单参数、503 降级和完整定义目录。

测试夹具必须明确标注为人工边界样例，不成为真实业务结果。

### 15.2 真实数据验收

验收报告必须记录真实测量值，不能预填：

1. 九文件 SHA-256、字节数、源行数和完整读取结果。
2. 规范化输出行数与源行数逐表一致。
3. Iceberg 来源表行数、不同稳定行身份和 Snapshot ID。
4. 主外键孤儿、重复键、必填缺失和非法字段计数。
5. Curated 事实表行数与预期粒度一致。
6. 订单明细、支付和评价聚合未发生扇出。
7. Trino 指标、Doris 发布和八个 FastAPI 契约使用同一 run 身份。
8. Doris 每张表行数和规范化摘要与发布元数据一致。
9. Docker/服务资源、执行时间和失败排障记录。
10. 全量仓库回归通过。

没有真实动态执行证据时，只能声明代码与离线契约完成，不能声明 G2-E 已动态验收。

## 16. 后续演进

- G3 使用同一页面框架分别展示实时行为域与历史订单域。
- G4 把 `behavior-v1` 和 `orders-v1` 指标定义、数据字典和运行手册导入知识库。
- 受控 Agent 可以组合两个域的独立证据，但不能执行跨来源身份关联。
- 若未来获得真实订单 CDC，再新增 `orders-v2-stream`，不得回写或重定义 `orders-v1` 历史语义。
- G5 再做真实容量、批量刷新耗时、API 延迟和故障恢复测量。

## 17. 成功标准

- **真实**：九个正式文件来源、版本和摘要可复核，不使用扩增或二次清洗版冒充原始数据。
- **正确**：事实粒度明确，多事实表无扇出，所有金额、比率和分母有版本化定义。
- **可追溯**：API 指标可追溯到完整 source bundle 和固定 Iceberg Snapshot 集合。
- **稳定**：失败候选不可见，历史发布不可变，重跑不会静默覆盖。
- **可用**：FastAPI 契约足以支撑订单、支付、履约、商品、卖家、客户和评价页面。
- **可扩展**：G3/G4 只增加消费者，不修改 `orders-v1` 的已发布语义。
- **可答辩**：能够解释为什么真实行为流使用 Kafka/DataStream，而历史关系订单使用有界批处理；为什么 Iceberg 保存真相、Trino 负责重算、Doris 负责服务、FastAPI 负责契约。
