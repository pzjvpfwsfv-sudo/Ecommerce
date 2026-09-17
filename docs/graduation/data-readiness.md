# 真实数据准备与分析适用性

## 当前状态

2026-09-17 已完成两个月完整源文件下载、全扫描与稳定用户样本核验：109,950,743 条源记录生成 2,199,938 条标准化事件，覆盖连续 61 天。**真实 Kafka/Flink 适配、业务指标验收、业务前端和 RAG 尚未完成。**此前的 1 万条/百万条前缀仅作为工具验证记录保留，不再充当正式分析窗口。

来源登记：`configs/datasets/rees46-multicategory.json`。官方目录 https://data.rees46.com/ ，数据说明 https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data 。

数据按发布方说明属于真实匿名化历史业务事件，不是本项目生成的数据，也不是实时连接企业生产接口。需引用数据集与 REES46；使用说明已经写入下载清单，重新发布数据前核对当时的源条款。

## 跨月验收结果

直接读取官方 `2019-Oct.csv.gz`、`2019-Nov.csv.gz`，不完整解压 CSV。按固定种子 `graduation-v1`、200 基点选择稳定用户，保留选中用户在两个源文件内的全部有效事件，不复制事件扩增规模。

| 项目 | 实测结果 |
| --- | --- |
| 10 月压缩文件 / 源记录 / 样本记录 | 1,741,928,540 字节 / 42,448,764 / 849,121 |
| 11 月压缩文件 / 源记录 / 样本记录 | 2,890,421,023 字节 / 67,501,979 / 1,350,817 |
| 源记录合计 / 样本记录合计 | 109,950,743 / 2,199,938 |
| 源观察范围（UTC） | 2019-10-01 00:00:00 至 2019-11-30 23:59:59 |
| 样本日期覆盖 / 每日事件数范围 | 连续 61 天 / 22,921 至 131,197 |
| 样本 view / cart / purchase | 2,086,366 / 80,023 / 33,549 |
| 样本用户 / 商品 | 107,000 / 107,443，均为样本内精确去重计数 |
| 样本会话基数 | 至少 200,000，已封顶，`exact=false`，不是精确会话数 |
| 品类 ID / 会话 ID 缺失 | 0 / 0 |
| 品类名称编码 / 品牌缺失 | 706,741（32.1255%）/ 304,428（13.8380%） |
| 源结构或用户缺失拒绝 / 已选非法事件 | 0 / 0 |
| 源和样本相邻时间逆序 | 0 |
| 标准化 JSONL 大小 | 1,045,479,893 字节，约 0.974 GiB |
| 最终版全扫描及抽样耗时 | 596.358 秒，约 9 分 56 秒；不含下载 |
| 抽样进程观测峰值工作集 | 65.09 MiB；不含操作系统、缓存、Docker 或其他进程 |

文件校验值：

- October SHA-256：`8ebca1ad741295297368f2cf0315e3f36853a1a11768fb16babf8c9b83838147`。
- November SHA-256：`7d9932fdf2667f3d800655f55a623d7f71b22ff8d606da55bf93cfc307ebf213`。
- 正式样本 `data/rees46/oct-nov-users-2pct-verified.jsonl` SHA-256：`18a3202d380c8f6c53ad767ec2043d0717df1d3604d2211639bb0fc4699a5437`。

两次全扫描的样本字节校验值、计数及质量画像一致；最终版增加了 CSV 记录长度限制和更严格的日期边界检查。源 SHA、完整 gzip 校验、逐源每日计数、已选有效/拒绝记录、样本事件/每日计数均对账通过；样本文件另外独立计算 SHA 确认落盘内容一致。

这次实测证明本机可以低内存流式准备亿级源文件并产出百万级分析样本，不证明整套湖仓服务只需 65 MiB，也不是 Kafka/Flink 吞吐或端到端延迟测试。稳定用户抽样保留的是这两个源文件中的有效行为，不是用户完整生命周期。

本地证据：源文件旁的 `.provenance.json`、正式样本旁的 `.provenance.json`、`tmp/graduation/cross-month-final-memory.json`、`cross-month-final-duration.json` 和 `regression-cross-month-final.log`。运行证据不包含在 Git 备份中，本文保留可复核的聚合结果及来源校验值。

## 历史前缀验证

输入为 `2019-Oct.csv.gz` 的前 1,000,000 条原始记录，不做随机造数或重复扩增。下载时转换为 CSV 记录并统一 LF 换行，业务字段与记录顺序保留；不是完整原始压缩文件的字节副本。

| 项目 | 实测结果 |
| --- | --- |
| 样本 CSV 大小 | 133,570,257 字节 |
| 实际读取压缩流字节 | 41,680,919 字节 |
| SHA-256（样本 CSV） | `1d5884dceb5e210bd9478eca73428eb749a87d5475f141cc84c35852e5f2748c` |
| 扫描 / 有效 / 拒绝 | 1,000,000 / 1,000,000 / 0 |
| view / cart / purchase | 968,513 / 14,639 / 16,848 |
| 时间范围（UTC） | 2019-10-01 00:00:00 至 2019-10-01 16:56:07 |
| 品类 ID 缺失 | 0 |
| 品类名称编码缺失 | 318,131（31.8131%） |
| 品牌缺失 | 147,472（14.7472%） |
| 会话 ID 缺失 | 0 |
| 相邻有效记录时间逆序 | 0 |

记录数对账通过：事件计数之和 = 每日计数之和 = 有效行数；扫描行数 = 有效 + 拒绝。未发现非法行不等于所有业务指标已正确，不代表其他月份没有质量问题。

用户、商品、会话基数默认封顶 10,000；本样本均触达上限，报告明确标记 `exact=false`。它们是下界，不是 UV 或精确用户数。需要精确统计时提高上限或在后续计算层使用可控状态/磁盘聚合，不能直接用于看板。

购买事件价格之和只保留为数据核验值，不把它当作订单金额、销售件数、净收入或利润；本阶段也未核实币种。

## 对功能的约束

- 可以验证事件解析、时间保存、来源追踪、原始行为类型、缺失统计和技术处理规模。
- 历史前缀不足一天，不用于业务验收；正式跨月样本可以支持后续趋势、留存和路径分析开发，但相应指标仍须验证分组有效样本量、会话归属、先后顺序和观察期截断规则。
- 品类 ID 存在不等于名称存在。没有可靠名称映射时显示品类 ID/未知名称，不能从其他数据集猜测填充；品牌分析必须显示未知组及覆盖率。
- 已核验的前缀及跨月用户样本没有 remove_from_cart 事件，不承诺全店或每个月都有该类数据，不用不存在的事件造出移除购物车统计。
- 事件计数只是描述性统计，不能把 purchase/view 事件数之比直接当用户或会话转化率。
- 正式样本已覆盖两个月，样本计数不能当作全店总量；首尾观察边界可能截断会话与留存，首次观测用户不能被称为新注册用户。

## 使用方法

在包含 `generators/real_data` 的开发目录执行。只需 Python 3.12，无需启动 Docker、安装新依赖或提供模型密钥。所有输出拒绝覆盖已有文件，重跑时使用新输出文件名。

```powershell
python -m generators.real_data fetch-prefix --output data/rees46/2019-Oct-prefix.csv --rows 1000000
python -m generators.real_data profile --input data/rees46/2019-Oct-prefix.csv --source-file 2019-Oct.csv.gz --scope prefix --report tmp/graduation/profile.json
```

下载默认最多请求 64 MiB 压缩片段，最多 1,000,000 条，单次总读取预算约五分钟（单次网络读取有 30 秒超时）。服务端不遵守 Range、内容损坏或预算不足时失败，不无限下载完整月份。

前缀 CSV 附带 `.provenance.json`，记录真实源文件名、来源、获取时间、读取字节数、样本行数和 SHA-256。画像入口校验已有清单的来源、校验值和范围，不能把前缀改标为 full_file。

标准化输出示例（同样采用新文件名）：

```powershell
python -m generators.real_data profile --input data/rees46/2019-Oct-prefix-10000.csv --source-file 2019-Oct.csv.gz --scope prefix --report tmp/graduation/normalized-check.json --normalized data/rees46/events-check.jsonl
```

完整下载后的文件也可以流式检查：

```powershell
python -m generators.real_data profile --input data/rees46/2019-Oct.csv.gz --source-file 2019-Oct.csv.gz --scope full_file --report tmp/graduation/october-full.json
```

`--scope full_file` 是输入范围声明，不是程序对源文件完整性或业务真实性的认证。没有清单的文件默认 unverified。`--max-rows` 会明确标注扫描截断；整文件校验值仍按完整本地文件计算。`--distinct-limit` 设置基数集合容量，不影响事件总数统计。

不把源 CSV、标准化明细、下载清单或运行 JSON 提交 Git。`data/` 和 `tmp/` 已忽略；本文只记录聚合证据。

## 接口与下一步

### 跨月准备入口

新增完整月份下载和稳定用户采样，仍不依赖 Docker 或第三方 Python 包。登记窗口为 2019-10-01（包含）至 2019-12-01（不包含），选择用户所用的哈希不含月份，因此一个用户的跨月有效行为不会被随机拆散。

```powershell
python -m generators.real_data fetch-month --month 2019-Oct --output-dir data/rees46/months
python -m generators.real_data fetch-month --month 2019-Nov --output-dir data/rees46/months
python -m generators.real_data sample-users --inputs data/rees46/months/2019-Oct.csv.gz data/rees46/months/2019-Nov.csv.gz --output data/rees46/oct-nov-users-2pct-verified.jsonl --basis-points 200 --seed graduation-v1
```

`fetch-month` 单文件上限 3 GiB、总读取预算 30 分钟，并预留至少 1 GiB 磁盘余量。以临时文件写入；中断不发布完整文件，此版不做下载断点续传。默认不覆盖已有档案或清单。

`sample-users` 直接读取完整 gzip，所有非空记录先核验事件时间属于登记月份，不能借缺少用户或列数错误跳过日期检查；SHA-256 与 gzip 校验通过后才发布 JSONL。全字段校验仅对选中的用户记录执行；非选中用户没有通过完整业务字段校验的保证。空白/结构错误/缺少用户的记录与已选用户非法事件分别计数，不静默丢弃质量问题。

三个 CSV 入口共用解析前的逻辑记录限制：最多 65,536 字符（含换行），带引号的多行记录共用同一预算。超过限制即失败并清理临时输出，不先构造异常超宽行后才检查列数；这不改变正常 CSV 的原始记录序号。

抽样规则是 `SHA-256(dataset_id + NUL + seed + NUL + user_id)` 前 8 字节按大端解释后对 10,000 取余，小于 200 则选中。它是约 2% 的用户样本，不是精确 2% 的事件、不代表全店总量。更换种子或比例属于新数据版本。

输出保留每条事件的原始月份文件与原始记录序号，和旧 `profile --scope user_sample` 接口不同：后者接收已抽样但不带源行号的原始九列 CSV，所以仍拒绝直接标准化。

下载清单中的 `gzip_integrity_verified=false` 仅表示下载阶段未校验压缩内容；完成后的样本清单逐源记录校验结果，不改写历史下载证据。文件名及来源一致不能取代校验值匹配。

### 后续接入

新事件格式包含原始九字段与 `schema_version,event_id,dataset_id,source_file,source_row_number`，金额用定点字符串、业务 ID 用字符串、未知维度用 null。稳定 ID 依赖原始文件身份和原始记录序号，不能把抽样后重排行号当原始行号。当前工具不能替代 Kafka 可恢复回放器。

`--scope user_sample` 当前仅支持画像，不允许 `--normalized`；缺少原始记录序号时拒绝生成看似可复用的事件 ID。CSV 空白记录计入扫描和拒绝数，不会使后续有效记录的原始序号偏移。

已实测：从 1 万条样本与百万条样本分别标准化前 1 万条，输出校验值完全相同。最终解析器重新扫描百万条后，计数、时间、缺失和事件金额核验值与初次结果一致。测试夹具只用于边界验证，不作为上述业务数据来源。

最终自动化验证：`python -m unittest discover -s tests -q` 共 449 项通过，耗时 165.842 秒，其中真实数据相关测试 40 项。动态 Docker 验收不在此次结果内。首轮前缀开发时独立审查曾因额度限制未完成；本次跨月实现完成独立审查，复现并修复“缺用户绕过日期检查”和“超宽 CSV 记录撑大内存”两项问题，复核未发现这两项修复引入明确回归。

下一步进入 G2 的可恢复历史回放：复用已验证的标准化 JSONL，不重复生成或改写业务时间；建立独立 Topic/表、DataStream 适配和版本化指标 API。各业务分组的有效样本量、会话边界及留存分母规则须在相应指标上线前验收。随后接前端与知识库；RAG 必须与最终指标口径同版本，不提前把待确认口径发布为可信知识。

G2-A 回放器现已实现，真实样本 400+600 条离线恢复与逐条核验通过；其专用 Kafka 适配器、操作命令、动态验收状态及重复发送边界见 [回放运行手册](replay-runbook.md)。新 DataStream 事件格式和湖仓表尚未接入。

## GitHub 备份边界

远程仓库为 https://github.com/pzjvpfwsfv-sudo/Ecommerce ，当前开发分支为 `codex/chapter-10-controlled-tools`。代码、配置模板、测试、中文设计及验收文档可以随该分支提交备份；主工作区未提交的其他修改不包含在本次开发提交中。

Git 不是完整运行环境备份：被忽略的真实数据、临时报告、密钥、Docker 卷及数据库内容不上传。本文保存了数据下载入口、抽样参数和校验值用于重建；如需保护运行现场，应另行备份数据盘和服务卷，不能把 GitHub 上有代码当作数据库已经备份。
