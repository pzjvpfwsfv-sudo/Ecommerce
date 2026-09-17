# G1 跨月真实数据实施计划

日期：2026-09-17。承接已确认的毕设总设计及 G1 数据准备计划。本次实现数据观察窗口，不改动 Kafka/Flink、业务页面或 RAG 接口。

## 目标与边界

取得 REES46 2019 年 10 月、11 月完整压缩文件，逐行检查并选取稳定用户子集，形成覆盖两个连续月份、保留原始记录身份的标准化 JSONL。完整观察窗口指这两个文件提供的历史范围，不等于用户完整生命周期、完整订单、全店用户或可直接计算所有指标。

- 官方来源只允许登记的 October/November 文件，保留文件 SHA-256、HTTP 长度、来源、时间和使用说明。
- 下载传输完整与 gzip/数据校验分开记录。全文件读取通过 gzip 校验、SHA 与声明一致后才发布抽样结果。
- 文件下载使用流式读取、大小上限、剩余磁盘检查和临时文件；不覆盖已有文件。此次不实现下载断点续传，中断不发布完整文件；业务回放恢复仍属 G2。
- 稳定抽样键为数据集 ID、种子和用户 ID，不包含月份、事件类型、商品或原始行号。默认 200/10000，即约 2% 用户；事件比例和条数不预设。
- 抽中用户保留两个月内所有有效事件，原始文件名和原始记录序号传入既有标准化器，不用抽样后的序号重建 ID。
- 非法行必须计数；抽中用户的非法行影响路径完整性，报告不得隐藏。不补造缺失字段，不复制数据凑数量。
- 流式读取 gzip，不解压完整 CSV；用户/商品/会话基数集合封顶，触顶时明确不是精确计数。
- 日期越界、源文件重复、窗口不连续、校验值不符或 gzip 损坏时不发布最终样本。时间逆序显式记录，未确认排序前不称作已就绪回放流。
- 原始数据和运行报告仍在被 Git 忽略的 `data/`、`tmp/` 中。

## 任务 1：完整月份下载

新增 `generators/real_data/monthly.py`，扩展来源配置、`file_io.py` 的安全二进制写入及 CLI `fetch-month`。测试放在 `tests/test_real_data_monthly.py`。

接口：`download_month(month, output_dir, *, max_bytes=3221225472) -> dict`。支持月份为来源登记中的 `2019-Oct` 和 `2019-Nov`。返回并保存 `.provenance.json`；完整 gzip 的语义有效性由后续全扫描确认。

- [x] 先写失败测试：登记来源、实际长度与 SHA、拒绝部分响应/超限/短读/已有文件、失败清理。
- [x] 最小实现并运行测试，保留既有前缀下载兼容性。
- [x] 下载两个月真实文件，记录实际结果，不把 HTTP 成功当作数据校验通过。

## 任务 2：稳定用户采样

新增 `generators/real_data/sampling.py`；CLI 新增 `sample-users`；测试放在 `tests/test_real_data_sampling.py`。

接口：`sample_users(inputs, output_path, *, basis_points=200, seed="graduation-v1", distinct_limit=200000) -> dict`。输出版本 1 标准化 JSONL 与 `.provenance.json`，后者含逐源完整性、抽样规则、窗口、有效/拒绝计数、事件分布、每日分布、缺失统计及基数精度。

- [x] 先验证失败：固定用户跨月选择一致、保留全部已选有效事件和源行号、倒序输入自动按月份排序、拒绝前缀/重复月份/断月、损坏压缩流或错误 SHA 不发布、样本非法记录不被隐藏。
- [x] 实现流式读取与临时输出，仅归一化选中的记录；全源可解析记录时间必须处于登记月份，结构/用户缺失记录单独计数。
- [x] 全扫描真实两个月文件，核验事件计数对账和时间覆盖。运行日志显示进度，不把处理全部原始行等同于全店行为统计已经实现。

```powershell
python -m generators.real_data fetch-month --month 2019-Oct --output-dir data/rees46/months
python -m generators.real_data fetch-month --month 2019-Nov --output-dir data/rees46/months
python -m generators.real_data sample-users --inputs data/rees46/months/2019-Oct.csv.gz data/rees46/months/2019-Nov.csv.gz --output data/rees46/oct-nov-users-2pct-verified.jsonl --basis-points 200 --seed graduation-v1
```

## 任务 3：验证与交付

- [x] 全量回归与 `git diff --check`；本地检查原始行身份、拒绝记录口径和输出发布边界。
- [x] 更新 `docs/graduation/data-readiness.md`，写实测行数、日期、缺失情况和剩余限制。
- [x] 更新进度，但不标记真实 Kafka/Flink 接入、留存/漏斗指标或 RAG 完成。用户追加要求 GitHub 备份，验证后提交并推送开发分支，不自动合并 main。

验收记录：两次扫描均得到 109,950,743 条源记录、2,199,938 条样本、61 天覆盖，标准化输出 SHA 完全一致。最终版耗时 596.358 秒，抽样进程观测峰值工作集 65.09 MiB，不包含整个服务栈。449 项 Python 回归通过；独立审查提出的日期校验绕过和超宽 CSV 记录问题已补失败测试、修复并复核。实现增加共享 `csv_io.py`，在解析前限制每条逻辑记录 65,536 字符。

成功后下一阶段：基于保留源行身份的 JSONL 实现可恢复 Kafka 回放、独立表及指标对账。知识库使用这些已确认的数据字典和指标口径版本，不索引每条业务事件。
