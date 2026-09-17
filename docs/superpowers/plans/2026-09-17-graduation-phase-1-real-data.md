# 毕设 G1：真实数据依据实施计划

> 执行方式：按 executing-plans 的任务检查点执行，使用 test-driven-development 先验证失败测试；完成后按 verification-before-completion 核验。本次用户已确认规划并开始实施，不重复等待执行方式选择。

**目标：** 提供可复现的真实数据下载样本、标准化和画像能力，给业务功能选型提供证据；不提前改写既有 Kafka/Flink 生产格式。

**架构：** Python 标准库流式读取 CSV/CSV.GZ，逐条校验并输出可选 NDJSON；计数和有界基数统计生成 JSON 画像。固定官方来源的有限范围下载生成带校验值的前缀样本，不伪装成完整月份。

**技术：** Python 3.12，unittest，csv/gzip/decimal/hashlib/urllib；本阶段不增加依赖，不启动 Docker。

**设计：** `docs/superpowers/specs/2026-09-17-graduation-system-design.md`。

## 全局约束

- 修改位于已有隔离工作区；不覆盖主目录用户修改，不清理现有卷。
- 原始数据和运行报告写入已忽略的 `data/`、`tmp/`，不提交百万行数据。
- 不补造渠道、设备、会话、订单、人口画像字段；测试夹具明确仅用于单元测试。
- 前缀、截断扫描、完整文件的含义分别记录；不由少量样本推断留存、全量 UV 或完整路径。
- 使用确定性 ID、UTC 原业务时间、字符串业务 ID 和十进制金额。

## 任务 1：标准化与画像

文件：新增 `generators/real_data/__init__.py`、`normalization.py`、`profiling.py`、`file_io.py`；测试 `tests/test_real_data.py`。

接口：`normalize_event(row, *, dataset_id, source_file, row_number) -> dict`；`profile_file(input_path, *, dataset_id, source_file, scope="unverified", max_rows=None, normalized_path=None, distinct_limit=10000) -> dict`。

- [x] 写失败测试：真实时间保留，ID 重试稳定且不同原始行不相同，缺失值保留，非法金额/时间/事件拒绝，CSV/GZIP 一致，画像计数、缺失和逆序正确，基数封顶后明确非精确，输出路径不覆盖源文件。
- [x] 运行 `python -m unittest discover -s tests -p test_real_data.py -v`，确认因功能缺失失败。
- [x] 实现逐行标准化和画像；不保留全部事件或无界用户集合。非法业务行计数，文件解析失败中止；可选标准化输出使用临时文件和失败清理，已有文件拒绝覆盖。
- [x] 重跑测试，核对标准化输出不含模拟属性。CLI 后续直接复用此接口。

测试核心断言：

```python
event = normalize_event(row, dataset_id="test-only", source_file="fixture.csv", row_number=1)
assert event["event_time"] == "2019-10-01T00:00:00+00:00"
assert event["brand"] is None
assert "device_type" not in event
assert event["event_id"] == normalize_event(row, dataset_id="test-only", source_file="fixture.csv", row_number=1)["event_id"]
```

## 任务 2：来源登记、受限下载与 CLI

文件：新增 `configs/datasets/rees46-multicategory.json`、`generators/real_data/download.py`、`__main__.py`；测试 `tests/test_real_data_cli.py`。

接口：`download_prefix(output_path, *, rows, max_compressed_bytes=67108864) -> dict`。固定 October 官方地址；使用 HTTP Range，拒绝忽略范围的响应。输出 CSV 及 `.provenance.json`，记录前缀性质、实际行数、来源 URL、文件大小与 SHA-256。下载不提供完整月份认证。

- [x] 写失败测试：固定来源、正确前缀提取、校验值、范围被忽略、压缩字节上限、损坏流、已有输出拒绝覆盖、失败不留完整文件、CLI 非法参数和报告路径冲突。
- [x] 验证失败后实现 `fetch-prefix` 和 `profile` 子命令。异常返回非零，不打印成功；报告不得覆盖输入或输出数据。
- [x] 运行两组新测试与既有回归测试。

使用入口：

```powershell
python -m generators.real_data fetch-prefix --output data/rees46/2019-Oct-prefix.csv --rows 1000000
python -m generators.real_data profile --input data/rees46/2019-Oct-prefix.csv --source-file 2019-Oct.csv.gz --scope prefix --report tmp/graduation/rees46-prefix-profile.json
```

## 任务 3：真实验证、文档与交付

文件：新增 `docs/graduation/data-readiness.md`，更新 `README.md` 的毕设入口。

- [x] 先运行小规模真实前缀检查；网络允许时读取百万级前缀。记录实际规模，不把未完成下载算作成功。
- [x] 记录时间范围、事件分布、缺失、非法行、顺序和基数是否精确；缺少完整观察窗口时保留“不支持留存验收”的结论。
- [x] 核对对账：扫描行数 = 有效行数 + 拒绝行数；事件分布和每日分布之和等于有效行数。
- [x] `git diff --check`，运行 Python 回归；本地检查发现的问题补回归测试。独立审查未完成的限制见下文。
- [x] 交付当前工具与证据，明确 G2 真实 Kafka/Flink 适配、G3 前端、G4 RAG 尚未完成。不自动合并或推送。

## G1 剩余数据验收

前缀工具交付后的完整月份、两个月观察窗口、稳定用户抽样和基础样本量已由 `2026-09-17-graduation-cross-month-data.md` 补齐：109,950,743 条源记录生成 2,199,938 条有效样本，覆盖 61 天。分组有效样本量、会话边界及观察期不足时的留存规则仍须在对应指标上线前验收。下一阶段单独编写 G2 实施计划，不用模糊的“接入全部功能”替代接口设计。

## 本次检查记录

- 新增 21 项测试经过失败到通过的验证。
- 最终 `python -m unittest discover -s tests -q`：430 项通过，耗时 163.595 秒。测试输出中的预期降级/失败日志不代表测试失败；最终结果为 OK，退出码 0。
- 百万条画像复跑与初次结果一致，原始行身份跨不同前缀大小保持稳定。没有运行 Docker 全链路验收。
- 本地检查修复：CSV 空白记录导致源序号偏移、缺少原始序号的用户样本导出、错误类型的来源清单缺少安全报错。
- 本轮前缀开发的独立审查因工具额度限制未完成；后续跨月实现已完成独立审查与问题修复，最终回归为 449 项通过，详见跨月计划及数据验收文档。
- 已有 `test_chapter_4_artifacts` 的三个固定网段断言与当前 Compose 参数化配置不符，已将测试同步为带默认值的参数断言；没有修改 Docker 配置。
