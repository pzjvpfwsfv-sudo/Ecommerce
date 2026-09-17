# G2-A 可恢复历史回放实施计划

> 执行要求：使用 executing-plans 在当前隔离工作区逐项执行，先失败测试后实现；用户已确认本轮范围，直接推进，不重复询问执行方式。

**目标：** 从已核验的真实 JSONL 固定速率发送事件，并安全保存可恢复进度。

**架构：** 输入验证、断点及锁、回放循环、Kafka 适配器分离；CLI 默认离线试跑，只有显式 kafka 模式建立连接。

**技术：** Python 3.12 标准库与既有 kafka-python 2.1.2，不增加依赖。

**设计：** `docs/superpowers/specs/2026-09-17-graduation-replay-design.md`。

## 全局约束

- 在 `codex/chapter-10-controlled-tools` 工作，不改主目录已有修改，不修改旧链路。
- 输入为 G1 JSONL；业务时间和 ID 不变，回放 metadata 单独添加。
- 默认 dry-run、100 条/秒、单次最多 1,000 条；Kafka 目标必须已存在且使用真实数据专用前缀。
- 输入/清单/断点身份错误在建立 producer 前拒绝；确认发送后才保存前缀进度。允许故障重发，不承诺端到端 exactly-once。
- 原始数据、断点和运行报告留在 Git 忽略目录；完成验证后备份代码和文档到现有 GitHub 开发分支。

## 任务 1：回放内核与安全状态

文件：`generators/real_data/replay_source.py`（清单与事件验证）、`replay_state.py`（文件锁、JSON 断点）、`replay.py`（回放循环和 CLI）；测试 `tests/test_real_data_replay.py`。

接口：`replay_file(input_path, checkpoint_path, *, mode, bootstrap_servers, topic, rate=100, max_events=1000, checkpoint_every=100, sink_factory=None, clock=monotonic, sleep=time.sleep) -> dict`。sink 的 `send(message, key)` 只在确认成功后返回，`close()` 释放资源；测试仅替换网络和时间，不替换断点实现。

- [x] 写失败测试并运行 `python -m unittest discover -s tests -p test_real_data_replay.py -v`：预期缺少模块，正常恢复、失败未确认位置、Ctrl+C、非法源/断点/模式、文件锁、有界输入、节流均受保护。
- [x] 实现以下确认顺序及 JSON 原子替换，运行测试至通过：

```python
sink.send(message, key)
state = state | {"confirmed_records": state["confirmed_records"] + 1,
                 "last_event_id": event["event_id"]}
if state["confirmed_records"] % checkpoint_every == 0:
    save_checkpoint(checkpoint_path, state)
```

- [x] 对每条消息重算 `normalize_event` 并比较完整业务对象；对源文件用同一打开句柄计算 SHA，恢复跳过的最后事件与断点一致；不改写 `event_time`。

## 任务 2：Kafka 适配器及 CLI

文件：`generators/real_data/replay_kafka.py`，扩展 `replay.py` 的 CLI；测试 `tests/test_real_data_replay_kafka.py`。

接口：`ReplayKafkaSink(bootstrap_servers, topic)`，`send(message, key) -> None`，`close() -> None`；`main(argv=None) -> int` 返回 0 为限量/完成，130 为暂停，1 为失败。

- [x] 先写失败测试：缺少目标 Topic 时不发送，关闭自动建 Topic，发送等待 future 结果，key 为 UTF-8，超时失败不得推进断点；CLI 默认不实例化 Kafka 适配器。
- [x] 实现明确的网络边界：

```python
future = producer.send(topic, key=key.encode("utf-8"), value=message)
future.get(timeout=15)
```

- [x] 运行两组测试；在真实 G1 文件上执行 dry-run 400+600 条，核验断点总数、任务 ID 连续、业务身份不变，保存本地报告。

## 任务 3：动态核验与交付

文件：更新 `docs/graduation/data-readiness.md`、`README.md`，新增 `docs/graduation/replay-runbook.md`。

- [x] Kafka 可用时创建唯一专用验证 Topic，执行 400+600 条真实回放并独立消费对账；不可用时明确记录阻塞，不改变旧服务。
- [x] 执行 `python -m unittest discover -s tests -q`、`git diff --check`；检查恢复、清理和超时边界并补回归。
- [x] 文档写实际证据、命令及至少一次边界；验证后提交推送开发分支，远端 SHA 对账，不合并 main。

检查记录：新增 23 项回放测试，全回归 472 项通过；真实离线恢复及逐条业务内容审计通过，Windows 跨进程锁竞争/进程终止释放实测通过。独立复核未发现重要 bug。

2026-09-17 动态验收补齐：定位 Docker 临时 socket 启动失败，退出后隔离两个临时目录并成功恢复引擎，未重置或删除数据卷。仅启动现有 Kafka controller/broker，核对宿主端口为 32600 后使用新断点，保留早先连接失败的 0 条断点。专用空 Topic 上完成 400+600 条真实回放，独立消费者确认 offset `[0, 1000)`、1,000 个唯一事件 ID、全部 14 个源字段与 key 一致。再次运行 23 项回放测试通过；避开 C 盘容量不足及工作树内临时路径后，最终完整回归 472 项通过，耗时 169.690 秒，未放宽原有安全检查。详见 `docs/graduation/replay-runbook.md`。本次不扩展为新 DataStream/湖仓接入或完整故障恢复验收。
