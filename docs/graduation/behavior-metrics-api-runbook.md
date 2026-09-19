# G2-D 行为指标与版本化 API 验收记录

## 结论

2026-09-19 对 G2-C 正式 Iceberg 表执行了真实刷新和三层验证，结果为 `PASS`。本次只验收 `g2c-correctness-subset` 的 1,002 条真实事件：Trino Snapshot、Doris 发布运行与六个 FastAPI 契约使用同一身份，四张指标表的行数和重算 SHA-256 均与发布元数据一致。

这是正确性验收，不是容量证据。1,002 条子集不是 2,199,938 条稳定用户 2% 全量样本，也不能用于推断吞吐、峰值延迟、长时稳定性或故障恢复容量。G2-D 据此关闭，下一阶段是 G2-E 独立 Olist 订单域。

## 执行命令

当前 PowerShell 进程仅使用以下 Docker CLI 回退路径，Python 临时目录固定为 `D:\EcommerceDev\temp`：

```powershell
$env:PATH='D:\DockerProgram\Docker\resources\bin;' + $env:PATH
$env:TEMP='D:\EcommerceDev\temp'
$env:TMP='D:\EcommerceDev\temp'
```

启动的服务仅限于计划中的 lakehouse、Doris 和 API 依赖：

```powershell
docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile lakehouse --profile serving up -d minio minio-init metastore-postgres doris-fe doris-be
docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile lakehouse run -d --name ecom-hive-metastore -e IS_RESUME=true hive-metastore
docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile lakehouse --profile serving up -d --no-deps trino api
```

启动后实测：Trino `http://localhost:8088/v1/info` 返回版本 `458` 且 `starting=false`；Doris `SELECT 1` 返回 `1`；API `http://localhost:8000/health` 返回 `{"status":"ok","service":"realtime-metrics-api"}`。

刷新、验证和回归命令：

```powershell
./scripts/refresh_g2d_behavior_metrics.ps1 -DataScope g2c-correctness-subset
./scripts/verify_g2d_behavior_metrics.ps1 -ExpectedDataScope g2c-correctness-subset
python -m unittest tests.test_g2d_behavior_metrics.G2dVerifierTests -v
python -m unittest tests.test_g2d_behavior_metrics tests.test_behavior_metrics_api -v
python -m unittest discover -s tests -q
```

## 实测身份

| 字段 | 实测值 |
| --- | --- |
| 源表 | `lakehouse.analytics.real_behavior_detail_v1` |
| `dataset_id` | `rees46-multicategory` |
| `metric_version` | `behavior-v1` |
| `data_scope` | `g2c-correctness-subset` |
| Snapshot | `881836466779140976` |
| Snapshot 提交时间 | `2026-09-18 07:50:17.775 UTC` |
| `metric_run_id` | `behavior-v1-s881836466779140976` |
| 窗口 | `2019-10-01` 至 `2019-10-01` |
| Doris `calculated_at` | `2026-09-19 12:02:43.624` |
| Doris `published_at` | `2026-09-19 12:02:53.275` |
| 发布状态 | `PUBLISHED` |

Trino 源总数和不同 `event_id` 均为 1,002。Doris `DAY` 和 `FULL` 总数均为 1,002，其中 `view=973`、`cart=16`、`remove_from_cart=0`、`purchase=13`；`clean=1,001`、`late=1`、重复事件为 0，质量对账状态为 `PASS`。未知品类事件为 287，未知品牌事件为 134；非法事件类型、空关键 ID、非法价格和非法派生日期均为 0。

## Doris 指标证据

| 指标族 | 行数 | 重算 SHA-256 |
| --- | ---: | --- |
| `behavior_overview_metrics` | 2 | `88bb2539175c62936d935cc41522e13894a65c78fc9b1461626939fb69d4a30b` |
| `behavior_funnel_metrics` | 2 | `d8fbfbbb0537270b8a407387618d191920afe36299095748d53de78d5e09d9f2` |
| `behavior_dimension_metrics` | 1,780 | `06a2a98914d758812b697c902c0c0be95794ffc268c6e05b5637c4a1e6d0ef58` |
| `behavior_quality_metrics` | 1 | `76e8493ab4bec1c80fee770c284c73142e34b4434daa2626885263a1bff097e3` |

`FULL` 有序漏斗为 `view_sessions=275 -> view_to_cart_sessions=11 -> completed_sessions=5`，对应 API 比率为 `0.040000`、`0.454545`和 `0.018182`。`purchase_amount_proxy=6003.15` 只是购买事件 `price_decimal` 之和的分析代理值；源数据缺少数量、币种、折扣、退款、取消和支付状态，定义契约继续禁止 `GMV`、`销售额`和`收入` 表述。

## API 验收

六个端点均返回 HTTP 200。除 definitions 为固定目录身份外，其余响应均返回同一 run、Snapshot、scope、`source_event_count=1002` 以及 `correctness subset; not the full 2% user sample` 警告。

| 端点 | 实测响应摘要 |
| --- | --- |
| `/api/v1/behavior/publication` | `PUBLISHED`；四组行数和 SHA-256 与 Doris 一致 |
| `/api/v1/behavior/overview?window=full` | 1 行；`event_count=1002`；`purchase_amount_proxy="6003.15"` |
| `/api/v1/behavior/funnel?window=full` | 1 行；`275 -> 11 -> 5` |
| `/api/v1/behavior/rankings?dimension=product&window=full&sort_by=purchases&limit=20` | 20 行；首条 `dimension_id=1004857`、`purchase_count=3` |
| `/api/v1/behavior/quality` | `clean=1001`、`late=1`、`distinct=1002`、`PASS` |
| `/api/v1/behavior/definitions` | 25 个唯一定义；唯一 `purchase_amount_proxy` 定义保留限制和禁止声明 |

## 耗时与回归

- 成功 refresh 外层实测耗时 `34,791 ms`。
- verifier 报告内部耗时 `11,900 ms`，外层实测耗时 `12,557 ms`。
- verifier 输出位于被 Git 忽略的 `tmp/graduation/g2d/behavior-v1-s881836466779140976/verification.json`。
- 验收故障对应的八项聚焦回归全部通过，耗时 `5.368 s`；G2-D/API 离线组合回归通过 `67` 项，耗时 `21.909 s`。
- 最终完整回归 `python -m unittest discover -s tests -q` 通过 `552` 项，耗时 `302.152 s`。

## 实际故障与恢复

- `ecom-hive-metastore` 启动前为 `Exited (137)`。先读取日志，日志显示旧进程曾以 `SKIP_SCHEMA_INIT=true` 启动；仅删除该停止容器，再以 `IS_RESUME=true` 重建。未删除 PostgreSQL、MinIO、Iceberg 或任何 Docker 卷。
- 该 one-off metastore 容器与 Compose 固定名称发生重名冲突，因依赖已运行，后续只用 `--no-deps` 启动 `trino api`，没有替换 metastore 或卷。
- API 首次启动在 `/app/app/config.py` 因固定 `parents[3]` 越界失败；改为向上发现已存在的定义目录，容器和主机默认路径均有回归覆盖。
- refresh 首次预检使用主机 `8080`，而 Compose 实测主机端口为 `8088`；现已分离主机健康端点 `8088` 与容器内 CLI 端点 `8080`。
- Trino CLI 在非交互执行时向 stderr 输出 JLine 告警，与 CSV 合并后被严格解析器拒绝；显式传入 `TERM=dumb` 后保留严格 CSV 验证而不过滤异常行。
- Doris 首次 DDL 拒绝四张指标表的 `UNIQUE KEY` 非前缀列顺序；仅重排列声明，没有改变 key、显式 Stream Load 列或 API 投影。
- Trino 实测 Snapshot 时间为 `2026-09-18 07:50:17.775 UTC`，共享模块新增该确定格式的标准化覆盖，输出 `2026-09-18T07:50:17.775Z`。
- Doris Stream Load 先报 `There is no 100-continue header`，加入必需的 `Expect:100-continue`后，FE 又重定向到 Windows 主机无法访问的 `172.21.80.3:8040`。直连已发布的 `localhost:8040` BE 端点成功。诊断请求导入的 2 条 overview 行作为未发布候选保留；最终 refresh 检测到 `overview=2, funnel=0, dimension=0, quality=0`，以新 attempt ID 重载并通过四表摘要校验后才发布。

## 重跑边界

重跑 refresh 会使用最新正 Snapshot 派生固定 run ID。已发布 run 必须在身份、四组行数和 SHA-256 全部一致时才可复用；未发布部分候选不删除，以新 attempt ID 继续并在发布前重算完整证据。verifier 只在所有断言通过后写入 `verification.json`，任何身份、计数、摘要、窗口、漏斗、API 警告或定义限制不一致都失败关闭。
