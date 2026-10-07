# G3 可视化工作台运行与验收手册

## 边界

工作台使用同源 `/app/` 页面和已有 FastAPI 指标接口；页面本身可公开加载，指标仍由服务端会话和角色鉴权。`WEB_DIST_DIR` 留空时 API-only 部署不挂载页面；设置为 `/web` 时，FastAPI 只读提供预先构建的静态文件。缺少 `index.html` 会阻止 API 启动，避免出现空白工作台。页面使用 HashRouter，不需要服务端 SPA 路由回退。

Olist 历史订单和 REES46 行为事件是独立来源，不合并用户身份或伪造跨源转化率。工作台展示已发布指标与证据，不把历史回放标成当日实时数据，也不在 API 失败时填演示值。

## 构建与启动

在项目根目录的 PowerShell 中执行；npm 缓存和临时文件均放 D 盘：

```powershell
$env:TEMP = 'D:\EcommerceDev\temp'
$env:TMP = 'D:\EcommerceDev\temp'
npm ci --prefix apps/web --cache D:\EcommerceDev\cache\npm
npm run build --prefix apps/web
$env:WEB_DIST_DIR = '/web'
docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile serving config --quiet
docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile serving up -d api
```

启动前先按 [G3 身份与权限运行手册](g3-auth-runbook.md) 设置**已有** `APP_DB_PASSWORD`，确认 Docker Desktop 和已发布指标服务可用。已有 `api` 容器可能被 Compose 重建；不要为了重试删除 `app-postgres-data`、Doris 或其他卷。每次重建 API 都要在当前终端重新设置 `WEB_DIST_DIR=/web`；不要把口令写入 `.env.example`。若只需要旧 API 而不提供工作台，保持 `WEB_DIST_DIR` 为空即可。

访问 `http://127.0.0.1:8000/app/`。`/health`、`/ready` 和受保护指标路由保持原路径；未登录读取指标应返回 401。

## 验收命令

```powershell
npm test --prefix apps/web -- --run
npm run typecheck --prefix apps/web
npm run build --prefix apps/web
python -m unittest tests.test_g3_web_static -v
python -m unittest discover -s tests -q
docker compose --env-file infra/.env.example -f infra/docker-compose.yml config --quiet
```

Python 应使用安装了 `services/api/requirements.txt` 和 `generators/requirements.txt` 的虚拟环境；本机环境位于 `D:\EcommerceDev\venv`。完整浏览器流程需要已发布 Olist 指标、管理员或分析员测试账号以及运行中的服务，不使用请求模拟：

```powershell
$env:G3_WEB_URL = 'http://127.0.0.1:8000/app/'
$env:G3_E2E_USERNAME = Read-Host '测试账号'
$secret = Read-Host '测试口令' -AsSecureString
$env:G3_E2E_PASSWORD = [System.Net.NetworkCredential]::new('', $secret).Password
npm run test:e2e --prefix apps/web
Remove-Item Env:G3_E2E_PASSWORD
Remove-Item Env:G3_E2E_USERNAME
```

浏览器测试使用系统 Edge，覆盖桌面与 390px 手机宽度的登录、总览、FULL 筛选、证据抽屉、质量页和退出。截图及 Playwright 临时结果位于 `D:\EcommerceDev\temp\g3-playwright`。真实运行时在证据抽屉记录响应的 `dataset_id`、`metric_run_id`、窗口和发布时间，并核对筛选前后是否仍为同一已发布 run；不要沿用旧文档中的 run ID 充当本次验证结果。

## 本次记录（2026-10-08）

- 前端 38 项单测、类型检查和生产构建通过；构建主 JS 约 864 kB（gzip 281 kB），Vite 提示可进一步拆包，当前不是功能阻断。
- FastAPI 静态挂载 3 项测试通过；合并鉴权、订单、行为和静态服务的重点回归 88 项通过。Compose 在空值和 `WEB_DIST_DIR=/web` 两种配置下均通过 `config --quiet`。独立的本机 FastAPI 在 18000 端口提供构建产物，真实 Edge 的桌面和 390px 登录页测试 2 项通过，截图已检查，无明显横向溢出。
- Docker 引擎本次未启动，8000 端口无已发布 API；因此需要真实账号的 2 项浏览器测试按设计跳过。登录后的六个功能模块、真实 `dataset_id` / `metric_run_id` 和容器内 `/web` 挂载尚未在本次验证，不应称为端到端通过。没有重启或改动现有容器与卷。
- 全量 Python 首轮执行 691 项，3 失败、6 错误、1 跳过，不能称为全绿。其中 `kafka-python` 漏装到本机虚拟环境；按 `generators/requirements.txt` 安装后，该组 5 项单测通过。旧 Chapter 10.5 冷启动验证单独执行 34 项，仍有 3 失败、1 错误，集中在原有 PowerShell 子进程/时限测试；本次没有修改相关脚本，需另行排障。
