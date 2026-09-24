# G3 身份与权限运行手册

## 边界

业务身份数据只进 `app-postgres`，不与 Hive Metastore 共库。管理员、分析员、只读用户都可读取指标；只有前两者可调用分析，只有管理员可管理用户。`/health` 与 `/ready` 是无敏感信息的探活接口。会话 cookie 为 HttpOnly、SameSite=Lax、8 小时有效；HTTPS 下带 Secure。受保护 POST 还要带登录响应中的 `csrf_token`，作为 `X-CSRF-Token`。

## 本机启动

在项目根目录的 PowerShell 中，先以交互方式设置仅对当前终端有效的数据库密码，不把口令写入 `.env.example` 或 Git：

```powershell
$secret = Read-Host 'APP_DB_PASSWORD' -AsSecureString
$env:APP_DB_PASSWORD = [System.Net.NetworkCredential]::new('', $secret).Password
docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile serving config --quiet
docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile serving up -d app-postgres api
```

数据库首次初始化后，改环境变量**不会**自动修改已有数据库口令。若此前已建卷，应使用原口令；不要为重试而删除卷。`app-postgres-data` 是独立命名卷，Docker Desktop 的数据位置以本机 `docker info` 为准；本项目当前 Docker 数据根在 `D:\DockerData\wsl`，不是 Metastore 卷。

API 容器正常启动后，在同一终端交互输入一次性管理员账号口令：

```powershell
$env:APP_ADMIN_USERNAME = Read-Host 'APP_ADMIN_USERNAME'
$adminSecret = Read-Host 'APP_ADMIN_PASSWORD (至少 12 位)' -AsSecureString
$env:APP_ADMIN_PASSWORD = [System.Net.NetworkCredential]::new('', $adminSecret).Password
docker exec -e APP_ADMIN_USERNAME -e APP_ADMIN_PASSWORD ecom-api python /app/scripts/bootstrap_g3_admin.py
$LASTEXITCODE
Remove-Item Env:APP_ADMIN_PASSWORD
```

退出码 `0` 表示创建成功或已有管理员，`2` 表示输入缺失/过短，`3` 表示用户名已属于非管理员，`4` 表示数据库或写入失败。脚本不打印口令。完成操作后也可移除当前终端的 `APP_DB_PASSWORD`；后续 `docker compose` 操作若需要重建容器，须重新提供原数据库口令。

## 验证

1. 未登录直接调用 `/api/v1/orders/overview`、`/api/v1/behavior/funnel` 或 `/metrics/realtime` 应返回 401；`/health` 应返回 200。
2. `POST /api/v1/auth/login` 用正确账号口令登录；响应只含 `id`、`username`、`role`、`csrf_token`，浏览器收到 `ecom_session` cookie。错误口令返回 401。
3. `GET /api/v1/auth/me` 应返回当前身份。只读用户调用两个 `/analysis/*` POST 和 `/api/v1/auth/users` 应返回 403；分析员可分析但不能管理用户。
4. 已登录的状态修改请求不带或带错 `X-CSRF-Token` 应返回 403；`POST /api/v1/auth/logout` 带正确 token 后，旧 cookie 再访问 `/me` 应返回 401。
5. 认证库不可用时，登录及已登录的受保护指标请求应返回 503，而非放行或退回演示数据。不要在生产环境为此测试停止共享数据库；本项优先在隔离验证环境进行。

## 本次验收记录（2026-09-25）

- 离线 `tests.test_g3_auth_api` 通过 16 项；完整回归通过 688 项、跳过 1 项；Compose `config --quiet` 通过。
- 另起 `g3-auth-verify` Compose 项目和独立 `app-postgres-data` 卷，仅向 `127.0.0.1:15432` 开放测试端口；没有重启现有 `ecom-api`、Metastore 或 Doris。真实 PostgreSQL 中确认三张表、管理员重复初始化、生产默认 `create_app(settings=...)` 装配、登录与 401/403、CSRF、注销后会话撤销、数据库仅保存会话摘要。主动停止**这个隔离库**后，登录和已登录指标请求均返回 503，之后恢复并停止测试容器；测试卷保留，未删除。
- 这不是现有 `ecom-api` 容器的端到端 HTTP 验收，也未验证前端登录流程。上线或答辩前仍需按“本机启动”步骤用目标容器再检查一次。业务数据仍来自 Olist 与 REES46 的已发布指标，不因登录功能改变口径。
