import json
from pathlib import Path
import shutil
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent.parent


class Chapter8ArtifactsTest(unittest.TestCase):
    def _run_powershell(self, command: str) -> subprocess.CompletedProcess[str]:
        executable = shutil.which("pwsh") or shutil.which("powershell")
        if executable is None:
            self.skipTest("PowerShell is required for Chapter 5 behavior coverage")
        return subprocess.run(
            [executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", command],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
            check=False,
        )

    def test_env_and_compose_define_safe_ai_defaults(self):
        env_text = (ROOT / "infra/.env.example").read_text(encoding="utf-8")
        compose_text = (ROOT / "infra/docker-compose.yml").read_text(encoding="utf-8")

        for setting in (
            "TRINO_BASE_URL=http://trino:8080",
            "TRINO_USER=ecommerce-ai",
            "TRINO_CATALOG=lakehouse",
            "TRINO_SCHEMA=analytics",
            "TRINO_REQUEST_TIMEOUT_SECONDS=10",
            "AI_ANALYZER_MODE=rule_based",
            "AI_API_KEY=",
            "AI_BASE_URL=",
            "AI_MODEL=",
            "AI_REQUEST_TIMEOUT_SECONDS=15",
            "AI_MAX_QUESTION_LENGTH=500",
        ):
            self.assertIn(setting, env_text)

        for variable in (
            "TRINO_BASE_URL",
            "TRINO_USER",
            "TRINO_CATALOG",
            "TRINO_SCHEMA",
            "TRINO_REQUEST_TIMEOUT_SECONDS",
            "AI_ANALYZER_MODE",
            "AI_API_KEY",
            "AI_BASE_URL",
            "AI_MODEL",
            "AI_REQUEST_TIMEOUT_SECONDS",
            "AI_MAX_QUESTION_LENGTH",
        ):
            self.assertIn(f"{variable}: ${{{variable}}}", compose_text)

    def test_verification_script_calls_real_analysis_endpoint_in_rule_mode(self):
        script_path = ROOT / "scripts/verify_chapter_8_analysis.ps1"
        if not script_path.exists():
            self.fail("Chapter 8 real verification script is missing")

        text = script_path.read_text(encoding="utf-8")
        for marker in (
            "verify_chapter_6_trino_queries.ps1",
            "run_chapter_4_pipeline.ps1",
            "flink-sql-connector-kafka-3.3.0-1.19.jar",
            "flink-doris-connector-1.19-25.1.0.jar",
            "/analysis/realtime",
            'AI_ANALYZER_MODE = "rule_based"',
            'AI_API_KEY = ""',
            "evidence",
        ):
            self.assertIn(marker, text)

    def test_verification_requires_stable_baseline_and_exact_realtime_delta(self):
        text = (ROOT / "scripts/verify_chapter_8_analysis.ps1").read_text(encoding="utf-8")
        for marker in (
            "[switch]$FunctionsOnly",
            "$stableSampleCount",
            "$baselineResponse",
            "$baselinePv",
            "$baselineUv",
            "$baselineUpdatedAt",
            'user_id = "chapter8-$runId-user-view"',
            'user_id = "chapter8-$runId-user-click"',
            "baseline_pv=",
            "baseline_uv=",
            "baseline_updated_at=",
            "post_updated_at=",
        ):
            self.assertIn(marker, text)

        command = r'''
$ErrorActionPreference = "Stop"
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    (Resolve-Path "scripts/verify_chapter_8_analysis.ps1"),
    [ref]$tokens,
    [ref]$errors
)
if ($errors) { throw $errors[0] }
$ast.FindAll({ param($node) $node -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $true) |
    ForEach-Object { Invoke-Expression $_.Extent.Text }
$baselineTime = [DateTimeOffset]::Parse("2026-07-22T00:00:00Z")
$newTime = [DateTimeOffset]::Parse("2026-07-22T00:00:01Z")
[ordered]@{
    backlog_is_stable = Test-SameRealtimeSnapshot -PreviousPv 100 -PreviousUv 50 -PreviousUpdatedAt $baselineTime -CurrentPv 103 -CurrentUv 53 -CurrentUpdatedAt $newTime
    backlog_overshoot = Test-ExpectedRealtimeDelta -BaselinePv 100 -BaselineUv 50 -BaselineUpdatedAt $baselineTime -CurrentPv 103 -CurrentUv 53 -CurrentUpdatedAt $newTime
    pv_only = Test-ExpectedRealtimeDelta -BaselinePv 100 -BaselineUv 50 -BaselineUpdatedAt $baselineTime -CurrentPv 102 -CurrentUv 50 -CurrentUpdatedAt $newTime
    non_two = Test-ExpectedRealtimeDelta -BaselinePv 100 -BaselineUv 50 -BaselineUpdatedAt $baselineTime -CurrentPv 101 -CurrentUv 51 -CurrentUpdatedAt $newTime
    stale_time = Test-ExpectedRealtimeDelta -BaselinePv 100 -BaselineUv 50 -BaselineUpdatedAt $baselineTime -CurrentPv 102 -CurrentUv 52 -CurrentUpdatedAt $baselineTime
    exact_run = Test-ExpectedRealtimeDelta -BaselinePv 100 -BaselineUv 50 -BaselineUpdatedAt $baselineTime -CurrentPv 102 -CurrentUv 52 -CurrentUpdatedAt $newTime
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertFalse(payload["backlog_is_stable"])
        self.assertFalse(payload["backlog_overshoot"])
        self.assertFalse(payload["pv_only"])
        self.assertFalse(payload["non_two"])
        self.assertFalse(payload["stale_time"])
        self.assertTrue(payload["exact_run"])

        functions_only = self._run_powershell(
            '. (Resolve-Path "scripts/verify_chapter_8_analysis.ps1") -FunctionsOnly; "loaded"'
        )
        self.assertEqual(0, functions_only.returncode, functions_only.stderr or functions_only.stdout)
        self.assertIn("loaded", functions_only.stdout)

    def test_connector_downloads_are_hash_pinned_partial_and_atomic(self):
        text = (ROOT / "scripts/verify_chapter_8_analysis.ps1").read_text(encoding="utf-8")
        for marker in (
            "F46F69333445C598EBA9E5068B0A58DD2B4BA797738FD0FD3EE4E862FE281691",
            "CE1C35B6A16B24F67E61EE95B7DAB9802B1FB654B9DA4FE171C174B2F8B1CA36",
            '"$FilePath.partial"',
            "Get-FileHash -Algorithm SHA256",
            "Invoke-WebRequest -Uri $Url -OutFile $partialPath",
            "Move-Item -LiteralPath $partialPath -Destination $FilePath",
            "Remove-Item -LiteralPath $partialPath -Force",
        ):
            self.assertIn(marker, text)

    def test_polling_reports_latest_state_instead_of_obsolete_request_error(self):
        text = (ROOT / "scripts/verify_chapter_8_analysis.ps1").read_text(encoding="utf-8")
        success_index = text.index("$candidate = Invoke-RestMethod")
        self.assertIn("$lastRequestError = $null", text[success_index:])
        clear_index = text.index("$lastRequestError = $null", success_index)
        self.assertGreater(clear_index, success_index)
        self.assertIn("$latestInvalidResponse", text)
        self.assertIn("Latest invalid response", text)

    def test_obsolete_nginx_placeholder_is_absent(self):
        self.assertFalse((ROOT / "infra/compose/app").exists())

    def test_chapter5_dependency_preserves_hashsets_and_typed_wait_boundary(self):
        text = (ROOT / "scripts/verify_chapter_5_end_to_end.ps1").read_text(encoding="utf-8")
        self.assertIn("[switch]$FunctionsOnly", text)
        command = r'''
$ErrorActionPreference = "Stop"
. (Resolve-Path "scripts/verify_chapter_5_end_to_end.ps1") -FunctionsOnly
function Invoke-MinioAdminJson {
    param([string]$ObjectPath)
    if ($ObjectPath -eq "empty") { return @() }
    return '{"key":"nested/only.parquet"}'
}
$empty = Get-IcebergObjectNames -ObjectPath "empty"
$single = Get-IcebergObjectNames -ObjectPath "single"
function docker {
    param([Parameter(ValueFromRemainingArguments = $true)][object[]]$Arguments)
    $global:LASTEXITCODE = 0
    return @(
        "[2026-07-22 00:00:00 UTC] 1KiB STANDARD new.metadata.json",
        "[2026-07-22 00:00:00 UTC] 1KiB STANDARD new.parquet"
    )
}
$waitResult = @(Wait-ForIcebergDataCommit -BaselineMetadataNames $empty -BaselineDataNames $single -TimeoutSeconds 1)
[ordered]@{
    empty_type = $empty.GetType().FullName
    empty_count = $empty.Count
    single_type = $single.GetType().FullName
    single_count = $single.Count
    single_contains = $single.Contains("only.parquet")
    wait_count = $waitResult.Count
} | ConvertTo-Json -Compress
'''
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["empty_type"].startswith("System.Collections.Generic.HashSet`1[[System.String,"))
        self.assertEqual(0, payload["empty_count"])
        self.assertEqual(payload["empty_type"], payload["single_type"])
        self.assertEqual(1, payload["single_count"])
        self.assertTrue(payload["single_contains"])
        self.assertEqual(2, payload["wait_count"])

    def test_readme_documents_chapter_8_strict_trust_boundary(self):
        text = (ROOT / "README.md").read_text(encoding="utf-8")
        for marker in (
            "第 8 章",
            "verify_chapter_8_analysis.ps1",
            "POST /analysis/realtime",
            "严格可信模式",
            "预定义派生值",
            "NFKC",
            "fail-closed",
            "整句语义正确",
            "结构化 claim",
            "固定安全响应",
            "普通日志不含异常消息或 stack",
            "`from None` 抑制默认 traceback context",
            "Python `__context__` 对象仍可能存在",
            "不宣称递归擦除",
        ):
            self.assertIn(marker, text)
        self.assertNotIn("异常链脱敏", text)

    def test_design_and_plan_record_strict_trust_hardening(self):
        paths = (
            ROOT / "docs/superpowers/specs/2026-07-18-chapter-8-grounded-ai-analysis-design.md",
            ROOT / "docs/superpowers/plans/2026-07-18-chapter-8-grounded-ai-analysis-implementation.md",
        )
        for path in paths:
            with self.subTest(path=path.name):
                text = path.read_text(encoding="utf-8")
                for marker in (
                    "严格可信模式加固记录",
                    "预定义派生值",
                    "NFKC",
                    "fail-closed",
                    "可见中文/英文数字分隔语义",
                    "主分析器与回退分析器",
                    "LogRecord.extra",
                    "固定安全响应",
                    "普通日志不含异常消息或 stack",
                    "`from None` 抑制默认 traceback context",
                    "Python `__context__` 对象仍可能存在",
                    "不宣称递归擦除",
                    "四字段显式完整",
                    "数值可追溯不等于整句语义正确",
                    "结构化 claim",
                ):
                    self.assertIn(marker, text)
                self.assertNotIn("递归覆盖 `__cause__` 与 `__context__`", text)

    def test_docs_describe_final_hardening_and_isolated_verification_boundary(self):
        paths = (
            ROOT / "README.md",
            ROOT / "docs/superpowers/specs/2026-07-18-chapter-8-grounded-ai-analysis-design.md",
            ROOT / "docs/superpowers/plans/2026-07-18-chapter-8-grounded-ai-analysis-implementation.md",
        )
        markers = (
            "SQL/代码输出守卫",
            "Cc/Cf",
            "单条 Trino statement",
            "try(from_iso8601_timestamp(event_time))",
            "标识符白名单",
            "连续 3 次稳定",
            "PV/UV 精确增长 2",
            "不是按 runId 从 Doris 做明细审计",
        )
        for path in paths:
            with self.subTest(path=path.name):
                text = path.read_text(encoding="utf-8")
                for marker in markers:
                    self.assertIn(marker, text)


if __name__ == "__main__":
    unittest.main()
