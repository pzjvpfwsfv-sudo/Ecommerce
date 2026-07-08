[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot "infra/docker-compose.yml"
$envFile = Join-Path $repoRoot "infra/.env.example"
$chapter5Verify = Join-Path $repoRoot "scripts/verify_chapter_5_end_to_end.ps1"
$querySqlPath = Join-Path $repoRoot "jobs/sql/11_trino_read_iceberg_user_behavior.sql"
$trinoBaseUrl = "http://localhost:8088"

function Wait-ForTrinoReady {
    param([int]$TimeoutSeconds = 90)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $info = Invoke-RestMethod -Method Get -Uri "$trinoBaseUrl/v1/info"
            if ($info.nodeVersion.version) {
                return
            }
        } catch {
            Start-Sleep -Seconds 3
        }
    }

    throw "Timed out waiting for Trino readiness."
}

function Invoke-TrinoStatement {
    param([string]$Sql)

    $response = Invoke-RestMethod `
        -Method Post `
        -Uri "$trinoBaseUrl/v1/statement" `
        -Headers @{
            "X-Trino-User" = "codex"
            "X-Trino-Source" = "chapter-6-validation"
            "X-Trino-Catalog" = "lakehouse"
            "X-Trino-Schema" = "analytics"
        } `
        -Body $Sql `
        -ContentType "text/plain"

    $rows = @()
    while ($true) {
        if ($response.data) {
            $rows += $response.data
        }
        if (-not $response.nextUri) {
            break
        }
        $response = Invoke-RestMethod -Method Get -Uri $response.nextUri
    }

    if ($response.error) {
        throw "Trino query failed: $($response.error.message)"
    }

    return $rows
}

Write-Host "[chapter6-verify] preparing iceberg data through chapter 5 verification..."
& $chapter5Verify

Write-Host "[chapter6-verify] starting trino service..."
docker compose --env-file $envFile -f $composeFile --profile lakehouse up -d trino | Out-Null

Write-Host "[chapter6-verify] waiting for trino readiness..."
Wait-ForTrinoReady

$sqlText = Get-Content -Path $querySqlPath -Raw
$statements = $sqlText -split ";"
$statements = $statements | ForEach-Object { $_.Trim() } | Where-Object { $_ }

$countRows = Invoke-TrinoStatement -Sql $statements[0]
if (-not $countRows -or [int]$countRows[0][0] -le 0) {
    throw "Trino count query returned zero rows."
}

$groupRows = Invoke-TrinoStatement -Sql $statements[1]
if (-not $groupRows -or $groupRows.Count -lt 1) {
    throw "Trino group query returned no event_type rows."
}

Write-Host "[chapter6-verify] event_count=$($countRows[0][0])"
Write-Host "[chapter6-verify] top_event_type=$($groupRows[0][0]) count=$($groupRows[0][1])"
