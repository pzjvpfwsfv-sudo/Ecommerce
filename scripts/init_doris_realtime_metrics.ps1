param(
    [string]$EnvFile = (Join-Path $PSScriptRoot '..\infra\.env.example')
)

$ErrorActionPreference = "Stop"

function Invoke-CheckedCommand {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$Command,
        [Parameter(Mandatory = $true)]
        [string]$FailureMessage
    )

    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw $FailureMessage
    }
}

function Assert-DockerAvailable {
    Invoke-CheckedCommand -Command { docker version } -FailureMessage "Docker daemon is not available. Please start Docker Desktop and retry."
}

$repositoryRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$composeFile = Join-Path $repositoryRoot "infra/docker-compose.yml"
$EnvFile = [System.IO.Path]::GetFullPath($EnvFile)
$sqlFile = Join-Path $repositoryRoot "infra/compose/doris/init/01_create_realtime_metrics.sql"
$composePrefix = @('compose', '--env-file', $EnvFile, '-f', $composeFile, '--profile', 'serving')

Assert-DockerAvailable
Invoke-CheckedCommand -Command { docker @composePrefix up -d --quiet-pull doris-fe doris-be } -FailureMessage "Failed to start Doris FE/BE containers."

Write-Host "[chapter4] waiting for Doris FE query port..."
$isReady = $false
for ($attempt = 1; $attempt -le 30; $attempt++) {
    & docker @composePrefix exec -T doris-fe sh -lc "mysql -uroot -h127.0.0.1 -P9030 -e 'SELECT 1'" | Out-Null
    if ($LASTEXITCODE -eq 0) {
        $isReady = $true
        break
    }

    Start-Sleep -Seconds 2
}

if (-not $isReady) {
    throw "Doris FE query port did not become ready in time."
}

Write-Host "[chapter4] waiting for Doris BE heartbeat..."
$isBackendReady = $false
for ($attempt = 1; $attempt -le 30; $attempt++) {
    $backendStatus = & docker @composePrefix exec -T doris-fe sh -lc "mysql -uroot -h127.0.0.1 -P9030 -N -e 'SHOW BACKENDS'" 2>$null
    if ($LASTEXITCODE -eq 0 -and (($backendStatus -join "`n") -match "`ttrue`t")) {
        $isBackendReady = $true
        break
    }

    Start-Sleep -Seconds 2
}

if (-not $isBackendReady) {
    throw "Doris BE did not become alive in time."
}

Write-Host "[chapter4] initializing analytics.realtime_metrics..."
Get-Content -Raw $sqlFile | & docker @composePrefix exec -T doris-fe sh -lc "mysql -uroot -h127.0.0.1 -P9030"
if ($LASTEXITCODE -ne 0) {
    throw "Failed to initialize analytics.realtime_metrics in Doris."
}

Write-Host "[chapter4] Doris table is ready."
