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

$composeFile = "infra/docker-compose.yml"
$envFile = "infra/.env.example"
$feContainerName = "ecom-doris-fe"
$sqlFile = "infra/compose/doris/init/01_create_realtime_metrics.sql"

Assert-DockerAvailable
Invoke-CheckedCommand -Command { docker compose --env-file $envFile -f $composeFile --profile serving up -d --quiet-pull doris-fe doris-be } -FailureMessage "Failed to start Doris FE/BE containers."

Write-Host "[chapter4] waiting for Doris FE query port..."
$isReady = $false
for ($attempt = 1; $attempt -le 30; $attempt++) {
    & docker exec $feContainerName sh -lc "mysql -uroot -h127.0.0.1 -P9030 -e 'SELECT 1'" | Out-Null
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
    $backendStatus = & docker exec $feContainerName sh -lc "mysql -uroot -h127.0.0.1 -P9030 -N -e 'SHOW BACKENDS'" 2>$null
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
Get-Content -Raw $sqlFile | & docker exec -i $feContainerName sh -lc "mysql -uroot -h127.0.0.1 -P9030"
if ($LASTEXITCODE -ne 0) {
    throw "Failed to initialize analytics.realtime_metrics in Doris."
}

Write-Host "[chapter4] Doris table is ready."
