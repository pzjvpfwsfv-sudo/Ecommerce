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
$connectorJar = "infra/compose/flink/lib/flink-doris-connector-1.19-25.1.0.jar"
$connectorUrl = "https://repo1.maven.org/maven2/org/apache/doris/flink-doris-connector-1.19/25.1.0/flink-doris-connector-1.19-25.1.0.jar"
$sqlFiles = @(
    "jobs/sql/01_source_user_behavior.sql",
    "jobs/sql/04_sink_doris_metrics.sql",
    "jobs/sql/05_pv_uv_to_doris.sql"
)
$combinedSqlFile = "tmp/chapter_4_flink_job.sql"
$containerName = "ecom-flink-sql-client"
$containerSqlPath = "/workspace/tmp/chapter_4_flink_job.sql"
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

Assert-DockerAvailable

if (-not (Test-Path $connectorJar)) {
    New-Item -ItemType Directory -Force -Path (Split-Path $connectorJar) | Out-Null
    Write-Host "[chapter4] downloading Doris connector..."
    Invoke-WebRequest -Uri $connectorUrl -OutFile $connectorJar
}

Invoke-CheckedCommand -Command { docker compose --env-file $envFile -f $composeFile --profile flink --profile serving up -d --force-recreate --quiet-pull } -FailureMessage "Failed to start the Chapter 4 Compose stack."

./scripts/init_doris_realtime_metrics.ps1

Write-Host "[chapter4] ensuring Kafka topic is ready..."
$isTopicReady = $false
for ($attempt = 1; $attempt -le 30; $attempt++) {
    & docker exec ecom-kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic user_behavior_events --partitions 1 --replication-factor 1
    if ($LASTEXITCODE -eq 0) {
        $isTopicReady = $true
        break
    }

    Start-Sleep -Seconds 2
}

if (-not $isTopicReady) {
    throw "Kafka topic user_behavior_events did not become ready in time."
}

New-Item -ItemType Directory -Force -Path "tmp" | Out-Null
[System.IO.File]::WriteAllText($combinedSqlFile, "", $utf8NoBom)
foreach ($sqlFile in $sqlFiles) {
    [System.IO.File]::AppendAllText($combinedSqlFile, (Get-Content -Raw $sqlFile), $utf8NoBom)
    [System.IO.File]::AppendAllText($combinedSqlFile, "`r`n`r`n", $utf8NoBom)
}

Write-Host "[chapter4] submitting Flink SQL Doris pipeline..."
Invoke-CheckedCommand -Command { docker exec $containerName /opt/flink/bin/sql-client.sh -f $containerSqlPath } -FailureMessage "Failed to submit the Chapter 4 Flink SQL pipeline."
