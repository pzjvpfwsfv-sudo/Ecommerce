$ErrorActionPreference = "Stop"

$composeFile = "infra/docker-compose.yml"
$envFile = "infra/.env.example"
$sqlFiles = @(
    "jobs/sql/01_source_user_behavior.sql",
    "jobs/sql/02_sink_print_metrics.sql",
    "jobs/sql/03_pv_uv_metrics.sql"
)
$connectorJar = "infra/compose/flink/lib/flink-sql-connector-kafka-3.3.0-1.19.jar"
$connectorUrl = "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar"
$kafkaContainerName = "ecom-kafka"
$combinedSqlFile = "tmp/chapter_3_flink_job.sql"
$containerName = "ecom-flink-sql-client"
$containerSqlPath = "/workspace/tmp/chapter_3_flink_job.sql"
$flinkOverviewUrl = "http://localhost:8081/overview"
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

if (-not (Test-Path $connectorJar)) {
    New-Item -ItemType Directory -Force -Path (Split-Path $connectorJar) | Out-Null
    Write-Host "下载 Kafka connector: $connectorUrl"
    Invoke-WebRequest -Uri $connectorUrl -OutFile $connectorJar
}

Write-Host "启动 Flink 最小运行环境..."
docker compose --env-file $envFile -f $composeFile --profile flink up -d --force-recreate
if ($LASTEXITCODE -ne 0) {
    throw "Flink 最小运行环境启动失败。"
}

for ($attempt = 1; $attempt -le 30; $attempt++) {
    try {
        docker exec $kafkaContainerName kafka-topics --bootstrap-server kafka:29092 --list | Out-Null
        break
    }
    catch {
        if ($attempt -eq 30) {
            throw "Kafka broker 未在预期时间内就绪。"
        }
    }

    Start-Sleep -Seconds 2
}

for ($attempt = 1; $attempt -le 30; $attempt++) {
    try {
        $response = Invoke-WebRequest -Uri $flinkOverviewUrl -UseBasicParsing -TimeoutSec 5
        if ($response.StatusCode -eq 200) {
            break
        }
    }
    catch {
        if ($attempt -eq 30) {
            throw "Flink Web UI 未在预期时间内就绪: $flinkOverviewUrl"
        }
    }

    Start-Sleep -Seconds 2
}

New-Item -ItemType Directory -Force -Path "tmp" | Out-Null
[System.IO.File]::WriteAllText($combinedSqlFile, "", $utf8NoBom)

foreach ($sqlFile in $sqlFiles) {
    Write-Host "合并 SQL: $sqlFile"
    [System.IO.File]::AppendAllText($combinedSqlFile, (Get-Content -Raw $sqlFile), $utf8NoBom)
    [System.IO.File]::AppendAllText($combinedSqlFile, "`r`n`r`n", $utf8NoBom)
}

Write-Host "提交 SQL: $containerSqlPath"
docker exec $containerName /opt/flink/bin/sql-client.sh -f $containerSqlPath
if ($LASTEXITCODE -ne 0) {
    throw "Flink SQL 提交失败。"
}

Write-Host "SQL 提交完成，可访问 http://localhost:8081 查看 Flink Web UI。"
