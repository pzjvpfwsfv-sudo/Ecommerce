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
$icebergRuntimeJar = "infra/compose/flink/lib/iceberg-flink-runtime-1.19-1.6.1.jar"
$icebergRuntimeUrl = "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/1.6.1/iceberg-flink-runtime-1.19-1.6.1.jar"
$icebergAwsBundleJar = "infra/compose/flink/lib/iceberg-aws-bundle-1.6.1.jar"
$icebergAwsBundleUrl = "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar"
$hadoopClientApiJar = "infra/compose/flink/lib/hadoop-client-api-3.3.6.jar"
$hadoopClientApiUrl = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar"
$hadoopClientRuntimeJar = "infra/compose/flink/lib/hadoop-client-runtime-3.3.6.jar"
$hadoopClientRuntimeUrl = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar"
$sqlFiles = @(
    "jobs/sql/01_source_user_behavior.sql",
    "jobs/sql/08_create_iceberg_catalog_local.sql",
    "jobs/sql/09_sink_user_behavior_to_iceberg_local.sql"
)
$combinedSqlFile = "tmp/chapter_5_local_validation.sql"
$containerName = "ecom-flink-sql-client"
$containerSqlPath = "/workspace/tmp/chapter_5_local_validation.sql"
$flinkOverviewUrl = "http://localhost:8081/overview"
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

Assert-DockerAvailable

foreach ($jar in @(
    @{ Path = $icebergRuntimeJar; Url = $icebergRuntimeUrl },
    @{ Path = $icebergAwsBundleJar; Url = $icebergAwsBundleUrl },
    @{ Path = $hadoopClientApiJar; Url = $hadoopClientApiUrl },
    @{ Path = $hadoopClientRuntimeJar; Url = $hadoopClientRuntimeUrl }
)) {
    if (-not (Test-Path $jar.Path)) {
        New-Item -ItemType Directory -Force -Path (Split-Path $jar.Path) | Out-Null
        Write-Host "[chapter5-local] downloading $($jar.Path)..."
        Invoke-WebRequest -Uri $jar.Url -OutFile $jar.Path
    }
}

New-Item -ItemType Directory -Force -Path "tmp/iceberg-warehouse" | Out-Null
Invoke-CheckedCommand -Command { docker compose --env-file $envFile -f $composeFile --profile flink up -d --force-recreate --quiet-pull } -FailureMessage "Failed to start the local filesystem validation stack."

Write-Host "[chapter5-local] ensuring Kafka topic is ready..."
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

Write-Host "[chapter5-local] waiting for Flink REST API..."
$isFlinkReady = $false
for ($attempt = 1; $attempt -le 30; $attempt++) {
    try {
        $response = Invoke-WebRequest -Uri $flinkOverviewUrl -UseBasicParsing -TimeoutSec 5
        if ($response.StatusCode -eq 200) {
            $isFlinkReady = $true
            break
        }
    }
    catch {
        # Flink can accept HTTP requests only after the JobManager finishes booting.
    }

    Start-Sleep -Seconds 2
}

if (-not $isFlinkReady) {
    throw "Flink REST API did not become ready in time: $flinkOverviewUrl"
}

[System.IO.File]::WriteAllText($combinedSqlFile, "", $utf8NoBom)
foreach ($sqlFile in $sqlFiles) {
    [System.IO.File]::AppendAllText($combinedSqlFile, (Get-Content -Raw $sqlFile), $utf8NoBom)
    [System.IO.File]::AppendAllText($combinedSqlFile, "`r`n`r`n", $utf8NoBom)
}

Write-Host "[chapter5-local] submitting Flink SQL Iceberg filesystem warehouse validation..."
Invoke-CheckedCommand -Command { docker exec $containerName /opt/flink/bin/sql-client.sh -f $containerSqlPath } -FailureMessage "Failed to submit the local filesystem Iceberg validation pipeline."
