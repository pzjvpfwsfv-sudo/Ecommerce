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

function Remove-StaleChapter5Containers {
    $canonicalNames = @(
        "ecom-kafka-controller",
        "ecom-kafka",
        "ecom-minio",
        "ecom-minio-init",
        "ecom-hive-metastore",
        "ecom-trino",
        "ecom-flink-jobmanager",
        "ecom-flink-taskmanager",
        "ecom-flink-sql-client"
    )

    $existingNames = docker ps -a --format "{{.Names}}"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to inspect existing Docker containers before Chapter 5 startup."
    }

    $staleNames = $existingNames | Where-Object {
        $_ -match "^[0-9a-f]{12}_ecom-" -and $_ -notin $canonicalNames
    }

    foreach ($staleName in $staleNames) {
        Write-Host "[chapter5] removing stale container $staleName..."
        docker rm -f $staleName | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to remove stale container $staleName before Chapter 5 startup."
        }
    }
}

function Wait-ForHiveMetastorePortReady {
    param(
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            docker exec ecom-flink-sql-client bash -lc "echo > /dev/tcp/hive-metastore/9083" 2>$null | Out-Null
        } catch {
        }

        if ($LASTEXITCODE -eq 0) {
            return
        }

        Start-Sleep -Seconds 3
    }

    throw "Timed out waiting for Hive Metastore thrift port to become reachable from the Flink SQL client."
}

$composeFile = "infra/docker-compose.yml"
$envFile = "infra/.env.example"
$icebergRuntimeJar = "infra/compose/flink/lib/iceberg-flink-runtime-1.19-1.6.1.jar"
$icebergRuntimeUrl = "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/1.6.1/iceberg-flink-runtime-1.19-1.6.1.jar"
$flinkHiveConnectorJar = "infra/compose/flink/lib/flink-sql-connector-hive-3.1.3_2.12-1.19.2.jar"
$flinkHiveConnectorUrl = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/1.19.2/flink-sql-connector-hive-3.1.3_2.12-1.19.2.jar"
$icebergAwsBundleJar = "infra/compose/flink/lib/iceberg-aws-bundle-1.6.1.jar"
$icebergAwsBundleUrl = "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar"
$hadoopClientApiJar = "infra/compose/flink/lib/hadoop-client-api-3.3.6.jar"
$hadoopClientApiUrl = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar"
$hadoopClientRuntimeJar = "infra/compose/flink/lib/hadoop-client-runtime-3.3.6.jar"
$hadoopClientRuntimeUrl = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar"
$hadoopAwsJar = "infra/compose/flink/lib/hadoop-aws-3.3.6.jar"
$hadoopAwsUrl = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar"
$awsSdkBundleJar = "infra/compose/flink/lib/aws-java-sdk-bundle-1.12.262.jar"
$awsSdkBundleUrl = "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
$sqlFiles = @(
    "jobs/sql/00_enable_iceberg_checkpointing.sql",
    "jobs/sql/01_source_user_behavior.sql",
    "jobs/sql/06_create_iceberg_catalog.sql",
    "jobs/sql/07_sink_user_behavior_to_iceberg.sql"
)
$combinedSqlFile = "tmp/chapter_5_flink_job.sql"
$containerName = "ecom-flink-sql-client"
$containerSqlPath = "/workspace/tmp/chapter_5_flink_job.sql"
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

Assert-DockerAvailable
Remove-StaleChapter5Containers

foreach ($jar in @(
    @{ Path = $icebergRuntimeJar; Url = $icebergRuntimeUrl },
    @{ Path = $flinkHiveConnectorJar; Url = $flinkHiveConnectorUrl },
    @{ Path = $icebergAwsBundleJar; Url = $icebergAwsBundleUrl },
    @{ Path = $hadoopClientApiJar; Url = $hadoopClientApiUrl },
    @{ Path = $hadoopClientRuntimeJar; Url = $hadoopClientRuntimeUrl },
    @{ Path = $hadoopAwsJar; Url = $hadoopAwsUrl },
    @{ Path = $awsSdkBundleJar; Url = $awsSdkBundleUrl }
)) {
    if (-not (Test-Path $jar.Path)) {
        New-Item -ItemType Directory -Force -Path (Split-Path $jar.Path) | Out-Null
        Write-Host "[chapter5] downloading $($jar.Path)..."
        Invoke-WebRequest -Uri $jar.Url -OutFile $jar.Path
    }
}

Write-Host "[chapter5] starting Kafka, Flink, MinIO, and Hive Metastore services..."
Invoke-CheckedCommand -Command { docker compose --env-file $envFile -f $composeFile --profile flink --profile lakehouse up -d --force-recreate --quiet-pull } -FailureMessage "Failed to start the Chapter 5 Compose stack."
Wait-ForHiveMetastorePortReady -TimeoutSeconds 90

[System.IO.File]::WriteAllText($combinedSqlFile, "", $utf8NoBom)
foreach ($sqlFile in $sqlFiles) {
    [System.IO.File]::AppendAllText($combinedSqlFile, (Get-Content -Raw $sqlFile), $utf8NoBom)
    [System.IO.File]::AppendAllText($combinedSqlFile, "`r`n`r`n", $utf8NoBom)
}

Write-Host "[chapter5] submitting Flink SQL Iceberg pipeline..."
$sqlOutput = docker exec $containerName /opt/flink/bin/sql-client.sh -f $containerSqlPath
if ($LASTEXITCODE -ne 0) {
    throw "Failed to submit the Chapter 5 Flink SQL pipeline."
}

$sqlOutput | Write-Host
$sqlOutputText = $sqlOutput -join "`n"
if ($sqlOutputText -match "\[ERROR\]") {
    throw "Chapter 5 Flink SQL pipeline reported statement errors."
}
