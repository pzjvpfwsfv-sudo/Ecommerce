[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$chapter6Verify = Join-Path $repoRoot "scripts/verify_chapter_6_trino_queries.ps1"
$chapter4Run = Join-Path $repoRoot "scripts/run_chapter_4_pipeline.ps1"
$analysisUrl = "http://localhost:8000/analysis/realtime"
$kafkaConnectorJar = Join-Path $repoRoot "infra/compose/flink/lib/flink-sql-connector-kafka-3.3.0-1.19.jar"
$kafkaConnectorUrl = "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar"
$dorisConnectorJar = Join-Path $repoRoot "infra/compose/flink/lib/flink-doris-connector-1.19-25.1.0.jar"
$dorisConnectorUrl = "https://repo1.maven.org/maven2/org/apache/doris/flink-doris-connector-1.19/25.1.0/flink-doris-connector-1.19-25.1.0.jar"
$previousAnalyzerMode = [Environment]::GetEnvironmentVariable("AI_ANALYZER_MODE", "Process")
$previousApiKey = [Environment]::GetEnvironmentVariable("AI_API_KEY", "Process")

function Restore-ProcessEnvironmentVariable {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [AllowNull()]
        [string]$Value
    )

    if ($null -eq $Value) {
        Remove-Item "Env:$Name" -ErrorAction SilentlyContinue
    } else {
        [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
    }
}

function Ensure-ConnectorFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,
        [Parameter(Mandatory = $true)]
        [string]$Url,
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    if (Test-Path -LiteralPath $FilePath -PathType Container) {
        # Docker creates an empty directory when a missing bind-mounted JAR is referenced.
        Remove-Item -LiteralPath $FilePath -Force
    }
    if (-not (Test-Path -LiteralPath $FilePath -PathType Leaf)) {
        New-Item -ItemType Directory -Force -Path (Split-Path $FilePath) | Out-Null
        Write-Host "[chapter8-verify] downloading shared Flink $Name connector..."
        Invoke-WebRequest -Uri $Url -OutFile $FilePath
    }
}

Push-Location $repoRoot
try {
    # Process variables override Compose interpolation and prevent accidental model calls.
    $env:AI_ANALYZER_MODE = "rule_based"
    $env:AI_API_KEY = ""

    Ensure-ConnectorFile -FilePath $kafkaConnectorJar -Url $kafkaConnectorUrl -Name "Kafka"
    Ensure-ConnectorFile -FilePath $dorisConnectorJar -Url $dorisConnectorUrl -Name "Doris"

    Write-Host "[chapter8-verify] preparing Iceberg history and Trino..."
    & $chapter6Verify

    Write-Host "[chapter8-verify] preparing Doris realtime metrics and API..."
    & $chapter4Run

    $runId = [Guid]::NewGuid().ToString("N")
    $eventTime = [DateTimeOffset]::UtcNow.ToString("o")
    $events = @(
        [ordered]@{
            event_id = "chapter8-$runId-view"
            user_id = "chapter8-user-view"
            product_id = "chapter8-product-view"
            event_type = "view"
            event_time = $eventTime
            channel = "app"
            device_type = "mobile"
            page_id = "home"
        },
        [ordered]@{
            event_id = "chapter8-$runId-click"
            user_id = "chapter8-user-click"
            product_id = "chapter8-product-click"
            event_type = "click"
            event_time = $eventTime
            channel = "web"
            device_type = "desktop"
            page_id = "detail"
        }
    ) | ForEach-Object { $_ | ConvertTo-Json -Compress }

    $events | docker exec -i ecom-kafka kafka-console-producer --bootstrap-server kafka:29092 --topic user_behavior_events | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to publish Chapter 8 validation events."
    }

    $response = $null
    $lastRequestError = $null
    $deadline = (Get-Date).AddSeconds(120)
    do {
        try {
            $body = @{ question = "How active are the current users?" } | ConvertTo-Json
            $candidate = Invoke-RestMethod -Method Post -Uri $analysisUrl -ContentType "application/json" -Body $body
            if (
                $candidate.analyzer -eq "rule_based" -and
                $candidate.evidence.realtime.pv -gt 0 -and
                $candidate.evidence.realtime.uv -gt 0 -and
                $candidate.evidence.historical.event_count -gt 0
            ) {
                $response = $candidate
                break
            }
        } catch {
            $lastRequestError = $_.Exception.Message
        }

        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)

    if ($null -eq $response) {
        $detail = if ($lastRequestError) { " Last request error: $lastRequestError" } else { "" }
        throw "Chapter 8 analysis endpoint did not return positive rule-based evidence in time.$detail"
    }

    Write-Host "[chapter8-verify] analyzer=$($response.analyzer)"
    Write-Host "[chapter8-verify] pv=$($response.evidence.realtime.pv) uv=$($response.evidence.realtime.uv)"
    Write-Host "[chapter8-verify] historical_event_count=$($response.evidence.historical.event_count)"
} finally {
    Restore-ProcessEnvironmentVariable -Name "AI_ANALYZER_MODE" -Value $previousAnalyzerMode
    Restore-ProcessEnvironmentVariable -Name "AI_API_KEY" -Value $previousApiKey
    Pop-Location
}
