[CmdletBinding()]
param(
    [switch]$FunctionsOnly
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$chapter6Verify = Join-Path $repoRoot "scripts/verify_chapter_6_trino_queries.ps1"
$chapter4Run = Join-Path $repoRoot "scripts/run_chapter_4_pipeline.ps1"
$analysisUrl = "http://localhost:8000/analysis/realtime"
$kafkaConnectorJar = Join-Path $repoRoot "infra/compose/flink/lib/flink-sql-connector-kafka-3.3.0-1.19.jar"
$kafkaConnectorUrl = "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar"
$kafkaConnectorSha256 = "F46F69333445C598EBA9E5068B0A58DD2B4BA797738FD0FD3EE4E862FE281691"
$dorisConnectorJar = Join-Path $repoRoot "infra/compose/flink/lib/flink-doris-connector-1.19-25.1.0.jar"
$dorisConnectorUrl = "https://repo1.maven.org/maven2/org/apache/doris/flink-doris-connector-1.19/25.1.0/flink-doris-connector-1.19-25.1.0.jar"
$dorisConnectorSha256 = "CE1C35B6A16B24F67E61EE95B7DAB9802B1FB654B9DA4FE171C174B2F8B1CA36"
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
        [string]$Name,
        [Parameter(Mandatory = $true)]
        [string]$ExpectedSha256
    )

    $partialPath = "$FilePath.partial"
    if (Test-Path -LiteralPath $FilePath -PathType Container) {
        # Docker creates an empty directory when a missing bind-mounted JAR is referenced.
        Remove-Item -LiteralPath $FilePath -Force
    }

    if (Test-Path -LiteralPath $FilePath -PathType Leaf) {
        $currentHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $FilePath).Hash
        if ($currentHash -eq $ExpectedSha256) {
            return
        }
    }

    New-Item -ItemType Directory -Force -Path (Split-Path $FilePath) | Out-Null
    if (Test-Path -LiteralPath $partialPath) {
        Remove-Item -LiteralPath $partialPath -Force
    }

    try {
        Write-Host "[chapter8-verify] downloading shared Flink $Name connector..."
        Invoke-WebRequest -Uri $Url -OutFile $partialPath
        $downloadedHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $partialPath).Hash
        if ($downloadedHash -ne $ExpectedSha256) {
            throw "Downloaded $Name connector checksum mismatch."
        }

        Move-Item -LiteralPath $partialPath -Destination $FilePath -Force
    } finally {
        if (Test-Path -LiteralPath $partialPath) {
            Remove-Item -LiteralPath $partialPath -Force
        }
    }
}

function Get-AnalysisResponseState {
    param($Response)

    return "analyzer=$($Response.analyzer), pv=$($Response.evidence.realtime.pv), uv=$($Response.evidence.realtime.uv), updated_at=$($Response.evidence.realtime.updated_at), historical=$($Response.evidence.historical.event_count)"
}

function Get-AnalysisTimeoutDetail {
    param(
        [AllowNull()][string]$LastRequestError,
        [AllowNull()][string]$LatestInvalidResponse
    )

    if ($LastRequestError) {
        return " Last request error: $LastRequestError"
    }
    if ($LatestInvalidResponse) {
        return " Latest invalid response: $LatestInvalidResponse"
    }
    return ""
}

function Test-SameRealtimeSnapshot {
    param(
        [long]$PreviousPv,
        [long]$PreviousUv,
        [DateTimeOffset]$PreviousUpdatedAt,
        [long]$CurrentPv,
        [long]$CurrentUv,
        [DateTimeOffset]$CurrentUpdatedAt
    )

    return (
        $CurrentPv -eq $PreviousPv -and
        $CurrentUv -eq $PreviousUv -and
        $CurrentUpdatedAt -eq $PreviousUpdatedAt
    )
}

function Test-ExpectedRealtimeDelta {
    param(
        [long]$BaselinePv,
        [long]$BaselineUv,
        [DateTimeOffset]$BaselineUpdatedAt,
        [long]$CurrentPv,
        [long]$CurrentUv,
        [DateTimeOffset]$CurrentUpdatedAt
    )

    return (
        $CurrentPv -eq ($BaselinePv + 2) -and
        $CurrentUv -eq ($BaselineUv + 2) -and
        $CurrentUpdatedAt -gt $BaselineUpdatedAt
    )
}

if ($FunctionsOnly) {
    return
}

Push-Location $repoRoot
try {
    # Process variables override Compose interpolation and prevent accidental model calls.
    $env:AI_ANALYZER_MODE = "rule_based"
    $env:AI_API_KEY = ""

    Ensure-ConnectorFile -FilePath $kafkaConnectorJar -Url $kafkaConnectorUrl -Name "Kafka" -ExpectedSha256 $kafkaConnectorSha256
    Ensure-ConnectorFile -FilePath $dorisConnectorJar -Url $dorisConnectorUrl -Name "Doris" -ExpectedSha256 $dorisConnectorSha256

    Write-Host "[chapter8-verify] preparing Iceberg history and Trino..."
    & $chapter6Verify

    Write-Host "[chapter8-verify] preparing Doris realtime metrics and API..."
    & $chapter4Run

    $body = @{ question = "How active are the current users?" } | ConvertTo-Json
    $baselineResponse = $null
    $lastRequestError = $null
    $latestInvalidResponse = $null
    $previousBaselinePv = $null
    $previousBaselineUv = $null
    $previousBaselineUpdatedAt = $null
    $stableSampleCount = 0
    $baselineDeadline = (Get-Date).AddSeconds(120)
    do {
        try {
            $candidate = Invoke-RestMethod -Method Post -Uri $analysisUrl -ContentType "application/json" -Body $body
            $lastRequestError = $null
            $latestInvalidResponse = Get-AnalysisResponseState -Response $candidate
            if (
                $candidate.analyzer -eq "rule_based" -and
                $null -ne $candidate.evidence.realtime.pv -and
                $candidate.evidence.realtime.updated_at -and
                $candidate.evidence.realtime.uv -gt 0 -and
                $candidate.evidence.historical.event_count -gt 0
            ) {
                $candidatePv = [long]$candidate.evidence.realtime.pv
                $candidateUv = [long]$candidate.evidence.realtime.uv
                $candidateUpdatedAt = [DateTimeOffset]::Parse(
                    [string]$candidate.evidence.realtime.updated_at
                )
                if (
                    $null -ne $previousBaselinePv -and
                    (Test-SameRealtimeSnapshot `
                        -PreviousPv $previousBaselinePv `
                        -PreviousUv $previousBaselineUv `
                        -PreviousUpdatedAt $previousBaselineUpdatedAt `
                        -CurrentPv $candidatePv `
                        -CurrentUv $candidateUv `
                        -CurrentUpdatedAt $candidateUpdatedAt)
                ) {
                    $stableSampleCount += 1
                } else {
                    $stableSampleCount = 1
                }
                $previousBaselinePv = $candidatePv
                $previousBaselineUv = $candidateUv
                $previousBaselineUpdatedAt = $candidateUpdatedAt
                if ($stableSampleCount -ge 3) {
                    $baselineResponse = $candidate
                    break
                }
            }
        } catch {
            $lastRequestError = $_.Exception.Message
        }

        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $baselineDeadline)

    if ($null -eq $baselineResponse) {
        $detail = Get-AnalysisTimeoutDetail -LastRequestError $lastRequestError -LatestInvalidResponse $latestInvalidResponse
        throw "Chapter 8 analysis endpoint did not return a usable baseline in time.$detail"
    }

    $baselinePv = [long]$baselineResponse.evidence.realtime.pv
    $baselineUv = [long]$baselineResponse.evidence.realtime.uv
    $baselineUpdatedAt = [DateTimeOffset]::Parse([string]$baselineResponse.evidence.realtime.updated_at)
    Write-Host "[chapter8-verify] baseline_pv=$baselinePv baseline_uv=$baselineUv baseline_updated_at=$($baselineUpdatedAt.ToString('o'))"

    $runId = [Guid]::NewGuid().ToString("N")
    $eventTime = [DateTimeOffset]::UtcNow.ToString("o")
    $events = @(
        [ordered]@{
            event_id = "chapter8-$runId-view"
            user_id = "chapter8-$runId-user-view"
            product_id = "chapter8-product-view"
            event_type = "view"
            event_time = $eventTime
            channel = "app"
            device_type = "mobile"
            page_id = "home"
        },
        [ordered]@{
            event_id = "chapter8-$runId-click"
            user_id = "chapter8-$runId-user-click"
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
    $latestInvalidResponse = $null
    $deadline = (Get-Date).AddSeconds(120)
    do {
        try {
            $candidate = Invoke-RestMethod -Method Post -Uri $analysisUrl -ContentType "application/json" -Body $body
            $lastRequestError = $null
            $latestInvalidResponse = Get-AnalysisResponseState -Response $candidate
            $candidateUpdatedAt = if ($candidate.evidence.realtime.updated_at) {
                [DateTimeOffset]::Parse([string]$candidate.evidence.realtime.updated_at)
            } else {
                $null
            }
            if (
                $candidate.analyzer -eq "rule_based" -and
                $candidate.evidence.historical.event_count -gt 0 -and
                (Test-ExpectedRealtimeDelta `
                    -BaselinePv $baselinePv `
                    -BaselineUv $baselineUv `
                    -BaselineUpdatedAt $baselineUpdatedAt `
                    -CurrentPv ([long]$candidate.evidence.realtime.pv) `
                    -CurrentUv ([long]$candidate.evidence.realtime.uv) `
                    -CurrentUpdatedAt $candidateUpdatedAt)
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
        $detail = Get-AnalysisTimeoutDetail -LastRequestError $lastRequestError -LatestInvalidResponse $latestInvalidResponse
        throw "Chapter 8 isolated verification did not observe an exact PV/UV +2 delta in time.$detail"
    }

    Write-Host "[chapter8-verify] analyzer=$($response.analyzer)"
    Write-Host "[chapter8-verify] post_pv=$($response.evidence.realtime.pv) post_updated_at=$($response.evidence.realtime.updated_at) uv=$($response.evidence.realtime.uv)"
    Write-Host "[chapter8-verify] historical_event_count=$($response.evidence.historical.event_count)"
} finally {
    Restore-ProcessEnvironmentVariable -Name "AI_ANALYZER_MODE" -Value $previousAnalyzerMode
    Restore-ProcessEnvironmentVariable -Name "AI_API_KEY" -Value $previousApiKey
    Pop-Location
}
