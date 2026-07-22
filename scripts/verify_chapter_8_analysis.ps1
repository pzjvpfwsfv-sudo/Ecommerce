[CmdletBinding()]
param(
    [switch]$FunctionsOnly
)

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

function Test-Chapter8VerificationEvidence {
    param(
        [long]$BaselinePv,
        [long]$BaselineUv,
        [DateTimeOffset]$BaselineUpdatedAt,
        [long]$CurrentPv,
        [long]$CurrentUv,
        [DateTimeOffset]$CurrentUpdatedAt,
        [long]$AuditEventCount,
        [long]$AuditDistinctEventId,
        [long]$AuditDistinctUserId
    )

    return (
        (Test-ExpectedRealtimeDelta `
            -BaselinePv $BaselinePv `
            -BaselineUv $BaselineUv `
            -BaselineUpdatedAt $BaselineUpdatedAt `
            -CurrentPv $CurrentPv `
            -CurrentUv $CurrentUv `
            -CurrentUpdatedAt $CurrentUpdatedAt) -and
        $AuditEventCount -eq 2 -and
        $AuditDistinctEventId -eq 2 -and
        $AuditDistinctUserId -eq 2
    )
}

function Wait-ForFlinkJobRunning {
    param(
        [Parameter(Mandatory = $true)][string]$JobName,
        [string]$FlinkBaseUrl = "http://localhost:8081",
        [int]$TimeoutSeconds = 60
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastState = "unavailable"
    while ((Get-Date) -lt $deadline) {
        try {
            $overview = Invoke-RestMethod -Method Get -Uri "$FlinkBaseUrl/jobs/overview"
            $job = @($overview.jobs | Where-Object { $_.name -eq $JobName }) | Select-Object -First 1
            $lastState = if ($job) { [string]$job.state } else { "not-listed" }
            if ($lastState -eq "RUNNING") {
                return
            }
            if ($lastState -in @("FAILED", "CANCELED", "FINISHED")) {
                throw "Chapter 8 audit Flink job entered terminal state $lastState."
            }
        } catch {
            if ($_.Exception.Message -match "terminal state") {
                throw
            }
        }
        Start-Sleep -Seconds 2
    }
    throw "Timed out waiting for Chapter 8 audit Flink job $JobName to run; latest state: $lastState."
}

function Invoke-Chapter8TrinoStatement {
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [string]$TrinoBaseUrl = "http://localhost:8088"
    )

    $headers = @{
        "X-Trino-User" = "codex"
        "X-Trino-Source" = "chapter-8-run-audit"
        "X-Trino-Catalog" = "lakehouse"
        "X-Trino-Schema" = "analytics"
    }
    $response = Invoke-RestMethod -Method Post -Uri "$TrinoBaseUrl/v1/statement" `
        -Headers $headers -Body $Sql -ContentType "text/plain"
    $rows = New-Object System.Collections.Generic.List[object]
    while ($true) {
        if ($response.error) {
            throw "Chapter 8 Trino audit failed: $($response.error.message)"
        }
        foreach ($row in @($response.data)) {
            if ($null -ne $row) {
                [void]$rows.Add($row)
            }
        }
        if (-not $response.nextUri) {
            return $rows.ToArray()
        }
        $response = Invoke-RestMethod -Method Get -Uri $response.nextUri
    }
}

function ConvertTo-Chapter8SqlStringLiteral {
    param([Parameter(Mandatory = $true)][string]$Value)
    return "'$($Value.Replace("'", "''"))'"
}

function Invoke-Chapter8RunAudit {
    param(
        [Parameter(Mandatory = $true)][string[]]$eventIds,
        [string]$TrinoBaseUrl = "http://localhost:8088"
    )

    if ($eventIds.Count -ne 2 -or $eventIds[0] -eq $eventIds[1]) {
        throw "Chapter 8 audit requires exactly two distinct event IDs."
    }
    $firstEventId = ConvertTo-Chapter8SqlStringLiteral $eventIds[0]
    $secondEventId = ConvertTo-Chapter8SqlStringLiteral $eventIds[1]
    $sql = @"
SELECT COUNT(*) AS event_count,
       COUNT(DISTINCT event_id) AS distinct_event_id,
       COUNT(DISTINCT user_id) AS distinct_user_id
FROM "lakehouse"."analytics"."user_behavior_detail"
WHERE event_id IN ($firstEventId, $secondEventId)
"@
    $rows = @(Invoke-Chapter8TrinoStatement -Sql $sql -TrinoBaseUrl $TrinoBaseUrl)
    if ($rows.Count -ne 1 -or @($rows[0]).Count -ne 3) {
        throw "Chapter 8 Trino audit returned a malformed result."
    }
    $row = @($rows[0])
    return [pscustomobject]@{
        EventCount = [long]$row[0]
        DistinctEventId = [long]$row[1]
        DistinctUserId = [long]$row[2]
    }
}

if ($FunctionsOnly) {
    return
}

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

    $runId = [Guid]::NewGuid().ToString("N")
    $auditGroupId = "chapter-8-iceberg-audit-$runId"
    $auditJobName = "chapter-8-iceberg-audit-$runId"
    $auditSqlPath = Join-Path $repoRoot "tmp/chapter_8_audit_job.sql"
    $auditSourcePath = Join-Path $repoRoot "jobs/sql/12_source_user_behavior_chapter_8_audit.sql"
    $auditSqlParts = @(
        "SET 'pipeline.name' = '$auditJobName';",
        (Get-Content -Raw (Join-Path $repoRoot "jobs/sql/00_enable_iceberg_checkpointing.sql")),
        ((Get-Content -Raw $auditSourcePath).Replace("__AUDIT_GROUP_ID__", $auditGroupId)),
        (Get-Content -Raw (Join-Path $repoRoot "jobs/sql/06_create_iceberg_catalog.sql")),
        (Get-Content -Raw (Join-Path $repoRoot "jobs/sql/07_sink_user_behavior_to_iceberg.sql"))
    )
    New-Item -ItemType Directory -Force -Path (Split-Path $auditSqlPath) | Out-Null
    [System.IO.File]::WriteAllText(
        $auditSqlPath,
        ($auditSqlParts -join "`r`n`r`n"),
        (New-Object System.Text.UTF8Encoding($false))
    )
    Write-Host "[chapter8-verify] submitting isolated latest-offset Iceberg audit job..."
    $auditSubmitOutput = @(docker exec ecom-flink-sql-client /opt/flink/bin/sql-client.sh -f /workspace/tmp/chapter_8_audit_job.sql)
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to submit Chapter 8 Iceberg audit job."
    }
    $auditSubmitText = $auditSubmitOutput -join "`n"
    if ($auditSubmitText -match "\[ERROR\]") {
        throw "Chapter 8 Iceberg audit submission reported a statement error."
    }
    Wait-ForFlinkJobRunning -JobName $auditJobName

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
    $eventIds = @("chapter8-$runId-view", "chapter8-$runId-click")

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
            $audit = Invoke-Chapter8RunAudit -eventIds $eventIds
            $latestInvalidResponse = "$(Get-AnalysisResponseState -Response $candidate), audit_event_count=$($audit.EventCount), audit_distinct_event_id=$($audit.DistinctEventId), audit_distinct_user_id=$($audit.DistinctUserId)"
            if (
                $candidate.analyzer -eq "rule_based" -and
                $candidate.evidence.historical.event_count -gt 0 -and
                (Test-Chapter8VerificationEvidence `
                    -BaselinePv $baselinePv `
                    -BaselineUv $baselineUv `
                    -BaselineUpdatedAt $baselineUpdatedAt `
                    -CurrentPv ([long]$candidate.evidence.realtime.pv) `
                    -CurrentUv ([long]$candidate.evidence.realtime.uv) `
                    -CurrentUpdatedAt $candidateUpdatedAt `
                    -AuditEventCount $audit.EventCount `
                    -AuditDistinctEventId $audit.DistinctEventId `
                    -AuditDistinctUserId $audit.DistinctUserId)
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
        throw "Chapter 8 isolated verification did not observe exact Doris +2/+2 and runId Iceberg audit evidence in time.$detail"
    }

    Write-Host "[chapter8-verify] analyzer=$($response.analyzer)"
    Write-Host "[chapter8-verify] post_pv=$($response.evidence.realtime.pv) post_updated_at=$($response.evidence.realtime.updated_at) uv=$($response.evidence.realtime.uv)"
    Write-Host "[chapter8-verify] historical_event_count=$($response.evidence.historical.event_count)"
    Write-Host "[chapter8-verify] audit_event_count=$($audit.EventCount) audit_distinct_event_id=$($audit.DistinctEventId) audit_distinct_user_id=$($audit.DistinctUserId)"
} finally {
    Restore-ProcessEnvironmentVariable -Name "AI_ANALYZER_MODE" -Value $previousAnalyzerMode
    Restore-ProcessEnvironmentVariable -Name "AI_API_KEY" -Value $previousApiKey
    Pop-Location
}
