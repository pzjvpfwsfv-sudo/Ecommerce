[CmdletBinding()]
param(
    [string]$RunId = "",
    [long]$ExpectedCleanCount = -1,
    [long]$ExpectedLateCount = -1,
    [string]$ExpectedEventRouteSha256 = "",
    [string]$JobId = "",
    [string]$TableName = "",
    [ValidateRange(10, 600)][int]$TimeoutSeconds = 120,
    [switch]$FunctionsOnly
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Assert-G2cTableName {
    param([Parameter(Mandatory = $true)][string]$Value)

    if ($Value -cne "real_behavior_detail_v1" -and
            $Value -cnotmatch "^real_behavior_detail_v1_[a-z0-9][a-z0-9_]{0,31}$") {
        throw "Table name is outside the G2-C namespace."
    }
    return $Value
}

function Render-G2cTrinoSql {
    param(
        [Parameter(Mandatory = $true)][string]$Template,
        [Parameter(Mandatory = $true)]$Deployment
    )

    $safeTable = Assert-G2cTableName -Value ([string]$Deployment.TableName)
    foreach ($topic in @(
        [string]$Deployment.CleanTopic,
        [string]$Deployment.LateTopic,
        [string]$Deployment.RawTopic
    )) {
        if ($topic -cnotmatch "^real_behavior_(?:clean|late|events)_v1_[a-z0-9][a-z0-9-]{0,31}$") {
            throw "Topic name is outside the G2-C namespace."
        }
    }
    $rendered = $Template.Replace("__TABLE_NAME__", $safeTable)
    $rendered = $rendered.Replace("__CLEAN_TOPIC__", [string]$Deployment.CleanTopic)
    $rendered = $rendered.Replace("__LATE_TOPIC__", [string]$Deployment.LateTopic)
    $rendered = $rendered.Replace("__RAW_TOPIC__", [string]$Deployment.RawTopic)
    if ($rendered -cmatch "__[A-Z0-9_]+__") { throw "Unresolved G2-C Trino SQL placeholder." }
    return $rendered
}

function Split-G2cSqlStatements {
    param([Parameter(Mandatory = $true)][string]$Sql)

    $statements = @($Sql -split ";" | ForEach-Object { $_.Trim() } | Where-Object { $_ })
    if ($statements.Count -ne 3) { throw "G2-C Trino verification requires exactly three statements." }
    return $statements
}

function ConvertFrom-G2cCsvResult {
    param([Parameter(Mandatory = $true)][AllowEmptyCollection()][string[]]$Lines)

    $content = @($Lines | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($content.Count -lt 2) { throw "G2-C Trino query returned no data row." }
    $rows = @($content -join "`n" | ConvertFrom-Csv)
    if ($rows.Count -ne 1) { throw "G2-C Trino query must return exactly one data row." }
    return $rows[0]
}

function Assert-G2cSummary {
    param(
        [Parameter(Mandatory = $true)]$Summary,
        [Parameter(Mandatory = $true)][long]$ExpectedCleanCount,
        [Parameter(Mandatory = $true)][long]$ExpectedLateCount,
        [Parameter(Mandatory = $true)][string]$ExpectedEventRouteSha256
    )

    if ($ExpectedCleanCount -lt 0 -or $ExpectedLateCount -lt 0) {
        throw "Expected G2-C route counts must be non-negative."
    }

    $total = [long]$Summary.total_count
    $clean = [long]$Summary.clean_count
    $late = [long]$Summary.late_count
    $distinct = [long]$Summary.distinct_event_count
    $invalidRoutes = [long]$Summary.invalid_route_count

    if ($total -ne ($clean + $late)) { throw "G2-C route reconciliation failed." }
    if ($total -ne $distinct) { throw "Duplicate G2-C event_id detected." }
    if ($invalidRoutes -ne 0) { throw "Invalid G2-C quality_route detected." }
    if ($clean -ne $ExpectedCleanCount -or $late -ne $ExpectedLateCount) {
        throw "G2-C route counts do not match the expected Kafka evidence."
    }
    if ($ExpectedEventRouteSha256 -cnotmatch "^[0-9a-f]{64}$" -or
            [string]$Summary.event_route_sha256 -cne $ExpectedEventRouteSha256) {
        throw "G2-C event/route digest does not match the expected Kafka evidence."
    }
}

function Assert-G2cQuality {
    param([Parameter(Mandatory = $true)]$Quality)

    foreach ($name in @(
        "invalid_original_count",
        "invalid_derived_count",
        "invalid_landing_count",
        "invalid_clean_diagnostic_count",
        "invalid_late_diagnostic_count"
    )) {
        $property = $Quality.PSObject.Properties[$name]
        if ($null -eq $property) { throw "G2-C quality result is missing $name." }
        if ([long]$property.Value -ne 0) { throw "G2-C quality assertion failed: $name." }
    }
}

function Assert-G2cSnapshot {
    param(
        [Parameter(Mandatory = $true)]$Snapshot,
        [Parameter(Mandatory = $true)][long]$NotBeforeEpochMs
    )

    if ([long]$Snapshot.snapshot_count -lt 1) {
        throw "G2-C Iceberg table has no committed snapshot."
    }
    $id = $Snapshot.PSObject.Properties["latest_snapshot_id"]
    $committedAt = $Snapshot.PSObject.Properties["latest_snapshot_committed_at"]
    if ($null -eq $id -or $null -eq $id.Value -or
            $null -eq $committedAt -or [string]::IsNullOrWhiteSpace([string]$committedAt.Value)) {
        throw "G2-C latest Iceberg snapshot identity is missing."
    }
    try {
        $committedText = ([string]$committedAt.Value) -replace " UTC$", " +00:00"
        $committed = [DateTimeOffset]::Parse(
            $committedText,
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::AssumeUniversal
        )
    } catch {
        throw "G2-C latest Iceberg snapshot time is invalid."
    }
    if ($committed.ToUnixTimeMilliseconds() -lt $NotBeforeEpochMs) {
        throw "G2-C latest Iceberg snapshot predates the requested Flink job."
    }
}

function Get-G2cCheckpointEvidence {
    param([Parameter(Mandatory = $true)]$Checkpoints)

    if ($null -eq $Checkpoints.counts -or [long]$Checkpoints.counts.completed -lt 1) {
        throw "G2-C Flink job has no completed checkpoint."
    }
    $latest = $Checkpoints.latest.completed
    if ($null -eq $latest -or [string]$latest.status -cne "COMPLETED") {
        throw "G2-C latest completed checkpoint evidence is invalid."
    }
    return [pscustomobject][ordered]@{
        CompletedCount = [long]$Checkpoints.counts.completed
        LatestId = [long]$latest.id
        LatestAckTimestamp = [long]$latest.latest_ack_timestamp
    }
}

function Get-G2cSingleRunningJob {
    param(
        [Parameter(Mandatory = $true)][string]$PipelineName,
        [Parameter(Mandatory = $true)][string]$JobId,
        [object[]]$Jobs = @()
    )

    $activeStates = @("CREATED", "RUNNING", "FAILING", "CANCELLING", "RESTARTING", "RECONCILING", "INITIALIZING")
    $active = @($Jobs | Where-Object { $_.name -ceq $PipelineName -and $_.state -in $activeStates })
    if ($active.Count -ne 1 -or [string]$active[0].state -cne "RUNNING") {
        throw "G2-C verification requires exactly one RUNNING job named $PipelineName."
    }
    if ([string]$active[0].jid -cne $JobId) {
        throw "G2-C running job does not match the requested job ID."
    }
    return $active[0]
}

function Wait-G2cTrinoReady {
    param(
        [int]$TimeoutSeconds = 120,
        [string]$BaseUrl = "http://localhost:8088"
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $info = Invoke-RestMethod -Method Get -Uri "$BaseUrl/v1/info" -TimeoutSec 10
            if (-not [string]::IsNullOrWhiteSpace([string]$info.nodeVersion.version)) { return }
        } catch {
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "Timed out waiting for Trino readiness."
}

function Invoke-G2cTrinoStatement {
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [Parameter(Mandatory = $true)][string]$ComposeFile,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $lines = @(& docker compose --env-file $EnvFile -f $ComposeFile exec -T trino trino `
        --server http://localhost:8080 --catalog lakehouse --schema analytics `
        --output-format CSV_HEADER_UNQUOTED --execute $Sql)
    if ($LASTEXITCODE -ne 0) { throw "G2-C Trino statement failed." }
    return ConvertFrom-G2cCsvResult -Lines $lines
}

function Wait-G2cExpectedSummary {
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [Parameter(Mandatory = $true)][string]$ComposeFile,
        [Parameter(Mandatory = $true)][string]$EnvFile,
        [Parameter(Mandatory = $true)][long]$ExpectedCleanCount,
        [Parameter(Mandatory = $true)][long]$ExpectedLateCount,
        [Parameter(Mandatory = $true)][string]$ExpectedEventRouteSha256,
        [int]$TimeoutSeconds = 120
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastError = $null
    do {
        try {
            $summary = Invoke-G2cTrinoStatement -Sql $Sql -ComposeFile $ComposeFile -EnvFile $EnvFile
            $clean = [long]$summary.clean_count
            $late = [long]$summary.late_count
            $total = [long]$summary.total_count
            if ($clean -gt $ExpectedCleanCount -or $late -gt $ExpectedLateCount -or
                    $total -gt ($ExpectedCleanCount + $ExpectedLateCount)) {
                throw "G2-C Iceberg counts exceeded the expected Kafka evidence."
            }
            if ($clean -eq $ExpectedCleanCount -and $late -eq $ExpectedLateCount) {
                Assert-G2cSummary -Summary $summary -ExpectedCleanCount $ExpectedCleanCount `
                    -ExpectedLateCount $ExpectedLateCount `
                    -ExpectedEventRouteSha256 $ExpectedEventRouteSha256
                return $summary
            }
        } catch {
            if ($_.Exception.Message -match "exceeded the expected|route reconciliation|Duplicate|Invalid G2-C") {
                throw
            }
            $lastError = $_.Exception.Message
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)

    throw "Timed out waiting for expected G2-C Iceberg counts. Last error: $lastError"
}

function Wait-G2cCheckpointEvidence {
    param(
        [Parameter(Mandatory = $true)][string]$JobId,
        [int]$TimeoutSeconds = 120,
        [string]$FlinkRestUrl = "http://localhost:8081"
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $checkpoints = Invoke-RestMethod -Method Get -Uri "$FlinkRestUrl/jobs/$JobId/checkpoints" -TimeoutSec 10
            return Get-G2cCheckpointEvidence -Checkpoints $checkpoints
        } catch {
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "Timed out waiting for a completed G2-C checkpoint."
}

function Write-G2cVerificationReport {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)]$Report
    )

    $directory = Join-Path $RepositoryRoot "tmp/graduation/g2c/$RunId"
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
    $path = Join-Path $directory "verification.json"
    $json = $Report | ConvertTo-Json -Depth 8
    [System.IO.File]::WriteAllText($path, $json + [Environment]::NewLine, [System.Text.UTF8Encoding]::new($false))
    return $path
}

if ($FunctionsOnly) { return }

$requestedRunId = $RunId
$requestedTableName = $TableName
$requestedCleanCount = $ExpectedCleanCount
$requestedLateCount = $ExpectedLateCount
$requestedEventRouteSha256 = $ExpectedEventRouteSha256
$requestedJobId = $JobId
$requestedTimeout = $TimeoutSeconds
$repositoryRoot = Split-Path -Parent $PSScriptRoot

. (Join-Path $PSScriptRoot "run_g2c_real_event_lakehouse.ps1") -FunctionsOnly
$deployment = Get-G2cDeployment -RunId $requestedRunId -TableName $requestedTableName
if ($requestedCleanCount -lt 0 -or $requestedLateCount -lt 0) {
    throw "Expected G2-C route counts must be supplied."
}
if ($requestedEventRouteSha256 -cnotmatch "^[0-9a-f]{64}$") {
    throw "Expected G2-C event/route digest must be a lowercase SHA-256 value."
}
if ($requestedJobId -cnotmatch "^[0-9a-f]{32}$") {
    throw "G2-C job ID must be a lowercase 32-character Flink ID."
}

$composeFile = Join-Path $repositoryRoot "infra/docker-compose.yml"
$envFile = Join-Path $repositoryRoot "infra/.env.example"
Enable-G2cDockerCli | Out-Null
Invoke-G2cChecked -Command { docker version --format "{{.Server.Version}}" } `
    -FailureMessage "Docker daemon is unavailable for G2-C verification." | Out-Null
Invoke-G2cChecked -Command {
    docker compose --env-file $envFile -f $composeFile --profile lakehouse up -d hive-metastore trino
} -FailureMessage "Failed to start Hive Metastore and Trino for G2-C verification." | Out-Null
Wait-G2cTrinoReady -TimeoutSeconds $requestedTimeout

$job = Get-G2cSingleRunningJob -PipelineName $deployment.PipelineName -JobId $requestedJobId `
    -Jobs @(Get-G2cFlinkJobs)
$checkpoint = Wait-G2cCheckpointEvidence -JobId $requestedJobId -TimeoutSeconds $requestedTimeout

$templatePath = Join-Path $repositoryRoot "jobs/sql/17_trino_verify_real_behavior.sql.template"
$template = Get-Content -LiteralPath $templatePath -Raw -Encoding UTF8
$verificationSql = Render-G2cTrinoSql -Template $template -Deployment $deployment
$statements = @(Split-G2cSqlStatements -Sql $verificationSql)
$summary = Wait-G2cExpectedSummary -Sql $statements[0] -ComposeFile $composeFile -EnvFile $envFile `
    -ExpectedCleanCount $requestedCleanCount -ExpectedLateCount $requestedLateCount `
    -ExpectedEventRouteSha256 $requestedEventRouteSha256 `
    -TimeoutSeconds $requestedTimeout
$quality = Invoke-G2cTrinoStatement -Sql $statements[1] -ComposeFile $composeFile -EnvFile $envFile
Assert-G2cQuality -Quality $quality
$snapshot = Invoke-G2cTrinoStatement -Sql $statements[2] -ComposeFile $composeFile -EnvFile $envFile
Assert-G2cSnapshot -Snapshot $snapshot -NotBeforeEpochMs ([long]$job.'start-time')

$report = [ordered]@{
    status = "PASS"
    verified_at = [DateTimeOffset]::UtcNow.ToString("o")
    run_id = $deployment.RunId
    pipeline_name = $deployment.PipelineName
    job_id = $requestedJobId
    job_started_at_epoch_ms = [long]$job.'start-time'
    table_name = $deployment.TableName
    expected = [ordered]@{
        clean = $requestedCleanCount
        late = $requestedLateCount
        event_route_sha256 = $requestedEventRouteSha256
    }
    summary = $summary
    quality = $quality
    checkpoint = $checkpoint
    snapshot = $snapshot
}
$reportPath = Write-G2cVerificationReport -RepositoryRoot $repositoryRoot -RunId $deployment.RunId -Report $report
$report.report_path = $reportPath
$report | ConvertTo-Json -Depth 8 -Compress
