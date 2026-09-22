[CmdletBinding()]
param(
    [string]$ManifestPath = '',
    [string]$Entity = '',
    [string]$JobId = '',
    [int]$TimeoutSeconds = 600,
    [switch]$FunctionsOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$requestedManifestPath = $ManifestPath
$requestedEntity = $Entity
$requestedJobId = $JobId
$requestedTimeoutSeconds = $TimeoutSeconds
$verifierFunctionsOnly = [bool]$FunctionsOnly
. (Join-Path $PSScriptRoot 'run_g2e_olist_source.ps1') -FunctionsOnly
$FunctionsOnly = $verifierFunctionsOnly

function Assert-G2eFinishedJob {
    param(
        [Parameter(Mandatory = $true)][string]$PipelineName,
        [Parameter(Mandatory = $true)][string]$JobId,
        [object[]]$Jobs = @()
    )

    if ($JobId -cnotmatch '^[0-9a-f]{32}$') { throw 'G2-E job ID must be a lowercase Flink job ID.' }
    $activeStates = @('CREATED', 'RUNNING', 'FAILING', 'CANCELLING', 'RESTARTING', 'RECONCILING', 'INITIALIZING')
    $active = @($Jobs | Where-Object { $_.name -ceq $PipelineName -and $_.state -in $activeStates })
    if ($active.Count -ne 0) { throw 'G2-E verification found an active duplicate pipeline.' }
    $matches = @($Jobs | Where-Object { $_.name -ceq $PipelineName -and $_.jid -ceq $JobId })
    if ($matches.Count -ne 1 -or [string]$matches[0].state -cne 'FINISHED') {
        throw 'G2-E supplied job ID is not the finished bounded pipeline job.'
    }
    return $matches[0]
}

function Get-G2eCheckpointEvidence {
    param($Checkpoints)

    if ($null -eq $Checkpoints) {
        return [pscustomobject][ordered]@{ status = 'not_observed'; completed_count = 0 }
    }
    $counts = $Checkpoints.PSObject.Properties['counts']
    if ($null -eq $counts -or $null -eq $counts.Value) {
        return [pscustomobject][ordered]@{ status = 'not_observed'; completed_count = 0 }
    }
    $completed = ConvertTo-G2eCount (Get-G2ePropertyValue $counts.Value 'completed') 'completed checkpoint count'
    if ($completed -lt 1) {
        return [pscustomobject][ordered]@{ status = 'not_observed'; completed_count = 0 }
    }
    $latest = Get-G2ePropertyValue (Get-G2ePropertyValue $Checkpoints 'latest') 'completed'
    if ($null -eq $latest -or [string](Get-G2ePropertyValue $latest 'status') -cne 'COMPLETED') {
        throw 'G2-E completed checkpoint evidence is contradictory.'
    }
    return [pscustomobject][ordered]@{
        status = 'completed'
        completed_count = $completed
        latest_id = [string](Get-G2ePropertyValue $latest 'id')
        latest_ack_timestamp = [string](Get-G2ePropertyValue $latest 'latest_ack_timestamp')
    }
}

function ConvertTo-G2eComparableJson {
    param([Parameter(Mandatory = $true)]$Value)

    $copy = $Value | ConvertTo-Json -Depth 12 | ConvertFrom-Json
    foreach ($name in @('verified_at', 'report_path', 'verification_mode', 'job_id', 'checkpoint')) {
        if ($null -ne $copy.PSObject.Properties[$name]) { $copy.PSObject.Properties.Remove($name) }
    }
    return ($copy | ConvertTo-Json -Depth 12 -Compress)
}

function Write-G2eSourceReport {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)]$Report
    )

    $fullPath = [IO.Path]::GetFullPath($Path)
    $directory = [IO.Path]::GetDirectoryName($fullPath)
    if ([string]::IsNullOrWhiteSpace($directory)) { throw 'G2-E report path has no directory.' }
    $null = New-Item -ItemType Directory -Force -Path $directory
    $newComparable = ConvertTo-G2eComparableJson $Report
    if ([IO.File]::Exists($fullPath)) {
        try { $existing = Get-Content -LiteralPath $fullPath -Raw -Encoding UTF8 | ConvertFrom-Json } catch {
            throw 'Existing G2-E report is malformed.'
        }
        if ((ConvertTo-G2eComparableJson $existing) -cne $newComparable) {
            throw 'Existing G2-E report contradicts current verification evidence.'
        }
        return $fullPath
    }

    $temporary = Join-Path $directory ('.' + [IO.Path]::GetFileName($fullPath) + '.' + [guid]::NewGuid().ToString('N') + '.part')
    $json = $Report | ConvertTo-Json -Depth 12
    try {
        $stream = [IO.FileStream]::new($temporary, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
        try {
            $bytes = [Text.UTF8Encoding]::new($false).GetBytes($json + "`n")
            $stream.Write($bytes, 0, $bytes.Length)
            $stream.Flush($true)
        } finally {
            $stream.Dispose()
        }
        try {
            [IO.File]::Move($temporary, $fullPath)
        } catch [IO.IOException] {
            if (-not [IO.File]::Exists($fullPath)) { throw }
            $existing = Get-Content -LiteralPath $fullPath -Raw -Encoding UTF8 | ConvertFrom-Json
            if ((ConvertTo-G2eComparableJson $existing) -cne $newComparable) { throw }
        }
    } finally {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
    }
    return $fullPath
}

if ($FunctionsOnly) { return }

if ([string]::IsNullOrWhiteSpace($requestedManifestPath) -or [string]::IsNullOrWhiteSpace($requestedEntity)) {
    throw 'ManifestPath and Entity are required for G2-E source verification.'
}
if ($requestedTimeoutSeconds -lt 1 -or $requestedTimeoutSeconds -gt 3600) {
    throw 'G2-E timeout must be between 1 and 3600 seconds.'
}

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repositoryRoot 'infra/docker-compose.yml'
$envFile = Join-Path $repositoryRoot 'infra/.env.example'
$dataRoot = if (-not [string]::IsNullOrWhiteSpace($env:OLIST_DATA_DIR)) {
    $env:OLIST_DATA_DIR
} else {
    Get-G2eEnvValue $envFile 'OLIST_DATA_DIR'
}
$deployment = Get-G2eRuntimeDeployment $requestedManifestPath $requestedEntity $dataRoot
$null = Enable-G2eDockerCli
$null = Invoke-G2eChecked { docker version --format '{{.Server.Version}}' } 'Docker daemon is unavailable for G2-E verification.'
$startup = Get-G2eServiceStartupPlan
$lakehouseServices = [string[]]$startup.Lakehouse
$flinkServices = [string[]]$startup.Flink
$null = Invoke-G2eChecked {
    docker compose --env-file $envFile -f $composeFile --profile lakehouse up -d @lakehouseServices
} 'Failed to start G2-E verification services.'
$null = Invoke-G2eChecked {
    docker compose --env-file $envFile -f $composeFile --profile flink up -d --no-deps @flinkServices
} 'Failed to start G2-E verification Flink services.'
Wait-G2eHttpReady 'http://localhost:8088/v1/info' $requestedTimeoutSeconds
Wait-G2eHttpReady 'http://localhost:8081/overview' $requestedTimeoutSeconds

$jobs = @(Get-G2eFlinkJobs)
Assert-G2eNoActivePipelineJob $deployment.PipelineName $jobs
$finishedJob = $null
$checkpoint = [pscustomobject][ordered]@{ status = 'not_requested'; completed_count = 0 }
if (-not [string]::IsNullOrWhiteSpace($requestedJobId)) {
    $finishedJob = Assert-G2eFinishedJob $deployment.PipelineName $requestedJobId $jobs
    $checkpoints = $null
    try {
        $checkpoints = Invoke-RestMethod -Method Get `
            -Uri "http://localhost:8081/jobs/$requestedJobId/checkpoints" -TimeoutSec 10
    } catch {
        $checkpoint = [pscustomobject][ordered]@{ status = 'not_observed'; completed_count = 0 }
    }
    if ($null -ne $checkpoints) { $checkpoint = Get-G2eCheckpointEvidence $checkpoints }
}

$templatePath = Join-Path $repositoryRoot 'jobs/sql/20_olist_source_verify.sql.template'
$observed = Get-G2eSourceState $deployment $composeFile $envFile $templatePath
$state = Assert-G2eSourceState $observed $deployment
if ($state.Kind -cne 'Verified') { throw 'G2-E verification requires an exact committed source table.' }

$report = [ordered]@{
    status = 'PASS'
    verified_at = [DateTimeOffset]::UtcNow.ToString('o')
    verification_mode = if ($null -eq $finishedJob) { 'already_ingested' } else { 'finished_bounded_job' }
    dataset_id = $deployment.DatasetId
    entity = $deployment.Entity
    source_bundle_sha256 = $deployment.SourceBundleSha256
    normalized_sha256 = $deployment.NormalizedSha256
    source_row_id_sequence_sha256 = $deployment.SourceRowIdSequenceSha256
    expected_row_count = $deployment.ExpectedRowCount
    target_table = $deployment.TargetTable
    pipeline_name = $deployment.PipelineName
    source_file = $deployment.SourceFile
    job_id = if ($null -eq $finishedJob) { $null } else { [string]$finishedJob.jid }
    checkpoint = $checkpoint
    iceberg = [ordered]@{
        row_count = $state.RowCount
        snapshot_id = $state.SnapshotId
        snapshot_committed_at = $state.SnapshotCommittedAt
    }
}
$reportPath = Join-Path $repositoryRoot "tmp/graduation/g2e/$($deployment.SourceBundleSha256)/sources/$($deployment.Entity).json"
$savedPath = Write-G2eSourceReport $reportPath $report
$savedReport = Get-Content -LiteralPath $savedPath -Raw -Encoding UTF8 | ConvertFrom-Json
$output = [ordered]@{}
foreach ($property in $savedReport.PSObject.Properties) { $output[$property.Name] = $property.Value }
$output.report_path = $savedPath
$output | ConvertTo-Json -Depth 12 -Compress
