[CmdletBinding()]
param(
    [ValidateSet(10000, 100000, 2199938)][int]$Stage = 10000,
    [string]$RunId = '',
    [switch]$FunctionsOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Import-Module (Join-Path $PSScriptRoot 'lib/G2d.BehaviorMetrics.psm1') -Force

function Assert-G5SourceArtifact {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][long]$ExpectedBytes,
        [Parameter(Mandatory = $true)][string]$ExpectedSha256
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { throw 'G5 source file is missing.' }
    $bytes = (Get-Item -LiteralPath $Path).Length
    if ($bytes -ne $ExpectedBytes) { throw 'G5 source byte count does not match.' }
    $hash = (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($hash -cne $ExpectedSha256) { throw 'G5 source SHA-256 does not match.' }
    return [pscustomobject]@{ path = $Path; bytes = $bytes; sha256 = $hash }
}

function Assert-G5StageHistory {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][ValidateSet(10000, 100000, 2199938)][int]$Stage,
        $Pilot10k,
        $Pilot100k
    )

    if ($Stage -ge 100000 -and ($null -eq $Pilot10k -or
            [string]$Pilot10k.status -cne 'PASS' -or [int]$Pilot10k.stage -ne 10000)) {
        throw 'G5 10,000-event pilot evidence is missing or failed.'
    }
    if ($Stage -eq 2199938 -and ($null -eq $Pilot100k -or
            [string]$Pilot100k.status -cne 'PASS' -or [int]$Pilot100k.stage -ne 100000 -or
            [long]$Pilot100k.d_delta_bytes -le 0)) {
        throw 'G5 100,000-event capacity evidence is missing or failed.'
    }
}

function Assert-G5Capacity {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][ValidateSet(10000, 100000, 2199938)][int]$Stage,
        [Parameter(Mandatory = $true)][long]$CFreeBytes,
        [Parameter(Mandatory = $true)][long]$DFreeBytes,
        [long]$Pilot100kDeltaBytes = 0
    )

    $cMinimum = 5L * 1GB
    $dMinimum = 20L * 1GB
    if ($CFreeBytes -lt $cMinimum) { throw 'G5 C: free space is below 5 GiB.' }
    if ($DFreeBytes -lt $dMinimum) { throw 'G5 D: free space is below 20 GiB.' }
    if ($Stage -eq 2199938) {
        if ($Pilot100kDeltaBytes -le 0) { throw 'G5 full sample requires measured 100,000-event disk growth.' }
        $projected = [long][Math]::Ceiling(
            1.5 * $Pilot100kDeltaBytes * (2199938.0 / 100000.0) + 5.0 * 1GB
        )
        if ($DFreeBytes -lt $projected) {
            throw 'G5 D: free space is below the 1.5x full-sample projection plus 5 GiB.'
        }
    }
    return [pscustomobject]@{
        c_free_bytes = $CFreeBytes
        d_free_bytes = $DFreeBytes
        d_required_bytes = if ($Stage -eq 2199938) { [Math]::Max($dMinimum, $projected) } else { $dMinimum }
    }
}

function Assert-G5FreshRun {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][long]$RawTopicCount,
        [Parameter(Mandatory = $true)][long]$CleanTopicCount,
        [Parameter(Mandatory = $true)][long]$LateTopicCount,
        [Parameter(Mandatory = $true)][long]$TableRowCount,
        [bool]$TableExists = $false,
        [Parameter(Mandatory = $true)][bool]$CheckpointExists,
        [Parameter(Mandatory = $true)][long]$JobHistoryCount
    )

    $null = Resolve-G2dSourceTable -RunId $RunId
    if ($RawTopicCount -ne 0 -or $CleanTopicCount -ne 0 -or $LateTopicCount -ne 0 -or
            $TableExists -or $TableRowCount -ne 0 -or $CheckpointExists -or $JobHistoryCount -ne 0) {
        throw 'G5 run ID or its isolated target has already been used.'
    }
}

function Read-G5PilotReport {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { return $null }
    return Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json
}

function Get-G5DockerCli {
    $command = Get-Command docker -ErrorAction SilentlyContinue
    if ($null -ne $command) { return $command.Source }
    $fallback = 'D:\DockerProgram\Docker\resources\bin\docker.exe'
    if (Test-Path -LiteralPath $fallback -PathType Leaf) { return $fallback }
    throw 'G5 Docker CLI is unavailable.'
}

function Get-G5RuntimeTarget {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][string]$CheckpointPath
    )

    $table = Resolve-G2dSourceTable -RunId $RunId
    if ($table -ceq 'real_behavior_detail_v1') { throw 'G5 requires an isolated run ID.' }
    $docker = Get-G5DockerCli
    $running = @(& $docker ps --format '{{.Names}}')
    if ($LASTEXITCODE -ne 0) { throw 'G5 Docker daemon is unavailable.' }
    foreach ($name in @('ecom-kafka', 'ecom-trino', 'ecom-flink-jobmanager')) {
        if ($running -cnotcontains $name) { throw "G5 required container '$name' is not running." }
    }
    $topics = @(& $docker exec ecom-kafka kafka-topics --bootstrap-server kafka:29092 --list)
    if ($LASTEXITCODE -ne 0) { throw 'G5 Kafka topic listing failed.' }
    $trino = @(& $docker exec ecom-trino trino --server http://localhost:8080 `
        --catalog lakehouse --schema analytics --output-format TSV --execute (
            "SELECT count(*) FROM lakehouse.information_schema.tables " +
            "WHERE table_schema = 'analytics' AND table_name = '$table'"
        ))
    if ($LASTEXITCODE -ne 0 -or $trino.Count -ne 1 -or [string]$trino[0] -cnotmatch '^[01]$') {
        throw 'G5 Trino table identity probe failed.'
    }
    $tableExists = [string]$trino[0] -ceq '1'
    $rows = 0L
    if ($tableExists) {
        $count = @(& $docker exec ecom-trino trino --server http://localhost:8080 `
            --catalog lakehouse --schema analytics --output-format TSV --execute (
                "SELECT count(*) FROM lakehouse.analytics.$table"
            ))
        if ($LASTEXITCODE -ne 0 -or $count.Count -ne 1 -or [string]$count[0] -cnotmatch '^[0-9]+$') {
            throw 'G5 Trino target row-count probe failed.'
        }
        $rows = [long]$count[0]
    }
    $ports = @(& $docker port ecom-flink-jobmanager 8081/tcp)
    if ($LASTEXITCODE -ne 0 -or $ports.Count -lt 1 -or [string]$ports[0] -notmatch ':(\d+)$') {
        throw 'G5 Flink REST port could not be discovered.'
    }
    $flinkPort = [int]$Matches[1]
    $jobs = Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:$flinkPort/jobs/overview" -TimeoutSec 10
    $jobName = "graduation-g2c-$RunId"
    $history = @($jobs.jobs | Where-Object { $_.name -ceq $jobName })
    return [pscustomobject]@{
        raw_topic = "real_behavior_events_v1_$RunId"
        clean_topic = "real_behavior_clean_v1_$RunId"
        late_topic = "real_behavior_late_v1_$RunId"
        table = $table
        raw_topic_count = @($topics | Where-Object { $_ -ceq "real_behavior_events_v1_$RunId" }).Count
        clean_topic_count = @($topics | Where-Object { $_ -ceq "real_behavior_clean_v1_$RunId" }).Count
        late_topic_count = @($topics | Where-Object { $_ -ceq "real_behavior_late_v1_$RunId" }).Count
        table_exists = $tableExists
        table_row_count = $rows
        replay_checkpoint_exists = Test-Path -LiteralPath $CheckpointPath
        flink_job_history_count = $history.Count
        flink_rest_port = $flinkPort
        flink_checkpoint_note = 'Inspect the isolated MinIO checkpoint prefix before first submission.'
    }
}

function Invoke-G5Preflight {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][ValidateSet(10000, 100000, 2199938)][int]$Stage,
        [Parameter(Mandatory = $true)][string]$RunId
    )

    $root = Split-Path $PSScriptRoot -Parent
    $sourcePath = Join-Path $root 'data/rees46/oct-nov-users-2pct-verified.jsonl'
    $manifestPath = "$sourcePath.provenance.json"
    $checkpointPath = Join-Path $root "tmp/graduation/g5/replay-$RunId.json"
    $reasons = [Collections.Generic.List[string]]::new()
    $source = $null
    $capacity = $null
    $target = $null
    try {
        $null = Resolve-G2dSourceTable -RunId $RunId
        if ([string]::IsNullOrWhiteSpace($RunId)) { throw 'G5 run ID is required.' }
    } catch { $reasons.Add($_.Exception.Message) }
    try {
        if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
            throw 'G5 source provenance manifest is missing.'
        }
        $manifest = Get-Content -LiteralPath $manifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
        if ([long]$manifest.artifact_bytes -ne 1045479893 -or
                [long]$manifest.sample_rows -ne 2199938 -or
                [string]$manifest.sha256 -cne '18a3202d380c8f6c53ad767ec2043d0717df1d3604d2211639bb0fc4699a5437') {
            throw 'G5 source provenance identity does not match the reviewed artifact.'
        }
        $source = Assert-G5SourceArtifact -Path $sourcePath -ExpectedBytes 1045479893 `
            -ExpectedSha256 '18a3202d380c8f6c53ad767ec2043d0717df1d3604d2211639bb0fc4699a5437'
    } catch { $reasons.Add($_.Exception.Message) }
    $pilot10 = Read-G5PilotReport -Path (Join-Path $root 'tmp/graduation/g5/pilot-10000.json')
    $pilot100 = Read-G5PilotReport -Path (Join-Path $root 'tmp/graduation/g5/pilot-100000.json')
    try { Assert-G5StageHistory -Stage $Stage -Pilot10k $pilot10 -Pilot100k $pilot100 }
    catch { $reasons.Add($_.Exception.Message) }
    try {
        $cFree = [IO.DriveInfo]::new('C:\').AvailableFreeSpace
        $dFree = [IO.DriveInfo]::new('D:\').AvailableFreeSpace
        $pilotDelta = if ($null -eq $pilot100) { 0L } else { [long]$pilot100.d_delta_bytes }
        $capacity = Assert-G5Capacity -Stage $Stage -CFreeBytes $cFree `
            -DFreeBytes $dFree -Pilot100kDeltaBytes $pilotDelta
    } catch { $reasons.Add($_.Exception.Message) }
    if ($reasons.Count -eq 0) {
        try {
            $target = Get-G5RuntimeTarget -RunId $RunId -CheckpointPath $checkpointPath
            Assert-G5FreshRun -RunId $RunId -RawTopicCount $target.raw_topic_count `
                -CleanTopicCount $target.clean_topic_count -LateTopicCount $target.late_topic_count `
                -TableRowCount $target.table_row_count -TableExists $target.table_exists `
                -CheckpointExists $target.replay_checkpoint_exists `
                -JobHistoryCount $target.flink_job_history_count
        } catch { $reasons.Add($_.Exception.Message) }
    }
    return [pscustomobject][ordered]@{
        status = if ($reasons.Count -eq 0) { 'PASS' } else { 'BLOCKED' }
        stage = $Stage
        run_id = $RunId
        source = $source
        capacity = $capacity
        target = $target
        checkpoint_path = $checkpointPath
        stop_reasons = @($reasons.ToArray())
        read_only = $true
    }
}

if ($FunctionsOnly) { return }
Invoke-G5Preflight -Stage $Stage -RunId $RunId | ConvertTo-Json -Depth 10 -Compress
