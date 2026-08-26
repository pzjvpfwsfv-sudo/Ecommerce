param(
    [switch]$ConfirmReset,
    [switch]$FunctionsOnly
)

$chapter105BeforeModules = @(Get-Module)
$chapter105CommonModule = Import-Module ([System.IO.Path]::Combine(
        $PSScriptRoot, 'lib', 'Chapter105.Common.psm1')) -PassThru
$chapter105CommonFunctions = @(
    'Get-Chapter105PrimaryRepositoryRoot',
    'Get-Chapter105StableMinioDataPath',
    'Invoke-Chapter105Native',
    'Write-Chapter105BootstrapReport'
)
$chapter105Definitions = @{}
foreach ($chapter105Name in $chapter105CommonFunctions) {
    if (-not $chapter105CommonModule.ExportedFunctions.ContainsKey($chapter105Name)) {
        throw 'Chapter 10.5 Common module reset interface is incomplete.'
    }
    $chapter105Definitions[$chapter105Name] =
        $chapter105CommonModule.ExportedFunctions[$chapter105Name].ScriptBlock
}
if ($chapter105BeforeModules -notcontains $chapter105CommonModule) {
    Remove-Module $chapter105CommonModule -Force
}
foreach ($chapter105Name in $chapter105CommonFunctions) {
    Set-Item -LiteralPath "Function:$chapter105Name" -Value $chapter105Definitions[$chapter105Name]
}
foreach ($chapter105LoadedModule in @(Get-Module)) {
    if ($chapter105BeforeModules -notcontains $chapter105LoadedModule) {
        Remove-Module $chapter105LoadedModule -Force
    }
}

$script:Chapter105DiagnosticCommand =
    'docker compose --env-file infra/.env -f infra/docker-compose.yml --profile flink --profile serving --profile lakehouse ps --all'
$script:Chapter105LastResetReport = $null

function Get-Chapter105ControlledComposePrefix {
    param([Parameter(Mandatory = $true)][string]$RepositoryRoot)

    $root = [System.IO.Path]::GetFullPath($RepositoryRoot).TrimEnd('\', '/')
    return @(
        'compose', '--env-file', (Join-Path $root 'infra\.env'),
        '-f', (Join-Path $root 'infra\docker-compose.yml'),
        '--profile', 'flink', '--profile', 'serving', '--profile', 'lakehouse'
    )
}

function Get-Chapter105RealtimeLogicalVolumes {
    return @(
        'kafka-controller-data',
        'kafka-broker-data',
        'doris-fe-meta',
        'doris-be-storage',
        'metastore-postgres-data'
    )
}

function Get-Chapter105RealtimeResetPlan {
    param([Parameter(Mandatory = $true)][string]$ProjectName)

    if ($ProjectName -cnotmatch '^[a-z0-9][a-z0-9_-]*$') {
        throw 'Compose project identity is unsafe.'
    }
    $volumes = @(Get-Chapter105RealtimeLogicalVolumes | ForEach-Object {
            "${ProjectName}_$_"
        })
    return [pscustomobject][ordered]@{
        project_name = $ProjectName
        volumes = $volumes
        object_prefixes = @(
            'flink-state/checkpoints/chapter-9',
            'flink-state/savepoints/chapter-9'
        )
        protected = [pscustomobject][ordered]@{
            warehouse_bucket = 'warehouse'
            minio_host_path = 'infra/compose/minio/data'
        }
    }
}

function Get-Chapter105ComposeProjectName {
    param([Parameter(Mandatory = $true)][string[]]$ComposePrefix)

    $output = @(Invoke-Chapter105Native -FilePath 'docker' `
            -Arguments ($ComposePrefix + @('config', '--format', 'json')) `
            -FailureMessage 'Compose project inspection failed.')
    try {
        $config = ($output -join "`n") | ConvertFrom-Json -ErrorAction Stop
        if ($config -isnot [System.Management.Automation.PSCustomObject] -or
            $config.name -isnot [string] -or
            [string]$config.name -cnotmatch '^[a-z0-9][a-z0-9_-]*$') {
            throw 'unsafe'
        }
        return [string]$config.name
    } catch {
        throw 'Compose project identity is unsafe.'
    }
}

function Assert-Chapter105RealtimeVolumeLabels {
    param(
        [Parameter(Mandatory = $true)][object]$Plan,
        [Parameter(Mandatory = $true)][string]$ProjectName
    )

    $logicalVolumes = @(Get-Chapter105RealtimeLogicalVolumes)
    for ($index = 0; $index -lt $logicalVolumes.Count; $index++) {
        $volume = [string]$Plan.volumes[$index]
        $output = @(Invoke-Chapter105Native -FilePath 'docker' -Arguments @(
                'volume', 'inspect', $volume, '--format', '{{json .Labels}}'
            ) -FailureMessage 'Realtime volume label inspection failed.')
        try {
            $labels = ($output -join "`n") | ConvertFrom-Json -ErrorAction Stop
            if ($labels -isnot [System.Management.Automation.PSCustomObject] -or
                [string]$labels.'com.docker.compose.project' -cne $ProjectName -or
                [string]$labels.'com.docker.compose.volume' -cne [string]$logicalVolumes[$index]) {
                throw 'unsafe'
            }
        } catch {
            throw 'Realtime volume ownership is unsafe.'
        }
    }
}

function Get-Chapter105LakeEvidence {
    param([Parameter(Mandatory = $true)][string[]]$ComposePrefix)

    $findOutput = @(Invoke-Chapter105Native -FilePath 'docker' -Arguments ($ComposePrefix + @(
                'exec', '-T', 'minio', 'mc', 'find', 'local/warehouse', '--json'
            )) -FailureMessage 'Warehouse evidence collection failed.')
    $count = 0
    [long]$size = 0
    $seenKeys = @{}
    try {
        foreach ($item in $findOutput) {
            foreach ($line in @([string]$item -split "`r?`n")) {
                if ([string]::IsNullOrWhiteSpace($line)) { continue }
                $record = $line | ConvertFrom-Json -ErrorAction Stop
                if ($record -isnot [System.Management.Automation.PSCustomObject] -or
                    $record.key -isnot [string] -or
                    [string]::IsNullOrWhiteSpace([string]$record.key) -or
                    $null -eq $record.size -or [long]$record.size -lt 0 -or
                    $seenKeys.ContainsKey([string]$record.key)) {
                    throw 'unsafe'
                }
                $seenKeys[[string]$record.key] = $true
                $count++
                $size += [long]$record.size
            }
        }
    } catch {
        throw 'Warehouse evidence is unsafe.'
    }

    $snapshotSql =
        'SELECT CAST(snapshot_id AS VARCHAR) FROM lakehouse.analytics."user_behavior_detail$snapshots" ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1'
    $snapshotOutput = @(Invoke-Chapter105Native -FilePath 'docker' -Arguments ($ComposePrefix + @(
                'exec', '-T', 'trino', 'trino', '--output-format', 'TSV', '--execute', $snapshotSql
            )) -FailureMessage 'Iceberg snapshot evidence collection failed.')
    $snapshotLines = @($snapshotOutput | ForEach-Object {
            @([string]$_ -split "`r?`n")
        } | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) })
    if ($snapshotLines.Count -ne 1 -or [string]$snapshotLines[0] -cnotmatch '^[0-9]+$') {
        throw 'Iceberg snapshot evidence is unsafe.'
    }

    return [pscustomobject][ordered]@{
        warehouse = [pscustomobject][ordered]@{
            object_count = $count
            total_size_bytes = $size
        }
        table = [pscustomobject][ordered]@{
            catalog = 'lakehouse'
            schema = 'analytics'
            table = 'user_behavior_detail'
            snapshot_id = [string]$snapshotLines[0]
        }
    }
}

function Assert-Chapter105LakeEvidencePreserved {
    param(
        [Parameter(Mandatory = $true)][object]$Before,
        [Parameter(Mandatory = $true)][object]$After
    )

    if ([long]$Before.warehouse.object_count -ne [long]$After.warehouse.object_count -or
        [long]$Before.warehouse.total_size_bytes -ne [long]$After.warehouse.total_size_bytes -or
        [string]$Before.table.snapshot_id -cne [string]$After.table.snapshot_id) {
        throw 'Protected lake evidence changed.'
    }
}

function Write-Chapter105ResetPlan {
    param([Parameter(Mandatory = $true)][object]$Plan)

    Write-Host ("[plan] reset targets: " + (($Plan.volumes + $Plan.object_prefixes) -join ', '))
    Write-Host ("[protected] warehouse bucket; " + [string]$Plan.protected.minio_host_path)
}

function Invoke-Chapter105RealtimeReset {
    param(
        [switch]$ConfirmReset,
        [string]$RepositoryRoot
    )

    if (-not $ConfirmReset) { throw 'Realtime reset requires -ConfirmReset.' }
    if ([string]::IsNullOrWhiteSpace($RepositoryRoot)) {
        $RepositoryRoot = Get-Chapter105PrimaryRepositoryRoot -StartPath $PSScriptRoot
    }

    $report = [pscustomobject][ordered]@{
        kind = 'chapter10_5_realtime_reset'
        status = 'running'
        started_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
        completed_at_utc = $null
        project_name = $null
        plan = $null
        evidence = [pscustomobject][ordered]@{ before = $null; after = $null }
        diagnostic_command = $script:Chapter105DiagnosticCommand
    }
    $script:Chapter105LastResetReport = $report
    try {
        $composePrefix = Get-Chapter105ControlledComposePrefix -RepositoryRoot $RepositoryRoot
        $projectName = Get-Chapter105ComposeProjectName -ComposePrefix $composePrefix
        $plan = Get-Chapter105RealtimeResetPlan -ProjectName $projectName
        $report.project_name = $projectName
        $report.plan = $plan
        Write-Chapter105ResetPlan -Plan $plan

        Assert-Chapter105RealtimeVolumeLabels -Plan $plan -ProjectName $projectName
        $report.evidence.before = Get-Chapter105LakeEvidence -ComposePrefix $composePrefix

        Invoke-Chapter105Native -FilePath 'docker' -Arguments ($composePrefix + @(
                'stop', 'api', 'trino', 'hive-metastore', 'metastore-postgres',
                'flink-sql-client', 'flink-taskmanager', 'flink-jobmanager',
                'doris-be', 'doris-fe', 'kafka-broker', 'kafka-controller'
            )) -FailureMessage 'Realtime service stop failed.' | Out-Null

        foreach ($prefix in @($plan.object_prefixes)) {
            $target = "local/$prefix/"
            Invoke-Chapter105Native -FilePath 'docker' -Arguments ($composePrefix + @(
                    'exec', '-T', 'minio', 'mc', 'rm', '--recursive', '--force', $target
                )) -FailureMessage 'Flink state prefix reset failed.' | Out-Null
        }
        foreach ($volume in @($plan.volumes)) {
            Invoke-Chapter105Native -FilePath 'docker' -Arguments @('volume', 'rm', [string]$volume) `
                -FailureMessage 'Realtime volume reset failed.' | Out-Null
        }

        Invoke-Chapter105Native -FilePath 'docker' -Arguments ($composePrefix + @(
                'up', '-d', 'kafka-controller', 'kafka-broker', 'doris-fe', 'doris-be',
                'minio', 'minio-init', 'metastore-postgres', 'hive-metastore', 'trino',
                'flink-jobmanager', 'flink-taskmanager', 'flink-sql-client', 'api'
            )) -FailureMessage 'Realtime service rebuild failed.' | Out-Null
        Invoke-Chapter105Native -FilePath 'powershell' -Arguments @(
            '-NoProfile', '-File', (Join-Path $RepositoryRoot 'scripts\restore_chapter_10_5_catalog.ps1'),
            '-EnvFile', (Join-Path $RepositoryRoot 'infra\.env')
        ) -FailureMessage 'Fixed catalog recovery failed.' | Out-Null

        $report.evidence.after = Get-Chapter105LakeEvidence -ComposePrefix $composePrefix
        Assert-Chapter105LakeEvidencePreserved -Before $report.evidence.before -After $report.evidence.after
        $report.status = 'passed'
        return $report
    } catch {
        $report.status = 'failed'
        Write-Host "[diagnostic] $($script:Chapter105DiagnosticCommand)"
        throw 'Chapter 10.5 realtime reset failed. State was preserved; run the fixed diagnostic command.'
    } finally {
        $report.completed_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
        $script:Chapter105LastResetReport = $report
    }
}

if ($FunctionsOnly) { return }

$chapter105Root = Get-Chapter105PrimaryRepositoryRoot -StartPath $PSScriptRoot
$chapter105ReportPath = Join-Path $chapter105Root 'tmp/chapter-10-5/realtime-reset-report.json'
$chapter105PreviousMinioDataDir = [Environment]::GetEnvironmentVariable('MINIO_DATA_DIR', 'Process')
$chapter105Failure = $null
try {
    [Environment]::SetEnvironmentVariable(
        'MINIO_DATA_DIR',
        (Get-Chapter105StableMinioDataPath -RepositoryRoot $chapter105Root),
        'Process'
    )
    Invoke-Chapter105RealtimeReset -ConfirmReset:$ConfirmReset -RepositoryRoot $chapter105Root | Out-Null
} catch {
    $chapter105Failure = $_.Exception.Message
} finally {
    [Environment]::SetEnvironmentVariable('MINIO_DATA_DIR', $chapter105PreviousMinioDataDir, 'Process')
    if ($null -eq $script:Chapter105LastResetReport) {
        $script:Chapter105LastResetReport = [pscustomobject][ordered]@{
            kind = 'chapter10_5_realtime_reset'
            status = 'failed'
            diagnostic_command = $script:Chapter105DiagnosticCommand
        }
    }
    try {
        Write-Chapter105BootstrapReport -Report $script:Chapter105LastResetReport -Path $chapter105ReportPath
    } catch {
        if ($null -eq $chapter105Failure) {
            $chapter105Failure = 'Chapter 10.5 realtime reset report could not be written safely.'
        }
    }
}
if ($null -ne $chapter105Failure) { throw $chapter105Failure }
