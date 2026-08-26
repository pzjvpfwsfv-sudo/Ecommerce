param(
    [switch]$ConfirmReset,
    [switch]$FunctionsOnly
)

$chapter105ResetFunctionsOnly = [bool]$FunctionsOnly
if (-not $chapter105ResetFunctionsOnly -and -not $ConfirmReset) {
    throw 'Realtime reset requires -ConfirmReset.'
}

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
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [string]$ProjectName
    )

    $root = [System.IO.Path]::GetFullPath($RepositoryRoot).TrimEnd('\', '/')
    $prefix = @('compose')
    if ($PSBoundParameters.ContainsKey('ProjectName')) {
        if ($ProjectName -cnotmatch '^[a-z0-9][a-z0-9_-]*$') {
            throw 'Compose project identity is unsafe.'
        }
        $prefix += @('--project-name', $ProjectName)
    }
    return @($prefix + @(
        '--env-file', (Join-Path $root 'infra\.env'),
        '-f', (Join-Path $root 'infra\docker-compose.yml'),
        '--profile', 'flink', '--profile', 'serving', '--profile', 'lakehouse'
    ))
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

function New-Chapter105ControlledComposeContext {
    param([Parameter(Mandatory = $true)][string]$RepositoryRoot)

    $inspectionPrefix = Get-Chapter105ControlledComposePrefix -RepositoryRoot $RepositoryRoot
    $projectName = Get-Chapter105ComposeProjectName -ComposePrefix $inspectionPrefix
    return [pscustomobject][ordered]@{
        project_name = $projectName
        prefix = @(Get-Chapter105ControlledComposePrefix -RepositoryRoot $RepositoryRoot `
                -ProjectName $projectName)
    }
}

function Assert-Chapter105ControlledComposeContext {
    param(
        [Parameter(Mandatory = $true)][object]$Context,
        [Parameter(Mandatory = $true)][string]$RepositoryRoot
    )

    if ($Context -isnot [System.Management.Automation.PSCustomObject] -or
        $Context.project_name -isnot [string] -or
        [string]$Context.project_name -cnotmatch '^[a-z0-9][a-z0-9_-]*$') {
        throw 'Compose project identity is unsafe.'
    }
    $expected = @(Get-Chapter105ControlledComposePrefix -RepositoryRoot $RepositoryRoot `
            -ProjectName ([string]$Context.project_name))
    $actual = @($Context.prefix)
    if ($actual.Count -ne $expected.Count) { throw 'Compose project identity is unsafe.' }
    for ($index = 0; $index -lt $expected.Count; $index++) {
        if ([string]$actual[$index] -cne [string]$expected[$index]) {
            throw 'Compose project identity is unsafe.'
        }
    }
}

function ConvertFrom-Chapter105StrictVolumeLabels {
    param(
        [Parameter(Mandatory = $true)][string]$Json,
        [Parameter(Mandatory = $true)][string]$ProjectName,
        [Parameter(Mandatory = $true)][string]$LogicalVolume
    )

    try {
        $trimmed = $Json.Trim()
        if ($trimmed.Length -lt 2 -or $trimmed[0] -cne '{' -or
            $trimmed[$trimmed.Length - 1] -cne '}') {
            throw 'unsafe'
        }
        $body = $trimmed.Substring(1, $trimmed.Length - 2)
        $labels = [System.Collections.Generic.Dictionary[string, string]]::new(
            [System.StringComparer]::Ordinal)
        $index = 0
        $first = $true
        while ($true) {
            while ($index -lt $body.Length -and [char]::IsWhiteSpace($body[$index])) { $index++ }
            if ($index -ge $body.Length) { break }
            if (-not $first) {
                if ($body[$index] -cne ',') { throw 'unsafe' }
                $index++
            }
            $remaining = $body.Substring($index)
            $match = [regex]::Match(
                $remaining,
                '^\s*"(?<key>(?:\\.|[^"\\])*)"\s*:\s*(?<value>"(?:\\.|[^"\\])*")\s*'
            )
            if (-not $match.Success) { throw 'unsafe' }
            $key = ('"' + $match.Groups['key'].Value + '"') |
                ConvertFrom-Json -ErrorAction Stop
            $value = $match.Groups['value'].Value | ConvertFrom-Json -ErrorAction Stop
            if ($key -isnot [string] -or $value -isnot [string] -or
                $key -cne $key.ToLowerInvariant() -or $labels.ContainsKey($key)) {
                throw 'unsafe'
            }
            $labels.Add($key, $value)
            $index += $match.Length
            $first = $false
        }
        if ($labels.Count -eq 0 -or
            -not $labels.ContainsKey('com.docker.compose.project') -or
            -not $labels.ContainsKey('com.docker.compose.volume') -or
            $labels['com.docker.compose.project'] -cne $ProjectName -or
            $labels['com.docker.compose.volume'] -cne $LogicalVolume) {
            throw 'unsafe'
        }
        return $labels
    } catch {
        throw 'Realtime volume ownership is unsafe.'
    }
}

function Get-Chapter105RealtimeVolumeInspection {
    param(
        [Parameter(Mandatory = $true)][string]$VolumeName,
        [Parameter(Mandatory = $true)][string]$LogicalVolume,
        [Parameter(Mandatory = $true)][string]$ProjectName
    )

    $format = '{"name":{{json .Name}},"created_at":{{json .CreatedAt}},' +
        '"mountpoint":{{json .Mountpoint}},"driver":{{json .Driver}},' +
        '"scope":{{json .Scope}},"labels":{{json .Labels}}}'
    $output = @(Invoke-Chapter105Native -FilePath 'docker' -Arguments @(
            'volume', 'inspect', $VolumeName, '--format', $format
        ) -FailureMessage 'Realtime volume label inspection failed.')
    try {
        if ($output.Count -ne 1) { throw 'unsafe' }
        $raw = [string]$output[0]
        $stringToken = '"(?:\\.|[^"\\])*"'
        $pattern = '^\s*\{\s*"name"\s*:\s*(?<name>' + $stringToken + ')' +
            '\s*,\s*"created_at"\s*:\s*(?<created>' + $stringToken + ')' +
            '\s*,\s*"mountpoint"\s*:\s*(?<mount>' + $stringToken + ')' +
            '\s*,\s*"driver"\s*:\s*(?<driver>' + $stringToken + ')' +
            '\s*,\s*"scope"\s*:\s*(?<scope>' + $stringToken + ')' +
            '\s*,\s*"labels"\s*:\s*(?<labels>\{.*\})\s*\}\s*$'
        $match = [regex]::Match($raw, $pattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)
        if (-not $match.Success) { throw 'unsafe' }
        $name = $match.Groups['name'].Value | ConvertFrom-Json -ErrorAction Stop
        $createdAt = $match.Groups['created'].Value | ConvertFrom-Json -ErrorAction Stop
        $mountpoint = $match.Groups['mount'].Value | ConvertFrom-Json -ErrorAction Stop
        $driver = $match.Groups['driver'].Value | ConvertFrom-Json -ErrorAction Stop
        $scope = $match.Groups['scope'].Value | ConvertFrom-Json -ErrorAction Stop
        if ($name -isnot [string] -or $name -cne $VolumeName -or
            $createdAt -isnot [string] -or [string]::IsNullOrWhiteSpace($createdAt) -or
            $mountpoint -isnot [string] -or [string]::IsNullOrWhiteSpace($mountpoint) -or
            $driver -isnot [string] -or [string]::IsNullOrWhiteSpace($driver) -or
            $scope -isnot [string] -or [string]::IsNullOrWhiteSpace($scope)) {
            throw 'unsafe'
        }
        $labels = ConvertFrom-Chapter105StrictVolumeLabels -Json $match.Groups['labels'].Value `
            -ProjectName $ProjectName -LogicalVolume $LogicalVolume
        $labelFingerprint = @($labels.Keys | Sort-Object -CaseSensitive | ForEach-Object {
                "$_=$($labels[$_])"
            }) -join "`n"
        return [pscustomobject][ordered]@{
            volume_name = $name
            logical_volume = $LogicalVolume
            fingerprint = @($name, $createdAt, $mountpoint, $driver, $scope, $labelFingerprint) -join "`0"
        }
    } catch {
        throw 'Realtime volume ownership is unsafe.'
    }
}

function Assert-Chapter105RealtimeVolumeLabels {
    param(
        [Parameter(Mandatory = $true)][object]$Plan,
        [Parameter(Mandatory = $true)][string]$ProjectName
    )

    $logicalVolumes = @(Get-Chapter105RealtimeLogicalVolumes)
    $inspections = @()
    for ($index = 0; $index -lt $logicalVolumes.Count; $index++) {
        $inspections += Get-Chapter105RealtimeVolumeInspection `
            -VolumeName ([string]$Plan.volumes[$index]) `
            -LogicalVolume ([string]$logicalVolumes[$index]) -ProjectName $ProjectName
    }
    return @($inspections)
}

function Remove-Chapter105ValidatedRealtimeVolume {
    param(
        [Parameter(Mandatory = $true)][object]$ExpectedInspection,
        [Parameter(Mandatory = $true)][string]$ProjectName
    )

    $current = Get-Chapter105RealtimeVolumeInspection `
        -VolumeName ([string]$ExpectedInspection.volume_name) `
        -LogicalVolume ([string]$ExpectedInspection.logical_volume) -ProjectName $ProjectName
    if ([string]$current.fingerprint -cne [string]$ExpectedInspection.fingerprint) {
        throw 'Realtime volume ownership is unsafe.'
    }
    Invoke-Chapter105Native -FilePath 'docker' -Arguments @(
        'volume', 'rm', [string]$ExpectedInspection.volume_name
    ) -FailureMessage 'Realtime volume reset failed.' | Out-Null
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

function Get-Chapter105RealtimeVolumeOwnerServices {
    return @(
        'kafka-controller',
        'kafka-broker',
        'doris-fe',
        'doris-be',
        'metastore-postgres'
    )
}

function Invoke-Chapter105RealtimeReset {
    param(
        [switch]$ConfirmReset,
        [string]$RepositoryRoot,
        [AllowNull()][object]$ComposeContext,
        [AllowNull()][object]$BeforeEvidence
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
        if ($null -eq $ComposeContext) {
            $ComposeContext = New-Chapter105ControlledComposeContext -RepositoryRoot $RepositoryRoot
        }
        Assert-Chapter105ControlledComposeContext -Context $ComposeContext `
            -RepositoryRoot $RepositoryRoot
        $composePrefix = @($ComposeContext.prefix)
        $projectName = [string]$ComposeContext.project_name
        $plan = Get-Chapter105RealtimeResetPlan -ProjectName $projectName
        $report.project_name = $projectName
        $report.plan = $plan
        Write-Chapter105ResetPlan -Plan $plan

        $volumeInspections = @(Assert-Chapter105RealtimeVolumeLabels -Plan $plan `
                -ProjectName $projectName)
        $report.evidence.before = if ($null -ne $BeforeEvidence) {
            $BeforeEvidence
        } else {
            Get-Chapter105LakeEvidence -ComposePrefix $composePrefix
        }

        Invoke-Chapter105Native -FilePath 'docker' -Arguments ($composePrefix + @(
                'rm', '--stop', '--force'
            ) + @(Get-Chapter105RealtimeVolumeOwnerServices)) `
            -FailureMessage 'Realtime owner container removal failed.' | Out-Null

        foreach ($inspection in @($volumeInspections)) {
            Remove-Chapter105ValidatedRealtimeVolume -ExpectedInspection $inspection `
                -ProjectName $projectName
        }
        foreach ($prefix in @($plan.object_prefixes)) {
            Invoke-Chapter105Native -FilePath 'docker' -Arguments ($composePrefix + @(
                    'exec', '-T', 'minio', 'mc', 'rm', '--recursive', '--force',
                    "local/$prefix/"
                )) -FailureMessage 'Flink state prefix reset failed.' | Out-Null
        }

        Invoke-Chapter105Native -FilePath 'docker' -Arguments ($composePrefix + @(
                'up', '-d', 'kafka-controller', 'kafka-broker', 'doris-fe', 'doris-be',
                'minio', 'minio-init', 'metastore-postgres', 'hive-metastore', 'trino',
                'flink-jobmanager', 'flink-taskmanager', 'flink-sql-client', 'api'
            )) -FailureMessage 'Realtime service rebuild failed.' | Out-Null
        Invoke-Chapter105Native -FilePath 'powershell' -Arguments @(
            '-NoProfile', '-File', (Join-Path $RepositoryRoot 'scripts\restore_chapter_10_5_catalog.ps1'),
            '-EnvFile', (Join-Path $RepositoryRoot 'infra\.env'),
            '-ComposeProjectName', $projectName
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

if ($chapter105ResetFunctionsOnly) { return }

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
