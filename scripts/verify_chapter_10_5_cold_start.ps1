[CmdletBinding()]
param(
    [switch]$KeepOnFailure,
    [ValidateRange(10, 90)][int]$TimeoutMinutes = 45
)

$ErrorActionPreference = 'Stop'

function Invoke-AcceptanceDocker {
    param([Parameter(Mandatory = $true)][string[]]$Arguments, [Parameter(Mandatory = $true)][string]$FailureMessage)

    $output = @(& docker @Arguments 2>&1)
    if ($LASTEXITCODE -ne 0) { throw $FailureMessage }
    return @($output | ForEach-Object { [string]$_ })
}

function Read-AcceptanceEnv {
    param([Parameter(Mandatory = $true)][string]$Path)

    $values = [ordered]@{}
    foreach ($line in Get-Content -LiteralPath $Path -Encoding UTF8) {
        if ([string]::IsNullOrWhiteSpace($line) -or $line.TrimStart().StartsWith('#')) { continue }
        $parts = $line.Split('=', 2)
        if ($parts.Count -ne 2 -or $parts[0] -cnotmatch '^[A-Z0-9_]+$') {
            throw 'Default environment file has an invalid line.'
        }
        if ($values.Contains($parts[0])) { throw 'Default environment file has a duplicate key.' }
        $values[$parts[0]] = $parts[1]
    }
    return $values
}

function Assert-AcceptanceProjectName {
    param([Parameter(Mandatory = $true)][string]$ProjectName, [Parameter(Mandatory = $true)][string]$DefaultProjectName)

    if ($ProjectName -cnotmatch '^chapter105-acceptance-[a-f0-9]{12}$' -or $ProjectName -ceq $DefaultProjectName) {
        throw 'Compose project identity is unsafe.'
    }
}

function Get-AcceptanceFreePort {
    for ($attempt = 1; $attempt -le 100; $attempt += 1) {
        $port = Get-Random -Minimum 20000 -Maximum 59000
        $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $port)
        try {
            $listener.Start()
            return $port
        } catch {
            continue
        } finally {
            $listener.Stop()
        }
    }
    throw 'Unable to reserve an isolated host port.'
}

function Get-AcceptanceSubnet {
    param([Parameter(Mandatory = $true)][string]$ProjectName)

    $occupied = @((Invoke-AcceptanceDocker -Arguments @('network', 'ls', '--format', '{{.ID}}') `
        -FailureMessage 'Docker network listing failed.') | ForEach-Object {
        $id = $_.Trim()
        if ($id) { Invoke-AcceptanceDocker -Arguments @('network', 'inspect', $id, '--format', '{{json .IPAM.Config}}') `
            -FailureMessage 'Docker network inspection failed.' }
    }) -join "`n"
    for ($octet = 20; $octet -le 240; $octet += 1) {
        $candidate = "172.30.$octet.0/24"
        if ($occupied -notmatch [regex]::Escape($candidate)) { return $candidate }
    }
    throw 'Unable to allocate an isolated Docker subnet.'
}

function Get-AcceptanceDefaultProjectState {
    param(
        [Parameter(Mandatory = $true)][string]$ProjectName,
        [Parameter(Mandatory = $true)][string]$MinioContainerName,
        [Parameter(Mandatory = $true)][string]$MinioUser,
        [Parameter(Mandatory = $true)][string]$MinioPassword
    )

    $label = "com.docker.compose.project=$ProjectName"
    $containers = @(Invoke-AcceptanceDocker -Arguments @('ps', '-a', '--filter', "label=$label", '--format', '{{.ID}}') `
        -FailureMessage 'Default project container snapshot failed.' | Where-Object { $_ }) | Sort-Object
    $volumes = @(Invoke-AcceptanceDocker -Arguments @('volume', 'ls', '--filter', "label=$label", '--format', '{{.Name}}') `
        -FailureMessage 'Default project volume snapshot failed.' | Where-Object { $_ }) | Sort-Object
    $networks = @(Invoke-AcceptanceDocker -Arguments @('network', 'ls', '--filter', "label=$label", '--format', '{{.ID}}') `
        -FailureMessage 'Default project network snapshot failed.' | Where-Object { $_ }) | Sort-Object
    $objectCount = $null
    $running = @(& docker inspect --format '{{.State.Running}}' $MinioContainerName 2>$null)
    if ($LASTEXITCODE -eq 0 -and $running.Count -eq 1 -and $running[0].Trim() -ceq 'true') {
        $countLines = Invoke-AcceptanceDocker -Arguments @(
            'exec', $MinioContainerName, 'sh', '-lc',
            "mc alias set local http://localhost:9000 '$MinioUser' '$MinioPassword' >/dev/null && mc find local --json | wc -l"
        ) -FailureMessage 'Default project MinIO object snapshot failed.'
        if ($countLines.Count -ne 1 -or $countLines[0].Trim() -cnotmatch '^[0-9]+$') {
            throw 'Default project MinIO object snapshot is invalid.'
        }
        $objectCount = [int]$countLines[0].Trim()
    }
    return [ordered]@{ containers = $containers; volumes = $volumes; networks = $networks; minio_object_count = $objectCount }
}

function Test-AcceptanceStateEqual {
    param([Parameter(Mandatory = $true)][object]$Before, [Parameter(Mandatory = $true)][object]$After)
    return (($Before | ConvertTo-Json -Depth 8 -Compress) -ceq ($After | ConvertTo-Json -Depth 8 -Compress))
}

function Assert-AcceptanceRunRoot {
    param([Parameter(Mandatory = $true)][string]$RepositoryRoot, [Parameter(Mandatory = $true)][string]$RunRoot)

    $acceptanceRoot = [System.IO.Path]::GetFullPath((Join-Path $RepositoryRoot 'tmp/chapter-10-5/acceptance')).TrimEnd('\', '/')
    $resolvedRunRoot = [System.IO.Path]::GetFullPath($RunRoot).TrimEnd('\', '/')
    if (-not $resolvedRunRoot.StartsWith($acceptanceRoot + [System.IO.Path]::DirectorySeparatorChar,
            [System.StringComparison]::OrdinalIgnoreCase) -or
        [System.IO.Path]::GetFileName($resolvedRunRoot) -cnotmatch '^[a-f0-9]{12}$') {
        throw 'Acceptance cleanup path is unsafe.'
    }
    return $resolvedRunRoot
}

function Remove-AcceptanceOwnedResources {
    param(
        [Parameter(Mandatory = $true)][string[]]$ComposePrefix,
        [Parameter(Mandatory = $true)][string]$ProjectName,
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string]$RunRoot
    )

    Assert-AcceptanceRunRoot -RepositoryRoot $RepositoryRoot -RunRoot $RunRoot | Out-Null
    Invoke-AcceptanceDocker -Arguments ($ComposePrefix + @('down', '--remove-orphans')) `
        -FailureMessage 'Isolated Compose cleanup failed.' | Out-Null
    $label = "com.docker.compose.project=$ProjectName"
    foreach ($volume in @(Invoke-AcceptanceDocker -Arguments @('volume', 'ls', '--filter', "label=$label", '--format', '{{.Name}}') `
            -FailureMessage 'Isolated volume listing failed.')) {
        if ([string]::IsNullOrWhiteSpace($volume)) { continue }
        $owner = @(Invoke-AcceptanceDocker -Arguments @('volume', 'inspect', $volume, '--format', '{{ index .Labels "com.docker.compose.project" }}') `
            -FailureMessage 'Isolated volume ownership inspection failed.')
        if ($owner.Count -ne 1 -or $owner[0].Trim() -cne $ProjectName) { throw 'Isolated volume ownership changed.' }
        Invoke-AcceptanceDocker -Arguments @('volume', 'rm', $volume) -FailureMessage 'Isolated volume cleanup failed.' | Out-Null
    }
    Remove-Item -LiteralPath $RunRoot -Recurse -Force
}

function Invoke-AcceptanceBootstrap {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string]$EnvFile,
        [Parameter(Mandatory = $true)][string]$ProjectName,
        [Parameter(Mandatory = $true)][string]$ReportPath,
        [switch]$InitializeEmptyCatalog
    )

    $arguments = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', (Join-Path $RepositoryRoot 'scripts/bootstrap_chapter_10_5.ps1'),
        '-EnvFile', $EnvFile, '-ComposeProjectName', $ProjectName, '-ReportPath', $ReportPath, '-SkipBuild', '-IsolatedAcceptance')
    if ($InitializeEmptyCatalog) { $arguments += '-InitializeEmptyCatalog' }
    $output = @(& powershell @arguments 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "Isolated Bootstrap failed: $($output -join ' ')" }
}

function Invoke-AcceptanceTrinoScalar {
    param([Parameter(Mandatory = $true)][string[]]$ComposePrefix, [Parameter(Mandatory = $true)][string]$Sql)
    $lines = @(Invoke-AcceptanceDocker -Arguments ($ComposePrefix + @('exec', '-T', 'trino', 'trino', '--server', 'http://localhost:8080',
        '--catalog', 'lakehouse', '--schema', 'analytics', '--output-format', 'CSV_HEADER', '--execute', $Sql)) `
        -FailureMessage 'Isolated Trino query failed.' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($lines.Count -ne 2) { throw 'Isolated Trino scalar query returned an invalid result.' }
    return $lines[1].Trim()
}

function Get-AcceptanceCheckpoint {
    param([Parameter(Mandatory = $true)][int]$FlinkPort, [Parameter(Mandatory = $true)][string]$JobName)
    $overview = (Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:$FlinkPort/jobs/overview" -TimeoutSec 10).Content | ConvertFrom-Json
    $jobs = @($overview.jobs | Where-Object { $_.name -ceq $JobName -and $_.state -ceq 'RUNNING' })
    if ($jobs.Count -ne 1) { throw 'Isolated Flink production job is not uniquely RUNNING.' }
    $checkpoints = (Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:$FlinkPort/jobs/$($jobs[0].jid)/checkpoints" -TimeoutSec 10).Content | ConvertFrom-Json
    if ($null -eq $checkpoints.latest -or $null -eq $checkpoints.latest.id -or [int64]$checkpoints.latest.id -lt 1) {
        throw 'Isolated Flink checkpoint evidence is missing.'
    }
    return [ordered]@{ job_id = [string]$jobs[0].jid; checkpoint_id = [int64]$checkpoints.latest.id }
}

$repositoryRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$defaults = Read-AcceptanceEnv -Path (Join-Path $repositoryRoot 'infra/.env.example')
$defaultProjectName = [string]$defaults['PROJECT_NAME']
$defaultMinioContainerName = [string]$defaults['MINIO_CONTAINER_NAME']
$runId = [Guid]::NewGuid().ToString('N').Substring(0, 12)
$projectName = "chapter105-acceptance-$runId"
Assert-AcceptanceProjectName -ProjectName $projectName -DefaultProjectName $defaultProjectName
$runRoot = Assert-AcceptanceRunRoot -RepositoryRoot $repositoryRoot -RunRoot (Join-Path $repositoryRoot "tmp/chapter-10-5/acceptance/$runId")
$reportPath = Join-Path $repositoryRoot "tmp/chapter-10-5/cold-start-report-$runId.json"
$envPath = Join-Path $runRoot 'isolated.env'
$bootstrapOneReport = Join-Path $runRoot 'bootstrap-first.json'
$bootstrapTwoReport = Join-Path $runRoot 'bootstrap-second.json'
$bootstrapThreeReport = Join-Path $runRoot 'bootstrap-recovery.json'
$report = [ordered]@{
    status = 'running'; run_id = $runId; compose_project = $projectName; report_path = $reportPath
    cold_start = 'pending'; idempotent_second_run = 'pending'; restart_recovery = 'pending'
    data_continuity = 'pending'; readiness = 'pending'; tool_analysis = 'pending'
    default_project_before = $null; default_project_after = $null; cleanup = 'pending'; error = $null
}
$completed = $false

try {
    Invoke-AcceptanceDocker -Arguments @('info') -FailureMessage 'Docker engine is unavailable. Start Docker Desktop and retry.' | Out-Null
    [System.IO.Directory]::CreateDirectory($runRoot) | Out-Null
    $subnet = Get-AcceptanceSubnet -ProjectName $projectName
    $prefix = ($subnet -replace '\.0/24$', '')
    $ports = @{}
    foreach ($name in @('KAFKA_PORT', 'API_PORT', 'FLINK_REST_PORT', 'DORIS_FE_HTTP_PORT', 'DORIS_FE_QUERY_PORT',
            'DORIS_FE_EDIT_LOG_PORT', 'DORIS_BE_HTTP_PORT', 'DORIS_BE_HEARTBEAT_PORT', 'MINIO_API_PORT', 'MINIO_CONSOLE_PORT', 'TRINO_PORT')) {
        do { $port = Get-AcceptanceFreePort } while ($ports.Values -contains $port)
        $ports[$name] = $port
    }
    $overrides = [ordered]@{
        PROJECT_NAME = $projectName; MINIO_DATA_DIR = (Join-Path $runRoot 'minio-data')
        DORIS_NETWORK_SUBNET = $subnet; DORIS_NETWORK_IP_RANGE = "$prefix.128/25"; DORIS_FE_STATIC_IP = "$prefix.2"; DORIS_BE_STATIC_IP = "$prefix.3"
        KAFKA_CONTROLLER_CONTAINER_NAME = "$projectName-kafka-controller"; KAFKA_CONTAINER_NAME = "$projectName-kafka"
        API_CONTAINER_NAME = "$projectName-api"; FLINK_JOBMANAGER_CONTAINER_NAME = "$projectName-jobmanager"; FLINK_TASKMANAGER_CONTAINER_NAME = "$projectName-taskmanager"
        FLINK_SQL_CLIENT_CONTAINER_NAME = "$projectName-sql-client"; DORIS_FE_CONTAINER_NAME = "$projectName-doris-fe"; DORIS_BE_CONTAINER_NAME = "$projectName-doris-be"
        MINIO_CONTAINER_NAME = "$projectName-minio"; MINIO_INIT_CONTAINER_NAME = "$projectName-minio-init"; METASTORE_POSTGRES_CONTAINER_NAME = "$projectName-postgres"
        HIVE_METASTORE_CONTAINER_NAME = "$projectName-hive"; TRINO_CONTAINER_NAME = "$projectName-trino"
    }
    foreach ($entry in $ports.GetEnumerator()) { $overrides[$entry.Key] = [string]$entry.Value }
    foreach ($key in $overrides.Keys) { $defaults[$key] = [string]$overrides[$key] }
    [System.IO.File]::WriteAllLines($envPath, @($defaults.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }), [System.Text.UTF8Encoding]::new($false))
    $composePrefix = @('compose', '--project-name', $projectName, '--env-file', $envPath, '-f', (Join-Path $repositoryRoot 'infra/docker-compose.yml'),
        '--profile', 'flink', '--profile', 'serving', '--profile', 'lakehouse')
    $report.default_project_before = Get-AcceptanceDefaultProjectState -ProjectName $defaultProjectName -MinioContainerName $defaultMinioContainerName `
        -MinioUser $defaults['MINIO_ROOT_USER'] -MinioPassword $defaults['MINIO_ROOT_PASSWORD']

    Invoke-AcceptanceBootstrap -RepositoryRoot $repositoryRoot -EnvFile $envPath -ProjectName $projectName -ReportPath $bootstrapOneReport -InitializeEmptyCatalog
    $firstCheckpoint = Get-AcceptanceCheckpoint -FlinkPort ([int]$ports['FLINK_REST_PORT']) -JobName $defaults['CHAPTER9_PRODUCTION_JOB_NAME']
    $report.cold_start = 'passed'

    Invoke-AcceptanceBootstrap -RepositoryRoot $repositoryRoot -EnvFile $envPath -ProjectName $projectName -ReportPath $bootstrapTwoReport
    $secondCheckpoint = Get-AcceptanceCheckpoint -FlinkPort ([int]$ports['FLINK_REST_PORT']) -JobName $defaults['CHAPTER9_PRODUCTION_JOB_NAME']
    if ($secondCheckpoint.job_id -cne $firstCheckpoint.job_id) { throw 'Second Bootstrap created a duplicate production job.' }
    $report.idempotent_second_run = 'passed'

    Invoke-AcceptanceTrinoScalar -ComposePrefix $composePrefix -Sql "INSERT INTO lakehouse.analytics.user_behavior_detail VALUES ('acceptance-$runId', 'user', 'product', 'view', '2026-08-27T00:00:00Z', 'acceptance', 'test', 'page')" | Out-Null
    $rowCountBefore = [int64](Invoke-AcceptanceTrinoScalar -ComposePrefix $composePrefix -Sql 'SELECT count(*) FROM lakehouse.analytics.user_behavior_detail')
    $snapshotBefore = Invoke-AcceptanceTrinoScalar -ComposePrefix $composePrefix -Sql 'SELECT snapshot_id FROM "user_behavior_detail$snapshots" ORDER BY committed_at DESC LIMIT 1'

    Invoke-AcceptanceDocker -Arguments ($composePrefix + @('rm', '-sf', 'hive-metastore', 'trino', 'flink-jobmanager', 'flink-taskmanager', 'flink-sql-client')) `
        -FailureMessage 'Isolated service recreation failed.' | Out-Null
    Invoke-AcceptanceDocker -Arguments ($composePrefix + @('up', '-d', 'hive-metastore', 'trino', 'flink-jobmanager', 'flink-taskmanager', 'flink-sql-client')) `
        -FailureMessage 'Isolated service restart failed.' | Out-Null
    Invoke-AcceptanceBootstrap -RepositoryRoot $repositoryRoot -EnvFile $envPath -ProjectName $projectName -ReportPath $bootstrapThreeReport
    $recoveryCheckpoint = Get-AcceptanceCheckpoint -FlinkPort ([int]$ports['FLINK_REST_PORT']) -JobName $defaults['CHAPTER9_PRODUCTION_JOB_NAME']
    $rowCountAfter = [int64](Invoke-AcceptanceTrinoScalar -ComposePrefix $composePrefix -Sql 'SELECT count(*) FROM lakehouse.analytics.user_behavior_detail')
    $snapshotAfter = Invoke-AcceptanceTrinoScalar -ComposePrefix $composePrefix -Sql 'SELECT snapshot_id FROM "user_behavior_detail$snapshots" ORDER BY committed_at DESC LIMIT 1'
    if ($rowCountAfter -lt $rowCountBefore -or [string]::IsNullOrWhiteSpace($snapshotBefore) -or [string]::IsNullOrWhiteSpace($snapshotAfter)) {
        throw 'Isolated data continuity evidence regressed.'
    }
    if ($recoveryCheckpoint.checkpoint_id -lt 1) { throw 'Isolated checkpoint recovery did not advance.' }
    $report.restart_recovery = 'passed'; $report.data_continuity = 'passed'

    $ready = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$($ports['API_PORT'])/ready" -TimeoutSec 20
    if ($ready.StatusCode -ne 200) { throw 'Isolated readiness endpoint did not return HTTP 200.' }
    $report.readiness = 'passed'
    & (Join-Path $repositoryRoot 'scripts/verify_chapter_10_tool_analysis.ps1') -AnalysisBaseUrl "http://127.0.0.1:$($ports['API_PORT'])" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Isolated tool analysis verification failed.' }
    $report.tool_analysis = 'passed'
    $completed = $true
} catch {
    $report.error = $_.Exception.Message
} finally {
    try {
        $report.default_project_after = Get-AcceptanceDefaultProjectState -ProjectName $defaultProjectName -MinioContainerName $defaultMinioContainerName `
            -MinioUser $defaults['MINIO_ROOT_USER'] -MinioPassword $defaults['MINIO_ROOT_PASSWORD']
        if ($null -ne $report.default_project_before -and -not (Test-AcceptanceStateEqual -Before $report.default_project_before -After $report.default_project_after)) {
            throw 'Default project resource or MinIO object state changed during isolated acceptance.'
        }
        if ($completed -and -not $KeepOnFailure) {
            Remove-AcceptanceOwnedResources -ComposePrefix $composePrefix -ProjectName $projectName -RepositoryRoot $repositoryRoot -RunRoot $runRoot
            $report.cleanup = 'passed'
        } else {
            $report.cleanup = 'preserved'
        }
    } catch {
        if ($null -eq $report.error) { $report.error = $_.Exception.Message }
        $report.cleanup = 'preserved'
        $completed = $false
    }
    $report.status = if ($completed) { 'passed' } else { 'failed' }
    [System.IO.Directory]::CreateDirectory((Split-Path -Parent $reportPath)) | Out-Null
    $report | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $reportPath -Encoding UTF8
}

if (-not $completed) { throw "Chapter 10.5 isolated cold-start acceptance failed. Report: $reportPath" }
Write-Output $reportPath
