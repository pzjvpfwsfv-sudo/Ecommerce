[CmdletBinding()]
param(
    [switch]$KeepOnFailure,
    [ValidateRange(10, 90)][int]$TimeoutMinutes = 45,
    [string]$DefaultEnvFile,
    [switch]$FunctionsOnly
)

$ErrorActionPreference = 'Stop'

function Get-AcceptanceRemainingMilliseconds {
    param([Parameter(Mandatory = $true)][DateTimeOffset]$Deadline)

    $remaining = [int64][Math]::Floor(($Deadline - [DateTimeOffset]::UtcNow).TotalMilliseconds)
    if ($remaining -lt 1) { throw 'Chapter 10.5 acceptance deadline expired.' }
    return [int][Math]::Min($remaining, [int]::MaxValue)
}

function ConvertTo-AcceptanceNativeArgument {
    param([AllowEmptyString()][string]$Value)

    if ($Value -notmatch '[\s"]' -and $Value.Length -gt 0) { return $Value }
    $escaped = [regex]::Replace($Value, '(\\*)"', '$1$1\"')
    $escaped = [regex]::Replace($escaped, '(\\+)$', '$1$1')
    return '"' + $escaped + '"'
}

function Stop-AcceptanceProcessTree {
    param([Parameter(Mandatory = $true)][System.Diagnostics.Process]$Process)

    try { if ($Process.HasExited) { return } } catch { return }
    if ($env:OS -ceq 'Windows_NT') {
        $taskkill = [System.Diagnostics.ProcessStartInfo]::new()
        $taskkill.FileName = Join-Path $env:SystemRoot 'System32\taskkill.exe'
        $taskkill.Arguments = "/PID $($Process.Id) /T /F"
        $taskkill.UseShellExecute = $false
        $taskkill.CreateNoWindow = $true
        $taskkill.RedirectStandardOutput = $true
        $taskkill.RedirectStandardError = $true
        $killer = [System.Diagnostics.Process]::new()
        $killer.StartInfo = $taskkill
        try {
            if (-not $killer.Start()) { throw 'start failed' }
            $null = $killer.StandardOutput.ReadToEndAsync()
            $null = $killer.StandardError.ReadToEndAsync()
            if (-not $killer.WaitForExit(5000)) {
                try { $killer.Kill() } catch { }
                throw 'timeout'
            }
        } catch {
            throw 'Timed-out process tree could not be terminated safely.'
        } finally {
            $killer.Dispose()
        }
    } else {
        try { $Process.Kill() } catch { }
    }
    if (-not $Process.WaitForExit(5000)) {
        throw 'Timed-out process tree could not be terminated safely.'
    }
}

function Initialize-AcceptanceWindowsJobInterop {
    if ($null -ne ('Chapter105.AcceptanceJobRunner' -as [type])) { return }
    Add-Type -Path (Join-Path $PSScriptRoot 'lib\Chapter105.AcceptanceJob.cs') -ErrorAction Stop
}

function Invoke-AcceptanceProcess {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][DateTimeOffset]$Deadline,
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )

    $remaining = Get-AcceptanceRemainingMilliseconds -Deadline $Deadline
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $FilePath
    $nativeArguments = @($Arguments | ForEach-Object {
        ConvertTo-AcceptanceNativeArgument -Value ([string]$_)
    })
    if ($env:OS -ceq 'Windows_NT') {
        $commandLine = @(
            ConvertTo-AcceptanceNativeArgument -Value $FilePath
            $nativeArguments
        ) -join ' '
        try {
            Initialize-AcceptanceWindowsJobInterop
            $result = [Chapter105.AcceptanceJobRunner]::Run($commandLine, $remaining)
        } catch {
            throw $FailureMessage
        }
        if ($result.TimedOut) { throw 'Chapter 10.5 acceptance deadline expired.' }
        if ($result.ExitCode -ne 0) { throw $FailureMessage }
        return @(([string]$result.StandardOutput -split "`r?`n") | Where-Object {
            -not [string]::IsNullOrWhiteSpace($_)
        })
    }
    $startInfo.Arguments = $nativeArguments -join ' '
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    try {
        if (-not $process.Start()) { throw $FailureMessage }
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit($remaining)) {
            Stop-AcceptanceProcessTree -Process $process
            throw 'Chapter 10.5 acceptance deadline expired.'
        }
        $stdout = $stdoutTask.GetAwaiter().GetResult()
        $stderr = $stderrTask.GetAwaiter().GetResult()
        if ($process.ExitCode -ne 0) { throw $FailureMessage }
        return @(($stdout -split "`r?`n") | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    } catch {
        if ($_.Exception.Message -ceq 'Chapter 10.5 acceptance deadline expired.') { throw }
        throw $FailureMessage
    } finally {
        $process.Dispose()
    }
}

function Invoke-AcceptanceDocker {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][DateTimeOffset]$Deadline,
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )
    return Invoke-AcceptanceProcess -FilePath 'docker' -Arguments $Arguments -Deadline $Deadline -FailureMessage $FailureMessage
}

function Read-AcceptanceEnv {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { throw 'Environment file is missing.' }
    $values = [ordered]@{}
    foreach ($line in Get-Content -LiteralPath $Path -Encoding UTF8) {
        if ([string]::IsNullOrWhiteSpace($line) -or $line.TrimStart().StartsWith('#')) { continue }
        $parts = $line.Split('=', 2)
        if ($parts.Count -ne 2 -or $parts[0] -cnotmatch '^[A-Z0-9_]+$' -or $values.Contains($parts[0])) {
            throw 'Environment file is invalid.'
        }
        $values[$parts[0]] = $parts[1]
    }
    return $values
}

function Get-AcceptanceActualDefaultEnvPath {
    param([Parameter(Mandatory = $true)][string]$RepositoryRoot, [string]$RequestedPath)

    $candidate = if ($RequestedPath) {
        if ([System.IO.Path]::IsPathRooted($RequestedPath)) { $RequestedPath } else { Join-Path $RepositoryRoot $RequestedPath }
    } elseif (Test-Path -LiteralPath (Join-Path $RepositoryRoot 'infra/.env') -PathType Leaf) {
        Join-Path $RepositoryRoot 'infra/.env'
    } else {
        Join-Path $RepositoryRoot 'infra/.env.example'
    }
    return [System.IO.Path]::GetFullPath($candidate)
}

function New-AcceptanceIsolatedEnvironment {
    param(
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$ReferenceValues,
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$DefaultValues,
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$IsolationValues
    )

    $values = [ordered]@{}
    foreach ($source in @($ReferenceValues, $DefaultValues, $IsolationValues)) {
        foreach ($entry in $source.GetEnumerator()) { $values[[string]$entry.Key] = [string]$entry.Value }
    }
    foreach ($entry in ([ordered]@{
            API_BIND_HOST = '127.0.0.1'
            TRINO_BASE_URL = 'http://trino:8080'
            FLINK_REST_URL = 'http://flink-jobmanager:8081'
            DORIS_INTERNAL_QUERY_PORT = '9030'
            KAFKA_CONTROLLER_HOST = 'kafka-controller'
            KAFKA_CONTROLLER_PORT = '9093'
            CHAPTER9_PRODUCTION_JOB_NAME = 'chapter-9-datastream-quality-production'
        }).GetEnumerator()) {
        $values[[string]$entry.Key] = [string]$entry.Value
    }
    return $values
}

function Set-AcceptanceEnvironment {
    param([Parameter(Mandatory = $true)][System.Collections.IDictionary]$Values)

    $previous = [ordered]@{}
    foreach ($key in $Values.Keys) {
        $previous[$key] = [Environment]::GetEnvironmentVariable([string]$key, 'Process')
        [Environment]::SetEnvironmentVariable([string]$key, [string]$Values[$key], 'Process')
    }
    return $previous
}

function Restore-AcceptanceEnvironment {
    param([Parameter(Mandatory = $true)][System.Collections.IDictionary]$Previous)
    foreach ($key in $Previous.Keys) {
        [Environment]::SetEnvironmentVariable([string]$key, $Previous[$key], 'Process')
    }
}

function Assert-AcceptanceProjectName {
    param([Parameter(Mandatory = $true)][string]$ProjectName, [Parameter(Mandatory = $true)][string]$DefaultProjectName)
    if ($ProjectName -cnotmatch '^chapter105-acceptance-[a-f0-9]{12}$' -or $ProjectName -ceq $DefaultProjectName) {
        throw 'Compose project identity is unsafe.'
    }
}

function Assert-AcceptanceRunRoot {
    param(
        [Parameter(Mandatory = $true)][string]$AcceptanceRoot,
        [Parameter(Mandatory = $true)][string]$RunRoot,
        [Parameter(Mandatory = $true)][string]$RunId
    )

    $safeError = 'Acceptance cleanup path is unsafe.'
    try {
        if ($RunId -cnotmatch '^[a-f0-9]{12}$') { throw $safeError }
        $root = [System.IO.Path]::GetFullPath($AcceptanceRoot).TrimEnd('\', '/')
        $candidate = [System.IO.Path]::GetFullPath($RunRoot).TrimEnd('\', '/')
        $expected = [System.IO.Path]::GetFullPath((Join-Path $root $RunId)).TrimEnd('\', '/')
        if (-not $candidate.Equals($expected, [System.StringComparison]::OrdinalIgnoreCase)) { throw $safeError }

        foreach ($path in @($root, $candidate)) {
            $current = [System.IO.Path]::GetPathRoot($path)
            $relative = $path.Substring($current.Length)
            foreach ($segment in $relative.Split(@('\', '/'), [System.StringSplitOptions]::RemoveEmptyEntries)) {
                $current = Join-Path $current $segment
                if (-not (Test-Path -LiteralPath $current)) { break }
                $item = Get-Item -LiteralPath $current -Force
                if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) { throw $safeError }
            }
        }
        return $candidate
    } catch {
        throw $safeError
    }
}

function Assert-AcceptanceRenderedConfig {
    param(
        [Parameter(Mandatory = $true)][string]$ConfigJson,
        [System.Collections.IDictionary]$ExpectedValues,
        [string]$ProjectName,
        [string]$MinioContainerName,
        [string]$MinioDataPath
    )

    $safeError = 'Rendered Compose isolation is unsafe.'
    try {
        if ($null -eq $ExpectedValues) {
            $ExpectedValues = [ordered]@{
                PROJECT_NAME = $ProjectName
                MINIO_CONTAINER_NAME = $MinioContainerName
                MINIO_DATA_DIR = $MinioDataPath
                DORIS_INTERNAL_QUERY_PORT = '9030'
            }
        }
        $ProjectName = [string]$ExpectedValues['PROJECT_NAME']
        $MinioContainerName = [string]$ExpectedValues['MINIO_CONTAINER_NAME']
        $MinioDataPath = [string]$ExpectedValues['MINIO_DATA_DIR']
        $config = $ConfigJson | ConvertFrom-Json -ErrorAction Stop
        if ([string]$config.name -cne $ProjectName -or
            [string]$config.networks.'platform-net'.name -cne "$ProjectName-net" -or
            [string]$config.services.minio.container_name -cne $MinioContainerName -or
            [string]$config.services.api.environment.DORIS_PORT -cne [string]$ExpectedValues['DORIS_INTERNAL_QUERY_PORT']) {
            throw $safeError
        }
        $mounts = @($config.services.minio.volumes | Where-Object { [string]$_.target -ceq '/data' })
        $renderedMinioPath = if (Test-Path -LiteralPath ([string]$mounts[0].source)) {
            (Get-Item -LiteralPath ([string]$mounts[0].source) -Force).FullName
        } else { [System.IO.Path]::GetFullPath([string]$mounts[0].source) }
        $expectedMinioPath = if (Test-Path -LiteralPath $MinioDataPath) {
            (Get-Item -LiteralPath $MinioDataPath -Force).FullName
        } else { [System.IO.Path]::GetFullPath($MinioDataPath) }
        if ($mounts.Count -ne 1 -or [string]$mounts[0].type -cne 'bind' -or
            -not $renderedMinioPath.Equals($expectedMinioPath, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw $safeError
        }
        foreach ($service in $config.services.PSObject.Properties) {
            $containerName = [string]$service.Value.container_name
            if ($containerName -and -not $containerName.StartsWith("$ProjectName-", [System.StringComparison]::Ordinal)) {
                throw $safeError
            }
        }

        $containerMap = [ordered]@{
            KAFKA_CONTROLLER_CONTAINER_NAME = 'kafka-controller'; KAFKA_CONTAINER_NAME = 'kafka-broker'; API_CONTAINER_NAME = 'api'
            FLINK_JOBMANAGER_CONTAINER_NAME = 'flink-jobmanager'; FLINK_TASKMANAGER_CONTAINER_NAME = 'flink-taskmanager'
            FLINK_SQL_CLIENT_CONTAINER_NAME = 'flink-sql-client'; DORIS_FE_CONTAINER_NAME = 'doris-fe'; DORIS_BE_CONTAINER_NAME = 'doris-be'
            MINIO_CONTAINER_NAME = 'minio'; MINIO_INIT_CONTAINER_NAME = 'minio-init'; METASTORE_POSTGRES_CONTAINER_NAME = 'metastore-postgres'
            HIVE_METASTORE_CONTAINER_NAME = 'hive-metastore'; TRINO_CONTAINER_NAME = 'trino'
        }
        foreach ($entry in $containerMap.GetEnumerator()) {
            if ($ExpectedValues.Contains($entry.Key) -and
                [string]$config.services.($entry.Value).container_name -cne [string]$ExpectedValues[$entry.Key]) { throw $safeError }
        }

        $portMap = @(
            @('KAFKA_PORT', 'kafka-broker', 9092), @('API_PORT', 'api', 8000), @('FLINK_REST_PORT', 'flink-jobmanager', 8081),
            @('DORIS_FE_HTTP_PORT', 'doris-fe', 8030), @('DORIS_FE_QUERY_PORT', 'doris-fe', 9030),
            @('DORIS_FE_EDIT_LOG_PORT', 'doris-fe', 9010), @('DORIS_BE_HTTP_PORT', 'doris-be', 8040),
            @('DORIS_BE_HEARTBEAT_PORT', 'doris-be', 9050), @('MINIO_API_PORT', 'minio', 9000),
            @('MINIO_CONSOLE_PORT', 'minio', 9001), @('TRINO_PORT', 'trino', 8080)
        )
        foreach ($mapping in $portMap) {
            if (-not $ExpectedValues.Contains([string]$mapping[0])) { continue }
            $ports = @($config.services.([string]$mapping[1]).ports | Where-Object { [int]$_.target -eq [int]$mapping[2] })
            if ($ports.Count -ne 1 -or [string]$ports[0].published -cne [string]$ExpectedValues[[string]$mapping[0]]) { throw $safeError }
        }
        if ($ExpectedValues.Contains('DORIS_NETWORK_SUBNET')) {
            $ipam = @($config.networks.custom_network.ipam.config)
            if ($ipam.Count -ne 1 -or [string]$ipam[0].subnet -cne [string]$ExpectedValues['DORIS_NETWORK_SUBNET'] -or
                [string]$ipam[0].ip_range -cne [string]$ExpectedValues['DORIS_NETWORK_IP_RANGE'] -or
                [string]$config.services.'doris-fe'.networks.custom_network.ipv4_address -cne [string]$ExpectedValues['DORIS_FE_STATIC_IP'] -or
                [string]$config.services.'doris-be'.networks.custom_network.ipv4_address -cne [string]$ExpectedValues['DORIS_BE_STATIC_IP']) {
                throw $safeError
            }
        }
        if ($ExpectedValues.Contains('API_BIND_HOST')) {
            $apiPorts = @($config.services.api.ports | Where-Object { [int]$_.target -eq 8000 })
            if ($apiPorts.Count -ne 1 -or
                [string]$apiPorts[0].host_ip -cne [string]$ExpectedValues['API_BIND_HOST']) { throw $safeError }
        }
        $apiEnvironment = $config.services.api.environment
        $endpointMap = [ordered]@{
            TRINO_BASE_URL = 'TRINO_BASE_URL'
            FLINK_REST_URL = 'FLINK_REST_URL'
            CHAPTER9_PRODUCTION_JOB_NAME = 'CHAPTER9_PRODUCTION_JOB_NAME'
        }
        foreach ($entry in $endpointMap.GetEnumerator()) {
            if ($ExpectedValues.Contains($entry.Key) -and
                [string]$apiEnvironment.($entry.Value) -cne [string]$ExpectedValues[$entry.Key]) { throw $safeError }
        }
        if ([string]$apiEnvironment.DORIS_HOST -cne 'doris-fe' -or
            [string]$apiEnvironment.DORIS_PORT -cne [string]$ExpectedValues['DORIS_INTERNAL_QUERY_PORT']) { throw $safeError }
        if ($ExpectedValues.Contains('KAFKA_CONTROLLER_HOST') -and $ExpectedValues.Contains('KAFKA_CONTROLLER_PORT')) {
            $expectedVoterSuffix = "@$([string]$ExpectedValues['KAFKA_CONTROLLER_HOST']):$([string]$ExpectedValues['KAFKA_CONTROLLER_PORT'])"
            foreach ($serviceName in @('kafka-controller', 'kafka-broker')) {
                $voters = [string]$config.services.($serviceName).environment.KAFKA_CONTROLLER_QUORUM_VOTERS
                if (-not $voters.EndsWith($expectedVoterSuffix, [System.StringComparison]::Ordinal)) { throw $safeError }
            }
        }
        return 'validated'
    } catch {
        throw $safeError
    }
}

function Get-AcceptanceFreePort {
    for ($attempt = 1; $attempt -le 100; $attempt += 1) {
        $port = Get-Random -Minimum 20000 -Maximum 59000
        $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $port)
        try { $listener.Start(); return $port } catch { continue } finally { $listener.Stop() }
    }
    throw 'Unable to reserve an isolated host port.'
}

function Get-AcceptanceSubnet {
    param([Parameter(Mandatory = $true)][DateTimeOffset]$Deadline)
    $ids = Invoke-AcceptanceDocker -Arguments @('network', 'ls', '--format', '{{.ID}}') -Deadline $Deadline `
        -FailureMessage 'Docker network listing failed.'
    $occupied = @($ids | ForEach-Object {
        Invoke-AcceptanceDocker -Arguments @('network', 'inspect', $_, '--format', '{{json .IPAM.Config}}') `
            -Deadline $Deadline -FailureMessage 'Docker network inspection failed.'
    }) -join "`n"
    for ($octet = 20; $octet -le 240; $octet += 1) {
        $candidate = "172.30.$octet.0/24"
        if ($occupied -notmatch [regex]::Escape($candidate)) { return $candidate }
    }
    throw 'Unable to allocate an isolated Docker subnet.'
}

function Get-AcceptanceCheckpointEvidence {
    param(
        [Parameter(Mandatory = $true)][object]$Overview,
        [Parameter(Mandatory = $true)][object]$Checkpoints,
        [Parameter(Mandatory = $true)][DateTimeOffset]$Now,
        [Parameter(Mandatory = $true)][int]$MaxAgeSeconds,
        [string]$JobName = 'chapter-9-datastream-quality-production'
    )

    $jobs = @($Overview.jobs | Where-Object { [string]$_.name -ceq $JobName -and [string]$_.state -ceq 'RUNNING' })
    if ($jobs.Count -ne 1 -or [string]$jobs[0].jid -cnotmatch '^[0-9a-f]{32}$') {
        throw 'Isolated Flink production job is not uniquely RUNNING.'
    }
    $completed = $Checkpoints.latest.completed
    if ($null -eq $completed -or [string]$completed.status -cne 'COMPLETED' -or
        $completed.id -isnot [ValueType] -or [int64]$completed.id -lt 1 -or
        $completed.latest_ack_timestamp -isnot [ValueType] -or
        [string]$completed.external_path -cnotmatch '^s3a://flink-state/checkpoints/chapter-9(?:/[A-Za-z0-9._-]+)+$') {
        throw 'Isolated Flink checkpoint evidence is invalid.'
    }
    $timestamp = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]$completed.latest_ack_timestamp)
    $age = ($Now - $timestamp).TotalSeconds
    if ($age -lt 0 -or $age -gt $MaxAgeSeconds) { throw 'Isolated Flink checkpoint is not fresh.' }
    return [ordered]@{
        job_id = [string]$jobs[0].jid
        checkpoint_id = [int64]$completed.id
        latest_ack_timestamp = [int64]$completed.latest_ack_timestamp
        external_path = [string]$completed.external_path
    }
}

function Get-AcceptanceCheckpoint {
    param(
        [Parameter(Mandatory = $true)][int]$FlinkPort,
        [Parameter(Mandatory = $true)][DateTimeOffset]$Deadline,
        [int]$MaxAgeSeconds = 120
    )
    $seconds = [Math]::Max(1, [Math]::Min(20, [Math]::Floor((Get-AcceptanceRemainingMilliseconds -Deadline $Deadline) / 1000)))
    $overview = (Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$FlinkPort/jobs/overview" -TimeoutSec $seconds).Content | ConvertFrom-Json
    $job = @($overview.jobs | Where-Object { [string]$_.name -ceq 'chapter-9-datastream-quality-production' -and [string]$_.state -ceq 'RUNNING' })
    if ($job.Count -ne 1) { throw 'Isolated Flink production job is not uniquely RUNNING.' }
    $checkpoints = (Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$FlinkPort/jobs/$($job[0].jid)/checkpoints" -TimeoutSec $seconds).Content | ConvertFrom-Json
    return Get-AcceptanceCheckpointEvidence -Overview $overview -Checkpoints $checkpoints -Now ([DateTimeOffset]::UtcNow) -MaxAgeSeconds $MaxAgeSeconds
}

function Set-AcceptanceRestartRecoveryState {
    param(
        [Parameter(Mandatory = $true)][string]$StateRoot,
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][string]$ExpectedJobId,
        [Parameter(Mandatory = $true)][string]$JobName,
        [Parameter(Mandatory = $true)][string]$SavepointPath
    )

    $safeError = 'Isolated restart recovery state is unsafe.'
    $lock = $null
    try {
        if ($RunId -cnotmatch '^[a-f0-9]{12}$' -or [System.IO.Path]::GetFileName($StateRoot) -cne $RunId -or
            $ExpectedJobId -cnotmatch '^[0-9a-f]{32}$' -or
            $SavepointPath -cnotmatch '^s3a://flink-state/savepoints/chapter-9(?:/[A-Za-z0-9._-]+)+$') { throw $safeError }
        $acceptanceRoot = Split-Path -Parent ([System.IO.Path]::GetFullPath($StateRoot))
        Assert-AcceptanceRunRoot -AcceptanceRoot $acceptanceRoot -RunRoot $StateRoot -RunId $RunId | Out-Null
        $stateDirectory = Join-Path $StateRoot 'tmp\chapter-9'
        $statePath = Join-Path $stateDirectory 'production-submit-state.json'
        $recoveryPath = Join-Path $stateDirectory 'acceptance-restart-recovery.json'
        if (-not (Test-Path -LiteralPath $statePath -PathType Leaf)) { throw $safeError }
        $lock = [System.IO.File]::Open("$statePath.lock", [System.IO.FileMode]::OpenOrCreate,
            [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
        $state = Get-Content -LiteralPath $statePath -Raw -Encoding UTF8 | ConvertFrom-Json -ErrorAction Stop
        if ([int]$state.schema_version -ne 1 -or [string]$state.kind -cne 'chapter9_production_submit' -or
            [string]$state.status -cne 'result' -or [string]$state.result.job_id -cne $ExpectedJobId -or
            [string]$state.result.job_name -cne $JobName) { throw $safeError }
        $transition = [ordered]@{
            schema_version = 1; kind = 'chapter10_5_acceptance_restart'; job_name = $JobName
            previous_job_id = $ExpectedJobId; savepoint_path = $SavepointPath
            created_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
        }
        $notStarted = [ordered]@{ schema_version = 1; kind = 'chapter9_production_submit'; status = 'not_started'; intent = $null; result = $null }
        $tempRecovery = "$recoveryPath.partial.$([Guid]::NewGuid().ToString('N'))"
        $tempState = "$statePath.partial.$([Guid]::NewGuid().ToString('N'))"
        [System.IO.File]::WriteAllText($tempRecovery, ($transition | ConvertTo-Json -Depth 8), [System.Text.UTF8Encoding]::new($false))
        [System.IO.File]::WriteAllText($tempState, ($notStarted | ConvertTo-Json -Depth 8), [System.Text.UTF8Encoding]::new($false))
        Assert-AcceptanceRunRoot -AcceptanceRoot $acceptanceRoot -RunRoot $StateRoot -RunId $RunId | Out-Null
        Move-Item -LiteralPath $tempRecovery -Destination $recoveryPath -Force
        Move-Item -LiteralPath $tempState -Destination $statePath -Force
        return [ordered]@{ state_path = $statePath; recovery_path = $recoveryPath }
    } catch {
        throw $safeError
    } finally {
        if ($null -ne $lock) { $lock.Dispose() }
    }
}

function Get-AcceptanceBootstrapEvidence {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][ValidateSet('submitted', 'no_op')][string]$ExpectedJobAction,
        [string]$ExpectedCatalogAction,
        [switch]$RequireInitializationNoOp
    )
    $safeError = 'Bootstrap phase report is invalid.'
    try {
        $report = Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json -ErrorAction Stop
        if ([string]$report.status -cne 'passed') { throw $safeError }
        $reportStarted = [DateTimeOffset]::Parse([string]$report.started_at)
        $reportCompleted = [DateTimeOffset]::Parse([string]$report.completed_at)
        if ($reportCompleted -lt $reportStarted) { throw $safeError }
        foreach ($name in @('preflight', 'dependencies', 'infrastructure', 'initialization', 'catalog', 'jobs', 'acceptance')) {
            $stage = $report.stages.PSObject.Properties[$name].Value
            if ($null -eq $stage -or [string]$stage.status -cne 'passed' -or
                [string]::IsNullOrWhiteSpace([string]$stage.started_at) -or
                [string]::IsNullOrWhiteSpace([string]$stage.completed_at)) { throw $safeError }
            if ([DateTimeOffset]::Parse([string]$stage.completed_at) -lt [DateTimeOffset]::Parse([string]$stage.started_at)) {
                throw $safeError
            }
        }
        $jobs = $report.stages.jobs.details
        if ([string]$jobs.action -cne $ExpectedJobAction -or [string]$jobs.job_id -cnotmatch '^[0-9a-f]{32}$') { throw $safeError }
        $catalogAction = [string]$report.stages.catalog.details.action
        if ($ExpectedCatalogAction -and $catalogAction -cne $ExpectedCatalogAction) { throw $safeError }
        $initialization = $report.stages.initialization.details
        $buckets = @($initialization.minio_buckets)
        if ([string]$initialization.topic.name -cne 'user_behavior_events' -or
            [string]$initialization.doris.name -cne 'analytics.realtime_metrics' -or
            $buckets.Count -ne 2 -or [string]$buckets[0].name -cne 'warehouse' -or
            [string]$buckets[1].name -cne 'flink-state') { throw $safeError }
        $initializationActions = @(
            [string]$initialization.topic.action,
            [string]$initialization.doris.action,
            [string]$buckets[0].action,
            [string]$buckets[1].action
        )
        foreach ($action in $initializationActions) {
            if ($action -cne 'created' -and $action -cne 'no_op') { throw $safeError }
            if ($RequireInitializationNoOp -and $action -cne 'no_op') { throw $safeError }
        }
        return [ordered]@{
            path = [System.IO.Path]::GetFullPath($Path); started_at = [string]$report.started_at
            completed_at = [string]$report.completed_at; job_action = [string]$jobs.action; job_id = [string]$jobs.job_id
            catalog_action = $catalogAction
            initialization = [ordered]@{
                topic = $initializationActions[0]; doris = $initializationActions[1]
                warehouse = $initializationActions[2]; flink_state = $initializationActions[3]
            }
        }
    } catch { throw $safeError }
}

function ConvertTo-AcceptanceBuildDiagnostic {
    param([AllowEmptyCollection()][string[]]$Lines)

    $text = (@($Lines | Select-Object -Last 80) -join "`n")
    if ([string]::IsNullOrWhiteSpace($text)) { return 'No Maven diagnostics were captured.' }
    $text = [regex]::Replace($text, '(?i)(https?://)[^/\s:@]+:[^@\s/]+@', '$1[REDACTED]@')
    $text = [regex]::Replace(
        $text,
        '(?i)(password|passwd|token|secret|access[_-]?key|secret[_-]?key)(\s*[:=]\s*)[^\s]+',
        '$1$2[REDACTED]'
    )
    if ($text.Length -gt 2048) { $text = $text.Substring($text.Length - 2048) }
    return $text
}

function Get-AcceptanceOwnedBuildContainer {
    param(
        [Parameter(Mandatory = $true)][string]$ContainerName,
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][DateTimeOffset]$Deadline
    )

    $ids = @(Invoke-AcceptanceDocker -Arguments @(
        'ps', '-aq', '--no-trunc', '--filter', "name=^/$ContainerName$"
    ) -Deadline $Deadline -FailureMessage 'Build container lookup failed.')
    if ($ids.Count -eq 0) { return $null }
    if ($ids.Count -ne 1 -or [string]$ids[0] -cnotmatch '^[a-f0-9]{64}$') {
        throw 'Build container lookup identity is unsafe.'
    }
    $details = @(Invoke-AcceptanceDocker -Arguments @('inspect', '--format', '{{json .}}', [string]$ids[0]) -Deadline $Deadline `
        -FailureMessage 'Build container ownership inspection failed.')
    if ($details.Count -ne 1) { throw 'Build container inspection result is unsafe.' }
    try { $container = $details[0] | ConvertFrom-Json -ErrorAction Stop }
    catch { throw 'Build container inspection JSON is unsafe.' }
    if ([string]$container.Id -cne [string]$ids[0] -or [string]$container.Name -cne "/$ContainerName" -or
        [string]$container.Config.Labels.'com.ecommerce.chapter105.acceptance.run-id' -cne $RunId -or
        [string]$container.Config.Labels.'com.ecommerce.chapter105.acceptance.role' -cne 'maven-build') {
        throw 'Build container ownership is unsafe.'
    }
    return [string]$container.Id
}

function Remove-AcceptanceBuildContainer {
    param(
        [Parameter(Mandatory = $true)][string]$ContainerName,
        [Parameter(Mandatory = $true)][string]$AcceptanceRoot,
        [Parameter(Mandatory = $true)][string]$RunRoot,
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][DateTimeOffset]$Deadline
    )

    Assert-AcceptanceRunRoot -AcceptanceRoot $AcceptanceRoot -RunRoot $RunRoot -RunId $RunId | Out-Null
    if ($ContainerName -cne "chapter105-acceptance-$RunId-maven") { throw 'Build container identity is unsafe.' }
    $containerId = Get-AcceptanceOwnedBuildContainer -ContainerName $ContainerName -RunId $RunId -Deadline $Deadline
    if ($null -ne $containerId) {
        Invoke-AcceptanceDocker -Arguments @('rm', '-f', $containerId) -Deadline $Deadline `
            -FailureMessage 'Build container cleanup failed.' | Out-Null
    }
}

function Invoke-AcceptanceArtifactBuild {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string]$AcceptanceRoot,
        [Parameter(Mandatory = $true)][string]$RunRoot,
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][DateTimeOffset]$Deadline
    )

    $validatedRunRoot = Assert-AcceptanceRunRoot -AcceptanceRoot $AcceptanceRoot -RunRoot $RunRoot -RunId $RunId
    $mavenCache = Join-Path $validatedRunRoot 'maven-cache'
    [System.IO.Directory]::CreateDirectory($mavenCache) | Out-Null
    Assert-AcceptanceRunRoot -AcceptanceRoot $AcceptanceRoot -RunRoot $validatedRunRoot -RunId $RunId | Out-Null
    $cacheItem = Get-Item -LiteralPath $mavenCache -Force
    if (($cacheItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0 -or
        -not ([System.IO.Path]::GetFullPath($cacheItem.Parent.FullName)).Equals(
            [System.IO.Path]::GetFullPath($validatedRunRoot), [System.StringComparison]::OrdinalIgnoreCase)) {
        throw 'Acceptance Maven cache path is unsafe.'
    }

    $image = 'maven:3.9.9-eclipse-temurin-17'
    $containerName = "chapter105-acceptance-$RunId-maven"
    $cidFile = Join-Path $validatedRunRoot 'maven-build.cid'
    if (Test-Path -LiteralPath $cidFile) { throw 'Acceptance Maven container identity file already exists.' }
    $containerId = $null
    $diagnostics = 'No Maven diagnostics were captured.'
    $buildFailure = $null
    $cleanupFailure = $null
    try {
        $created = @(Invoke-AcceptanceDocker -Arguments @(
            'create', '--name', $containerName, '--cidfile', $cidFile,
            '--label', "com.ecommerce.chapter105.acceptance.run-id=$RunId",
            '--label', 'com.ecommerce.chapter105.acceptance.role=maven-build',
            '--volume', "$RepositoryRoot`:/workspace",
            '--volume', "$mavenCache`:/root/.m2",
            '--workdir', '/workspace/jobs/datastream-quality',
            $image, 'mvn', '--batch-mode', '--no-transfer-progress', 'clean', '-DskipTests', 'package'
        ) -Deadline $Deadline -FailureMessage 'Clean-checkout DataStream build container creation failed.')
        if ($created.Count -ne 1 -or [string]$created[0] -cnotmatch '^[a-f0-9]{64}$') {
            throw 'Clean-checkout DataStream build container identity is invalid.'
        }
        $containerId = [string]$created[0]
        $output = @(Invoke-AcceptanceDocker -Arguments @('start', '-a', $containerId) -Deadline $Deadline `
            -FailureMessage 'Clean-checkout DataStream artifact build failed.')
        $diagnostics = ConvertTo-AcceptanceBuildDiagnostic -Lines $output
    } catch {
        $buildFailure = $_.Exception.Message
        $cleanupDeadline = [DateTimeOffset]::UtcNow.AddSeconds(15)
        try {
            $ownedContainerId = Get-AcceptanceOwnedBuildContainer -ContainerName $containerName -RunId $RunId -Deadline $cleanupDeadline
            if ($null -ne $ownedContainerId) {
                $logs = @(Invoke-AcceptanceDocker -Arguments @('logs', '--tail', '80', $ownedContainerId) -Deadline $cleanupDeadline `
                    -FailureMessage 'Build diagnostics unavailable.')
                if ($logs.Count -gt 0) { $diagnostics = ConvertTo-AcceptanceBuildDiagnostic -Lines $logs }
            }
        } catch { }
    } finally {
        $cleanupDeadline = [DateTimeOffset]::UtcNow.AddSeconds(15)
        try {
            Remove-AcceptanceBuildContainer -ContainerName $containerName -AcceptanceRoot $AcceptanceRoot `
                -RunRoot $validatedRunRoot -RunId $RunId -Deadline $cleanupDeadline
        } catch {
            $cleanupFailure = $_.Exception.Message
        }
    }

    if ($null -ne $cleanupFailure) {
        throw "Clean-checkout DataStream build container cleanup failed: $containerName. $cleanupFailure Diagnostics: $diagnostics"
    }
    if ($null -ne $buildFailure) {
        $failurePrefix = if ($buildFailure -ceq 'Chapter 10.5 acceptance deadline expired.') {
            $buildFailure
        } else {
            'Clean-checkout DataStream artifact build failed.'
        }
        throw "$failurePrefix Diagnostics: $diagnostics"
    }
    $jar = Join-Path $RepositoryRoot 'jobs/datastream-quality/target/datastream-quality-1.0.0.jar'
    if (-not (Test-Path -LiteralPath $jar -PathType Leaf) -or (Get-Item -LiteralPath $jar).Length -lt 1) {
        throw 'Clean-checkout DataStream artifact is missing.'
    }
    return [ordered]@{
        jar_path = $jar
        image = $image
        container_name = $containerName
        cache = 'maven-cache'
        diagnostics = $diagnostics
    }
}

function Invoke-AcceptanceBoundarySnapshot {
    param(
        [Parameter(Mandatory = $true)][ValidateSet('File', 'Directory')][string]$Kind,
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][DateTimeOffset]$Deadline
    )

    $encodedPath = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Path))
    $snapshotScript = @"
`$kind = '$Kind'
`$path = [IO.Path]::GetFullPath([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('$encodedPath')))
"@ + "`n" + @'
$ErrorActionPreference = 'Stop'
function Get-Hash([string]$FilePath) {
    $stream = [IO.File]::OpenRead($FilePath)
    $algorithm = [Security.Cryptography.SHA256]::Create()
    try { $bytes = $algorithm.ComputeHash($stream) }
    finally { $algorithm.Dispose(); $stream.Dispose() }
    return ([BitConverter]::ToString($bytes)).Replace('-', '').ToLowerInvariant()
}
if ($kind -ceq 'File') {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        $result = [ordered]@{ exists = $false; sha256 = $null }
    } else {
        $item = Get-Item -LiteralPath $path -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw 'reparse point' }
        $result = [ordered]@{ exists = $true; sha256 = Get-Hash $path }
    }
} elseif (-not (Test-Path -LiteralPath $path -PathType Container)) {
    $result = [ordered]@{ exists = $false; files = @() }
} else {
    $root = $path.TrimEnd('\', '/')
    $rootItem = Get-Item -LiteralPath $root -Force
    if (($rootItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw 'reparse point' }
    $stack = [Collections.Generic.Stack[string]]::new()
    $stack.Push($root)
    $files = [Collections.Generic.List[object]]::new()
    while ($stack.Count -gt 0) {
        $current = $stack.Pop()
        foreach ($entry in [IO.Directory]::EnumerateFileSystemEntries($current)) {
            $item = Get-Item -LiteralPath $entry -Force
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw 'reparse point' }
            if ($item.PSIsContainer) { $stack.Push($item.FullName) }
            else {
                $files.Add([ordered]@{
                    path = $item.FullName.Substring($root.Length).TrimStart('\', '/')
                    length = [int64]$item.Length
                    sha256 = Get-Hash $item.FullName
                }) | Out-Null
            }
        }
    }
    $result = [ordered]@{ exists = $true; files = @($files | Sort-Object path) }
}
$result | ConvertTo-Json -Depth 8 -Compress
'@
    $encodedScript = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($snapshotScript))
    $lines = @(Invoke-AcceptanceProcess -FilePath 'powershell' -Arguments @(
        '-NoProfile', '-EncodedCommand', $encodedScript
    ) -Deadline $Deadline -FailureMessage 'Shared-state boundary snapshot failed.')
    if ($lines.Count -ne 1) { throw 'Shared-state boundary snapshot failed.' }
    try { return $lines[0] | ConvertFrom-Json -ErrorAction Stop }
    catch { throw 'Shared-state boundary snapshot failed.' }
}

function Get-AcceptanceFileBoundary {
    param([Parameter(Mandatory = $true)][string]$Path, [Parameter(Mandatory = $true)][DateTimeOffset]$Deadline)
    return Invoke-AcceptanceBoundarySnapshot -Kind File -Path $Path -Deadline $Deadline
}

function Get-AcceptanceDirectoryBoundary {
    param([Parameter(Mandatory = $true)][string]$Path, [Parameter(Mandatory = $true)][DateTimeOffset]$Deadline)
    return Invoke-AcceptanceBoundarySnapshot -Kind Directory -Path $Path -Deadline $Deadline
}

function Get-AcceptanceDefaultProjectState {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string]$DefaultEnvPath,
        [Parameter(Mandatory = $true)][DateTimeOffset]$Deadline
    )
    $compose = @('compose', '--env-file', $DefaultEnvPath, '-f', (Join-Path $RepositoryRoot 'infra/docker-compose.yml'),
        '--profile', 'flink', '--profile', 'serving', '--profile', 'lakehouse')
    $configJson = (Invoke-AcceptanceDocker -Arguments ($compose + @('config', '--format', 'json')) -Deadline $Deadline `
        -FailureMessage 'Default Compose configuration snapshot failed.') -join "`n"
    $config = $configJson | ConvertFrom-Json
    $project = [string]$config.name
    $label = "com.docker.compose.project=$project"
    $containers = @(Invoke-AcceptanceDocker -Arguments @('ps', '-a', '--filter', "label=$label", '--format', '{{.ID}}') -Deadline $Deadline -FailureMessage 'Default container snapshot failed.') | Sort-Object
    $volumes = @(Invoke-AcceptanceDocker -Arguments @('volume', 'ls', '--filter', "label=$label", '--format', '{{.Name}}') -Deadline $Deadline -FailureMessage 'Default volume snapshot failed.') | Sort-Object
    $networks = @(Invoke-AcceptanceDocker -Arguments @('network', 'ls', '--filter', "label=$label", '--format', '{{.ID}}') -Deadline $Deadline -FailureMessage 'Default network snapshot failed.') | Sort-Object
    $attachments = @()
    foreach ($container in $containers) {
        $attachments += Invoke-AcceptanceDocker -Arguments @('inspect', $container, '--format', '{{json .NetworkSettings.Networks}}') -Deadline $Deadline -FailureMessage 'Default network attachment snapshot failed.'
    }
    $primaryRoot = $RepositoryRoot
    $common = @(Invoke-AcceptanceProcess -FilePath 'git' -Arguments @('-C', $RepositoryRoot, 'rev-parse', '--path-format=absolute', '--git-common-dir') `
        -Deadline $Deadline -FailureMessage 'Default Git common-directory snapshot failed.')
    if ($common.Count -eq 1 -and [System.IO.Path]::GetFileName([string]$common[0]) -ceq '.git') {
        $primaryRoot = Split-Path -Parent ([System.IO.Path]::GetFullPath([string]$common[0]))
    }
    $minioMounts = @($config.services.minio.volumes | Where-Object { [string]$_.target -ceq '/data' -and [string]$_.type -ceq 'bind' })
    if ($minioMounts.Count -ne 1) { throw 'Default MinIO shared-state boundary is invalid.' }
    $minioDataPath = [System.IO.Path]::GetFullPath([string]$minioMounts[0].source)
    return [ordered]@{
        resolved_config_sha256 = ([BitConverter]::ToString([Security.Cryptography.SHA256]::Create().ComputeHash([Text.Encoding]::UTF8.GetBytes($configJson))).Replace('-', '').ToLowerInvariant())
        project = $project; containers = $containers; volumes = $volumes; networks = $networks; attachments = @($attachments | Sort-Object)
        chapter9_state = Get-AcceptanceDirectoryBoundary -Path (Join-Path $primaryRoot 'tmp/chapter-9') -Deadline $Deadline
        production_state = Get-AcceptanceFileBoundary -Path (Join-Path $primaryRoot 'tmp/chapter-9/production-submit-state.json') -Deadline $Deadline
        legacy_state = Get-AcceptanceFileBoundary -Path (Join-Path $primaryRoot 'tmp/chapter-9/cutover-manifest.json.partial') -Deadline $Deadline
        minio_data_path = $minioDataPath
        minio_data = Get-AcceptanceDirectoryBoundary -Path $minioDataPath -Deadline $Deadline
    }
}

function Test-AcceptanceStateEqual {
    param([Parameter(Mandatory = $true)][object]$Before, [Parameter(Mandatory = $true)][object]$After)
    return (($Before | ConvertTo-Json -Depth 20 -Compress) -ceq ($After | ConvertTo-Json -Depth 20 -Compress))
}

function Assert-AcceptanceContinuity {
    param(
        [Parameter(Mandatory = $true)][int64]$RowCountInitial,
        [Parameter(Mandatory = $true)][int64]$RowCountBeforeRestart,
        [Parameter(Mandatory = $true)][int64]$RowCountAfterRestart,
        [Parameter(Mandatory = $true)][string]$SnapshotBeforeRestart,
        [Parameter(Mandatory = $true)][string]$SnapshotAfterRestart
    )
    if ($RowCountBeforeRestart -ne ($RowCountInitial + 1) -or $RowCountAfterRestart -ne $RowCountBeforeRestart -or
        [string]::IsNullOrWhiteSpace($SnapshotBeforeRestart) -or $SnapshotAfterRestart -cne $SnapshotBeforeRestart) {
        throw 'Isolated data continuity evidence is invalid.'
    }
    return [ordered]@{
        row_count_initial = $RowCountInitial; inserted_rows = 1; row_count_before_restart = $RowCountBeforeRestart
        row_count_after_restart = $RowCountAfterRestart; snapshot_id_before_restart = $SnapshotBeforeRestart
        snapshot_id_after_restart = $SnapshotAfterRestart
    }
}

function New-AcceptanceReport {
    param(
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][string]$ProjectName,
        [Parameter(Mandatory = $true)][DateTimeOffset]$Deadline
    )
    return [ordered]@{
        status = 'running'; run_id = $RunId; compose_project = $ProjectName; deadline_utc = $Deadline.ToString('o')
        cold_start = 'pending'; idempotent_second_run = 'pending'; restart_recovery = 'pending'
        data_continuity = 'pending'; readiness = 'pending'; tool_analysis = 'pending'
        evidence = [ordered]@{}; default_project_before = $null; default_project_after = $null
        cleanup = 'pending'; error = $null
    }
}

function Invoke-AcceptanceBootstrap {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string]$EnvFile,
        [Parameter(Mandatory = $true)][string]$ProjectName,
        [Parameter(Mandatory = $true)][string]$ReportPath,
        [Parameter(Mandatory = $true)][DateTimeOffset]$Deadline,
        [switch]$InitializeEmptyCatalog
    )
    $arguments = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', (Join-Path $RepositoryRoot 'scripts/bootstrap_chapter_10_5.ps1'),
        '-EnvFile', $EnvFile, '-ComposeProjectName', $ProjectName, '-ReportPath', $ReportPath,
        '-DeadlineUtc', $Deadline.ToString('o'), '-SkipBuild', '-IsolatedAcceptance')
    if ($InitializeEmptyCatalog) { $arguments += '-InitializeEmptyCatalog' }
    Invoke-AcceptanceProcess -FilePath 'powershell' -Arguments $arguments -Deadline $Deadline -FailureMessage 'Isolated Bootstrap failed.' | Out-Null
}

function Invoke-AcceptanceTrinoScalar {
    param(
        [Parameter(Mandatory = $true)][string[]]$ComposePrefix,
        [Parameter(Mandatory = $true)][string]$Sql,
        [Parameter(Mandatory = $true)][DateTimeOffset]$Deadline
    )
    $lines = @(Invoke-AcceptanceDocker -Arguments ($ComposePrefix + @('exec', '-T', 'trino', 'trino', '--server', 'http://localhost:8080',
        '--catalog', 'lakehouse', '--schema', 'analytics', '--output-format', 'CSV_HEADER_UNQUOTED', '--execute', $Sql)) `
        -Deadline $Deadline -FailureMessage 'Isolated Trino query failed.')
    if ($lines.Count -ne 2) { throw 'Isolated Trino scalar query returned an invalid result.' }
    return $lines[1].Trim()
}

function New-AcceptanceSavepoint {
    param(
        [Parameter(Mandatory = $true)][string[]]$ComposePrefix,
        [Parameter(Mandatory = $true)][string]$JobId,
        [Parameter(Mandatory = $true)][string]$SavepointBase,
        [Parameter(Mandatory = $true)][DateTimeOffset]$Deadline
    )
    $lines = @(Invoke-AcceptanceDocker -Arguments ($ComposePrefix + @('exec', '-T', 'flink-jobmanager', '/opt/flink/bin/flink',
        'stop', '--savepointPath', $SavepointBase, $JobId)) -Deadline $Deadline -FailureMessage 'Isolated Flink savepoint failed.')
    if ($lines.Count -ne 1 -or $lines[0] -notmatch '^Savepoint completed\. Path: (?<path>s3a://flink-state/savepoints/chapter-9(?:/[A-Za-z0-9._-]+)+)$') {
        throw 'Isolated Flink savepoint evidence is invalid.'
    }
    return $Matches['path']
}

function Remove-AcceptanceOwnedResources {
    param(
        [Parameter(Mandatory = $true)][string[]]$ComposePrefix,
        [Parameter(Mandatory = $true)][string]$ProjectName,
        [Parameter(Mandatory = $true)][string]$AcceptanceRoot,
        [Parameter(Mandatory = $true)][string]$RunRoot,
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][DateTimeOffset]$Deadline
    )
    Assert-AcceptanceRunRoot -AcceptanceRoot $AcceptanceRoot -RunRoot $RunRoot -RunId $RunId | Out-Null
    Invoke-AcceptanceDocker -Arguments ($ComposePrefix + @('down', '--remove-orphans')) -Deadline $Deadline -FailureMessage 'Isolated Compose cleanup failed.' | Out-Null
    $label = "com.docker.compose.project=$ProjectName"
    foreach ($volume in @(Invoke-AcceptanceDocker -Arguments @('volume', 'ls', '--filter', "label=$label", '--format', '{{.Name}}') -Deadline $Deadline -FailureMessage 'Isolated volume listing failed.')) {
        if ([string]::IsNullOrWhiteSpace($volume)) { continue }
        $owner = @(Invoke-AcceptanceDocker -Arguments @('volume', 'inspect', $volume, '--format', '{{ index .Labels "com.docker.compose.project" }}') -Deadline $Deadline -FailureMessage 'Isolated volume ownership inspection failed.')
        if ($owner.Count -ne 1 -or $owner[0].Trim() -cne $ProjectName) { throw 'Isolated volume ownership changed.' }
        Invoke-AcceptanceDocker -Arguments @('volume', 'rm', $volume) -Deadline $Deadline -FailureMessage 'Isolated volume cleanup failed.' | Out-Null
    }
    # Revalidate every existing segment immediately before bounded recursive deletion.
    $validated = Assert-AcceptanceRunRoot -AcceptanceRoot $AcceptanceRoot -RunRoot $RunRoot -RunId $RunId
    $deleteJob = Start-Job -ScriptBlock {
        param($AcceptanceRoot, $RunRoot, $RunId, $DeadlineUtc)
        $ErrorActionPreference = 'Stop'
        $safeError = 'Acceptance cleanup path is unsafe.'
        $root = [IO.Path]::GetFullPath($AcceptanceRoot).TrimEnd('\', '/')
        $candidate = [IO.Path]::GetFullPath($RunRoot).TrimEnd('\', '/')
        $expected = [IO.Path]::GetFullPath((Join-Path $root $RunId)).TrimEnd('\', '/')
        if ($RunId -cnotmatch '^[a-f0-9]{12}$' -or
            -not $candidate.Equals($expected, [StringComparison]::OrdinalIgnoreCase)) { throw $safeError }
        foreach ($path in @($root, $candidate)) {
            $current = [IO.Path]::GetPathRoot($path)
            foreach ($segment in $path.Substring($current.Length).Split(@('\', '/'), [StringSplitOptions]::RemoveEmptyEntries)) {
                $current = Join-Path $current $segment
                if (-not (Test-Path -LiteralPath $current)) { break }
                if (((Get-Item -LiteralPath $current -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw $safeError }
            }
        }
        if ([DateTimeOffset]::UtcNow -ge [DateTimeOffset]::Parse($DeadlineUtc)) { throw 'Chapter 10.5 acceptance deadline expired.' }
        Remove-Item -LiteralPath $candidate -Recurse -Force
    } -ArgumentList $AcceptanceRoot, $validated, $RunId, $Deadline.ToString('o')
    try {
        $remainingSeconds = [Math]::Max(1, [Math]::Ceiling((Get-AcceptanceRemainingMilliseconds -Deadline $Deadline) / 1000))
        if ($null -eq (Wait-Job -Job $deleteJob -Timeout $remainingSeconds)) {
            Stop-Job -Job $deleteJob
            throw 'Chapter 10.5 acceptance deadline expired.'
        }
        Receive-Job -Job $deleteJob -ErrorAction Stop | Out-Null
        if ($deleteJob.State -ne 'Completed') { throw 'Isolated run-directory cleanup failed.' }
    } finally {
        Remove-Job -Job $deleteJob -Force
    }
}

function Invoke-Chapter105ColdStartVerification {
    param([switch]$KeepOnFailure, [int]$TimeoutMinutes = 45, [string]$DefaultEnvFile)

    $deadline = [DateTimeOffset]::UtcNow.AddMinutes($TimeoutMinutes)
    $repositoryRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
    $acceptanceRoot = [System.IO.Path]::GetFullPath((Join-Path $repositoryRoot 'tmp/chapter-10-5/acceptance'))
    $defaultEnvPath = Get-AcceptanceActualDefaultEnvPath -RepositoryRoot $repositoryRoot -RequestedPath $DefaultEnvFile
    $defaultEnvironment = Read-AcceptanceEnv -Path $defaultEnvPath
    $referenceEnvironment = Read-AcceptanceEnv -Path (Join-Path $repositoryRoot 'infra/.env.example')
    $runId = [Guid]::NewGuid().ToString('N').Substring(0, 12)
    $projectName = "chapter105-acceptance-$runId"
    Assert-AcceptanceProjectName -ProjectName $projectName -DefaultProjectName ([string]$defaultEnvironment['PROJECT_NAME'])
    $runRoot = Assert-AcceptanceRunRoot -AcceptanceRoot $acceptanceRoot -RunRoot (Join-Path $acceptanceRoot $runId) -RunId $runId
    [System.IO.Directory]::CreateDirectory($runRoot) | Out-Null
    $envPath = Join-Path $runRoot 'isolated.env'
    $minioPath = Join-Path $runRoot 'minio-data'
    [System.IO.Directory]::CreateDirectory($minioPath) | Out-Null
    $firstReport = Join-Path $runRoot 'bootstrap-first.json'
    $secondReport = Join-Path $runRoot 'bootstrap-second.json'
    $recoveryReport = Join-Path $runRoot 'bootstrap-recovery.json'
    $reportPath = Join-Path $repositoryRoot "tmp/chapter-10-5/cold-start-report-$runId.json"
    $report = New-AcceptanceReport -RunId $runId -ProjectName $projectName -Deadline $deadline
    $previousEnvironment = $null
    $isolationValues = $null
    $completed = $false
    try {
        Invoke-AcceptanceDocker -Arguments @('info') -Deadline $deadline -FailureMessage 'Docker engine is unavailable.' | Out-Null
        $build = Invoke-AcceptanceArtifactBuild -RepositoryRoot $repositoryRoot -AcceptanceRoot $acceptanceRoot `
            -RunRoot $runRoot -RunId $runId -Deadline $deadline
        $jar = [string]$build.jar_path
        $report.evidence.artifact_build = [ordered]@{
            image = [string]$build.image
            container_name = [string]$build.container_name
            cache = [string]$build.cache
            diagnostics = [string]$build.diagnostics
        }

        $report.default_project_before = Get-AcceptanceDefaultProjectState -RepositoryRoot $repositoryRoot -DefaultEnvPath $defaultEnvPath -Deadline $deadline
        $subnet = Get-AcceptanceSubnet -Deadline $deadline
        $prefix = $subnet -replace '\.0/24$', ''
        $ports = [ordered]@{}
        foreach ($name in @('KAFKA_PORT', 'API_PORT', 'FLINK_REST_PORT', 'DORIS_FE_HTTP_PORT', 'DORIS_FE_QUERY_PORT',
                'DORIS_FE_EDIT_LOG_PORT', 'DORIS_BE_HTTP_PORT', 'DORIS_BE_HEARTBEAT_PORT', 'MINIO_API_PORT', 'MINIO_CONSOLE_PORT', 'TRINO_PORT')) {
            do { $port = Get-AcceptanceFreePort } while ($ports.Values -contains $port)
            $ports[$name] = [string]$port
        }
        $isolationValues = [ordered]@{
            PROJECT_NAME = $projectName; MINIO_DATA_DIR = $minioPath; DORIS_INTERNAL_QUERY_PORT = '9030'
            DORIS_NETWORK_SUBNET = $subnet; DORIS_NETWORK_IP_RANGE = "$prefix.128/25"; DORIS_FE_STATIC_IP = "$prefix.2"; DORIS_BE_STATIC_IP = "$prefix.3"
            KAFKA_CONTROLLER_CONTAINER_NAME = "$projectName-kafka-controller"; KAFKA_CONTAINER_NAME = "$projectName-kafka"; API_CONTAINER_NAME = "$projectName-api"
            FLINK_JOBMANAGER_CONTAINER_NAME = "$projectName-jobmanager"; FLINK_TASKMANAGER_CONTAINER_NAME = "$projectName-taskmanager"; FLINK_SQL_CLIENT_CONTAINER_NAME = "$projectName-sql-client"
            DORIS_FE_CONTAINER_NAME = "$projectName-doris-fe"; DORIS_BE_CONTAINER_NAME = "$projectName-doris-be"; MINIO_CONTAINER_NAME = "$projectName-minio"
            MINIO_INIT_CONTAINER_NAME = "$projectName-minio-init"; METASTORE_POSTGRES_CONTAINER_NAME = "$projectName-postgres"
            HIVE_METASTORE_CONTAINER_NAME = "$projectName-hive"; TRINO_CONTAINER_NAME = "$projectName-trino"
        }
        foreach ($entry in $ports.GetEnumerator()) { $isolationValues[$entry.Key] = $entry.Value }
        $isolatedEnvironment = New-AcceptanceIsolatedEnvironment -ReferenceValues $referenceEnvironment `
            -DefaultValues $defaultEnvironment -IsolationValues $isolationValues
        [System.IO.File]::WriteAllLines($envPath, @($isolatedEnvironment.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }), [System.Text.UTF8Encoding]::new($false))
        $previousEnvironment = Set-AcceptanceEnvironment -Values $isolatedEnvironment
        $composePrefix = @('compose', '--project-name', $projectName, '--env-file', $envPath, '-f', (Join-Path $repositoryRoot 'infra/docker-compose.yml'),
            '--profile', 'flink', '--profile', 'serving', '--profile', 'lakehouse')
        $rendered = (Invoke-AcceptanceDocker -Arguments ($composePrefix + @('config', '--format', 'json')) -Deadline $deadline -FailureMessage 'Isolated Compose render failed.') -join "`n"
        Assert-AcceptanceRenderedConfig -ConfigJson $rendered -ExpectedValues $isolatedEnvironment | Out-Null

        Invoke-AcceptanceBootstrap -RepositoryRoot $repositoryRoot -EnvFile $envPath -ProjectName $projectName -ReportPath $firstReport -Deadline $deadline -InitializeEmptyCatalog
        $firstBootstrap = Get-AcceptanceBootstrapEvidence -Path $firstReport -ExpectedJobAction 'submitted' -ExpectedCatalogAction 'already_registered'
        $firstCheckpoint = Get-AcceptanceCheckpoint -FlinkPort ([int]$ports['FLINK_REST_PORT']) -Deadline $deadline
        $report.cold_start = 'passed'; $report.evidence.cold_start = [ordered]@{ bootstrap = $firstBootstrap; checkpoint = $firstCheckpoint }

        Invoke-AcceptanceBootstrap -RepositoryRoot $repositoryRoot -EnvFile $envPath -ProjectName $projectName -ReportPath $secondReport -Deadline $deadline
        $secondBootstrap = Get-AcceptanceBootstrapEvidence -Path $secondReport -ExpectedJobAction 'no_op' `
            -ExpectedCatalogAction 'already_registered' -RequireInitializationNoOp
        $secondCheckpoint = Get-AcceptanceCheckpoint -FlinkPort ([int]$ports['FLINK_REST_PORT']) -Deadline $deadline
        if ($secondBootstrap.job_id -cne $firstBootstrap.job_id -or $secondCheckpoint.job_id -cne $firstCheckpoint.job_id) { throw 'Second Bootstrap was not idempotent.' }
        $report.idempotent_second_run = 'passed'; $report.evidence.idempotent_second_run = [ordered]@{ bootstrap = $secondBootstrap; checkpoint = $secondCheckpoint }

        $rowInitial = [int64](Invoke-AcceptanceTrinoScalar -ComposePrefix $composePrefix -Sql 'SELECT count(*) FROM lakehouse.analytics.user_behavior_detail' -Deadline $deadline)
        Invoke-AcceptanceTrinoScalar -ComposePrefix $composePrefix -Sql "INSERT INTO lakehouse.analytics.user_behavior_detail VALUES ('acceptance-$runId','user','product','view','2026-08-27T00:00:00Z','acceptance','test','page')" -Deadline $deadline | Out-Null
        $rowBefore = [int64](Invoke-AcceptanceTrinoScalar -ComposePrefix $composePrefix -Sql 'SELECT count(*) FROM lakehouse.analytics.user_behavior_detail' -Deadline $deadline)
        $snapshotBefore = Invoke-AcceptanceTrinoScalar -ComposePrefix $composePrefix -Sql 'SELECT snapshot_id FROM "user_behavior_detail$snapshots" ORDER BY committed_at DESC LIMIT 1' -Deadline $deadline
        $savepoint = New-AcceptanceSavepoint -ComposePrefix $composePrefix -JobId $secondCheckpoint.job_id -SavepointBase $isolatedEnvironment['CHAPTER9_SAVEPOINT_URI'] -Deadline $deadline
        $transition = Set-AcceptanceRestartRecoveryState -StateRoot $runRoot -RunId $runId -ExpectedJobId $secondCheckpoint.job_id `
            -JobName 'chapter-9-datastream-quality-production' -SavepointPath $savepoint
        Invoke-AcceptanceDocker -Arguments ($composePrefix + @('rm', '-sf', 'hive-metastore', 'trino', 'flink-jobmanager', 'flink-taskmanager', 'flink-sql-client')) -Deadline $deadline -FailureMessage 'Isolated service recreation failed.' | Out-Null
        Invoke-AcceptanceDocker -Arguments ($composePrefix + @('up', '-d', 'hive-metastore', 'trino', 'flink-jobmanager', 'flink-taskmanager', 'flink-sql-client')) -Deadline $deadline -FailureMessage 'Isolated service restart failed.' | Out-Null
        Invoke-AcceptanceBootstrap -RepositoryRoot $repositoryRoot -EnvFile $envPath -ProjectName $projectName -ReportPath $recoveryReport -Deadline $deadline
        $recoveryBootstrap = Get-AcceptanceBootstrapEvidence -Path $recoveryReport -ExpectedJobAction 'submitted' `
            -ExpectedCatalogAction 'already_registered' -RequireInitializationNoOp
        $recoveryCheckpoint = Get-AcceptanceCheckpoint -FlinkPort ([int]$ports['FLINK_REST_PORT']) -Deadline $deadline
        $rowAfter = [int64](Invoke-AcceptanceTrinoScalar -ComposePrefix $composePrefix -Sql 'SELECT count(*) FROM lakehouse.analytics.user_behavior_detail' -Deadline $deadline)
        $snapshotAfter = Invoke-AcceptanceTrinoScalar -ComposePrefix $composePrefix -Sql 'SELECT snapshot_id FROM "user_behavior_detail$snapshots" ORDER BY committed_at DESC LIMIT 1' -Deadline $deadline
        if ($recoveryBootstrap.job_id -ceq $secondBootstrap.job_id -or $recoveryCheckpoint.checkpoint_id -le $secondCheckpoint.checkpoint_id) { throw 'Restart recovery did not advance checkpoint state.' }
        $continuity = Assert-AcceptanceContinuity -RowCountInitial $rowInitial -RowCountBeforeRestart $rowBefore `
            -RowCountAfterRestart $rowAfter -SnapshotBeforeRestart ([string]$snapshotBefore) -SnapshotAfterRestart ([string]$snapshotAfter)
        $report.restart_recovery = 'passed'; $report.data_continuity = 'passed'
        $report.evidence.restart_recovery = [ordered]@{ bootstrap = $recoveryBootstrap; checkpoint_before = $secondCheckpoint; checkpoint_after = $recoveryCheckpoint; savepoint_path = $savepoint; transition = $transition }
        $report.evidence.data_continuity = $continuity

        $timeoutSeconds = [Math]::Max(1, [Math]::Min(20, [Math]::Floor((Get-AcceptanceRemainingMilliseconds -Deadline $deadline) / 1000)))
        $ready = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$($ports['API_PORT'])/ready" -TimeoutSec $timeoutSeconds
        if ($ready.StatusCode -ne 200) { throw 'Isolated readiness failed.' }
        $report.readiness = 'passed'; $report.evidence.readiness = [ordered]@{ status_code = $ready.StatusCode; checked_at_utc = [DateTimeOffset]::UtcNow.ToString('o') }
        Invoke-AcceptanceProcess -FilePath 'powershell' -Arguments @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', (Join-Path $repositoryRoot 'scripts/verify_chapter_10_tool_analysis.ps1'), '-AnalysisBaseUrl', "http://127.0.0.1:$($ports['API_PORT'])") -Deadline $deadline -FailureMessage 'Isolated tool analysis failed.' | Out-Null
        $report.tool_analysis = 'passed'; $report.evidence.tool_analysis = [ordered]@{ status = 'passed'; checked_at_utc = [DateTimeOffset]::UtcNow.ToString('o') }
        $completed = $true
    } catch {
        $report.error = $_.Exception.Message
    } finally {
        if ($null -ne $previousEnvironment) { Restore-AcceptanceEnvironment -Previous $previousEnvironment; $previousEnvironment = $null }
        try {
            $report.default_project_after = Get-AcceptanceDefaultProjectState -RepositoryRoot $repositoryRoot -DefaultEnvPath $defaultEnvPath -Deadline $deadline
            if ($null -ne $report.default_project_before -and -not (Test-AcceptanceStateEqual -Before $report.default_project_before -After $report.default_project_after)) {
                throw 'Default project shared state changed during isolated acceptance.'
            }
            if ($completed -and -not $KeepOnFailure) {
                $cleanupPrevious = Set-AcceptanceEnvironment -Values $isolatedEnvironment
                try { Remove-AcceptanceOwnedResources -ComposePrefix $composePrefix -ProjectName $projectName -AcceptanceRoot $acceptanceRoot -RunRoot $runRoot -RunId $runId -Deadline $deadline }
                finally { Restore-AcceptanceEnvironment -Previous $cleanupPrevious }
                $report.cleanup = 'passed'
            } else { $report.cleanup = 'preserved' }
        } catch {
            if ($null -eq $report.error) { $report.error = $_.Exception.Message }
            $report.cleanup = 'preserved'; $completed = $false
        }
        $report.status = if ($completed) { 'passed' } else { 'failed' }
        [System.IO.Directory]::CreateDirectory((Split-Path -Parent $reportPath)) | Out-Null
        [System.IO.File]::WriteAllText($reportPath, ($report | ConvertTo-Json -Depth 30), [System.Text.UTF8Encoding]::new($false))
    }
    if (-not $completed) { throw "Chapter 10.5 isolated cold-start acceptance failed. Report: $reportPath" }
    return $reportPath
}

if ($FunctionsOnly) { return }

Invoke-Chapter105ColdStartVerification -KeepOnFailure:$KeepOnFailure -TimeoutMinutes $TimeoutMinutes -DefaultEnvFile $DefaultEnvFile
