[CmdletBinding()]
param(
    [string]$EnvFile,
    [string]$ComposeProjectName,
    [switch]$FunctionsOnly
)

$script:DorisRealtimeMetricsDefaultEnvFile = Join-Path $PSScriptRoot '..\infra\.env.example'
if ([string]::IsNullOrWhiteSpace($EnvFile)) {
    $EnvFile = $script:DorisRealtimeMetricsDefaultEnvFile
}

function Invoke-CheckedCommand {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Command,
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )

    & $Command
    if ($LASTEXITCODE -ne 0) { throw $FailureMessage }
}

function Assert-DockerAvailable {
    Invoke-CheckedCommand -Command { docker version } `
        -FailureMessage 'Docker daemon is not available. Please start Docker Desktop and retry.'
}

function Get-DorisRealtimeMetricsComposePrefix {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string]$EnvFile,
        [string]$ComposeProjectName
    )

    if (-not [string]::IsNullOrWhiteSpace($ComposeProjectName) -and
        $ComposeProjectName -cnotmatch '^[a-z0-9][a-z0-9_-]*$') {
        throw 'Compose project identity is unsafe.'
    }
    $prefix = @('compose')
    if (-not [string]::IsNullOrWhiteSpace($ComposeProjectName)) {
        $prefix += @('--project-name', $ComposeProjectName)
    }
    return @($prefix + @(
        '--env-file', ([System.IO.Path]::GetFullPath($EnvFile)),
        '-f', (Join-Path $RepositoryRoot 'infra/docker-compose.yml'),
        '--profile', 'serving'
    ))
}

function Invoke-DorisRealtimeMetricsInitialization {
    param(
        [string]$EnvFile,
        [string]$ComposeProjectName
    )

    $ErrorActionPreference = 'Stop'
    if ([string]::IsNullOrWhiteSpace($EnvFile)) {
        $EnvFile = $script:DorisRealtimeMetricsDefaultEnvFile
    }
    $repositoryRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
    $sqlFile = Join-Path $repositoryRoot 'infra/compose/doris/init/01_create_realtime_metrics.sql'
    $composePrefix = Get-DorisRealtimeMetricsComposePrefix -RepositoryRoot $repositoryRoot `
        -EnvFile $EnvFile -ComposeProjectName $ComposeProjectName

    Assert-DockerAvailable
    Invoke-CheckedCommand -Command { docker @composePrefix up -d --quiet-pull doris-fe doris-be } `
        -FailureMessage 'Failed to start Doris FE/BE containers.'

    Write-Host '[chapter4] waiting for Doris FE query port...'
    $isReady = $false
    for ($attempt = 1; $attempt -le 30; $attempt++) {
        & docker @composePrefix exec -T doris-fe sh -lc "mysql -uroot -h127.0.0.1 -P9030 -e 'SELECT 1'" |
            Out-Null
        if ($LASTEXITCODE -eq 0) {
            $isReady = $true
            break
        }
        Start-Sleep -Seconds 2
    }
    if (-not $isReady) { throw 'Doris FE query port did not become ready in time.' }

    Write-Host '[chapter4] waiting for Doris BE heartbeat...'
    $isBackendReady = $false
    for ($attempt = 1; $attempt -le 30; $attempt++) {
        $backendStatus = & docker @composePrefix exec -T doris-fe sh -lc `
            "mysql -uroot -h127.0.0.1 -P9030 -N -e 'SHOW BACKENDS'" 2>$null
        if ($LASTEXITCODE -eq 0 -and (($backendStatus -join "`n") -match "`ttrue`t")) {
            $isBackendReady = $true
            break
        }
        Start-Sleep -Seconds 2
    }
    if (-not $isBackendReady) { throw 'Doris BE did not become alive in time.' }

    Write-Host '[chapter4] initializing analytics.realtime_metrics...'
    Get-Content -Raw $sqlFile | & docker @composePrefix exec -T doris-fe sh -lc `
        'mysql -uroot -h127.0.0.1 -P9030'
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to initialize analytics.realtime_metrics in Doris.'
    }
    Write-Host '[chapter4] Doris table is ready.'
}

if ($FunctionsOnly) { return }

Invoke-DorisRealtimeMetricsInitialization -EnvFile $EnvFile `
    -ComposeProjectName $ComposeProjectName
