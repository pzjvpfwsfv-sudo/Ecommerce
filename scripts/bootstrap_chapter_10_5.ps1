[CmdletBinding()]
param(
    [string]$EnvFile = 'infra/.env',
    [switch]$SkipBuild,
    [string]$ReportPath = 'tmp/chapter-10-5/bootstrap-report.json',
    [switch]$FunctionsOnly
)

Set-StrictMode -Version Latest

Import-Module (Join-Path $PSScriptRoot 'lib\Chapter105.Common.psm1') -Force

function Get-Chapter105FlinkJobDecision {
    param(
        [Parameter(Mandatory = $true)][object]$Overview,
        [Parameter(Mandatory = $true)][string]$JobName
    )

    $jobsProperty = @($Overview.PSObject.Properties | Where-Object { $_.Name -ceq 'jobs' })
    if ($Overview -isnot [System.Management.Automation.PSCustomObject] -or $jobsProperty.Count -ne 1 -or
        $jobsProperty[0].Value -isnot [System.Collections.IEnumerable]) {
        throw 'Flink jobs overview has an invalid structure.'
    }
    $matches = @($jobsProperty[0].Value | Where-Object {
        $_ -is [System.Management.Automation.PSCustomObject] -and [string]$_.name -ceq $JobName
    })
    if ($matches.Count -eq 0) { return [pscustomobject]@{ action = 'submit'; job_id = $null } }
    if ($matches.Count -ne 1) { throw 'Flink job identity is ambiguous.' }
    $job = $matches[0]
    if ([string]$job.state -cne 'RUNNING' -or [string]$job.jid -cnotmatch '^[0-9a-f]{32}$') {
        throw 'Flink job identity is not a unique RUNNING job.'
    }
    return [pscustomobject]@{ action = 'no_op'; job_id = [string]$job.jid }
}

function Invoke-Chapter105FlinkOverview {
    param([Parameter(Mandatory = $true)][int]$Port)

    return Invoke-Chapter105FlinkResource -Port $Port -Resource '/jobs/overview'
}

function Invoke-Chapter105FlinkResource {
    param([Parameter(Mandatory = $true)][int]$Port, [Parameter(Mandatory = $true)][string]$Resource)

    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:$Port$Resource" -TimeoutSec 5 -ErrorAction Stop
        if ($response.StatusCode -ne 200) { throw 'unexpected status' }
        return $response.Content | ConvertFrom-Json -ErrorAction Stop
    } catch {
        throw 'Flink REST request failed.'
    }
}

function New-Chapter105BootstrapReport {
    $stages = [ordered]@{}
    foreach ($name in @('preflight', 'dependencies', 'infrastructure', 'initialization', 'catalog', 'jobs', 'acceptance')) {
        $stages[$name] = [ordered]@{ started_at = $null; completed_at = $null; status = 'pending'; details = @{} }
    }
    return [ordered]@{ status = 'running'; started_at = [DateTimeOffset]::UtcNow.ToString('o'); completed_at = $null; stages = $stages }
}

function Invoke-Chapter105BootstrapStage {
    param(
        [Parameter(Mandatory = $true)][object]$Report,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][scriptblock]$Action
    )

    $stage = $Report.stages[$Name]
    $stage.started_at = [DateTimeOffset]::UtcNow.ToString('o')
    $stage.status = 'running'
    try {
        $details = & $Action
        $stage.details = if ($null -eq $details) { @{} } else { $details }
        $stage.status = 'passed'
    } catch {
        $stage.details = @{ error = 'stage failed' }
        $stage.status = 'failed'
        throw
    } finally {
        $stage.completed_at = [DateTimeOffset]::UtcNow.ToString('o')
    }
}

function Assert-Chapter105Preflight {
    param([Parameter(Mandatory = $true)][string]$RepositoryRoot, [Parameter(Mandatory = $true)][System.Collections.IDictionary]$Environment)

    foreach ($key in @('FLINK_REST_PORT', 'API_PORT', 'MINIO_API_PORT', 'TRINO_PORT', 'DORIS_FE_QUERY_PORT')) {
        if (-not $Environment.ContainsKey($key) -or [int]$Environment[$key] -lt 1 -or [int]$Environment[$key] -gt 65535) {
            throw 'Environment port configuration is invalid.'
        }
    }
    foreach ($key in @('MINIO_ROOT_USER', 'MINIO_ROOT_PASSWORD', 'DORIS_DATABASE', 'DORIS_TABLE_REALTIME_METRICS', 'CHAPTER9_CHECKPOINT_URI', 'FLINK_CHECKPOINT_MAX_AGE_SECONDS')) {
        if (-not $Environment.ContainsKey($key) -or [string]::IsNullOrWhiteSpace([string]$Environment[$key])) {
            throw 'Environment required configuration is missing.'
        }
    }
    if ([int]$Environment['FLINK_CHECKPOINT_MAX_AGE_SECONDS'] -lt 1) { throw 'Flink checkpoint age configuration is invalid.' }
    Invoke-Chapter105Native -FilePath 'docker' -Arguments @('version') -FailureMessage 'Docker daemon is unavailable.' | Out-Null
    Invoke-Chapter105Native -FilePath 'docker' -Arguments @('compose', 'version') -FailureMessage 'Docker Compose is unavailable.' | Out-Null
    if ($PSVersionTable.PSVersion.Major -lt 5) { throw 'PowerShell version is unsupported.' }
    $java = Invoke-Chapter105Native -FilePath 'java' -Arguments @('-version') -FailureMessage 'Java 17 is unavailable.'
    if (($java -join "`n") -notmatch '(?m)(?:version )?"?17(?:\.|\s|$)') { throw 'Java 17 is unavailable.' }
    Invoke-Chapter105Native -FilePath 'mvn' -Arguments @('-version') -FailureMessage 'Maven is unavailable.' | Out-Null
    Invoke-Chapter105Native -FilePath 'python' -Arguments @('--version') -FailureMessage 'Python is unavailable.' | Out-Null
    $minioDataPath = Get-Chapter105StableMinioDataPath -RepositoryRoot $RepositoryRoot
    [System.IO.Directory]::CreateDirectory($minioDataPath) | Out-Null
    $probePath = Join-Path $minioDataPath ".bootstrap-write-$([Guid]::NewGuid().ToString('N'))"
    try { [System.IO.File]::WriteAllText($probePath, '') } finally { if (Test-Path -LiteralPath $probePath) { Remove-Item -LiteralPath $probePath -Force } }
    return @{ repository_root = $RepositoryRoot; minio_data_dir = $minioDataPath }
}

function Wait-Chapter105ComposeReady {
    param([Parameter(Mandatory = $true)][string[]]$ComposePrefix)

    Invoke-Chapter105Retry -Attempts 30 -SleepSeconds 2 -FailureMessage 'Compose services did not become ready.' -Action {
        $raw = Invoke-Chapter105Native -FilePath 'docker' -Arguments ($ComposePrefix + @('ps', '--format', 'json')) -FailureMessage 'Compose status query failed.'
        $services = @($raw | ForEach-Object { $_ | ConvertFrom-Json -ErrorAction Stop })
        $byService = @{}
        foreach ($service in $services) { $byService[[string]$service.Service] = $service }
        foreach ($serviceName in @('minio', 'minio-init', 'flink-jobmanager', 'flink-taskmanager', 'doris-fe', 'doris-be', 'trino')) {
            if (-not $byService.ContainsKey($serviceName)) { throw 'required service is absent' }
        }
        if ([string]$byService['minio'].Health -ne 'healthy' -or [string]$byService['minio-init'].State -ne 'exited' -or
            [int]$byService['minio-init'].ExitCode -ne 0) { throw 'services are not ready' }
        return @{ services = $byService.Count }
    }
}

function Invoke-Chapter105JobSubmission {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string[]]$ComposePrefix,
        [Parameter(Mandatory = $true)][string]$CheckpointUri,
        [switch]$SkipBuild
    )

    if (-not $SkipBuild) {
        Invoke-Chapter105Native -FilePath 'mvn' -Arguments @('-f', (Join-Path $RepositoryRoot 'jobs\datastream-quality\pom.xml'), '-DskipTests', 'package') -FailureMessage 'Flink job build failed.' | Out-Null
    }
    Invoke-Chapter105Native -FilePath 'docker' -Arguments ($ComposePrefix + @(
        'exec', '-T', 'flink-jobmanager', '/opt/flink/bin/flink', 'run', '-d',
        '-c', 'com.ecommerce.quality.DataQualityJob',
        '/workspace/jobs/datastream-quality/target/datastream-quality-1.0.0.jar',
        '--bootstrap-servers', 'kafka:29092', '--input-topic', 'user_behavior_events', '--mode', 'production',
        '--consumer-group', 'chapter9-quality-production', '--checkpoint-uri', $CheckpointUri,
        '--transaction-prefix', 'chapter9-production', '--job-version', 'chapter-9-v1'
    )) -FailureMessage 'Flink job submission failed.' | Out-Null
}

function Assert-Chapter105FreshCheckpoint {
    param(
        [Parameter(Mandatory = $true)][object]$Checkpoints,
        [Parameter(Mandatory = $true)][int]$MaxAgeSeconds
    )

    $counts = $Checkpoints.PSObject.Properties['counts'].Value
    $latest = $Checkpoints.PSObject.Properties['latest'].Value
    if ($counts -isnot [System.Management.Automation.PSCustomObject] -or $latest -isnot [System.Management.Automation.PSCustomObject] -or
        $counts.PSObject.Properties['completed'].Value -isnot [System.ValueType] -or $latest.PSObject.Properties['completed'].Value -isnot [System.Management.Automation.PSCustomObject]) {
        throw 'Flink checkpoint response has an invalid structure.'
    }
    $completed = [int64]$counts.PSObject.Properties['completed'].Value
    $timestamp = [int64]$latest.PSObject.Properties['completed'].Value.PSObject.Properties['latest_ack_timestamp'].Value
    $checkpointTime = [DateTimeOffset]::FromUnixTimeMilliseconds($timestamp)
    $age = [DateTimeOffset]::UtcNow - $checkpointTime
    if ($completed -lt 1 -or $age.TotalSeconds -lt 0 -or $age.TotalSeconds -gt $MaxAgeSeconds) {
        throw 'Flink checkpoint is not fresh.'
    }
}

function Invoke-Chapter105Acceptance {
    param(
        [Parameter(Mandatory = $true)][int]$ApiPort,
        [Parameter(Mandatory = $true)][int]$FlinkPort,
        [Parameter(Mandatory = $true)][string]$JobName,
        [Parameter(Mandatory = $true)][int]$CheckpointMaxAgeSeconds,
        [Parameter(Mandatory = $true)][string[]]$ComposePrefix
    )

    try {
        $ready = Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:$ApiPort/ready" -TimeoutSec 5 -ErrorAction Stop
        if ($ready.StatusCode -ne 200) { throw 'not ready' }
        $payload = $ready.Content | ConvertFrom-Json -ErrorAction Stop
        if ([string]$payload.status -cne 'ready') { throw 'not ready' }
    } catch { throw 'Readiness acceptance failed.' }
    $decision = Get-Chapter105FlinkJobDecision -Overview (Invoke-Chapter105FlinkOverview -Port $FlinkPort) -JobName $JobName
    if ($decision.action -ne 'no_op') { throw 'Flink job acceptance failed.' }
    $doris = Invoke-Chapter105Native -FilePath 'docker' -Arguments ($ComposePrefix + @(
        'exec', '-T', 'doris-fe', 'mysql', '-h127.0.0.1', '-P9030', '-uroot', '-N', '-e',
        'SELECT COUNT(*) FROM analytics.realtime_metrics;'
    )) -FailureMessage 'Doris metrics acceptance failed.'
    if ($doris.Count -ne 1 -or ($doris[0].Trim() -notmatch '^[0-9]+$')) { throw 'Doris metrics acceptance failed.' }
    $trino = Invoke-Chapter105Native -FilePath 'docker' -Arguments ($ComposePrefix + @(
        'exec', '-T', 'trino', 'trino', '--server', 'http://localhost:8080', '--catalog', 'lakehouse', '--schema', 'analytics',
        '--output-format', 'CSV_HEADER', '--execute', 'SELECT COUNT(*) AS event_count FROM lakehouse.analytics.user_behavior_detail;'
    )) -FailureMessage 'Trino fixed table acceptance failed.'
    $trinoLines = @($trino | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($trinoLines.Count -ne 2 -or $trinoLines[0] -cne 'event_count' -or $trinoLines[1] -notmatch '^[0-9]+$') { throw 'Trino fixed table acceptance failed.' }
    Assert-Chapter105FreshCheckpoint -Checkpoints (Invoke-Chapter105FlinkResource -Port $FlinkPort -Resource "/jobs/$($decision.job_id)/checkpoints") -MaxAgeSeconds $CheckpointMaxAgeSeconds
    & (Join-Path $PSScriptRoot 'verify_chapter_10_tool_analysis.ps1') -AnalysisBaseUrl "http://localhost:$ApiPort" | Out-Null
    return @{ job_id = $decision.job_id; ready = $true; tools = 'rule_based' }
}

if ($FunctionsOnly) { return }

$ErrorActionPreference = 'Stop'
$report = New-Chapter105BootstrapReport
try {
    $repositoryRoot = Get-Chapter105PrimaryRepositoryRoot -StartPath $PSScriptRoot
    $envPath = if ([System.IO.Path]::IsPathRooted($EnvFile)) { $EnvFile } else { Join-Path $repositoryRoot $EnvFile }
    $environment = Read-Chapter105EnvFile -Path $envPath
    $environment['MINIO_DATA_DIR'] = Get-Chapter105StableMinioDataPath -RepositoryRoot $repositoryRoot
    $env:MINIO_DATA_DIR = $environment['MINIO_DATA_DIR']
    $composePrefix = @('compose', '--env-file', $envPath, '-f', (Join-Path $repositoryRoot 'infra\docker-compose.yml'), '--profile', 'flink', '--profile', 'serving', '--profile', 'lakehouse')
    $jobName = 'chapter-9-datastream-quality-production'

    Invoke-Chapter105BootstrapStage -Report $report -Name 'preflight' -Action { Assert-Chapter105Preflight -RepositoryRoot $repositoryRoot -Environment $environment }
    Invoke-Chapter105BootstrapStage -Report $report -Name 'dependencies' -Action {
        Invoke-Chapter105Native -FilePath 'powershell' -Arguments @('-NoProfile', '-File', (Join-Path $PSScriptRoot 'install_runtime_dependencies.ps1'), '-RepositoryRoot', $repositoryRoot) -FailureMessage 'Runtime dependency installation failed.' | Out-Null
        @{ installer = 'completed' }
    }
    Invoke-Chapter105BootstrapStage -Report $report -Name 'infrastructure' -Action {
        Invoke-Chapter105Native -FilePath 'docker' -Arguments ($composePrefix + @('up', '-d')) -FailureMessage 'Infrastructure startup failed.' | Out-Null
        Wait-Chapter105ComposeReady -ComposePrefix $composePrefix
    }
    Invoke-Chapter105BootstrapStage -Report $report -Name 'initialization' -Action {
        Invoke-Chapter105Native -FilePath 'docker' -Arguments ($composePrefix + @('exec', '-T', 'kafka', 'kafka-topics', '--bootstrap-server', 'kafka:29092', '--create', '--if-not-exists', '--topic', 'user_behavior_events', '--partitions', '1', '--replication-factor', '1')) -FailureMessage 'Kafka topic initialization failed.' | Out-Null
        Invoke-Chapter105Native -FilePath 'powershell' -Arguments @('-NoProfile', '-File', (Join-Path $PSScriptRoot 'init_doris_realtime_metrics.ps1')) -FailureMessage 'Doris initialization failed.' | Out-Null
        @{ topic = 'user_behavior_events'; doris = 'initialized'; minio_buckets = 'compose-init' }
    }
    Invoke-Chapter105BootstrapStage -Report $report -Name 'catalog' -Action {
        Invoke-Chapter105Native -FilePath 'powershell' -Arguments @('-NoProfile', '-File', (Join-Path $PSScriptRoot 'restore_chapter_10_5_catalog.ps1')) -FailureMessage 'Catalog recovery failed.' | Out-Null
        @{ catalog = 'recovered' }
    }
    Invoke-Chapter105BootstrapStage -Report $report -Name 'jobs' -Action {
        . (Join-Path $PSScriptRoot 'run_chapter_9_production_cutover.ps1') -FunctionsOnly
        $decision = Get-Chapter105FlinkJobDecision -Overview (Invoke-Chapter105FlinkOverview -Port ([int]$environment['FLINK_REST_PORT'])) -JobName $jobName
        if ($decision.action -eq 'submit') {
            $checkpointUri = Get-Chapter9StateUri -Kind 'checkpoint'
            Invoke-Chapter105JobSubmission -RepositoryRoot $repositoryRoot -ComposePrefix $composePrefix -CheckpointUri $checkpointUri -SkipBuild:$SkipBuild
            $decision = Invoke-Chapter105Retry -Attempts 30 -SleepSeconds 2 -FailureMessage 'Flink job did not reach a unique RUNNING state.' -Action {
                $current = Get-Chapter105FlinkJobDecision -Overview (Invoke-Chapter105FlinkOverview -Port ([int]$environment['FLINK_REST_PORT'])) -JobName $jobName
                if ($current.action -ne 'no_op') { throw 'job pending' }
                $current
            }
        }
        @{ job_id = $decision.job_id; action = $decision.action }
    }
    Invoke-Chapter105BootstrapStage -Report $report -Name 'acceptance' -Action {
        Invoke-Chapter105Acceptance -ApiPort ([int]$environment['API_PORT']) -FlinkPort ([int]$environment['FLINK_REST_PORT']) `
            -JobName $jobName -CheckpointMaxAgeSeconds ([int]$environment['FLINK_CHECKPOINT_MAX_AGE_SECONDS']) -ComposePrefix $composePrefix
    }
    $report.status = 'passed'
} catch {
    $report.status = 'failed'
} finally {
    $report.completed_at = [DateTimeOffset]::UtcNow.ToString('o')
    Write-Chapter105BootstrapReport -Report $report -Path $ReportPath
}

if ($report.status -ne 'passed') { throw 'Chapter 10.5 bootstrap failed. See the bootstrap report for safe stage status.' }
