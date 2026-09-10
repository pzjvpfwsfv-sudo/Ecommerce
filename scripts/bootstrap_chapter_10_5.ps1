[CmdletBinding()]
param(
    [string]$EnvFile = 'infra/.env',
    [switch]$SkipBuild,
    [string]$ReportPath = 'tmp/chapter-10-5/bootstrap-report.json',
    [string]$ComposeProjectName,
    [switch]$InitializeEmptyCatalog,
    [switch]$IsolatedAcceptance,
    [string]$DeadlineUtc,
    [switch]$FunctionsOnly
)

$chapter105BeforeModules = @(Get-Module)
$chapter105CommonModule = Import-Module ([System.IO.Path]::Combine($PSScriptRoot, 'lib', 'Chapter105.Common.psm1')) -PassThru
$chapter105PublicFunctions = @(
    'Read-Chapter105EnvFile',
    'Get-Chapter105PrimaryRepositoryRoot',
    'Get-Chapter105StableMinioDataPath',
    'Resolve-Chapter105RepositoryPath',
    'Invoke-Chapter105Native',
    'Invoke-Chapter105Retry',
    'ConvertTo-Chapter105RedactedValue',
    'Write-Chapter105BootstrapReport'
)
$chapter105Definitions = @{}
foreach ($chapter105Name in $chapter105PublicFunctions) {
    if (-not $chapter105CommonModule.ExportedFunctions.ContainsKey($chapter105Name)) {
        throw 'Chapter 10.5 Common module test interface is incomplete.'
    }
    $chapter105Definitions[$chapter105Name] = $chapter105CommonModule.ExportedFunctions[$chapter105Name].ScriptBlock
}
if ($chapter105BeforeModules -notcontains $chapter105CommonModule) {
    Remove-Module $chapter105CommonModule -Force
}
foreach ($chapter105Name in $chapter105PublicFunctions) {
    Set-Item -LiteralPath "Function:$chapter105Name" -Value $chapter105Definitions[$chapter105Name]
}
foreach ($chapter105LoadedModule in @(Get-Module)) {
    if ($chapter105BeforeModules -notcontains $chapter105LoadedModule) {
        Remove-Module $chapter105LoadedModule -Force
    }
}
foreach ($chapter105Variable in @(
        'chapter105BeforeModules', 'chapter105CommonModule', 'chapter105PublicFunctions',
        'chapter105Definitions', 'chapter105Name', 'chapter105LoadedModule', 'chapter105Variable'
    )) {
    $ExecutionContext.SessionState.PSVariable.Remove($chapter105Variable)
}

function Get-Chapter105FlinkJobDecision {
    param(
        [Parameter(Mandatory = $true)][object]$Overview,
        [Parameter(Mandatory = $true)][string]$JobName
    )

    if ($Overview -isnot [System.Management.Automation.PSCustomObject]) {
        throw 'Flink jobs overview has an invalid structure.'
    }
    $jobsProperty = @($Overview.PSObject.Properties | Where-Object { $_.Name -ceq 'jobs' })
    if ($jobsProperty.Count -ne 1 -or $jobsProperty[0].Value -isnot [System.Collections.IList] -or
        $jobsProperty[0].Value -is [string]) {
        throw 'Flink jobs overview has an invalid structure.'
    }
    $validStates = @('CREATED', 'RUNNING', 'FAILING', 'FAILED', 'CANCELLING', 'CANCELED', 'FINISHED', 'RESTARTING', 'SUSPENDED', 'RECONCILING', 'INITIALIZING')
    $seenIds = @{}
    $targetJobs = @()
    foreach ($job in @($jobsProperty[0].Value)) {
        if ($job -isnot [System.Management.Automation.PSCustomObject]) { throw 'Flink job entry has an invalid structure.' }
        $jid = $job.PSObject.Properties['jid'].Value
        $name = $job.PSObject.Properties['name'].Value
        $state = $job.PSObject.Properties['state'].Value
        if ($jid -isnot [string] -or $jid -cnotmatch '^[0-9a-f]{32}$' -or
            $name -isnot [string] -or [string]::IsNullOrWhiteSpace($name) -or
            $state -isnot [string] -or $validStates -cnotcontains $state -or $seenIds.ContainsKey($jid)) {
            throw 'Flink job entry has an invalid structure.'
        }
        $seenIds[$jid] = $true
        if ($name -ceq $JobName) { $targetJobs += $job }
    }
    if ($targetJobs.Count -eq 0) { return [pscustomobject]@{ action = 'submit'; job_id = $null } }
    if ($targetJobs.Count -ne 1) { throw 'Flink job identity is ambiguous.' }
    $job = $targetJobs[0]
    if ([string]$job.state -cne 'RUNNING' -or [string]$job.jid -cnotmatch '^[0-9a-f]{32}$') {
        throw 'Flink job identity is not a unique RUNNING job.'
    }
    return [pscustomobject]@{ action = 'no_op'; job_id = [string]$job.jid }
}

function Invoke-Chapter105EnsureJob {
    param(
        [Parameter(Mandatory = $true)][object]$Overview,
        [Parameter(Mandatory = $true)][string]$JobName,
        [Parameter(Mandatory = $true)][scriptblock]$SubmitAction
    )

    $decision = Get-Chapter105FlinkJobDecision -Overview $Overview -JobName $JobName
    if ($decision.action -eq 'submit') { return & $SubmitAction }
    return $decision
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

function Assert-Chapter105JobArtifact {
    param([Parameter(Mandatory = $true)][string]$RepositoryRoot)

    $safeError = 'Flink job artifact is invalid.'
    try {
        $root = [System.IO.Path]::GetFullPath($RepositoryRoot).TrimEnd('\', '/')
        if (-not (Test-Path -LiteralPath $root -PathType Container)) { throw $safeError }

        $artifact = [System.IO.Path]::GetFullPath((Join-Path $root `
                    'jobs\datastream-quality\target\datastream-quality-1.0.0.jar'))
        $rootPrefix = $root + [System.IO.Path]::DirectorySeparatorChar
        if (-not $artifact.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw $safeError
        }

        $current = $root
        $rootItem = Get-Item -LiteralPath $current -Force
        if (-not $rootItem.PSIsContainer -or
            ($rootItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw $safeError
        }
        foreach ($segment in $artifact.Substring($root.Length).Split(
                @('\', '/'), [System.StringSplitOptions]::RemoveEmptyEntries)) {
            $current = Join-Path $current $segment
            if (-not (Test-Path -LiteralPath $current)) { throw $safeError }
            $item = Get-Item -LiteralPath $current -Force
            if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw $safeError
            }
        }
        if ($item.PSIsContainer -or [int64]$item.Length -le 0 -or
            -not [System.IO.Path]::GetFullPath($item.FullName).Equals(
                $artifact, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw $safeError
        }
        return $artifact
    } catch {
        throw $safeError
    }
}

function Assert-Chapter105Preflight {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$Environment,
        [Parameter(Mandatory = $true)][string]$MinioDataPath,
        [switch]$SkipBuild
    )

    foreach ($key in @('FLINK_REST_PORT', 'API_PORT', 'MINIO_API_PORT', 'TRINO_PORT', 'DORIS_FE_QUERY_PORT')) {
        if (-not $Environment.Contains($key) -or [int]$Environment[$key] -lt 1 -or [int]$Environment[$key] -gt 65535) {
            throw 'Environment port configuration is invalid.'
        }
    }
    foreach ($key in @('MINIO_ROOT_USER', 'MINIO_ROOT_PASSWORD', 'DORIS_DATABASE', 'DORIS_TABLE_REALTIME_METRICS', 'CHAPTER9_CHECKPOINT_URI', 'CHAPTER9_SAVEPOINT_URI', 'FLINK_CHECKPOINT_MAX_AGE_SECONDS')) {
        if (-not $Environment.Contains($key) -or [string]::IsNullOrWhiteSpace([string]$Environment[$key])) {
            throw 'Environment required configuration is missing.'
        }
    }
    if ([int]$Environment['FLINK_CHECKPOINT_MAX_AGE_SECONDS'] -lt 1) { throw 'Flink checkpoint age configuration is invalid.' }
    if ($SkipBuild) { Assert-Chapter105JobArtifact -RepositoryRoot $RepositoryRoot | Out-Null }
    Invoke-Chapter105Native -FilePath 'docker' -Arguments @('version') -FailureMessage 'Docker daemon is unavailable.' | Out-Null
    Invoke-Chapter105Native -FilePath 'docker' -Arguments @('compose', 'version') -FailureMessage 'Docker Compose is unavailable.' | Out-Null
    if ($PSVersionTable.PSVersion.Major -lt 5) { throw 'PowerShell version is unsupported.' }
    if (-not $SkipBuild) {
        $java = Invoke-Chapter105Native -FilePath 'java' -Arguments @('-version') -FailureMessage 'Java 17 is unavailable.'
        if (($java -join "`n") -notmatch '(?m)(?:version )?"?17(?:\.|\s|$)') { throw 'Java 17 is unavailable.' }
        Invoke-Chapter105Native -FilePath 'mvn' -Arguments @('-version') -FailureMessage 'Maven is unavailable.' | Out-Null
    }
    Invoke-Chapter105Native -FilePath 'python' -Arguments @('--version') -FailureMessage 'Python is unavailable.' | Out-Null
    $minioDataPath = $MinioDataPath
    [System.IO.Directory]::CreateDirectory($minioDataPath) | Out-Null
    $probePath = Join-Path $minioDataPath ".bootstrap-write-$([Guid]::NewGuid().ToString('N'))"
    try { [System.IO.File]::WriteAllText($probePath, '') } finally { if (Test-Path -LiteralPath $probePath) { Remove-Item -LiteralPath $probePath -Force } }
    return @{ repository_root = $RepositoryRoot; minio_data_dir = $minioDataPath }
}

function Assert-Chapter105ReportPathWritable {
    param([Parameter(Mandatory = $true)][string]$Path)

    $directory = Split-Path -Parent $Path
    [System.IO.Directory]::CreateDirectory($directory) | Out-Null
    $probe = Join-Path $directory ".bootstrap-report-probe-$([Guid]::NewGuid().ToString('N'))"
    try {
        [System.IO.File]::WriteAllText($probe, '', [System.Text.UTF8Encoding]::new($false))
    } catch {
        throw 'Bootstrap report destination is not writable.'
    } finally {
        if (Test-Path -LiteralPath $probe -PathType Leaf) { Remove-Item -LiteralPath $probe -Force }
    }
}

function ConvertFrom-Chapter105ComposePsOutput {
    param([Parameter(Mandatory = $true)][string[]]$Lines)

    if ($Lines.Count -lt 1) { throw 'compose status has an invalid structure' }
    foreach ($line in $Lines) {
        if ([string]::IsNullOrWhiteSpace($line)) { throw 'compose status has an invalid structure' }
        $parsed = $line | ConvertFrom-Json -ErrorAction Stop
        if ($parsed -is [System.Collections.IList] -and $parsed -isnot [string]) {
            foreach ($item in $parsed) {
                if ($item -isnot [System.Management.Automation.PSCustomObject]) {
                    throw 'compose status has an invalid structure'
                }
                Write-Output $item
            }
        } elseif ($parsed -is [System.Management.Automation.PSCustomObject]) {
            Write-Output $parsed
        } else {
            throw 'compose status has an invalid structure'
        }
    }
}

function Wait-Chapter105ComposeReady {
    param(
        [Parameter(Mandatory = $true)][string[]]$ComposePrefix,
        [int]$Attempts = 30,
        [int]$SleepSeconds = 2,
        [DateTimeOffset]$Deadline
    )

    $effectiveAttempts = $Attempts
    if ($PSBoundParameters.ContainsKey('Deadline')) {
        $remainingSeconds = ($Deadline - [DateTimeOffset]::UtcNow).TotalSeconds
        if ($remainingSeconds -le 0) { throw 'Compose services did not become ready.' }
        $pollIntervalSeconds = [Math]::Max(1, $SleepSeconds)
        # The verifier's 45-minute global deadline permits at most 1,350 two-second polls.
        $effectiveAttempts = [Math]::Min(1350, [Math]::Max(1, [Math]::Ceiling($remainingSeconds / $pollIntervalSeconds)))
    }

    Invoke-Chapter105Retry -Attempts ([int]$effectiveAttempts) -SleepSeconds $SleepSeconds -FailureMessage 'Compose services did not become ready.' -Action {
        $raw = Invoke-Chapter105Native -FilePath 'docker' -Arguments ($ComposePrefix + @('ps', '--all', '--format', 'json')) -FailureMessage 'Compose status query failed.'
        $services = @(ConvertFrom-Chapter105ComposePsOutput -Lines $raw)
        $byService = @{}
        foreach ($service in $services) {
            $name = [string]$service.Service
            if ([string]::IsNullOrWhiteSpace($name) -or $byService.ContainsKey($name)) { throw 'compose status is ambiguous' }
            $byService[$name] = $service
        }
        $runningServices = @(
            'kafka-controller', 'kafka-broker', 'api', 'flink-jobmanager', 'flink-taskmanager',
            'flink-sql-client', 'doris-fe', 'doris-be', 'minio', 'metastore-postgres',
            'hive-metastore', 'trino'
        )
        foreach ($serviceName in $runningServices + @('minio-init')) {
            if (-not $byService.ContainsKey($serviceName)) { throw 'required service is absent' }
        }
        foreach ($serviceName in $runningServices) {
            $service = $byService[$serviceName]
            $health = [string]$service.Health
            if ([string]$service.State -ne 'running' -or ($health -and $health -ne 'healthy')) { throw 'services are not ready' }
        }
        if ([string]$byService['minio-init'].State -ne 'exited' -or [int]$byService['minio-init'].ExitCode -ne 0) {
            throw 'services are not ready'
        }
        return @{ services = $byService.Count }
    }
}

function Invoke-Chapter105JobSubmission {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string[]]$ComposePrefix,
        [Parameter(Mandatory = $true)][string]$CheckpointUri,
        [string]$SavepointPath,
        [switch]$SkipBuild
    )

    if (-not $SkipBuild) {
        Invoke-Chapter105Native -FilePath 'mvn' -Arguments @('-f', (Join-Path $RepositoryRoot 'jobs\datastream-quality\pom.xml'), '-DskipTests', 'package') -FailureMessage 'Flink job build failed.' | Out-Null
    }
    $runArguments = @(
        'exec', '-T', 'flink-jobmanager', '/opt/flink/bin/flink', 'run', '-d'
    )
    if ($SavepointPath) {
        $runArguments += @('-s', (Assert-CutoverSavepointPath -Path $SavepointPath))
    }
    $runArguments += @(
        '-c', 'com.ecommerce.quality.DataQualityJob',
        '/workspace/jobs/datastream-quality/target/datastream-quality-1.0.0.jar',
        '--bootstrap-servers', 'kafka:29092', '--input-topic', 'user_behavior_events', '--mode', 'production',
        '--consumer-group', 'chapter9-quality-production', '--checkpoint-uri', $CheckpointUri,
        '--transaction-prefix', 'chapter9-production', '--job-version', 'chapter-9-v1'
    )
    $output = Invoke-Chapter105Native -FilePath 'docker' -Arguments ($ComposePrefix + $runArguments) -FailureMessage 'Flink job submission failed.'
    return Get-SubmittedJobId -Lines $output
}

function Invoke-Chapter105ReconciledJobSubmission {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string[]]$ComposePrefix,
        [Parameter(Mandatory = $true)][string]$CheckpointUri,
        [string]$SavepointPath,
        [Parameter(Mandatory = $true)][int]$FlinkPort,
        [Parameter(Mandatory = $true)][string]$JobName,
        [switch]$SkipBuild
    )

    $submittedJobId = $null
    try {
        $candidate = @(Invoke-Chapter105JobSubmission -RepositoryRoot $RepositoryRoot -ComposePrefix $ComposePrefix `
                -CheckpointUri $CheckpointUri -SavepointPath $SavepointPath -SkipBuild:$SkipBuild)
        if ($candidate.Count -eq 1 -and $candidate[0] -is [string] -and
            [string]$candidate[0] -cmatch '^[0-9a-f]{32}$') {
            $submittedJobId = [string]$candidate[0]
        }
    } catch { }

    $running = Wait-Chapter105UniqueRunningJob -FlinkPort $FlinkPort -JobName $JobName -Attempts 30 -SleepSeconds 1
    if ($running.job_id -isnot [string] -or [string]$running.job_id -cnotmatch '^[0-9a-f]{32}$' -or
        ($null -ne $submittedJobId -and [string]$running.job_id -cne $submittedJobId)) {
        throw 'Flink job submission failed.'
    }
    return [string]$running.job_id
}

function Get-Chapter105Task4RecoveryPlan {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string]$SavepointUri,
        [string]$StateRoot
    )

    $validatedSavepointUri = Get-Chapter9StateUri -Kind 'savepoint' -Environment @{
        CHAPTER9_SAVEPOINT_URI = $SavepointUri
    }
    if ($StateRoot) {
        $restartPath = Join-Path $StateRoot 'tmp\chapter-9\acceptance-restart-recovery.json'
        if (Test-Path -LiteralPath $restartPath -PathType Leaf) {
            try {
                $restart = Get-Content -LiteralPath $restartPath -Raw -Encoding UTF8 | ConvertFrom-Json -ErrorAction Stop
                if ($restart.schema_version -isnot [int] -or [int]$restart.schema_version -ne 1 -or
                    [string]$restart.kind -cne 'chapter10_5_acceptance_restart' -or
                    [string]$restart.job_name -cne 'chapter-9-datastream-quality-production' -or
                    [string]$restart.previous_job_id -cnotmatch '^[0-9a-f]{32}$') {
                    throw 'invalid restart recovery'
                }
                $restartSavepoint = Assert-CutoverSavepointPath -Path ([string]$restart.savepoint_path)
                return [pscustomobject]@{ action = 'restore'; savepoint_path = $restartSavepoint }
            } catch {
                throw 'Task 4 recovery state is invalid.'
            }
        }
        return [pscustomobject]@{ action = 'fresh'; savepoint_path = $null }
    }
    $task4StateRoot = Join-Path $RepositoryRoot 'tmp\chapter-9'
    $finalPath = Join-Path $task4StateRoot 'cutover-manifest.json'
    $partialPath = Join-Path $task4StateRoot 'cutover-manifest.json.partial'
    if (Test-Path -LiteralPath $finalPath -PathType Leaf) {
        throw 'Task 4 recovery state is invalid.'
    }
    if (-not (Test-Path -LiteralPath $partialPath -PathType Leaf)) {
        if (Test-Path -LiteralPath $partialPath) { throw 'Task 4 recovery state is invalid.' }
        return [pscustomobject]@{
            action = 'fresh'; savepoint_path = $null
        }
    }
    try {
        $state = Get-Content -LiteralPath $partialPath -Raw -Encoding UTF8 | ConvertFrom-Json -ErrorAction Stop
        $rawOffsets = $state.PSObject.Properties['raw_offsets'].Value
        if ($state -isnot [System.Management.Automation.PSCustomObject] -or
            $state.schema_version -isnot [int] -or [int]$state.schema_version -ne 2 -or
            $state.cutover_id -isnot [string] -or [string]::IsNullOrWhiteSpace([string]$state.cutover_id) -or
            $state.shadow_job_id -isnot [string] -or [string]$state.shadow_job_id -cnotmatch '^[0-9a-f]{32}$' -or
            $rawOffsets -isnot [System.Collections.IList] -or $rawOffsets -is [string] -or @($rawOffsets).Count -lt 1 -or
            $state.PSObject.Properties['mutations'].Value -isnot [System.Management.Automation.PSCustomObject] -or
            $state.mutations.PSObject.Properties['shadow_stop'].Value -isnot [System.Management.Automation.PSCustomObject]) {
            throw 'invalid state'
        }
        $savepointPath = [string]$state.savepoint_path
        if (-not $savepointPath) { throw 'savepoint is missing' }
        $savepointPath = Assert-CutoverSavepointPath -Path $savepointPath
        if (-not $savepointPath.StartsWith($validatedSavepointUri.TrimEnd('/') + '/', [System.StringComparison]::Ordinal)) {
            throw 'savepoint base mismatch'
        }
    } catch {
        throw 'Task 4 recovery state is invalid.'
    }
    return [pscustomobject]@{
        action = 'restore'; savepoint_path = $savepointPath
    }
}

function Invoke-Chapter105JobsStage {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [string]$StateRoot,
        [Parameter(Mandatory = $true)][object]$Overview,
        [Parameter(Mandatory = $true)][string]$JobName,
        [Parameter(Mandatory = $true)][string]$SavepointUri,
        [switch]$IsolatedAcceptance,
        [Parameter(Mandatory = $true)][scriptblock]$SubmitAction
    )

    $hasStateRoot = -not [string]::IsNullOrWhiteSpace($StateRoot)
    $statePathArguments = @{ RepositoryRoot = $RepositoryRoot }
    if ($hasStateRoot) {
        $statePathArguments['StateRoot'] = $StateRoot
    }
    $statePath = Get-CutoverProductionSubmitStatePath @statePathArguments
    $bootstrapSubmitAction = $SubmitAction
    $boundaryArguments = @{ Path = $statePath; Jobs = $Overview; ExpectedName = $JobName }
    if (-not $IsolatedAcceptance) {
        $boundaryArguments['LegacyStatePath'] = Get-CutoverLegacyProductionSubmitStatePath -RepositoryRoot $RepositoryRoot
    }
    return Invoke-CutoverProductionSubmitBoundary @boundaryArguments -Action {
            $recoveryArguments = @{ RepositoryRoot = $RepositoryRoot; SavepointUri = $SavepointUri }
            if ($hasStateRoot) { $recoveryArguments['StateRoot'] = $StateRoot }
            $recovery = Get-Chapter105Task4RecoveryPlan @recoveryArguments
            return & $bootstrapSubmitAction $recovery.savepoint_path
        }
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
    $completedRecord = $latest.PSObject.Properties['completed'].Value
    if ($completedRecord.PSObject.Properties['status'].Value -cne 'COMPLETED') {
        throw 'Flink checkpoint response has no completed checkpoint.'
    }
    $completed = [int64]$counts.PSObject.Properties['completed'].Value
    $timestamp = [int64]$completedRecord.PSObject.Properties['latest_ack_timestamp'].Value
    $checkpointTime = [DateTimeOffset]::FromUnixTimeMilliseconds($timestamp)
    $age = [DateTimeOffset]::UtcNow - $checkpointTime
    if ($completed -lt 1 -or $age.TotalSeconds -lt 0 -or $age.TotalSeconds -gt $MaxAgeSeconds) {
        throw 'Flink checkpoint is not fresh.'
    }
}

function Wait-Chapter105Ready {
    param(
        [Parameter(Mandatory = $true)][int]$ApiPort,
        [int]$Attempts = 30,
        [int]$SleepSeconds = 2
    )

    Invoke-Chapter105Retry -Attempts $Attempts -SleepSeconds $SleepSeconds -FailureMessage 'Readiness acceptance failed.' -Action {
        $response = Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:$ApiPort/ready" -TimeoutSec 5 -ErrorAction Stop
        if ($response.StatusCode -ne 200) { throw 'not ready' }
        $payload = $response.Content | ConvertFrom-Json -ErrorAction Stop
        if ($payload -isnot [System.Management.Automation.PSCustomObject] -or [string]$payload.status -cne 'ready') {
            throw 'not ready'
        }
        return 'ready'
    }
}

function Wait-Chapter105UniqueRunningJob {
    param(
        [Parameter(Mandatory = $true)][int]$FlinkPort,
        [Parameter(Mandatory = $true)][string]$JobName,
        [int]$Attempts = 30,
        [int]$SleepSeconds = 2
    )

    Invoke-Chapter105Retry -Attempts $Attempts -SleepSeconds $SleepSeconds -FailureMessage 'Flink job did not reach a unique RUNNING state.' -Action {
        $decision = Get-Chapter105FlinkJobDecision -Overview (Invoke-Chapter105FlinkOverview -Port $FlinkPort) -JobName $JobName
        if ($decision.action -ne 'no_op') { throw 'job pending' }
        return $decision
    }
}

function Wait-Chapter105CompletedCheckpoint {
    param(
        [Parameter(Mandatory = $true)][int]$FlinkPort,
        [Parameter(Mandatory = $true)][string]$JobId,
        [Parameter(Mandatory = $true)][int]$MaxAgeSeconds,
        [int]$Attempts = 30,
        [int]$SleepSeconds = 2
    )

    Invoke-Chapter105Retry -Attempts $Attempts -SleepSeconds $SleepSeconds -FailureMessage 'Flink completed checkpoint did not become fresh.' -Action {
        $checkpoints = Invoke-Chapter105FlinkResource -Port $FlinkPort -Resource "/jobs/$JobId/checkpoints"
        Assert-Chapter105FreshCheckpoint -Checkpoints $checkpoints -MaxAgeSeconds $MaxAgeSeconds
        return 'completed'
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

    $null = Wait-Chapter105Ready -ApiPort $ApiPort
    $decision = Wait-Chapter105UniqueRunningJob -FlinkPort $FlinkPort -JobName $JobName
    $doris = Invoke-Chapter105Native -FilePath 'docker' -Arguments ($ComposePrefix + @(
        'exec', '-T', 'doris-fe', 'mysql', '-h127.0.0.1', '-P9030', '-uroot', '-N', '-e',
        'SELECT COUNT(*) FROM analytics.realtime_metrics;'
    )) -FailureMessage 'Doris metrics acceptance failed.'
    if ($doris.Count -ne 1 -or ($doris[0].Trim() -notmatch '^[0-9]+$')) { throw 'Doris metrics acceptance failed.' }
    $trino = Invoke-Chapter105Native -FilePath 'docker' -Arguments ($ComposePrefix + @(
        'exec', '-T', 'trino', 'trino', '--server', 'http://localhost:8080', '--catalog', 'lakehouse', '--schema', 'analytics',
        '--output-format', 'CSV_HEADER_UNQUOTED', '--execute', 'SELECT COUNT(*) AS event_count FROM lakehouse.analytics.user_behavior_detail;'
    )) -FailureMessage 'Trino fixed table acceptance failed.'
    $trinoLines = @($trino | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($trinoLines.Count -ne 2 -or $trinoLines[0] -cne 'event_count' -or $trinoLines[1] -notmatch '^[0-9]+$') { throw 'Trino fixed table acceptance failed.' }
    $null = Wait-Chapter105CompletedCheckpoint -FlinkPort $FlinkPort -JobId $decision.job_id -MaxAgeSeconds $CheckpointMaxAgeSeconds
    & (Join-Path $PSScriptRoot 'verify_chapter_10_tool_analysis.ps1') -AnalysisBaseUrl "http://localhost:$ApiPort" | Out-Null
    return @{ job_id = $decision.job_id; ready = $true; tools = 'rule_based' }
}

function Initialize-Chapter105EmptyCatalog {
    param([Parameter(Mandatory = $true)][string[]]$ComposePrefix)

    foreach ($statement in @(
            "CREATE SCHEMA IF NOT EXISTS lakehouse.analytics WITH (location = 's3a://warehouse/iceberg/analytics.db')",
            "CREATE TABLE IF NOT EXISTS lakehouse.analytics.user_behavior_detail (event_id VARCHAR, user_id VARCHAR, product_id VARCHAR, event_type VARCHAR, event_time VARCHAR, channel VARCHAR, device_type VARCHAR, page_id VARCHAR) WITH (location = 's3a://warehouse/iceberg/analytics.db/user_behavior_detail')"
        )) {
        Invoke-Chapter105Native -FilePath 'docker' -Arguments ($ComposePrefix + @(
            'exec', '-T', 'trino', 'trino', '--server', 'http://localhost:8080',
            '--catalog', 'lakehouse', '--execute', $statement
        )) -FailureMessage 'Isolated empty catalog initialization failed.' | Out-Null
    }
}

function Invoke-Chapter105Initialization {
    param(
        [Parameter(Mandatory = $true)][string[]]$ComposePrefix,
        [Parameter(Mandatory = $true)][string]$EnvPath,
        [string]$ComposeProjectName,
        [string]$StateRoot
    )

    $statePath = if ($StateRoot) { Join-Path $StateRoot 'tmp\chapter-10-5\initialization-state.json' } else { $null }
    $action = 'created'
    if ($statePath -and (Test-Path -LiteralPath $statePath -PathType Leaf)) {
        try {
            $state = Get-Content -LiteralPath $statePath -Raw -Encoding UTF8 | ConvertFrom-Json -ErrorAction Stop
            if ([int]$state.schema_version -ne 1 -or [string]$state.kind -cne 'chapter10_5_initialization' -or
                [string]$state.status -cne 'initialized' -or
                (@($state.resources) -join ',') -cne 'user_behavior_events,analytics.realtime_metrics,warehouse,flink-state') {
                throw 'invalid state'
            }
            $action = 'no_op'
        } catch {
            throw 'Chapter 10.5 initialization state is invalid.'
        }
    }

    if ($action -ceq 'created') {
        Invoke-Chapter105Native -FilePath 'docker' -Arguments ($ComposePrefix + @(
            'exec', '-T', 'kafka-broker', 'kafka-topics', '--bootstrap-server', 'kafka-broker:29092',
            '--create', '--if-not-exists', '--topic', 'user_behavior_events', '--partitions', '1', '--replication-factor', '1'
        )) -FailureMessage 'Kafka topic initialization failed.' | Out-Null
        $dorisArguments = @(
            '-NoProfile', '-File', (Join-Path $PSScriptRoot 'init_doris_realtime_metrics.ps1'),
            '-EnvFile', $envPath
        )
        if (-not [string]::IsNullOrWhiteSpace($ComposeProjectName)) {
            $dorisArguments += @('-ComposeProjectName', $ComposeProjectName)
        }
        Invoke-Chapter105Native -FilePath 'powershell' -Arguments $dorisArguments `
            -FailureMessage 'Doris initialization failed.' | Out-Null

        if ($statePath) {
            $stateDirectory = Split-Path -Parent $statePath
            [System.IO.Directory]::CreateDirectory($stateDirectory) | Out-Null
            $state = [ordered]@{
                schema_version = 1; kind = 'chapter10_5_initialization'; status = 'initialized'
                resources = @('user_behavior_events', 'analytics.realtime_metrics', 'warehouse', 'flink-state')
                completed_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
            }
            $partial = "$statePath.partial.$([Guid]::NewGuid().ToString('N'))"
            [System.IO.File]::WriteAllText($partial, ($state | ConvertTo-Json -Depth 6), [System.Text.UTF8Encoding]::new($false))
            Move-Item -LiteralPath $partial -Destination $statePath -Force
        }
    }

    return [ordered]@{
        topic = [ordered]@{ name = 'user_behavior_events'; action = $action }
        doris = [ordered]@{ name = 'analytics.realtime_metrics'; action = $action }
        minio_buckets = @(
            [ordered]@{ name = 'warehouse'; action = $action },
            [ordered]@{ name = 'flink-state'; action = $action }
        )
    }
}

function Assert-Chapter105EmptyCatalogGate {
    param([switch]$InitializeEmptyCatalog, [switch]$IsolatedAcceptance)

    if ($InitializeEmptyCatalog -and -not $IsolatedAcceptance) {
        throw 'Empty catalog initialization requires isolated acceptance.'
    }
}

function Resolve-Chapter105IsolationContext {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string]$ProjectName,
        [Parameter(Mandatory = $true)][string]$EnvPath,
        [Parameter(Mandatory = $true)][string]$ReportPath,
        [Parameter(Mandatory = $true)][string]$MinioDataPath,
        [switch]$InitializeEmptyCatalog
    )

    $safeError = 'Isolated acceptance identity is unsafe.'
    try {
        $match = [regex]::Match($ProjectName, '^chapter105-acceptance-(?<run>[a-f0-9]{12})$')
        if (-not $match.Success) { throw $safeError }
        $root = [System.IO.Path]::GetFullPath($RepositoryRoot).TrimEnd('\', '/')
        $runId = $match.Groups['run'].Value
        $runRoot = [System.IO.Path]::GetFullPath((Join-Path $root "acceptance/$runId")).TrimEnd('\', '/')
        $expectedEnv = Join-Path $runRoot 'isolated.env'
        $expectedMinio = Join-Path $runRoot 'minio-data'
        $resolvedEnv = [System.IO.Path]::GetFullPath($EnvPath)
        $resolvedReport = [System.IO.Path]::GetFullPath($ReportPath)
        $resolvedMinio = [System.IO.Path]::GetFullPath($MinioDataPath).TrimEnd('\', '/')
        if (-not $resolvedEnv.Equals($expectedEnv, [System.StringComparison]::OrdinalIgnoreCase) -or
            -not (Split-Path -Parent $resolvedReport).Equals($runRoot, [System.StringComparison]::OrdinalIgnoreCase) -or
            [System.IO.Path]::GetFileName($resolvedReport) -cnotmatch '^bootstrap-(first|second|recovery)\.json$' -or
            -not $resolvedMinio.Equals($expectedMinio, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw $safeError
        }
        $environment = Read-Chapter105EnvFile -Path $resolvedEnv
        if ([string]$environment['PROJECT_NAME'] -cne $ProjectName -or
            -not [System.IO.Path]::GetFullPath([string]$environment['MINIO_DATA_DIR']).TrimEnd('\', '/').Equals(
                $expectedMinio, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw $safeError
        }
        foreach ($candidate in @($root, $runRoot, $resolvedEnv, $resolvedReport, $resolvedMinio)) {
            $current = [System.IO.Path]::GetPathRoot($candidate)
            $relative = $candidate.Substring($current.Length)
            foreach ($segment in $relative.Split(@('\', '/'), [System.StringSplitOptions]::RemoveEmptyEntries)) {
                $current = Join-Path $current $segment
                if (-not (Test-Path -LiteralPath $current)) { break }
                if (((Get-Item -LiteralPath $current -Force).Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                    throw $safeError
                }
            }
        }
        if ($InitializeEmptyCatalog -and -not $ProjectName.StartsWith('chapter105-acceptance-', [System.StringComparison]::Ordinal)) {
            throw $safeError
        }
        return [pscustomobject]@{ run_id = $runId; state_root = $runRoot; run_root = $runRoot }
    } catch {
        throw $safeError
    }
}

function Invoke-Chapter105Bootstrap {
    param(
        [string]$EnvFile = 'infra/.env',
        [switch]$SkipBuild,
        [string]$ReportPath = 'tmp/chapter-10-5/bootstrap-report.json',
        [string]$ComposeProjectName,
        [switch]$InitializeEmptyCatalog,
        [switch]$IsolatedAcceptance,
        [string]$DeadlineUtc
    )

    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'
    $report = New-Chapter105BootstrapReport
    $fallbackReportPath = [System.IO.Path]::GetFullPath([System.IO.Path]::Combine(
            $PSScriptRoot, '..', 'tmp', 'chapter-10-5', 'bootstrap-report.fallback.json'))
    $context = [pscustomobject]@{
        ReportPath = $fallbackReportPath
        RepositoryRoot = $null
        EnvPath = $null
        Environment = $null
        ComposePrefix = $null
        ComposeProjectName = $null
        StateRoot = $null
        CheckpointUri = $null
        SavepointUri = $null
        ComposeDeadline = $null
    }
    $jobName = 'chapter-9-datastream-quality-production'
    $bootstrapFailed = $false
    $reportWriteFailed = $false
    $previousMinioDataDir = [Environment]::GetEnvironmentVariable('MINIO_DATA_DIR', 'Process')
    try {
        Invoke-Chapter105BootstrapStage -Report $report -Name 'preflight' -Action {
            if (-not [string]::IsNullOrWhiteSpace($DeadlineUtc)) {
                try {
                    $context.ComposeDeadline = [DateTimeOffset]::Parse(
                        $DeadlineUtc, [Globalization.CultureInfo]::InvariantCulture, [Globalization.DateTimeStyles]::RoundtripKind)
                } catch {
                    throw 'Acceptance deadline is invalid.'
                }
                if ($context.ComposeDeadline -le [DateTimeOffset]::UtcNow) {
                    throw 'Acceptance deadline has expired.'
                }
            }
            Assert-Chapter105EmptyCatalogGate -InitializeEmptyCatalog:$InitializeEmptyCatalog `
                -IsolatedAcceptance:$IsolatedAcceptance
            if ($IsolatedAcceptance) {
                if ([string]::IsNullOrWhiteSpace($ComposeProjectName) -or
                    $ComposeProjectName -cnotmatch '^chapter105-acceptance-[a-f0-9]{12}$') {
                    throw 'Isolated acceptance identity is unsafe.'
                }
                $context.RepositoryRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..')).TrimEnd('\', '/')
            } else {
                $context.RepositoryRoot = Get-Chapter105PrimaryRepositoryRoot -StartPath $PSScriptRoot
            }
            $defaultReportPath = Resolve-Chapter105RepositoryPath -RepositoryRoot $context.RepositoryRoot `
                -Path 'tmp/chapter-10-5/bootstrap-report.json' -AllowedRelativeRoot 'tmp/chapter-10-5'
            $requestedReportPath = if ($ReportPath -ceq 'tmp/chapter-10-5/bootstrap-report.json') {
                $defaultReportPath
            } else {
                Resolve-Chapter105RepositoryPath -RepositoryRoot $context.RepositoryRoot `
                    -Path $ReportPath -AllowedRelativeRoot 'tmp/chapter-10-5'
            }
            Assert-Chapter105ReportPathWritable -Path $requestedReportPath
            $context.ReportPath = $requestedReportPath
            $envCandidate = if ([System.IO.Path]::IsPathRooted($EnvFile)) {
                [System.IO.Path]::GetFullPath($EnvFile)
            } else {
                [System.IO.Path]::GetFullPath((Join-Path $context.RepositoryRoot $EnvFile))
            }
            $acceptanceRoot = Resolve-Chapter105RepositoryPath -RepositoryRoot $context.RepositoryRoot `
                -Path 'tmp/chapter-10-5/acceptance/.bootstrap-anchor' -AllowedRelativeRoot 'tmp/chapter-10-5'
            $isAcceptanceEnv = $envCandidate.StartsWith((Split-Path -Parent $acceptanceRoot) + [System.IO.Path]::DirectorySeparatorChar,
                [System.StringComparison]::OrdinalIgnoreCase)
            $context.EnvPath = Resolve-Chapter105RepositoryPath -RepositoryRoot $context.RepositoryRoot `
                -Path $EnvFile -AllowedRelativeRoot $(if ($isAcceptanceEnv) { 'tmp/chapter-10-5/acceptance' } else { 'infra' })
            $context.Environment = Read-Chapter105EnvFile -Path $context.EnvPath
            $acceptanceDirectory = Split-Path -Parent $acceptanceRoot
            if ($isAcceptanceEnv) {
                $configuredMinioData = [System.IO.Path]::GetFullPath([string]$context.Environment['MINIO_DATA_DIR'])
                if (-not $configuredMinioData.StartsWith($acceptanceDirectory + [System.IO.Path]::DirectorySeparatorChar,
                        [System.StringComparison]::OrdinalIgnoreCase)) {
                    throw 'Isolated MinIO data directory is outside the acceptance root.'
                }
            } else {
                $configuredMinioData = Get-Chapter105StableMinioDataPath -RepositoryRoot $context.RepositoryRoot
            }
            $context.Environment['MINIO_DATA_DIR'] = $configuredMinioData
            if ($IsolatedAcceptance) {
                $isolation = Resolve-Chapter105IsolationContext `
                    -RepositoryRoot (Join-Path $context.RepositoryRoot 'tmp/chapter-10-5') `
                    -ProjectName $ComposeProjectName -EnvPath $context.EnvPath `
                    -ReportPath $context.ReportPath -MinioDataPath $configuredMinioData `
                    -InitializeEmptyCatalog:$InitializeEmptyCatalog
                $context.StateRoot = $isolation.state_root
            }
            [Environment]::SetEnvironmentVariable('MINIO_DATA_DIR', $context.Environment['MINIO_DATA_DIR'], 'Process')
            if (-not [string]::IsNullOrWhiteSpace($ComposeProjectName) -and
                $ComposeProjectName -cnotmatch '^[a-z0-9][a-z0-9_-]*$') {
                throw 'Compose project identity is unsafe.'
            }
            $context.ComposeProjectName = $ComposeProjectName
            $context.ComposePrefix = @('compose')
            if (-not [string]::IsNullOrWhiteSpace($context.ComposeProjectName)) {
                $context.ComposePrefix += @('--project-name', $context.ComposeProjectName)
            }
            $context.ComposePrefix += @(
                '--env-file', $context.EnvPath, '-f', (Join-Path $context.RepositoryRoot 'infra\docker-compose.yml'),
                '--profile', 'flink', '--profile', 'serving', '--profile', 'lakehouse'
            )
            . (Join-Path $PSScriptRoot 'run_chapter_9_production_cutover.ps1') -FunctionsOnly
            $context.CheckpointUri = Get-Chapter9StateUri -Kind 'checkpoint' -Environment $context.Environment
            $context.SavepointUri = Get-Chapter9StateUri -Kind 'savepoint' -Environment $context.Environment
            Assert-Chapter105Preflight -RepositoryRoot $context.RepositoryRoot -Environment $context.Environment `
                -MinioDataPath $context.Environment['MINIO_DATA_DIR'] -SkipBuild:$SkipBuild
        }
        $repositoryRoot = $context.RepositoryRoot
        $cutoverRepositoryRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..')).TrimEnd('\', '/')
        $envPath = $context.EnvPath
        Invoke-Chapter105BootstrapStage -Report $report -Name 'dependencies' -Action {
            Invoke-Chapter105Native -FilePath 'powershell' -Arguments @(
                '-NoProfile', '-File', (Join-Path $PSScriptRoot 'install_runtime_dependencies.ps1'),
                '-RepositoryRoot', $repositoryRoot
            ) -FailureMessage 'Runtime dependency installation failed.' | Out-Null
            @{ installer = 'completed' }
        }
        Invoke-Chapter105BootstrapStage -Report $report -Name 'infrastructure' -Action {
            $upArguments = @('up', '-d')
            if ($null -ne $context.ComposeDeadline) {
                $remainingSeconds = [int][Math]::Floor(($context.ComposeDeadline - [DateTimeOffset]::UtcNow).TotalSeconds)
                if ($remainingSeconds -lt 1) { throw 'Acceptance deadline has expired.' }
                $upArguments += @('--wait', '--wait-timeout', [string]$remainingSeconds)
            }
            Invoke-Chapter105Native -FilePath 'docker' -Arguments ($context.ComposePrefix + $upArguments) `
                -FailureMessage 'Infrastructure startup failed.' | Out-Null
            if ($null -ne $context.ComposeDeadline) {
                @{ services = 13; readiness = 'compose_native_wait' }
            } else {
                Wait-Chapter105ComposeReady -ComposePrefix $context.ComposePrefix
            }
        }
        Invoke-Chapter105BootstrapStage -Report $report -Name 'initialization' -Action {
            Invoke-Chapter105Initialization -ComposePrefix $context.ComposePrefix -EnvPath $envPath `
                -ComposeProjectName $context.ComposeProjectName -StateRoot $context.StateRoot
        }
        Invoke-Chapter105BootstrapStage -Report $report -Name 'catalog' -Action {
            if ($InitializeEmptyCatalog) {
                Initialize-Chapter105EmptyCatalog -ComposePrefix $context.ComposePrefix
            }
            $catalogArguments = @(
                '-NoProfile', '-File', (Join-Path $PSScriptRoot 'restore_chapter_10_5_catalog.ps1'),
                '-EnvFile', $envPath
            )
            if (-not [string]::IsNullOrWhiteSpace($context.ComposeProjectName)) {
                $catalogArguments += @('-ComposeProjectName', $context.ComposeProjectName)
            }
            $catalogOutput = @(Invoke-Chapter105Native -FilePath 'powershell' -Arguments $catalogArguments `
                -FailureMessage 'Catalog recovery failed.'
            )
            $catalogAction = if ($catalogOutput.Count -gt 0) { [string]$catalogOutput[-1] } else { 'recovered' }
            @{ catalog = 'recovered'; action = $catalogAction }
        }
        Invoke-Chapter105BootstrapStage -Report $report -Name 'jobs' -Action {
            . (Join-Path $PSScriptRoot 'run_chapter_9_production_cutover.ps1') -FunctionsOnly
            $overview = Invoke-Chapter105FlinkOverview -Port ([int]$context.Environment['FLINK_REST_PORT'])
            $jobArguments = @{
                RepositoryRoot = $cutoverRepositoryRoot
                Overview = $overview
                JobName = $jobName
                SavepointUri = $context.SavepointUri
            }
            if ($null -ne $context.StateRoot) { $jobArguments['StateRoot'] = $context.StateRoot }
            if ($IsolatedAcceptance) { $jobArguments['IsolatedAcceptance'] = $true }
            $decision = Invoke-Chapter105JobsStage @jobArguments -SubmitAction {
                param($savepointPath)
                $id = Invoke-Chapter105ReconciledJobSubmission -RepositoryRoot $repositoryRoot `
                    -ComposePrefix $context.ComposePrefix -CheckpointUri $context.CheckpointUri `
                    -SavepointPath $savepointPath -FlinkPort ([int]$context.Environment['FLINK_REST_PORT']) `
                    -JobName $jobName -SkipBuild:$SkipBuild
                [pscustomobject]@{ job_id = $id }
            }
            if ($decision.action -eq 'submitted') {
                $running = Wait-Chapter105UniqueRunningJob -FlinkPort ([int]$context.Environment['FLINK_REST_PORT']) -JobName $jobName
                if ($running.job_id -ne $decision.job_id) { throw 'Submitted Flink job identity changed during recovery.' }
            }
            @{ job_id = $decision.job_id; action = $decision.action; state_root = $context.StateRoot }
        }
        Invoke-Chapter105BootstrapStage -Report $report -Name 'acceptance' -Action {
            Invoke-Chapter105Acceptance -ApiPort ([int]$context.Environment['API_PORT']) `
                -FlinkPort ([int]$context.Environment['FLINK_REST_PORT']) -JobName $jobName `
                -CheckpointMaxAgeSeconds ([int]$context.Environment['FLINK_CHECKPOINT_MAX_AGE_SECONDS']) `
                -ComposePrefix $context.ComposePrefix
        }
        $report.status = 'passed'
    } catch {
        $bootstrapFailed = $true
        $report.status = 'failed'
    } finally {
        [Environment]::SetEnvironmentVariable('MINIO_DATA_DIR', $previousMinioDataDir, 'Process')
        $report.completed_at = [DateTimeOffset]::UtcNow.ToString('o')
        try {
            Write-Chapter105BootstrapReport -Report $report -Path $context.ReportPath
        } catch {
            $reportWriteFailed = $true
        }
    }
    if ($bootstrapFailed) { throw 'Chapter 10.5 bootstrap failed. See the bootstrap report for safe stage status.' }
    if ($reportWriteFailed) { throw 'Chapter 10.5 bootstrap report could not be written safely.' }
}

if ($FunctionsOnly) { return }

Invoke-Chapter105Bootstrap -EnvFile $EnvFile -SkipBuild:$SkipBuild -ReportPath $ReportPath `
    -ComposeProjectName $ComposeProjectName -InitializeEmptyCatalog:$InitializeEmptyCatalog `
    -IsolatedAcceptance:$IsolatedAcceptance -DeadlineUtc $DeadlineUtc
