param([switch]$FunctionsOnly)

$ErrorActionPreference = "Stop"

$kafka = "ecom-kafka"
$jobName = "chapter-9-datastream-quality-shadow"
$runId = "chapter9-recovery-" + [Guid]::NewGuid().ToString("N")
$eventId = "$runId-state"

function Invoke-FlinkJobManagerCommand {
    param([string[]]$Command, [string]$FailureMessage)

    $root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
    $output = & docker compose --env-file (Join-Path $root "infra/.env.example") `
        -f (Join-Path $root "infra/docker-compose.yml") exec -T flink-jobmanager @Command 2>&1
    if ($LASTEXITCODE -ne 0) { throw "$FailureMessage Output: $($output -join "`n")" }
    return @($output | ForEach-Object { [string]$_ })
}

function Invoke-FlinkRest([string]$Resource) {
    $output = Invoke-FlinkJobManagerCommand -Command @(
        "curl", "--fail", "--silent", "--show-error", "http://localhost:8081$Resource"
    ) -FailureMessage "Flink REST request failed: $Resource"
    return ($output -join "`n" | ConvertFrom-Json)
}

function Assert-Chapter9StateUri([string]$Path, [ValidateSet("checkpoint", "savepoint")][string]$Kind) {
    $base = "s3a://flink-state/$($Kind)s/chapter-9"
    if ([string]::IsNullOrWhiteSpace($Path) -or $Path -match "\.\." -or
        $Path -cnotmatch "^$([regex]::Escape($base))(?:/[A-Za-z0-9._-]+)*$") {
        throw "Invalid Chapter 9 $Kind URI: $Path"
    }
    return $Path
}

function Get-Chapter9StateUri([ValidateSet("checkpoint", "savepoint")][string]$Kind) {
    $default = "s3a://flink-state/$($Kind)s/chapter-9"
    $name = "CHAPTER9_$($Kind.ToUpperInvariant())_URI"
    $configured = [Environment]::GetEnvironmentVariable($name)
    if ([string]::IsNullOrWhiteSpace($configured)) { $configured = $default }
    return Assert-Chapter9StateUri -Path $configured.Trim() -Kind $Kind
}

function Get-SavepointPath([string[]]$Lines) {
    $paths = @($Lines | ForEach-Object {
        $match = [regex]::Match([string]$_, "(?i)Savepoint completed\.\s*Path:\s*(\S+)")
        if ($match.Success) { Assert-Chapter9StateUri -Path $match.Groups[1].Value -Kind "savepoint" }
    } | Sort-Object -Unique)
    if ($paths.Count -ne 1 -or $paths[0] -cnotmatch "^s3a://flink-state/savepoints/chapter-9/[A-Za-z0-9._-]+$") {
        throw "Expected exactly one validated remote Savepoint path."
    }
    return [string]$paths[0]
}

function Get-ShadowJob {
    $jobs = Invoke-FlinkRest "/jobs/overview"
    return @($jobs.jobs | Where-Object { $_.name -eq $jobName -and $_.state -eq "RUNNING" }) | Select-Object -First 1
}

function Wait-RunningJob([string]$ExpectedId, [int]$Attempts = 60) {
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        try {
            $job = Get-ShadowJob
            if ($null -ne $job -and ($ExpectedId -eq "" -or $job.jid -eq $ExpectedId)) { return $job }
        } catch {}
        Start-Sleep -Seconds 2
    }
    throw "Shadow job did not return to RUNNING in time."
}

function Wait-NewCheckpoint([string]$JobId, [int64]$Baseline) {
    for ($attempt = 1; $attempt -le 60; $attempt++) {
        $checkpoints = Invoke-FlinkRest "/jobs/$JobId/checkpoints"
        if ([int64]$checkpoints.counts.completed -gt $Baseline) { return $checkpoints }
        Start-Sleep -Seconds 2
    }
    throw "No new completed checkpoint appeared in time."
}

function Send-KafkaValue([string]$Value) {
    $Value | docker exec -i $kafka kafka-console-producer --bootstrap-server kafka:29092 --topic user_behavior_events
    if ($LASTEXITCODE -ne 0) { throw "Kafka recovery event send failed." }
}

function Read-CommittedTopic([string]$Topic) {
    $command = "kafka-console-consumer --bootstrap-server kafka:29092 --topic $Topic --from-beginning --timeout-ms 5000 --consumer-property isolation.level=read_committed 2>/dev/null || true"
    return @(docker exec $kafka bash -lc $command)
}

if ($FunctionsOnly) { return }

& (Join-Path $PSScriptRoot "run_chapter_9_shadow.ps1")
$job = Wait-RunningJob ""
$jobId = $job.jid
$before = Invoke-FlinkRest "/jobs/$jobId/checkpoints"
$checkpointBaseline = [int64]$before.counts.completed

docker restart ecom-flink-taskmanager | Out-Null
if ($LASTEXITCODE -ne 0) { throw "TaskManager restart failed." }
$null = Wait-RunningJob $jobId
$afterRestart = Wait-NewCheckpoint $jobId $checkpointBaseline

$event = [ordered]@{
    event_id = $eventId
    user_id = "user-$runId"
    product_id = "product-1"
    event_type = "view"
    event_time = [DateTimeOffset]::UtcNow.ToString("o")
    channel = "app"
    device_type = "android"
    page_id = "home"
} | ConvertTo-Json -Compress
Send-KafkaValue $event
$preSavepointCheckpoint = [int64]$afterRestart.counts.completed
$null = Wait-NewCheckpoint $jobId $preSavepointCheckpoint

$checkpointUri = Get-Chapter9StateUri "checkpoint"
$savepointUri = Get-Chapter9StateUri "savepoint"
$stopOutput = Invoke-FlinkJobManagerCommand -Command @(
    "/opt/flink/bin/flink", "stop", "--savepointPath", $savepointUri, $jobId
) -FailureMessage "Stop with Savepoint failed."
$savepointPath = Get-SavepointPath $stopOutput

$restoreOutput = Invoke-FlinkJobManagerCommand -Command @(
    "/opt/flink/bin/flink", "run", "-d", "-s", $savepointPath,
    "-c", "com.ecommerce.quality.DataQualityJob",
    "/tmp/datastream-quality-1.0.0.jar",
    "--bootstrap-servers", "kafka:29092",
    "--input-topic", "user_behavior_events",
    "--mode", "shadow",
    "--consumer-group", "chapter9-quality-shadow",
    "--checkpoint-uri", $checkpointUri,
    "--transaction-prefix", "chapter9-shadow",
    "--job-version", "chapter-9-v1"
) -FailureMessage "Restore from Savepoint failed."
$restoreText = $restoreOutput -join "`n"
if ($restoreText -notmatch "JobID ([0-9a-f]{32})") { throw "Could not parse restored Job ID." }
$restoredJobId = $Matches[1]
$null = Wait-RunningJob $restoredJobId
$null = Wait-NewCheckpoint $restoredJobId 0

Send-KafkaValue $event
Start-Sleep -Seconds 20
$clean = @(Read-CommittedTopic "user_behavior_clean_shadow" | Where-Object { $_ -like "*$eventId*" })
$dlq = @(Read-CommittedTopic "user_behavior_dlq" | Where-Object { $_ -like "*$eventId*DUPLICATE_EVENT*" -or ($_ -like "*$eventId*" -and $_ -like "*DUPLICATE_EVENT*") })
if ($clean.Count -ne 1) { throw "Savepoint state test expected exactly one clean record, got $($clean.Count)." }
if ($dlq.Count -ne 1) { throw "Savepoint state test expected exactly one duplicate DLQ record, got $($dlq.Count)." }

Write-Host "TaskManager recovery passed: job_id=$jobId completed_checkpoints=$($afterRestart.counts.completed)"
Write-Host "Savepoint restore passed: path=$savepointPath restored_job_id=$restoredJobId clean=1 duplicate_dlq=1"
