$ErrorActionPreference = "Stop"

$kafka = "ecom-kafka"
$jobManager = "ecom-flink-jobmanager"
$jobName = "chapter-9-datastream-quality-shadow"
$runId = "chapter9-recovery-" + [Guid]::NewGuid().ToString("N")
$eventId = "$runId-state"

function Get-ShadowJob {
    $jobs = Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview"
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
        $checkpoints = Invoke-RestMethod -Uri "http://localhost:8081/jobs/$JobId/checkpoints"
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

& (Join-Path $PSScriptRoot "run_chapter_9_shadow.ps1")
$job = Wait-RunningJob ""
$jobId = $job.jid
$before = Invoke-RestMethod -Uri "http://localhost:8081/jobs/$jobId/checkpoints"
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

docker exec $jobManager sh -lc "mkdir -p /workspace/tmp/savepoints/chapter-9 && chmod 0777 /workspace/tmp/savepoints/chapter-9"
if ($LASTEXITCODE -ne 0) { throw "Could not prepare shared Savepoint directory." }
$stopOutput = docker exec $jobManager /opt/flink/bin/flink stop `
    --savepointPath file:///workspace/tmp/savepoints/chapter-9 $jobId
if ($LASTEXITCODE -ne 0) { throw "Stop with Savepoint failed." }
$stopText = $stopOutput -join "`n"
if ($stopText -notmatch "Path:\s+(file:\S+)") { throw "Could not parse Savepoint path." }
$savepointPath = $Matches[1]

$restoreOutput = docker exec $jobManager /opt/flink/bin/flink run -d -s $savepointPath `
    -c com.ecommerce.quality.DataQualityJob `
    /tmp/datastream-quality-1.0.0.jar `
    --bootstrap-servers kafka:29092 `
    --input-topic user_behavior_events `
    --mode shadow `
    --consumer-group chapter9-quality-shadow `
    --checkpoint-uri file:///workspace/tmp/checkpoints/chapter-9 `
    --transaction-prefix chapter9-shadow `
    --job-version chapter-9-v1
if ($LASTEXITCODE -ne 0) { throw "Restore from Savepoint failed." }
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
