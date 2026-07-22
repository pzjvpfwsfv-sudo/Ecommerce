$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$kafka = "ecom-kafka"
$runId = "chapter9-" + [Guid]::NewGuid().ToString("N")
$rawTopic = "user_behavior_events"
$cleanTopic = "user_behavior_clean_shadow"
$dlqTopic = "user_behavior_dlq"
$lateTopic = "user_behavior_late"
$expectedReasons = @("DUPLICATE_EVENT", "MALFORMED_JSON", "MISSING_REQUIRED_FIELD", "INVALID_EVENT_TIME", "FUTURE_EVENT_TIME")
$expectedMetrics = @("valid_events_total", "dlq_events_total", "late_events_total", "duplicate_events_total", "parse_errors_total", "validation_errors_total")

function Send-KafkaValue([string]$Topic, [string]$Value) {
    $Value | docker exec -i $kafka kafka-console-producer --bootstrap-server kafka:29092 --topic $Topic
    if ($LASTEXITCODE -ne 0) { throw "Kafka test event send failed." }
}

function New-EventJson([string]$EventId, [string]$EventTime) {
    return [ordered]@{
        event_id = $EventId
        user_id = "user-$runId"
        product_id = "product-1"
        event_type = "view"
        event_time = $EventTime
        channel = "app"
        device_type = "android"
        page_id = "home"
    } | ConvertTo-Json -Compress
}

function Read-CommittedTopic([string]$Topic) {
    $command = "kafka-console-consumer --bootstrap-server kafka:29092 --topic $Topic --from-beginning --timeout-ms 5000 --consumer-property isolation.level=read_committed 2>/dev/null || true"
    return @(docker exec $kafka bash -lc $command)
}

& (Join-Path $PSScriptRoot "run_chapter_9_shadow.ps1")

$jobs = Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview"
$job = @($jobs.jobs | Where-Object { $_.name -eq "chapter-9-datastream-quality-shadow" -and $_.state -eq "RUNNING" }) | Select-Object -First 1
if ($null -eq $job) { throw "Chapter 9 shadow job is not RUNNING." }
$jobId = $job.jid

$now = [DateTimeOffset]::UtcNow
$duplicate = New-EventJson "$runId-duplicate" $now.ToString("o")
$advancer = New-EventJson "$runId-advancer" $now.AddSeconds(30).ToString("o")
$missing = (New-EventJson "$runId-missing" $now.ToString("o") | ConvertFrom-Json)
$missing.PSObject.Properties.Remove("user_id")
$missingJson = $missing | ConvertTo-Json -Compress
$invalidTime = New-EventJson "$runId-invalid-time" "2026-07-22 10:00:00"
$future = New-EventJson "$runId-future" $now.AddMinutes(10).ToString("o")

Send-KafkaValue $rawTopic $duplicate
Send-KafkaValue $rawTopic $duplicate
Send-KafkaValue $rawTopic "$runId-{bad"
Send-KafkaValue $rawTopic $missingJson
Send-KafkaValue $rawTopic $invalidTime
Send-KafkaValue $rawTopic $future
Send-KafkaValue $rawTopic $advancer
Start-Sleep -Seconds 5
Send-KafkaValue $rawTopic (New-EventJson "$runId-late" $now.AddSeconds(-30).ToString("o"))

# EXACTLY_ONCE Kafka outputs become read_committed after a successful checkpoint.
Start-Sleep -Seconds 25
$cleanLines = @(Read-CommittedTopic $cleanTopic | Where-Object { $_ -like "*$runId*" })
$dlqLines = @(Read-CommittedTopic $dlqTopic | Where-Object { $_ -like "*$runId*" })
$lateLines = @(Read-CommittedTopic $lateTopic | Where-Object { $_ -like "*$runId*" })

$cleanRecords = @($cleanLines | ForEach-Object { $_ | ConvertFrom-Json })
$dlqRecords = @($dlqLines | ForEach-Object { $_ | ConvertFrom-Json })
$lateRecords = @($lateLines | ForEach-Object { $_ | ConvertFrom-Json })
$duplicateCleanCount = @($cleanRecords | Where-Object { $_.event_id -eq "$runId-duplicate" }).Count
if ($cleanRecords.Count -ne 2 -or $dlqRecords.Count -ne 5 -or $lateRecords.Count -ne 1) {
    throw "Output counts differ: raw=8 clean=$($cleanRecords.Count) dlq=$($dlqRecords.Count) late=$($lateRecords.Count)"
}
if ($duplicateCleanCount -ne 1) { throw "Duplicate event_id was not emitted exactly once to clean." }
foreach ($reason in $expectedReasons) {
    if (@($dlqRecords | Where-Object { $_.reason_code -eq $reason }).Count -ne 1) {
        throw "DLQ reason code is missing or duplicated: $reason"
    }
}
if (8 -ne ($cleanRecords.Count + $dlqRecords.Count + $lateRecords.Count)) {
    throw "raw = clean + dlq + late reconciliation failed."
}

$checkpoints = Invoke-RestMethod -Uri "http://localhost:8081/jobs/$jobId/checkpoints"
if ([int64]$checkpoints.counts.completed -lt 1) { throw "No completed Flink checkpoint was found." }
$details = Invoke-RestMethod -Uri "http://localhost:8081/jobs/$jobId"
$missingMetrics = @($expectedMetrics)
for ($attempt = 1; $attempt -le 10 -and $missingMetrics.Count -gt 0; $attempt++) {
    $metricIds = @()
    foreach ($vertex in $details.vertices) {
        $metrics = Invoke-RestMethod -Uri "http://localhost:8081/jobs/$jobId/vertices/$($vertex.id)/metrics"
        $metricIds += @($metrics | ForEach-Object { $_.id })
    }
    $missingMetrics = @($expectedMetrics | Where-Object { $expected = $_; -not ($metricIds | Where-Object { $_ -like "*$expected" }) })
    if ($missingMetrics.Count -gt 0) { Start-Sleep -Seconds 2 }
}
if ($missingMetrics.Count -gt 0) { throw "Flink metrics were not registered: $($missingMetrics -join ',')" }

Write-Host "Chapter 9 shadow verification passed: run_id=$runId job_id=$jobId"
Write-Host "raw=8 clean=2 dlq=5 late=1; raw = clean + dlq + late"
Write-Host "completed_checkpoints=$($checkpoints.counts.completed) metrics=$($expectedMetrics -join ',')"
