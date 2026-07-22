$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$compose = Join-Path $root "infra/docker-compose.yml"
$envFile = Join-Path $root "infra/.env.example"
$jar = Join-Path $root "jobs/datastream-quality/target/datastream-quality-1.0.0.jar"
$kafka = "ecom-kafka"
$jobManager = "ecom-flink-jobmanager"
$jobName = "chapter-9-datastream-quality-shadow"
$topics = @("user_behavior_events", "user_behavior_clean_shadow", "user_behavior_dlq", "user_behavior_late")

& (Join-Path $PSScriptRoot "build_chapter_9_datastream.ps1")

$requiredContainers = @($kafka, $jobManager, "ecom-flink-taskmanager")
$missing = @()
foreach ($container in $requiredContainers) {
    $running = (docker inspect --format '{{.State.Running}}' $container 2>$null | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or $running -ne "true") { $missing += $container }
}
if ($missing.Count -gt 0) {
    Write-Host "Starting missing Kafka/Flink containers..."
    docker compose --env-file $envFile -f $compose --profile flink up -d
    if ($LASTEXITCODE -ne 0) { throw "Kafka/Flink startup failed." }
}

for ($attempt = 1; $attempt -le 60; $attempt++) {
    try {
        docker exec $kafka kafka-topics --bootstrap-server kafka:29092 --list 2>$null | Out-Null
        $overview = Invoke-RestMethod -Uri "http://localhost:8081/overview" -TimeoutSec 3
        if ($null -ne $overview) { break }
    } catch {
        if ($attempt -eq 60) { throw "Kafka/Flink did not become ready in time." }
        Start-Sleep -Seconds 2
    }
}

foreach ($topic in $topics) {
    docker exec $kafka kafka-topics --bootstrap-server kafka:29092 `
        --create --if-not-exists --topic $topic --partitions 1 --replication-factor 1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Topic creation failed: $topic" }
}

$jobs = Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview"
$existing = @($jobs.jobs | Where-Object { $_.name -eq $jobName -and $_.state -eq "RUNNING" })
if ($existing.Count -gt 0) {
    Write-Host "Shadow job is already running. Job ID: $($existing[0].jid)"
    return
}

docker cp $jar "${jobManager}:/tmp/datastream-quality-1.0.0.jar"
if ($LASTEXITCODE -ne 0) { throw "Failed to copy Fat JAR to JobManager." }

$submitOutput = docker exec $jobManager /opt/flink/bin/flink run -d `
    -c com.ecommerce.quality.DataQualityJob `
    /tmp/datastream-quality-1.0.0.jar `
    --bootstrap-servers kafka:29092 `
    --input-topic user_behavior_events `
    --mode shadow `
    --consumer-group chapter9-quality-shadow `
    --checkpoint-uri file:///workspace/tmp/checkpoints/chapter-9 `
    --transaction-prefix chapter9-shadow `
    --job-version chapter-9-v1
if ($LASTEXITCODE -ne 0) { throw "Chapter 9 shadow job submission failed." }

$submitOutput | ForEach-Object { Write-Host $_ }
if (($submitOutput -join "`n") -notmatch "JobID ([0-9a-f]{32})") {
    throw "Could not parse Job ID from Flink submission output."
}
Write-Host "Shadow job submitted. Job ID: $($Matches[1])"
