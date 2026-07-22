param(
    [switch]$FunctionsOnly
)

$ErrorActionPreference = "Stop"

function Get-WorkspaceMountSource([string]$Container) {
    $inspection = @(docker inspect $Container | ConvertFrom-Json)
    if ($LASTEXITCODE -ne 0) { throw "Docker inspect failed for $Container." }

    $workspaceMount = @($inspection[0].Mounts | Where-Object { $_.Destination -eq "/workspace" }) |
        Select-Object -First 1
    if ($null -eq $workspaceMount) { throw "Container $Container has no /workspace mount." }

    return [string]$workspaceMount.Source
}

function Assert-FlinkCapacity([object]$Overview) {
    if ($Overview.taskmanagers -ne 1) {
        throw "Expected exactly one TaskManager, got $($Overview.taskmanagers)."
    }
    if ($Overview."slots-total" -ne 4) {
        throw "Expected four total slots, got $($Overview.'slots-total')."
    }
}

function Wait-NewCompletedCheckpoint(
    [string]$JobId,
    [int64]$Baseline,
    [int]$Attempts = 60,
    [int]$SleepSeconds = 2
) {
    $lastRequestError = $null
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        try {
            $checkpoints = Invoke-RestMethod -Uri "http://localhost:8081/jobs/$JobId/checkpoints"
            if ([int64]$checkpoints.counts.completed -gt $Baseline) { return $checkpoints }
        } catch {
            $lastRequestError = $_.Exception.Message
        }
        if ($attempt -lt $Attempts -and $SleepSeconds -gt 0) { Start-Sleep -Seconds $SleepSeconds }
    }

    $message = "No new completed checkpoint appeared for shadow job $JobId after $Attempts attempts."
    if ($null -ne $lastRequestError) { throw "$message Last error: $lastRequestError" }
    throw $message
}

if ($FunctionsOnly) { return }

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$compose = Join-Path $root "infra/docker-compose.yml"
$envFile = Join-Path $root "infra/.env.example"
$jobManager = "ecom-flink-jobmanager"
$taskManager = "ecom-flink-taskmanager"
$jobName = "chapter-9-datastream-quality-shadow"

$jobManagerWorkspace = Get-WorkspaceMountSource $jobManager
$taskManagerWorkspace = Get-WorkspaceMountSource $taskManager
if ($jobManagerWorkspace -ne $root -or $taskManagerWorkspace -ne $root) {
    throw "Workspace mount mismatch.`nCurrent root: $root`nJobManager /workspace: $jobManagerWorkspace`nTaskManager /workspace: $taskManagerWorkspace"
}

$jobsBefore = Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview"
$shadowJob = @($jobsBefore.jobs | Where-Object { $_.name -eq $jobName -and $_.state -eq "RUNNING" }) |
    Select-Object -First 1
if ($null -eq $shadowJob) { throw "Shadow job is not RUNNING before resize." }

$jobId = [string]$shadowJob.jid
$checkpointsBefore = Invoke-RestMethod -Uri "http://localhost:8081/jobs/$jobId/checkpoints"
$completedBefore = [int64]$checkpointsBefore.counts.completed
$overviewBefore = Invoke-RestMethod -Uri "http://localhost:8081/overview"
Write-Host "Before resize: job_id=$jobId taskmanagers=$($overviewBefore.taskmanagers) slots_total=$($overviewBefore.'slots-total') completed_checkpoints=$completedBefore"

docker compose --env-file $envFile -f $compose --profile flink up -d `
    --no-deps --force-recreate flink-taskmanager
if ($LASTEXITCODE -ne 0) { throw "TaskManager resize failed." }

$overviewAfter = $null
for ($attempt = 1; $attempt -le 60; $attempt++) {
    try {
        $overviewAfter = Invoke-RestMethod -Uri "http://localhost:8081/overview"
        Assert-FlinkCapacity $overviewAfter
        break
    } catch {
        if ($attempt -eq 60) { throw }
        Start-Sleep -Seconds 2
    }
}

$recoveredJob = $null
for ($attempt = 1; $attempt -le 60; $attempt++) {
    try {
        $jobsAfter = Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview"
        $recoveredJob = @($jobsAfter.jobs | Where-Object { $_.jid -eq $jobId -and $_.state -eq "RUNNING" }) |
            Select-Object -First 1
        if ($null -ne $recoveredJob) { break }
    } catch {}
    Start-Sleep -Seconds 2
}
if ($null -eq $recoveredJob) { throw "Shadow job $jobId did not recover to RUNNING." }

$checkpointsAfter = Wait-NewCompletedCheckpoint -JobId $jobId -Baseline $completedBefore

Write-Host "After resize: job_id=$jobId taskmanagers=$($overviewAfter.taskmanagers) slots_total=$($overviewAfter.'slots-total') completed_checkpoints=$($checkpointsAfter.counts.completed)"
