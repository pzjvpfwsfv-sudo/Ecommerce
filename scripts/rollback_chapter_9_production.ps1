param(
    [switch]$TrafficPaused,
    [switch]$DryRun,
    [switch]$FunctionsOnly
)

$ErrorActionPreference = "Stop"

function Invoke-DockerCommand {
    param(
        [string[]]$Arguments,
        [string]$FailureMessage
    )

    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $raw = & docker @Arguments 2>&1
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    $output = @($raw | ForEach-Object {
        if ($_ -is [System.Management.Automation.ErrorRecord]) {
            [string]$_.Exception.Message
        } else {
            [string]$_
        }
    })
    if ($exitCode -ne 0) {
        $details = ($output -join "`n").Trim()
        if ($details) { throw "$FailureMessage Output: $details" }
        throw $FailureMessage
    }
    return $output
}

function Get-FlinkJobs {
    return Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview" -TimeoutSec 5
}

function Get-FlinkJob([string]$JobId) {
    return Invoke-RestMethod -Uri "http://localhost:8081/jobs/$JobId" -TimeoutSec 5
}

function Assert-RollbackJobId([string]$JobId, [string]$Field) {
    if ([string]::IsNullOrWhiteSpace($JobId) -or $JobId -notmatch "^[0-9a-f]{32}$") {
        throw "Manifest $Field must be a lowercase 32-character Flink Job ID."
    }
}

function Get-RollbackProperty([object]$Object, [string]$Name, [bool]$Required = $true) {
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) {
        if ($Required) { throw "Manifest field is missing: $Name." }
        return $null
    }
    return $property.Value
}

function Assert-RollbackManifest([object]$Manifest) {
    if ($null -eq $Manifest) { throw "Cutover manifest is empty." }
    foreach ($field in @(
        "cutover_id", "created_at", "raw_offsets", "shadow_job_id", "savepoint_path",
        "production_job_id", "doris_job_id", "iceberg_job_id"
    )) {
        Get-RollbackProperty -Object $Manifest -Name $field | Out-Null
    }

    $cutoverId = [string]$Manifest.cutover_id
    if ($cutoverId -notmatch "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$") {
        throw "Manifest cutover_id is not a canonical lowercase GUID."
    }
    try { [DateTimeOffset]::Parse([string]$Manifest.created_at) | Out-Null } catch {
        throw "Manifest created_at is not an ISO timestamp."
    }
    Assert-RollbackJobId -JobId ([string]$Manifest.shadow_job_id) -Field "shadow_job_id"
    Assert-RollbackJobId -JobId ([string]$Manifest.production_job_id) -Field "production_job_id"
    Assert-RollbackJobId -JobId ([string]$Manifest.doris_job_id) -Field "doris_job_id"
    Assert-RollbackJobId -JobId ([string]$Manifest.iceberg_job_id) -Field "iceberg_job_id"

    $savepoint = [string]$Manifest.savepoint_path
    if ($savepoint -notmatch "^file:/{1,3}workspace/tmp/savepoints/chapter-9/[^/]+$") {
        throw "Manifest savepoint_path is outside the Chapter 9 savepoint directory."
    }

    if ($Manifest.raw_offsets -is [string]) {
        throw "Manifest raw_offsets must be an array of partition offsets."
    }
    $offsets = @($Manifest.raw_offsets)
    if ($offsets.Count -eq 0) { throw "Manifest raw_offsets is empty." }
    $partitions = @{}
    foreach ($offset in $offsets) {
        $value = [string]$offset
        if ($value -notmatch "^partition:(\d+),offset:(\d+)$") {
            throw "Invalid raw offset in cutover manifest: $value"
        }
        $partition = [int]$Matches[1]
        if ($partitions.ContainsKey($partition)) { throw "Duplicate raw offset partition: $partition." }
        $partitions[$partition] = $true
    }

    $cleanIds = @(Get-RollbackCleanEventIds -Manifest $Manifest)
    if ($cleanIds.Count -gt 0 -and $cleanIds.Count -ne 2) {
        throw "Manifest clean event IDs must contain exactly two IDs when present."
    }
    if ($cleanIds.Count -eq 2) {
        if ($cleanIds | Where-Object { [string]::IsNullOrWhiteSpace([string]$_) }) {
            throw "Manifest clean event IDs must be non-empty."
        }
        if (@($cleanIds | Select-Object -Unique).Count -ne 2) {
            throw "Manifest clean event IDs must be distinct."
        }
    }
    return [pscustomobject]@{
        CutoverId = $cutoverId
        RawOffsets = @($offsets | ForEach-Object { [string]$_ })
        CleanEventIds = $cleanIds
    }
}

function Get-RollbackCleanEventIds([object]$Manifest) {
    $direct = Get-RollbackProperty -Object $Manifest -Name "clean_event_ids" -Required $false
    if ($null -ne $direct) { return @($direct | ForEach-Object { [string]$_ }) }

    $eventIds = Get-RollbackProperty -Object $Manifest -Name "event_ids" -Required $false
    if ($null -eq $eventIds) { return @() }
    $clean = Get-RollbackProperty -Object $eventIds -Name "clean" -Required $false
    if ($null -ne $clean) { return @($clean | ForEach-Object { [string]$_ }) }
    $duplicate = Get-RollbackProperty -Object $eventIds -Name "duplicate" -Required $false
    $advancer = Get-RollbackProperty -Object $eventIds -Name "advancer" -Required $false
    if ($null -ne $duplicate -or $null -ne $advancer) {
        if ($null -eq $duplicate -or $null -eq $advancer) {
            throw "Manifest event_ids must include both duplicate and advancer clean IDs."
        }
        return @([string]$duplicate, [string]$advancer)
    }
    return @()
}

function Assert-RollbackLiveJobs([object]$Manifest, [object]$Jobs) {
    if ($null -eq $Jobs -or $null -eq $Jobs.jobs) { throw "Flink jobs overview is empty." }
    $expected = @(
        [pscustomobject]@{ Id = [string]$Manifest.production_job_id; Name = "chapter-9-datastream-quality-production" },
        [pscustomobject]@{ Id = [string]$Manifest.doris_job_id; Name = "chapter-9-doris-clean" },
        [pscustomobject]@{ Id = [string]$Manifest.iceberg_job_id; Name = "chapter-9-iceberg-clean" }
    )
    $validated = @()
    foreach ($item in $expected) {
        $byId = @($Jobs.jobs | Where-Object { [string]$_.jid -eq $item.Id })
        $byName = @($Jobs.jobs | Where-Object { [string]$_.name -eq $item.Name })
        if ($byId.Count -ne 1 -or $byName.Count -ne 1 -or
            [string]$byId[0].name -ne $item.Name -or [string]$byId[0].state -ne "RUNNING" -or
            [string]$byName[0].jid -ne $item.Id) {
            throw "Manifest does not match exact RUNNING Job ID/name for $($item.Name)."
        }
        $validated += $byId[0]
    }
    if (@($validated | Select-Object -ExpandProperty jid -Unique).Count -ne 3) {
        throw "Manifest production Job IDs are not unique."
    }
    return @($validated)
}

function ConvertTo-RollbackOffsetsLiteral([string[]]$RawOffsets) {
    $validated = @{}
    foreach ($offset in $RawOffsets) {
        if ($offset -notmatch "^partition:(\d+),offset:(\d+)$") {
            throw "Invalid rollback offset: $offset"
        }
        $partition = [int]$Matches[1]
        if ($validated.ContainsKey($partition)) { throw "Duplicate rollback partition: $partition." }
        $validated[$partition] = [int64]$Matches[2]
    }
    return @($validated.Keys | Sort-Object | ForEach-Object { "partition:$($_),offset:$($validated[$_])" }) -join ";"
}

function Write-RollbackFileAtomic([string]$Path, [string]$Content) {
    $nextPath = "$Path.next"
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($nextPath, $Content.Trim() + "`r`n", $utf8NoBom)
    [void](Move-Item -LiteralPath $nextPath -Destination $Path -Force)
}

function Render-RollbackSql {
    param(
        [string]$Template,
        [string[]]$RawOffsets,
        [string]$CutoverId,
        [string]$DorisPath,
        [string]$IcebergPath,
        [string]$DorisSink,
        [string]$DorisInsert,
        [string]$IcebergCatalog,
        [string]$IcebergInsert
    )

    $specificOffsets = ConvertTo-RollbackOffsetsLiteral -RawOffsets $RawOffsets
    $source = $Template.Replace("__SPECIFIC_OFFSETS__", $specificOffsets)
    $dorisGroup = "chapter9-doris-raw-rollback-$CutoverId"
    $icebergGroup = "chapter9-iceberg-raw-rollback-$CutoverId"
    $dorisSource = $source.Replace("__ROLLBACK_GROUP_ID__", $dorisGroup)
    $icebergSource = $source.Replace("__ROLLBACK_GROUP_ID__", $icebergGroup)
    $dorisSql = @(
        "SET 'pipeline.name' = '$dorisGroup';", $dorisSource, $DorisSink, $DorisInsert
    ) -join "`r`n`r`n"
    $icebergSql = @(
        "SET 'execution.checkpointing.interval' = '10 s';",
        "SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';",
        "SET 'pipeline.name' = '$icebergGroup';", $icebergSource, $IcebergCatalog, $IcebergInsert
    ) -join "`r`n`r`n"
    Write-RollbackFileAtomic -Path $DorisPath -Content $dorisSql
    Write-RollbackFileAtomic -Path $IcebergPath -Content $icebergSql
    return [pscustomobject]@{ DorisGroup = $dorisGroup; IcebergGroup = $icebergGroup }
}

function Get-SubmittedRollbackJobId([string[]]$Lines) {
    $ids = @()
    foreach ($line in $Lines) {
        foreach ($match in [regex]::Matches([string]$line, "(?i)\b[0-9a-f]{32}\b")) {
            $ids += $match.Value.ToLowerInvariant()
        }
    }
    $unique = @($ids | Sort-Object -Unique)
    if ($unique.Count -ne 1) { throw "Expected exactly one submitted rollback Job ID, found $($unique.Count)." }
    return [string]$unique[0]
}

function Submit-RollbackSqlJob {
    param(
        [string]$SqlClient,
        [string]$ContainerSqlPath,
        [string[]]$ConnectorPaths,
        [string]$ParentClasspath = ""
    )
    $args = @("exec")
    if ($ParentClasspath) { $args += @("-e", "HADOOP_CLASSPATH=$ParentClasspath") }
    $args += @($SqlClient, "/opt/flink/bin/sql-client.sh")
    if ($ParentClasspath) { $args += @("-D", "classloader.resolve-order=parent-first") }
    foreach ($path in $ConnectorPaths) { $args += @("-j", $path) }
    $args += @("-f", $ContainerSqlPath)
    $output = Invoke-DockerCommand -Arguments $args -FailureMessage "Flink SQL submission failed for $ContainerSqlPath."
    if (($output -join "`n") -match "\[ERROR\]") {
        throw "Flink SQL reported an error for $ContainerSqlPath. Output: $($output -join "`n")"
    }
    return @($output)
}

function Wait-RollbackJobRunning {
    param(
        [string]$JobId,
        [string]$ExpectedName,
        [int]$Attempts = 60,
        [int]$SleepSeconds = 2
    )
    $lastState = "NOT_FOUND"
    $lastError = $null
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        try {
            $job = Get-FlinkJob -JobId $JobId
            if ([string]$job.name -ne $ExpectedName) { throw "Job $JobId has unexpected name $($job.name)." }
            $lastState = [string]$job.state
            if ($lastState -eq "RUNNING") { return $job }
            if ($lastState -in @("FAILED", "CANCELED", "FINISHED", "SUSPENDED")) {
                throw "Job $JobId entered terminal state $lastState."
            }
        } catch {
            $lastError = $_.Exception.Message
            if ($lastError -match "unexpected name|terminal state") { throw }
        }
        if ($attempt -lt $Attempts -and $SleepSeconds -gt 0) { Start-Sleep -Seconds $SleepSeconds }
    }
    throw "Job $JobId did not reach RUNNING. Last state: $lastState. Last error: $lastError"
}

function Wait-RollbackJobStopped {
    param(
        [string]$JobId,
        [int]$Attempts = 60,
        [int]$SleepSeconds = 2
    )
    $lastState = "NOT_FOUND"
    $lastError = $null
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        try {
            $job = Get-FlinkJob -JobId $JobId
            $lastState = [string]$job.state
            if ($lastState -in @("CANCELED", "FINISHED", "SUSPENDED", "FAILED")) { return $job }
        } catch {
            $lastError = $_.Exception.Message
            if ($lastError -match "404|Not Found|does not exist") {
                return [pscustomobject]@{ jid = $JobId; state = "NOT_FOUND" }
            }
        }
        if ($attempt -lt $Attempts -and $SleepSeconds -gt 0) { Start-Sleep -Seconds $SleepSeconds }
    }
    throw "Job $JobId did not stop. Last state: $lastState. Last error: $lastError"
}

function Get-RollbackCheckpointSummary([string]$JobId) {
    try {
        $checkpoints = Invoke-RestMethod -Uri "http://localhost:8081/jobs/$JobId/checkpoints" -TimeoutSec 5
        $latest = $checkpoints.latest.completed
        return [pscustomobject]@{
            Completed = [int64]$checkpoints.counts.completed
            LatestId = if ($null -eq $latest) { "NONE" } else { [string]$latest.id }
            LatestStatus = if ($null -eq $latest) { "NONE" } else { [string]$latest.status }
        }
    } catch {
        throw "Checkpoint lookup failed for Job $JobId. Last error: $($_.Exception.Message)"
    }
}

function Write-RollbackJobEvidence([object[]]$Jobs) {
    foreach ($job in $Jobs) {
        $checkpoint = Get-RollbackCheckpointSummary -JobId ([string]$job.jid)
        Write-Host "[job] id=$($job.jid) name=$($job.name) state=$($job.state) checkpoints_completed=$($checkpoint.Completed) latest_checkpoint=$($checkpoint.LatestId) latest_status=$($checkpoint.LatestStatus)"
    }
}

if ($FunctionsOnly) { return }
if (-not $TrafficPaused) { throw "Rollback requires explicit -TrafficPaused confirmation." }

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$tmpRoot = Join-Path $root "tmp/chapter-9"
$manifestPath = Join-Path $tmpRoot "cutover-manifest.json"
$templatePath = Join-Path $root "jobs/sql/15_source_user_behavior_raw_rollback.sql.template"
$dorisPath = Join-Path $tmpRoot "rollback-doris-raw.sql"
$icebergPath = Join-Path $tmpRoot "rollback-iceberg-raw.sql"
$manifest = Get-Content -Raw -Encoding UTF8 $manifestPath | ConvertFrom-Json
$validated = Assert-RollbackManifest -Manifest $manifest

$jobs = Get-FlinkJobs
$liveJobs = @(Assert-RollbackLiveJobs -Manifest $manifest -Jobs $jobs)
Write-Host "[rollback] cutover_id=$($validated.CutoverId) raw_offsets=$($validated.RawOffsets -join ';')"
Write-RollbackJobEvidence -Jobs $liveJobs

$verificationPath = Join-Path $tmpRoot "production-verification.json"
if ($validated.CleanEventIds.Count -eq 0 -and (Test-Path -LiteralPath $verificationPath -PathType Leaf)) {
    $verification = Get-Content -Raw -Encoding UTF8 $verificationPath | ConvertFrom-Json
    if ([string]$verification.cutover_id -eq $validated.CutoverId) {
        $verifiedClean = @($verification.event_ids.duplicate, $verification.event_ids.advancer)
        if ($verifiedClean.Count -eq 2 -and $verifiedClean[0] -and $verifiedClean[1]) {
            $validated.CleanEventIds = $verifiedClean
        }
    }
}
if ($validated.CleanEventIds.Count -eq 2) {
    Write-Host "[rollback] clean_event_ids=$($validated.CleanEventIds -join ';')"
} else {
    Write-Host "[rollback] clean_event_ids=NOT_RECORDED"
}

$template = Get-Content -Raw -Encoding UTF8 $templatePath
$dorisSink = Get-Content -Raw -Encoding UTF8 (Join-Path $root "jobs/sql/04_sink_doris_metrics.sql")
$dorisInsert = Get-Content -Raw -Encoding UTF8 (Join-Path $root "jobs/sql/05_pv_uv_to_doris.sql")
$icebergCatalog = Get-Content -Raw -Encoding UTF8 (Join-Path $root "jobs/sql/06_create_iceberg_catalog.sql")
$icebergInsert = Get-Content -Raw -Encoding UTF8 (Join-Path $root "jobs/sql/07_sink_user_behavior_to_iceberg.sql")
$rendered = Render-RollbackSql -Template $template -RawOffsets $validated.RawOffsets `
    -CutoverId $validated.CutoverId -DorisPath $dorisPath -IcebergPath $icebergPath `
    -DorisSink $dorisSink -DorisInsert $dorisInsert -IcebergCatalog $icebergCatalog -IcebergInsert $icebergInsert
Write-Host "[rollback] rendered_doris=$dorisPath group=$($rendered.DorisGroup)"
Write-Host "[rollback] rendered_iceberg=$icebergPath group=$($rendered.IcebergGroup)"

if ($DryRun) {
    Write-Host "[rollback] DRY_RUN no stop/cancel/submit; production jobs remain unchanged."
    Write-Host "[plan] stop production $($manifest.production_job_id) with --savepointPath file:///workspace/tmp/savepoints/chapter-9"
    Write-Host "[plan] stop Doris clean $($manifest.doris_job_id)"
    Write-Host "[plan] stop Iceberg clean $($manifest.iceberg_job_id)"
    Write-Host "[plan] start Doris rollback job by submitting $dorisPath as $($rendered.DorisGroup)"
    Write-Host "[plan] start Iceberg rollback job by submitting $icebergPath as $($rendered.IcebergGroup)"
    Write-Host "[plan] require both rollback jobs RUNNING; traffic resume remains operator-controlled"
    return
}

$jobManager = "ecom-flink-jobmanager"
$sqlClient = "ecom-flink-sql-client"
$savepointRoot = "file:///workspace/tmp/savepoints/chapter-9"
$dorisJobName = $rendered.DorisGroup
$icebergJobName = $rendered.IcebergGroup

Write-Host "[rollback] revalidating exact production jobs before mutation"
Assert-RollbackLiveJobs -Manifest $manifest -Jobs (Get-FlinkJobs) | Out-Null

Write-Host "[rollback] stopping production DataStream with Savepoint"
Invoke-DockerCommand -Arguments @(
    "exec", $jobManager, "/opt/flink/bin/flink", "stop", "--savepointPath", $savepointRoot,
    $manifest.production_job_id
) -FailureMessage "Production DataStream Stop-with-Savepoint failed." | ForEach-Object { Write-Host $_ }
Wait-RollbackJobStopped -JobId $manifest.production_job_id | Out-Null

Write-Host "[rollback] stopping exact Doris clean job"
Invoke-DockerCommand -Arguments @(
    "exec", $jobManager, "/opt/flink/bin/flink", "cancel", $manifest.doris_job_id
) -FailureMessage "Doris clean job cancel failed." | ForEach-Object { Write-Host $_ }
Wait-RollbackJobStopped -JobId $manifest.doris_job_id | Out-Null

Write-Host "[rollback] stopping exact Iceberg clean job"
Invoke-DockerCommand -Arguments @(
    "exec", $jobManager, "/opt/flink/bin/flink", "cancel", $manifest.iceberg_job_id
) -FailureMessage "Iceberg clean job cancel failed." | ForEach-Object { Write-Host $_ }
Wait-RollbackJobStopped -JobId $manifest.iceberg_job_id | Out-Null

$dorisConnectors = @(
    "/workspace/tmp/chapter-9/lib/flink-sql-connector-kafka-3.3.0-1.19.jar",
    "/workspace/tmp/chapter-9/lib/flink-doris-connector-1.19-25.1.0.jar"
)
$icebergConnectors = @(
    "/workspace/tmp/chapter-9/lib/flink-sql-connector-kafka-3.3.0-1.19.jar",
    "/workspace/tmp/chapter-9/lib/iceberg-flink-runtime-1.19-1.6.1.jar",
    "/workspace/tmp/chapter-9/lib/iceberg-aws-bundle-1.6.1.jar",
    "/workspace/tmp/chapter-9/lib/hadoop-client-api-3.3.6.jar",
    "/workspace/tmp/chapter-9/lib/hadoop-client-runtime-3.3.6.jar",
    "/workspace/tmp/chapter-9/lib/hadoop-aws-3.3.6.jar",
    "/workspace/tmp/chapter-9/lib/aws-java-sdk-bundle-1.12.262.jar"
)
$icebergClasspath = $icebergConnectors -join ":"

Write-Host "[rollback] submitting raw Doris rollback SQL"
$dorisOutput = Submit-RollbackSqlJob -SqlClient $sqlClient -ContainerSqlPath "/workspace/tmp/chapter-9/rollback-doris-raw.sql" `
    -ConnectorPaths $dorisConnectors
$dorisOutput | ForEach-Object { Write-Host $_ }

Write-Host "[rollback] submitting raw Iceberg rollback SQL"
$icebergOutput = Submit-RollbackSqlJob -SqlClient $sqlClient -ContainerSqlPath "/workspace/tmp/chapter-9/rollback-iceberg-raw.sql" `
    -ConnectorPaths $icebergConnectors -ParentClasspath $icebergClasspath
$icebergOutput | ForEach-Object { Write-Host $_ }

$rollbackDorisId = Wait-RollbackJobRunning -JobId (Get-SubmittedRollbackJobId -Lines $dorisOutput) -ExpectedName $dorisJobName
$rollbackIcebergId = Wait-RollbackJobRunning -JobId (Get-SubmittedRollbackJobId -Lines $icebergOutput) -ExpectedName $icebergJobName
Write-Host "[rollback] RUNNING doris_job_id=$($rollbackDorisId.jid) name=$($rollbackDorisId.name)"
Write-Host "[rollback] RUNNING iceberg_job_id=$($rollbackIcebergId.jid) name=$($rollbackIcebergId.name)"
Write-Host "[rollback] traffic may be resumed by the operator; generator was not started."
