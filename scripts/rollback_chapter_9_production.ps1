param(
    [switch]$TrafficPaused,
    [switch]$DryRun,
    [switch]$Resume,
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

function Assert-RollbackSavepointPath([string]$Path, [string]$EvidenceName = "manifest") {
    if ([string]::IsNullOrWhiteSpace($Path) -or $Path -match "\.\.") {
        throw "Invalid $EvidenceName savepoint evidence: $Path"
    }
    if ($Path -notmatch "^file:/workspace/tmp/savepoints/chapter-9/[A-Za-z0-9._-]+$") {
        throw "Invalid $EvidenceName savepoint evidence: $Path"
    }
    return $Path
}

function Assert-RollbackManifest([object]$Manifest) {
    if ($null -eq $Manifest) { throw "Cutover manifest is empty." }
    $allowedFields = @(
        "cutover_id", "created_at", "raw_offsets", "shadow_job_id", "savepoint_path",
        "production_job_id", "doris_job_id", "iceberg_job_id", "clean_event_ids"
    )
    $actualFields = @($Manifest.PSObject.Properties.Name)
    $unknownFields = @($actualFields | Where-Object { $_ -notin $allowedFields })
    if ($unknownFields.Count -gt 0) {
        throw "Manifest contains unknown fields: $($unknownFields -join ',')."
    }
    $requiredFields = @(
        "cutover_id", "created_at", "shadow_job_id", "savepoint_path",
        "production_job_id", "doris_job_id", "iceberg_job_id"
    )
    foreach ($field in $requiredFields) {
        $property = $Manifest.PSObject.Properties[$field]
        if ($null -eq $property -or $null -eq $property.Value -or $property.Value -isnot [string]) {
            throw "Manifest field $field must be a non-null string."
        }
    }

    $cutoverId = [string]$Manifest.cutover_id
    if ($cutoverId -notmatch "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$") {
        throw "Manifest cutover_id is not a canonical lowercase GUID."
    }
    try {
        $createdAt = [DateTimeOffset]::ParseExact(
            [string]$Manifest.created_at, "o",
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::RoundtripKind
        )
        if ($createdAt.ToString("o", [System.Globalization.CultureInfo]::InvariantCulture) -ne [string]$Manifest.created_at) {
            throw "round-trip mismatch"
        }
    } catch {
        throw "Manifest created_at must be round-trip ISO-8601."
    }
    Assert-RollbackJobId -JobId ([string]$Manifest.shadow_job_id) -Field "shadow_job_id"
    Assert-RollbackJobId -JobId ([string]$Manifest.production_job_id) -Field "production_job_id"
    Assert-RollbackJobId -JobId ([string]$Manifest.doris_job_id) -Field "doris_job_id"
    Assert-RollbackJobId -JobId ([string]$Manifest.iceberg_job_id) -Field "iceberg_job_id"

    $savepoint = Assert-RollbackSavepointPath -Path ([string]$Manifest.savepoint_path)

    if ($Manifest.raw_offsets -isnot [array]) {
        throw "Manifest raw_offsets must be an array of partition offsets."
    }
    $offsets = @($Manifest.raw_offsets)
    if ($offsets.Count -eq 0) { throw "Manifest raw_offsets is empty." }
    $partitions = @{}
    foreach ($offset in $offsets) {
        if ($offset -isnot [string]) { throw "Manifest raw_offsets elements must be strings." }
        $value = [string]$offset
        if ($value -notmatch "^partition:(\d+),offset:(\d+)$") {
            throw "Invalid raw offset in cutover manifest: $value"
        }
        try {
            $partition = [int64]$Matches[1]
            $offsetValue = [int64]$Matches[2]
        } catch {
            throw "Raw offset is outside the supported non-negative integer range: $value"
        }
        if ($partition -lt 0 -or $offsetValue -lt 0) { throw "Raw offsets cannot be negative: $value" }
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
    $property = $Manifest.PSObject.Properties["clean_event_ids"]
    if ($null -eq $property) { return @() }
    if ($null -eq $property.Value -or $property.Value -isnot [array]) {
        throw "Manifest clean_event_ids must be an array when present."
    }
    $ids = @($property.Value)
    if ($ids.Count -ne 2) { throw "Manifest clean_event_ids must contain exactly two IDs." }
    foreach ($id in $ids) {
        if ($id -isnot [string] -or [string]$id -notmatch "^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$") {
            throw "Manifest clean_event_ids elements have an invalid format."
        }
    }
    if (@($ids | Select-Object -Unique).Count -ne 2) {
        throw "Manifest clean_event_ids must be distinct."
    }
    return @($ids | ForEach-Object { [string]$_ })
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
        $byName = @($Jobs.jobs | Where-Object {
            [string]$_.name -eq $item.Name -and [string]$_.state -eq "RUNNING"
        })
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

function Select-RollbackEvidenceJobs([object]$Manifest, [object]$Jobs) {
    if ($null -eq $Jobs -or $null -eq $Jobs.jobs) { throw "Flink jobs overview is empty." }
    $manifestIds = @(
        [string]$Manifest.production_job_id,
        [string]$Manifest.doris_job_id,
        [string]$Manifest.iceberg_job_id
    )
    return @($Jobs.jobs | Where-Object {
        [string]$_.jid -in $manifestIds -or [string]$_.state -eq "RUNNING"
    } | Group-Object { [string]$_.jid } | ForEach-Object { $_.Group[0] })
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

function Get-RollbackSavepointPath([string[]]$Lines) {
    $paths = @()
    foreach ($line in $Lines) {
        $match = [regex]::Match(
            [string]$line,
            "(?i)Savepoint completed\.\s*Path:\s*(file:/workspace/tmp/savepoints/chapter-9/[^\s]+)"
        )
        if ($match.Success) { $paths += $match.Groups[1].Value }
    }
    $uniquePaths = @($paths | Sort-Object -Unique)
    if ($uniquePaths.Count -ne 1) {
        $evidence = if ($uniquePaths.Count -eq 0) { "MISSING" } else { $uniquePaths -join ";" }
        throw "Stop-with-savepoint output must contain exactly one valid path. savepoint evidence=$evidence"
    }
    return Assert-RollbackSavepointPath -Path ([string]$uniquePaths[0]) -EvidenceName "stop output"
}

function Get-RollbackSavepointDirectorySnapshot([string]$JobManager = "ecom-flink-jobmanager") {
    $output = Invoke-DockerCommand -Arguments @(
        "exec", $JobManager, "sh", "-lc",
        "find /workspace/tmp/savepoints/chapter-9 -mindepth 1 -maxdepth 1 -type d -printf '%f\n' 2>/dev/null | sort"
    ) -FailureMessage "Unable to snapshot the rollback savepoint directory."
    return @($output | ForEach-Object {
        $value = ([string]$_).Trim()
        if ($value -and $value -match '^[A-Za-z0-9._-]+$') { $value }
    } | Sort-Object -Unique)
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
            $returnedId = [string]$job.jid
            if ($returnedId -ne $JobId) {
                throw "Rollback Job ID mismatch: requested=$JobId returned=$returnedId."
            }
            if ([string]$job.name -ne $ExpectedName) { throw "Job $JobId has unexpected name $($job.name)." }
            $lastState = [string]$job.state
            if ($lastState -eq "RUNNING") {
                return [pscustomobject]@{
                    RequestedJobId = $JobId
                    ReturnedJobId = $returnedId
                    Name = [string]$job.name
                    State = $lastState
                    Job = $job
                }
            }
            if ($lastState -in @("FAILED", "CANCELED", "FINISHED", "SUSPENDED")) {
                throw "Job $JobId entered terminal state $lastState."
            }
        } catch {
            $lastError = $_.Exception.Message
            if ($lastError -match "mismatch|unexpected name|terminal state") { throw }
        }
        if ($attempt -lt $Attempts -and $SleepSeconds -gt 0) { Start-Sleep -Seconds $SleepSeconds }
    }
    throw "Job $JobId did not reach RUNNING. Last state: $lastState. Last error: $lastError"
}

function Wait-RollbackProductionFinished {
    param(
        [string]$JobId,
        [string]$ExpectedName,
        [string]$SavepointPath,
        [int]$Attempts = 60,
        [int]$SleepSeconds = 2
    )
    $lastState = "NOT_FOUND"
    $lastError = $null
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        try {
            $job = Get-FlinkJob -JobId $JobId
            $returnedId = [string]$job.jid
            if ($returnedId -ne $JobId) {
                throw "Production Job ID mismatch: requested=$JobId returned=$returnedId."
            }
            if ([string]$job.name -ne $ExpectedName) {
                throw "Production Job $JobId has unexpected name $($job.name)."
            }
            $lastState = [string]$job.state
            if ($lastState -eq "FINISHED") {
                return [pscustomobject]@{
                    RequestedJobId = $JobId
                    ReturnedJobId = $returnedId
                    Name = [string]$job.name
                    State = $lastState
                    SavepointPath = $SavepointPath
                    Job = $job
                }
            }
            if ($lastState -in @("FAILED", "CANCELED", "SUSPENDED")) {
                throw "Production JobID=$JobId failed to stop safely. last_state=$lastState savepoint evidence=$SavepointPath"
            }
        } catch {
            $lastError = $_.Exception.Message
            if ($lastError -match "mismatch|unexpected name|failed to stop safely") { throw }
        }
        if ($attempt -lt $Attempts -and $SleepSeconds -gt 0) { Start-Sleep -Seconds $SleepSeconds }
    }
    throw "Production JobID=$JobId did not reach FINISHED. last_state=$lastState savepoint evidence=$SavepointPath last_error=$lastError"
}

function Wait-RollbackCleanCanceled {
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
            $returnedId = [string]$job.jid
            if ($returnedId -ne $JobId) {
                throw "Clean Job ID mismatch: requested=$JobId returned=$returnedId."
            }
            if ([string]$job.name -ne $ExpectedName) {
                throw "Clean Job $JobId has unexpected name $($job.name)."
            }
            $lastState = [string]$job.state
            if ($lastState -eq "CANCELED") {
                return [pscustomobject]@{
                    RequestedJobId = $JobId
                    ReturnedJobId = $returnedId
                    Name = [string]$job.name
                    State = $lastState
                    Job = $job
                }
            }
            if ($lastState -in @("FAILED", "FINISHED", "SUSPENDED")) {
                throw "Clean JobID=$JobId cancel failed. last_state=$lastState"
            }
        } catch {
            $lastError = $_.Exception.Message
            if ($lastError -match "mismatch|unexpected name|cancel failed") { throw }
        }
        if ($attempt -lt $Attempts -and $SleepSeconds -gt 0) { Start-Sleep -Seconds $SleepSeconds }
    }
    throw "Clean JobID=$JobId did not reach CANCELED. last_state=$lastState last_error=$lastError"
}

function Write-RollbackDryRunPlan {
    param(
        [string]$ProductionJobId,
        [string]$DorisJobId,
        [string]$IcebergJobId,
        [string]$DorisGroup,
        [string]$IcebergGroup,
        [string]$DorisPath,
        [string]$IcebergPath
    )
    Write-Host "[rollback] DRY_RUN no stop/cancel/submit; production jobs remain unchanged."
    Write-Host "[plan] stop production $ProductionJobId with --savepointPath file:///workspace/tmp/savepoints/chapter-9"
    Write-Host "[plan] stop Doris clean $DorisJobId"
    Write-Host "[plan] stop Iceberg clean $IcebergJobId"
    Write-Host "[plan] start Doris rollback job by submitting $DorisPath as $DorisGroup"
    Write-Host "[plan] start Iceberg rollback job by submitting $IcebergPath as $IcebergGroup"
    Write-Host "[plan] require both rollback jobs RUNNING; traffic resume remains operator-controlled"
}

function Write-RollbackProgressAtomic {
    param(
        [Parameter(Mandatory = $true)][object]$Progress,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $nextPath = "$Path.next"
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Path) | Out-Null
    [System.IO.File]::WriteAllText($nextPath, (($Progress | ConvertTo-Json -Depth 15) + "`n"), (New-Object System.Text.UTF8Encoding($false)))
    Move-Item -LiteralPath $nextPath -Destination $Path -Force
}

function New-RollbackMutationState {
    return [pscustomobject]@{ status = "not_started"; intent = $null; result = $null }
}

function New-RollbackProgress {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][hashtable]$ManifestIds
    )

    $progress = [pscustomobject]@{
        schema_version = 1
        status = "in_progress"
        started_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
        manifest_ids = [pscustomobject]$ManifestIds
        stages = [pscustomobject]@{
            production_stop = New-RollbackMutationState
            doris_cancel = New-RollbackMutationState
            iceberg_cancel = New-RollbackMutationState
            doris_submit = New-RollbackMutationState
            iceberg_submit = New-RollbackMutationState
            finalization = New-RollbackMutationState
        }
        rollback_jobs = [pscustomobject]@{ doris = $null; iceberg = $null }
    }
    Write-RollbackProgressAtomic -Progress $progress -Path $Path
    return $progress
}

function Invoke-RollbackMutation {
    param(
        [Parameter(Mandatory = $true)][object]$Progress,
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Stage,
        [Parameter(Mandatory = $true)][string]$Operation,
        [hashtable]$Details = @{},
        [Parameter(Mandatory = $true)][scriptblock]$Action
    )

    $stageState = $Progress.stages.$Stage
    if ($null -eq $stageState) { throw "Unknown rollback mutation stage: $Stage" }
    $stageState.status = "intent"
    $stageState.intent = [pscustomobject]@{
        operation = $Operation
        details = [pscustomobject]$Details
        created_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
    }
    [void](Write-RollbackProgressAtomic -Progress $Progress -Path $Path)
    try {
        $value = & $Action
        $stageState.status = "result"
        $result = [pscustomobject]@{
            status = "result"
            completed_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
        }
        if ($value -is [pscustomobject]) {
            foreach ($property in $value.PSObject.Properties) {
                $result | Add-Member -Force -NotePropertyName $property.Name -NotePropertyValue $property.Value
            }
        }
        $stageState.result = $result
        [void](Write-RollbackProgressAtomic -Progress $Progress -Path $Path)
        return $value
    } catch {
        $stageState.status = "failed"
        $stageState.result = [pscustomobject]@{
            status = "failed"
            error = $_.Exception.Message
            completed_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
        }
        [void](Write-RollbackProgressAtomic -Progress $Progress -Path $Path)
        throw
    }
}

function Get-RollbackResumePlan {
    param(
        [Parameter(Mandatory = $true)][object]$Progress,
        [Parameter(Mandatory = $true)][string]$Stage,
        [Parameter(Mandatory = $true)][object]$Job,
        [string[]]$SavepointCandidates = @()
    )

    $expected = switch -Regex ($Stage) {
        "production" { "chapter-9-datastream-quality-production"; break }
        "doris" { "chapter-9-doris-clean"; break }
        "iceberg" { "chapter-9-iceberg-clean"; break }
        default { throw "Unknown rollback reconciliation stage: $Stage" }
    }
    if ([string]$Job.name -ne $expected) { throw "Rollback Job name mismatch for $Stage." }
    $state = $Progress.stages.$Stage
    if ($null -eq $state -or $null -eq $state.intent) {
        throw "Rollback resume requires a persisted intent for $Stage."
    }
    $jobState = [string]$Job.state
    if ($Stage -eq "production_stop") {
        if ($jobState -eq "RUNNING") { return [pscustomobject]@{ Action = "retry_stop" } }
        if ($jobState -eq "FINISHED" -and $state.result.savepoint_path -and $state.result.new_savepoint_verified) {
            return [pscustomobject]@{ Action = "complete" }
        }
        if ($jobState -eq "FINISHED") {
            $before = @($state.intent.details.savepoint_directory_snapshot)
            $newCandidates = @($SavepointCandidates | Where-Object { $_ -notin $before })
            if ($newCandidates.Count -eq 1) {
                $recoveredPath = Assert-RollbackSavepointPath `
                    -Path "file:/workspace/tmp/savepoints/chapter-9/$($newCandidates[0])" `
                    -EvidenceName "recovered directory snapshot"
                return [pscustomobject]@{ Action = "recover_savepoint"; SavepointPath = $recoveredPath }
            }
        }
        throw "Production resume is fail-closed for state $jobState without a verified new savepoint."
    }
    if ($jobState -eq "CANCELED") { return [pscustomobject]@{ Action = "complete" } }
    if ($jobState -eq "RUNNING") { return [pscustomobject]@{ Action = "retry_cancel" } }
    throw "Rollback clean-job resume is fail-closed for state $jobState."
}

function Resolve-RollbackSubmittedJob {
    param(
        [Parameter(Mandatory = $true)][object]$Progress,
        [Parameter(Mandatory = $true)][string]$Stage,
        [Parameter(Mandatory = $true)][string]$ExpectedName,
        [Parameter(Mandatory = $true)][object]$Jobs,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $state = $Progress.stages.$Stage
    if ($null -eq $state -or $null -eq $state.intent) { throw "No rollback submission intent exists for $ExpectedName." }
    $jobId = [string]$state.result.job_id
    if ($jobId) {
        $byId = @($Jobs.jobs | Where-Object { [string]$_.jid -eq $jobId })
        if ($byId.Count -ne 1 -or [string]$byId[0].name -ne $ExpectedName -or [string]$byId[0].state -ne "RUNNING") {
            throw "Persisted rollback Job ID is not the exact RUNNING $ExpectedName."
        }
        return $jobId
    }
    $namedJobs = @($Jobs.jobs | Where-Object {
        [string]$_.name -eq $ExpectedName -and [string]$_.state -eq "RUNNING"
    })
    if ($namedJobs.Count -ne 1) { throw "Cannot adopt ${ExpectedName}: expected one exact-name RUNNING job." }
    if ([string]$namedJobs[0].state -ne "RUNNING" -or [string]$namedJobs[0].jid -notmatch "^[0-9a-f]{32}$") {
        throw "Cannot adopt ${ExpectedName}: job is terminal or malformed."
    }
    $jobId = [string]$namedJobs[0].jid
    $state.status = "result"
    $state.result = [pscustomobject]@{ status = "result"; job_id = $jobId; adopted = $true; completed_at_utc = [DateTimeOffset]::UtcNow.ToString("o") }
    Write-RollbackProgressAtomic -Progress $Progress -Path $Path
    return [string]$jobId
}

function Invoke-RollbackSubmissionStage {
    param(
        [Parameter(Mandatory = $true)][object]$Progress,
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][ValidateSet("doris_submit", "iceberg_submit")][string]$Stage,
        [Parameter(Mandatory = $true)][string]$ExpectedName,
        [Parameter(Mandatory = $true)][string]$Operation,
        [Parameter(Mandatory = $true)][object]$Jobs,
        [Parameter(Mandatory = $true)][scriptblock]$SubmitAction
    )

    $stageState = $Progress.stages.$Stage
    if ($null -eq $stageState) { throw "Unknown rollback submission stage: $Stage" }
    if ($null -eq $stageState.intent) {
        $submitted = Invoke-RollbackMutation -Progress $Progress -Path $Path -Stage $Stage `
            -Operation $Operation -Details @{ name = $ExpectedName } -Action $SubmitAction
        $jobId = [string]$submitted.job_id
        if ($jobId -notmatch '^[0-9a-f]{32}$') {
            throw "Rollback submission returned an invalid Job ID for $ExpectedName."
        }
    } else {
        $jobId = Resolve-RollbackSubmittedJob -Progress $Progress -Stage $Stage `
            -ExpectedName $ExpectedName -Jobs $Jobs -Path $Path
        $submitted = [pscustomobject]@{ output = @(); job_id = $jobId }
    }
    $key = if ($Stage -eq "doris_submit") { "doris" } else { "iceberg" }
    $Progress.rollback_jobs.$key = $jobId
    Write-RollbackProgressAtomic -Progress $Progress -Path $Path
    return $submitted
}

function Invoke-RollbackRealMode {
    param(
        [object]$Manifest,
        [string]$DorisJobName,
        [string]$IcebergJobName,
        [string]$DorisSqlPath,
        [string]$IcebergSqlPath,
        [string[]]$DorisConnectors,
        [string[]]$IcebergConnectors,
        [string]$IcebergClasspath,
        [string]$JobManager = "ecom-flink-jobmanager",
        [string]$SqlClient = "ecom-flink-sql-client",
        [AllowNull()][object]$Progress = $null,
        [string]$ProgressPath = "",
        [switch]$Resume,
        [int]$Attempts = 60,
        [int]$SleepSeconds = 2
    )

    Write-Host "[rollback] stopping production DataStream with Savepoint"
    $savepointPath = $null
    $productionResult = $null
    $productionJob = if ($Resume -and $null -ne $Progress) {
        Get-FlinkJob -JobId $Manifest.production_job_id
    } else { $null }
    $savepointSnapshot = if ($null -ne $Progress -and
        (-not $Resume -or -not $Progress.stages.production_stop.result.savepoint_path)) {
        @(Get-RollbackSavepointDirectorySnapshot -JobManager $JobManager)
    } else { @() }
    $productionAction = if ($Resume -and $null -ne $Progress) {
        Get-RollbackResumePlan -Progress $Progress -Stage "production_stop" -Job $productionJob `
            -SavepointCandidates $savepointSnapshot
    } else { [pscustomobject]@{ Action = "retry_stop" } }
    if ($productionAction.Action -eq "complete" -or $productionAction.Action -eq "recover_savepoint") {
        $savepointPath = if ($productionAction.SavepointPath) {
            [string]$productionAction.SavepointPath
        } else { [string]$Progress.stages.production_stop.result.savepoint_path }
        Assert-RollbackSavepointPath -Path $savepointPath -EvidenceName "progress" | Out-Null
        $productionResult = [pscustomobject]@{ RequestedJobId = $Manifest.production_job_id; ReturnedJobId = $Manifest.production_job_id; State = "FINISHED" }
        if ($productionAction.Action -eq "recover_savepoint") {
            $Progress.stages.production_stop.status = "result"
            $Progress.stages.production_stop.result = [pscustomobject]@{
                status = "result"
                savepoint_path = $savepointPath
                new_savepoint_verified = $true
                recovered_from_directory_snapshot = $true
                completed_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
            }
            Write-RollbackProgressAtomic -Progress $Progress -Path $ProgressPath
        }
    } else {
        $stopAction = {
            $stopOutput = @(Invoke-DockerCommand -Arguments @(
                "exec", $JobManager, "/opt/flink/bin/flink", "stop", "--savepointPath",
                "file:///workspace/tmp/savepoints/chapter-9", $Manifest.production_job_id
            ) -FailureMessage "Production DataStream Stop-with-Savepoint failed.")
            $stopOutput | ForEach-Object { Write-Host $_ }
            $newPath = Get-RollbackSavepointPath -Lines $stopOutput
            $result = Wait-RollbackProductionFinished -JobId $Manifest.production_job_id `
                -ExpectedName "chapter-9-datastream-quality-production" -SavepointPath $newPath `
                -Attempts $Attempts -SleepSeconds $SleepSeconds
            [pscustomobject]@{ savepoint_path = $newPath; new_savepoint_verified = $true; result = $result }
        }
        if ($null -ne $Progress) {
            $productionResult = Invoke-RollbackMutation -Progress $Progress -Path $ProgressPath `
                -Stage "production_stop" -Operation "stop_production_with_savepoint" `
                -Details @{ job_id = $Manifest.production_job_id; savepoint_directory_snapshot = $savepointSnapshot } `
                -Action $stopAction
        } else { $productionResult = & $stopAction }
        $savepointPath = [string]$productionResult.savepoint_path
        if ($null -ne $Progress) {
            $Progress.stages.production_stop.result | Add-Member -Force -NotePropertyName savepoint_path -NotePropertyValue $savepointPath
            $Progress.stages.production_stop.result | Add-Member -Force -NotePropertyName new_savepoint_verified -NotePropertyValue $true
            Write-RollbackProgressAtomic -Progress $Progress -Path $ProgressPath
        }
    }
    Write-Host "[rollback] production stopped requested_job_id=$($productionResult.RequestedJobId) returned_job_id=$($productionResult.ReturnedJobId) state=$($productionResult.State) savepoint=$savepointPath"

    Write-Host "[rollback] stopping exact Doris clean job"
    $dorisPlan = if ($Resume -and $null -ne $Progress) {
        Get-RollbackResumePlan -Progress $Progress -Stage "doris_cancel" -Job (Get-FlinkJob $Manifest.doris_job_id)
    } else { [pscustomobject]@{ Action = "retry_cancel" } }
    if ($dorisPlan.Action -eq "complete") {
        $dorisResult = [pscustomobject]@{ RequestedJobId = $Manifest.doris_job_id; ReturnedJobId = $Manifest.doris_job_id; State = "CANCELED" }
    } else {
        $cancelDoris = { Invoke-DockerCommand -Arguments @("exec", $JobManager, "/opt/flink/bin/flink", "cancel", $Manifest.doris_job_id) `
            -FailureMessage "Doris clean job cancel failed." | Out-Null; Wait-RollbackCleanCanceled -JobId $Manifest.doris_job_id `
            -ExpectedName "chapter-9-doris-clean" -Attempts $Attempts -SleepSeconds $SleepSeconds }
        if ($null -ne $Progress) {
            $dorisResult = Invoke-RollbackMutation -Progress $Progress -Path $ProgressPath -Stage "doris_cancel" `
                -Operation "cancel_doris_clean" -Details @{ job_id = $Manifest.doris_job_id } -Action $cancelDoris
        } else { $dorisResult = & $cancelDoris }
    }
    Write-Host "[rollback] Doris canceled requested_job_id=$($dorisResult.RequestedJobId) returned_job_id=$($dorisResult.ReturnedJobId) state=$($dorisResult.State)"

    Write-Host "[rollback] stopping exact Iceberg clean job"
    $icebergPlan = if ($Resume -and $null -ne $Progress) {
        Get-RollbackResumePlan -Progress $Progress -Stage "iceberg_cancel" -Job (Get-FlinkJob $Manifest.iceberg_job_id)
    } else { [pscustomobject]@{ Action = "retry_cancel" } }
    if ($icebergPlan.Action -eq "complete") {
        $icebergResult = [pscustomobject]@{ RequestedJobId = $Manifest.iceberg_job_id; ReturnedJobId = $Manifest.iceberg_job_id; State = "CANCELED" }
    } else {
        $cancelIceberg = { Invoke-DockerCommand -Arguments @("exec", $JobManager, "/opt/flink/bin/flink", "cancel", $Manifest.iceberg_job_id) `
            -FailureMessage "Iceberg clean job cancel failed." | Out-Null; Wait-RollbackCleanCanceled -JobId $Manifest.iceberg_job_id `
            -ExpectedName "chapter-9-iceberg-clean" -Attempts $Attempts -SleepSeconds $SleepSeconds }
        if ($null -ne $Progress) {
            $icebergResult = Invoke-RollbackMutation -Progress $Progress -Path $ProgressPath -Stage "iceberg_cancel" `
                -Operation "cancel_iceberg_clean" -Details @{ job_id = $Manifest.iceberg_job_id } -Action $cancelIceberg
        } else { $icebergResult = & $cancelIceberg }
    }
    Write-Host "[rollback] Iceberg canceled requested_job_id=$($icebergResult.RequestedJobId) returned_job_id=$($icebergResult.ReturnedJobId) state=$($icebergResult.State)"

    Write-Host "[rollback] submitting raw Doris rollback SQL"
    $dorisSubmit = {
        $output = @(Submit-RollbackSqlJob -SqlClient $SqlClient -ContainerSqlPath $DorisSqlPath -ConnectorPaths $DorisConnectors)
        [pscustomobject]@{ output = $output; job_id = (Get-SubmittedRollbackJobId -Lines $output) }
    }
    if ($null -ne $Progress) {
        $dorisJobs = if ($Progress.stages.doris_submit.intent) {
            Get-FlinkJobs
        } else { [pscustomobject]@{ jobs = @() } }
        $dorisSubmitted = Invoke-RollbackSubmissionStage -Progress $Progress -Path $ProgressPath `
            -Stage "doris_submit" -ExpectedName $DorisJobName -Operation "submit_doris_raw_rollback" `
            -Jobs $dorisJobs -SubmitAction $dorisSubmit
    } else { $dorisSubmitted = & $dorisSubmit }
    $dorisOutput = @($dorisSubmitted.output)
    $dorisOutput | ForEach-Object { Write-Host $_ }

    Write-Host "[rollback] submitting raw Iceberg rollback SQL"
    $icebergSubmit = {
        $output = @(Submit-RollbackSqlJob -SqlClient $SqlClient -ContainerSqlPath $IcebergSqlPath `
            -ConnectorPaths $IcebergConnectors -ParentClasspath $IcebergClasspath)
        [pscustomobject]@{ output = $output; job_id = (Get-SubmittedRollbackJobId -Lines $output) }
    }
    if ($null -ne $Progress) {
        $icebergJobs = if ($Progress.stages.iceberg_submit.intent) {
            Get-FlinkJobs
        } else { [pscustomobject]@{ jobs = @() } }
        $icebergSubmitted = Invoke-RollbackSubmissionStage -Progress $Progress -Path $ProgressPath `
            -Stage "iceberg_submit" -ExpectedName $IcebergJobName -Operation "submit_iceberg_raw_rollback" `
            -Jobs $icebergJobs -SubmitAction $icebergSubmit
    } else { $icebergSubmitted = & $icebergSubmit }
    $icebergOutput = @($icebergSubmitted.output)
    $icebergOutput | ForEach-Object { Write-Host $_ }

    $dorisRollbackJobId = [string]$dorisSubmitted.job_id
    $icebergRollbackJobId = [string]$icebergSubmitted.job_id
    $rollbackDoris = Wait-RollbackJobRunning -JobId $dorisRollbackJobId `
        -ExpectedName $DorisJobName -Attempts $Attempts -SleepSeconds $SleepSeconds
    $rollbackIceberg = Wait-RollbackJobRunning -JobId $icebergRollbackJobId `
        -ExpectedName $IcebergJobName -Attempts $Attempts -SleepSeconds $SleepSeconds
    Write-Host "[rollback] RUNNING requested_job_id=$($rollbackDoris.RequestedJobId) returned_job_id=$($rollbackDoris.ReturnedJobId) name=$($rollbackDoris.Name)"
    Write-Host "[rollback] RUNNING requested_job_id=$($rollbackIceberg.RequestedJobId) returned_job_id=$($rollbackIceberg.ReturnedJobId) name=$($rollbackIceberg.Name)"
    if ($null -ne $Progress) {
        $Progress.rollback_jobs.doris = [string]$rollbackDoris.RequestedJobId
        $Progress.rollback_jobs.iceberg = [string]$rollbackIceberg.RequestedJobId
        Write-RollbackProgressAtomic -Progress $Progress -Path $ProgressPath
    }
    return [pscustomobject]@{
        SavepointPath = $savepointPath
        DorisJob = $rollbackDoris
        IcebergJob = $rollbackIceberg
    }
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
$progressPath = Join-Path $tmpRoot "rollback-progress.json"
$templatePath = Join-Path $root "jobs/sql/15_source_user_behavior_raw_rollback.sql.template"
$dorisPath = Join-Path $tmpRoot "rollback-doris-raw.sql"
$icebergPath = Join-Path $tmpRoot "rollback-iceberg-raw.sql"
$manifest = Get-Content -Raw -Encoding UTF8 $manifestPath | ConvertFrom-Json
$validated = Assert-RollbackManifest -Manifest $manifest

if (-not $DryRun -and (Test-Path -LiteralPath $progressPath -PathType Leaf) -and -not $Resume) {
    throw "Rollback progress already exists: use explicit -Resume to reconcile $progressPath."
}
if (-not $DryRun -and $Resume -and -not (Test-Path -LiteralPath $progressPath -PathType Leaf)) {
    throw "-Resume requires rollback progress: $progressPath"
}

$jobs = Get-FlinkJobs
$liveJobs = if ($Resume) {
    @(Select-RollbackEvidenceJobs -Manifest $manifest -Jobs $jobs)
} else { @(Assert-RollbackLiveJobs -Manifest $manifest -Jobs $jobs) }
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
    Write-RollbackDryRunPlan -ProductionJobId $manifest.production_job_id -DorisJobId $manifest.doris_job_id `
        -IcebergJobId $manifest.iceberg_job_id -DorisGroup $rendered.DorisGroup -IcebergGroup $rendered.IcebergGroup `
        -DorisPath $dorisPath -IcebergPath $icebergPath
    return
}

$rollbackProgress = $null
if ($Resume) {
    $rollbackProgress = Get-Content -Raw -Encoding UTF8 $progressPath | ConvertFrom-Json
    foreach ($key in @("production", "doris", "iceberg")) {
        if ([string]$rollbackProgress.manifest_ids.$key -ne [string]$manifest.($key + "_job_id")) {
            throw "Rollback progress manifest identity mismatch for $key."
        }
    }
} else {
    $rollbackProgress = New-RollbackProgress -Path $progressPath -ManifestIds @{
        production = [string]$manifest.production_job_id
        doris = [string]$manifest.doris_job_id
        iceberg = [string]$manifest.iceberg_job_id
    }
}

$jobManager = "ecom-flink-jobmanager"
$sqlClient = "ecom-flink-sql-client"
$dorisJobName = $rendered.DorisGroup
$icebergJobName = $rendered.IcebergGroup

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
Write-Host "[rollback] revalidating exact production jobs before mutation"
if (-not $Resume) { Assert-RollbackLiveJobs -Manifest $manifest -Jobs (Get-FlinkJobs) | Out-Null }
Invoke-RollbackRealMode -Manifest $manifest -DorisJobName $dorisJobName -IcebergJobName $icebergJobName `
    -DorisSqlPath "/workspace/tmp/chapter-9/rollback-doris-raw.sql" `
    -IcebergSqlPath "/workspace/tmp/chapter-9/rollback-iceberg-raw.sql" `
    -DorisConnectors $dorisConnectors -IcebergConnectors $icebergConnectors -IcebergClasspath $icebergClasspath `
    -Progress $rollbackProgress -ProgressPath $progressPath -Resume:$Resume | Out-Null
$rollbackProgress.stages.finalization.status = "intent"
$rollbackProgress.stages.finalization.intent = [pscustomobject]@{
    operation = "finalize_rollback_progress"
    details = [pscustomobject]@{ production = $manifest.production_job_id; doris = $manifest.doris_job_id; iceberg = $manifest.iceberg_job_id }
    created_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
}
Write-RollbackProgressAtomic -Progress $rollbackProgress -Path $progressPath
$rollbackProgress.status = "complete"
$rollbackProgress.completed_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
$rollbackProgress.stages.finalization.status = "result"
$rollbackProgress.stages.finalization.result = [pscustomobject]@{
    status = "result"
    completed_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
}
Write-RollbackProgressAtomic -Progress $rollbackProgress -Path $progressPath
Write-Host "[rollback] traffic may be resumed by the operator; generator was not started."
