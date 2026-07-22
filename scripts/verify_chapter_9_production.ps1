[CmdletBinding()]
param(
    [switch]$FunctionsOnly,
    [int]$TimeoutSeconds = 180
)

$ErrorActionPreference = "Stop"

function Invoke-ProductionDockerCommand {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $rawOutput = & docker @Arguments 2>&1
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    $output = @($rawOutput | ForEach-Object {
        if ($_ -is [System.Management.Automation.ErrorRecord]) {
            $message = [string]$_.Exception.Message
            if (-not $message -and $null -ne $_.TargetObject) {
                $message = [string]$_.TargetObject
            }
            if ($message) { $message }
        } else {
            [string]$_
        }
    })
    if ($exitCode -ne 0) {
        $detail = ($output -join "`n").Trim()
        if ($detail) { throw "$FailureMessage Output: $detail" }
        throw $FailureMessage
    }
    return $output
}

function New-ProductionEventJson {
    param(
        [Parameter(Mandatory = $true)][string]$EventId,
        [Parameter(Mandatory = $true)][string]$UserId,
        [Parameter(Mandatory = $true)][string]$EventTime,
        [Parameter(Mandatory = $true)][string]$RunId
    )

    return [ordered]@{
        event_id = $EventId
        user_id = $UserId
        product_id = "$RunId-product"
        event_type = "view"
        event_time = $EventTime
        channel = "app"
        device_type = "android"
        page_id = "home"
    } | ConvertTo-Json -Compress
}

function Send-ProductionKafkaValue {
    param(
        [Parameter(Mandatory = $true)][string]$Topic,
        [Parameter(Mandatory = $true)][string]$Value,
        [string]$KafkaContainer = "ecom-kafka"
    )

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = $Value | & docker exec -i $KafkaContainer kafka-console-producer `
            --bootstrap-server kafka:29092 --topic $Topic 2>&1
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($exitCode -ne 0) {
        throw "Kafka event send failed for $Topic. Output: $($output -join "`n")"
    }
}

function Read-ProductionCommittedTopic {
    param(
        [Parameter(Mandatory = $true)][string]$Topic,
        [string]$KafkaContainer = "ecom-kafka"
    )

    $command = "kafka-console-consumer --bootstrap-server kafka:29092 --topic $Topic --from-beginning --timeout-ms 5000 --consumer-property isolation.level=read_committed 2>/dev/null || true"
    return @(Invoke-ProductionDockerCommand -Arguments @("exec", $KafkaContainer, "bash", "-lc", $command) `
        -FailureMessage "Failed to read committed Kafka topic $Topic.")
}

function ConvertFrom-ProductionKafkaGroupDescription {
    param(
        [Parameter(Mandatory = $true)][AllowEmptyString()][string[]]$Lines,
        [Parameter(Mandatory = $true)][string]$ExpectedGroup,
        [Parameter(Mandatory = $true)][string]$ExpectedTopic,
        [Parameter(Mandatory = $true)][int[]]$ExpectedPartitions
    )

    $rows = @()
    $partitions = @{}
    foreach ($line in $Lines) {
        $value = ([string]$line).Trim()
        if (-not $value -or $value -match "^GROUP\s+TOPIC" -or
            $value -match "^Consumer group .* has no active members") {
            continue
        }
        if ($value -notmatch "^(\S+)\s+(\S+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+.*$") {
            throw "Unexpected Kafka group row: $value"
        }
        if ($Matches[1] -ne $ExpectedGroup) {
            throw "Unexpected Kafka group $($Matches[1]); expected $ExpectedGroup."
        }
        if ($Matches[2] -ne $ExpectedTopic) {
            throw "Unexpected Kafka topic $($Matches[2]); expected $ExpectedTopic."
        }
        $partition = [int]$Matches[3]
        if ($partitions.ContainsKey($partition)) {
            throw "Duplicate Kafka group partition: $partition."
        }
        $partitions[$partition] = $true
        $rows += [pscustomobject]@{
            Group = $Matches[1]
            Topic = $Matches[2]
            Partition = $partition
            CurrentOffset = [int64]$Matches[4]
            LogEndOffset = [int64]$Matches[5]
            Lag = [int64]$Matches[6]
        }
    }
    if ($rows.Count -eq 0) {
        throw "Kafka group $ExpectedGroup does not exist or has no readable offsets."
    }
    $actualPartitions = @($rows.Partition | Sort-Object)
    $expected = @($ExpectedPartitions | Sort-Object -Unique)
    if (($actualPartitions -join ",") -ne ($expected -join ",")) {
        throw "Kafka group $ExpectedGroup partition set mismatch. Expected $($expected -join ','), got $($actualPartitions -join ',')."
    }
    return [pscustomobject]@{
        Group = $ExpectedGroup
        Topic = $ExpectedTopic
        Rows = @($rows | Sort-Object Partition)
        TotalLag = [int64](($rows | Measure-Object -Property Lag -Sum).Sum)
    }
}

function Wait-ProductionKafkaGroupLagZero {
    param(
        [Parameter(Mandatory = $true)][string]$Group,
        [Parameter(Mandatory = $true)][string]$Topic,
        [Parameter(Mandatory = $true)][int[]]$ExpectedPartitions,
        [string]$KafkaContainer = "ecom-kafka",
        [int]$TimeoutSeconds = 180
    )

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastError = "not queried"
    do {
        try {
            $lines = @(Invoke-ProductionDockerCommand -Arguments @(
                "exec", $KafkaContainer, "kafka-consumer-groups",
                "--bootstrap-server", "kafka:29092", "--describe", "--group", $Group
            ) -FailureMessage "Failed to describe Kafka group $Group.")
            $description = ConvertFrom-ProductionKafkaGroupDescription -Lines $lines `
                -ExpectedGroup $Group -ExpectedTopic $Topic -ExpectedPartitions $ExpectedPartitions
            if ($description.TotalLag -eq 0) { return $description }
            $lastError = "total lag is $($description.TotalLag)"
        } catch {
            $lastError = $_.Exception.Message
        }
        if ([DateTimeOffset]::UtcNow -lt $deadline) { Start-Sleep -Seconds 2 }
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "Kafka group $Group lag did not reach zero. Last error: $lastError"
}

function Get-ProductionExpectedJobs {
    param([Parameter(Mandatory = $true)][object]$Manifest)

    return @(
        [pscustomobject]@{
            Key = "production"
            Id = [string]$Manifest.production_job_id
            Name = "chapter-9-datastream-quality-production"
        },
        [pscustomobject]@{
            Key = "doris"
            Id = [string]$Manifest.doris_job_id
            Name = "chapter-9-doris-clean"
        },
        [pscustomobject]@{
            Key = "iceberg"
            Id = [string]$Manifest.iceberg_job_id
            Name = "chapter-9-iceberg-clean"
        }
    )
}

function Assert-ProductionJobsAndCapacity {
    param(
        [Parameter(Mandatory = $true)][object]$Manifest,
        [Parameter(Mandatory = $true)][object]$Jobs,
        [Parameter(Mandatory = $true)][object]$Overview
    )

    if ([int]$Overview.taskmanagers -ne 1 -or [int]$Overview."slots-total" -ne 4 -or
        [int]$Overview."jobs-running" -ne 3) {
        throw "Flink overview mismatch: taskmanagers=$($Overview.taskmanagers), slots-total=$($Overview.'slots-total'), jobs-running=$($Overview.'jobs-running')."
    }
    $expectedJobs = @(Get-ProductionExpectedJobs -Manifest $Manifest)
    $validated = @()
    foreach ($expected in $expectedJobs) {
        if ($expected.Id -notmatch "^[0-9a-f]{32}$") {
            throw "Manifest Job ID is invalid for $($expected.Name): $($expected.Id)"
        }
        $byId = @($Jobs.jobs | Where-Object { [string]$_.jid -eq $expected.Id })
        $byName = @($Jobs.jobs | Where-Object { [string]$_.name -eq $expected.Name })
        if ($byId.Count -ne 1 -or $byName.Count -ne 1 -or
            [string]$byId[0].name -ne $expected.Name -or
            [string]$byId[0].state -ne "RUNNING" -or
            [string]$byName[0].jid -ne $expected.Id) {
            throw "Manifest does not match exact RUNNING Job ID/name for $($expected.Name)."
        }
        $validated += $byId[0]
    }
    $runningJobs = @($Jobs.jobs | Where-Object { [string]$_.state -eq "RUNNING" })
    if ($runningJobs.Count -ne 3) {
        throw "Flink jobs overview contains $($runningJobs.Count) RUNNING jobs; expected 3."
    }
    return $validated
}

function Wait-ProductionJobsAndCapacity {
    param(
        [Parameter(Mandatory = $true)][object]$Manifest,
        [string]$FlinkBaseUrl = "http://localhost:8081",
        [int]$TimeoutSeconds = 180
    )

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastError = "not queried"
    do {
        try {
            $jobs = Invoke-RestMethod -Uri "$FlinkBaseUrl/jobs/overview" -TimeoutSec 5
            $overview = Invoke-RestMethod -Uri "$FlinkBaseUrl/overview" -TimeoutSec 5
            $validated = @(Assert-ProductionJobsAndCapacity -Manifest $Manifest -Jobs $jobs -Overview $overview)
            return [pscustomobject]@{ Jobs = $validated; Overview = $overview }
        } catch {
            $lastError = $_.Exception.Message
            if ($lastError -match "terminal state") { throw }
        }
        if ([DateTimeOffset]::UtcNow -lt $deadline) { Start-Sleep -Seconds 2 }
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "Flink jobs and capacity did not match production manifest. Last error: $lastError"
}

function Get-ProductionCheckpointEvidence {
    param(
        [Parameter(Mandatory = $true)][string]$JobId,
        [Parameter(Mandatory = $true)][string]$ExpectedName,
        [string]$FlinkBaseUrl = "http://localhost:8081"
    )

    $job = Invoke-RestMethod -Uri "$FlinkBaseUrl/jobs/$JobId" -TimeoutSec 5
    if ([string]$job.jid -ne $JobId -or [string]$job.name -ne $ExpectedName -or
        [string]$job.state -ne "RUNNING") {
        throw "Flink Job ID/name/state mismatch for $ExpectedName."
    }
    $checkpoints = Invoke-RestMethod -Uri "$FlinkBaseUrl/jobs/$JobId/checkpoints" -TimeoutSec 5
    if ($null -eq $checkpoints.latest.completed -or
        [string]$checkpoints.latest.completed.status -ne "COMPLETED") {
        throw "Job $ExpectedName has no completed checkpoint."
    }
    return [pscustomobject]@{
        Completed = [int64]$checkpoints.counts.completed
        LatestId = [int64]$checkpoints.latest.completed.id
        LatestAckTimestamp = [int64]$checkpoints.latest.completed.latest_ack_timestamp
    }
}

function Wait-NewProductionCheckpoint {
    param(
        [Parameter(Mandatory = $true)][string]$JobId,
        [Parameter(Mandatory = $true)][string]$ExpectedName,
        [Parameter(Mandatory = $true)][int64]$Baseline,
        [string]$FlinkBaseUrl = "http://localhost:8081",
        [int]$TimeoutSeconds = 180
    )

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastError = "not queried"
    do {
        try {
            $evidence = Get-ProductionCheckpointEvidence -JobId $JobId `
                -ExpectedName $ExpectedName -FlinkBaseUrl $FlinkBaseUrl
            if ($evidence.Completed -gt $Baseline) { return $evidence }
            $lastError = "completed checkpoint count is $($evidence.Completed), baseline is $Baseline"
        } catch {
            $lastError = $_.Exception.Message
            if ($lastError -match "state mismatch") { throw }
        }
        if ([DateTimeOffset]::UtcNow -lt $deadline) { Start-Sleep -Seconds 2 }
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "Job $ExpectedName did not complete a new checkpoint. Last error: $lastError"
}

function Get-ProductionRecordEventId {
    param([Parameter(Mandatory = $true)][object]$Record)

    if ($Record.PSObject.Properties.Name -contains "event_id" -and $Record.event_id) {
        return [string]$Record.event_id
    }
    if ($Record.PSObject.Properties.Name -contains "event" -and $Record.event.event_id) {
        return [string]$Record.event.event_id
    }
    if ($Record.PSObject.Properties.Name -contains "raw_payload" -and $Record.raw_payload) {
        $rawPayload = [string]$Record.raw_payload
        if ($rawPayload -match '"event_id"\s*:\s*"([^"]+)"') { return $Matches[1] }
    }
    return $null
}

function Assert-ProductionOutputMatrix {
    param(
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][string[]]$RawValues,
        [Parameter(Mandatory = $true)][object[]]$CleanRecords,
        [Parameter(Mandatory = $true)][object[]]$DlqRecords,
        [Parameter(Mandatory = $true)][object[]]$LateRecords
    )

    $expectedReasons = @(
        "DUPLICATE_EVENT",
        "MALFORMED_JSON",
        "MISSING_REQUIRED_FIELD",
        "INVALID_EVENT_TIME",
        "FUTURE_EVENT_TIME"
    )
    if ($RawValues.Count -ne 8 -or @($RawValues | Where-Object { $_ -notlike "*$RunId*" }).Count -ne 0 -or
        $CleanRecords.Count -ne 2 -or $DlqRecords.Count -ne 5 -or $LateRecords.Count -ne 1) {
        throw "Output counts mismatch: raw=$($RawValues.Count) clean=$($CleanRecords.Count) dlq=$($DlqRecords.Count) late=$($LateRecords.Count)."
    }
    $cleanIds = @($CleanRecords | ForEach-Object { Get-ProductionRecordEventId $_ })
    $expectedCleanIds = @("$RunId-duplicate", "$RunId-advancer")
    if ((@($cleanIds | Sort-Object) -join ",") -ne (@($expectedCleanIds | Sort-Object) -join ",")) {
        throw "Clean event IDs do not match the two expected IDs."
    }
    $duplicateClean = @($cleanIds | Where-Object { $_ -eq "$RunId-duplicate" }).Count
    if ($duplicateClean -ne 1) { throw "Duplicate clean event count must be exactly 1." }
    $cleanUsers = @($CleanRecords | ForEach-Object { [string]$_.user_id } | Sort-Object -Unique)
    if ($cleanUsers.Count -ne 2 -or $cleanUsers -contains "") {
        throw "The two clean events must use two distinct non-empty user IDs."
    }
    $reasonCounts = [ordered]@{}
    foreach ($reason in $expectedReasons) {
        $matches = @($DlqRecords | Where-Object { [string]$_.reason_code -eq $reason })
        if ($matches.Count -ne 1) { throw "DLQ reason must occur exactly once: $reason."
        }
        $reasonCounts[$reason] = 1
    }
    $unknownReasons = @($DlqRecords | Where-Object { [string]$_.reason_code -notin $expectedReasons })
    if ($unknownReasons.Count -ne 0) { throw "DLQ contains an unexpected reason code." }
    $expectedDlqIds = [ordered]@{
        DUPLICATE_EVENT = "$RunId-duplicate"
        MALFORMED_JSON = "$RunId-malformed"
        MISSING_REQUIRED_FIELD = "$RunId-missing"
        INVALID_EVENT_TIME = "$RunId-invalid-time"
        FUTURE_EVENT_TIME = "$RunId-future"
    }
    foreach ($reason in $expectedReasons) {
        $record = @($DlqRecords | Where-Object { [string]$_.reason_code -eq $reason })[0]
        $actualId = Get-ProductionRecordEventId $record
        if ($actualId -ne $expectedDlqIds[$reason]) {
            throw "DLQ event ID mismatch for $reason. Expected $($expectedDlqIds[$reason]), got $actualId."
        }
    }
    $lateId = Get-ProductionRecordEventId $LateRecords[0]
    if ($lateId -ne "$RunId-late") { throw "Late event ID mismatch: $lateId."
    }
    if (8 -ne ($CleanRecords.Count + $DlqRecords.Count + $LateRecords.Count)) {
        throw "raw = clean + dlq + late reconciliation failed."
    }
    return [pscustomobject]@{
        Raw = $RawValues.Count
        Clean = $CleanRecords.Count
        Dlq = $DlqRecords.Count
        Late = $LateRecords.Count
        DuplicateClean = $duplicateClean
        Reasons = $expectedReasons
        ReasonCounts = [pscustomobject]$reasonCounts
    }
}

function Wait-ProductionOutputMatrix {
    param(
        [Parameter(Mandatory = $true)][string]$RunId,
        [string]$KafkaContainer = "ecom-kafka",
        [int]$TimeoutSeconds = 180
    )

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastError = "not queried"
    do {
        try {
            $rawValues = @(Read-ProductionCommittedTopic -Topic "user_behavior_events" `
                -KafkaContainer $KafkaContainer | Where-Object { $_ -like "*$RunId*" })
            $cleanRecords = @(Read-ProductionCommittedTopic -Topic "user_behavior_clean" `
                -KafkaContainer $KafkaContainer | Where-Object { $_ -like "*$RunId*" } |
                ForEach-Object { $_ | ConvertFrom-Json })
            $dlqRecords = @(Read-ProductionCommittedTopic -Topic "user_behavior_dlq" `
                -KafkaContainer $KafkaContainer | Where-Object { $_ -like "*$RunId*" } |
                ForEach-Object { $_ | ConvertFrom-Json })
            $lateRecords = @(Read-ProductionCommittedTopic -Topic "user_behavior_late" `
                -KafkaContainer $KafkaContainer | Where-Object { $_ -like "*$RunId*" } |
                ForEach-Object { $_ | ConvertFrom-Json })
            $matrix = Assert-ProductionOutputMatrix -RunId $RunId -RawValues $rawValues `
                -CleanRecords $cleanRecords -DlqRecords $dlqRecords -LateRecords $lateRecords
            return [pscustomobject]@{
                Matrix = $matrix
                CleanRecords = $cleanRecords
                DlqRecords = $dlqRecords
                LateRecords = $lateRecords
            }
        } catch {
            $lastError = $_.Exception.Message
        }
        if ([DateTimeOffset]::UtcNow -lt $deadline) { Start-Sleep -Seconds 2 }
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "Production Kafka output matrix did not converge. Last error: $lastError"
}

function Get-ProductionDorisMetrics {
    param([string]$DorisContainer = "ecom-doris-fe")

    $sql = "SELECT metric_name, metric_value, DATE_FORMAT(updated_at,'%Y-%m-%dT%H:%i:%s') FROM analytics.realtime_metrics ORDER BY metric_name;"
    $lines = @(Invoke-ProductionDockerCommand -Arguments @(
        "exec", $DorisContainer, "mysql", "-hdoris-fe", "-P9030", "-uroot", "-N", "-B", "-e", $sql
    ) -FailureMessage "Failed to query analytics.realtime_metrics in Doris.")
    $metrics = @{}
    foreach ($line in $lines) {
        $parts = ([string]$line).Trim() -split "`t"
        if ($parts.Count -ne 3 -or $parts[0] -notin @("pv", "uv") -or $metrics.ContainsKey($parts[0])) {
            throw "Unexpected Doris realtime_metrics row: $line"
        }
        $metrics[$parts[0]] = [pscustomobject]@{
            Value = [int64]$parts[1]
            UpdatedAt = [string]$parts[2]
        }
    }
    if (-not $metrics.ContainsKey("pv") -or -not $metrics.ContainsKey("uv")) {
        throw "Doris realtime_metrics did not return exact pv and uv rows."
    }
    return [pscustomobject]@{
        Pv = $metrics.pv.Value
        Uv = $metrics.uv.Value
        PvUpdatedAt = $metrics.pv.UpdatedAt
        UvUpdatedAt = $metrics.uv.UpdatedAt
    }
}

function Wait-ProductionDorisMetrics {
    param([int]$TimeoutSeconds = 180)

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastError = "not queried"
    do {
        try {
            $metrics = Get-ProductionDorisMetrics
            if ($metrics.Pv -eq 2 -and $metrics.Uv -eq 2) { return $metrics }
            $lastError = "pv=$($metrics.Pv), uv=$($metrics.Uv)"
        } catch {
            $lastError = $_.Exception.Message
        }
        if ([DateTimeOffset]::UtcNow -lt $deadline) { Start-Sleep -Seconds 2 }
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "Doris metrics did not reach pv=2 and uv=2. Last error: $lastError"
}

function Invoke-ProductionTrinoStatement {
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [string]$TrinoBaseUrl = "http://localhost:8088"
    )

    $headers = @{
        "X-Trino-User" = "codex"
        "X-Trino-Source" = "chapter-9-production-verification"
        "X-Trino-Catalog" = "lakehouse"
        "X-Trino-Schema" = "analytics"
    }
    $response = Invoke-RestMethod -Method Post -Uri "$TrinoBaseUrl/v1/statement" `
        -Headers $headers -Body $Sql -ContentType "text/plain" -TimeoutSec 10
    $rows = New-Object System.Collections.Generic.List[object]
    while ($true) {
        if ($response.error) { throw "Trino query failed: $($response.error.message)" }
        foreach ($row in @($response.data)) {
            if ($null -ne $row) { [void]$rows.Add($row) }
        }
        if (-not $response.nextUri) { return $rows.ToArray() }
        $response = Invoke-RestMethod -Method Get -Uri $response.nextUri -TimeoutSec 10
    }
}

function ConvertTo-ProductionSqlStringLiteral {
    param([Parameter(Mandatory = $true)][string]$Value)
    return "'$($Value.Replace("'", "''"))'"
}

function Get-ProductionTrinoBaseline {
    $sql = @"
SELECT COUNT(*) AS event_count, COUNT(DISTINCT event_id) AS distinct_event_id
FROM lakehouse.analytics.user_behavior_detail
"@
    $rows = @(Invoke-ProductionTrinoStatement -Sql $sql)
    if ($rows.Count -ne 1 -or @($rows[0]).Count -ne 2) {
        throw "Trino baseline returned a malformed result."
    }
    $row = @($rows[0])
    return [pscustomobject]@{
        EventCount = [int64]$row[0]
        DistinctEventId = [int64]$row[1]
    }
}

function Get-ProductionTrinoRunEvidence {
    param(
        [Parameter(Mandatory = $true)][string[]]$CleanEventIds,
        [Parameter(Mandatory = $true)][string[]]$ExcludedEventIds
    )

    if ($CleanEventIds.Count -ne 2 -or $ExcludedEventIds.Count -ne 5) {
        throw "Trino run audit requires two clean IDs and five non-clean IDs."
    }
    $cleanLiterals = @($CleanEventIds | ForEach-Object { ConvertTo-ProductionSqlStringLiteral $_ }) -join ","
    $excludedLiterals = @($ExcludedEventIds | ForEach-Object { ConvertTo-ProductionSqlStringLiteral $_ }) -join ","
    $sql = @"
SELECT COUNT(*) AS event_count,
       COUNT(DISTINCT event_id) AS distinct_event_id,
       COUNT(DISTINCT user_id) AS distinct_user_id
FROM lakehouse.analytics.user_behavior_detail
WHERE event_id IN ($cleanLiterals)
"@
    $rows = @(Invoke-ProductionTrinoStatement -Sql $sql)
    if ($rows.Count -ne 1 -or @($rows[0]).Count -ne 3) {
        throw "Trino exact clean-ID audit returned a malformed result."
    }
    $excludedSql = @"
SELECT COUNT(*) AS excluded_event_count
FROM lakehouse.analytics.user_behavior_detail
WHERE event_id IN ($excludedLiterals)
"@
    $excludedRows = @(Invoke-ProductionTrinoStatement -Sql $excludedSql)
    if ($excludedRows.Count -ne 1 -or @($excludedRows[0]).Count -ne 1) {
        throw "Trino excluded-ID audit returned a malformed result."
    }
    $row = @($rows[0])
    return [pscustomobject]@{
        EventCount = [int64]$row[0]
        DistinctEventId = [int64]$row[1]
        DistinctUserId = [int64]$row[2]
        ExcludedEventCount = [int64]@($excludedRows[0])[0]
    }
}

function Wait-ProductionTrinoEvidence {
    param(
        [Parameter(Mandatory = $true)][string[]]$CleanEventIds,
        [Parameter(Mandatory = $true)][string[]]$ExcludedEventIds,
        [Parameter(Mandatory = $true)][int64]$BaselineEventCount,
        [int]$TimeoutSeconds = 180
    )

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastError = "not queried"
    do {
        try {
            $run = Get-ProductionTrinoRunEvidence -CleanEventIds $CleanEventIds `
                -ExcludedEventIds $ExcludedEventIds
            $total = Get-ProductionTrinoBaseline
            if ($run.EventCount -eq 2 -and $run.DistinctEventId -eq 2 -and
                $run.DistinctUserId -eq 2 -and $run.ExcludedEventCount -eq 0 -and
                $total.EventCount -ge ($BaselineEventCount + 2)) {
                return [pscustomobject]@{ Run = $run; Total = $total }
            }
            $lastError = "exact=$($run.EventCount)/$($run.DistinctEventId)/$($run.DistinctUserId), excluded=$($run.ExcludedEventCount), total=$($total.EventCount)"
        } catch {
            $lastError = $_.Exception.Message
        }
        if ([DateTimeOffset]::UtcNow -lt $deadline) { Start-Sleep -Seconds 2 }
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "Trino production evidence did not converge. Last error: $lastError"
}

function Assert-ProductionApiEvidence {
    param(
        [Parameter(Mandatory = $true)][object]$Response,
        [Parameter(Mandatory = $true)][DateTimeOffset]$BatchStart,
        [Parameter(Mandatory = $true)][int64]$TrinoBaseline
    )

    if ([string]::IsNullOrWhiteSpace([string]$Response.generated_at)) {
        throw "API generated_at is missing."
    }
    $generatedAt = [DateTimeOffset]::Parse([string]$Response.generated_at)
    if ($generatedAt -lt $BatchStart) { throw "API generated_at is older than batch start."
    }
    if ([string]::IsNullOrWhiteSpace([string]$Response.analyzer)) {
        throw "API analyzer is empty."
    }
    if (-not ($Response.PSObject.Properties.Name -contains "warnings") -or
        $null -eq $Response.warnings -or $Response.warnings -is [string]) {
        throw "API warnings must be an acceptable array."
    }
    if ([int64]$Response.evidence.realtime.pv -ne 2 -or
        [int64]$Response.evidence.realtime.uv -ne 2) {
        throw "API realtime evidence must be pv=2 and uv=2."
    }
    if ($null -eq $Response.evidence.historical -or
        [int64]$Response.evidence.historical.event_count -lt ($TrinoBaseline + 2)) {
        throw "API historical event_count is below Trino baseline plus two."
    }
    return [pscustomobject]@{
        GeneratedAt = $generatedAt
        Analyzer = [string]$Response.analyzer
        Warnings = @($Response.warnings)
        RealtimePv = [int64]$Response.evidence.realtime.pv
        RealtimeUv = [int64]$Response.evidence.realtime.uv
        HistoricalEventCount = [int64]$Response.evidence.historical.event_count
    }
}

function Wait-ProductionApiEvidence {
    param(
        [Parameter(Mandatory = $true)][DateTimeOffset]$BatchStart,
        [Parameter(Mandatory = $true)][int64]$TrinoBaseline,
        [int]$TimeoutSeconds = 180
    )

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastError = "not queried"
    $body = @{ question = "Summarize the current production activity." } | ConvertTo-Json
    do {
        try {
            $response = Invoke-RestMethod -Method Post -Uri "http://localhost:8000/analysis/realtime" `
                -ContentType "application/json" -Body $body -TimeoutSec 20
            $evidence = Assert-ProductionApiEvidence -Response $response `
                -BatchStart $BatchStart -TrinoBaseline $TrinoBaseline
            return [pscustomobject]@{ Response = $response; Evidence = $evidence }
        } catch {
            $lastError = $_.Exception.Message
        }
        if ([DateTimeOffset]::UtcNow -lt $deadline) { Start-Sleep -Seconds 2 }
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "Chapter 8 API production evidence did not converge. Last error: $lastError"
}

function Write-ProductionEvidenceAtomic {
    param(
        [Parameter(Mandatory = $true)][object]$Evidence,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $directory = Split-Path -Parent $Path
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
    $partialPath = Join-Path $directory "production-verification.json.partial"
    $json = $Evidence | ConvertTo-Json -Depth 12
    [System.IO.File]::WriteAllText(
        $partialPath,
        $json,
        (New-Object System.Text.UTF8Encoding($false))
    )
    Move-Item -LiteralPath $partialPath -Destination $Path -Force
}

if ($FunctionsOnly) { return }

$repoRoot = Split-Path -Parent $PSScriptRoot
$manifestPath = Join-Path $repoRoot "tmp/chapter-9/cutover-manifest.json"
$evidencePath = Join-Path $repoRoot "tmp/chapter-9/production-verification.json"
$rawTopic = "user_behavior_events"
$runId = "chapter9-production-" + [Guid]::NewGuid().ToString("N")

Push-Location $repoRoot
try {
    if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
        throw "Cutover manifest is missing: $manifestPath"
    }
    $manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
    $initialFlink = Wait-ProductionJobsAndCapacity -Manifest $manifest -TimeoutSeconds $TimeoutSeconds
    $expectedJobs = @(Get-ProductionExpectedJobs -Manifest $manifest)
    $checkpointBaselines = [ordered]@{}
    foreach ($expected in $expectedJobs) {
        $checkpointBaselines[$expected.Key] = Get-ProductionCheckpointEvidence `
            -JobId $expected.Id -ExpectedName $expected.Name
    }
    $dorisBaseline = Get-ProductionDorisMetrics
    $trinoBaseline = Get-ProductionTrinoBaseline
    $batchStart = [DateTimeOffset]::UtcNow

    $eventIds = [ordered]@{
        duplicate = "$runId-duplicate"
        malformed = "$runId-malformed"
        missing_required = "$runId-missing"
        invalid_time = "$runId-invalid-time"
        future = "$runId-future"
        advancer = "$runId-advancer"
        late = "$runId-late"
    }
    $now = [DateTimeOffset]::UtcNow
    $userOne = "$runId-user-1"
    $userTwo = "$runId-user-2"
    $duplicate = New-ProductionEventJson -EventId $eventIds.duplicate -UserId $userOne `
        -EventTime $now.ToString("o") -RunId $runId
    $advancer = New-ProductionEventJson -EventId $eventIds.advancer -UserId $userTwo `
        -EventTime $now.AddSeconds(30).ToString("o") -RunId $runId
    $missing = New-ProductionEventJson -EventId $eventIds.missing_required -UserId $userOne `
        -EventTime $now.ToString("o") -RunId $runId | ConvertFrom-Json
    $missing.PSObject.Properties.Remove("user_id")
    $missingJson = $missing | ConvertTo-Json -Compress
    $invalidTime = New-ProductionEventJson -EventId $eventIds.invalid_time -UserId $userOne `
        -EventTime "2026-07-22 10:00:00" -RunId $runId
    $future = New-ProductionEventJson -EventId $eventIds.future -UserId $userOne `
        -EventTime $now.AddMinutes(10).ToString("o") -RunId $runId
    $malformed = '{"event_id":"' + $eventIds.malformed + '"'

    Write-Host "[chapter9-production-verify] sending unique run $runId"
    Send-ProductionKafkaValue -Topic $rawTopic -Value $duplicate
    Send-ProductionKafkaValue -Topic $rawTopic -Value $duplicate
    Send-ProductionKafkaValue -Topic $rawTopic -Value $malformed
    Send-ProductionKafkaValue -Topic $rawTopic -Value $missingJson
    Send-ProductionKafkaValue -Topic $rawTopic -Value $invalidTime
    Send-ProductionKafkaValue -Topic $rawTopic -Value $future
    Send-ProductionKafkaValue -Topic $rawTopic -Value $advancer

    $productionJob = @($expectedJobs | Where-Object { $_.Key -eq "production" })[0]
    $watermarkCheckpoint = Wait-NewProductionCheckpoint -JobId $productionJob.Id `
        -ExpectedName $productionJob.Name -Baseline $checkpointBaselines.production.Completed `
        -TimeoutSeconds $TimeoutSeconds
    $late = New-ProductionEventJson -EventId $eventIds.late -UserId $userOne `
        -EventTime $now.AddSeconds(-30).ToString("o") -RunId $runId
    Send-ProductionKafkaValue -Topic $rawTopic -Value $late
    $productionFinalCheckpoint = Wait-NewProductionCheckpoint -JobId $productionJob.Id `
        -ExpectedName $productionJob.Name -Baseline $watermarkCheckpoint.Completed `
        -TimeoutSeconds $TimeoutSeconds

    $output = Wait-ProductionOutputMatrix -RunId $runId -TimeoutSeconds $TimeoutSeconds
    $rawPartitions = @($manifest.raw_offsets | ForEach-Object {
        if ($_ -notmatch "^partition:(\d+),offset:\d+$") {
            throw "Invalid raw offset in cutover manifest: $_"
        }
        [int]$Matches[1]
    })
    $groupSpecs = @(
        [pscustomobject]@{ Key = "production"; Group = "chapter9-quality-production"; Topic = $rawTopic },
        [pscustomobject]@{ Key = "doris"; Group = "chapter9-doris-clean-v1"; Topic = "user_behavior_clean" },
        [pscustomobject]@{ Key = "iceberg"; Group = "chapter9-iceberg-clean-v1"; Topic = "user_behavior_clean" }
    )
    $groups = [ordered]@{}
    foreach ($spec in $groupSpecs) {
        $groups[$spec.Key] = Wait-ProductionKafkaGroupLagZero -Group $spec.Group `
            -Topic $spec.Topic -ExpectedPartitions $rawPartitions -TimeoutSeconds $TimeoutSeconds
    }

    $checkpointFinals = [ordered]@{ production = $productionFinalCheckpoint }
    foreach ($expected in @($expectedJobs | Where-Object { $_.Key -ne "production" })) {
        $checkpointFinals[$expected.Key] = Wait-NewProductionCheckpoint -JobId $expected.Id `
            -ExpectedName $expected.Name -Baseline $checkpointBaselines[$expected.Key].Completed `
            -TimeoutSeconds $TimeoutSeconds
    }
    $finalFlink = Wait-ProductionJobsAndCapacity -Manifest $manifest -TimeoutSeconds $TimeoutSeconds
    $dorisFinal = Wait-ProductionDorisMetrics -TimeoutSeconds $TimeoutSeconds
    $cleanEventIds = @($eventIds.duplicate, $eventIds.advancer)
    $excludedEventIds = @(
        $eventIds.malformed,
        $eventIds.missing_required,
        $eventIds.invalid_time,
        $eventIds.future,
        $eventIds.late
    )
    $trinoFinal = Wait-ProductionTrinoEvidence -CleanEventIds $cleanEventIds `
        -ExcludedEventIds $excludedEventIds -BaselineEventCount $trinoBaseline.EventCount `
        -TimeoutSeconds $TimeoutSeconds
    $apiFinal = Wait-ProductionApiEvidence -BatchStart $batchStart `
        -TrinoBaseline $trinoBaseline.EventCount -TimeoutSeconds $TimeoutSeconds

    $jobEvidence = [ordered]@{}
    foreach ($expected in $expectedJobs) {
        $job = @($finalFlink.Jobs | Where-Object { [string]$_.jid -eq $expected.Id })[0]
        $jobEvidence[$expected.Key] = [ordered]@{
            id = $expected.Id
            name = $expected.Name
            state = [string]$job.state
            checkpoint_baseline = $checkpointBaselines[$expected.Key]
            checkpoint_final = $checkpointFinals[$expected.Key]
        }
    }
    $groupEvidence = [ordered]@{}
    foreach ($spec in $groupSpecs) {
        $description = $groups[$spec.Key]
        $groupEvidence[$spec.Key] = [ordered]@{
            group = $description.Group
            topic = $description.Topic
            lag = $description.TotalLag
            partitions = @($description.Rows | ForEach-Object { $_.Partition })
        }
    }
    $evidence = [ordered]@{
        run_id = $runId
        cutover_id = [string]$manifest.cutover_id
        batch_start_utc = $batchStart.ToString("o")
        verified_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
        event_ids = $eventIds
        counts = [ordered]@{
            raw = $output.Matrix.Raw
            clean = $output.Matrix.Clean
            dlq = $output.Matrix.Dlq
            late = $output.Matrix.Late
            duplicate_clean = $output.Matrix.DuplicateClean
        }
        dlq_reason_counts = $output.Matrix.ReasonCounts
        flink = [ordered]@{
            overview = [ordered]@{
                taskmanagers = [int]$finalFlink.Overview.taskmanagers
                slots_total = [int]$finalFlink.Overview."slots-total"
                slots_available = [int]$finalFlink.Overview."slots-available"
                jobs_running = [int]$finalFlink.Overview."jobs-running"
            }
            jobs = $jobEvidence
        }
        kafka_groups = $groupEvidence
        doris = [ordered]@{ baseline = $dorisBaseline; final = $dorisFinal }
        trino = [ordered]@{
            baseline = $trinoBaseline
            total_final = $trinoFinal.Total
            exact_clean_ids = $cleanEventIds
            exact_counts = $trinoFinal.Run
            excluded_ids = $excludedEventIds
            duplicate_dlq_id_collision = [ordered]@{
                id = $eventIds.duplicate
                iceberg_count = 1
                evidence = "The exact clean-ID query proves one row for each of two distinct clean IDs."
            }
        }
        api = [ordered]@{
            generated_at = $apiFinal.Evidence.GeneratedAt.ToString("o")
            analyzer = $apiFinal.Evidence.Analyzer
            warnings = @($apiFinal.Evidence.Warnings)
            realtime_pv = $apiFinal.Evidence.RealtimePv
            realtime_uv = $apiFinal.Evidence.RealtimeUv
            historical_event_count = $apiFinal.Evidence.HistoricalEventCount
        }
    }
    Write-ProductionEvidenceAtomic -Evidence $evidence -Path $evidencePath

    Write-Host "[chapter9-production-verify] passed run_id=$runId"
    Write-Host "raw=8 clean=2 dlq=5 late=1 duplicate_clean=1; raw = clean + dlq + late"
    Write-Host "doris pv=$($dorisFinal.Pv) uv=$($dorisFinal.Uv); trino exact=2/2/2 excluded=0"
    Write-Host "api generated_at=$($apiFinal.Evidence.GeneratedAt.ToString('o')) analyzer=$($apiFinal.Evidence.Analyzer)"
} finally {
    Pop-Location
}
