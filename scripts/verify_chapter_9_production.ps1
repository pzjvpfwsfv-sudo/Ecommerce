[CmdletBinding()]
param(
    [switch]$FunctionsOnly,
    [string]$ResumeRunId,
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

function ConvertFrom-ProductionKafkaDumpLog {
    param(
        [Parameter(Mandatory = $true)][AllowEmptyString()][string[]]$Lines,
        [Parameter(Mandatory = $true)][int]$Partition,
        [Parameter(Mandatory = $true)][int64]$StartOffset,
        [Parameter(Mandatory = $true)][int64]$EndOffsetExclusive
    )

    if ($EndOffsetExclusive -lt $StartOffset) { throw "Kafka dump offset range is invalid." }
    $classifications = New-Object System.Collections.Generic.List[object]
    $latestControl = @()
    foreach ($line in $Lines) {
        $value = ([string]$line).Trim()
        if ($value -match "^baseOffset:\s*(\d+)\s+lastOffset:\s*(\d+).+isControl:\s*(true|false)\b") {
            $baseOffset = [int64]$Matches[1]
            $lastOffset = [int64]$Matches[2]
            $isControl = $Matches[3] -eq "true"
            $latestControl = @()
            for ($offset = $baseOffset; $offset -le $lastOffset; $offset++) {
                if ($offset -ge $StartOffset -and $offset -lt $EndOffsetExclusive) {
                    $record = [pscustomobject]@{
                        Partition = $Partition
                        Offset = $offset
                        Kind = if ($isControl) { "transaction_control" } else { "readable_data" }
                        ControlType = if ($isControl) { "UNKNOWN" } else { $null }
                    }
                    [void]$classifications.Add($record)
                    if ($isControl) { $latestControl += $record }
                }
            }
            continue
        }
        if ($latestControl.Count -gt 0 -and $value -match "endTxnMarker:\s*(COMMIT|ABORT)\b") {
            foreach ($record in $latestControl) { $record.ControlType = $Matches[1] }
        }
    }
    return $classifications.ToArray()
}

function Assert-StableProductionKafkaLag {
    param(
        [Parameter(Mandatory = $true)][object]$Before,
        [Parameter(Mandatory = $true)][object]$After,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$Classifications
    )

    foreach ($snapshot in @($Before, $After)) {
        $calculatedLag = [int64]0
        foreach ($row in @($snapshot.Rows)) {
            $offsetLag = [int64]$row.LogEndOffset - [int64]$row.CurrentOffset
            if ($offsetLag -lt 0 -or [int64]$row.Lag -ne $offsetLag) {
                throw "Kafka group offset arithmetic is inconsistent."
            }
            $calculatedLag += $offsetLag
        }
        if ([int64]$snapshot.TotalLag -ne $calculatedLag) {
            throw "Kafka group offset arithmetic is inconsistent."
        }
    }
    if ($Before.Group -ne $After.Group -or $Before.Topic -ne $After.Topic -or
        @($Before.Rows).Count -ne @($After.Rows).Count) {
        throw "Kafka group snapshot changed during classification."
    }
    $expected = @{}
    foreach ($beforeRow in @($Before.Rows)) {
        $afterRows = @($After.Rows | Where-Object { $_.Partition -eq $beforeRow.Partition })
        if ($afterRows.Count -ne 1 -or
            [int64]$afterRows[0].CurrentOffset -ne [int64]$beforeRow.CurrentOffset -or
            [int64]$afterRows[0].LogEndOffset -ne [int64]$beforeRow.LogEndOffset) {
            throw "Kafka group offsets changed during classification."
        }
        for ($offset = [int64]$beforeRow.CurrentOffset;
            $offset -lt [int64]$beforeRow.LogEndOffset; $offset++) {
            $expected["$($beforeRow.Partition):$offset"] = $true
        }
    }
    $actual = @{}
    foreach ($record in $Classifications) {
        $key = "$($record.Partition):$($record.Offset)"
        if ($actual.ContainsKey($key) -or -not $expected.ContainsKey($key)) {
            throw "Kafka classification contains a duplicate or unexpected offset: $key."
        }
        $actual[$key] = $true
    }
    if ($actual.Count -ne $expected.Count) {
        throw "Kafka classification must cover every lagged offset."
    }
    $readable = @($Classifications | Where-Object { $_.Kind -eq "readable_data" })
    if ($readable.Count -gt 0) {
        throw "Kafka lag range contains readable data at offsets $(@($readable.Offset) -join ',')."
    }
    $invalidControl = @($Classifications | Where-Object {
        $_.Kind -ne "transaction_control" -or $_.ControlType -notin @("COMMIT", "ABORT")
    })
    if ($invalidControl.Count -gt 0) {
        throw "Kafka lag range contains an unproven transaction control record."
    }
    return [pscustomobject]@{
        Group = [string]$Before.Group
        Topic = [string]$Before.Topic
        CliLag = [int64]$Before.TotalLag
        ReadableDataLag = 0
        Rows = @($Before.Rows)
        Classifications = @($Classifications | Sort-Object Partition, Offset)
    }
}

function Get-ProductionKafkaGroupDescription {
    param(
        [string]$Group,
        [string]$Topic,
        [int[]]$ExpectedPartitions,
        [string]$KafkaContainer = "ecom-kafka"
    )

    $lines = @(Invoke-ProductionDockerCommand -Arguments @(
        "exec", $KafkaContainer, "kafka-consumer-groups",
        "--bootstrap-server", "kafka:29092", "--describe", "--group", $Group
    ) -FailureMessage "Failed to describe Kafka group $Group.")
    return ConvertFrom-ProductionKafkaGroupDescription -Lines $lines `
        -ExpectedGroup $Group -ExpectedTopic $Topic -ExpectedPartitions $ExpectedPartitions
}

function Get-ProductionKafkaOffsetClassifications {
    param(
        [string]$Topic,
        [int]$Partition,
        [int64]$StartOffset,
        [int64]$EndOffsetExclusive,
        [string]$KafkaContainer = "ecom-kafka"
    )

    if ($Topic -notmatch "^[A-Za-z0-9._-]+$") { throw "Kafka topic is unsafe for log inspection." }
    if ($EndOffsetExclusive -eq $StartOffset) { return @() }
    $command = 'files=$(printf ''%s,'' /var/lib/kafka/data/' + $Topic + '-' + $Partition +
        '/*.log); files=${files%,}; kafka-dump-log --deep-iteration --print-data-log --files "$files"'
    $lines = @(Invoke-ProductionDockerCommand -Arguments @(
        "exec", $KafkaContainer, "bash", "-lc", $command
    ) -FailureMessage "Failed to classify Kafka offsets for $Topic partition $Partition.")
    return @(ConvertFrom-ProductionKafkaDumpLog -Lines $lines -Partition $Partition `
        -StartOffset $StartOffset -EndOffsetExclusive $EndOffsetExclusive)
}

function Wait-ProductionKafkaGroupReadableLagZero {
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
            $before = Get-ProductionKafkaGroupDescription -Group $Group -Topic $Topic `
                -ExpectedPartitions $ExpectedPartitions -KafkaContainer $KafkaContainer
            $classifications = @()
            foreach ($row in @($before.Rows)) {
                $classifications += @(Get-ProductionKafkaOffsetClassifications -Topic $Topic `
                    -Partition $row.Partition -StartOffset $row.CurrentOffset `
                    -EndOffsetExclusive $row.LogEndOffset -KafkaContainer $KafkaContainer)
            }
            $after = Get-ProductionKafkaGroupDescription -Group $Group -Topic $Topic `
                -ExpectedPartitions $ExpectedPartitions -KafkaContainer $KafkaContainer
            return Assert-StableProductionKafkaLag -Before $before -After $after `
                -Classifications $classifications
        } catch {
            $lastError = $_.Exception.Message
            if ($lastError -match "contains readable data") { throw }
        }
        if ([DateTimeOffset]::UtcNow -lt $deadline) { Start-Sleep -Seconds 2 }
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "Kafka group $Group readable lag did not reach zero. Last error: $lastError"
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
        $byName = @($Jobs.jobs | Where-Object {
            [string]$_.name -eq $expected.Name -and [string]$_.state -eq "RUNNING"
        })
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

function Get-ProductionCurrentWatermarkMetric {
    param(
        [Parameter(Mandatory = $true)][string]$JobId,
        [Parameter(Mandatory = $true)][string]$ExpectedName,
        [string]$FlinkBaseUrl = "http://localhost:8081"
    )

    $job = Invoke-RestMethod -Uri "$FlinkBaseUrl/jobs/$JobId" -TimeoutSec 5
    if ([string]$job.jid -ne $JobId -or [string]$job.name -ne $ExpectedName -or
        [string]$job.state -ne "RUNNING") {
        throw "Flink Job ID/name/state mismatch while reading watermark."
    }
    $vertices = @($job.vertices | Where-Object { [string]$_.name -like "*route-late-events*" })
    if ($vertices.Count -ne 1) { throw "Expected one route-late-events vertex."
    }
    $metricsUrl = "$FlinkBaseUrl/jobs/$JobId/vertices/$($vertices[0].id)/metrics"
    $metricResponse = Invoke-RestMethod -Uri $metricsUrl -TimeoutSec 5
    $metrics = @($metricResponse)
    $metricIds = @($metrics | Where-Object {
        [string]$_.id -match "^\d+\.route-late-events\.currentInputWatermark$"
    })
    if ($metricIds.Count -ne 1) { throw "Expected one late-event input watermark metric."
    }
    $metricId = [string]$metricIds[0].id
    $encodedMetric = [Uri]::EscapeDataString($metricId)
    $valueResponse = Invoke-RestMethod -Uri "$metricsUrl`?get=$encodedMetric" -TimeoutSec 5
    $values = @($valueResponse)
    if ($values.Count -ne 1 -or [string]$values[0].id -ne $metricId) {
        throw "Flink watermark metric returned a malformed value."
    }
    return [pscustomobject]@{
        VertexId = [string]$vertices[0].id
        MetricId = $metricId
        Watermark = [int64]$values[0].value
    }
}

function Wait-ProductionWatermarkPast {
    param(
        [Parameter(Mandatory = $true)][string]$JobId,
        [Parameter(Mandatory = $true)][string]$ExpectedName,
        [Parameter(Mandatory = $true)][int64]$ThresholdEpochMs,
        [string]$FlinkBaseUrl = "http://localhost:8081",
        [int]$Attempts = 90,
        [int]$SleepSeconds = 2
    )

    $lastError = "not queried"
    $lastWatermark = $null
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        try {
            $metric = Get-ProductionCurrentWatermarkMetric -JobId $JobId `
                -ExpectedName $ExpectedName -FlinkBaseUrl $FlinkBaseUrl
            $lastWatermark = $metric.Watermark
            if ($lastWatermark -gt $ThresholdEpochMs) {
                return [pscustomobject]@{
                    VertexId = $metric.VertexId
                    MetricId = $metric.MetricId
                    Watermark = $lastWatermark
                    ThresholdEpochMs = $ThresholdEpochMs
                }
            }
            $lastError = "watermark $lastWatermark has not passed $ThresholdEpochMs"
        } catch {
            $lastError = $_.Exception.Message
            if ($lastError -match "state mismatch") { throw }
        }
        if ($attempt -lt $Attempts -and $SleepSeconds -gt 0) { Start-Sleep -Seconds $SleepSeconds }
    }
    throw "Production watermark did not pass the late event timestamp. Last watermark: $lastWatermark. Last error: $lastError"
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
    $lateCleanCount = @($cleanIds | Where-Object { $_ -eq "$RunId-late" }).Count
    $lateDlqCount = @($DlqRecords | Where-Object {
        (Get-ProductionRecordEventId $_) -eq "$RunId-late"
    }).Count
    if ($lateCleanCount -ne 0 -or $lateDlqCount -ne 0) {
        throw "Late event ID must exist only in the late output."
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
        LateOutputProof = [pscustomobject]@{
            EventId = "$RunId-late"
            LateTopicCount = 1
            CleanCount = $lateCleanCount
            DlqCount = $lateDlqCount
        }
    }
}

function Assert-ProductionResumeMatrix {
    param(
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][string[]]$RawValues,
        [Parameter(Mandatory = $true)][object[]]$CleanRecords,
        [Parameter(Mandatory = $true)][object[]]$DlqRecords,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$LateRecords
    )

    $resumeAction = if ($RawValues.Count -eq 7 -and $CleanRecords.Count -eq 2 -and
        $DlqRecords.Count -eq 5 -and $LateRecords.Count -eq 0) {
        "send_late"
    } elseif ($RawValues.Count -eq 8 -and $CleanRecords.Count -eq 2 -and
        $DlqRecords.Count -eq 5 -and $LateRecords.Count -eq 1) {
        "read_only_finalize"
    } else {
        throw "Resume counts must be raw/clean/dlq/late=7/2/5/0 or 8/2/5/1."
    }
    $expectedRawIds = [ordered]@{
        "$RunId-duplicate" = 2
        "$RunId-malformed" = 1
        "$RunId-missing" = 1
        "$RunId-invalid-time" = 1
        "$RunId-future" = 1
        "$RunId-advancer" = 1
    }
    if ($resumeAction -eq "read_only_finalize") {
        $expectedRawIds["$RunId-late"] = 1
    }
    $rawById = @{}
    foreach ($value in $RawValues) {
        if ([string]$value -notmatch '"event_id"\s*:\s*"([^"]+)"') {
            throw "Resume raw record has no auditable event_id."
        }
        $id = $Matches[1]
        if (-not $expectedRawIds.Contains($id)) {
            throw "Resume raw record has an unexpected event_id: $id."
        }
        if (-not $rawById.ContainsKey($id)) { $rawById[$id] = @() }
        $rawById[$id] = @($rawById[$id]) + [string]$value
    }
    foreach ($entry in $expectedRawIds.GetEnumerator()) {
        if (@($rawById[$entry.Key]).Count -ne [int]$entry.Value) {
            throw "Resume raw event cardinality mismatch for $($entry.Key)."
        }
    }
    $duplicateValues = @($rawById["$RunId-duplicate"])
    if ($duplicateValues[0] -cne $duplicateValues[1]) {
        throw "Resume duplicate raw payloads must be byte-identical."
    }
    try {
        $duplicateEvent = $duplicateValues[0] | ConvertFrom-Json
        $advancerEvent = @($rawById["$RunId-advancer"])[0] | ConvertFrom-Json
        $batchStart = [DateTimeOffset]::Parse([string]$duplicateEvent.event_time)
        $advancerTime = [DateTimeOffset]::Parse([string]$advancerEvent.event_time)
    } catch {
        throw "Resume could not parse duplicate/advancer event_time."
    }
    if ([string]$duplicateEvent.user_id -eq "" -or [string]$advancerEvent.user_id -eq "" -or
        [string]$duplicateEvent.user_id -eq [string]$advancerEvent.user_id -or
        ($advancerTime - $batchStart).TotalSeconds -ne 30) {
        throw "Resume duplicate/advancer events do not match the original matrix."
    }
    $lateEventTime = $advancerTime.AddSeconds(-60)
    $lateJson = New-ProductionEventJson -EventId "$RunId-late" `
        -UserId ([string]$duplicateEvent.user_id) -EventTime $lateEventTime.ToString("o") `
        -RunId $RunId
    if ($resumeAction -eq "read_only_finalize") {
        try {
            $lateRawEvent = @($rawById["$RunId-late"])[0] | ConvertFrom-Json
            $lateRawTime = [DateTimeOffset]::Parse([string]$lateRawEvent.event_time)
        } catch {
            throw "Resume could not parse the existing late raw event."
        }
        $expectedLate = $lateJson | ConvertFrom-Json
        foreach ($property in @(
            "event_id", "user_id", "product_id", "event_type", "channel", "device_type", "page_id"
        )) {
            if ([string]$lateRawEvent.$property -cne [string]$expectedLate.$property) {
                throw "Resume existing late raw event does not match reconstructed $property."
            }
        }
        if ($lateRawTime -ne $lateEventTime) {
            throw "Resume existing late raw event does not match reconstructed event_time."
        }
        $matrix = Assert-ProductionOutputMatrix -RunId $RunId -RawValues $RawValues `
            -CleanRecords $CleanRecords -DlqRecords $DlqRecords `
            -LateRecords $LateRecords
    } else {
        $syntheticLate = $lateJson | ConvertFrom-Json
        $matrix = Assert-ProductionOutputMatrix -RunId $RunId `
            -RawValues @($RawValues + $lateJson) `
            -CleanRecords $CleanRecords -DlqRecords $DlqRecords `
            -LateRecords @($syntheticLate)
    }
    return [pscustomobject]@{
        Raw = $RawValues.Count
        Clean = 2
        Dlq = 5
        Late = $LateRecords.Count
        ResumeAction = $resumeAction
        LateOutputProof = $matrix.LateOutputProof
        BatchStart = $batchStart
        BatchStartBasis = "duplicate_raw_event_time_upper_bound"
        AdvancerEventTime = $advancerTime
        LateEventTime = $lateEventTime
        LateEventTimeBasis = "advancer_raw_event_time_minus_60_seconds"
        LateJson = $lateJson
    }
}

function Get-ProductionReadOnlyWatermarkProof {
    param(
        [Parameter(Mandatory = $true)][object]$ResumeState,
        [Parameter(Mandatory = $true)][object[]]$ResumeChain,
        [Parameter(Mandatory = $true)][string]$JobId,
        [Parameter(Mandatory = $true)][string]$ExpectedName,
        [Parameter(Mandatory = $true)][int64]$ThresholdEpochMs,
        [string]$FlinkBaseUrl = "http://localhost:8081"
    )

    if ([string]$ResumeState.ResumeAction -ne "read_only_finalize" -or
        [int]$ResumeState.Raw -ne 8 -or [int]$ResumeState.Clean -ne 2 -or
        [int]$ResumeState.Dlq -ne 5 -or [int]$ResumeState.Late -ne 1) {
        throw "Only a strict 8/2/5/1 read-only finalize may use persisted watermark proof."
    }
    $lateProof = $ResumeState.LateOutputProof
    if ($null -eq $lateProof -or [int]$lateProof.LateTopicCount -ne 1 -or
        [int]$lateProof.CleanCount -ne 0 -or [int]$lateProof.DlqCount -ne 0) {
        throw "Read-only finalize requires exclusive persisted late output proof."
    }
    $priorApiFailures = @($ResumeChain | Where-Object {
        [string]$_.path -match "\.resume-[0-9a-f]{32}\.failed\.json$" -and
        $_.events_sent -eq $true -and
        [string]$_.error -match "^Chapter 8 API production evidence did not converge\. Last error: .+$"
    } | ForEach-Object {
        try {
            $failedAt = [DateTimeOffset]::Parse([string]$_.failed_at_utc)
        } catch {
            throw "Prior API-gate failed evidence has an invalid timestamp."
        }
        [pscustomobject]@{ FailedAt = $failedAt; Evidence = $_ }
    } | Sort-Object FailedAt)
    if ($priorApiFailures.Count -eq 0) {
        throw "Read-only finalize requires prior resume evidence that reached only the API gate."
    }

    $currentMetric = $null
    try {
        $observed = Get-ProductionCurrentWatermarkMetric -JobId $JobId `
            -ExpectedName $ExpectedName -FlinkBaseUrl $FlinkBaseUrl
        if ([int64]$observed.Watermark -ne [int64]::MinValue) {
            $currentMetric = $observed
        }
    } catch {
        $currentMetric = $null
    }
    return [pscustomobject]@{
        WatermarkProofSource = "observed_late_output_after_prior_gate"
        ThresholdEpochMs = $ThresholdEpochMs
        LateOutputProof = $lateProof
        PriorGateEvidence = $priorApiFailures[-1].Evidence
        CurrentMetric = $currentMetric
    }
}

function Get-ProductionResumeTopicState {
    param(
        [Parameter(Mandatory = $true)][string]$RunId,
        [string]$KafkaContainer = "ecom-kafka"
    )

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
    return Assert-ProductionResumeMatrix -RunId $RunId -RawValues $rawValues `
        -CleanRecords $cleanRecords -DlqRecords $dlqRecords -LateRecords $lateRecords
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
            UpdatedAt = [DateTimeOffset]::Parse(
                "$($parts[2])Z",
                [Globalization.CultureInfo]::InvariantCulture
            )
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

function Assert-ProductionDorisFreshness {
    param(
        [Parameter(Mandatory = $true)][object]$Metrics,
        [Parameter(Mandatory = $true)][DateTimeOffset]$BatchStart
    )

    if ([int64]$Metrics.Pv -ne 2 -or [int64]$Metrics.Uv -ne 2) {
        throw "Doris metrics must be exactly pv=2 and uv=2."
    }
    $pvUpdatedAt = [DateTimeOffset]$Metrics.PvUpdatedAt
    $uvUpdatedAt = [DateTimeOffset]$Metrics.UvUpdatedAt
    if ($pvUpdatedAt -le $BatchStart -or $uvUpdatedAt -le $BatchStart) {
        throw "Doris updated_at must be strictly later than batch start."
    }
    return [pscustomobject]@{
        Pv = [int64]$Metrics.Pv
        Uv = [int64]$Metrics.Uv
        PvUpdatedAt = $pvUpdatedAt
        UvUpdatedAt = $uvUpdatedAt
    }
}

function Wait-ProductionDorisMetrics {
    param(
        [Parameter(Mandatory = $true)][DateTimeOffset]$BatchStart,
        [int]$TimeoutSeconds = 180
    )

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastError = "not queried"
    do {
        try {
            $metrics = Get-ProductionDorisMetrics
            return Assert-ProductionDorisFreshness -Metrics $metrics -BatchStart $BatchStart
        } catch {
            $lastError = $_.Exception.Message
        }
        if ([DateTimeOffset]::UtcNow -lt $deadline) { Start-Sleep -Seconds 2 }
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "Doris metrics did not reach fresh pv=2 and uv=2. Last error: $lastError"
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

function Get-ProductionRecoveredTrinoBaseline {
    param(
        [Parameter(Mandatory = $true)][int64]$CurrentEventCount,
        [Parameter(Mandatory = $true)][int64]$CurrentDistinctEventId,
        [Parameter(Mandatory = $true)][int64]$RunEventCount,
        [Parameter(Mandatory = $true)][int64]$RunDistinctEventId
    )

    if ($RunEventCount -ne 2 -or $RunDistinctEventId -ne 2 -or
        $CurrentEventCount -lt 2 -or $CurrentDistinctEventId -lt 2) {
        throw "Recovered Trino baseline requires an exact existing run count of 2/2."
    }
    return [pscustomobject]@{
        EventCount = $CurrentEventCount - $RunEventCount
        DistinctEventId = $CurrentDistinctEventId - $RunDistinctEventId
        SampleType = "derived_recovered"
        Basis = "current total minus exact run 2 rows and 2 distinct event IDs"
        CurrentEventCount = $CurrentEventCount
        CurrentDistinctEventId = $CurrentDistinctEventId
        SubtractedRunEventCount = $RunEventCount
        SubtractedRunDistinctEventId = $RunDistinctEventId
    }
}

function Get-ProductionTrinoRunEvidence {
    param(
        [Parameter(Mandatory = $true)][string[]]$CleanEventIds,
        [Parameter(Mandatory = $true)][string[]]$ExcludedEventIds,
        [Parameter(Mandatory = $true)][string]$DuplicateEventId
    )

    if ($CleanEventIds.Count -ne 2 -or $ExcludedEventIds.Count -ne 5) {
        throw "Trino run audit requires two clean IDs and five non-clean IDs."
    }
    $cleanLiterals = @($CleanEventIds | ForEach-Object { ConvertTo-ProductionSqlStringLiteral $_ }) -join ","
    $excludedLiterals = @($ExcludedEventIds | ForEach-Object { ConvertTo-ProductionSqlStringLiteral $_ }) -join ","
    $duplicateLiteral = ConvertTo-ProductionSqlStringLiteral $DuplicateEventId
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
    $duplicateSql = @"
SELECT COUNT(*) AS duplicate_event_count
FROM lakehouse.analytics.user_behavior_detail
WHERE event_id = $duplicateLiteral
"@
    $duplicateRows = @(Invoke-ProductionTrinoStatement -Sql $duplicateSql)
    if ($duplicateRows.Count -ne 1 -or @($duplicateRows[0]).Count -ne 1) {
        throw "Trino duplicate-ID audit returned a malformed result."
    }
    $row = @($rows[0])
    return [pscustomobject]@{
        EventCount = [int64]$row[0]
        DistinctEventId = [int64]$row[1]
        DistinctUserId = [int64]$row[2]
        ExcludedEventCount = [int64]@($excludedRows[0])[0]
        DuplicateEventCount = [int64]@($duplicateRows[0])[0]
    }
}

function Wait-ProductionTrinoEvidence {
    param(
        [Parameter(Mandatory = $true)][string[]]$CleanEventIds,
        [Parameter(Mandatory = $true)][string[]]$ExcludedEventIds,
        [Parameter(Mandatory = $true)][string]$DuplicateEventId,
        [Parameter(Mandatory = $true)][int64]$BaselineEventCount,
        [int]$TimeoutSeconds = 180
    )

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastError = "not queried"
    do {
        try {
            $run = Get-ProductionTrinoRunEvidence -CleanEventIds $CleanEventIds `
                -ExcludedEventIds $ExcludedEventIds -DuplicateEventId $DuplicateEventId
            $total = Get-ProductionTrinoBaseline
            if ($run.EventCount -eq 2 -and $run.DistinctEventId -eq 2 -and
                $run.DistinctUserId -eq 2 -and $run.ExcludedEventCount -eq 0 -and
                $run.DuplicateEventCount -eq 1 -and
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
    if ($generatedAt -le $BatchStart) { throw "API generated_at must be later than batch start."
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
    if ([string]::IsNullOrWhiteSpace([string]$Response.evidence.realtime.updated_at)) {
        throw "API realtime updated_at is missing."
    }
    $realtimeUpdatedAt = [DateTimeOffset]::Parse([string]$Response.evidence.realtime.updated_at)
    if ($realtimeUpdatedAt -le $BatchStart) {
        throw "API realtime updated_at must be later than batch start."
    }
    if ($null -eq $Response.evidence.historical -or
        [int64]$Response.evidence.historical.event_count -lt ($TrinoBaseline + 2)) {
        throw "API historical event_count is below Trino baseline plus two."
    }
    $historicalLatestEventTime = $null
    if ($Response.evidence.historical.PSObject.Properties.Name -contains "latest_event_time" -and
        -not [string]::IsNullOrWhiteSpace([string]$Response.evidence.historical.latest_event_time)) {
        $historicalLatestEventTime = [DateTimeOffset]::Parse(
            [string]$Response.evidence.historical.latest_event_time
        )
    }
    return [pscustomobject]@{
        GeneratedAt = $generatedAt
        Analyzer = [string]$Response.analyzer
        Warnings = @($Response.warnings)
        RealtimePv = [int64]$Response.evidence.realtime.pv
        RealtimeUv = [int64]$Response.evidence.realtime.uv
        RealtimeUpdatedAt = $realtimeUpdatedAt
        HistoricalEventCount = [int64]$Response.evidence.historical.event_count
        HistoricalLatestEventTime = $historicalLatestEventTime
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

function Write-ProductionJsonAtomic {
    param(
        [Parameter(Mandatory = $true)][object]$Value,
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$PartialPath
    )

    $directory = Split-Path -Parent $Path
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
    $json = $Value | ConvertTo-Json -Depth 15
    [System.IO.File]::WriteAllText(
        $PartialPath,
        $json,
        (New-Object System.Text.UTF8Encoding($false))
    )
    Move-Item -LiteralPath $PartialPath -Destination $Path -Force
}

function Enter-ProductionRunLock {
    param([Parameter(Mandatory = $true)][string]$Path)

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Path) | Out-Null
    try {
        return [IO.File]::Open(
            $Path,
            [IO.FileMode]::OpenOrCreate,
            [IO.FileAccess]::ReadWrite,
            [IO.FileShare]::None
        )
    } catch {
        throw "Another Chapter 9 production verifier is already running."
    }
}

function Initialize-ProductionEvidenceRun {
    param(
        [Parameter(Mandatory = $true)][string]$FinalPath,
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][string]$DorisJobId
    )

    $directory = Split-Path -Parent $FinalPath
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
    $archivedPath = $null
    if (Test-Path -LiteralPath $FinalPath -PathType Leaf) {
        $oldRunId = "unknown"
        try {
            $old = Get-Content -Raw -LiteralPath $FinalPath | ConvertFrom-Json
            if ([string]$old.run_id -match "^[A-Za-z0-9._-]+$") { $oldRunId = [string]$old.run_id }
        } catch {}
        $archivedPath = Join-Path $directory "production-verification.audit-$oldRunId.json"
        if (Test-Path -LiteralPath $archivedPath) {
            $archivedPath = Join-Path $directory (
                "production-verification.audit-$oldRunId-$([Guid]::NewGuid().ToString('N')).json"
            )
        }
        Move-Item -LiteralPath $FinalPath -Destination $archivedPath
    }
    $prefix = Join-Path $directory "production-verification.$RunId"
    $paths = [pscustomobject]@{
        FinalPath = $FinalPath
        ArchivedPath = $archivedPath
        PartialPath = "$prefix.partial"
        InProgressPath = "$prefix.in-progress.json"
        FailedPath = "$prefix.failed.json"
        StatePartialPath = "$prefix.state.partial"
    }
    Write-ProductionJsonAtomic -Value ([ordered]@{
        status = "in_progress"
        run_id = $RunId
        doris_job_id = $DorisJobId
        events_sent = $false
        initial_sent = $false
        late_sent = $false
        started_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
    }) -Path $paths.InProgressPath -PartialPath $paths.StatePartialPath
    return $paths
}

function Assert-ProductionResumeAllowed {
    param(
        [Parameter(Mandatory = $true)][string]$Directory,
        [Parameter(Mandatory = $true)][string]$FinalPath,
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][string]$DorisJobId
    )

    if ($RunId -notmatch '^chapter9-production-[0-9a-f]{32}$') {
        throw "Resume run ID is invalid."
    }
    if (Test-Path -LiteralPath $FinalPath -PathType Leaf) {
        throw "Resume is forbidden while fixed success evidence exists."
    }
    $inProgress = @(Get-ChildItem -LiteralPath $Directory `
        -Filter "production-verification.$RunId*.in-progress.json" -File `
        -ErrorAction SilentlyContinue)
    if ($inProgress.Count -ne 0) {
        throw "Resume run already has an in-progress attempt."
    }
    $failedPaths = @(Get-ChildItem -LiteralPath $Directory `
        -Filter "production-verification.$RunId*.failed.json" -File `
        -ErrorAction SilentlyContinue)
    if ($failedPaths.Count -eq 0) {
        throw "Resume requires matching failed evidence."
    }
    $resumeChain = @($failedPaths | ForEach-Object {
        try {
            $failed = Get-Content -Raw -LiteralPath $_.FullName | ConvertFrom-Json
            $failedAt = [DateTimeOffset]::Parse([string]$failed.failed_at_utc)
        } catch {
            throw "Resume failed evidence is unreadable or has no valid failed_at_utc: $($_.FullName)"
        }
        if ([string]$failed.status -ne "failed" -or [string]$failed.run_id -ne $RunId -or
            $failed.events_sent -ne $true -or [string]$failed.doris_job_id -ne $DorisJobId) {
            throw "Resume failed evidence status/run/events/Doris identity mismatch: $($_.FullName)"
        }
        [pscustomobject]@{
            Path = $_.FullName
            FailedAt = $failedAt
            Evidence = $failed
        }
    } | Sort-Object FailedAt)
    if (@($resumeChain | Group-Object { $_.FailedAt.ToString("o") } |
        Where-Object { $_.Count -ne 1 }).Count -ne 0) {
        throw "Resume failed evidence timestamps must uniquely order the resume chain."
    }
    foreach ($path in @(Get-ChildItem -LiteralPath $Directory `
        -Filter "production-verification*.json" -File -ErrorAction SilentlyContinue)) {
        try { $record = Get-Content -Raw -LiteralPath $path.FullName | ConvertFrom-Json } catch { continue }
        if ([string]$record.run_id -eq $RunId -and [string]$record.status -eq "success") {
            throw "Resume is forbidden for a successful logical run."
        }
    }
    $latest = $resumeChain[-1]
    return [pscustomobject]@{
        SourceFailedPath = $latest.Path
        SourceFailedEvidence = $latest.Evidence
        ResumeChainPaths = @($resumeChain | ForEach-Object { $_.Path })
        ResumeChain = @($resumeChain | ForEach-Object {
            [pscustomobject]@{
                path = $_.Path
                failed_at_utc = $_.FailedAt.ToString("o")
                error = [string]$_.Evidence.error
                events_sent = [bool]$_.Evidence.events_sent
                doris_job_id = [string]$_.Evidence.doris_job_id
            }
        })
    }
}

function Initialize-ProductionResumeEvidenceRun {
    param(
        [Parameter(Mandatory = $true)][string]$FinalPath,
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][string]$DorisJobId,
        [Parameter(Mandatory = $true)][string]$SourceFailedPath,
        [string[]]$ResumeChainPaths = @()
    )

    if (Test-Path -LiteralPath $FinalPath) {
        throw "Resume cannot initialize while fixed success evidence exists."
    }
    if (-not (Test-Path -LiteralPath $SourceFailedPath -PathType Leaf)) {
        throw "Resume source failed evidence is missing."
    }
    $directory = Split-Path -Parent $FinalPath
    $attemptId = [Guid]::NewGuid().ToString("N")
    $prefix = Join-Path $directory "production-verification.$RunId.resume-$attemptId"
    if ($ResumeChainPaths.Count -eq 0) { $ResumeChainPaths = @($SourceFailedPath) }
    if ($SourceFailedPath -ne $ResumeChainPaths[-1]) {
        throw "Resume source failed evidence must be the latest resume chain entry."
    }
    foreach ($chainPath in $ResumeChainPaths) {
        if (-not (Test-Path -LiteralPath $chainPath -PathType Leaf)) {
            throw "Resume chain evidence is missing: $chainPath"
        }
    }
    $paths = [pscustomobject]@{
        FinalPath = $FinalPath
        ArchivedPath = $null
        PartialPath = "$prefix.partial"
        InProgressPath = "$prefix.in-progress.json"
        FailedPath = "$prefix.failed.json"
        StatePartialPath = "$prefix.state.partial"
        SourceFailedPath = $SourceFailedPath
        ResumeChainPaths = @($ResumeChainPaths)
        ResumeAttemptId = $attemptId
    }
    Write-ProductionJsonAtomic -Value ([ordered]@{
        status = "resume_in_progress"
        run_id = $RunId
        logical_run_resumed = $true
        source_failed_evidence = $SourceFailedPath
        resume_chain = @($ResumeChainPaths)
        doris_job_id = $DorisJobId
        events_sent = $true
        initial_sent = $true
        late_sent = $false
        started_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
    }) -Path $paths.InProgressPath -PartialPath $paths.StatePartialPath
    return $paths
}

function Write-ProductionInProgressState {
    param([Parameter(Mandatory = $true)][object]$RunState)

    $state = [ordered]@{
        status = "in_progress"
        run_id = [string]$RunState.RunId
        doris_job_id = [string]$RunState.DorisJobId
        events_sent = ([bool]$RunState.InitialSent -or [bool]$RunState.LateSent)
        initial_sent = [bool]$RunState.InitialSent
        late_sent = [bool]$RunState.LateSent
        updated_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
    }
    if ([bool]$RunState.LogicalRunResumed) {
        $state.status = "resume_in_progress"
        $state.logical_run_resumed = $true
        $state.source_failed_evidence = [string]$RunState.SourceFailedEvidence
        $state.resume_chain = @($RunState.ResumeChain)
        $state.resume_action = [string]$RunState.ResumeAction
    }
    Write-ProductionJsonAtomic -Value $state -Path $RunState.RunPaths.InProgressPath `
        -PartialPath $RunState.RunPaths.StatePartialPath
}

function Set-ProductionResumeRunState {
    param(
        [Parameter(Mandatory = $true)][object]$RunState,
        [Parameter(Mandatory = $true)][object]$ResumeState
    )

    if (-not [bool]$RunState.LogicalRunResumed -or -not [bool]$RunState.InitialSent) {
        throw "Resume run state must identify an already-sent logical run."
    }
    if ([bool]$RunState.LateSent) {
        throw "Resume run state was already initialized."
    }
    if ($RunState.PSObject.Properties.Name -notcontains "ResumeAction") {
        $RunState | Add-Member -NotePropertyName ResumeAction -NotePropertyValue $null
    }
    $RunState.ResumeAction = [string]$ResumeState.ResumeAction
    switch ($RunState.ResumeAction) {
        "send_late" { $RunState.LateSent = $false }
        "read_only_finalize" { $RunState.LateSent = $true }
        default { throw "Resume action is invalid: $($RunState.ResumeAction)" }
    }
    Write-ProductionInProgressState -RunState $RunState
}

function Assert-ProductionRunAllowed {
    param(
        [Parameter(Mandatory = $true)][string]$Directory,
        [Parameter(Mandatory = $true)][string]$DorisJobId
    )

    foreach ($path in @(Get-ChildItem -LiteralPath $Directory -Filter "production-verification*.json" `
        -File -ErrorAction SilentlyContinue)) {
        try { $record = Get-Content -Raw -LiteralPath $path.FullName | ConvertFrom-Json } catch { continue }
        $recordDorisId = [string]$record.doris_job_id
        if (-not $recordDorisId -and $record.flink.jobs.doris.id) {
            $recordDorisId = [string]$record.flink.jobs.doris.id
        }
        $contaminated = [string]$record.status -eq "success" -or [bool]$record.events_sent
        if ($contaminated -and $recordDorisId -eq $DorisJobId) {
            throw "Doris job $DorisJobId already has a sent production verification run."
        }
    }
}

function Invoke-ProductionSendOnce {
    param(
        [Parameter(Mandatory = $true)][object]$RunState,
        [Parameter(Mandatory = $true)][ValidateSet("Initial", "Late")][string]$Stage,
        [Parameter(Mandatory = $true)][scriptblock]$Action
    )

    $property = if ($Stage -eq "Initial") { "InitialSent" } else { "LateSent" }
    if ([bool]$RunState.$property) { throw "Production run stage $Stage was already sent." }
    $RunState.$property = $true
    Write-ProductionInProgressState -RunState $RunState
    & $Action
}

function Write-ProductionRunFailure {
    param(
        [Parameter(Mandatory = $true)][object]$RunPaths,
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][string]$ErrorMessage,
        [Parameter(Mandatory = $true)][bool]$EventsSent,
        [AllowNull()][string]$DorisJobId
    )

    Remove-Item -LiteralPath $RunPaths.InProgressPath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $RunPaths.PartialPath -Force -ErrorAction SilentlyContinue
    Write-ProductionJsonAtomic -Value ([ordered]@{
        status = "failed"
        run_id = $RunId
        doris_job_id = $DorisJobId
        events_sent = $EventsSent
        failed_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
        error = $ErrorMessage
    }) -Path $RunPaths.FailedPath -PartialPath $RunPaths.StatePartialPath
}

function Write-ProductionEvidenceAtomic {
    param(
        [Parameter(Mandatory = $true)][object]$Evidence,
        [Parameter(Mandatory = $true)][object]$RunPaths
    )

    Write-ProductionJsonAtomic -Value $Evidence -Path $RunPaths.FinalPath `
        -PartialPath $RunPaths.PartialPath
    Remove-Item -LiteralPath $RunPaths.InProgressPath -Force -ErrorAction SilentlyContinue
}

if ($FunctionsOnly) { return }

$repoRoot = Split-Path -Parent $PSScriptRoot
$manifestPath = Join-Path $repoRoot "tmp/chapter-9/cutover-manifest.json"
$evidencePath = Join-Path $repoRoot "tmp/chapter-9/production-verification.json"
$evidenceDirectory = Split-Path -Parent $evidencePath
$lockPath = Join-Path $evidenceDirectory "production-verification.lock"
$rawTopic = "user_behavior_events"
$logicalRunResumed = -not [string]::IsNullOrWhiteSpace($ResumeRunId)
$runId = if ($logicalRunResumed) {
    $ResumeRunId
} else {
    "chapter9-production-" + [Guid]::NewGuid().ToString("N")
}
$runState = [pscustomobject]@{
    InitialSent = $logicalRunResumed
    LateSent = $false
    RunId = $runId
    DorisJobId = $null
    RunPaths = $null
    LogicalRunResumed = $logicalRunResumed
    SourceFailedEvidence = $null
    ResumeChain = @()
    ResumeAction = $null
}
$runPaths = $null
$runLock = $null
$manifest = $null
$resumeAuthorization = $null
$resumeState = $null
$resumeTrinoPre = $null

Push-Location $repoRoot
try {
    $runLock = Enter-ProductionRunLock -Path $lockPath
    if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
        throw "Cutover manifest is missing: $manifestPath"
    }
    $manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
    if ($logicalRunResumed) {
        $resumeAuthorization = Assert-ProductionResumeAllowed -Directory $evidenceDirectory `
            -FinalPath $evidencePath -RunId $runId `
            -DorisJobId ([string]$manifest.doris_job_id)
        $runPaths = Initialize-ProductionResumeEvidenceRun -FinalPath $evidencePath `
            -RunId $runId -DorisJobId ([string]$manifest.doris_job_id) `
            -SourceFailedPath $resumeAuthorization.SourceFailedPath `
            -ResumeChainPaths $resumeAuthorization.ResumeChainPaths
        $runState.SourceFailedEvidence = $resumeAuthorization.SourceFailedPath
        $runState.ResumeChain = @($resumeAuthorization.ResumeChainPaths)
    } else {
        Assert-ProductionRunAllowed -Directory $evidenceDirectory `
            -DorisJobId ([string]$manifest.doris_job_id)
        $runPaths = Initialize-ProductionEvidenceRun -FinalPath $evidencePath -RunId $runId `
            -DorisJobId ([string]$manifest.doris_job_id)
    }
    $runState.DorisJobId = [string]$manifest.doris_job_id
    $runState.RunPaths = $runPaths
    $initialFlink = Wait-ProductionJobsAndCapacity -Manifest $manifest -TimeoutSeconds $TimeoutSeconds
    $expectedJobs = @(Get-ProductionExpectedJobs -Manifest $manifest)
    $checkpointBaselines = [ordered]@{}
    foreach ($expected in $expectedJobs) {
        $checkpointBaselines[$expected.Key] = Get-ProductionCheckpointEvidence `
            -JobId $expected.Id -ExpectedName $expected.Name
    }
    $dorisBaseline = Get-ProductionDorisMetrics
    $eventIds = [ordered]@{
        duplicate = "$runId-duplicate"
        malformed = "$runId-malformed"
        missing_required = "$runId-missing"
        invalid_time = "$runId-invalid-time"
        future = "$runId-future"
        advancer = "$runId-advancer"
        late = "$runId-late"
    }
    if ($logicalRunResumed) {
        $resumeState = Get-ProductionResumeTopicState -RunId $runId
        Set-ProductionResumeRunState -RunState $runState -ResumeState $resumeState
        $batchStart = $resumeState.BatchStart
        $lateEventTime = $resumeState.LateEventTime
        $late = $resumeState.LateJson
        $failedAt = [DateTimeOffset]::Parse(
            [string]$resumeAuthorization.SourceFailedEvidence.failed_at_utc
        )
        if ($failedAt -le $batchStart) {
            throw "Resume failed evidence timestamp does not follow reconstructed batch start."
        }
        $trinoCurrent = Get-ProductionTrinoBaseline
        $resumeTrinoPre = Get-ProductionTrinoRunEvidence `
            -CleanEventIds @($eventIds.duplicate, $eventIds.advancer) `
            -ExcludedEventIds @(
                $eventIds.malformed, $eventIds.missing_required, $eventIds.invalid_time,
                $eventIds.future, $eventIds.late
            ) -DuplicateEventId $eventIds.duplicate
        if ($resumeTrinoPre.EventCount -ne 2 -or $resumeTrinoPre.DistinctEventId -ne 2 -or
            $resumeTrinoPre.DistinctUserId -ne 2 -or
            $resumeTrinoPre.ExcludedEventCount -ne 0 -or
            $resumeTrinoPre.DuplicateEventCount -ne 1) {
            throw "Resume Trino preflight does not prove the exact existing two clean rows."
        }
        $trinoBaseline = Get-ProductionRecoveredTrinoBaseline `
            -CurrentEventCount $trinoCurrent.EventCount `
            -CurrentDistinctEventId $trinoCurrent.DistinctEventId `
            -RunEventCount $resumeTrinoPre.EventCount `
            -RunDistinctEventId $resumeTrinoPre.DistinctEventId
        Write-Host "[chapter9-production-verify] resuming logical run $runId; action=$($resumeState.ResumeAction)"
    } else {
        $trinoBaseline = Get-ProductionTrinoBaseline
        $batchStart = [DateTimeOffset]::UtcNow
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
        $lateEventTime = $now.AddSeconds(-30)
        $late = New-ProductionEventJson -EventId $eventIds.late -UserId $userOne `
            -EventTime $lateEventTime.ToString("o") -RunId $runId
        Write-Host "[chapter9-production-verify] sending unique run $runId"
        Invoke-ProductionSendOnce -RunState $runState -Stage Initial -Action {
            Send-ProductionKafkaValue -Topic $rawTopic -Value $duplicate
            Send-ProductionKafkaValue -Topic $rawTopic -Value $duplicate
            Send-ProductionKafkaValue -Topic $rawTopic -Value $malformed
            Send-ProductionKafkaValue -Topic $rawTopic -Value $missingJson
            Send-ProductionKafkaValue -Topic $rawTopic -Value $invalidTime
            Send-ProductionKafkaValue -Topic $rawTopic -Value $future
            Send-ProductionKafkaValue -Topic $rawTopic -Value $advancer
        }
    }

    $productionJob = @($expectedJobs | Where-Object { $_.Key -eq "production" })[0]
    if ($logicalRunResumed -and $resumeState.ResumeAction -eq "read_only_finalize") {
        $watermarkEvidence = Get-ProductionReadOnlyWatermarkProof `
            -ResumeState $resumeState -ResumeChain $resumeAuthorization.ResumeChain `
            -JobId $productionJob.Id -ExpectedName $productionJob.Name `
            -ThresholdEpochMs $lateEventTime.ToUnixTimeMilliseconds()
        $productionFinalCheckpoint = Wait-NewProductionCheckpoint -JobId $productionJob.Id `
            -ExpectedName $productionJob.Name `
            -Baseline $checkpointBaselines.production.Completed `
            -TimeoutSeconds $TimeoutSeconds
    } else {
        $watermarkEvidence = Wait-ProductionWatermarkPast -JobId $productionJob.Id `
            -ExpectedName $productionJob.Name `
            -ThresholdEpochMs $lateEventTime.ToUnixTimeMilliseconds()
        $preLateCheckpoint = Get-ProductionCheckpointEvidence -JobId $productionJob.Id `
            -ExpectedName $productionJob.Name
        Invoke-ProductionSendOnce -RunState $runState -Stage Late -Action {
            Send-ProductionKafkaValue -Topic $rawTopic -Value $late
        }
        $productionFinalCheckpoint = Wait-NewProductionCheckpoint -JobId $productionJob.Id `
            -ExpectedName $productionJob.Name -Baseline $preLateCheckpoint.Completed `
            -TimeoutSeconds $TimeoutSeconds
    }

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
        $groups[$spec.Key] = Wait-ProductionKafkaGroupReadableLagZero -Group $spec.Group `
            -Topic $spec.Topic -ExpectedPartitions $rawPartitions -TimeoutSeconds $TimeoutSeconds
    }

    $checkpointFinals = [ordered]@{ production = $productionFinalCheckpoint }
    foreach ($expected in @($expectedJobs | Where-Object { $_.Key -ne "production" })) {
        $checkpointFinals[$expected.Key] = Wait-NewProductionCheckpoint -JobId $expected.Id `
            -ExpectedName $expected.Name -Baseline $checkpointBaselines[$expected.Key].Completed `
            -TimeoutSeconds $TimeoutSeconds
    }
    $finalFlink = Wait-ProductionJobsAndCapacity -Manifest $manifest -TimeoutSeconds $TimeoutSeconds
    $dorisFinal = Wait-ProductionDorisMetrics -BatchStart $batchStart `
        -TimeoutSeconds $TimeoutSeconds
    $cleanEventIds = @($eventIds.duplicate, $eventIds.advancer)
    $excludedEventIds = @(
        $eventIds.malformed,
        $eventIds.missing_required,
        $eventIds.invalid_time,
        $eventIds.future,
        $eventIds.late
    )
    $trinoFinal = Wait-ProductionTrinoEvidence -CleanEventIds $cleanEventIds `
        -ExcludedEventIds $excludedEventIds -DuplicateEventId $eventIds.duplicate `
        -BaselineEventCount $trinoBaseline.EventCount `
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
            cli_lag = $description.CliLag
            readable_data_lag = $description.ReadableDataLag
            partitions = @($description.Rows | ForEach-Object {
                $partition = $_.Partition
                [ordered]@{
                    partition = $partition
                    current_offset = $_.CurrentOffset
                    log_end_offset = $_.LogEndOffset
                    cli_lag = $_.Lag
                    readable_data_lag = 0
                    classifications = @($description.Classifications | Where-Object {
                        $_.Partition -eq $partition
                    } | ForEach-Object {
                        [ordered]@{
                            offset = $_.Offset
                            kind = $_.Kind
                            control_type = $_.ControlType
                        }
                    })
                }
            })
        }
    }
    $evidence = [ordered]@{
        status = "success"
        run_id = $runId
        logical_run_resumed = $logicalRunResumed
        source_failed_evidence = if ($logicalRunResumed) {
            [string]$resumeAuthorization.SourceFailedPath
        } else { $null }
        resume_chain = if ($logicalRunResumed) {
            @($resumeAuthorization.ResumeChain)
        } else { @() }
        doris_job_id = [string]$manifest.doris_job_id
        events_sent = $true
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
        resume = if ($logicalRunResumed) {
            [ordered]@{
                source_failed_status = [string]$resumeAuthorization.SourceFailedEvidence.status
                source_failed_events_sent = [bool]$resumeAuthorization.SourceFailedEvidence.events_sent
                pre_resume_counts = [ordered]@{
                    raw = $resumeState.Raw
                    clean = $resumeState.Clean
                    dlq = $resumeState.Dlq
                    late = $resumeState.Late
                }
                resume_action = $resumeState.ResumeAction
                send_action = if ($resumeState.ResumeAction -eq "send_late") {
                    "late_only"
                } else { "none" }
                initial_events_resent = $false
                late_event_sent_this_attempt = ($resumeState.ResumeAction -eq "send_late")
                events_sent_this_attempt = ($resumeState.ResumeAction -eq "send_late")
                reconstructed_fields = [ordered]@{
                    batch_start_utc = $batchStart.ToString("o")
                    batch_start_basis = $resumeState.BatchStartBasis
                    advancer_event_time = $resumeState.AdvancerEventTime.ToString("o")
                    late_event_time = $lateEventTime.ToString("o")
                    late_event_time_basis = $resumeState.LateEventTimeBasis
                    trino_baseline_sample_type = $trinoBaseline.SampleType
                    trino_baseline_basis = $trinoBaseline.Basis
                }
                trino_preexisting_exact = $resumeTrinoPre
            }
        } else { $null }
        flink = [ordered]@{
            overview = [ordered]@{
                taskmanagers = [int]$finalFlink.Overview.taskmanagers
                slots_total = [int]$finalFlink.Overview."slots-total"
                slots_available = [int]$finalFlink.Overview."slots-available"
                jobs_running = [int]$finalFlink.Overview."jobs-running"
            }
            jobs = $jobEvidence
            watermark_gate = [ordered]@{
                watermark_proof_source = if ($logicalRunResumed -and
                    $resumeState.ResumeAction -eq "read_only_finalize") {
                    $watermarkEvidence.WatermarkProofSource
                } else { "live_operator_watermark" }
                vertex_id = if ($watermarkEvidence.CurrentMetric) {
                    $watermarkEvidence.CurrentMetric.VertexId
                } else { $watermarkEvidence.VertexId }
                metric_id = if ($watermarkEvidence.CurrentMetric) {
                    $watermarkEvidence.CurrentMetric.MetricId
                } else { $watermarkEvidence.MetricId }
                watermark = if ($watermarkEvidence.CurrentMetric) {
                    $watermarkEvidence.CurrentMetric.Watermark
                } else { $watermarkEvidence.Watermark }
                late_event_timestamp = $watermarkEvidence.ThresholdEpochMs
                current_metric = if ($watermarkEvidence.CurrentMetric) {
                    [ordered]@{
                        vertex_id = $watermarkEvidence.CurrentMetric.VertexId
                        metric_id = $watermarkEvidence.CurrentMetric.MetricId
                        watermark = $watermarkEvidence.CurrentMetric.Watermark
                    }
                } elseif ($logicalRunResumed -and
                    $resumeState.ResumeAction -eq "read_only_finalize") { $null } else {
                    [ordered]@{
                        vertex_id = $watermarkEvidence.VertexId
                        metric_id = $watermarkEvidence.MetricId
                        watermark = $watermarkEvidence.Watermark
                    }
                }
                late_output_proof = $watermarkEvidence.LateOutputProof
                prior_api_gate_failed_evidence = $watermarkEvidence.PriorGateEvidence
            }
        }
        kafka_groups = $groupEvidence
        doris = [ordered]@{
            baseline = [ordered]@{
                pv = $dorisBaseline.Pv
                uv = $dorisBaseline.Uv
                pv_updated_at = $dorisBaseline.PvUpdatedAt.ToString("o")
                uv_updated_at = $dorisBaseline.UvUpdatedAt.ToString("o")
            }
            final = [ordered]@{
                pv = $dorisFinal.Pv
                uv = $dorisFinal.Uv
                pv_updated_at = $dorisFinal.PvUpdatedAt.ToString("o")
                uv_updated_at = $dorisFinal.UvUpdatedAt.ToString("o")
            }
        }
        trino = [ordered]@{
            baseline = $trinoBaseline
            total_final = $trinoFinal.Total
            exact_clean_ids = $cleanEventIds
            exact_counts = $trinoFinal.Run
            excluded_validation_and_late_ids = $excludedEventIds
            duplicate_event = [ordered]@{
                id = $eventIds.duplicate
                iceberg_count = $trinoFinal.Run.DuplicateEventCount
                assertion = "measured_sql"
            }
        }
        api = [ordered]@{
            generated_at = $apiFinal.Evidence.GeneratedAt.ToString("o")
            analyzer = $apiFinal.Evidence.Analyzer
            warnings = @($apiFinal.Evidence.Warnings)
            realtime_pv = $apiFinal.Evidence.RealtimePv
            realtime_uv = $apiFinal.Evidence.RealtimeUv
            realtime_updated_at = $apiFinal.Evidence.RealtimeUpdatedAt.ToString("o")
            historical_event_count = $apiFinal.Evidence.HistoricalEventCount
            historical_latest_event_time = if ($apiFinal.Evidence.HistoricalLatestEventTime) {
                $apiFinal.Evidence.HistoricalLatestEventTime.ToString("o")
            } else { $null }
        }
    }
    Write-ProductionEvidenceAtomic -Evidence $evidence -RunPaths $runPaths

    Write-Host "[chapter9-production-verify] passed run_id=$runId"
    Write-Host "raw=8 clean=2 dlq=5 late=1 duplicate_clean=1; raw = clean + dlq + late"
    Write-Host "doris pv=$($dorisFinal.Pv) uv=$($dorisFinal.Uv); trino exact=2/2/2 excluded=0"
    Write-Host "api generated_at=$($apiFinal.Evidence.GeneratedAt.ToString('o')) analyzer=$($apiFinal.Evidence.Analyzer)"
} catch {
    $failure = $_
    if ($null -ne $runPaths) {
        try {
            Write-ProductionRunFailure -RunPaths $runPaths -RunId $runId `
                -ErrorMessage $failure.Exception.Message `
                -EventsSent ($runState.InitialSent -or $runState.LateSent) `
                -DorisJobId $(if ($null -ne $manifest) { [string]$manifest.doris_job_id } else { $null })
        } catch {}
    }
    throw $failure
} finally {
    if ($null -ne $runLock) { $runLock.Dispose() }
    Pop-Location
}
