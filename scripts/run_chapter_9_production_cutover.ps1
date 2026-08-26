param(
    [switch]$TrafficPaused,
    [switch]$ResumePartial,
    [switch]$FunctionsOnly
)

$ErrorActionPreference = "Stop"
$script:Chapter9FunctionsOnly = [bool]$FunctionsOnly

function Invoke-DockerCommand {
    param(
        [string[]]$Arguments,
        [string]$FailureMessage
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
            if (-not $message -and $null -ne $_.TargetObject) { $message = [string]$_.TargetObject }
            if ($message) { $message }
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

function Assert-Chapter9StateUri {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][ValidateSet("checkpoint", "savepoint")][string]$Kind
    )

    $base = "s3a://flink-state/$($Kind)s/chapter-9"
    if ([string]::IsNullOrWhiteSpace($Path) -or $Path -match "\.\." -or
        $Path -match '(?:^|/)\.(?:/|$)' -or
        $Path -cnotmatch "^$([regex]::Escape($base))(?:/[A-Za-z0-9._-]+)*$") {
        throw "Invalid Chapter 9 $Kind URI: $Path"
    }
    return $Path
}

function Get-Chapter9StateUri {
    param(
        [Parameter(Mandatory = $true)][ValidateSet("checkpoint", "savepoint")][string]$Kind,
        [System.Collections.IDictionary]$Environment
    )

    $default = "s3a://flink-state/$($Kind)s/chapter-9"
    $name = "CHAPTER9_$($Kind.ToUpperInvariant())_URI"
    $configured = if ($null -ne $Environment -and $Environment.Contains($name)) {
        [string]$Environment[$name]
    } else {
        [Environment]::GetEnvironmentVariable($name)
    }
    if ([string]::IsNullOrWhiteSpace($configured)) { $configured = $default }
    return Assert-Chapter9StateUri -Path $configured.Trim() -Kind $Kind
}

function Invoke-FlinkJobManagerCommand {
    param(
        [Parameter(Mandatory = $true)][string[]]$Command,
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )

    $root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
    $arguments = @(
        "compose", "--env-file", (Join-Path $root "infra/.env.example"),
        "-f", (Join-Path $root "infra/docker-compose.yml"),
        "exec", "-T", "flink-jobmanager"
    ) + $Command
    return Invoke-DockerCommand -Arguments $arguments -FailureMessage $FailureMessage
}

function Invoke-FlinkRest {
    param([Parameter(Mandatory = $true)][string]$Resource)

    if ($script:Chapter9FunctionsOnly) {
        return Invoke-RestMethod -Uri "http://localhost:8081$Resource" -TimeoutSec 5
    }
    $output = Invoke-FlinkJobManagerCommand -Command @(
        "curl", "--fail", "--silent", "--show-error", "http://localhost:8081$Resource"
    ) -FailureMessage "Flink REST request failed: $Resource"
    return ($output -join "`n" | ConvertFrom-Json)
}

function Get-WorkspaceMountSource([string]$Container) {
    $output = Invoke-DockerCommand -Arguments @("inspect", $Container) `
        -FailureMessage "Docker inspect failed for $Container."
    $inspection = @($output -join "`n" | ConvertFrom-Json)
    $workspaceMount = @($inspection[0].Mounts | Where-Object { $_.Destination -eq "/workspace" }) |
        Select-Object -First 1
    if ($null -eq $workspaceMount) { throw "Container $Container has no /workspace mount." }
    return [string]$workspaceMount.Source
}

function Assert-ContainerBindMountSource {
    param(
        [string]$Container,
        [string]$Destination,
        [string]$ExpectedSource
    )

    $output = Invoke-DockerCommand -Arguments @("inspect", $Container) `
        -FailureMessage "Docker inspect failed for $Container."
    $inspection = @($output -join "`n" | ConvertFrom-Json)
    $mounts = @($inspection[0].Mounts | Where-Object { $_.Destination -eq $Destination })
    if ($mounts.Count -ne 1 -or $mounts[0].Type -ne "bind") {
        throw "Container $Container must have exactly one bind mount at $Destination."
    }
    $actual = [System.IO.Path]::GetFullPath([string]$mounts[0].Source).TrimEnd("\", "/")
    $expected = [System.IO.Path]::GetFullPath($ExpectedSource).TrimEnd("\", "/")
    if (-not [string]::Equals($actual, $expected, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Container $Container mount source mismatch at $Destination. Expected $expected, got $actual."
    }
}

function Assert-ContainerRunning([string]$Container) {
    $output = Invoke-DockerCommand -Arguments @("inspect", "--format", "{{.State.Running}}", $Container) `
        -FailureMessage "Required container is unavailable: $Container."
    if (($output -join "").Trim() -ne "true") { throw "Required container is not running: $Container." }
}

function Assert-FlinkCapacity([object]$Overview) {
    if ([int]$Overview.taskmanagers -ne 1) {
        throw "Expected exactly one TaskManager, got $($Overview.taskmanagers)."
    }
    if ([int]$Overview."slots-total" -ne 4) {
        throw "Expected four total slots, got $($Overview.'slots-total')."
    }
}

function ConvertFrom-KafkaOffsets {
    param([string[]]$Lines, [string]$ExpectedTopic)

    $rows = @()
    $partitions = @{}
    foreach ($line in $Lines) {
        $value = ([string]$line).Trim()
        if (-not $value) { continue }
        if ($value -notmatch "^([^:\s]+):(\d+):(\d+)$") {
            throw "Unexpected kafka-get-offsets row: $value"
        }
        if ($Matches[1] -ne $ExpectedTopic) {
            throw "Unexpected Kafka topic $($Matches[1]); expected $ExpectedTopic."
        }
        $partition = [int]$Matches[2]
        if ($partitions.ContainsKey($partition)) {
            throw "Duplicate Kafka offset partition: $partition."
        }
        $partitions[$partition] = $true
        $rows += [pscustomobject]@{
            Topic = $Matches[1]
            Partition = $partition
            Offset = [int64]$Matches[3]
        }
    }
    if ($rows.Count -eq 0) { throw "kafka-get-offsets returned no partition offsets." }
    return @($rows | Sort-Object Partition | ForEach-Object {
        "partition:$($_.Partition),offset:$($_.Offset)"
    })
}

function ConvertFrom-KafkaGroupDescription {
    param([string[]]$Lines, [string]$ExpectedGroup, [string]$ExpectedTopic)

    $rows = @()
    $partitions = @{}
    foreach ($line in $Lines) {
        $value = ([string]$line).Trim()
        if (-not $value -or $value -match "^GROUP\s+TOPIC" -or $value -match "^Consumer group ") {
            continue
        }
        if ($value -notmatch "^(\S+)\s+(\S+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+\S+\s+\S+\s+\S+$") {
            throw "Unexpected kafka-consumer-groups row: $value"
        }
        if ($Matches[1] -ne $ExpectedGroup) {
            throw "Unexpected Kafka consumer group $($Matches[1]); expected $ExpectedGroup."
        }
        if ($Matches[2] -ne $ExpectedTopic) {
            throw "Unexpected Kafka topic $($Matches[2]); expected $ExpectedTopic."
        }
        $partition = [int]$Matches[3]
        if ($partitions.ContainsKey($partition)) {
            throw "Duplicate Kafka consumer group partition: $partition."
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
    if ($rows.Count -eq 0) { throw "Kafka consumer group description returned no partition rows." }
    $totalLag = [int64](($rows | Measure-Object -Property Lag -Sum).Sum)
    return [pscustomobject]@{ Rows = @($rows); TotalLag = $totalLag }
}

function Get-SubmittedJobId([string[]]$Lines) {
    $ids = @()
    foreach ($line in $Lines) {
        foreach ($match in [regex]::Matches([string]$line, "(?i)\b[0-9a-f]{32}\b")) {
            $ids += $match.Value.ToLowerInvariant()
        }
    }
    $uniqueIds = @($ids | Sort-Object -Unique)
    if ($uniqueIds.Count -ne 1) {
        throw "Expected exactly one submitted Flink Job ID, found $($uniqueIds.Count)."
    }
    return [string]$uniqueIds[0]
}

function Get-SavepointPath([string[]]$Lines) {
    if ($Lines.Count -ne 1) {
        throw "Expected exactly one completed Savepoint response line, found $($Lines.Count)."
    }
    $match = [regex]::Match([string]$Lines[0], '^Savepoint completed\. Path: (\S+)$')
    if (-not $match.Success) { throw "Expected exactly one completed Savepoint response line." }
    return Assert-CutoverSavepointPath -Path $match.Groups[1].Value
}

function Assert-CutoverSavepointPath([string]$Path) {
    if ([string]::IsNullOrWhiteSpace($Path) -or $Path -match "\.\." -or
        $Path -match '(?:^|/)\.(?:/|$)' -or
        $Path -cnotmatch "^s3a://flink-state/savepoints/chapter-9(?:/[A-Za-z0-9._-]+)+$") {
        throw "Invalid cutover Savepoint evidence: $Path"
    }
    return $Path
}

function Get-CutoverShadowStopResumePlan {
    param(
        [Parameter(Mandatory = $true)][object]$State,
        [Parameter(Mandatory = $true)][object]$Jobs
    )

    $expectedName = "chapter-9-datastream-quality-shadow"
    $shadowId = [string]$State.shadow_job_id
    if ($shadowId -notmatch '^[0-9a-f]{32}$') { throw "Cutover shadow Job ID is invalid." }
    $byId = @($Jobs.jobs | Where-Object { [string]$_.jid -eq $shadowId })
    if ($byId.Count -ne 1 -or [string]$byId[0].name -ne $expectedName) {
        throw "Cutover shadow Job ID/name does not match the exact manifest job."
    }
    $jobState = [string]$byId[0].state
    $mutation = $State.mutations.shadow_stop
    $resultPath = [string]$State.savepoint_path
    if (-not $resultPath) { $resultPath = [string]$mutation.result.savepoint_path }
    if (-not $resultPath) { $resultPath = [string]$mutation.result.details.savepoint_path }
    if ($resultPath) {
        Assert-CutoverSavepointPath -Path $resultPath | Out-Null
        if ($jobState -ne "FINISHED") {
            throw "Persisted shadow Savepoint requires the exact shadow job to be FINISHED, got $jobState."
        }
        return [pscustomobject]@{ Action = "complete"; SavepointPath = $resultPath }
    }
    if ($null -eq $mutation.intent) {
        if ($mutation.status -ne "not_started") {
            throw "Shadow stop outcome is unknown without a validated persisted Savepoint URI."
        }
        if ($jobState -eq "RUNNING") { return [pscustomobject]@{ Action = "stop" } }
        throw "Shadow stop has no intent and the exact shadow job is $jobState."
    }
    throw "Shadow stop outcome is unknown without a validated persisted Savepoint URI."
}

function Set-CutoverShadowStopResult {
    param(
        [Parameter(Mandatory = $true)][object]$State,
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$SavepointPath
    )

    $validatedPath = Assert-CutoverSavepointPath -Path $SavepointPath
    $mutation = $State.mutations.shadow_stop
    if ($null -eq $mutation.intent) { throw "Shadow stop result requires a persisted intent." }
    $mutation.status = "result"
    $mutation.result = [pscustomobject]@{
        status = "result"
        savepoint_path = $validatedPath
        completed_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
    }
    if ($State -is [System.Collections.IDictionary]) {
        $State["savepoint_path"] = $validatedPath
    } elseif ($State.PSObject.Properties.Name -contains "savepoint_path") {
        $State.savepoint_path = $validatedPath
    } else {
        $State | Add-Member -NotePropertyName savepoint_path -NotePropertyValue $validatedPath
    }
    Write-CutoverStateAtomic -State $State -Path $Path
    return $validatedPath
}

function Invoke-CutoverShadowStopStage {
    param(
        [Parameter(Mandatory = $true)][object]$State,
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][object]$Jobs,
        [Parameter(Mandatory = $true)][scriptblock]$StopAction
    )

    $plan = Get-CutoverShadowStopResumePlan -State $State -Jobs $Jobs
    if ($plan.Action -eq "complete") {
        $savepointPath = Set-CutoverShadowStopResult -State $State -Path $Path `
            -SavepointPath $plan.SavepointPath
        return [pscustomobject]@{ savepoint_path = $savepointPath; action = "complete" }
    }
    $stopResult = Invoke-CutoverMutation -State $State -Path $Path -Stage "shadow_stop" `
        -Operation "stop_shadow_with_savepoint" -Details @{
            job_id = [string]$State.shadow_job_id
            name = "chapter-9-datastream-quality-shadow"
            savepoint_destination = (Get-Chapter9StateUri -Kind "savepoint")
        } -Action $StopAction
    $savepointPath = Set-CutoverShadowStopResult -State $State -Path $Path `
        -SavepointPath ([string]$stopResult.savepoint_path)
    return [pscustomobject]@{ savepoint_path = $savepointPath; action = $plan.Action }
}

function Assert-FileHash([string]$Path, [string]$ExpectedHash) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Connector is not a file: $Path"
    }
    $actualHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToUpperInvariant()
    if ($actualHash -ne $ExpectedHash.ToUpperInvariant()) {
        throw "SHA-256 mismatch for $Path. Expected $ExpectedHash, got $actualHash."
    }
    return $actualHash
}

function Install-Connector {
    param(
        [hashtable]$Connector,
        [string]$DestinationDirectory,
        [string]$CacheDirectory
    )

    $destination = Join-Path $DestinationDirectory $Connector.Name
    if (Test-Path -LiteralPath $destination) {
        $hash = Assert-FileHash -Path $destination -ExpectedHash $Connector.Hash
        return [pscustomobject]@{ Name = $Connector.Name; Source = "tmp-cache"; Hash = $hash }
    }

    $partial = "$destination.partial"
    if (Test-Path -LiteralPath $partial) {
        Assert-FileHash -Path $partial -ExpectedHash $Connector.Hash | Out-Null
        Move-Item -LiteralPath $partial -Destination $destination
        return [pscustomobject]@{ Name = $Connector.Name; Source = "verified-partial"; Hash = $Connector.Hash }
    }

    $cached = Join-Path $CacheDirectory $Connector.Name
    $source = "maven:$($Connector.Url)"
    if (Test-Path -LiteralPath $cached) {
        Assert-FileHash -Path $cached -ExpectedHash $Connector.Hash | Out-Null
        Copy-Item -LiteralPath $cached -Destination $partial
        $source = "local-cache:$cached"
    } else {
        Invoke-WebRequest -UseBasicParsing -Uri $Connector.Url -OutFile $partial
    }

    Assert-FileHash -Path $partial -ExpectedHash $Connector.Hash | Out-Null
    Move-Item -LiteralPath $partial -Destination $destination
    return [pscustomobject]@{ Name = $Connector.Name; Source = $source; Hash = $Connector.Hash }
}

function Write-ManifestPartial([System.Collections.IDictionary]$Manifest, [string]$PartialPath) {
    $nextPath = "$PartialPath.next"
    $json = $Manifest | ConvertTo-Json -Depth 5
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($nextPath, $json + "`n", $utf8NoBom)
    Move-Item -LiteralPath $nextPath -Destination $PartialPath -Force
}

function Write-CutoverStateAtomic {
    param(
        [Parameter(Mandatory = $true)][object]$State,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $nextPath = "$Path.next"
    $directory = Split-Path -Parent $Path
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
    $json = $State | ConvertTo-Json -Depth 12
    [System.IO.File]::WriteAllText($nextPath, $json + "`n", (New-Object System.Text.UTF8Encoding($false)))
    Move-Item -LiteralPath $nextPath -Destination $Path -Force
}

function Assert-CutoverStateRoot {
    param([Parameter(Mandatory = $true)][string]$Path)

    $safeError = "Production submission recovery state is unsafe."
    try {
        if ([string]::IsNullOrWhiteSpace($Path) -or -not [System.IO.Path]::IsPathRooted($Path)) {
            throw $safeError
        }
        $fullPath = [System.IO.Path]::GetFullPath($Path).TrimEnd("\", "/")
        if (-not (Test-Path -LiteralPath $fullPath -PathType Container)) { throw $safeError }

        $pathRoot = [System.IO.Path]::GetPathRoot($fullPath)
        $current = $pathRoot
        $relative = $fullPath.Substring($pathRoot.Length)
        foreach ($segment in $relative.Split(@("\", "/"), [System.StringSplitOptions]::RemoveEmptyEntries)) {
            $current = Join-Path $current $segment
            $item = Get-Item -LiteralPath $current -Force
            if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw $safeError
            }
        }
        return $fullPath
    } catch {
        throw $safeError
    }
}

function Get-CutoverPrimaryRepositoryRoot {
    param([Parameter(Mandatory = $true)][string]$RepositoryRoot)

    $safeError = "Production submission recovery state is unsafe."
    try {
        $startPath = [System.IO.Path]::GetFullPath($RepositoryRoot)
        if (-not (Test-Path -LiteralPath $startPath -PathType Container)) { throw $safeError }
        $commonDirectory = @(& git -C $startPath rev-parse --path-format=absolute --git-common-dir 2>$null)
        if ($LASTEXITCODE -ne 0 -or $commonDirectory.Count -ne 1) { throw $safeError }
        $commonPath = [System.IO.Path]::GetFullPath([string]$commonDirectory[0]).TrimEnd("\", "/")
        if ([System.IO.Path]::GetFileName($commonPath) -cne ".git" -or
            -not (Test-Path -LiteralPath $commonPath -PathType Container)) {
            throw $safeError
        }
        $primaryRoot = Assert-CutoverStateRoot -Path (Split-Path -Parent $commonPath)
        $insideWorkTree = @(& git -C $primaryRoot rev-parse --is-inside-work-tree 2>$null)
        if ($LASTEXITCODE -ne 0 -or $insideWorkTree.Count -ne 1 -or [string]$insideWorkTree[0] -cne "true") {
            throw $safeError
        }
        return $primaryRoot
    } catch {
        throw $safeError
    }
}

function Get-CutoverProductionSubmitStatePath {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [string]$StateRoot
    )

    $safeError = "Production submission recovery state is unsafe."
    try {
        $resolvedRoot = if ($PSBoundParameters.ContainsKey("StateRoot")) {
            Assert-CutoverStateRoot -Path $StateRoot
        } else {
            Get-CutoverPrimaryRepositoryRoot -RepositoryRoot $RepositoryRoot
        }
        $current = $resolvedRoot
        foreach ($segment in @("tmp", "chapter-9")) {
            $current = Join-Path $current $segment
            if (Test-Path -LiteralPath $current) {
                $item = Get-Item -LiteralPath $current -Force
                if (-not $item.PSIsContainer -or
                    ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                    throw $safeError
                }
            }
        }
        return [System.IO.Path]::GetFullPath((Join-Path $current "production-submit-state.json"))
    } catch {
        throw $safeError
    }
}

function Get-CutoverLegacyProductionSubmitStatePath {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [string]$Path
    )

    $safeError = "Production submission recovery state is unsafe."
    try {
        $root = [System.IO.Path]::GetFullPath($RepositoryRoot).TrimEnd("\", "/")
        if (-not (Test-Path -LiteralPath $root -PathType Container)) { throw $safeError }
        $expected = [System.IO.Path]::GetFullPath((Join-Path $root "tmp/chapter-9/cutover-manifest.json.partial"))
        if ($PSBoundParameters.ContainsKey("Path")) {
            if ([string]::IsNullOrWhiteSpace($Path) -or -not [System.IO.Path]::IsPathRooted($Path)) {
                throw $safeError
            }
            $candidate = [System.IO.Path]::GetFullPath($Path)
            if (-not $candidate.Equals($expected, [System.StringComparison]::OrdinalIgnoreCase)) {
                throw $safeError
            }
        }
        return $expected
    } catch {
        throw $safeError
    }
}

function Write-CutoverProductionSubmitStateAtomic {
    param(
        [Parameter(Mandatory = $true)][object]$State,
        [Parameter(Mandatory = $true)][string]$Path
    )

    Write-CutoverStateAtomic -State $State -Path $Path
}

function New-CutoverProductionSubmitState {
    param([Parameter(Mandatory = $true)][string]$Path)

    $state = [pscustomobject]@{
        schema_version = 1
        kind = "chapter9_production_submit"
        status = "not_started"
        intent = $null
        result = $null
    }
    Write-CutoverProductionSubmitStateAtomic -State $state -Path $Path
    return $state
}

function Test-CutoverRecord {
    param([object]$Value)

    return $Value -is [System.Management.Automation.PSCustomObject] -or
        $Value -is [System.Collections.IDictionary]
}

function Test-CutoverExactProperties {
    param(
        [Parameter(Mandatory = $true)][object]$Record,
        [Parameter(Mandatory = $true)][string[]]$Names
    )

    if (-not (Test-CutoverRecord -Value $Record)) { return $false }
    $actual = if ($Record -is [System.Collections.IDictionary]) {
        @($Record.Keys | ForEach-Object { [string]$_ })
    } else {
        @($Record.PSObject.Properties.Name)
    }
    if ($actual.Count -ne $Names.Count) { return $false }
    foreach ($name in $Names) {
        if ($actual -cnotcontains $name) { return $false }
    }
    return $true
}

function Test-CutoverRoundTripTimestamp {
    param([object]$Value)

    if ($Value -isnot [string] -or [string]::IsNullOrWhiteSpace([string]$Value)) { return $false }
    $parsed = [DateTimeOffset]::MinValue
    return [DateTimeOffset]::TryParse(
        [string]$Value,
        [System.Globalization.CultureInfo]::InvariantCulture,
        [System.Globalization.DateTimeStyles]::RoundtripKind,
        [ref]$parsed
    )
}

function Get-CutoverProductionSubmitIntentKind {
    param(
        [Parameter(Mandatory = $true)][object]$Intent,
        [Parameter(Mandatory = $true)][string]$ExpectedName
    )

    $safeError = "Production submission recovery state is unsafe."
    if (Test-CutoverExactProperties -Record $Intent -Names @("operation", "job_name", "created_at_utc")) {
        if ($Intent.operation -isnot [string] -or
            [string]$Intent.operation -cne "submit_chapter9_production" -or
            $Intent.job_name -isnot [string] -or [string]$Intent.job_name -cne $ExpectedName -or
            -not (Test-CutoverRoundTripTimestamp -Value $Intent.created_at_utc)) {
            throw $safeError
        }
        return "canonical"
    }
    if (Test-CutoverExactProperties -Record $Intent -Names @("operation", "details", "created_at_utc")) {
        if ($Intent.operation -isnot [string] -or
            [string]$Intent.operation -cne "submit_production_from_savepoint" -or
            -not (Test-CutoverExactProperties -Record $Intent.details -Names @("name", "savepoint_path")) -or
            $Intent.details.name -isnot [string] -or [string]$Intent.details.name -cne $ExpectedName -or
            $Intent.details.savepoint_path -isnot [string] -or
            -not (Test-CutoverRoundTripTimestamp -Value $Intent.created_at_utc)) {
            throw $safeError
        }
        Assert-CutoverSavepointPath -Path ([string]$Intent.details.savepoint_path) | Out-Null
        return "legacy"
    }
    throw $safeError
}

function Assert-CutoverProductionSubmitState {
    param(
        [Parameter(Mandatory = $true)][object]$State,
        [Parameter(Mandatory = $true)][string]$ExpectedName
    )

    $safeError = "Production submission recovery state is unsafe."
    try {
        if (-not (Test-CutoverExactProperties -Record $State -Names @(
                    "schema_version", "kind", "status", "intent", "result"
                )) -or
            $State.schema_version -isnot [int] -or [int]$State.schema_version -ne 1 -or
            $State.kind -isnot [string] -or [string]$State.kind -cne "chapter9_production_submit" -or
            $State.status -isnot [string]) {
            throw $safeError
        }

        $status = [string]$State.status
        if ($status -ceq "not_started") {
            if ($null -ne $State.intent -or $null -ne $State.result) { throw $safeError }
            return
        }

        $intentKind = Get-CutoverProductionSubmitIntentKind -Intent $State.intent -ExpectedName $ExpectedName
        if ($status -ceq "intent") {
            if ($null -ne $State.result) { throw $safeError }
            return
        }

        if ($status -ceq "result" -and $intentKind -ceq "legacy") {
            if (-not (Test-CutoverExactProperties -Record $State.result -Names @(
                        "status", "details", "completed_at_utc", "job_id"
                    )) -or
                $State.result.status -isnot [string] -or [string]$State.result.status -cne "result" -or
                -not (Test-CutoverExactProperties -Record $State.result.details -Names @("job_id")) -or
                $State.result.job_id -isnot [string] -or [string]$State.result.job_id -cnotmatch "^[0-9a-f]{32}$" -or
                $State.result.details.job_id -isnot [string] -or
                [string]$State.result.details.job_id -cne [string]$State.result.job_id -or
                -not (Test-CutoverRoundTripTimestamp -Value $State.result.completed_at_utc)) {
                throw $safeError
            }
        } else {
            if ($intentKind -cne "canonical") { throw $safeError }
            $resultNames = if ($status -ceq "result") {
                @("status", "job_id", "job_name", "completed_at_utc")
            } elseif ($status -ceq "failed") {
                @("status", "job_name", "completed_at_utc")
            } else {
                throw $safeError
            }
            if (-not (Test-CutoverExactProperties -Record $State.result -Names $resultNames) -or
                $State.result.status -isnot [string] -or [string]$State.result.status -cne $status -or
                $State.result.job_name -isnot [string] -or [string]$State.result.job_name -cne $ExpectedName -or
                -not (Test-CutoverRoundTripTimestamp -Value $State.result.completed_at_utc)) {
                throw $safeError
            }
            if ($status -ceq "result" -and
                ($State.result.job_id -isnot [string] -or [string]$State.result.job_id -cnotmatch "^[0-9a-f]{32}$")) {
                throw $safeError
            }
        }

        $created = [DateTimeOffset]::Parse(
            [string]$State.intent.created_at_utc,
            [System.Globalization.CultureInfo]::InvariantCulture
        )
        $completed = [DateTimeOffset]::Parse(
            [string]$State.result.completed_at_utc,
            [System.Globalization.CultureInfo]::InvariantCulture
        )
        if ($completed -lt $created) { throw $safeError }
    } catch {
        throw $safeError
    }
}

function Test-CutoverProductionSubmitStatesEquivalent {
    param(
        [Parameter(Mandatory = $true)][object]$Left,
        [Parameter(Mandatory = $true)][object]$Right,
        [Parameter(Mandatory = $true)][string]$ExpectedName
    )

    try {
        Assert-CutoverProductionSubmitState -State $Left -ExpectedName $ExpectedName
        Assert-CutoverProductionSubmitState -State $Right -ExpectedName $ExpectedName
        if ([string]$Left.status -cne [string]$Right.status) { return $false }
        $status = [string]$Left.status
        if ($status -ceq "not_started") { return $true }

        $leftKind = Get-CutoverProductionSubmitIntentKind -Intent $Left.intent -ExpectedName $ExpectedName
        $rightKind = Get-CutoverProductionSubmitIntentKind -Intent $Right.intent -ExpectedName $ExpectedName
        if ($leftKind -cne $rightKind -or
            [string]$Left.intent.operation -cne [string]$Right.intent.operation -or
            [string]$Left.intent.created_at_utc -cne [string]$Right.intent.created_at_utc) {
            return $false
        }
        if ($leftKind -ceq "canonical") {
            if ([string]$Left.intent.job_name -cne [string]$Right.intent.job_name) { return $false }
        } elseif ([string]$Left.intent.details.name -cne [string]$Right.intent.details.name -or
            [string]$Left.intent.details.savepoint_path -cne [string]$Right.intent.details.savepoint_path) {
            return $false
        }
        if ($status -ceq "intent") { return $true }

        if ([string]$Left.result.status -cne [string]$Right.result.status -or
            [string]$Left.result.completed_at_utc -cne [string]$Right.result.completed_at_utc) {
            return $false
        }
        if ($status -ceq "failed") {
            return [string]$Left.result.job_name -ceq [string]$Right.result.job_name
        }
        if ([string]$Left.result.job_id -cne [string]$Right.result.job_id) { return $false }
        if ($leftKind -ceq "canonical") {
            return [string]$Left.result.job_name -ceq [string]$Right.result.job_name
        }
        return [string]$Left.result.details.job_id -ceq [string]$Right.result.details.job_id
    } catch {
        return $false
    }
}

function New-CutoverMutationState {
    return [pscustomobject]@{
        status = "not_started"
        intent = $null
        result = $null
    }
}

function New-CutoverRecoveryState {
    param(
        [Parameter(Mandatory = $true)][string]$CutoverId,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $state = [pscustomobject]@{
        schema_version = 2
        cutover_id = $CutoverId
        phase = "prepared"
        mutations = [pscustomobject]@{
            shadow_stop = New-CutoverMutationState
            doris_submit = New-CutoverMutationState
            iceberg_submit = New-CutoverMutationState
            finalization = New-CutoverMutationState
        }
    }
    Write-CutoverStateAtomic -State $state -Path $Path
    return $state
}

function Set-CutoverMutationIntent {
    param(
        [Parameter(Mandatory = $true)][object]$State,
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Stage,
        [Parameter(Mandatory = $true)][string]$Operation,
        [hashtable]$Details = @{}
    )

    $mutation = $State.mutations.$Stage
    if ($null -eq $mutation) { throw "Unknown cutover mutation stage: $Stage" }
    $mutation.status = "intent"
    $mutation.intent = [pscustomobject]@{
        operation = $Operation
        details = [pscustomobject]$Details
        created_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
    }
    $mutation.result = $null
    $State.phase = $Stage
    Write-CutoverStateAtomic -State $State -Path $Path
}

function Set-CutoverMutationResult {
    param(
        [Parameter(Mandatory = $true)][object]$State,
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Stage,
        [Parameter(Mandatory = $true)][ValidateSet("result", "failed")][string]$Status,
        [hashtable]$Details = @{}
    )

    $mutation = $State.mutations.$Stage
    if ($null -eq $mutation -or $null -eq $mutation.intent) {
        throw "Cutover mutation result requires a persisted intent: $Stage"
    }
    $mutation.status = $Status
    $result = [ordered]@{
        status = $Status
        details = [pscustomobject]$Details
        completed_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
    }
    if ($Details.ContainsKey("job_id")) {
        $result["job_id"] = [string]$Details["job_id"]
    }
    $mutation.result = [pscustomobject]$result
    Write-CutoverStateAtomic -State $State -Path $Path
}

function Set-CutoverMutationJobId {
    param(
        [Parameter(Mandatory = $true)][object]$State,
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Stage,
        [Parameter(Mandatory = $true)][string]$JobId
    )

    $mutation = $State.mutations.$Stage
    if ($null -eq $mutation -or $null -eq $mutation.result) {
        throw "Cannot persist a cutover Job ID without a mutation result: $Stage"
    }
    $mutation.result | Add-Member -Force -NotePropertyName job_id -NotePropertyValue $JobId
    Write-CutoverStateAtomic -State $State -Path $Path
}

function Invoke-CutoverMutation {
    param(
        [Parameter(Mandatory = $true)][object]$State,
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Stage,
        [Parameter(Mandatory = $true)][string]$Operation,
        [hashtable]$Details = @{},
        [Parameter(Mandatory = $true)][scriptblock]$Action
    )

    Set-CutoverMutationIntent -State $State -Path $Path -Stage $Stage -Operation $Operation -Details $Details
    try {
        $value = & $Action
        $resultDetails = @{}
        if ($value -is [pscustomobject]) {
            foreach ($property in $value.PSObject.Properties) { $resultDetails[$property.Name] = $property.Value }
        }
        Set-CutoverMutationResult -State $State -Path $Path -Stage $Stage -Status "result" -Details $resultDetails
        return $value
    } catch {
        Set-CutoverMutationResult -State $State -Path $Path -Stage $Stage -Status "failed" `
            -Details @{ error = $_.Exception.Message }
        throw
    }
}

function Ensure-CutoverRecoveryState {
    param(
        [Parameter(Mandatory = $true)][object]$State,
        [Parameter(Mandatory = $true)][string]$Path
    )

    if ($null -eq $State.PSObject.Properties["schema_version"]) {
        $State | Add-Member -NotePropertyName schema_version -NotePropertyValue 2
    }
    if ($null -eq $State.PSObject.Properties["phase"]) {
        $State | Add-Member -NotePropertyName phase -NotePropertyValue "legacy"
    }
    if ($null -eq $State.PSObject.Properties["mutations"]) {
        $State | Add-Member mutations ([pscustomobject]@{
            shadow_stop = New-CutoverMutationState
            doris_submit = New-CutoverMutationState
            iceberg_submit = New-CutoverMutationState
            finalization = New-CutoverMutationState
        })
    }
    if (-not [string]::IsNullOrWhiteSpace([string]$State.savepoint_path) -and
        $null -eq $State.mutations.shadow_stop.intent) {
        $State.mutations.shadow_stop.status = "result"
        $State.mutations.shadow_stop.intent = [pscustomobject]@{
            operation = "legacy_shadow_stop_with_savepoint"
            details = [pscustomobject]@{ name = "chapter-9-datastream-quality-shadow" }
            created_at_utc = [string]$State.created_at
        }
        $State.mutations.shadow_stop.result = [pscustomobject]@{
            status = "result"
            savepoint_path = [string]$State.savepoint_path
            completed_at_utc = [string]$State.created_at
        }
    }
    foreach ($mapping in @(
        @{ Stage = "doris_submit"; Field = "doris_job_id"; Name = "chapter-9-doris-clean" },
        @{ Stage = "iceberg_submit"; Field = "iceberg_job_id"; Name = "chapter-9-iceberg-clean" }
    )) {
        $mutation = $State.mutations.($mapping.Stage)
        if ([string]::IsNullOrWhiteSpace([string]$mutation.result.job_id) -and
            -not [string]::IsNullOrWhiteSpace([string]$State.($mapping.Field))) {
            $mutation.status = "result"
            $mutation.intent = [pscustomobject]@{
                operation = "legacy_submission"
                details = [pscustomobject]@{ name = $mapping.Name }
                created_at_utc = [string]$State.created_at
            }
            $mutation.result = [pscustomobject]@{
                status = "result"
                job_id = [string]$State.($mapping.Field)
                completed_at_utc = [string]$State.created_at
            }
        }
    }
    Write-CutoverStateAtomic -State $State -Path $Path
    return $State
}

function Sync-CutoverResolvedJobId {
    param(
        [Parameter(Mandatory = $true)][object]$State,
        [Parameter(Mandatory = $true)][string]$Stage,
        [Parameter(Mandatory = $true)][string]$JobId,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $field = switch ($Stage) {
        "doris_submit" { "doris_job_id"; break }
        "iceberg_submit" { "iceberg_job_id"; break }
        default { throw "Unknown cutover submission stage: $Stage" }
    }
    if ($JobId -notmatch '^[0-9a-f]{32}$') {
        throw "Resolved cutover Job ID is invalid for ${Stage}: $JobId"
    }
    $current = [string]$State.$field
    if ($current -and $current -ne $JobId) {
        throw "Partial manifest $field conflicts with resolved Job ID $JobId."
    }
    if ($State -is [System.Collections.IDictionary]) {
        $State[$field] = $JobId
    } elseif ($State.PSObject.Properties.Name -contains $field) {
        $State.$field = $JobId
    } else {
        $State | Add-Member -NotePropertyName $field -NotePropertyValue $JobId
    }
    Write-CutoverStateAtomic -State $State -Path $Path
    return $JobId
}

function Resolve-CutoverJobReference {
    param(
        [Parameter(Mandatory = $true)][object]$State,
        [Parameter(Mandatory = $true)][string]$Stage,
        [Parameter(Mandatory = $true)][string]$ExpectedName,
        [Parameter(Mandatory = $true)][object]$Jobs,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $mutation = $State.mutations.$Stage
    if ($null -eq $mutation) { throw "Unknown cutover submission stage: $Stage" }
    if ($null -eq $mutation.intent) {
        throw "No submission intent exists for $ExpectedName; adoption is forbidden."
    }
    $jobId = [string]$mutation.result.job_id
    if ($jobId) {
        $byId = @($Jobs.jobs | Where-Object { [string]$_.jid -eq $jobId })
        if ($byId.Count -ne 1 -or [string]$byId[0].name -ne $ExpectedName -or
            [string]$byId[0].state -ne "RUNNING") {
            throw "Persisted cutover job is not the exact RUNNING $ExpectedName."
        }
        return Sync-CutoverResolvedJobId -State $State -Stage $Stage `
            -JobId $jobId -Path $Path
    }
    $namedJobs = @($Jobs.jobs | Where-Object {
        [string]$_.name -eq $ExpectedName -and [string]$_.state -eq "RUNNING"
    })
    if ($namedJobs.Count -ne 1) {
        throw "Cannot adopt ${ExpectedName}: expected one exact-name job, found $($namedJobs.Count)."
    }
    if ([string]$namedJobs[0].state -ne "RUNNING" -or
        [string]$namedJobs[0].jid -notmatch "^[0-9a-f]{32}$") {
        throw "Cannot adopt ${ExpectedName}: exact-name job is not RUNNING."
    }
    $jobId = [string]$namedJobs[0].jid
    [void]($mutation.status = "result")
    [void]($mutation.result = [pscustomobject]@{
        status = "result"
        job_id = $jobId
        adopted = $true
        completed_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
    })
    return Sync-CutoverResolvedJobId -State $State -Stage $Stage `
        -JobId $jobId -Path $Path
}

function ConvertFrom-CutoverLegacyProductionSubmitState {
    param(
        [Parameter(Mandatory = $true)][object]$Mutation,
        [Parameter(Mandatory = $true)][object]$Manifest,
        [Parameter(Mandatory = $true)][string]$ExpectedName
    )

    $safeError = "Production submission recovery state is unsafe."
    try {
        if (-not (Test-CutoverExactProperties -Record $Mutation -Names @("status", "intent", "result")) -or
            $Mutation.status -isnot [string]) {
            throw $safeError
        }
        $status = [string]$Mutation.status
        $projectedId = $Manifest.production_job_id
        if ($status -ceq "not_started") {
            if ($null -ne $Mutation.intent -or $null -ne $Mutation.result -or $null -ne $projectedId) {
                throw $safeError
            }
            return [pscustomobject][ordered]@{
                schema_version = 1
                kind = "chapter9_production_submit"
                status = "not_started"
                intent = $null
                result = $null
            }
        }
        if ($status -ceq "failed") { throw $safeError }
        if ($status -cnotin @("intent", "result") -or
            -not (Test-CutoverExactProperties -Record $Mutation.intent -Names @(
                    "operation", "details", "created_at_utc"
                )) -or
            $Mutation.intent.operation -isnot [string] -or
            [string]$Mutation.intent.operation -cne "submit_production_from_savepoint" -or
            -not (Test-CutoverExactProperties -Record $Mutation.intent.details -Names @(
                    "name", "savepoint_path"
                )) -or
            $Mutation.intent.details.name -isnot [string] -or
            [string]$Mutation.intent.details.name -cne $ExpectedName -or
            $Mutation.intent.details.savepoint_path -isnot [string] -or
            -not (Test-CutoverRoundTripTimestamp -Value $Mutation.intent.created_at_utc)) {
            throw $safeError
        }
        $savepointPath = Assert-CutoverSavepointPath -Path ([string]$Mutation.intent.details.savepoint_path)
        if ($Manifest.savepoint_path -isnot [string] -or
            [string]$Manifest.savepoint_path -cne $savepointPath) {
            throw $safeError
        }
        $manifestCreated = [DateTimeOffset]::Parse(
            [string]$Manifest.created_at,
            [System.Globalization.CultureInfo]::InvariantCulture
        )
        $intentCreated = [DateTimeOffset]::Parse(
            [string]$Mutation.intent.created_at_utc,
            [System.Globalization.CultureInfo]::InvariantCulture
        )
        if ($intentCreated -lt $manifestCreated) { throw $safeError }

        if ($status -ceq "intent") {
            if ($null -ne $Mutation.result -or $null -ne $projectedId) { throw $safeError }
            $state = [pscustomobject][ordered]@{
                schema_version = 1
                kind = "chapter9_production_submit"
                status = "intent"
                intent = $Mutation.intent
                result = $null
            }
            Assert-CutoverProductionSubmitState -State $state -ExpectedName $ExpectedName
            return $state
        }

        if (-not (Test-CutoverExactProperties -Record $Mutation.result -Names @(
                    "status", "details", "completed_at_utc", "job_id"
                )) -or
            $Mutation.result.status -isnot [string] -or [string]$Mutation.result.status -cne "result" -or
            -not (Test-CutoverExactProperties -Record $Mutation.result.details -Names @("job_id")) -or
            $Mutation.result.job_id -isnot [string] -or [string]$Mutation.result.job_id -cnotmatch "^[0-9a-f]{32}$" -or
            $Mutation.result.details.job_id -isnot [string] -or
            [string]$Mutation.result.details.job_id -cne [string]$Mutation.result.job_id -or
            $projectedId -isnot [string] -or [string]$projectedId -cne [string]$Mutation.result.job_id -or
            -not (Test-CutoverRoundTripTimestamp -Value $Mutation.result.completed_at_utc)) {
            throw $safeError
        }
        $state = [pscustomobject][ordered]@{
            schema_version = 1
            kind = "chapter9_production_submit"
            status = "result"
            intent = $Mutation.intent
            result = $Mutation.result
        }
        Assert-CutoverProductionSubmitState -State $state -ExpectedName $ExpectedName
        return $state
    } catch {
        throw $safeError
    }
}

function Assert-CutoverProductionSubmitManifestEnvelope {
    param([Parameter(Mandatory = $true)][object]$Manifest)

    $safeError = "Production submission recovery state is unsafe."
    $validPhases = @(
        "prepared", "shadow_stop", "production_submit", "doris_submit", "iceberg_submit", "finalization"
    )
    try {
        if (-not (Test-CutoverExactProperties -Record $Manifest -Names @(
                    "schema_version", "cutover_id", "phase", "created_at", "raw_offsets",
                    "shadow_job_id", "savepoint_path", "production_job_id", "doris_job_id",
                    "iceberg_job_id", "mutations"
                )) -or
            $Manifest.schema_version -isnot [int] -or [int]$Manifest.schema_version -ne 2 -or
            $Manifest.cutover_id -isnot [string] -or [string]::IsNullOrWhiteSpace([string]$Manifest.cutover_id) -or
            $Manifest.phase -isnot [string] -or $validPhases -cnotcontains [string]$Manifest.phase -or
            -not (Test-CutoverRoundTripTimestamp -Value $Manifest.created_at) -or
            $Manifest.raw_offsets -isnot [System.Collections.IList] -or $Manifest.raw_offsets -is [string] -or
            @($Manifest.raw_offsets).Count -lt 1 -or
            $Manifest.shadow_job_id -isnot [string] -or [string]$Manifest.shadow_job_id -cnotmatch "^[0-9a-f]{32}$" -or
            -not (Test-CutoverRecord -Value $Manifest.mutations)) {
            throw $safeError
        }
        foreach ($offset in @($Manifest.raw_offsets)) {
            if ($offset -isnot [string] -or [string]::IsNullOrWhiteSpace([string]$offset)) { throw $safeError }
        }
        if ($null -ne $Manifest.savepoint_path) {
            if ($Manifest.savepoint_path -isnot [string]) { throw $safeError }
            Assert-CutoverSavepointPath -Path ([string]$Manifest.savepoint_path) | Out-Null
        }
        foreach ($jobId in @($Manifest.production_job_id, $Manifest.doris_job_id, $Manifest.iceberg_job_id)) {
            if ($null -ne $jobId -and ($jobId -isnot [string] -or [string]$jobId -cnotmatch "^[0-9a-f]{32}$")) {
                throw $safeError
            }
        }
    } catch {
        throw $safeError
    }
}

function Remove-CutoverLegacyProductionSubmitProperty {
    param([Parameter(Mandatory = $true)][object]$Manifest)

    if ($Manifest.mutations -is [System.Collections.IDictionary]) {
        [void]$Manifest.mutations.Remove("production_submit")
    } else {
        $Manifest.mutations.PSObject.Properties.Remove("production_submit")
    }
}

function Move-CutoverLegacyProductionSubmitState {
    param(
        [Parameter(Mandatory = $true)][string]$ManifestPath,
        [Parameter(Mandatory = $true)][string]$ProductionStatePath,
        [AllowNull()][object]$CanonicalState,
        [AllowNull()][object]$LegacyStateProjection,
        [Parameter(Mandatory = $true)][string]$ExpectedName
    )

    $safeError = "Production submission recovery state is unsafe."
    try {
        if (-not (Test-Path -LiteralPath $ManifestPath)) { return $CanonicalState }
        if (-not (Test-Path -LiteralPath $ManifestPath -PathType Leaf)) { throw $safeError }
        $manifest = Get-Content -LiteralPath $ManifestPath -Raw -Encoding UTF8 | ConvertFrom-Json -ErrorAction Stop
        Assert-CutoverProductionSubmitManifestEnvelope -Manifest $manifest

        $currentNames = @("shadow_stop", "doris_submit", "iceberg_submit", "finalization")
        $legacyNames = @("shadow_stop", "production_submit", "doris_submit", "iceberg_submit", "finalization")
        if (Test-CutoverExactProperties -Record $manifest.mutations -Names $currentNames) {
            return $CanonicalState
        }
        if (-not (Test-CutoverExactProperties -Record $manifest.mutations -Names $legacyNames)) {
            throw $safeError
        }
        if (@("production_submit", "doris_submit", "iceberg_submit", "finalization") `
                -cnotcontains [string]$manifest.phase) {
            throw $safeError
        }

        $mapped = ConvertFrom-CutoverLegacyProductionSubmitState `
            -Mutation $manifest.mutations.production_submit -Manifest $manifest -ExpectedName $ExpectedName
        if ($null -ne $CanonicalState) {
            if (-not (Test-CutoverProductionSubmitStatesEquivalent `
                    -Left $CanonicalState -Right $mapped -ExpectedName $ExpectedName)) {
                throw $safeError
            }
        } else {
            Write-CutoverProductionSubmitStateAtomic -State $mapped -Path $ProductionStatePath
            $CanonicalState = $mapped
        }

        Remove-CutoverLegacyProductionSubmitProperty -Manifest $manifest
        Write-CutoverStateAtomic -State $manifest -Path $ManifestPath
        if ($null -ne $LegacyStateProjection) {
            Remove-CutoverLegacyProductionSubmitProperty -Manifest $LegacyStateProjection
        }
        return $CanonicalState
    } catch {
        throw $safeError
    }
}

function Invoke-CutoverResumePartialStateBoundary {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [string]$StateRoot,
        [Parameter(Mandatory = $true)][string]$PartialPath,
        [Parameter(Mandatory = $true)][string]$ExpectedName
    )

    $safeError = "Production submission recovery state is unsafe."
    $lock = $null
    try {
        $statePathArguments = @{ RepositoryRoot = $RepositoryRoot }
        if ($PSBoundParameters.ContainsKey("StateRoot")) {
            $statePathArguments["StateRoot"] = $StateRoot
        }
        $statePath = Get-CutoverProductionSubmitStatePath @statePathArguments
        $legacyPath = Get-CutoverLegacyProductionSubmitStatePath `
            -RepositoryRoot $RepositoryRoot -Path $PartialPath
        New-Item -ItemType Directory -Force -Path (Split-Path -Parent $statePath) | Out-Null
        $lock = [System.IO.File]::Open(
            "$statePath.lock",
            [System.IO.FileMode]::OpenOrCreate,
            [System.IO.FileAccess]::ReadWrite,
            [System.IO.FileShare]::None
        )

        $state = if (Test-Path -LiteralPath $statePath -PathType Leaf) {
            Get-Content -LiteralPath $statePath -Raw -Encoding UTF8 | ConvertFrom-Json -ErrorAction Stop
        } elseif (Test-Path -LiteralPath $statePath) {
            throw $safeError
        } else {
            $null
        }
        if ($null -ne $state) {
            Assert-CutoverProductionSubmitState -State $state -ExpectedName $ExpectedName
        }
        $state = Move-CutoverLegacyProductionSubmitState -ManifestPath $legacyPath `
            -ProductionStatePath $statePath -CanonicalState $state `
            -LegacyStateProjection $null -ExpectedName $ExpectedName
        if ($null -ne $state -and [string]$state.status -cin @("intent", "failed")) {
            throw $safeError
        }

        if (-not (Test-Path -LiteralPath $legacyPath -PathType Leaf)) { throw $safeError }
        $manifest = Get-Content -LiteralPath $legacyPath -Raw -Encoding UTF8 |
            ConvertFrom-Json -ErrorAction Stop
        Assert-CutoverProductionSubmitManifestEnvelope -Manifest $manifest
        if (-not (Test-CutoverExactProperties -Record $manifest.mutations -Names @(
                    "shadow_stop", "doris_submit", "iceberg_submit", "finalization"
                ))) {
            throw $safeError
        }
        $recoveryState = Ensure-CutoverRecoveryState -State $manifest -Path $legacyPath
        if ($null -ne $recoveryState.mutations.PSObject.Properties["production_submit"]) {
            throw $safeError
        }
        return $recoveryState
    } catch {
        throw $safeError
    } finally {
        if ($null -ne $lock) { $lock.Dispose() }
    }
}

function Invoke-CutoverResumePartialControlFlow {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [string]$StateRoot,
        [Parameter(Mandatory = $true)][string]$PartialPath,
        [Parameter(Mandatory = $true)][string]$ExpectedName,
        [Parameter(Mandatory = $true)][scriptblock]$Action
    )

    $boundaryArguments = @{
        RepositoryRoot = $RepositoryRoot
        PartialPath = $PartialPath
        ExpectedName = $ExpectedName
    }
    if ($PSBoundParameters.ContainsKey("StateRoot")) {
        $boundaryArguments["StateRoot"] = $StateRoot
    }
    $recoveryState = Invoke-CutoverResumePartialStateBoundary @boundaryArguments
    $savedManifest = $recoveryState
    return & $Action $savedManifest $recoveryState
}

function Invoke-CutoverProductionSubmitBoundary {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][object]$Jobs,
        [Parameter(Mandatory = $true)][string]$ExpectedName,
        [string]$LegacyStatePath,
        [AllowNull()][object]$LegacyStateProjection,
        [Parameter(Mandatory = $true)][scriptblock]$Action
    )

    $safeError = "Production submission recovery state is unsafe."
    $lock = $null
    try {
        $directory = Split-Path -Parent $Path
        New-Item -ItemType Directory -Force -Path $directory | Out-Null
        $lock = [System.IO.File]::Open(
            "$Path.lock",
            [System.IO.FileMode]::OpenOrCreate,
            [System.IO.FileAccess]::ReadWrite,
            [System.IO.FileShare]::None
        )
        $state = if (Test-Path -LiteralPath $Path -PathType Leaf) {
            Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json -ErrorAction Stop
        } elseif (Test-Path -LiteralPath $Path) {
            throw $safeError
        } else {
            $null
        }
        if ($null -ne $state) {
            Assert-CutoverProductionSubmitState -State $state -ExpectedName $ExpectedName
        }
        if ($PSBoundParameters.ContainsKey("LegacyStatePath")) {
            $state = Move-CutoverLegacyProductionSubmitState -ManifestPath $LegacyStatePath `
                -ProductionStatePath $Path -CanonicalState $state `
                -LegacyStateProjection $LegacyStateProjection -ExpectedName $ExpectedName
        }
        if ($null -eq $state) { $state = New-CutoverProductionSubmitState -Path $Path }
        Assert-CutoverProductionSubmitState -State $state -ExpectedName $ExpectedName
        $status = [string]$state.status
        if ($status -ceq "intent" -or $status -ceq "failed") { throw $safeError }

        if ($Jobs -isnot [System.Management.Automation.PSCustomObject]) { throw $safeError }
        $jobsProperty = @($Jobs.PSObject.Properties | Where-Object { $_.Name -ceq "jobs" })
        if ($jobsProperty.Count -ne 1 -or $jobsProperty[0].Value -isnot [System.Collections.IList] -or
            $jobsProperty[0].Value -is [string]) {
            throw $safeError
        }
        $validStates = @("CREATED", "RUNNING", "FAILING", "FAILED", "CANCELLING", "CANCELED", "FINISHED", "RESTARTING", "SUSPENDED", "RECONCILING", "INITIALIZING")
        $seenIds = @{}
        $namedJobs = @()
        foreach ($job in @($jobsProperty[0].Value)) {
            if ($job -isnot [System.Management.Automation.PSCustomObject] -or
                $job.jid -isnot [string] -or [string]$job.jid -cnotmatch "^[0-9a-f]{32}$" -or
                $job.name -isnot [string] -or [string]::IsNullOrWhiteSpace([string]$job.name) -or
                $job.state -isnot [string] -or $validStates -cnotcontains [string]$job.state -or
                $seenIds.ContainsKey([string]$job.jid)) {
                throw $safeError
            }
            $seenIds[[string]$job.jid] = $true
            if ([string]$job.name -ceq $ExpectedName) { $namedJobs += $job }
        }

        if ($status -ceq "result") {
            $jobId = [string]$state.result.job_id
            $byId = @($jobsProperty[0].Value | Where-Object { [string]$_.jid -ceq $jobId })
            if ($namedJobs.Count -ne 1 -or $byId.Count -ne 1 -or
                [string]$byId[0].name -cne $ExpectedName -or [string]$byId[0].state -cne "RUNNING" -or
                [string]$namedJobs[0].jid -cne $jobId) {
                throw $safeError
            }
            return [pscustomobject]@{ action = "no_op"; job_id = $jobId }
        }
        if ($status -cne "not_started" -or $namedJobs.Count -ne 0) { throw $safeError }

        $state.status = "intent"
        $state.intent = [pscustomobject]@{
            operation = "submit_chapter9_production"
            job_name = $ExpectedName
            created_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
        }
        $state.result = $null
        Write-CutoverProductionSubmitStateAtomic -State $state -Path $Path

        try { $candidate = & $Action } catch { throw $safeError }
        if (-not (Test-CutoverExactProperties -Record $candidate -Names @("job_id")) -or
            $candidate.job_id -isnot [string] -or [string]$candidate.job_id -cnotmatch "^[0-9a-f]{32}$") {
            throw $safeError
        }
        $jobId = [string]$candidate.job_id
        $state.status = "result"
        $state.result = [pscustomobject]@{
            status = "result"
            job_id = $jobId
            job_name = $ExpectedName
            completed_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
        }
        Write-CutoverProductionSubmitStateAtomic -State $state -Path $Path
        return [pscustomobject]@{ action = "submitted"; job_id = $jobId }
    } catch {
        throw $safeError
    } finally {
        if ($null -ne $lock) { $lock.Dispose() }
    }
}

function Invoke-CutoverProductionSubmitStage {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [string]$StateRoot,
        [Parameter(Mandatory = $true)][object]$Jobs,
        [Parameter(Mandatory = $true)][string]$ExpectedName,
        [object]$CutoverState,
        [string]$CutoverStatePath,
        [Parameter(Mandatory = $true)][scriptblock]$Action
    )

    if ($null -ne $CutoverState -and [string]::IsNullOrWhiteSpace($CutoverStatePath)) {
        throw "Production submission recovery state is unsafe."
    }
    $statePathArguments = @{ RepositoryRoot = $RepositoryRoot }
    if ($PSBoundParameters.ContainsKey("StateRoot")) {
        $statePathArguments["StateRoot"] = $StateRoot
    }
    $statePath = Get-CutoverProductionSubmitStatePath @statePathArguments
    $legacyPathArguments = @{ RepositoryRoot = $RepositoryRoot }
    if (-not [string]::IsNullOrWhiteSpace($CutoverStatePath)) {
        $legacyPathArguments["Path"] = $CutoverStatePath
    }
    $legacyStatePath = Get-CutoverLegacyProductionSubmitStatePath @legacyPathArguments
    return Invoke-CutoverProductionSubmitBoundary -Path $statePath -Jobs $Jobs `
        -ExpectedName $ExpectedName -LegacyStatePath $legacyStatePath `
        -LegacyStateProjection $CutoverState -Action $Action
}

function Complete-CutoverManifest {
    param(
        [string]$PartialPath,
        [string]$FinalPath,
        [string]$ProductionJobId,
        [string]$DorisJobId,
        [string]$IcebergJobId,
        [object]$ProductionCheckpoints,
        [object]$DorisCheckpoints,
        [object]$IcebergCheckpoints
    )

    if (Test-Path -LiteralPath $FinalPath) { throw "Final cutover manifest already exists: $FinalPath" }
    $partial = Get-Content -Raw -Encoding UTF8 $PartialPath | ConvertFrom-Json
    if (-not [string]::IsNullOrWhiteSpace([string]$partial.iceberg_job_id) -and
        [string]$partial.iceberg_job_id -ne $IcebergJobId) {
        throw "Partial manifest Iceberg Job ID conflicts with finalization."
    }
    if ([string]$partial.production_job_id -ne $ProductionJobId -or
        [string]$partial.doris_job_id -ne $DorisJobId) {
        throw "Partial manifest Job IDs changed before finalization."
    }

    foreach ($job in @(
        @{ Id = $ProductionJobId; Name = "chapter-9-datastream-quality-production"; Checkpoints = $ProductionCheckpoints },
        @{ Id = $DorisJobId; Name = "chapter-9-doris-clean"; Checkpoints = $DorisCheckpoints },
        @{ Id = $IcebergJobId; Name = "chapter-9-iceberg-clean"; Checkpoints = $IcebergCheckpoints }
    )) {
        if ([int64]$job.Checkpoints.counts.completed -le 0 -or
            $job.Checkpoints.latest.completed.status -ne "COMPLETED") {
            throw "Job $($job.Id) has no successful checkpoint evidence for finalization."
        }
        Wait-FlinkJobRunning -JobId $job.Id -ExpectedName $job.Name | Out-Null
    }

    $final = [ordered]@{
        cutover_id = [string]$partial.cutover_id
        created_at = [string]$partial.created_at
        raw_offsets = @($partial.raw_offsets)
        shadow_job_id = [string]$partial.shadow_job_id
        savepoint_path = [string]$partial.savepoint_path
        production_job_id = $ProductionJobId
        doris_job_id = $DorisJobId
        iceberg_job_id = $IcebergJobId
    }
    $nextPath = "$FinalPath.next"
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($nextPath, (($final | ConvertTo-Json -Depth 5) + "`n"), $utf8NoBom)
    Move-Item -LiteralPath $nextPath -Destination $FinalPath
    Write-Host "[complete] manifest=$FinalPath"
    Write-Host "[complete] production_job_id=$ProductionJobId checkpoints=$($ProductionCheckpoints.counts.completed)"
    Write-Host "[complete] doris_job_id=$DorisJobId checkpoints=$($DorisCheckpoints.counts.completed)"
    Write-Host "[complete] iceberg_job_id=$IcebergJobId checkpoints=$($IcebergCheckpoints.counts.completed)"
}

function Get-FlinkJobs {
    return Invoke-FlinkRest -Resource "/jobs/overview"
}

function Assert-JobNamesAbsent([string[]]$Names) {
    $jobs = Get-FlinkJobs
    foreach ($name in $Names) {
        $matches = @($jobs.jobs | Where-Object { $_.name -eq $name -and $_.state -eq "RUNNING" })
        if ($matches.Count -gt 0) { throw "Flink job name already exists: $name." }
    }
}

function Assert-ResumeManifest([object]$Manifest, [object]$Jobs) {
    foreach ($field in @(
        "cutover_id", "shadow_job_id", "savepoint_path", "production_job_id", "doris_job_id"
    )) {
        if ([string]::IsNullOrWhiteSpace([string]$Manifest.$field)) {
            throw "Partial manifest field is missing: $field."
        }
    }
    if (@($Manifest.raw_offsets).Count -eq 0) { throw "Partial manifest raw_offsets is empty." }
    if (-not [string]::IsNullOrWhiteSpace([string]$Manifest.iceberg_job_id)) {
        throw "Partial manifest already contains an Iceberg Job ID."
    }

    foreach ($expected in @(
        @{ Id = [string]$Manifest.production_job_id; Name = "chapter-9-datastream-quality-production" },
        @{ Id = [string]$Manifest.doris_job_id; Name = "chapter-9-doris-clean" }
    )) {
        $byId = @($Jobs.jobs | Where-Object { $_.jid -eq $expected.Id })
        $byName = @($Jobs.jobs | Where-Object {
            $_.name -eq $expected.Name -and $_.state -eq "RUNNING"
        })
        if ($byId.Count -ne 1 -or $byName.Count -ne 1 -or
            $byId[0].name -ne $expected.Name -or $byId[0].state -ne "RUNNING" -or
            $byName[0].jid -ne $expected.Id) {
            throw "Partial manifest does not match the exact RUNNING job $($expected.Name)."
        }
    }
    $icebergJobs = @($Jobs.jobs | Where-Object {
        $_.name -eq "chapter-9-iceberg-clean" -and $_.state -eq "RUNNING"
    })
    if ($icebergJobs.Count -gt 1) { throw "Multiple Iceberg jobs exist before partial recovery." }
    if ($icebergJobs.Count -eq 1) {
        if ($icebergJobs[0].state -ne "RUNNING" -or
            [string]$icebergJobs[0].jid -notmatch "^[0-9a-f]{32}$") {
            throw "Existing Iceberg job is not an exact RUNNING job."
        }
        return [string]$icebergJobs[0].jid
    }
    return $null
}

function Get-OnlyRunningShadowJob([string]$Name) {
    $jobs = Get-FlinkJobs
    $running = @($jobs.jobs | Where-Object { $_.name -eq $Name -and $_.state -eq "RUNNING" })
    if ($running.Count -ne 1) {
        throw "Expected exactly one RUNNING shadow job named $Name."
    }
    return $running[0]
}

function Assert-RecentCompletedCheckpoint([string]$JobId, [int]$MaximumAgeSeconds = 120) {
    $checkpoints = Invoke-FlinkRest -Resource "/jobs/$JobId/checkpoints"
    $latest = $checkpoints.latest.completed
    if ($null -eq $latest -or $latest.status -ne "COMPLETED") {
        throw "Shadow job $JobId has no latest completed checkpoint."
    }
    $ageMilliseconds = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() - [int64]$latest.latest_ack_timestamp
    if ($ageMilliseconds -lt 0 -or $ageMilliseconds -gt ($MaximumAgeSeconds * 1000)) {
        throw "Shadow job $JobId latest checkpoint is not recent. Age milliseconds: $ageMilliseconds."
    }
    return $checkpoints
}

function Wait-ShadowLagZero {
    param(
        [string[]]$ExpectedOffsets,
        [string]$KafkaContainer,
        [int]$Attempts = 60,
        [int]$SleepSeconds = 2
    )

    $expected = @{}
    foreach ($offset in $ExpectedOffsets) {
        if ($offset -notmatch "^partition:(\d+),offset:(\d+)$") {
            throw "Invalid expected Kafka SQL offset: $offset"
        }
        $partition = [int]$Matches[1]
        if ($expected.ContainsKey($partition)) { throw "Duplicate expected Kafka partition: $partition." }
        $expected[$partition] = [int64]$Matches[2]
    }

    $lastLag = $null
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $output = Invoke-DockerCommand -Arguments @(
            "exec", $KafkaContainer, "kafka-consumer-groups",
            "--bootstrap-server", "kafka:29092", "--describe",
            "--group", "chapter9-quality-shadow"
        ) -FailureMessage "Failed to describe shadow Kafka consumer group."
        $description = ConvertFrom-KafkaGroupDescription -Lines $output `
            -ExpectedGroup "chapter9-quality-shadow" -ExpectedTopic "user_behavior_events"
        $lastLag = $description.TotalLag
        $expectedPartitions = @($expected.Keys | Sort-Object)
        $actualPartitions = @($description.Rows.Partition | Sort-Object)
        if (($actualPartitions -join ",") -ne ($expectedPartitions -join ",")) {
            throw "Shadow group partition set does not match raw offsets."
        }
        if ($description.TotalLag -eq 0) {
            foreach ($row in $description.Rows) {
                if (-not $expected.ContainsKey($row.Partition) -or
                    $row.CurrentOffset -ne $expected[$row.Partition] -or
                    $row.LogEndOffset -ne $expected[$row.Partition]) {
                    throw "Shadow group offsets do not match the paused raw topic at partition $($row.Partition)."
                }
            }
            return $description
        }
        if ($attempt -lt $Attempts -and $SleepSeconds -gt 0) { Start-Sleep -Seconds $SleepSeconds }
    }
    throw "Shadow Kafka consumer group lag did not reach zero. Last total lag: $lastLag."
}

function Wait-FlinkJobRunning {
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
            $job = Invoke-FlinkRest -Resource "/jobs/$JobId"
            if ($job.name -ne $ExpectedName) {
                throw "Job ID $JobId has unexpected name $($job.name)."
            }
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

function Wait-NewCompletedCheckpoint {
    param(
        [string]$JobId,
        [string]$ExpectedName,
        [int64]$Baseline = 0,
        [int]$Attempts = 60,
        [int]$SleepSeconds = 2
    )

    $lastError = $null
    $lastState = "NOT_FOUND"
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        try {
            $job = Invoke-FlinkRest -Resource "/jobs/$JobId"
            if ($job.name -ne $ExpectedName) {
                throw "Job ID $JobId has unexpected name $($job.name)."
            }
            if ($job.jid -and [string]$job.jid -ne $JobId) {
                throw "Flink REST returned unexpected Job ID $($job.jid); expected $JobId."
            }
            $lastState = [string]$job.state
            if ($lastState -in @("FAILED", "CANCELED", "FINISHED", "SUSPENDED")) {
                throw "Job $JobId entered terminal state $lastState while waiting for a checkpoint."
            }
            if ($lastState -eq "RUNNING") {
                $checkpoints = Invoke-FlinkRest -Resource "/jobs/$JobId/checkpoints"
                if ([int64]$checkpoints.counts.completed -gt $Baseline -and
                    $checkpoints.latest.completed.status -eq "COMPLETED") {
                    return $checkpoints
                }
            }
        } catch {
            $lastError = $_.Exception.Message
            if ($lastError -match "unexpected name|unexpected Job ID|terminal state") { throw }
        }
        if ($attempt -lt $Attempts -and $SleepSeconds -gt 0) { Start-Sleep -Seconds $SleepSeconds }
    }
    throw "No completed checkpoint appeared for job $JobId. Last state: $lastState. Last error: $lastError"
}

function Wait-NewNamedJob {
    param(
        [string]$Name,
        [int]$Attempts = 60,
        [int]$SleepSeconds = 2
    )

    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $jobs = Get-FlinkJobs
        $running = @($jobs.jobs | Where-Object {
            [string]$_.name -eq $Name -and [string]$_.state -eq "RUNNING"
        })
        if ($running.Count -gt 1) {
            throw "Multiple RUNNING Flink jobs have the exact name $Name."
        }
        if ($running.Count -eq 1) {
            $candidate = $running[0]
            $jobId = [string]$candidate.jid
            $byId = @($jobs.jobs | Where-Object { [string]$_.jid -eq $jobId })
            if ($jobId -notmatch '^[0-9a-f]{32}$' -or $byId.Count -ne 1 -or
                [string]$candidate.name -ne $Name -or [string]$candidate.state -ne "RUNNING") {
                throw "Unique RUNNING Flink job $Name has inconsistent ID/name/state evidence."
            }
            return $jobId
        }
        if ($attempt -lt $Attempts -and $SleepSeconds -gt 0) { Start-Sleep -Seconds $SleepSeconds }
    }
    throw "Flink job with exact name $Name did not appear."
}

function Write-SqlFile([string]$Path, [string[]]$Parts) {
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, (($Parts -join "`r`n`r`n").Trim() + "`r`n"), $utf8NoBom)
}

function Submit-SqlJob {
    param(
        [string]$SqlClient,
        [string]$ContainerSqlPath,
        [object[]]$Connectors,
        [string]$ParentClasspath = ""
    )

    $sqlClientArguments = @("exec")
    if ($ParentClasspath) {
        $sqlClientArguments += @("-e", "HADOOP_CLASSPATH=$ParentClasspath")
    }
    $sqlClientArguments += @($SqlClient, "/opt/flink/bin/sql-client.sh")
    if ($ParentClasspath) {
        $sqlClientArguments += @("-D", "classloader.resolve-order=parent-first")
    }
    foreach ($connector in $Connectors) {
        $containerPath = [string]$connector.ContainerPath
        $sqlClientArguments += @("-j", $containerPath)
    }
    $sqlClientArguments += @("-f", $ContainerSqlPath)
    $output = Invoke-DockerCommand -Arguments $sqlClientArguments `
        -FailureMessage "Flink SQL Client submission failed for $ContainerSqlPath."
    $outputText = $output -join "`n"
    if ($outputText -match "\[ERROR\]") {
        throw "Flink SQL Client reported a statement error for $ContainerSqlPath. Output: $outputText"
    }
    return $output
}

if ($FunctionsOnly) { return }

if (-not $TrafficPaused) {
    throw "Cutover requires explicit -TrafficPaused confirmation."
}

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$tmpRoot = Join-Path $root "tmp/chapter-9"
$connectorDirectory = Join-Path $tmpRoot "lib"
$manifestPath = Join-Path $tmpRoot "cutover-manifest.json"
$manifestPartialPath = Join-Path $tmpRoot "cutover-manifest.json.partial"
$checkpointUri = Get-Chapter9StateUri -Kind "checkpoint"
$savepointUri = Get-Chapter9StateUri -Kind "savepoint"
$fatJar = Join-Path $root "jobs/datastream-quality/target/datastream-quality-1.0.0.jar"
$mainRoot = $root
$worktreesDirectory = Split-Path $root -Parent
if ((Split-Path $worktreesDirectory -Leaf) -eq ".worktrees") {
    $mainRoot = Split-Path $worktreesDirectory -Parent
}
$connectorCache = Join-Path $mainRoot "infra/compose/flink/lib"

$kafka = "ecom-kafka"
$jobManager = "ecom-flink-jobmanager"
$taskManager = "ecom-flink-taskmanager"
$sqlClient = "ecom-flink-sql-client"
$shadowJobName = "chapter-9-datastream-quality-shadow"
$productionJobName = "chapter-9-datastream-quality-production"
$dorisJobName = "chapter-9-doris-clean"
$icebergJobName = "chapter-9-iceberg-clean"

$connectors = @(
    @{ Name = "flink-sql-connector-kafka-3.3.0-1.19.jar"; Hash = "F46F69333445C598EBA9E5068B0A58DD2B4BA797738FD0FD3EE4E862FE281691"; Url = "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar"; ContainerPath = "/workspace/tmp/chapter-9/lib/flink-sql-connector-kafka-3.3.0-1.19.jar" },
    @{ Name = "flink-doris-connector-1.19-25.1.0.jar"; Hash = "CE1C35B6A16B24F67E61EE95B7DAB9802B1FB654B9DA4FE171C174B2F8B1CA36"; Url = "https://repo1.maven.org/maven2/org/apache/doris/flink-doris-connector-1.19/25.1.0/flink-doris-connector-1.19-25.1.0.jar"; ContainerPath = "/workspace/tmp/chapter-9/lib/flink-doris-connector-1.19-25.1.0.jar" },
    @{ Name = "flink-sql-connector-hive-3.1.3_2.12-1.19.2.jar"; Hash = "B7C401F01BF69DD72B052F4B0C548829ABB3528DFAA1DDFF68CD07EB4C552FEF"; Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/1.19.2/flink-sql-connector-hive-3.1.3_2.12-1.19.2.jar"; ContainerPath = "/workspace/tmp/chapter-9/lib/flink-sql-connector-hive-3.1.3_2.12-1.19.2.jar" },
    @{ Name = "iceberg-flink-runtime-1.19-1.6.1.jar"; Hash = "D0B3FC51623E7091B4D5DB96178D8ED79102E51A93F649E3CE82EE4471C080AB"; Url = "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/1.6.1/iceberg-flink-runtime-1.19-1.6.1.jar"; ContainerPath = "/workspace/tmp/chapter-9/lib/iceberg-flink-runtime-1.19-1.6.1.jar" },
    @{ Name = "iceberg-aws-bundle-1.6.1.jar"; Hash = "D14A49CED66A20CBD30F73EBB379646248D784FC5CD49D7295D36524380330E3"; Url = "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar"; ContainerPath = "/workspace/tmp/chapter-9/lib/iceberg-aws-bundle-1.6.1.jar" },
    @{ Name = "hadoop-client-api-3.3.6.jar"; Hash = "F3D2347A6E1C6885D5BCFD4F60C3AC3810EC11068FC161E04329BAABF412D963"; Url = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar"; ContainerPath = "/workspace/tmp/chapter-9/lib/hadoop-client-api-3.3.6.jar" },
    @{ Name = "hadoop-client-runtime-3.3.6.jar"; Hash = "15F01BC804294DF06D2EFFC87DE363A83CF589F50558BDBF48F72541AD8DE854"; Url = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar"; ContainerPath = "/workspace/tmp/chapter-9/lib/hadoop-client-runtime-3.3.6.jar" },
    @{ Name = "hadoop-aws-3.3.6.jar"; Hash = "FBA9EB73E6F0F5458355627FE095F5124705D4048551F4D6AA4084777B824C13"; Url = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar"; ContainerPath = "/workspace/tmp/chapter-9/lib/hadoop-aws-3.3.6.jar" },
    @{ Name = "aws-java-sdk-bundle-1.12.262.jar"; Hash = "873FE7CF495126619997BEC21C44DE5D992544AEA7E632FDC77ADB1A0915BAE5"; Url = "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"; ContainerPath = "/workspace/tmp/chapter-9/lib/aws-java-sdk-bundle-1.12.262.jar" }
)

Write-Host "[preflight] validating Docker services and workspace mounts"
Invoke-DockerCommand -Arguments @("version") -FailureMessage "Docker daemon is not available." | Out-Null
foreach ($container in @(
    $kafka, $jobManager, $taskManager, $sqlClient,
    "ecom-doris-fe", "ecom-doris-be", "ecom-minio", "ecom-hive-metastore"
)) {
    Assert-ContainerRunning $container
}
foreach ($container in @($jobManager, $taskManager, $sqlClient)) {
    $mount = [System.IO.Path]::GetFullPath((Get-WorkspaceMountSource $container)).TrimEnd("\", "/")
    if ($mount -ne $root.TrimEnd("\", "/")) {
        throw "Workspace mount mismatch for $container. Current root: $root. Container /workspace: $mount."
    }
}
$expectedMinioData = Join-Path $root "infra/compose/minio/data"
Assert-ContainerBindMountSource -Container "ecom-minio" -Destination "/data" `
    -ExpectedSource $expectedMinioData
$minioReady = Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:9000/minio/health/ready" -TimeoutSec 5
if ($minioReady.StatusCode -ne 200) { throw "MinIO readiness endpoint did not return HTTP 200." }
Invoke-DockerCommand -Arguments @(
    "exec", "ecom-minio", "sh", "-lc", "test -d /data && test -r /data && test -w /data"
) -FailureMessage "MinIO /data is not a readable and writable directory." | Out-Null
foreach ($endpoint in @(
    "/dev/tcp/hive-metastore/9083",
    "/dev/tcp/doris-fe/8030",
    "/dev/tcp/minio/9000"
)) {
    Invoke-DockerCommand -Arguments @("exec", $sqlClient, "bash", "-lc", "echo > $endpoint") `
        -FailureMessage "SQL Client cannot reach required endpoint $endpoint." | Out-Null
}
$dorisTable = Invoke-DockerCommand -Arguments @(
    "exec", "ecom-doris-fe", "mysql", "-hdoris-fe", "-P9030", "-uroot", "-N",
    "-e", "SHOW TABLES FROM analytics LIKE 'realtime_metrics';"
) -FailureMessage "Doris realtime_metrics preflight failed."
if (($dorisTable -join "`n").Trim() -ne "realtime_metrics") {
    throw "Doris table analytics.realtime_metrics is missing."
}

$overview = Invoke-FlinkRest -Resource "/overview"
Assert-FlinkCapacity $overview

New-Item -ItemType Directory -Force -Path $tmpRoot, $connectorDirectory | Out-Null
if (Test-Path -LiteralPath $manifestPath) {
    if (-not $ResumePartial) {
        throw "Final cutover manifest already exists: $manifestPath"
    }
}
if ($ResumePartial -and (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
    if (-not (Test-Path -LiteralPath $manifestPartialPath -PathType Leaf)) {
        throw "Final cutover manifest exists but its partial recovery state is missing."
    }
    $finalManifest = Get-Content -Raw -Encoding UTF8 $manifestPath | ConvertFrom-Json
    if ([string]::IsNullOrWhiteSpace([string]$finalManifest.iceberg_job_id)) {
        throw "Final cutover manifest is incomplete and cannot be safely resumed."
    }
    Write-Host "[resume] final cutover manifest already durable; finalization is complete."
    return
}
if ($ResumePartial -and -not (Test-Path -LiteralPath $manifestPartialPath -PathType Leaf)) {
    throw "Partial cutover manifest is missing: $manifestPartialPath"
}
if (-not $ResumePartial -and (Test-Path -LiteralPath $manifestPartialPath)) {
    throw "Partial cutover manifest already exists: $manifestPartialPath"
}

Write-Host "[preflight] preparing pinned SQL connectors"
foreach ($connector in $connectors) {
    $evidence = Install-Connector -Connector $connector -DestinationDirectory $connectorDirectory -CacheDirectory $connectorCache
    Write-Host "[connector] name=$($evidence.Name) source=$($evidence.Source) sha256=$($evidence.Hash)"
}

$checkpointSql = Get-Content -Raw -Encoding UTF8 (Join-Path $root "jobs/sql/00_enable_iceberg_checkpointing.sql")
$dorisSourceSql = Get-Content -Raw -Encoding UTF8 (Join-Path $root "jobs/sql/13_source_user_behavior_clean_doris.sql")
$dorisSinkSql = Get-Content -Raw -Encoding UTF8 (Join-Path $root "jobs/sql/04_sink_doris_metrics.sql")
$dorisInsertSql = Get-Content -Raw -Encoding UTF8 (Join-Path $root "jobs/sql/05_pv_uv_to_doris.sql")
$icebergSourceSql = Get-Content -Raw -Encoding UTF8 (Join-Path $root "jobs/sql/14_source_user_behavior_clean_iceberg.sql")
$icebergCatalogSql = Get-Content -Raw -Encoding UTF8 (Join-Path $root "jobs/sql/06_create_iceberg_catalog.sql")
$icebergInsertSql = Get-Content -Raw -Encoding UTF8 (Join-Path $root "jobs/sql/07_sink_user_behavior_to_iceberg.sql")
$dorisConnectors = @($connectors | Where-Object { $_.Name -in @(
    "flink-sql-connector-kafka-3.3.0-1.19.jar",
    "flink-doris-connector-1.19-25.1.0.jar"
) })
$icebergConnectors = @($connectors | Where-Object { $_.Name -notin @(
    "flink-doris-connector-1.19-25.1.0.jar"
) })
$icebergParentClasspath = ($icebergConnectors | ForEach-Object { $_.ContainerPath }) -join ":"

Write-SqlFile -Path (Join-Path $tmpRoot "doris-preflight.sql") -Parts @($dorisSourceSql, $dorisSinkSql)
Write-SqlFile -Path (Join-Path $tmpRoot "iceberg-preflight.sql") -Parts @(
    $icebergSourceSql, $icebergCatalogSql,
    "SET 'execution.runtime-mode' = 'batch';",
    "SELECT COUNT(*) FROM lakehouse.analytics.user_behavior_detail;"
)
Write-Host "[preflight] validating Doris and Iceberg SQL connectors"
if (-not $ResumePartial) {
    Submit-SqlJob -SqlClient $sqlClient -ContainerSqlPath "/workspace/tmp/chapter-9/doris-preflight.sql" `
        -Connectors $dorisConnectors | Out-Null
    Submit-SqlJob -SqlClient $sqlClient -ContainerSqlPath "/workspace/tmp/chapter-9/iceberg-preflight.sql" `
        -Connectors $icebergConnectors -ParentClasspath $icebergParentClasspath | Out-Null
}

Write-SqlFile -Path (Join-Path $tmpRoot "doris-clean.sql") -Parts @(
    "SET 'pipeline.name' = '$dorisJobName';", $checkpointSql,
    $dorisSourceSql, $dorisSinkSql, $dorisInsertSql
)
Write-SqlFile -Path (Join-Path $tmpRoot "iceberg-clean.sql") -Parts @(
    $checkpointSql, "SET 'pipeline.name' = '$icebergJobName';",
    $icebergSourceSql, $icebergCatalogSql, $icebergInsertSql
)

if ($ResumePartial) {
    Invoke-CutoverResumePartialControlFlow -RepositoryRoot $root -PartialPath $manifestPartialPath `
        -ExpectedName $productionJobName -Action {
        param($savedManifest, $recoveryState)
        $resumeJobs = Get-FlinkJobs
    $stopResult = Invoke-CutoverShadowStopStage -State $recoveryState -Path $manifestPartialPath `
        -Jobs $resumeJobs -StopAction {
            $output = Invoke-FlinkJobManagerCommand -Command @(
                "/opt/flink/bin/flink", "stop",
                "--savepointPath", $savepointUri,
                [string]$recoveryState.shadow_job_id
            ) -FailureMessage "Shadow Stop-with-Savepoint resume failed."
            $output | ForEach-Object { Write-Host $_ }
            [pscustomobject]@{ savepoint_path = (Get-SavepointPath $output) }
        }
    $savepointPath = [string]$stopResult.savepoint_path
    if ($savedManifest.production_job_id -and $savedManifest.doris_job_id) {
        Assert-ResumeManifest -Manifest $savedManifest -Jobs $resumeJobs | Out-Null
    }
    $productionJobId = $null
    $dorisJobId = $null
    $icebergJobId = $null
    if ($recoveryState.mutations.doris_submit.intent) {
        $dorisJobId = Resolve-CutoverJobReference -State $recoveryState -Stage "doris_submit" `
            -ExpectedName $dorisJobName -Jobs $resumeJobs -Path $manifestPartialPath
    }
    if ($recoveryState.mutations.iceberg_submit.intent) {
        $icebergJobId = Resolve-CutoverJobReference -State $recoveryState -Stage "iceberg_submit" `
            -ExpectedName $icebergJobName -Jobs $resumeJobs -Path $manifestPartialPath
    }

    $productionResult = Invoke-CutoverProductionSubmitStage -RepositoryRoot $root `
        -Jobs $resumeJobs -ExpectedName $productionJobName `
        -CutoverState $recoveryState -CutoverStatePath $manifestPartialPath -Action {
            $output = Invoke-FlinkJobManagerCommand -Command @(
                "/opt/flink/bin/flink", "run", "-d", "-s", $savepointPath,
                "-c", "com.ecommerce.quality.DataQualityJob", "/tmp/datastream-quality-1.0.0.jar",
                "--bootstrap-servers", "kafka:29092", "--input-topic", "user_behavior_events",
                "--mode", "production", "--consumer-group", "chapter9-quality-production",
                "--checkpoint-uri", $checkpointUri,
                "--transaction-prefix", "chapter9-production", "--job-version", "chapter-9-v1"
            ) -FailureMessage "Production resume submission failed."
            $id = Get-SubmittedJobId $output
            Wait-FlinkJobRunning -JobId $id -ExpectedName $productionJobName | Out-Null
            [pscustomobject]@{ job_id = $id }
        }
    $productionJobId = [string]$productionResult.job_id
    $recoveryState.production_job_id = $productionJobId
    Write-CutoverStateAtomic -State $recoveryState -Path $manifestPartialPath
    $productionCheckpoints = Wait-NewCompletedCheckpoint -JobId $productionJobId -ExpectedName $productionJobName

    if (-not $dorisJobId) {
        $dorisResult = Invoke-CutoverMutation -State $recoveryState -Path $manifestPartialPath `
            -Stage "doris_submit" -Operation "submit_doris_clean" -Details @{ name = $dorisJobName } -Action {
                [void](Submit-SqlJob -SqlClient $sqlClient -ContainerSqlPath "/workspace/tmp/chapter-9/doris-clean.sql" -Connectors $dorisConnectors)
                $id = Wait-NewNamedJob -Name $dorisJobName
                [pscustomobject]@{ job_id = $id }
            }
        $dorisJobId = [string]$dorisResult.job_id
        Set-CutoverMutationJobId -State $recoveryState -Path $manifestPartialPath -Stage "doris_submit" -JobId $dorisJobId
        $savedManifest.doris_job_id = $dorisJobId
        Write-ManifestPartial -Manifest $savedManifest -PartialPath $manifestPartialPath
    }
    $dorisCheckpoints = Wait-NewCompletedCheckpoint -JobId $dorisJobId -ExpectedName $dorisJobName

    if (-not $icebergJobId) {
        $icebergResult = Invoke-CutoverMutation -State $recoveryState -Path $manifestPartialPath `
            -Stage "iceberg_submit" -Operation "submit_iceberg_clean" -Details @{ name = $icebergJobName } -Action {
                [void](Submit-SqlJob -SqlClient $sqlClient -ContainerSqlPath "/workspace/tmp/chapter-9/iceberg-clean.sql" `
                    -Connectors $icebergConnectors -ParentClasspath $icebergParentClasspath)
                $id = Wait-NewNamedJob -Name $icebergJobName
                [pscustomobject]@{ job_id = $id }
            }
        $icebergJobId = [string]$icebergResult.job_id
        Set-CutoverMutationJobId -State $recoveryState -Path $manifestPartialPath -Stage "iceberg_submit" -JobId $icebergJobId
    }
    Wait-FlinkJobRunning -JobId $icebergJobId -ExpectedName $icebergJobName | Out-Null
    $icebergCheckpoints = Wait-NewCompletedCheckpoint -JobId $icebergJobId -ExpectedName $icebergJobName
    Set-CutoverMutationIntent -State $recoveryState -Path $manifestPartialPath -Stage "finalization" `
        -Operation "finalize_cutover_manifest" -Details @{ final_path = $manifestPath }
    Complete-CutoverManifest -PartialPath $manifestPartialPath -FinalPath $manifestPath `
        -ProductionJobId $productionJobId -DorisJobId $dorisJobId -IcebergJobId $icebergJobId `
        -ProductionCheckpoints $productionCheckpoints -DorisCheckpoints $dorisCheckpoints `
        -IcebergCheckpoints $icebergCheckpoints
    Set-CutoverMutationResult -State $recoveryState -Path $manifestPartialPath -Stage "finalization" -Status "result" `
        -Details @{ final_path = $manifestPath }
        return
    } | Out-Null
    return
}
# ResumePartial ends before normal cutover.

$shadowJob = Get-OnlyRunningShadowJob $shadowJobName
$shadowJobId = [string]$shadowJob.jid
Assert-RecentCompletedCheckpoint $shadowJobId | Out-Null
Assert-JobNamesAbsent @($productionJobName, $dorisJobName, $icebergJobName)
if (-not (Test-Path -LiteralPath $fatJar -PathType Leaf)) { throw "DataStream Fat JAR is missing: $fatJar" }
if ((Get-Item -LiteralPath $fatJar).Length -le 0) { throw "DataStream Fat JAR is empty: $fatJar" }

Invoke-DockerCommand -Arguments @("cp", $fatJar, "${jobManager}:/tmp/datastream-quality-1.0.0.jar") `
    -FailureMessage "Failed to copy DataStream Fat JAR to JobManager." | Out-Null
Invoke-DockerCommand -Arguments @("exec", $jobManager, "test", "-s", "/tmp/datastream-quality-1.0.0.jar") `
    -FailureMessage "DataStream Fat JAR verification failed inside JobManager." | Out-Null

Invoke-DockerCommand -Arguments @(
    "exec", $kafka, "kafka-topics", "--bootstrap-server", "kafka:29092",
    "--create", "--if-not-exists", "--topic", "user_behavior_clean",
    "--partitions", "1", "--replication-factor", "1"
) -FailureMessage "Failed to create or verify user_behavior_clean." | Out-Null

$rawOffsetOutput = Invoke-DockerCommand -Arguments @(
    "exec", $kafka, "kafka-get-offsets", "--bootstrap-server", "kafka:29092",
    "--topic", "user_behavior_events"
) -FailureMessage "Failed to read raw Kafka log-end offsets."
$rawOffsets = @(ConvertFrom-KafkaOffsets -Lines $rawOffsetOutput -ExpectedTopic "user_behavior_events")
$shadowGroup = Wait-ShadowLagZero -ExpectedOffsets $rawOffsets -KafkaContainer $kafka
$rawOffsetConfirmation = @(ConvertFrom-KafkaOffsets -Lines (Invoke-DockerCommand -Arguments @(
    "exec", $kafka, "kafka-get-offsets", "--bootstrap-server", "kafka:29092",
    "--topic", "user_behavior_events"
) -FailureMessage "Failed to confirm paused raw Kafka offsets.") -ExpectedTopic "user_behavior_events")
if (($rawOffsets -join ";") -ne ($rawOffsetConfirmation -join ";")) {
    throw "Raw Kafka offsets changed after the zero-lag check. Traffic is not paused."
}

$manifest = [ordered]@{
    schema_version = 2
    cutover_id = [guid]::NewGuid().ToString()
    phase = "prepared"
    created_at = [DateTimeOffset]::UtcNow.ToString("o")
    raw_offsets = @($rawOffsets)
    shadow_job_id = $shadowJobId
    savepoint_path = $null
    production_job_id = $null
    doris_job_id = $null
    iceberg_job_id = $null
    mutations = [ordered]@{
        shadow_stop = New-CutoverMutationState
        doris_submit = New-CutoverMutationState
        iceberg_submit = New-CutoverMutationState
        finalization = New-CutoverMutationState
    }
}
Write-ManifestPartial -Manifest $manifest -PartialPath $manifestPartialPath
Write-Host "[cutover] manifest_partial=$manifestPartialPath raw_offsets=$($rawOffsets -join ';') shadow_lag=$($shadowGroup.TotalLag) shadow_job_id=$shadowJobId"

Write-Host "[cutover] stopping shadow job with Savepoint"
$stopResult = Invoke-CutoverShadowStopStage -State $manifest -Path $manifestPartialPath `
    -Jobs (Get-FlinkJobs) -StopAction {
        $output = Invoke-FlinkJobManagerCommand -Command @(
            "/opt/flink/bin/flink", "stop",
            "--savepointPath", $savepointUri, $shadowJobId
        ) -FailureMessage "Shadow Stop-with-Savepoint failed."
        $output | ForEach-Object { Write-Host $_ }
        [pscustomobject]@{ savepoint_path = (Get-SavepointPath $output) }
    }
$savepointPath = [string]$stopResult.savepoint_path

Write-Host "[cutover] starting production DataStream from Savepoint"
# Restore contract: flink run -d -s $savepointPath.
# Production contract: --mode production --consumer-group chapter9-quality-production --transaction-prefix chapter9-production.
$productionResult = Invoke-CutoverProductionSubmitStage -RepositoryRoot $root `
    -Jobs (Get-FlinkJobs) -ExpectedName $productionJobName `
    -CutoverState $manifest -CutoverStatePath $manifestPartialPath -Action {
        $output = Invoke-FlinkJobManagerCommand -Command @(
            "/opt/flink/bin/flink", "run", "-d", "-s", $savepointPath,
            "-c", "com.ecommerce.quality.DataQualityJob", "/tmp/datastream-quality-1.0.0.jar",
            "--bootstrap-servers", "kafka:29092", "--input-topic", "user_behavior_events",
            "--mode", "production", "--consumer-group", "chapter9-quality-production",
            "--checkpoint-uri", $checkpointUri,
            "--transaction-prefix", "chapter9-production", "--job-version", "chapter-9-v1"
        ) -FailureMessage "Production DataStream submission or Savepoint restore failed."
        $output | ForEach-Object { Write-Host $_ }
        $id = Get-SubmittedJobId $output
        Wait-FlinkJobRunning -JobId $id -ExpectedName $productionJobName | Out-Null
        [pscustomobject]@{ job_id = $id }
    }
$productionJobId = [string]$productionResult.job_id
$manifest["production_job_id"] = $productionJobId
Write-ManifestPartial -Manifest $manifest -PartialPath $manifestPartialPath
$productionCheckpoints = Wait-NewCompletedCheckpoint -JobId $productionJobId -ExpectedName $productionJobName

Write-Host "[cutover] submitting Doris clean SQL job"
$dorisResult = Invoke-CutoverMutation -State $manifest -Path $manifestPartialPath `
    -Stage "doris_submit" -Operation "submit_doris_clean" -Details @{ name = $dorisJobName } -Action {
        [void](Submit-SqlJob -SqlClient $sqlClient -ContainerSqlPath "/workspace/tmp/chapter-9/doris-clean.sql" -Connectors $dorisConnectors)
        [pscustomobject]@{ job_id = (Wait-NewNamedJob -Name $dorisJobName) }
    }
$dorisJobId = [string]$dorisResult.job_id
Set-CutoverMutationJobId -State $manifest -Path $manifestPartialPath -Stage "doris_submit" -JobId $dorisJobId
Wait-FlinkJobRunning -JobId $dorisJobId -ExpectedName $dorisJobName | Out-Null
$dorisCheckpoints = Wait-NewCompletedCheckpoint -JobId $dorisJobId -ExpectedName $dorisJobName
$manifest["doris_job_id"] = $dorisJobId
Write-ManifestPartial -Manifest $manifest -PartialPath $manifestPartialPath

Write-Host "[cutover] submitting Iceberg clean SQL job"
$icebergResult = Invoke-CutoverMutation -State $manifest -Path $manifestPartialPath `
    -Stage "iceberg_submit" -Operation "submit_iceberg_clean" -Details @{ name = $icebergJobName } -Action {
        [void](Submit-SqlJob -SqlClient $sqlClient -ContainerSqlPath "/workspace/tmp/chapter-9/iceberg-clean.sql" `
            -Connectors $icebergConnectors -ParentClasspath $icebergParentClasspath)
        [pscustomobject]@{ job_id = (Wait-NewNamedJob -Name $icebergJobName) }
    }
$icebergJobId = [string]$icebergResult.job_id
Set-CutoverMutationJobId -State $manifest -Path $manifestPartialPath -Stage "iceberg_submit" -JobId $icebergJobId
Wait-FlinkJobRunning -JobId $icebergJobId -ExpectedName $icebergJobName | Out-Null
$icebergCheckpoints = Wait-NewCompletedCheckpoint -JobId $icebergJobId -ExpectedName $icebergJobName
Set-CutoverMutationIntent -State $manifest -Path $manifestPartialPath -Stage "finalization" `
    -Operation "finalize_cutover_manifest" -Details @{ final_path = $manifestPath }
Complete-CutoverManifest -PartialPath $manifestPartialPath -FinalPath $manifestPath `
    -ProductionJobId $productionJobId -DorisJobId $dorisJobId -IcebergJobId $icebergJobId `
    -ProductionCheckpoints $productionCheckpoints -DorisCheckpoints $dorisCheckpoints `
    -IcebergCheckpoints $icebergCheckpoints
Set-CutoverMutationResult -State $manifest -Path $manifestPartialPath -Stage "finalization" -Status "result" `
    -Details @{ final_path = $manifestPath }
