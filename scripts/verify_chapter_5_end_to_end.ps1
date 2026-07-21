[CmdletBinding()]
param(
    [switch]$FunctionsOnly
)

$ErrorActionPreference = "Stop"

function Invoke-CheckedCommand {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$Command,
        [Parameter(Mandatory = $true)]
        [string]$FailureMessage
    )

    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw $FailureMessage
    }
}

function Assert-DockerAvailable {
    Invoke-CheckedCommand -Command { docker version } -FailureMessage "Docker daemon is not available. Please start Docker Desktop and retry."
}

function Wait-ForKafkaReady {
    param(
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $status = docker inspect -f "{{.State.Status}}" ecom-kafka 2>$null
        if ($LASTEXITCODE -ne 0) {
            Start-Sleep -Seconds 2
            continue
        }

        if ($status -eq "exited") {
            Start-Sleep -Seconds 5
            docker start ecom-kafka | Out-Null
            Start-Sleep -Seconds 5
            continue
        }

        if ($status -ne "running") {
            Start-Sleep -Seconds 2
            continue
        }

        docker exec ecom-kafka kafka-topics --bootstrap-server kafka:29092 --list | Out-Null
        if ($LASTEXITCODE -eq 0) {
            return
        }

        Start-Sleep -Seconds 2
    }

    throw "Timed out waiting for Kafka to become ready."
}

function Wait-ForHiveMetastoreReady {
    param(
        [int]$TimeoutSeconds = 60
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $status = docker inspect -f "{{.State.Status}}" ecom-hive-metastore 2>$null
        if ($LASTEXITCODE -eq 0 -and $status -eq "running") {
            return
        }

        Start-Sleep -Seconds 2
    }

    throw "Timed out waiting for Hive Metastore to become ready."
}

function New-ValidationEventsFile {
    param(
        [int]$Count = 30
    )

    $eventsFile = "tmp/chapter_5_validation_events.jsonl"
    $eventsPath = [System.IO.Path]::GetFullPath($eventsFile)
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    $lines = for ($index = 1; $index -le $Count; $index++) {
        $event = [ordered]@{
            event_id = "evt_validate_{0:000000}" -f $index
            user_id = "u_{0}" -f (Get-Random -Minimum 1000 -Maximum 9999)
            product_id = "p_{0}" -f (Get-Random -Minimum 1000 -Maximum 9999)
            event_type = @("view", "click", "cart")[(Get-Random -Minimum 0 -Maximum 3)]
            event_time = [DateTimeOffset]::UtcNow.ToString("o")
            channel = @("app", "web", "mini_program")[(Get-Random -Minimum 0 -Maximum 3)]
            device_type = @("ios", "android", "pc")[(Get-Random -Minimum 0 -Maximum 3)]
            page_id = @("home", "search_result", "product_detail", "cart_page")[(Get-Random -Minimum 0 -Maximum 4)]
        }
        ($event | ConvertTo-Json -Compress)
    }

    [System.IO.File]::WriteAllText($eventsPath, ($lines -join "`n"), $utf8NoBom)
    return $eventsFile
}

function Publish-ValidationEvents {
    param(
        [Parameter(Mandatory = $true)]
        [string]$EventsFile
    )

    Get-Content $EventsFile | docker exec -i ecom-kafka kafka-console-producer --bootstrap-server kafka:29092 --topic user_behavior_events | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to publish validation events to Kafka."
    }
}

function Invoke-MinioAdminJson {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ObjectPath
    )

    $listing = docker exec ecom-minio-init mc ls --recursive --json $ObjectPath
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to inspect Iceberg objects in MinIO at path: $ObjectPath"
    }

    return $listing
}

function Get-IcebergObjectNames {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ObjectPath
    )

    $listing = Invoke-MinioAdminJson -ObjectPath $ObjectPath
    $names = New-Object System.Collections.Generic.HashSet[string]
    foreach ($line in $listing) {
        $trimmed = ([string]$line).Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed)) {
            continue
        }

        $entry = $trimmed | ConvertFrom-Json
        $key = ([string]$entry.key).TrimEnd("/")
        $name = ($key -split "/")[-1]
        if (-not [string]::IsNullOrWhiteSpace($name)) {
            [void]$names.Add($name)
        }
    }

    return ,$names
}

function Wait-ForIcebergDataCommit {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [System.Collections.Generic.HashSet[string]]$BaselineMetadataNames,
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [System.Collections.Generic.HashSet[string]]$BaselineDataNames,
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $listing = docker exec ecom-minio-init sh -lc "mc ls --recursive local/warehouse/iceberg/analytics.db/user_behavior_detail"
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to inspect Iceberg objects in MinIO."
        }

        $hasNewMetadata = $false
        $hasNewDataFile = $false
        foreach ($line in $listing) {
            $trimmed = $line.Trim()
            if ([string]::IsNullOrWhiteSpace($trimmed)) {
                continue
            }

            $name = [regex]::Match($trimmed, "(?<name>\S+)$").Groups["name"].Value
            if ($name -like "*.metadata.json" -and -not $BaselineMetadataNames.Contains($name)) {
                $hasNewMetadata = $true
            }

            if ($name -like "*.parquet" -and -not $BaselineDataNames.Contains($name)) {
                $hasNewDataFile = $true
            }
        }

        if ($hasNewMetadata -and $hasNewDataFile) {
            return $listing
        }

        Start-Sleep -Seconds 5
    }

    throw "Timed out waiting for new Iceberg metadata and data files in MinIO."
}

if ($FunctionsOnly) {
    return
}

Assert-DockerAvailable

$chapter5Runner = Join-Path $PSScriptRoot "run_chapter_5_iceberg_pipeline.ps1"
& $chapter5Runner
if ($LASTEXITCODE -ne 0) {
    throw "Failed to submit the Chapter 5 pipeline before end-to-end validation."
}

Wait-ForKafkaReady -TimeoutSeconds 60
Wait-ForHiveMetastoreReady -TimeoutSeconds 60

$baselineMetadataNames = Get-IcebergObjectNames -ObjectPath "local/warehouse/iceberg/analytics.db/user_behavior_detail/metadata"
$baselineDataNames = Get-IcebergObjectNames -ObjectPath "local/warehouse/iceberg/analytics.db/user_behavior_detail/data"
$eventsFile = New-ValidationEventsFile -Count 30
Write-Host "[chapter5-verify] publishing validation events..."
Publish-ValidationEvents -EventsFile $eventsFile

Write-Host "[chapter5-verify] waiting for Iceberg data commit..."
$listing = Wait-ForIcebergDataCommit -BaselineMetadataNames $baselineMetadataNames -BaselineDataNames $baselineDataNames -TimeoutSeconds 90
Write-Host $listing
