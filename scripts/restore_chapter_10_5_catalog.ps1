param([switch]$FunctionsOnly)

function Invoke-Chapter105Compose {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )

    $repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
    $composeArguments = @(
        "compose",
        "--env-file", (Join-Path $repositoryRoot "infra/.env.example"),
        "-f", (Join-Path $repositoryRoot "infra/docker-compose.yml"),
        "--profile", "lakehouse"
    ) + $Arguments

    try {
        $output = @(& docker @composeArguments 2>&1)
    } catch {
        throw $FailureMessage
    }
    if ($LASTEXITCODE -ne 0) {
        throw $FailureMessage
    }
    return @($output | ForEach-Object { [string]$_ })
}

function Invoke-Chapter105Trino {
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )

    return Invoke-Chapter105Compose -Arguments @(
        "exec", "-T", "trino",
        "trino",
        "--server", "http://localhost:8080",
        "--catalog", "lakehouse",
        "--schema", "analytics",
        "--output-format", "CSV_HEADER",
        "--execute", $Sql
    ) -FailureMessage $FailureMessage
}

function Select-IcebergMetadataCandidate {
    param([string[]]$Names)

    if (@($Names).Count -eq 0) {
        throw "No Iceberg metadata candidate is available."
    }

    $byVersion = @{}
    foreach ($name in @($Names)) {
        if ($name -isnot [string]) {
            throw "Iceberg metadata candidate name is invalid."
        }
        $match = [regex]::Match(
            $name,
            '^(?<version>[0-9]{5})-(?<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.metadata\.json$'
        )
        if (-not $match.Success) {
            throw "Iceberg metadata candidate name is invalid."
        }

        $version = [int]$match.Groups["version"].Value
        if ($byVersion.ContainsKey($version)) {
            throw "Iceberg metadata candidate version is ambiguous."
        }
        $byVersion[$version] = $name
    }

    $highestVersion = @($byVersion.Keys | Measure-Object -Maximum).Maximum
    return [string]$byVersion[[int]$highestVersion]
}

function Get-IcebergMetadataValue {
    param(
        [Parameter(Mandatory = $true)][object]$Metadata,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $properties = @($Metadata.PSObject.Properties | Where-Object { $_.Name -ceq $Name })
    if ($properties.Count -ne 1) {
        throw "Iceberg metadata structure is invalid."
    }
    return $properties[0].Value
}

function Assert-IcebergMetadata {
    param([object]$Metadata)

    if ($Metadata -isnot [pscustomobject]) {
        throw "Iceberg metadata structure is invalid."
    }

    $location = Get-IcebergMetadataValue -Metadata $Metadata -Name "location"
    $tableUuid = Get-IcebergMetadataValue -Metadata $Metadata -Name "table-uuid"
    $snapshotId = Get-IcebergMetadataValue -Metadata $Metadata -Name "current-snapshot-id"

    if ($location -isnot [string] -or
        $location -cne "s3a://warehouse/iceberg/analytics.db/user_behavior_detail") {
        throw "Iceberg metadata location is invalid."
    }
    if ($tableUuid -isnot [string] -or
        $tableUuid -cnotmatch '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') {
        throw "Iceberg metadata table UUID is invalid."
    }
    if ($snapshotId -isnot [int32] -and $snapshotId -isnot [int64]) {
        throw "Iceberg metadata snapshot identifier is invalid."
    }
}

function Get-Chapter105CatalogTableCount {
    $sql = @'
SELECT count(*) AS table_count
FROM lakehouse.information_schema.tables
WHERE table_schema = 'analytics'
  AND table_name = 'user_behavior_detail'
'@.Trim()
    $lines = @(Invoke-Chapter105Trino -Sql $sql -FailureMessage "Trino catalog existence check failed." |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) })

    if ($lines.Count -ne 2 -or $lines[0] -cne "table_count" -or $lines[1] -notmatch '^(0|1)$') {
        throw "Trino catalog existence check returned an invalid result."
    }
    return [int]$lines[1]
}

function Assert-Chapter105FixedTableReadable {
    $sql = "SELECT 1 FROM lakehouse.analytics.user_behavior_detail LIMIT 1"
    $null = Invoke-Chapter105Trino -Sql $sql -FailureMessage "Trino fixed table read validation failed."
}

function Get-Chapter105MetadataNames {
    $listCommand = 'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc ls --recursive --json local/warehouse/iceberg/analytics.db/user_behavior_detail/metadata'
    $lines = Invoke-Chapter105Compose -Arguments @(
        "run", "--rm", "--no-deps", "--entrypoint", "/bin/sh", "minio-init", "-lc", $listCommand
    ) -FailureMessage "Iceberg metadata listing failed."

    $prefix = "iceberg/analytics.db/user_behavior_detail/metadata/"
    $names = @()
    foreach ($line in @($lines)) {
        try {
            $entry = $line | ConvertFrom-Json -ErrorAction Stop
        } catch {
            throw "Iceberg metadata listing returned an invalid result."
        }
        if ($entry -isnot [pscustomobject]) {
            throw "Iceberg metadata listing returned an invalid result."
        }

        $key = Get-IcebergMetadataValue -Metadata $entry -Name "key"
        if ($key -isnot [string] -or -not $key.StartsWith($prefix, [System.StringComparison]::Ordinal)) {
            throw "Iceberg metadata listing returned an invalid result."
        }
        $names += $key.Substring($prefix.Length)
    }
    return @($names)
}

function Get-Chapter105MetadataObject {
    param([Parameter(Mandatory = $true)][string]$MetadataFileName)

    $validatedName = Select-IcebergMetadataCandidate -Names @($MetadataFileName)
    $readCommand = 'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc cat local/warehouse/iceberg/analytics.db/user_behavior_detail/metadata/' + $validatedName
    $lines = Invoke-Chapter105Compose -Arguments @(
        "run", "--rm", "--no-deps", "--entrypoint", "/bin/sh", "minio-init", "-lc", $readCommand
    ) -FailureMessage "Iceberg metadata read failed."

    try {
        $metadata = ($lines -join "`n") | ConvertFrom-Json -ErrorAction Stop
    } catch {
        throw "Iceberg metadata is invalid."
    }
    if ($metadata -is [array]) {
        throw "Iceberg metadata is invalid."
    }
    return $metadata
}

function Invoke-Chapter105RegisterTable {
    param([Parameter(Mandatory = $true)][string]$MetadataFileName)

    $validatedName = Select-IcebergMetadataCandidate -Names @($MetadataFileName)
    $sql = @"
CALL lakehouse.system.register_table(
    schema_name => 'analytics',
    table_name => 'user_behavior_detail',
    table_location => 's3a://warehouse/iceberg/analytics.db/user_behavior_detail',
    metadata_file_name => '$validatedName'
)
"@.Trim()
    $null = Invoke-Chapter105Trino -Sql $sql -FailureMessage "Fixed Iceberg catalog registration failed."
}

function Assert-Chapter105FixedTableAggregateReadable {
    $sql = @'
SELECT count(*) AS event_count, max(event_time) AS max_event_time
FROM lakehouse.analytics.user_behavior_detail
'@.Trim()
    $lines = @(Invoke-Chapter105Trino -Sql $sql -FailureMessage "Trino fixed table aggregate validation failed." |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) })

    if ($lines.Count -ne 2 -or $lines[0] -cne "event_count,max_event_time" -or $lines[1] -notmatch '^[0-9]+,') {
        throw "Trino fixed table aggregate validation returned an invalid result."
    }
}

function Restore-Chapter105Catalog {
    $tableCount = Get-Chapter105CatalogTableCount
    if ($tableCount -eq 1) {
        Assert-Chapter105FixedTableReadable
        return "already_registered"
    }
    if ($tableCount -ne 0) {
        throw "Trino catalog existence check returned an invalid result."
    }

    $metadataFileName = Select-IcebergMetadataCandidate -Names (Get-Chapter105MetadataNames)
    $metadata = Get-Chapter105MetadataObject -MetadataFileName $metadataFileName
    Assert-IcebergMetadata -Metadata $metadata
    Invoke-Chapter105RegisterTable -MetadataFileName $metadataFileName
    Assert-Chapter105FixedTableAggregateReadable
    return "restored"
}

if ($FunctionsOnly) { return }

& {
    $ErrorActionPreference = "Stop"
    Restore-Chapter105Catalog
}
