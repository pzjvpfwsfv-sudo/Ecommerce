[CmdletBinding()]
param(
    [string]$ManifestPath = '',
    [string]$Entity = '',
    [int]$TimeoutSeconds = 600,
    [switch]$PlanOnly,
    [switch]$FunctionsOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-G2eRegistry {
    return [ordered]@{
        orders = [pscustomobject]@{
            SourceFile = 'olist_orders_dataset.csv'; TargetTable = 'orders_src_v1'; KeyColumns = @('order_id')
        }
        order_items = [pscustomobject]@{
            SourceFile = 'olist_order_items_dataset.csv'; TargetTable = 'order_items_src_v1'; KeyColumns = @('order_id', 'order_item_id')
        }
        order_payments = [pscustomobject]@{
            SourceFile = 'olist_order_payments_dataset.csv'; TargetTable = 'order_payments_src_v1'; KeyColumns = @('order_id', 'payment_sequential')
        }
        order_reviews = [pscustomobject]@{
            SourceFile = 'olist_order_reviews_dataset.csv'; TargetTable = 'order_reviews_src_v1'; KeyColumns = @('source_row_id')
        }
        customers = [pscustomobject]@{
            SourceFile = 'olist_customers_dataset.csv'; TargetTable = 'customers_src_v1'; KeyColumns = @('customer_id')
        }
        products = [pscustomobject]@{
            SourceFile = 'olist_products_dataset.csv'; TargetTable = 'products_src_v1'; KeyColumns = @('product_id')
        }
        sellers = [pscustomobject]@{
            SourceFile = 'olist_sellers_dataset.csv'; TargetTable = 'sellers_src_v1'; KeyColumns = @('seller_id')
        }
        geolocation = [pscustomobject]@{
            SourceFile = 'olist_geolocation_dataset.csv'; TargetTable = 'geolocation_src_v1'; KeyColumns = @('source_row_id')
        }
        category_translation = [pscustomobject]@{
            SourceFile = 'product_category_name_translation.csv'; TargetTable = 'category_translation_src_v1'; KeyColumns = @('product_category_name')
        }
    }
}

function Get-G2ePropertyValue {
    param(
        [Parameter(Mandatory = $true)]$Object,
        [Parameter(Mandatory = $true)][string]$Name
    )

    if ($Object -is [Collections.IDictionary]) {
        if (-not $Object.Contains($Name)) { throw "Missing G2-E property '$Name'." }
        return $Object[$Name]
    }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) { throw "Missing G2-E property '$Name'." }
    return $property.Value
}

function Assert-G2eSha256 {
    param(
        [Parameter(Mandatory = $true)]$Value,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $text = [string]$Value
    if ($text -cnotmatch '^[0-9a-f]{64}$') { throw "G2-E $Name must be a lowercase SHA-256 value." }
    return $text
}

function ConvertTo-G2eCount {
    param(
        [Parameter(Mandatory = $true)]$Value,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    if ($text -notmatch '^(0|[1-9][0-9]*)$') { throw "G2-E $Name must be a non-negative integer." }
    try {
        return [long]::Parse($text, [Globalization.CultureInfo]::InvariantCulture)
    } catch {
        throw "G2-E $Name exceeds the supported integer range."
    }
}

function Get-G2eSourceDeployment {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Manifest,
        [Parameter(Mandatory = $true)][string]$Entity
    )

    $registry = Get-G2eRegistry
    if (-not $registry.Contains($Entity)) { throw 'Unknown G2-E Olist source entity.' }
    if ((ConvertTo-G2eCount (Get-G2ePropertyValue $Manifest 'schema_version') 'schema version') -ne 1) {
        throw 'G2-E source manifest schema version is not supported.'
    }
    if ([string](Get-G2ePropertyValue $Manifest 'dataset_id') -cne 'olist-brazilian-ecommerce-v2') {
        throw 'G2-E source manifest dataset identity is invalid.'
    }
    $bundle = Assert-G2eSha256 (Get-G2ePropertyValue $Manifest 'source_bundle_sha256') 'bundle identity'
    $files = @(Get-G2ePropertyValue $Manifest 'files')
    if ($files.Count -ne $registry.Count) { throw 'G2-E source manifest must contain exactly nine files.' }

    $validated = [ordered]@{}
    foreach ($file in $files) {
        $fileEntity = [string](Get-G2ePropertyValue $file 'entity')
        if (-not $registry.Contains($fileEntity) -or $validated.Contains($fileEntity)) {
            throw 'G2-E source manifest contains an unknown or duplicate entity.'
        }
        $spec = $registry[$fileEntity]
        if ([string](Get-G2ePropertyValue $file 'name') -cne [string]$spec.SourceFile) {
            throw "G2-E source filename is invalid for $fileEntity."
        }
        $rowCount = ConvertTo-G2eCount (Get-G2ePropertyValue $file 'row_count') "$fileEntity row count"
        if ($rowCount -lt 1) { throw "G2-E source row count must be positive for $fileEntity." }
        $normalized = Get-G2ePropertyValue $file 'normalized'
        if ([string](Get-G2ePropertyValue $normalized 'name') -cne "normalized/$fileEntity.jsonl") {
            throw "G2-E normalized filename is invalid for $fileEntity."
        }
        $normalizedRows = ConvertTo-G2eCount (Get-G2ePropertyValue $normalized 'row_count') "$fileEntity normalized row count"
        if ($normalizedRows -ne $rowCount) { throw "G2-E normalized row count differs for $fileEntity." }
        $validated[$fileEntity] = [pscustomobject][ordered]@{
            Entity = $fileEntity
            SourceFile = [string]$spec.SourceFile
            TargetTable = [string]$spec.TargetTable
            KeyColumns = [string[]]$spec.KeyColumns
            ExpectedRowCount = $rowCount
            NormalizedRelativePath = [string](Get-G2ePropertyValue $normalized 'name')
            NormalizedSha256 = Assert-G2eSha256 (Get-G2ePropertyValue $normalized 'sha256') "$fileEntity normalized digest"
            SourceRowIdSequenceSha256 = Assert-G2eSha256 `
                (Get-G2ePropertyValue $normalized 'source_row_id_sequence_sha256') "$fileEntity row identity digest"
        }
    }
    foreach ($name in $registry.Keys) {
        if (-not $validated.Contains($name)) { throw "G2-E source manifest is missing $name." }
    }

    $selected = $validated[$Entity]
    return [pscustomobject][ordered]@{
        Entity = $selected.Entity
        DatasetId = 'olist-brazilian-ecommerce-v2'
        SourceBundleSha256 = $bundle
        SourceFile = $selected.SourceFile
        TargetTable = $selected.TargetTable
        KeyColumns = $selected.KeyColumns
        ExpectedRowCount = $selected.ExpectedRowCount
        NormalizedRelativePath = $selected.NormalizedRelativePath
        NormalizedSha256 = $selected.NormalizedSha256
        SourceRowIdSequenceSha256 = $selected.SourceRowIdSequenceSha256
        PipelineName = "graduation-g2e-$Entity-$bundle"
        ContainerSourcePath = "/data/olist/prepared/$bundle/normalized/$Entity.jsonl"
    }
}

function Test-G2ePathContained {
    param(
        [Parameter(Mandatory = $true)][string]$RootPath,
        [Parameter(Mandatory = $true)][string]$CandidatePath
    )

    $comparison = if ([IO.Path]::DirectorySeparatorChar -eq [char]92) {
        [StringComparison]::OrdinalIgnoreCase
    } else {
        [StringComparison]::Ordinal
    }
    $root = [IO.Path]::GetFullPath($RootPath).TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    )
    $candidate = [IO.Path]::GetFullPath($CandidatePath)
    return $candidate.Equals($root, $comparison) -or $candidate.StartsWith(
        ($root + [IO.Path]::DirectorySeparatorChar),
        $comparison
    )
}

function Assert-G2eNoReparsePoint {
    param(
        [Parameter(Mandatory = $true)][string]$RootPath,
        [Parameter(Mandatory = $true)][string]$CandidatePath
    )

    $root = [IO.Path]::GetFullPath($RootPath)
    $candidate = [IO.Path]::GetFullPath($CandidatePath)
    if (-not (Test-G2ePathContained $root $candidate)) { throw 'G2-E path escapes its fixed data root.' }
    $current = $candidate
    while (Test-G2ePathContained $root $current) {
        if ([IO.File]::Exists($current) -or [IO.Directory]::Exists($current)) {
            $item = Get-Item -LiteralPath $current -Force -ErrorAction Stop
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw 'G2-E data paths cannot contain a reparse point.'
            }
        }
        if ($current.Equals($root, [StringComparison]::OrdinalIgnoreCase)) { break }
        $parent = [IO.Path]::GetDirectoryName($current)
        if ([string]::IsNullOrEmpty($parent) -or $parent -ceq $current) { break }
        $current = $parent
    }
}

function Assert-G2eManifestPath {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ManifestPath,
        [Parameter(Mandatory = $true)][string]$DataRoot
    )

    if (-not [IO.Path]::IsPathRooted($ManifestPath) -or -not [IO.Path]::IsPathRooted($DataRoot)) {
        throw 'G2-E manifest and data root paths must be absolute.'
    }
    $manifest = [IO.Path]::GetFullPath($ManifestPath)
    $root = [IO.Path]::GetFullPath($DataRoot)
    if ($manifest.Length -lt 2 -or $root.Length -lt 2 -or
            $manifest.Substring(0, 2).ToLowerInvariant() -ne 'd:' -or
            $root.Substring(0, 2).ToLowerInvariant() -ne 'd:') {
        throw 'G2-E source data and manifest must remain on the D drive.'
    }
    if (-not (Test-G2ePathContained $root $manifest)) {
        throw 'G2-E manifest is outside OLIST_DATA_DIR.'
    }
    if ([IO.File]::Exists($manifest)) { Assert-G2eNoReparsePoint $root $manifest }
    return $manifest
}

function Assert-G2eFileSha256 {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$ExpectedSha256
    )

    $expected = Assert-G2eSha256 $ExpectedSha256 'expected file digest'
    if (-not [IO.File]::Exists($Path)) { throw 'G2-E normalized source file does not exist.' }
    $actual = (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($actual -cne $expected) { throw 'G2-E normalized source file digest does not match the manifest.' }
    return $actual
}

function Get-G2eEnvValue {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $matches = @(Get-Content -LiteralPath $Path -Encoding UTF8 | Where-Object {
        $_ -match ('^' + [regex]::Escape($Name) + '=')
    })
    if ($matches.Count -ne 1) { throw "G2-E environment file must define $Name exactly once." }
    $value = $matches[0].Substring($Name.Length + 1).Trim()
    if ([string]::IsNullOrWhiteSpace($value)) { throw "G2-E environment value $Name is blank." }
    return $value
}

function Get-G2eRuntimeDeployment {
    param(
        [Parameter(Mandatory = $true)][string]$ManifestPath,
        [Parameter(Mandatory = $true)][string]$Entity,
        [Parameter(Mandatory = $true)][string]$DataRoot
    )

    $safeManifest = Assert-G2eManifestPath $ManifestPath $DataRoot
    if (-not [IO.File]::Exists($safeManifest)) { throw 'G2-E manifest file does not exist.' }
    try {
        $manifest = Get-Content -LiteralPath $safeManifest -Raw -Encoding UTF8 | ConvertFrom-Json
    } catch {
        throw 'G2-E manifest is not valid JSON.'
    }
    $deployment = Get-G2eSourceDeployment $manifest $Entity
    $expectedManifest = Join-Path $DataRoot "prepared/$($deployment.SourceBundleSha256)/source-bundle.json"
    if (-not ([IO.Path]::GetFullPath($safeManifest).Equals(
                [IO.Path]::GetFullPath($expectedManifest),
                [StringComparison]::OrdinalIgnoreCase))) {
        throw 'G2-E manifest path does not match its bundle identity.'
    }
    $normalizedPath = Join-Path (Split-Path $safeManifest -Parent) $deployment.NormalizedRelativePath
    if (-not (Test-G2ePathContained $DataRoot $normalizedPath)) {
        throw 'G2-E normalized source path escapes OLIST_DATA_DIR.'
    }
    Assert-G2eNoReparsePoint $DataRoot $normalizedPath
    $null = Assert-G2eFileSha256 $normalizedPath $deployment.NormalizedSha256

    return [pscustomobject][ordered]@{
        Entity = $deployment.Entity
        DatasetId = $deployment.DatasetId
        SourceBundleSha256 = $deployment.SourceBundleSha256
        SourceFile = $deployment.SourceFile
        TargetTable = $deployment.TargetTable
        KeyColumns = $deployment.KeyColumns
        ExpectedRowCount = $deployment.ExpectedRowCount
        NormalizedRelativePath = $deployment.NormalizedRelativePath
        NormalizedSha256 = $deployment.NormalizedSha256
        SourceRowIdSequenceSha256 = $deployment.SourceRowIdSequenceSha256
        PipelineName = $deployment.PipelineName
        ContainerSourcePath = $deployment.ContainerSourcePath
        ManifestPath = [IO.Path]::GetFullPath($safeManifest)
        NormalizedPath = [IO.Path]::GetFullPath($normalizedPath)
        DataRoot = [IO.Path]::GetFullPath($DataRoot)
    }
}

function Assert-G2eRenderedSql {
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [Parameter(Mandatory = $true)]$Deployment
    )

    if ($Sql -cmatch '__[A-Z0-9_]+__') { throw 'G2-E ingest SQL has an unresolved token.' }
    if ([regex]::Matches($Sql, '(?im)^\s*INSERT\s+INTO\s+').Count -ne 1) {
        throw 'G2-E ingest SQL must contain exactly one INSERT INTO statement.'
    }
    $target = "lakehouse.olist.$($Deployment.TargetTable)"
    if ($Sql -cnotmatch ('(?im)^\s*INSERT\s+INTO\s+' + [regex]::Escape($target) + '\s*$')) {
        throw 'G2-E ingest SQL writes outside the fixed target.'
    }
    foreach ($required in @(
            "SET 'execution.runtime-mode' = 'batch';",
            "SET 'parallelism.default' = '1';",
            "SET 'pipeline.name' = '$($Deployment.PipelineName)';",
            "'path' = '$($Deployment.ContainerSourcePath)'",
            "'json.fail-on-missing-field' = 'true'",
            "'json.ignore-parse-errors' = 'false'",
            "source_bundle_sha256 = '$($Deployment.SourceBundleSha256)'"
        )) {
        if (-not $Sql.Contains($required)) { throw 'G2-E ingest SQL is missing a fixed contract value.' }
    }
    if ($Sql.Contains("'connector' = 'kafka'") -or
            $Sql -match '(?im)^\s*(DROP|DELETE|TRUNCATE|ALTER|REPLACE)\b') {
        throw 'G2-E ingest SQL contains a forbidden operation.'
    }
}

function Render-G2eVerificationSql {
    param(
        [Parameter(Mandatory = $true)][string]$Template,
        [Parameter(Mandatory = $true)]$Deployment
    )

    $tokens = @([regex]::Matches($Template, '__[A-Z0-9_]+__') | ForEach-Object { $_.Value } | Sort-Object -Unique)
    if (($tokens -join ',') -cne '__KEY_COLUMNS__,__TARGET_TABLE__') {
        throw 'G2-E verification SQL token set is invalid.'
    }
    $registry = Get-G2eRegistry
    if (-not $registry.Contains([string]$Deployment.Entity) -or
            [string]$registry[[string]$Deployment.Entity].TargetTable -cne [string]$Deployment.TargetTable) {
        throw 'G2-E verification deployment is outside the fixed registry.'
    }
    $sql = $Template.Replace('__TARGET_TABLE__', [string]$Deployment.TargetTable)
    $sql = $sql.Replace('__KEY_COLUMNS__', ([string[]]$Deployment.KeyColumns -join ', '))
    if ($sql -cmatch '__[A-Z0-9_]+__') { throw 'G2-E verification SQL has an unresolved token.' }
    return $sql
}

function Split-G2eNamedSql {
    param([Parameter(Mandatory = $true)][string]$Sql)

    $pattern = '(?ms)^-- result:([a-z_]+)\s*\r?\n(.*?;)\s*(?=^-- result:|\z)'
    $matches = [regex]::Matches($Sql, $pattern)
    $expected = @('table_exists', 'row_summary', 'identity', 'key_uniqueness', 'snapshot')
    if ($matches.Count -ne $expected.Count) { throw 'G2-E verification SQL must contain five named statements.' }
    $result = for ($index = 0; $index -lt $matches.Count; $index++) {
        $name = $matches[$index].Groups[1].Value
        if ($name -cne $expected[$index]) { throw 'G2-E verification SQL statement order is invalid.' }
        [pscustomobject][ordered]@{ Name = $name; Sql = $matches[$index].Groups[2].Value.Trim() }
    }
    return $result
}

function ConvertFrom-G2eCsvResult {
    param([Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$Lines)

    $content = @($Lines | Where-Object {
        $_ -isnot [Management.Automation.ErrorRecord] -and
        -not [string]::IsNullOrWhiteSpace([string]$_)
    } | ForEach-Object { [string]$_ })
    if ($content.Count -lt 2) { throw 'G2-E Trino query returned no data row.' }
    try { $rows = @($content -join "`n" | ConvertFrom-Csv) } catch { throw 'G2-E Trino returned malformed CSV.' }
    if ($rows.Count -ne 1) { throw 'G2-E Trino query must return exactly one data row.' }
    return $rows[0]
}

function Assert-G2eSourceState {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Observed,
        [Parameter(Mandatory = $true)]$Expected
    )

    $exists = ConvertTo-G2eCount (Get-G2ePropertyValue $Observed 'table_exists') 'table existence count'
    if ($exists -eq 0) { return [pscustomobject][ordered]@{ Kind = 'Absent' } }
    if ($exists -ne 1) { throw 'Unsafe existing Olist source state: invalid table existence count.' }

    $rows = ConvertTo-G2eCount (Get-G2ePropertyValue $Observed 'row_count') 'row count'
    $distinct = ConvertTo-G2eCount (Get-G2ePropertyValue $Observed 'distinct_source_row_id_count') 'distinct row ID count'
    $bundleCount = ConvertTo-G2eCount (Get-G2ePropertyValue $Observed 'bundle_count') 'bundle count'
    $fileCount = ConvertTo-G2eCount (Get-G2ePropertyValue $Observed 'source_file_count') 'source file count'
    $duplicates = ConvertTo-G2eCount (Get-G2ePropertyValue $Observed 'duplicate_key_count') 'duplicate key count'
    $snapshots = ConvertTo-G2eCount (Get-G2ePropertyValue $Observed 'snapshot_count') 'snapshot count'

    if ($rows -eq 0) {
        $emptyValues = @(
            (Get-G2ePropertyValue $Observed 'min_source_row_number'),
            (Get-G2ePropertyValue $Observed 'max_source_row_number'),
            (Get-G2ePropertyValue $Observed 'source_row_id_sequence_sha256'),
            (Get-G2ePropertyValue $Observed 'source_bundle_sha256'),
            (Get-G2ePropertyValue $Observed 'source_file'),
            (Get-G2ePropertyValue $Observed 'latest_snapshot_id'),
            (Get-G2ePropertyValue $Observed 'latest_snapshot_committed_at')
        )
        if ($distinct -eq 0 -and $bundleCount -eq 0 -and $fileCount -eq 0 -and
                $duplicates -eq 0 -and $snapshots -eq 0 -and
                @($emptyValues | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) }).Count -eq 0) {
            return [pscustomobject][ordered]@{ Kind = 'EmptyWithoutSnapshot' }
        }
        throw 'Unsafe existing Olist source state: contradictory empty table.'
    }

    $expectedRows = ConvertTo-G2eCount $Expected.ExpectedRowCount 'expected row count'
    $minimum = ConvertTo-G2eCount (Get-G2ePropertyValue $Observed 'min_source_row_number') 'minimum row number'
    $maximum = ConvertTo-G2eCount (Get-G2ePropertyValue $Observed 'max_source_row_number') 'maximum row number'
    if ($rows -ne $expectedRows -or $distinct -ne $expectedRows -or $minimum -ne 1 -or $maximum -ne $expectedRows) {
        throw 'Unsafe existing Olist source state: row reconciliation failed.'
    }
    if ($bundleCount -ne 1 -or [string](Get-G2ePropertyValue $Observed 'source_bundle_sha256') -cne [string]$Expected.SourceBundleSha256) {
        throw 'Unsafe existing Olist source state: bundle identity differs.'
    }
    if ($fileCount -ne 1 -or [string](Get-G2ePropertyValue $Observed 'source_file') -cne [string]$Expected.SourceFile) {
        throw 'Unsafe existing Olist source state: source filename differs.'
    }
    if ($duplicates -ne 0 -or [string](Get-G2ePropertyValue $Observed 'source_row_id_sequence_sha256') -cne [string]$Expected.SourceRowIdSequenceSha256) {
        throw 'Unsafe existing Olist source state: key or row identity digest differs.'
    }
    $snapshotId = [string](Get-G2ePropertyValue $Observed 'latest_snapshot_id')
    $snapshotTime = [string](Get-G2ePropertyValue $Observed 'latest_snapshot_committed_at')
    if ($snapshots -lt 1 -or $snapshotId -notmatch '^[1-9][0-9]*$' -or [string]::IsNullOrWhiteSpace($snapshotTime)) {
        throw 'Unsafe existing Olist source state: committed Iceberg snapshot is missing.'
    }
    return [pscustomobject][ordered]@{
        Kind = 'Verified'
        RowCount = $rows
        SnapshotId = $snapshotId
        SnapshotCommittedAt = $snapshotTime
    }
}

function Enable-G2eDockerCli {
    param([string]$FallbackBin = 'D:\DockerProgram\Docker\resources\bin')

    $command = Get-Command docker -ErrorAction SilentlyContinue
    if ($null -eq $command) {
        $candidate = Join-Path $FallbackBin 'docker.exe'
        if (-not [IO.File]::Exists($candidate)) { throw 'Docker CLI is unavailable for G2-E.' }
        $env:PATH = "$FallbackBin;$env:PATH"
        $command = Get-Command docker -ErrorAction Stop
    }
    return $command.Source
}

function Get-G2eServiceStartupPlan {
    return [pscustomobject][ordered]@{
        Lakehouse = [string[]]@('minio', 'minio-init', 'metastore-postgres', 'hive-metastore', 'trino')
        Flink = [string[]]@('flink-jobmanager', 'flink-taskmanager', 'flink-sql-client')
    }
}

function Invoke-G2eChecked {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Command,
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )

    $output = @(& $Command 2>&1)
    if ($LASTEXITCODE -ne 0) { throw $FailureMessage }
    return $output
}

function Get-G2eFlinkJobs {
    param([string]$FlinkRestUrl = 'http://localhost:8081')

    $response = Invoke-RestMethod -Method Get -Uri "$FlinkRestUrl/jobs/overview" -TimeoutSec 10
    return @($response.jobs)
}

function Assert-G2eNoActivePipelineJob {
    param(
        [Parameter(Mandatory = $true)][string]$PipelineName,
        [object[]]$Jobs = @()
    )

    $activeStates = @('CREATED', 'RUNNING', 'FAILING', 'CANCELLING', 'RESTARTING', 'RECONCILING', 'INITIALIZING')
    $matches = @($Jobs | Where-Object { $_.name -ceq $PipelineName -and $_.state -in $activeStates })
    if ($matches.Count -ne 0) { throw 'An active duplicate G2-E source pipeline already exists.' }
}

function Wait-G2eNewFinishedJob {
    param(
        [Parameter(Mandatory = $true)][string]$PipelineName,
        [string[]]$BeforeJobIds = @(),
        [int]$TimeoutSeconds = 600,
        [string]$FlinkRestUrl = 'http://localhost:8081'
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $matches = @(Get-G2eFlinkJobs $FlinkRestUrl | Where-Object {
            $_.name -ceq $PipelineName -and $BeforeJobIds -cnotcontains [string]$_.jid
        })
        if ($matches.Count -gt 1) { throw 'Multiple new G2-E jobs match the fixed pipeline identity.' }
        if ($matches.Count -eq 1) {
            $state = [string]$matches[0].state
            if ($state -ceq 'FINISHED') { return $matches[0] }
            if ($state -in @('FAILED', 'CANCELED', 'SUSPENDED')) {
                throw "G2-E bounded Flink job ended in $state."
            }
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw 'Timed out waiting for the exact G2-E bounded Flink job to finish.'
}

function Wait-G2eHttpReady {
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [int]$TimeoutSeconds = 120
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $null = Invoke-RestMethod -Method Get -Uri $Url -TimeoutSec 10
            return
        } catch {
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "Timed out waiting for G2-E dependency at $Url."
}

function Invoke-G2eTrinoStatement {
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [Parameter(Mandatory = $true)][string]$ComposeFile,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $lines = @(& docker compose --env-file $EnvFile -f $ComposeFile exec -T trino trino `
        --server http://localhost:8080 --catalog lakehouse --schema olist `
        --output-format CSV_HEADER --execute $Sql 2>&1)
    if ($LASTEXITCODE -ne 0) { throw 'G2-E Trino verification statement failed.' }
    return ConvertFrom-G2eCsvResult -Lines $lines
}

function Get-G2eTrinoRowIdSequenceEvidence {
    param(
        [Parameter(Mandatory = $true)]$Deployment,
        [Parameter(Mandatory = $true)][string]$ComposeFile,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $registry = Get-G2eRegistry
    $entity = [string]$Deployment.Entity
    $targetTable = [string]$Deployment.TargetTable
    if (-not $registry.Contains($entity) -or
            [string]$registry[$entity].TargetTable -cne $targetTable) {
        throw 'G2-E row identity stream is outside the fixed registry.'
    }

    $sql = "SELECT source_row_id FROM lakehouse.olist.$targetTable ORDER BY source_row_number"
    $sha256 = [Security.Cryptography.SHA256]::Create()
    $buffer = [Text.StringBuilder]::new(1048576)
    [long]$rowCount = 0
    try {
        & docker compose --env-file $EnvFile -f $ComposeFile exec -T trino trino `
            --server http://localhost:8080 --catalog lakehouse --schema olist `
            --output-format TSV --execute $sql 2>&1 | ForEach-Object {
            if ($_ -is [Management.Automation.ErrorRecord]) { return }
            $rowId = [string]$_
            if ([string]::IsNullOrWhiteSpace($rowId)) { return }
            if ($rowId -cnotmatch '^[0-9a-f]{64}$') {
                throw 'G2-E Trino row identity stream returned an invalid row ID.'
            }
            [void]$buffer.Append($rowId).Append("`n")
            $rowCount++
            if ($buffer.Length -ge 1048576) {
                $bytes = [Text.Encoding]::UTF8.GetBytes($buffer.ToString())
                [void]$sha256.TransformBlock($bytes, 0, $bytes.Length, $bytes, 0)
                [void]$buffer.Clear()
            }
        }
        $exitCode = $LASTEXITCODE
        if ($exitCode -ne 0) { throw 'G2-E Trino row identity stream failed.' }

        if ($buffer.Length -gt 0) {
            $bytes = [Text.Encoding]::UTF8.GetBytes($buffer.ToString())
            [void]$sha256.TransformBlock($bytes, 0, $bytes.Length, $bytes, 0)
        }
        [void]$sha256.TransformFinalBlock([byte[]]@(), 0, 0)
        $digest = ([BitConverter]::ToString($sha256.Hash)).Replace('-', '').ToLowerInvariant()
    } finally {
        $sha256.Dispose()
    }

    $expectedRows = ConvertTo-G2eCount $Deployment.ExpectedRowCount 'expected row identity count'
    if ($rowCount -ne $expectedRows) { throw 'G2-E Trino row identity stream count differs.' }
    return [pscustomobject][ordered]@{ RowCount = $rowCount; Sha256 = $digest }
}

function Get-G2eSourceState {
    param(
        [Parameter(Mandatory = $true)]$Deployment,
        [Parameter(Mandatory = $true)][string]$ComposeFile,
        [Parameter(Mandatory = $true)][string]$EnvFile,
        [Parameter(Mandatory = $true)][string]$TemplatePath
    )

    $template = Get-Content -LiteralPath $TemplatePath -Raw -Encoding UTF8
    $parts = @(Split-G2eNamedSql (Render-G2eVerificationSql $template $Deployment))
    $existence = Invoke-G2eTrinoStatement $parts[0].Sql $ComposeFile $EnvFile
    $exists = ConvertTo-G2eCount (Get-G2ePropertyValue $existence 'table_exists') 'table existence count'
    if ($exists -eq 0) { return [pscustomobject][ordered]@{ table_exists = 0 } }
    if ($exists -ne 1) { throw 'G2-E table existence query is ambiguous.' }
    $summary = Invoke-G2eTrinoStatement $parts[1].Sql $ComposeFile $EnvFile
    $sequence = Get-G2eTrinoRowIdSequenceEvidence $Deployment $ComposeFile $EnvFile
    $identity = Invoke-G2eTrinoStatement $parts[2].Sql $ComposeFile $EnvFile
    $keys = Invoke-G2eTrinoStatement $parts[3].Sql $ComposeFile $EnvFile
    $snapshot = Invoke-G2eTrinoStatement $parts[4].Sql $ComposeFile $EnvFile
    return [pscustomobject][ordered]@{
        table_exists = 1
        row_count = Get-G2ePropertyValue $summary 'row_count'
        distinct_source_row_id_count = Get-G2ePropertyValue $summary 'distinct_source_row_id_count'
        min_source_row_number = Get-G2ePropertyValue $summary 'min_source_row_number'
        max_source_row_number = Get-G2ePropertyValue $summary 'max_source_row_number'
        source_row_id_sequence_sha256 = $sequence.Sha256
        bundle_count = Get-G2ePropertyValue $identity 'bundle_count'
        source_bundle_sha256 = Get-G2ePropertyValue $identity 'source_bundle_sha256'
        source_file_count = Get-G2ePropertyValue $identity 'source_file_count'
        source_file = Get-G2ePropertyValue $identity 'source_file'
        duplicate_key_count = Get-G2ePropertyValue $keys 'duplicate_key_count'
        snapshot_count = Get-G2ePropertyValue $snapshot 'snapshot_count'
        latest_snapshot_id = Get-G2ePropertyValue $snapshot 'latest_snapshot_id'
        latest_snapshot_committed_at = Get-G2ePropertyValue $snapshot 'latest_snapshot_committed_at'
    }
}

function New-G2eSourceResult {
    param(
        [Parameter(Mandatory = $true)][string]$Status,
        [Parameter(Mandatory = $true)]$Deployment,
        [Parameter(Mandatory = $true)]$State,
        [string]$JobId = '',
        [string]$ReportPath = ''
    )

    return [pscustomobject][ordered]@{
        status = $Status
        entity = $Deployment.Entity
        source_bundle_sha256 = $Deployment.SourceBundleSha256
        target_table = $Deployment.TargetTable
        job_id = if ([string]::IsNullOrWhiteSpace($JobId)) { $null } else { $JobId }
        snapshot_id = if ($State.Kind -ceq 'Verified') { [string]$State.SnapshotId } else { $null }
        report_path = if ([string]::IsNullOrWhiteSpace($ReportPath)) { $null } else { $ReportPath }
    }
}

if ($FunctionsOnly) { return }

if ([string]::IsNullOrWhiteSpace($ManifestPath) -or [string]::IsNullOrWhiteSpace($Entity)) {
    throw 'ManifestPath and Entity are required for G2-E source ingestion.'
}
if ($TimeoutSeconds -lt 1 -or $TimeoutSeconds -gt 3600) { throw 'G2-E timeout must be between 1 and 3600 seconds.' }

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repositoryRoot 'infra/docker-compose.yml'
$envFile = Join-Path $repositoryRoot 'infra/.env.example'
$dataRoot = if (-not [string]::IsNullOrWhiteSpace($env:OLIST_DATA_DIR)) {
    $env:OLIST_DATA_DIR
} else {
    Get-G2eEnvValue $envFile 'OLIST_DATA_DIR'
}
$deployment = Get-G2eRuntimeDeployment $ManifestPath $Entity $dataRoot
Push-Location $repositoryRoot
try {
    $rendered = @(& python -m generators.olist_data render-flink-sql `
        --entity $deployment.Entity --source-bundle-sha256 $deployment.SourceBundleSha256 2>&1)
    $renderExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}
if ($renderExitCode -ne 0) { throw 'Failed to render fixed G2-E ingest SQL.' }
$sql = $rendered -join "`n"
Assert-G2eRenderedSql $sql $deployment

if ($PlanOnly) {
    [ordered]@{
        mode = 'plan-only'
        entity = $deployment.Entity
        source_bundle_sha256 = $deployment.SourceBundleSha256
        pipeline_name = $deployment.PipelineName
        target_table = $deployment.TargetTable
        source_path = $deployment.ContainerSourcePath
    } | ConvertTo-Json -Compress
    return
}

$null = Enable-G2eDockerCli
$null = Invoke-G2eChecked { docker version --format '{{.Server.Version}}' } 'Docker daemon is unavailable for G2-E.'
$startup = Get-G2eServiceStartupPlan
$lakehouseServices = [string[]]$startup.Lakehouse
$flinkServices = [string[]]$startup.Flink
$null = Invoke-G2eChecked {
    docker compose --env-file $envFile -f $composeFile --profile lakehouse up -d @lakehouseServices
} 'Failed to start the G2-E lakehouse dependencies.'
$null = Invoke-G2eChecked {
    docker compose --env-file $envFile -f $composeFile --profile flink up -d --no-deps @flinkServices
} 'Failed to start the bounded G2-E Flink services.'
Wait-G2eHttpReady 'http://localhost:8081/overview' $TimeoutSeconds
Wait-G2eHttpReady 'http://localhost:8088/v1/info' $TimeoutSeconds

$jobs = @(Get-G2eFlinkJobs)
Assert-G2eNoActivePipelineJob $deployment.PipelineName $jobs
$templatePath = Join-Path $repositoryRoot 'jobs/sql/20_olist_source_verify.sql.template'
$observed = Get-G2eSourceState $deployment $composeFile $envFile $templatePath
$state = Assert-G2eSourceState $observed $deployment
$verifyScript = Join-Path $PSScriptRoot 'verify_g2e_olist_source.ps1'
if ($state.Kind -ceq 'Verified') {
    $verificationOutput = @(& $verifyScript -ManifestPath $deployment.ManifestPath `
        -Entity $deployment.Entity -TimeoutSeconds $TimeoutSeconds)
    if ($verificationOutput.Count -lt 1) { throw 'G2-E verifier returned no report.' }
    $verification = $verificationOutput[-1] | ConvertFrom-Json
    New-G2eSourceResult 'already_ingested' $deployment $state '' ([string]$verification.report_path) |
        ConvertTo-Json -Compress
    return
}
if ($state.Kind -notin @('Absent', 'EmptyWithoutSnapshot')) {
    throw "Unsafe existing Olist source state: $($state.Kind)"
}

$beforeIds = [string[]]@($jobs | ForEach-Object { [string]$_.jid })
$relativeSql = "tmp/graduation/g2e/$($deployment.SourceBundleSha256)/submissions/$($deployment.Entity)-$([guid]::NewGuid().ToString('N')).sql"
$sqlPath = Join-Path $repositoryRoot ($relativeSql.Replace('/', [IO.Path]::DirectorySeparatorChar))
$null = New-Item -ItemType Directory -Force -Path (Split-Path $sqlPath -Parent)
[IO.File]::WriteAllText($sqlPath, $sql, [Text.UTF8Encoding]::new($false))
try {
    $containerSql = "/workspace/$relativeSql"
    $null = Invoke-G2eChecked {
        docker compose --env-file $envFile -f $composeFile exec -T flink-sql-client `
            /opt/flink/bin/sql-client.sh -f $containerSql
    } 'G2-E bounded Flink SQL submission failed.'
    $job = Wait-G2eNewFinishedJob $deployment.PipelineName $beforeIds $TimeoutSeconds
    $verificationOutput = @(& $verifyScript -ManifestPath $deployment.ManifestPath `
        -Entity $deployment.Entity -JobId ([string]$job.jid) -TimeoutSeconds $TimeoutSeconds)
    if ($verificationOutput.Count -lt 1) { throw 'G2-E verifier returned no report.' }
    $verification = $verificationOutput[-1] | ConvertFrom-Json
    $finalObserved = Get-G2eSourceState $deployment $composeFile $envFile $templatePath
    $finalState = Assert-G2eSourceState $finalObserved $deployment
    New-G2eSourceResult 'ingested' $deployment $finalState ([string]$job.jid) `
        ([string]$verification.report_path) | ConvertTo-Json -Compress
} finally {
    Remove-Item -LiteralPath $sqlPath -Force -ErrorAction SilentlyContinue
}
