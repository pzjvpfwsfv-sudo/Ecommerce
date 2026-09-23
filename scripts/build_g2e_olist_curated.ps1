[CmdletBinding()]
param(
    [string]$ManifestPath = '',
    [int]$TimeoutSeconds = 600,
    [switch]$FunctionsOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$curatedRequestedManifestPath = $ManifestPath
$curatedRequestedTimeout = $TimeoutSeconds
$curatedFunctionsOnly = [bool]$FunctionsOnly
. (Join-Path $PSScriptRoot 'verify_g2e_olist_source.ps1') -FunctionsOnly
$FunctionsOnly = $curatedFunctionsOnly

function Get-G2eCuratedStageOrder {
    return [string[]]@(
        'customer_dim', 'category_dim', 'product_dim', 'seller_dim', 'geolocation_dim',
        'order_fact', 'order_item_fact', 'payment_fact', 'review_fact'
    )
}

function Get-G2eCuratedRegistry {
    return [ordered]@{
        customer_dim = [pscustomobject]@{
            TargetTable = 'customer_dim_v1'; SourceTable = 'customers_src_v1'; KeyExpression = 'customer_id'
        }
        category_dim = [pscustomobject]@{
            TargetTable = 'category_dim_v1'; SourceTable = 'category_translation_src_v1'; KeyExpression = 'product_category_name'
        }
        product_dim = [pscustomobject]@{
            TargetTable = 'product_dim_v1'; SourceTable = 'products_src_v1'; KeyExpression = 'product_id'
        }
        seller_dim = [pscustomobject]@{
            TargetTable = 'seller_dim_v1'; SourceTable = 'sellers_src_v1'; KeyExpression = 'seller_id'
        }
        geolocation_dim = [pscustomobject]@{
            TargetTable = 'geolocation_dim_v1'; SourceTable = 'geolocation_src_v1'; KeyExpression = 'geolocation_zip_code_prefix'
        }
        order_fact = [pscustomobject]@{
            TargetTable = 'order_fact_v1'; SourceTable = 'orders_src_v1'; KeyExpression = 'order_id'
        }
        order_item_fact = [pscustomobject]@{
            TargetTable = 'order_item_fact_v1'; SourceTable = 'order_items_src_v1'; KeyExpression = 'ROW(order_id, order_item_id)'
        }
        payment_fact = [pscustomobject]@{
            TargetTable = 'payment_fact_v1'; SourceTable = 'order_payments_src_v1'; KeyExpression = 'ROW(order_id, payment_sequential)'
        }
        review_fact = [pscustomobject]@{
            TargetTable = 'review_fact_v1'; SourceTable = 'order_reviews_src_v1'; KeyExpression = 'source_row_id'
        }
    }
}

function Get-G2eMapKeys {
    param([Parameter(Mandatory = $true)]$Map)

    if ($Map -is [Collections.IDictionary]) { return [string[]]@($Map.Keys) }
    return [string[]]@($Map.PSObject.Properties.Name)
}

function Get-G2eMapValue {
    param(
        [Parameter(Mandatory = $true)]$Map,
        [Parameter(Mandatory = $true)][string]$Key
    )

    if ($Map -is [Collections.IDictionary]) {
        if (-not $Map.Contains($Key)) { throw "G2-E snapshot map is missing '$Key'." }
        return $Map[$Key]
    }
    $property = $Map.PSObject.Properties[$Key]
    if ($null -eq $property) { throw "G2-E snapshot map is missing '$Key'." }
    return $property.Value
}

function Assert-G2eExactMapKeys {
    param(
        [Parameter(Mandatory = $true)]$Map,
        [Parameter(Mandatory = $true)][string[]]$ExpectedKeys,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $actual = [string[]](Get-G2eMapKeys $Map)
    [Array]::Sort($actual, [StringComparer]::Ordinal)
    $expected = [string[]]$ExpectedKeys.Clone()
    [Array]::Sort($expected, [StringComparer]::Ordinal)
    if (($actual -join "`n") -cne ($expected -join "`n")) { throw "G2-E $Name keys are not exact." }
}

function Assert-G2ePositiveSnapshotId {
    param(
        [Parameter(Mandatory = $true)]$Value,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $text = [string]$Value
    if ($text -notmatch '^[1-9][0-9]*$') { throw "G2-E $Name must be a positive decimal Snapshot ID." }
    return $text
}

function ConvertTo-G2eCanonicalSnapshotJson {
    param(
        [Parameter(Mandatory = $true)]$Map,
        [Parameter(Mandatory = $true)][string[]]$ExpectedKeys
    )

    Assert-G2eExactMapKeys $Map $ExpectedKeys 'Snapshot map'
    $keys = [string[]]$ExpectedKeys.Clone()
    [Array]::Sort($keys, [StringComparer]::Ordinal)
    $pairs = foreach ($key in $keys) {
        $value = Assert-G2ePositiveSnapshotId (Get-G2eMapValue $Map $key) "$key snapshot"
        '"' + $key + '":"' + $value + '"'
    }
    return '{' + ($pairs -join ',') + '}'
}

function Get-G2eUtf8Sha256 {
    param([Parameter(Mandatory = $true)][string]$Value)

    $algorithm = [Security.Cryptography.SHA256]::Create()
    try { $bytes = $algorithm.ComputeHash([Text.Encoding]::UTF8.GetBytes($Value)) } finally { $algorithm.Dispose() }
    return ([BitConverter]::ToString($bytes)).Replace('-', '').ToLowerInvariant()
}

function Get-G2eSourceSnapshotIdentity {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][object[]]$Reports)

    $sourceRegistry = Get-G2eRegistry
    if ($Reports.Count -ne $sourceRegistry.Count) { throw 'G2-E curated build requires exactly nine source reports.' }
    $seen = [ordered]@{}
    $snapshots = [ordered]@{}
    $rowCounts = [ordered]@{}
    $bundle = $null
    foreach ($report in $Reports) {
        if ([string](Get-G2ePropertyValue $report 'status') -cne 'PASS') { throw 'G2-E source report status is not PASS.' }
        if ([string](Get-G2ePropertyValue $report 'dataset_id') -cne 'olist-brazilian-ecommerce-v2') {
            throw 'G2-E source report dataset identity is invalid.'
        }
        $entity = [string](Get-G2ePropertyValue $report 'entity')
        if (-not $sourceRegistry.Contains($entity) -or $seen.Contains($entity)) {
            throw 'G2-E source reports contain an unknown or duplicate entity.'
        }
        $seen[$entity] = $true
        $spec = $sourceRegistry[$entity]
        if ([string](Get-G2ePropertyValue $report 'target_table') -cne [string]$spec.TargetTable -or
                [string](Get-G2ePropertyValue $report 'source_file') -cne [string]$spec.SourceFile) {
            throw "G2-E source report registry identity is invalid for $entity."
        }
        $reportBundle = Assert-G2eSha256 (Get-G2ePropertyValue $report 'source_bundle_sha256') 'source report bundle'
        if ($null -eq $bundle) { $bundle = $reportBundle } elseif ($reportBundle -cne $bundle) {
            throw 'G2-E source reports mix bundle identities.'
        }
        $expectedRows = ConvertTo-G2eCount (Get-G2ePropertyValue $report 'expected_row_count') "$entity expected rows"
        if ($expectedRows -lt 1) { throw "G2-E source report row count must be positive for $entity." }
        $iceberg = Get-G2ePropertyValue $report 'iceberg'
        $actualRows = ConvertTo-G2eCount (Get-G2ePropertyValue $iceberg 'row_count') "$entity Iceberg rows"
        if ($actualRows -ne $expectedRows) { throw "G2-E source report row count differs for $entity." }
        $snapshot = Assert-G2ePositiveSnapshotId (Get-G2ePropertyValue $iceberg 'snapshot_id') "$entity source"
        if ([string]::IsNullOrWhiteSpace([string](Get-G2ePropertyValue $iceberg 'snapshot_committed_at'))) {
            throw "G2-E source report snapshot time is missing for $entity."
        }
        $null = Assert-G2eSha256 (Get-G2ePropertyValue $report 'normalized_sha256') "$entity normalized digest"
        $null = Assert-G2eSha256 (Get-G2ePropertyValue $report 'source_row_id_sequence_sha256') "$entity row digest"
        $snapshots[[string]$spec.TargetTable] = $snapshot
        $rowCounts[[string]$spec.TargetTable] = $expectedRows
    }
    foreach ($entity in $sourceRegistry.Keys) {
        if (-not $seen.Contains($entity)) { throw "G2-E source report is missing $entity." }
    }
    $tables = [string[]]@($sourceRegistry.Values | ForEach-Object { [string]$_.TargetTable })
    $canonical = ConvertTo-G2eCanonicalSnapshotJson $snapshots $tables
    return [pscustomobject][ordered]@{
        DatasetId = 'olist-brazilian-ecommerce-v2'
        SourceBundleSha256 = $bundle
        SourceSnapshots = $snapshots
        SourceRowCounts = $rowCounts
        SourceSnapshotCanonicalJson = $canonical
        SourceSnapshotSetSha256 = Get-G2eUtf8Sha256 $canonical
    }
}

function Get-G2eCuratedSnapshotTables {
    $registry = Get-G2eCuratedRegistry
    return [string[]]@(Get-G2eCuratedStageOrder | ForEach-Object { [string]$registry[$_].TargetTable })
}

function Get-G2eCuratedTokenMap {
    param(
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)]$CuratedSnapshots,
        [switch]$RequireCompleteCurated
    )

    $sourceRegistry = Get-G2eRegistry
    $sourceTables = [string[]]@($sourceRegistry.Values | ForEach-Object { [string]$_.TargetTable })
    Assert-G2eExactMapKeys $Identity.SourceSnapshots $sourceTables 'source Snapshot map'
    $bundle = Assert-G2eSha256 $Identity.SourceBundleSha256 'source bundle identity'
    $snapshotSet = Assert-G2eSha256 $Identity.SourceSnapshotSetSha256 'source Snapshot-set identity'
    if ($RequireCompleteCurated) {
        Assert-G2eExactMapKeys $CuratedSnapshots (Get-G2eCuratedSnapshotTables) 'curated Snapshot map'
    } else {
        $allowed = Get-G2eCuratedSnapshotTables
        foreach ($key in Get-G2eMapKeys $CuratedSnapshots) {
            if ($allowed -cnotcontains $key) { throw 'G2-E curated Snapshot map contains an unknown table.' }
        }
    }

    $tokens = [ordered]@{
        '__SOURCE_BUNDLE_SHA256__' = $bundle
        '__SOURCE_SNAPSHOT_SET_SHA256__' = $snapshotSet
    }
    foreach ($entity in $sourceRegistry.Keys) {
        $token = '__' + $entity.ToUpperInvariant() + '_SRC_SNAPSHOT__'
        $table = [string]$sourceRegistry[$entity].TargetTable
        $tokens[$token] = Assert-G2ePositiveSnapshotId (Get-G2eMapValue $Identity.SourceSnapshots $table) "$table source"
    }
    $curatedRegistry = Get-G2eCuratedRegistry
    foreach ($stage in Get-G2eCuratedStageOrder) {
        $table = [string]$curatedRegistry[$stage].TargetTable
        if ((Get-G2eMapKeys $CuratedSnapshots) -ccontains $table) {
            $token = '__' + $stage.ToUpperInvariant() + '_SNAPSHOT__'
            $tokens[$token] = Assert-G2ePositiveSnapshotId (Get-G2eMapValue $CuratedSnapshots $table) "$table curated"
        }
    }
    return $tokens
}

function Render-G2eCuratedSqlCore {
    param(
        [Parameter(Mandatory = $true)][string]$Template,
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)]$CuratedSnapshots,
        [switch]$RequireCompleteCurated
    )

    $tokens = Get-G2eCuratedTokenMap $Identity $CuratedSnapshots -RequireCompleteCurated:$RequireCompleteCurated
    $known = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($key in $tokens.Keys) { $null = $known.Add([string]$key) }
    foreach ($stage in Get-G2eCuratedStageOrder) { $null = $known.Add('__' + $stage.ToUpperInvariant() + '_SNAPSHOT__') }
    foreach ($match in [regex]::Matches($Template, '__[A-Z0-9_]+__')) {
        if (-not $known.Contains($match.Value)) { throw "Unknown G2-E curated SQL token: $($match.Value)" }
    }
    $sql = $Template
    foreach ($key in $tokens.Keys) { $sql = $sql.Replace([string]$key, [string]$tokens[$key]) }
    if ($sql -cmatch '__[A-Z0-9_]+__') { throw 'G2-E curated SQL has an unresolved Snapshot token.' }
    if ($sql -match '(?im)^\s*(DROP|DELETE|TRUNCATE|CREATE\s+OR\s+REPLACE)\b') {
        throw 'G2-E curated SQL contains a destructive operation.'
    }
    return $sql
}

function Render-G2eCuratedModel {
    param(
        [Parameter(Mandatory = $true)][string]$Template,
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)]$CuratedSnapshots
    )
    return Render-G2eCuratedSqlCore $Template $Identity $CuratedSnapshots -RequireCompleteCurated
}

function Split-G2eCuratedStages {
    param([Parameter(Mandatory = $true)][string]$Sql)

    $pattern = '(?ms)^-- stage:([a-z_]+)\s*\r?\n(.*?;)\s*(?=^-- stage:|\z)'
    $matches = [regex]::Matches($Sql, $pattern)
    $expected = Get-G2eCuratedStageOrder
    if ($matches.Count -ne $expected.Count) { throw 'G2-E curated model must contain exactly nine stages.' }
    $result = for ($index = 0; $index -lt $matches.Count; $index++) {
        $name = $matches[$index].Groups[1].Value
        if ($name -cne $expected[$index]) { throw 'G2-E curated stage order is invalid.' }
        [pscustomobject][ordered]@{ Name = $name; Sql = $matches[$index].Groups[2].Value.Trim() }
    }
    return $result
}

function Split-G2eCuratedVerificationSql {
    param([Parameter(Mandatory = $true)][string]$Sql)

    $pattern = '(?ms)^-- result:([a-z_]+)\s*\r?\n(.*?;)\s*(?=^-- result:|\z)'
    $matches = [regex]::Matches($Sql, $pattern)
    $expected = [string[]]@('hard_gate', 'reportable_quality', 'grain_reconciliation', 'anti_fanout', 'snapshot_identity')
    if ($matches.Count -ne $expected.Count) { throw 'G2-E curated verification must contain exactly five results.' }
    $result = for ($index = 0; $index -lt $matches.Count; $index++) {
        $name = $matches[$index].Groups[1].Value
        if ($name -cne $expected[$index]) { throw 'G2-E curated verification result order is invalid.' }
        [pscustomobject][ordered]@{ Name = $name; Sql = $matches[$index].Groups[2].Value.Trim() }
    }
    return $result
}

function Get-G2eHardGateFields {
    return [string[]]@(
        'duplicate_order_key_count', 'duplicate_customer_key_count', 'duplicate_product_key_count',
        'duplicate_seller_key_count', 'duplicate_category_key_count', 'duplicate_item_key_count',
        'duplicate_payment_key_count', 'orphan_order_customer_count', 'orphan_item_order_count',
        'orphan_item_product_count', 'orphan_item_seller_count', 'orphan_payment_order_count',
        'orphan_review_order_count', 'blank_required_id_count', 'mixed_bundle_table_count',
        'negative_amount_count', 'invalid_purchase_time_count', 'invalid_optional_time_count'
    )
}

function Get-G2eReportableQualityFields {
    return [string[]]@(
        'duplicate_review_id_count', 'unknown_order_status_count', 'unknown_payment_type_count',
        'missing_product_category_count', 'missing_category_translation_count', 'multi_review_order_count',
        'missing_optional_time_count', 'lifecycle_order_anomaly_count', 'payment_item_total_mismatch_count'
    )
}

function Assert-G2eZeroHardGate {
    param([Parameter(Mandatory = $true)]$Row)

    foreach ($name in Get-G2eHardGateFields) {
        if ((ConvertTo-G2eCount (Get-G2ePropertyValue $Row $name) $name) -ne 0) {
            throw "G2-E hard gate failed: $name"
        }
    }
    return $Row
}

function Assert-G2eReportableQuality {
    param([Parameter(Mandatory = $true)]$Row)

    [long]$total = 0
    foreach ($name in Get-G2eReportableQualityFields) {
        $count = ConvertTo-G2eCount (Get-G2ePropertyValue $Row $name) $name
        if ($count -gt ([long]::MaxValue - $total)) { throw 'G2-E reportable quality total exceeds Int64.' }
        $total += $count
    }
    return [pscustomobject][ordered]@{ TotalReportableCount = $total; Counts = $Row }
}

function ConvertTo-G2eDecimal {
    param(
        [Parameter(Mandatory = $true)]$Value,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    try {
        $number = [decimal]::Parse($text, [Globalization.NumberStyles]::Number, [Globalization.CultureInfo]::InvariantCulture)
    } catch { throw "G2-E $Name is not a canonical decimal." }
    if ($number -lt 0) { throw "G2-E $Name cannot be negative." }
    return $number
}

function Assert-G2eAntiFanout {
    param([Parameter(Mandatory = $true)]$Row)

    $pairs = @(
        @('source_order_count', 'fact_order_count'), @('source_item_count', 'fact_item_count'),
        @('source_payment_count', 'fact_payment_count'), @('source_review_count', 'fact_review_count')
    )
    $evidence = [ordered]@{}
    foreach ($pair in $pairs) {
        $source = ConvertTo-G2eCount (Get-G2ePropertyValue $Row $pair[0]) $pair[0]
        $fact = ConvertTo-G2eCount (Get-G2ePropertyValue $Row $pair[1]) $pair[1]
        if ($source -ne $fact) { throw "G2-E anti-fanout row count differs: $($pair[0])." }
        $evidence[$pair[0]] = $source
        $evidence[$pair[1]] = $fact
    }
    foreach ($prefix in @('item_value', 'freight_value', 'payment_value')) {
        $source = ConvertTo-G2eDecimal (Get-G2ePropertyValue $Row "source_${prefix}_sum") "source ${prefix} sum"
        $fact = ConvertTo-G2eDecimal (Get-G2ePropertyValue $Row "fact_${prefix}_sum") "fact ${prefix} sum"
        if ($source -ne $fact) { throw "G2-E anti-fanout amount differs: $prefix." }
        $evidence["source_${prefix}_sum"] = $source.ToString('0.00', [Globalization.CultureInfo]::InvariantCulture)
        $evidence["fact_${prefix}_sum"] = $fact.ToString('0.00', [Globalization.CultureInfo]::InvariantCulture)
    }
    return [pscustomobject]$evidence
}

function Assert-G2eGrainReconciliation {
    param([Parameter(Mandatory = $true)]$Row)

    foreach ($stage in Get-G2eCuratedStageOrder) {
        $expected = ConvertTo-G2eCount (Get-G2ePropertyValue $Row "${stage}_expected_count") "$stage expected count"
        $actual = ConvertTo-G2eCount (Get-G2ePropertyValue $Row "${stage}_row_count") "$stage row count"
        $distinct = ConvertTo-G2eCount (Get-G2ePropertyValue $Row "${stage}_distinct_key_count") "$stage distinct key count"
        if ($actual -ne $expected -or $distinct -ne $actual) { throw "G2-E curated grain differs for $stage." }
    }
    return $Row
}

function Assert-G2eCuratedStageState {
    param(
        [Parameter(Mandatory = $true)]$Observed,
        [Parameter(Mandatory = $true)]$Expected
    )

    $exists = ConvertTo-G2eCount (Get-G2ePropertyValue $Observed 'table_exists') 'curated table existence count'
    if ($exists -eq 0) { return [pscustomobject][ordered]@{ Kind = 'Absent' } }
    if ($exists -ne 1) { throw 'Unsafe existing Olist curated state: table existence is ambiguous.' }
    $rows = ConvertTo-G2eCount (Get-G2ePropertyValue $Observed 'row_count') 'curated row count'
    $distinct = ConvertTo-G2eCount (Get-G2ePropertyValue $Observed 'distinct_key_count') 'curated distinct key count'
    $bundleCount = ConvertTo-G2eCount (Get-G2ePropertyValue $Observed 'bundle_count') 'curated bundle count'
    $snapshotSetCount = ConvertTo-G2eCount (Get-G2ePropertyValue $Observed 'snapshot_set_count') 'curated source-set count'
    $snapshotCount = ConvertTo-G2eCount (Get-G2ePropertyValue $Observed 'snapshot_count') 'curated snapshot count'
    if ($rows -eq 0) {
        if ($distinct -eq 0 -and $bundleCount -eq 0 -and $snapshotSetCount -eq 0 -and $snapshotCount -eq 0 -and
                [string]::IsNullOrWhiteSpace([string](Get-G2ePropertyValue $Observed 'source_bundle_sha256')) -and
                [string]::IsNullOrWhiteSpace([string](Get-G2ePropertyValue $Observed 'source_snapshot_set_sha256')) -and
                [string]::IsNullOrWhiteSpace([string](Get-G2ePropertyValue $Observed 'latest_snapshot_id'))) {
            return [pscustomobject][ordered]@{ Kind = 'EmptyWithoutSnapshot' }
        }
        throw 'Unsafe existing Olist curated state: contradictory empty table.'
    }
    $expectedRows = ConvertTo-G2eCount $Expected.ExpectedRowCount 'expected curated row count'
    if ($rows -ne $expectedRows -or $distinct -ne $rows) { throw 'Unsafe existing Olist curated state: grain differs.' }
    if ($bundleCount -ne 1 -or [string](Get-G2ePropertyValue $Observed 'source_bundle_sha256') -cne [string]$Expected.SourceBundleSha256) {
        throw 'Unsafe existing Olist curated state: bundle differs.'
    }
    if ($snapshotSetCount -ne 1 -or [string](Get-G2ePropertyValue $Observed 'source_snapshot_set_sha256') -cne [string]$Expected.SourceSnapshotSetSha256) {
        throw 'Unsafe existing Olist curated state: source Snapshot set differs.'
    }
    $latest = Assert-G2ePositiveSnapshotId (Get-G2ePropertyValue $Observed 'latest_snapshot_id') 'curated latest'
    if ($snapshotCount -lt 1) { throw 'Unsafe existing Olist curated state: no committed Snapshot.' }
    $recorded = [string]$Expected.RecordedSnapshotId
    if (-not [string]::IsNullOrWhiteSpace($recorded) -and $latest -cne (Assert-G2ePositiveSnapshotId $recorded 'recorded curated')) {
        throw 'Unsafe existing Olist curated state: latest Snapshot differs from its report.'
    }
    return [pscustomobject][ordered]@{ Kind = 'Verified'; RowCount = $rows; SnapshotId = $latest }
}

function Get-G2eSourceReportsFromDisk {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string]$Bundle
    )

    $reports = @()
    foreach ($entity in (Get-G2eRegistry).Keys) {
        $path = Join-Path $RepositoryRoot "tmp/graduation/g2e/$Bundle/sources/$entity.json"
        if (-not [IO.File]::Exists($path)) { throw "G2-E source verification report is missing for $entity." }
        try { $reports += Get-Content -LiteralPath $path -Raw -Encoding UTF8 | ConvertFrom-Json } catch {
            throw "G2-E source verification report is malformed for $entity."
        }
    }
    return $reports
}

function Get-G2eCuratedSourceContext {
    param(
        [Parameter(Mandatory = $true)][string]$ManifestPath,
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $dataRoot = if (-not [string]::IsNullOrWhiteSpace($env:OLIST_DATA_DIR)) { $env:OLIST_DATA_DIR } else { Get-G2eEnvValue $EnvFile 'OLIST_DATA_DIR' }
    $manifestDeployment = Get-G2eRuntimeDeployment $ManifestPath 'orders' $dataRoot
    $reports = @(Get-G2eSourceReportsFromDisk $RepositoryRoot $manifestDeployment.SourceBundleSha256)
    $identity = Get-G2eSourceSnapshotIdentity $reports
    if ($identity.SourceBundleSha256 -cne $manifestDeployment.SourceBundleSha256) {
        throw 'G2-E source reports do not match the requested manifest.'
    }
    $identity | Add-Member -NotePropertyName ManifestPath -NotePropertyValue $manifestDeployment.ManifestPath
    return $identity
}

function Get-G2eCuratedStageReportPath {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string]$Bundle,
        [Parameter(Mandatory = $true)][string]$Stage
    )
    if ((Get-G2eCuratedStageOrder) -cnotcontains $Stage) { throw 'Unknown G2-E curated stage.' }
    return Join-Path $RepositoryRoot "tmp/graduation/g2e/$Bundle/curated/stages/$Stage.json"
}

function Assert-G2eCuratedStageReport {
    param(
        [Parameter(Mandatory = $true)]$Report,
        [Parameter(Mandatory = $true)][string]$Stage,
        [Parameter(Mandatory = $true)]$Identity
    )

    $registry = Get-G2eCuratedRegistry
    if (-not $registry.Contains($Stage)) { throw 'Unknown G2-E curated stage report.' }
    if ([string](Get-G2ePropertyValue $Report 'status') -cne 'PASS' -or
            [string](Get-G2ePropertyValue $Report 'stage') -cne $Stage -or
            [string](Get-G2ePropertyValue $Report 'target_table') -cne [string]$registry[$Stage].TargetTable -or
            [string](Get-G2ePropertyValue $Report 'source_bundle_sha256') -cne [string]$Identity.SourceBundleSha256 -or
            [string](Get-G2ePropertyValue $Report 'source_snapshot_set_sha256') -cne [string]$Identity.SourceSnapshotSetSha256) {
        throw "G2-E curated stage report identity differs for $Stage."
    }
    $sourceTables = [string[]]@((Get-G2eRegistry).Values | ForEach-Object { [string]$_.TargetTable })
    $reportedMap = Get-G2ePropertyValue $Report 'source_snapshots'
    $reportedJson = ConvertTo-G2eCanonicalSnapshotJson $reportedMap $sourceTables
    $expectedJson = ConvertTo-G2eCanonicalSnapshotJson $Identity.SourceSnapshots $sourceTables
    if ($reportedJson -cne $expectedJson) { throw "G2-E curated stage source Snapshot map differs for $Stage." }
    $rows = ConvertTo-G2eCount (Get-G2ePropertyValue $Report 'row_count') "$Stage report rows"
    if ($rows -lt 1) { throw "G2-E curated stage report row count must be positive for $Stage." }
    $null = Assert-G2ePositiveSnapshotId (Get-G2ePropertyValue $Report 'snapshot_id') "$Stage report"
    return $Report
}

function Get-G2eRecordedCuratedStage {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Stage,
        [Parameter(Mandatory = $true)]$Identity
    )

    if (-not [IO.File]::Exists($Path)) { return $null }
    try { $report = Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json } catch { throw "G2-E curated stage report is malformed for $Stage." }
    return Assert-G2eCuratedStageReport $report $Stage $Identity
}

function Get-G2eCuratedStageState {
    param(
        [Parameter(Mandatory = $true)][string]$Stage,
        [Parameter(Mandatory = $true)][string]$ComposeFile,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $registry = Get-G2eCuratedRegistry
    if (-not $registry.Contains($Stage)) { throw 'Unknown G2-E curated stage.' }
    $spec = $registry[$Stage]
    $table = [string]$spec.TargetTable
    $exists = Invoke-G2eTrinoStatement "SELECT count(*) AS table_exists FROM lakehouse.information_schema.tables WHERE table_schema = 'olist' AND table_name = '$table'" $ComposeFile $EnvFile
    if ((ConvertTo-G2eCount (Get-G2ePropertyValue $exists 'table_exists') 'curated table existence') -eq 0) {
        return [pscustomobject][ordered]@{ table_exists = 0 }
    }
    $summarySql = "SELECT count(*) AS row_count, count(DISTINCT $($spec.KeyExpression)) AS distinct_key_count, count(DISTINCT source_bundle_sha256) AS bundle_count, min(source_bundle_sha256) AS source_bundle_sha256, count(DISTINCT source_snapshot_set_sha256) AS snapshot_set_count, min(source_snapshot_set_sha256) AS source_snapshot_set_sha256 FROM lakehouse.olist.$table"
    $summary = Invoke-G2eTrinoStatement $summarySql $ComposeFile $EnvFile
    $snapshot = Invoke-G2eTrinoStatement "SELECT count(*) AS snapshot_count, cast(max_by(snapshot_id, committed_at) AS varchar) AS latest_snapshot_id FROM lakehouse.olist.`"$table`$snapshots`"" $ComposeFile $EnvFile
    return [pscustomobject][ordered]@{
        table_exists = 1
        row_count = Get-G2ePropertyValue $summary 'row_count'
        distinct_key_count = Get-G2ePropertyValue $summary 'distinct_key_count'
        bundle_count = Get-G2ePropertyValue $summary 'bundle_count'
        source_bundle_sha256 = Get-G2ePropertyValue $summary 'source_bundle_sha256'
        snapshot_set_count = Get-G2ePropertyValue $summary 'snapshot_set_count'
        source_snapshot_set_sha256 = Get-G2ePropertyValue $summary 'source_snapshot_set_sha256'
        snapshot_count = Get-G2ePropertyValue $snapshot 'snapshot_count'
        latest_snapshot_id = Get-G2ePropertyValue $snapshot 'latest_snapshot_id'
    }
}

function Invoke-G2eTrinoCommand {
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [Parameter(Mandatory = $true)][string]$ComposeFile,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $output = @(& docker compose --env-file $EnvFile -f $ComposeFile exec -T trino trino `
        --server http://localhost:8080 --catalog lakehouse --schema olist --execute $Sql 2>&1)
    if ($LASTEXITCODE -ne 0) { throw 'G2-E curated Trino statement failed.' }
    return $output
}

function Get-G2eCuratedExpectedRowCount {
    param(
        [Parameter(Mandatory = $true)][string]$Stage,
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)][string]$ComposeFile,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $registry = Get-G2eCuratedRegistry
    $sourceTable = [string]$registry[$Stage].SourceTable
    if ($Stage -cne 'geolocation_dim') { return [long](Get-G2eMapValue $Identity.SourceRowCounts $sourceTable) }
    $snapshot = Get-G2eMapValue $Identity.SourceSnapshots 'geolocation_src_v1'
    $sql = "SELECT count(DISTINCT geolocation_zip_code_prefix) AS expected_count FROM lakehouse.olist.geolocation_src_v1 FOR VERSION AS OF $snapshot WHERE source_bundle_sha256 = '$($Identity.SourceBundleSha256)'"
    $row = Invoke-G2eTrinoStatement $sql $ComposeFile $EnvFile
    return ConvertTo-G2eCount (Get-G2ePropertyValue $row 'expected_count') 'geolocation expected row count'
}

function Wait-G2eCuratedStageExact {
    param(
        [Parameter(Mandatory = $true)][string]$Stage,
        [Parameter(Mandatory = $true)]$Expected,
        [Parameter(Mandatory = $true)][string]$ComposeFile,
        [Parameter(Mandatory = $true)][string]$EnvFile,
        [int]$TimeoutSeconds = 600
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastError = ''
    do {
        try {
            $observed = Get-G2eCuratedStageState $Stage $ComposeFile $EnvFile
            $state = Assert-G2eCuratedStageState $observed $Expected
            if ($state.Kind -ceq 'Verified') { return $state }
            $lastError = "state=$($state.Kind)"
        } catch { $lastError = $_.Exception.Message }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "Timed out waiting for exact G2-E curated stage $Stage. Last error: $lastError"
}

if ($FunctionsOnly) { return }

if ([string]::IsNullOrWhiteSpace($curatedRequestedManifestPath)) { throw 'ManifestPath is required for G2-E curated build.' }
if ($curatedRequestedTimeout -lt 1 -or $curatedRequestedTimeout -gt 3600) { throw 'G2-E curated timeout must be between 1 and 3600 seconds.' }

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repositoryRoot 'infra/docker-compose.yml'
$envFile = Join-Path $repositoryRoot 'infra/.env.example'
$identity = Get-G2eCuratedSourceContext $curatedRequestedManifestPath $repositoryRoot $envFile
$null = Enable-G2eDockerCli
$null = Invoke-G2eChecked { docker version --format '{{.Server.Version}}' } 'Docker daemon is unavailable for G2-E curated build.'
$startup = Get-G2eServiceStartupPlan
$lakehouseServices = [string[]]$startup.Lakehouse
$null = Invoke-G2eChecked {
    docker compose --env-file $envFile -f $composeFile --profile lakehouse up -d @lakehouseServices
} 'Failed to start G2-E curated lakehouse services.'
Wait-G2eHttpReady 'http://localhost:8088/v1/info' $curatedRequestedTimeout

$verificationTemplatePath = Join-Path $repositoryRoot 'jobs/sql/22_olist_curated_verify.sql.template'
$verificationTemplate = Get-Content -LiteralPath $verificationTemplatePath -Raw -Encoding UTF8
$rawVerificationParts = @(Split-G2eCuratedVerificationSql $verificationTemplate)
$hardGateSql = Render-G2eCuratedSqlCore $rawVerificationParts[0].Sql $identity ([ordered]@{})
$hardGate = Invoke-G2eTrinoStatement $hardGateSql $composeFile $envFile
$null = Assert-G2eZeroHardGate $hardGate

$modelTemplate = Get-Content -LiteralPath (Join-Path $repositoryRoot 'jobs/sql/21_olist_curated_model.sql.template') -Raw -Encoding UTF8
$rawStages = @(Split-G2eCuratedStages $modelTemplate)
$curatedSnapshots = [ordered]@{}
foreach ($stageName in Get-G2eCuratedStageOrder) {
    $spec = (Get-G2eCuratedRegistry)[$stageName]
    $stage = @($rawStages | Where-Object { $_.Name -ceq $stageName })[0]
    $reportPath = Get-G2eCuratedStageReportPath $repositoryRoot $identity.SourceBundleSha256 $stageName
    $recorded = Get-G2eRecordedCuratedStage $reportPath $stageName $identity
    $expectedRows = Get-G2eCuratedExpectedRowCount $stageName $identity $composeFile $envFile
    $expected = [pscustomobject][ordered]@{
        Stage = $stageName
        ExpectedRowCount = $expectedRows
        SourceBundleSha256 = $identity.SourceBundleSha256
        SourceSnapshotSetSha256 = $identity.SourceSnapshotSetSha256
        RecordedSnapshotId = if ($null -eq $recorded) { '' } else { [string]$recorded.snapshot_id }
    }
    $observed = Get-G2eCuratedStageState $stageName $composeFile $envFile
    $state = Assert-G2eCuratedStageState $observed $expected
    if ($state.Kind -ceq 'Verified') {
        if ($null -eq $recorded) { throw "G2-E refuses to adopt unrecorded curated stage $stageName." }
        $curatedSnapshots[[string]$spec.TargetTable] = [string]$state.SnapshotId
        continue
    }
    if ($state.Kind -cnotin @('Absent', 'EmptyWithoutSnapshot')) { throw "Unsafe G2-E curated stage state: $($state.Kind)" }
    if ($state.Kind -ceq 'EmptyWithoutSnapshot') {
        throw "G2-E curated stage $stageName exists empty; atomic CTAS must be inspected instead of overwritten."
    }
    $renderedStage = Render-G2eCuratedSqlCore $stage.Sql $identity $curatedSnapshots
    $null = Invoke-G2eTrinoCommand $renderedStage $composeFile $envFile
    $state = Wait-G2eCuratedStageExact $stageName $expected $composeFile $envFile $curatedRequestedTimeout
    $stageReport = [ordered]@{
        status = 'PASS'
        verified_at = [DateTimeOffset]::UtcNow.ToString('o')
        stage = $stageName
        target_table = [string]$spec.TargetTable
        source_bundle_sha256 = $identity.SourceBundleSha256
        source_snapshot_set_sha256 = $identity.SourceSnapshotSetSha256
        source_snapshots = $identity.SourceSnapshots
        row_count = $state.RowCount
        snapshot_id = $state.SnapshotId
    }
    $null = Write-G2eSourceReport $reportPath $stageReport
    $curatedSnapshots[[string]$spec.TargetTable] = [string]$state.SnapshotId
}

$verifyScript = Join-Path $PSScriptRoot 'verify_g2e_olist_curated.ps1'
$verificationOutput = @(& $verifyScript -ManifestPath $identity.ManifestPath -TimeoutSeconds $curatedRequestedTimeout)
if ($verificationOutput.Count -lt 1) { throw 'G2-E curated verifier returned no report.' }
$verificationOutput[-1]
