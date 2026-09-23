[CmdletBinding()]
param(
    [string]$ManifestPath = '',
    [int]$TimeoutSeconds = 300,
    [switch]$FunctionsOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$refreshRequestedManifestPath = $ManifestPath
$refreshRequestedTimeout = $TimeoutSeconds
$refreshFunctionsOnly = [bool]$FunctionsOnly
. (Join-Path $PSScriptRoot 'verify_g2e_olist_curated.ps1') -FunctionsOnly
$FunctionsOnly = $refreshFunctionsOnly
Import-Module (Join-Path $PSScriptRoot 'lib/G2e.OrderMetrics.psm1') -Force

$script:G2eProjectRoot = Split-Path $PSScriptRoot -Parent
$script:G2eComposeFile = Join-Path $script:G2eProjectRoot 'infra/docker-compose.yml'
$script:G2eMetricSqlPath = Join-Path $script:G2eProjectRoot 'jobs/sql/23_g2e_order_metrics.sql.template'
$script:G2eMetricDdlPath = Join-Path $script:G2eProjectRoot 'infra/compose/doris/init/03_create_order_metrics.sql'
$script:G2eDockerExecutable = $null
$script:G2eDorisStreamLoadUrl = 'http://localhost:8040'
$script:G2eDorisUsername = 'root'
$script:G2eDorisPassword = ''
$script:G2eMetricFamilies = [string[]]@('overview', 'delivery', 'payment', 'ranking', 'review', 'quality')
$script:G2eTargetSpecs = [ordered]@{
    overview = [pscustomobject][ordered]@{
        Name = 'overview'; Table = 'order_metric_overview'
        Columns = [string[]]@(
            'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
            'window_end', 'order_count', 'delivered_order_count', 'canceled_order_count',
            'unavailable_order_count', 'status_eligible_order_count', 'status_excluded_order_count',
            'delivered_rate', 'canceled_rate', 'unique_customer_count', 'repeat_customer_count',
            'repeat_customer_rate', 'item_row_count', 'item_value_sum', 'freight_value_sum',
            'payment_value_sum', 'items_per_order_avg'
        )
        UniqueKeyColumns = [string[]]@('metric_run_id', 'window_type', 'window_start')
    }
    delivery = [pscustomobject][ordered]@{
        Name = 'delivery'; Table = 'order_metric_delivery'
        Columns = [string[]]@(
            'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
            'window_end', 'delivery_eligible_order_count', 'delivery_excluded_order_count',
            'delivery_days_avg', 'delivery_days_p50', 'delivery_days_p90',
            'late_delivery_order_count', 'late_delivery_eligible_order_count',
            'late_delivery_excluded_order_count', 'late_delivery_rate'
        )
        UniqueKeyColumns = [string[]]@('metric_run_id', 'window_type', 'window_start')
    }
    payment = [pscustomobject][ordered]@{
        Name = 'payment'; Table = 'order_metric_payment'
        Columns = [string[]]@(
            'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
            'window_end', 'payment_type', 'is_all', 'global_order_count', 'payment_order_count',
            'payment_row_count', 'installment_order_count', 'payment_type_order_count',
            'payment_type_value_sum'
        )
        UniqueKeyColumns = [string[]]@(
            'metric_run_id', 'window_type', 'window_start', 'payment_type'
        )
    }
    ranking = [pscustomobject][ordered]@{
        Name = 'ranking'; Table = 'order_metric_ranking'
        Columns = [string[]]@(
            'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
            'window_end', 'dimension_type', 'dimension_id', 'dimension_name', 'is_unknown',
            'ranking_order_count', 'ranking_item_row_count', 'ranking_customer_count',
            'ranking_item_value_sum', 'ranking_freight_value_sum', 'ranking_payment_value_sum',
            'ranking_late_delivery_order_count', 'ranking_late_delivery_eligible_order_count',
            'ranking_late_delivery_rate', 'payment_value_is_additive'
        )
        UniqueKeyColumns = [string[]]@(
            'metric_run_id', 'window_type', 'window_start', 'dimension_type', 'dimension_id'
        )
    }
    review = [pscustomobject][ordered]@{
        Name = 'review'; Table = 'order_metric_review'
        Columns = [string[]]@(
            'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
            'window_end', 'review_row_count', 'reviewed_order_count', 'all_order_count',
            'review_coverage_rate', 'review_score_avg', 'low_score_order_count',
            'low_score_rate', 'multi_review_order_count'
        )
        UniqueKeyColumns = [string[]]@('metric_run_id', 'window_type', 'window_start')
    }
    quality = [pscustomobject][ordered]@{
        Name = 'quality'; Table = 'order_metric_quality'
        Columns = [string[]]@(
            'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
            'window_end', 'source_row_count', 'iceberg_row_count', 'duplicate_key_count',
            'orphan_key_count', 'invalid_value_count', 'temporal_anomaly_count',
            'amount_comparable_order_count', 'amount_reconciled_order_count',
            'amount_mismatch_order_count', 'amount_reconciliation_rate',
            'payment_item_freight_abs_difference_avg',
            'payment_item_freight_abs_difference_p50',
            'payment_item_freight_abs_difference_p90', 'raw_row_counts_json',
            'normalized_row_counts_json', 'iceberg_row_counts_json', 'normalized_sha256_json',
            'source_snapshots_json', 'curated_snapshots_json', 'fact_reconciliations_json',
            'reportable_quality_json', 'reconciliation_status'
        )
        UniqueKeyColumns = [string[]]@('metric_run_id')
    }
}
$script:G2ePublicationColumns = [string[]]@(
    'metric_run_id', 'dataset_id', 'metric_version', 'source_bundle_sha256',
    'source_snapshots_json', 'curated_snapshots_json', 'implementation_revision',
    'source_order_count', 'window_start', 'window_end', 'calculated_at', 'published_at',
    'overview_row_count', 'overview_sha256', 'delivery_row_count', 'delivery_sha256',
    'payment_row_count', 'payment_sha256', 'ranking_row_count', 'ranking_sha256',
    'review_row_count', 'review_sha256', 'quality_row_count', 'quality_sha256', 'status'
)

function Get-G2eTargetSpec {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet('overview', 'delivery', 'payment', 'ranking', 'review', 'quality')]
        [string]$Family
    )

    return $script:G2eTargetSpecs[$Family]
}

function Get-G2eRefreshPlan {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][string]$MetricRunId)

    if ($MetricRunId -cnotmatch '^orders-v1-b[0-9a-f]{64}$') {
        throw 'G2-E metric run ID is unsafe.'
    }
    $plan = [Collections.Generic.List[object]]::new()
    foreach ($family in $script:G2eMetricFamilies) {
        $spec = Get-G2eTargetSpec -Family $family
        $plan.Add([pscustomobject][ordered]@{ Name = $family; Target = $spec.Table })
    }
    $plan.Add([pscustomobject][ordered]@{
        Name = 'publication'; Target = 'order_metric_publications'
    })
    return @($plan.ToArray())
}

function Get-G2ePathComparison {
    if ([IO.Path]::DirectorySeparatorChar -eq [char]92) {
        return [StringComparison]::OrdinalIgnoreCase
    }
    return [StringComparison]::Ordinal
}

function Test-G2eRefreshPathContained {
    param(
        [Parameter(Mandatory = $true)][string]$RootPath,
        [Parameter(Mandatory = $true)][string]$CandidatePath
    )

    $root = [IO.Path]::GetFullPath($RootPath).TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    )
    $candidate = [IO.Path]::GetFullPath($CandidatePath)
    $comparison = Get-G2ePathComparison
    return $candidate.Equals($root, $comparison) -or $candidate.StartsWith(
        ($root + [IO.Path]::DirectorySeparatorChar),
        $comparison
    )
}

function Assert-G2eRefreshNoReparsePoint {
    param(
        [Parameter(Mandatory = $true)][string]$RootPath,
        [Parameter(Mandatory = $true)][string]$CandidatePath
    )

    $root = [IO.Path]::GetFullPath($RootPath)
    $candidate = [IO.Path]::GetFullPath($CandidatePath)
    if (-not (Test-G2eRefreshPathContained -RootPath $root -CandidatePath $candidate)) {
        throw 'G2-E metric output escapes the fixed workspace root.'
    }
    $current = $candidate
    while (Test-G2eRefreshPathContained -RootPath $root -CandidatePath $current) {
        if ([IO.File]::Exists($current) -or [IO.Directory]::Exists($current)) {
            $item = Get-Item -LiteralPath $current -Force -ErrorAction Stop
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw 'G2-E metric output paths cannot contain a reparse point.'
            }
        }
        if ($current.Equals($root, (Get-G2ePathComparison))) { break }
        $parent = [IO.Path]::GetDirectoryName($current)
        if ([string]::IsNullOrEmpty($parent) -or $parent -ceq $current) { break }
        $current = $parent
    }
    return $candidate
}

function Get-G2eMetricsPaths {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][string]$SourceBundleSha256)

    $bundle = Assert-G2eSha256 $SourceBundleSha256 'metric source bundle'
    $projectRoot = [IO.Path]::GetFullPath($script:G2eProjectRoot)
    if ([IO.Path]::GetPathRoot($projectRoot).ToLowerInvariant() -cne 'd:\') {
        throw 'G2-E metric outputs must remain in the D-drive workspace.'
    }
    $bundleDirectory = [IO.Path]::GetFullPath((
        Join-Path $projectRoot "tmp/graduation/g2e/$bundle"
    ))
    $metricsDirectory = [IO.Path]::GetFullPath((Join-Path $bundleDirectory 'metrics'))
    $reportPath = [IO.Path]::GetFullPath((Join-Path $metricsDirectory 'refresh.json'))
    $lockPath = [IO.Path]::GetFullPath((Join-Path $metricsDirectory '.refresh.lock'))
    $null = Assert-G2eRefreshNoReparsePoint -RootPath $projectRoot -CandidatePath $metricsDirectory
    return [pscustomobject][ordered]@{
        BundleDirectory = $bundleDirectory
        MetricsDirectory = $metricsDirectory
        ReportPath = $reportPath
        LockPath = $lockPath
    }
}

function Initialize-G2eMetricsDirectory {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][string]$SourceBundleSha256)

    $paths = Get-G2eMetricsPaths -SourceBundleSha256 $SourceBundleSha256
    if ([IO.File]::Exists($paths.BundleDirectory) -or [IO.File]::Exists($paths.MetricsDirectory)) {
        throw 'G2-E fixed metric output directory conflicts with an existing file.'
    }
    [IO.Directory]::CreateDirectory($paths.MetricsDirectory) | Out-Null
    return Get-G2eMetricsPaths -SourceBundleSha256 $SourceBundleSha256
}

function Assert-G2eReportPath {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$SourceBundleSha256,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $paths = Get-G2eMetricsPaths -SourceBundleSha256 $SourceBundleSha256
    $candidate = [IO.Path]::GetFullPath($Path)
    if (-not $candidate.Equals($paths.ReportPath, (Get-G2ePathComparison))) {
        throw 'G2-E refresh report must use the fixed metrics/refresh.json path.'
    }
    return Assert-G2eRefreshNoReparsePoint `
        -RootPath $script:G2eProjectRoot -CandidatePath $candidate
}

function Enter-G2eRunLock {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][string]$Path)

    $fullPath = [IO.Path]::GetFullPath($Path)
    $parent = [IO.Path]::GetDirectoryName($fullPath)
    if ([string]::IsNullOrWhiteSpace($parent)) { throw 'G2-E run lock path has no parent.' }
    [IO.Directory]::CreateDirectory($parent) | Out-Null
    try {
        return [IO.File]::Open(
            $fullPath,
            [IO.FileMode]::CreateNew,
            [IO.FileAccess]::Write,
            [IO.FileShare]::None
        )
    } catch {
        throw 'Another G2-E metric refresh owns the fixed run lock.'
    }
}

function Assert-G2eRenderedMetricSql {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][string]$Sql)

    $tokenScan = $Sql.Replace("'__ALL__'", '').Replace("'__UNKNOWN__'", '')
    if ([string]::IsNullOrWhiteSpace($Sql) -or $tokenScan -match '__[A-Z0-9_]+__') {
        throw 'G2-E metric SQL contains an unresolved token.'
    }
    if ($Sql -match '(?im)^\s*(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|REPLACE|CREATE)\b') {
        throw 'G2-E metric calculation SQL must be read-only.'
    }
    return $Sql
}

function Assert-G2eExactEvidenceMap {
    param(
        [Parameter(Mandatory = $true)][Collections.IDictionary]$Evidence,
        [switch]$AllowNull
    )

    if ($Evidence.Count -ne $script:G2eMetricFamilies.Count) {
        throw 'G2-E metric evidence must contain exactly six families.'
    }
    foreach ($family in $script:G2eMetricFamilies) {
        if (-not $Evidence.Contains($family)) { throw 'G2-E metric evidence is incomplete.' }
        $item = $Evidence[$family]
        if ($null -eq $item) {
            if ($AllowNull) { continue }
            throw 'G2-E metric evidence cannot be null.'
        }
        $count = ConvertTo-G2eCount (Get-G2ePropertyValue $item 'RowCount') "$family evidence rows"
        if ($count -lt 1) { throw 'G2-E metric evidence row counts must be positive.' }
        $null = Assert-G2eSha256 (Get-G2ePropertyValue $item 'Sha256') "$family evidence digest"
    }
}

function Test-G2eRefreshProperty {
    param(
        [Parameter(Mandatory = $true)]$Object,
        [Parameter(Mandatory = $true)][string]$Name
    )

    if ($Object -is [Collections.IDictionary]) { return $Object.Contains($Name) }
    return $null -ne $Object.PSObject.Properties[$Name]
}

function ConvertTo-G2ePublicationEvidence {
    param([Parameter(Mandatory = $true)][Collections.IDictionary]$Candidates)

    if ($Candidates.Count -ne $script:G2eMetricFamilies.Count) {
        throw 'G2-E publication candidates must contain exactly six families.'
    }
    $evidence = [ordered]@{}
    foreach ($family in $script:G2eMetricFamilies) {
        if (-not $Candidates.Contains($family) -or $null -eq $Candidates[$family]) {
            throw 'G2-E publication candidates are incomplete.'
        }
        $candidate = $Candidates[$family]
        $hasDigest = Test-G2eRefreshProperty $candidate 'Sha256'
        $hasCanonicalDigest = Test-G2eRefreshProperty $candidate 'CanonicalSha256'
        if (-not $hasDigest -and -not $hasCanonicalDigest) {
            throw "G2-E publication candidate digest is missing for $family."
        }
        $digest = if ($hasDigest) {
            [string](Get-G2ePropertyValue $candidate 'Sha256')
        } else {
            [string](Get-G2ePropertyValue $candidate 'CanonicalSha256')
        }
        if ($hasDigest -and $hasCanonicalDigest -and
                $digest -cne [string](Get-G2ePropertyValue $candidate 'CanonicalSha256')) {
            throw "G2-E publication candidate digests disagree for $family."
        }
        $evidence[$family] = [pscustomobject][ordered]@{
            RowCount = Get-G2ePropertyValue $candidate 'RowCount'
            Sha256 = $digest
        }
    }
    Assert-G2eExactEvidenceMap -Evidence $evidence
    return $evidence
}

function Assert-G2eStoredIdentity {
    param(
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)][object[]]$Rows,
        [Parameter(Mandatory = $true)][string]$Family
    )

    if ($Rows.Count -eq 0) { throw "G2-E stored $Family rows are empty." }
    foreach ($row in $Rows) {
        if ([string](Get-G2ePropertyValue $row 'metric_run_id') -cne [string]$Identity.MetricRunId -or
                [string](Get-G2ePropertyValue $row 'dataset_id') -cne [string]$Identity.DatasetId -or
                [string](Get-G2ePropertyValue $row 'metric_version') -cne [string]$Identity.MetricVersion) {
            throw "G2-E stored $Family row identity does not match the metric identity."
        }
    }
}

function Assert-G2eStoredCandidate {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet('overview', 'delivery', 'payment', 'ranking', 'review', 'quality')]
        [string]$Family,
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$CandidateRows,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$StoredRows
    )

    $spec = Get-G2eTargetSpec -Family $Family
    if ($CandidateRows.Count -eq 0 -or $StoredRows.Count -ne $CandidateRows.Count) {
        throw "G2-E stored row count mismatch for $($spec.Table)."
    }
    Assert-G2eStoredIdentity -Identity $Identity -Rows $StoredRows -Family $Family
    $candidateDigest = Get-G2eCanonicalDigest -Rows $CandidateRows `
        -Columns $spec.Columns -UniqueKeyColumns $spec.UniqueKeyColumns
    $storedDigest = Get-G2eCanonicalDigest -Rows $StoredRows `
        -Columns $spec.Columns -UniqueKeyColumns $spec.UniqueKeyColumns
    if ($storedDigest -cne $candidateDigest) {
        throw "G2-E stored digest mismatch for $($spec.Table)."
    }
    return [pscustomobject][ordered]@{
        RowCount = [long]$StoredRows.Count
        Sha256 = $storedDigest
    }
}

function Assert-G2eStreamLoadResponse {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Response,
        [Parameter(Mandatory = $true)][long]$ExpectedRows,
        [Parameter(Mandatory = $true)][string]$TableName
    )

    $status = [string](Get-G2ePropertyValue $Response 'Status')
    $loaded = ConvertTo-G2eCount (Get-G2ePropertyValue $Response 'NumberLoadedRows') 'loaded rows'
    $filtered = ConvertTo-G2eCount (Get-G2ePropertyValue $Response 'NumberFilteredRows') 'filtered rows'
    if ($status -cne 'Success' -or $loaded -ne $ExpectedRows -or $filtered -ne 0) {
        throw "G2-E Stream Load failed for $TableName."
    }
}

function Assert-G2eUnpublishedCandidateState {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][Collections.IDictionary]$ExpectedEvidence,
        [Parameter(Mandatory = $true)][Collections.IDictionary]$StoredEvidence
    )

    Assert-G2eExactEvidenceMap -Evidence $ExpectedEvidence
    Assert-G2eExactEvidenceMap -Evidence $StoredEvidence -AllowNull
    $reuse = [Collections.Generic.List[string]]::new()
    $load = [Collections.Generic.List[string]]::new()
    foreach ($family in $script:G2eMetricFamilies) {
        $expected = $ExpectedEvidence[$family]
        $stored = $StoredEvidence[$family]
        if ($null -eq $stored) {
            $load.Add($family)
            continue
        }
        if ([string]$stored.RowCount -cne [string]$expected.RowCount -or
                [string]$stored.Sha256 -cne [string]$expected.Sha256) {
            throw "G2-E unpublished candidate evidence differs for $family."
        }
        $reuse.Add($family)
    }
    return [pscustomobject][ordered]@{
        status = 'continue'
        reuse = [string[]]$reuse.ToArray()
        load = [string[]]$load.ToArray()
    }
}

function ConvertTo-G2eRefreshTimestamp {
    param([Parameter(Mandatory = $true)]$Value, [Parameter(Mandatory = $true)][string]$Name)

    $parsed = [datetimeoffset]::MinValue
    if (-not [datetimeoffset]::TryParse(
            [string]$Value,
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::AssumeUniversal,
            [ref]$parsed)) {
        throw "G2-E $Name must be a timestamp."
    }
    return $parsed.ToUniversalTime()
}

function Get-G2eIdentitySnapshotJson {
    param(
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)][ValidateSet('source', 'curated')][string]$Kind
    )

    if ($Kind -ceq 'source') {
        $tables = [string[]]@((Get-G2eRegistry).Values | ForEach-Object { [string]$_.TargetTable })
        return ConvertTo-G2eCanonicalSnapshotJson $Identity.SourceSnapshots $tables
    }
    return ConvertTo-G2eCanonicalSnapshotJson `
        $Identity.CuratedSnapshots (Get-G2eCuratedSnapshotTables)
}

function New-G2ePublicationRecord {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)][Alias('Candidates')][Collections.IDictionary]$Evidence,
        [Parameter(Mandatory = $true)][string]$PublishedAt
    )

    $null = Get-G2eRefreshPlan -MetricRunId ([string]$Identity.MetricRunId)
    $publicationEvidence = ConvertTo-G2ePublicationEvidence -Candidates $Evidence
    $calculated = ConvertTo-G2eRefreshTimestamp $Identity.CalculatedAt 'calculated_at'
    $published = ConvertTo-G2eRefreshTimestamp $PublishedAt 'published_at'
    if ($published -lt $calculated) { throw 'G2-E publication precedes metric calculation.' }
    $record = [ordered]@{
        metric_run_id = [string]$Identity.MetricRunId
        dataset_id = [string]$Identity.DatasetId
        metric_version = [string]$Identity.MetricVersion
        source_bundle_sha256 = [string]$Identity.SourceBundleSha256
        source_snapshots_json = Get-G2eIdentitySnapshotJson $Identity source
        curated_snapshots_json = Get-G2eIdentitySnapshotJson $Identity curated
        implementation_revision = [string]$Identity.ImplementationRevision
        source_order_count = [string]$Identity.SourceOrderCount
        window_start = [string]$Identity.WindowStart
        window_end = [string]$Identity.WindowEnd
        calculated_at = $calculated.ToString(
            "yyyy-MM-dd'T'HH:mm:ss.fff'Z'", [Globalization.CultureInfo]::InvariantCulture
        )
        published_at = $published.ToString(
            "yyyy-MM-dd'T'HH:mm:ss.fff'Z'", [Globalization.CultureInfo]::InvariantCulture
        )
    }
    foreach ($family in $script:G2eMetricFamilies) {
        $record["${family}_row_count"] = [string]$publicationEvidence[$family].RowCount
        $record["${family}_sha256"] = [string]$publicationEvidence[$family].Sha256
    }
    $record.status = 'PUBLISHED'
    return [pscustomobject]$record
}

function Assert-G2eExistingPublication {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)]$Publication,
        [Parameter(Mandatory = $true)][Collections.IDictionary]$StoredEvidence
    )

    Assert-G2eExactEvidenceMap -Evidence $StoredEvidence
    $expected = [ordered]@{
        metric_run_id = [string]$Identity.MetricRunId
        dataset_id = [string]$Identity.DatasetId
        metric_version = [string]$Identity.MetricVersion
        source_bundle_sha256 = [string]$Identity.SourceBundleSha256
        source_snapshots_json = Get-G2eIdentitySnapshotJson $Identity source
        curated_snapshots_json = Get-G2eIdentitySnapshotJson $Identity curated
        implementation_revision = [string]$Identity.ImplementationRevision
        source_order_count = [string]$Identity.SourceOrderCount
        window_start = [string]$Identity.WindowStart
        window_end = [string]$Identity.WindowEnd
        status = 'PUBLISHED'
    }
    foreach ($field in $expected.Keys) {
        if ([string](Get-G2ePropertyValue $Publication $field) -cne [string]$expected[$field]) {
            throw "G2-E existing publication field '$field' does not match."
        }
    }
    $calculated = ConvertTo-G2eRefreshTimestamp `
        (Get-G2ePropertyValue $Publication 'calculated_at') 'calculated_at'
    $published = ConvertTo-G2eRefreshTimestamp `
        (Get-G2ePropertyValue $Publication 'published_at') 'published_at'
    if ($published -lt $calculated) { throw 'G2-E existing publication timestamps are inverted.' }
    foreach ($family in $script:G2eMetricFamilies) {
        $countField = "${family}_row_count"
        $hashField = "${family}_sha256"
        if ([string](Get-G2ePropertyValue $Publication $countField) -cne
                [string]$StoredEvidence[$family].RowCount -or
                [string](Get-G2ePropertyValue $Publication $hashField) -cne
                [string]$StoredEvidence[$family].Sha256) {
            throw "G2-E existing publication evidence differs for $family."
        }
    }
    return [pscustomobject][ordered]@{
        status = 'already_published'
        metric_run_id = [string]$Identity.MetricRunId
    }
}

function ConvertTo-G2eSqlString {
    param([Parameter(Mandatory = $true)][AllowEmptyString()][string]$Value)

    if ($Value.IndexOf([char]0) -ge 0) { throw 'G2-E SQL text contains a NUL character.' }
    return "'" + $Value.Replace("'", "''") + "'"
}

function ConvertTo-G2eDorisTimestampLiteral {
    param([Parameter(Mandatory = $true)]$Value)

    $timestamp = ConvertTo-G2eRefreshTimestamp $Value 'Doris timestamp'
    return ConvertTo-G2eSqlString $timestamp.ToString(
        'yyyy-MM-dd HH:mm:ss.fff', [Globalization.CultureInfo]::InvariantCulture
    )
}

function New-G2ePublicationInsertSql {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)]$Publication)

    if ([string](Get-G2ePropertyValue $Publication 'status') -cne 'PUBLISHED') {
        throw 'G2-E publication status must be PUBLISHED.'
    }
    $values = [ordered]@{}
    $numericFields = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $null = $numericFields.Add('source_order_count')
    foreach ($family in $script:G2eMetricFamilies) { $null = $numericFields.Add("${family}_row_count") }
    $hashFields = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $null = $hashFields.Add('source_bundle_sha256')
    foreach ($family in $script:G2eMetricFamilies) { $null = $hashFields.Add("${family}_sha256") }
    foreach ($field in $script:G2ePublicationColumns) {
        $text = [string](Get-G2ePropertyValue $Publication $field)
        if ($numericFields.Contains($field)) {
            if ($text -notmatch '^[1-9][0-9]*$') { throw "G2-E publication field '$field' is invalid." }
            $values[$field] = $text
        } elseif ($hashFields.Contains($field)) {
            if ($text -cnotmatch '^[0-9a-f]{64}$') { throw "G2-E publication field '$field' is invalid." }
            $values[$field] = ConvertTo-G2eSqlString $text
        } elseif ($field -ceq 'implementation_revision') {
            if ($text -cnotmatch '^[0-9a-f]{40}$') { throw 'G2-E implementation revision is invalid.' }
            $values[$field] = ConvertTo-G2eSqlString $text
        } elseif ($field -in @('calculated_at', 'published_at')) {
            $values[$field] = ConvertTo-G2eDorisTimestampLiteral $text
        } else {
            $values[$field] = ConvertTo-G2eSqlString $text
        }
    }
    $sqlValues = @($script:G2ePublicationColumns | ForEach-Object { $values[$_] }) -join ', '
    return "INSERT INTO analytics.order_metric_publications " +
        "($($script:G2ePublicationColumns -join ', ')) VALUES ($sqlValues);"
}

function Assert-G2ePublicationReadback {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Expected,
        [Parameter(Mandatory = $true)]$Actual
    )

    foreach ($field in $script:G2ePublicationColumns) {
        if ($field -in @('calculated_at', 'published_at')) {
            $expectedTime = ConvertTo-G2eRefreshTimestamp (Get-G2ePropertyValue $Expected $field) $field
            $actualTime = ConvertTo-G2eRefreshTimestamp (Get-G2ePropertyValue $Actual $field) $field
            if ($actualTime -ne $expectedTime) { throw "G2-E publication readback differs for '$field'." }
        } elseif ([string](Get-G2ePropertyValue $Actual $field) -cne
                [string](Get-G2ePropertyValue $Expected $field)) {
            throw "G2-E publication readback differs for '$field'."
        }
    }
    return $Actual
}

function Invoke-G2ePublicationSequence {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][scriptblock]$VerifyAction,
        [Parameter(Mandatory = $true)][scriptblock]$PublishAction
    )

    $evidence = [ordered]@{}
    foreach ($family in $script:G2eMetricFamilies) {
        $evidence[$family] = & $VerifyAction $family
    }
    Assert-G2eExactEvidenceMap -Evidence $evidence
    return & $PublishAction $evidence
}

function Get-G2eRefreshEnvFile {
    $preferred = Join-Path $script:G2eProjectRoot 'infra/.env'
    if ([IO.File]::Exists($preferred)) { return $preferred }
    $example = Join-Path $script:G2eProjectRoot 'infra/.env.example'
    if (-not [IO.File]::Exists($example)) { throw 'G2-E Compose environment file is missing.' }
    return $example
}

function Get-G2eRefreshEnvValue {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Name,
        [switch]$AllowEmpty
    )

    $matches = @(Get-Content -LiteralPath $Path -Encoding UTF8 | Where-Object {
        $_ -match ('^' + [regex]::Escape($Name) + '=')
    })
    if ($matches.Count -ne 1) { throw "G2-E environment file must define $Name exactly once." }
    $value = $matches[0].Substring($Name.Length + 1).Trim()
    if (-not $AllowEmpty -and [string]::IsNullOrWhiteSpace($value)) {
        throw "G2-E environment value $Name is blank."
    }
    return $value
}

function Initialize-G2eDorisCredentials {
    param([Parameter(Mandatory = $true)][string]$EnvFile)

    $script:G2eDorisUsername = Get-G2eRefreshEnvValue $EnvFile 'DORIS_USERNAME'
    $script:G2eDorisPassword = Get-G2eRefreshEnvValue $EnvFile 'DORIS_PASSWORD' -AllowEmpty
}

function Get-G2eDockerExecutable {
    [CmdletBinding()]
    param()

    $command = Get-Command docker -ErrorAction SilentlyContinue
    if ($null -ne $command) { return $command.Source }
    $fallback = 'D:\DockerProgram\Docker\resources\bin\docker.exe'
    if ([IO.File]::Exists($fallback)) { return $fallback }
    throw 'Docker CLI is unavailable for G2-E metric refresh.'
}

function Invoke-G2eComposeCommand {
    param(
        [Parameter(Mandatory = $true)][string]$EnvFile,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    if ([string]::IsNullOrWhiteSpace([string]$script:G2eDockerExecutable)) {
        throw 'G2-E Docker executable was not initialized.'
    }
    $output = @(& $script:G2eDockerExecutable compose --env-file $EnvFile `
        -f $script:G2eComposeFile @Arguments 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "G2-E Docker Compose command failed: $($output -join ' ')"
    }
    return $output
}

function Start-G2eMetricServices {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][string]$EnvFile)

    $services = [string[]]@(
        'minio', 'minio-init', 'metastore-postgres', 'hive-metastore', 'trino',
        'doris-fe', 'doris-be'
    )
    $arguments = [string[]]@(
        '--profile', 'lakehouse', '--profile', 'serving', 'up', '-d'
    ) + $services
    $null = Invoke-G2eComposeCommand -EnvFile $EnvFile -Arguments $arguments
}

function Invoke-G2eMetricTrinoStatement {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Sql,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $allowed = @('live_snapshots', 'metric_window') + $script:G2eMetricFamilies
    if ($allowed -cnotcontains $Name) { throw 'Unknown G2-E metric Trino statement.' }
    $null = Assert-G2eRenderedMetricSql -Sql $Sql
    $arguments = [string[]]@(
        'exec', '-T', '-e', 'TERM=dumb', 'trino', 'trino', '--server', 'http://localhost:8080',
        '--catalog', 'lakehouse', '--schema', 'olist', '--output-format',
        'CSV_HEADER_UNQUOTED', '--execute', $Sql
    )
    $lines = @(Invoke-G2eComposeCommand -EnvFile $EnvFile -Arguments $arguments)
    return @(ConvertFrom-G2eCsv -CsvText ($lines -join "`n"))
}

function Invoke-G2eDorisSql {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [Parameter(Mandatory = $true)][string]$EnvFile,
        [switch]$NoHeaders
    )

    $arguments = [Collections.Generic.List[string]]::new()
    foreach ($item in @(
            'exec', '-T', 'doris-fe', 'mysql', '-h127.0.0.1', '-P9030',
            ("-u$script:G2eDorisUsername"), '--batch')) {
        $arguments.Add($item)
    }
    if (-not [string]::IsNullOrEmpty($script:G2eDorisPassword)) {
        $arguments.Add("--password=$script:G2eDorisPassword")
    }
    if ($NoHeaders) { $arguments.Add('--skip-column-names') }
    $commandArguments = [string[]]$arguments.ToArray()
    $output = @($Sql | & $script:G2eDockerExecutable compose --env-file $EnvFile `
        -f $script:G2eComposeFile @commandArguments 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "G2-E Doris SQL failed: $($output -join ' ')" }
    return [string[]]@($output | ForEach-Object { [string]$_ })
}

function ConvertFrom-G2eMysqlCell {
    param([Parameter(Mandatory = $true)][AllowEmptyString()][string]$Text)

    if ($Text -ceq 'NULL') { return $null }
    $builder = [Text.StringBuilder]::new()
    for ($index = 0; $index -lt $Text.Length; $index++) {
        if ($Text[$index] -ne [char]92 -or $index + 1 -ge $Text.Length) {
            $null = $builder.Append($Text[$index])
            continue
        }
        $index++
        $escaped = $Text[$index]
        if ($escaped -eq 'n') { $null = $builder.Append("`n") }
        elseif ($escaped -eq 'r') { $null = $builder.Append("`r") }
        elseif ($escaped -eq 't') { $null = $builder.Append("`t") }
        elseif ($escaped -eq '0') { $null = $builder.Append([char]0) }
        elseif ($escaped -eq 'Z') { $null = $builder.Append([char]26) }
        else { $null = $builder.Append($escaped) }
    }
    return $builder.ToString()
}

function ConvertFrom-G2eMysqlBatch {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][AllowEmptyCollection()][string[]]$Lines)

    if ($Lines.Count -eq 0) { return @() }
    $headers = [string[]]@($Lines[0].Split([char[]]@("`t"), [StringSplitOptions]::None))
    $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($header in $headers) {
        if ([string]::IsNullOrWhiteSpace($header) -or -not $seen.Add($header)) {
            throw 'G2-E Doris result headers must be unique.'
        }
    }
    $rows = [Collections.Generic.List[object]]::new()
    for ($rowIndex = 1; $rowIndex -lt $Lines.Count; $rowIndex++) {
        $cells = [string[]]@($Lines[$rowIndex].Split(
            [char[]]@("`t"), [StringSplitOptions]::None
        ))
        if ($cells.Count -ne $headers.Count) { throw 'G2-E Doris result column count differs.' }
        $row = [ordered]@{}
        for ($columnIndex = 0; $columnIndex -lt $headers.Count; $columnIndex++) {
            $row[$headers[$columnIndex]] = ConvertFrom-G2eMysqlCell $cells[$columnIndex]
        }
        $rows.Add([pscustomobject]$row)
    }
    return @($rows.ToArray())
}

function Invoke-G2eDorisQuery {
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    return @(ConvertFrom-G2eMysqlBatch -Lines (
        Invoke-G2eDorisSql -Sql $Sql -EnvFile $EnvFile
    ))
}

function Wait-G2eTrinoDependency {
    [CmdletBinding()]
    param(
        [int]$TimeoutSeconds = 300
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $null = Invoke-RestMethod -Method Get -Uri 'http://localhost:8088/v1/info' -TimeoutSec 5
            return
        } catch {
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw 'Timed out waiting for the G2-E Trino dependency.'
}

function Wait-G2eDorisDependency {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$EnvFile,
        [int]$TimeoutSeconds = 300
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $probe = @(Invoke-G2eDorisSql -Sql 'SELECT 1 AS ready;' -EnvFile $EnvFile -NoHeaders)
            if ($probe.Count -eq 1 -and [string]$probe[0] -ceq '1') { return }
        } catch {
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw 'Timed out waiting for the G2-E Doris dependency.'
}

function Get-G2eCleanImplementationRevision {
    [CmdletBinding()]
    param()

    $status = @(& git -C $script:G2eProjectRoot status --short --untracked-files=no 2>&1)
    if ($LASTEXITCODE -ne 0) { throw 'G2-E could not inspect the Git implementation state.' }
    if ($status.Count -ne 0) { throw 'G2-E metric refresh requires a clean tracked Git state.' }
    $revision = (@(& git -C $script:G2eProjectRoot rev-parse HEAD 2>&1) -join '').Trim()
    if ($LASTEXITCODE -ne 0 -or $revision -cnotmatch '^[0-9a-f]{40}$') {
        throw 'G2-E could not resolve a lowercase 40-character implementation revision.'
    }
    return $revision
}

function Get-G2eRefreshEvidence {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ManifestPath,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $dataRoot = if (-not [string]::IsNullOrWhiteSpace($env:OLIST_DATA_DIR)) {
        $env:OLIST_DATA_DIR
    } else {
        Get-G2eEnvValue $EnvFile 'OLIST_DATA_DIR'
    }
    $manifestFile = Assert-G2eManifestPath -ManifestPath $ManifestPath -DataRoot $dataRoot
    try { $manifest = Get-Content -LiteralPath $manifestFile -Raw -Encoding UTF8 | ConvertFrom-Json } catch {
        throw 'G2-E formal manifest is malformed.'
    }
    $deployment = Get-G2eRuntimeDeployment $manifestFile 'orders' $dataRoot
    $sourceReports = @(Get-G2eSourceReportsFromDisk `
        $script:G2eProjectRoot $deployment.SourceBundleSha256)
    $sourceIdentity = Get-G2eSourceSnapshotIdentity $sourceReports
    if ($sourceIdentity.SourceBundleSha256 -cne $deployment.SourceBundleSha256) {
        throw 'G2-E verified source reports differ from the formal manifest.'
    }
    $recorded = Get-G2eCuratedSnapshotsFromReports $script:G2eProjectRoot $sourceIdentity
    $curatedPath = Join-Path $script:G2eProjectRoot `
        "tmp/graduation/g2e/$($deployment.SourceBundleSha256)/curated/verification.json"
    if (-not [IO.File]::Exists($curatedPath)) { throw 'G2-E curated verification report is missing.' }
    try { $curatedReport = Get-Content -LiteralPath $curatedPath -Raw -Encoding UTF8 | ConvertFrom-Json } catch {
        throw 'G2-E curated verification report is malformed.'
    }
    if ([string](Get-G2ePropertyValue $curatedReport 'status') -cne 'PASS') {
        throw 'G2-E curated verification report is not PASS.'
    }
    return [pscustomobject][ordered]@{
        Manifest = $manifest
        ManifestPath = $manifestFile
        SourceReports = $sourceReports
        SourceIdentity = $sourceIdentity
        CuratedReport = $curatedReport
        CuratedSnapshots = $recorded.Snapshots
    }
}

function Get-G2eExpectedSnapshotMap {
    param([Parameter(Mandatory = $true)]$Evidence)

    $expected = [ordered]@{}
    foreach ($table in $Evidence.SourceIdentity.SourceSnapshots.Keys) {
        $expected[[string]$table] = [string]$Evidence.SourceIdentity.SourceSnapshots[$table]
    }
    foreach ($table in $Evidence.CuratedSnapshots.Keys) {
        $expected[[string]$table] = [string]$Evidence.CuratedSnapshots[$table]
    }
    return $expected
}

function Get-G2eLiveSnapshotMap {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Evidence,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $expected = Get-G2eExpectedSnapshotMap $Evidence
    $selects = [Collections.Generic.List[string]]::new()
    foreach ($table in $expected.Keys) {
        if ([string]$table -cnotmatch '^[a-z0-9_]+$') { throw 'G2-E Snapshot table name is unsafe.' }
        $metadataTable = 'lakehouse.olist."{0}$snapshots"' -f $table
        $selects.Add(
            "SELECT '$table' AS table_name, " +
            'CAST(max_by(snapshot_id, committed_at) AS varchar) AS snapshot_id FROM ' +
            $metadataTable
        )
    }
    $rows = @(Invoke-G2eMetricTrinoStatement -Name live_snapshots `
        -Sql (($selects.ToArray()) -join "`nUNION ALL`n") -EnvFile $EnvFile)
    if ($rows.Count -ne $expected.Count) { throw 'G2-E live Snapshot result is incomplete.' }
    $actual = [ordered]@{}
    foreach ($row in $rows) {
        $table = [string](Get-G2ePropertyValue $row 'table_name')
        if (-not $expected.Contains($table) -or $actual.Contains($table)) {
            throw 'G2-E live Snapshot result contains an unknown or duplicate table.'
        }
        $actual[$table] = Assert-G2ePositiveSnapshotId `
            (Get-G2ePropertyValue $row 'snapshot_id') "$table live"
    }
    foreach ($table in $expected.Keys) {
        if (-not $actual.Contains($table) -or [string]$actual[$table] -cne [string]$expected[$table]) {
            throw "G2-E live Snapshot differs from verified evidence for $table."
        }
    }
    return $actual
}

function Get-G2eMetricWindow {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Evidence,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $snapshot = Assert-G2ePositiveSnapshotId `
        $Evidence.CuratedSnapshots.order_fact_v1 'order_fact_v1 metric window'
    $sql = @"
SELECT CAST(min(purchase_date) AS varchar) AS window_start,
       CAST(max(purchase_date) AS varchar) AS window_end,
       count(*) AS source_order_count
FROM lakehouse.olist.order_fact_v1 FOR VERSION AS OF $snapshot;
"@
    $rows = @(Invoke-G2eMetricTrinoStatement -Name metric_window -Sql $sql -EnvFile $EnvFile)
    if ($rows.Count -ne 1) { throw 'G2-E metric window query must return exactly one row.' }
    $count = ConvertTo-G2eCount `
        (Get-G2ePropertyValue $rows[0] 'source_order_count') 'metric source order count'
    $expectedCount = ConvertTo-G2eCount `
        $Evidence.SourceIdentity.SourceRowCounts['orders_src_v1'] 'verified orders source count'
    if ($count -ne $expectedCount) {
        throw 'G2-E metric window order count differs from verified source evidence.'
    }
    return [pscustomobject][ordered]@{
        WindowStart = [string](Get-G2ePropertyValue $rows[0] 'window_start')
        WindowEnd = [string](Get-G2ePropertyValue $rows[0] 'window_end')
        SourceOrderCount = $count
    }
}

function Invoke-G2eMetricQueries {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Evidence,
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $template = [IO.File]::ReadAllText($script:G2eMetricSqlPath, [Text.Encoding]::UTF8)
    $parts = G2e.OrderMetrics\Split-G2eNamedSql `
        -Sql $template -CuratedSnapshots $Evidence.CuratedSnapshots
    $results = [ordered]@{}
    foreach ($family in $script:G2eMetricFamilies) {
        $sql = Assert-G2eRenderedMetricSql -Sql ([string]$parts[$family])
        $results[$family] = @(Invoke-G2eMetricTrinoStatement `
            -Name $family -Sql $sql -EnvFile $EnvFile)
    }
    $results.quality = @(Merge-G2eQualityEvidence -SqlQuality $results.quality `
        -Manifest $Evidence.Manifest -SourceReports $Evidence.SourceReports `
        -CuratedReport $Evidence.CuratedReport)
    $bundle = Assert-G2eMetricBundle -Identity $Identity `
        -Overview $results.overview -Delivery $results.delivery -Payment $results.payment `
        -Ranking $results.ranking -Review $results.review -Quality $results.quality
    return [pscustomobject][ordered]@{ Results = $results; Bundle = $bundle }
}

function Export-G2eCandidateArtifact {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet('overview', 'delivery', 'payment', 'ranking', 'review', 'quality')]
        [string]$Family,
        [Parameter(Mandatory = $true)][object[]]$Rows,
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)][string]$MetricsDirectory
    )

    $directory = [IO.Path]::GetFullPath($MetricsDirectory)
    $finalPath = [IO.Path]::GetFullPath((Join-Path $directory "$Family.csv"))
    $null = Assert-G2eRefreshNoReparsePoint -RootPath $script:G2eProjectRoot -CandidatePath $finalPath
    $stagingPath = Join-Path $directory ('.' + $Family + '-' + [guid]::NewGuid().ToString('N') + '.csv')
    try {
        $null = Export-G2eCandidateCsv -Target $Family -Rows $Rows -Identity $Identity `
            -OutputDirectory $directory -OutputPath $stagingPath
        $stagingHash = (Get-FileHash -LiteralPath $stagingPath -Algorithm SHA256).Hash.ToLowerInvariant()
        if ([IO.File]::Exists($finalPath)) {
            $finalHash = (Get-FileHash -LiteralPath $finalPath -Algorithm SHA256).Hash.ToLowerInvariant()
            if ($finalHash -cne $stagingHash) {
                throw "Existing G2-E $Family candidate file conflicts with current evidence."
            }
        } else {
            [IO.File]::Move($stagingPath, $finalPath)
        }
    } finally {
        if ([IO.File]::Exists($stagingPath)) { [IO.File]::Delete($stagingPath) }
    }
    $candidateRows = @(ConvertFrom-G2eCsv -CsvText (
        [IO.File]::ReadAllText($finalPath, [Text.Encoding]::UTF8)
    ))
    $spec = Get-G2eTargetSpec -Family $Family
    $canonical = Get-G2eCanonicalDigest -Rows $candidateRows `
        -Columns $spec.Columns -UniqueKeyColumns $spec.UniqueKeyColumns
    return [pscustomobject][ordered]@{
        Path = $finalPath
        Rows = $candidateRows
        RowCount = [long]$candidateRows.Count
        CanonicalSha256 = $canonical
        FileSha256 = (Get-FileHash -LiteralPath $finalPath -Algorithm SHA256).Hash.ToLowerInvariant()
    }
}

function Initialize-G2eDorisSchema {
    param([Parameter(Mandatory = $true)][string]$EnvFile)

    $ddl = [IO.File]::ReadAllText($script:G2eMetricDdlPath, [Text.Encoding]::UTF8)
    if ($ddl -match '(?im)^\s*(DROP|DELETE|TRUNCATE|UPDATE|REPLACE)\b') {
        throw 'G2-E fixed Doris DDL contains a destructive statement.'
    }
    $null = Invoke-G2eDorisSql -Sql $ddl -EnvFile $EnvFile -NoHeaders
}

function Get-G2eExpectedDorisSchemas {
    $schemas = [ordered]@{ order_metric_publications = $script:G2ePublicationColumns }
    foreach ($family in $script:G2eMetricFamilies) {
        $spec = Get-G2eTargetSpec -Family $family
        $schemas[$spec.Table] = $spec.Columns
    }
    return $schemas
}

function Assert-G2eDorisTables {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][string]$EnvFile)

    $expected = Get-G2eExpectedDorisSchemas
    $quotedTables = @($expected.Keys | ForEach-Object { ConvertTo-G2eSqlString ([string]$_) }) -join ', '
    $rows = @(Invoke-G2eDorisQuery -EnvFile $EnvFile -Sql (
        'SELECT table_name, column_name FROM information_schema.columns ' +
        "WHERE table_schema = 'analytics' AND table_name IN ($quotedTables) " +
        'ORDER BY table_name, ordinal_position;'
    ))
    $actual = [ordered]@{}
    foreach ($row in $rows) {
        $table = [string](Get-G2ePropertyValue $row 'table_name')
        $column = [string](Get-G2ePropertyValue $row 'column_name')
        if (-not $expected.Contains($table)) { throw 'G2-E Doris schema returned an unknown table.' }
        if (-not $actual.Contains($table)) { $actual[$table] = [Collections.Generic.List[string]]::new() }
        $actual[$table].Add($column)
    }
    foreach ($table in $expected.Keys) {
        if (-not $actual.Contains($table)) { throw "G2-E Doris schema is missing $table." }
        $actualColumns = [string[]]$actual[$table].ToArray()
        $expectedColumns = [string[]]$expected[$table].Clone()
        [Array]::Sort($actualColumns, [StringComparer]::Ordinal)
        [Array]::Sort($expectedColumns, [StringComparer]::Ordinal)
        if (($actualColumns -join ',') -cne ($expectedColumns -join ',')) {
            throw "G2-E Doris schema differs for $table."
        }
    }
}

function Read-G2eStoredMetricRows {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet('overview', 'delivery', 'payment', 'ranking', 'review', 'quality')]
        [string]$Family,
        [Parameter(Mandatory = $true)][string]$MetricRunId,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $null = Get-G2eRefreshPlan -MetricRunId $MetricRunId
    $spec = Get-G2eTargetSpec -Family $Family
    $runLiteral = ConvertTo-G2eSqlString $MetricRunId
    $sql = "SELECT $($spec.Columns -join ', ') FROM analytics.$($spec.Table) " +
        "WHERE metric_run_id = $runLiteral ORDER BY $($spec.UniqueKeyColumns -join ', ');"
    return @(Invoke-G2eDorisQuery -Sql $sql -EnvFile $EnvFile)
}

function Read-G2ePublicationRows {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$MetricRunId,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $null = Get-G2eRefreshPlan -MetricRunId $MetricRunId
    $runLiteral = ConvertTo-G2eSqlString $MetricRunId
    $sql = "SELECT $($script:G2ePublicationColumns -join ', ') " +
        'FROM analytics.order_metric_publications ' +
        "WHERE metric_run_id = $runLiteral ORDER BY metric_run_id;"
    return @(Invoke-G2eDorisQuery -Sql $sql -EnvFile $EnvFile)
}

function Get-G2eStoredEvidence {
    param(
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $evidence = [ordered]@{}
    foreach ($family in $script:G2eMetricFamilies) {
        $rows = @(Read-G2eStoredMetricRows `
            -Family $family -MetricRunId $Identity.MetricRunId -EnvFile $EnvFile)
        Assert-G2eStoredIdentity -Identity $Identity -Rows $rows -Family $family
        $spec = Get-G2eTargetSpec -Family $family
        $evidence[$family] = [pscustomobject][ordered]@{
            RowCount = [long]$rows.Count
            Sha256 = Get-G2eCanonicalDigest -Rows $rows `
                -Columns $spec.Columns -UniqueKeyColumns $spec.UniqueKeyColumns
        }
    }
    return $evidence
}

function Invoke-G2eStreamLoad {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet('overview', 'delivery', 'payment', 'ranking', 'review', 'quality')]
        [string]$Family,
        [Parameter(Mandatory = $true)][string]$MetricRunId,
        [Parameter(Mandatory = $true)][string]$AttemptId,
        [Parameter(Mandatory = $true)][string]$CandidatePath
    )

    $null = Get-G2eRefreshPlan -MetricRunId $MetricRunId
    if ($AttemptId -cnotmatch '^[0-9a-f]{32}$') { throw 'G2-E attempt ID is unsafe.' }
    $path = [IO.Path]::GetFullPath($CandidatePath)
    $null = Assert-G2eRefreshNoReparsePoint -RootPath $script:G2eProjectRoot -CandidatePath $path
    if (-not [IO.File]::Exists($path)) { throw 'G2-E Stream Load candidate is missing.' }
    if ($null -eq (Get-Command curl.exe -ErrorAction SilentlyContinue)) {
        throw 'curl.exe is required for G2-E Doris Stream Load.'
    }
    $spec = Get-G2eTargetSpec -Family $Family
    $bundlePrefix = $MetricRunId.Substring($MetricRunId.Length - 64, 16)
    $label = "g2e-$bundlePrefix-$Family-$AttemptId"
    if ($label -cnotmatch '^[a-z0-9-]+$') { throw 'G2-E Stream Load label is unsafe.' }
    $arguments = @(
        '--silent', '--show-error', '--location-trusted', '--user',
        ("$script:G2eDorisUsername`:$script:G2eDorisPassword"), '--request', 'PUT',
        '--header', 'Expect:100-continue', '--header', "label:$label", '--header', 'format:csv',
        '--header', 'column_separator:,', '--header', 'skip_lines:1',
        '--header', 'enclose:"', '--header', 'trim_double_quotes:true',
        '--header', 'strict_mode:true', '--header', 'max_filter_ratio:0',
        '--header', ('columns:' + ($spec.Columns -join ',')),
        '--upload-file', $path,
        "$script:G2eDorisStreamLoadUrl/api/analytics/$($spec.Table)/_stream_load"
    )
    $output = @(& curl.exe @arguments 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "G2-E Stream Load request failed for $($spec.Table)." }
    try { return (($output -join "`n") | ConvertFrom-Json) } catch {
        throw "G2-E Stream Load returned malformed JSON for $($spec.Table)."
    }
}

function Publish-G2eMetadata {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Publication,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $before = @(Read-G2ePublicationRows `
        -MetricRunId $Publication.metric_run_id -EnvFile $EnvFile)
    if ($before.Count -ne 0) { throw 'G2-E publication appeared before final insertion.' }
    $sql = New-G2ePublicationInsertSql -Publication $Publication
    $null = Invoke-G2eDorisSql -Sql $sql -EnvFile $EnvFile -NoHeaders
    $after = @(Read-G2ePublicationRows `
        -MetricRunId $Publication.metric_run_id -EnvFile $EnvFile)
    if ($after.Count -ne 1) { throw 'G2-E publication readback did not return exactly one row.' }
    return Assert-G2ePublicationReadback -Expected $Publication -Actual $after[0]
}

function Write-G2eRefreshReport {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$SourceBundleSha256,
        [Parameter(Mandatory = $true)]$Report
    )

    $paths = Get-G2eMetricsPaths -SourceBundleSha256 $SourceBundleSha256
    if (-not [IO.Directory]::Exists($paths.MetricsDirectory)) {
        throw 'G2-E metrics directory does not exist for report output.'
    }
    $path = Assert-G2eReportPath -SourceBundleSha256 $SourceBundleSha256 -Path $paths.ReportPath
    $json = ($Report | ConvertTo-Json -Depth 16) + "`n"
    if ([IO.File]::Exists($path)) {
        $existing = [IO.File]::ReadAllText($path, [Text.Encoding]::UTF8)
        if ($existing -cne $json) { throw 'Existing G2-E refresh report conflicts with current evidence.' }
        return $path
    }
    $temporary = Join-Path $paths.MetricsDirectory `
        ('.refresh-' + [guid]::NewGuid().ToString('N') + '.tmp')
    try {
        [IO.File]::WriteAllText($temporary, $json, [Text.UTF8Encoding]::new($false))
        [IO.File]::Move($temporary, $path)
    } finally {
        if ([IO.File]::Exists($temporary)) { [IO.File]::Delete($temporary) }
    }
    return $path
}

function Assert-G2eExistingRefreshReport {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)][Collections.IDictionary]$CandidateReport,
        [Parameter(Mandatory = $true)]$Publication
    )

    if (-not [IO.File]::Exists($Path)) { return $false }
    try { $report = [IO.File]::ReadAllText($Path, [Text.Encoding]::UTF8) | ConvertFrom-Json } catch {
        throw 'Existing G2-E refresh report is malformed.'
    }
    if ([string](Get-G2ePropertyValue $report 'status') -notin @('published', 'already_published') -or
            [string](Get-G2ePropertyValue $report 'metric_run_id') -cne [string]$Identity.MetricRunId -or
            [string](Get-G2ePropertyValue $report 'source_bundle_sha256') -cne [string]$Identity.SourceBundleSha256 -or
            [string](Get-G2ePropertyValue $report 'implementation_revision') -cne [string]$Identity.ImplementationRevision) {
        throw 'Existing G2-E refresh report identity conflicts with current evidence.'
    }
    $reportedCandidates = Get-G2ePropertyValue $report 'candidates'
    foreach ($family in $script:G2eMetricFamilies) {
        $reported = Get-G2ePropertyValue $reportedCandidates $family
        $expected = $CandidateReport[$family]
        if ([string](Get-G2ePropertyValue $reported 'row_count') -cne
                [string]$expected.row_count -or
                [string](Get-G2ePropertyValue $reported 'sha256') -cne
                [string]$expected.sha256 -or
                [string](Get-G2ePropertyValue $reported 'file_sha256') -cne
                [string]$expected.file_sha256 -or
                -not [IO.Path]::GetFullPath([string](Get-G2ePropertyValue $reported 'path')).Equals(
                    [IO.Path]::GetFullPath([string]$expected.path),
                    (Get-G2ePathComparison))) {
            throw "Existing G2-E refresh report candidate differs for $family."
        }
    }
    $null = Assert-G2ePublicationReadback `
        -Expected $Publication -Actual (Get-G2ePropertyValue $report 'publication')
    return $true
}

function Invoke-G2eRefresh {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ManifestPath,
        [int]$TimeoutSeconds = 300
    )

    if ($TimeoutSeconds -lt 1 -or $TimeoutSeconds -gt 3600) {
        throw 'G2-E metric refresh timeout must be between 1 and 3600 seconds.'
    }
    $envFile = Get-G2eRefreshEnvFile
    Initialize-G2eDorisCredentials -EnvFile $envFile
    $evidence = Get-G2eRefreshEvidence -ManifestPath $ManifestPath -EnvFile $envFile
    $paths = Initialize-G2eMetricsDirectory `
        -SourceBundleSha256 $evidence.SourceIdentity.SourceBundleSha256
    $lock = $null
    try {
        $lock = Enter-G2eRunLock -Path $paths.LockPath
        $revision = Get-G2eCleanImplementationRevision
        $script:G2eDockerExecutable = Get-G2eDockerExecutable
        $dockerVersion = @(& $script:G2eDockerExecutable version --format '{{.Server.Version}}' 2>&1)
        if ($LASTEXITCODE -ne 0 -or $dockerVersion.Count -ne 1) {
            throw 'Docker Engine is unavailable for G2-E metric refresh.'
        }
        Start-G2eMetricServices -EnvFile $envFile
        Wait-G2eTrinoDependency -TimeoutSeconds $TimeoutSeconds
        $null = Get-G2eLiveSnapshotMap -Evidence $evidence -EnvFile $envFile
        $window = Get-G2eMetricWindow -Evidence $evidence -EnvFile $envFile
        $calculatedAt = [datetimeoffset]::UtcNow.ToString(
            "yyyy-MM-dd'T'HH:mm:ss.fff'Z'", [Globalization.CultureInfo]::InvariantCulture
        )
        $identity = Get-G2eMetricIdentity -Manifest $evidence.Manifest `
            -SourceReports $evidence.SourceReports -CuratedReport $evidence.CuratedReport `
            -WindowStart $window.WindowStart -WindowEnd $window.WindowEnd `
            -CalculatedAt $calculatedAt -ImplementationRevision $revision
        $null = Get-G2eRefreshPlan -MetricRunId $identity.MetricRunId
        $queryBundle = Invoke-G2eMetricQueries `
            -Evidence $evidence -Identity $identity -EnvFile $envFile

        $candidates = [ordered]@{}
        $candidateEvidence = [ordered]@{}
        $candidateReport = [ordered]@{}
        foreach ($family in $script:G2eMetricFamilies) {
            $candidate = Export-G2eCandidateArtifact -Family $family `
                -Rows $queryBundle.Results[$family] -Identity $identity `
                -MetricsDirectory $paths.MetricsDirectory
            $candidates[$family] = $candidate
            $candidateEvidence[$family] = [pscustomobject][ordered]@{
                RowCount = $candidate.RowCount
                Sha256 = $candidate.CanonicalSha256
            }
            $candidateReport[$family] = [ordered]@{
                path = $candidate.Path
                row_count = $candidate.RowCount
                sha256 = $candidate.CanonicalSha256
                file_sha256 = $candidate.FileSha256
            }
        }
        Assert-G2eExactEvidenceMap -Evidence $candidateEvidence

        Wait-G2eDorisDependency -EnvFile $envFile -TimeoutSeconds $TimeoutSeconds
        Initialize-G2eDorisSchema -EnvFile $envFile
        Assert-G2eDorisTables -EnvFile $envFile
        $publicationRows = @(Read-G2ePublicationRows `
            -MetricRunId $identity.MetricRunId -EnvFile $envFile)
        if ($publicationRows.Count -gt 1) { throw 'G2-E metric run has multiple publication rows.' }
        if ($publicationRows.Count -eq 1) {
            $storedEvidence = Get-G2eStoredEvidence -Identity $identity -EnvFile $envFile
            $allStored = Assert-G2eUnpublishedCandidateState `
                -ExpectedEvidence $candidateEvidence -StoredEvidence $storedEvidence
            if ($allStored.load.Count -ne 0) { throw 'G2-E published run is missing candidate rows.' }
            $null = Assert-G2eExistingPublication -Identity $identity `
                -Publication $publicationRows[0] -StoredEvidence $storedEvidence
            if (-not (Assert-G2eExistingRefreshReport -Path $paths.ReportPath `
                    -Identity $identity -CandidateReport $candidateReport `
                    -Publication $publicationRows[0])) {
                $report = [ordered]@{
                    status = 'already_published'
                    metric_run_id = $identity.MetricRunId
                    source_bundle_sha256 = $identity.SourceBundleSha256
                    implementation_revision = $identity.ImplementationRevision
                    candidates = $candidateReport
                    publication = $publicationRows[0]
                }
                $null = Write-G2eRefreshReport `
                    -SourceBundleSha256 $identity.SourceBundleSha256 -Report $report
            }
            return [pscustomobject][ordered]@{
                status = 'already_published'
                metric_run_id = $identity.MetricRunId
                report_path = $paths.ReportPath
            }
        }

        $storedState = [ordered]@{}
        foreach ($family in $script:G2eMetricFamilies) {
            $storedRows = @(Read-G2eStoredMetricRows `
                -Family $family -MetricRunId $identity.MetricRunId -EnvFile $envFile)
            if ($storedRows.Count -eq 0) {
                $storedState[$family] = $null
            } else {
                $storedState[$family] = Assert-G2eStoredCandidate -Family $family `
                    -Identity $identity -CandidateRows $candidates[$family].Rows `
                    -StoredRows $storedRows
            }
        }
        $retryState = Assert-G2eUnpublishedCandidateState `
            -ExpectedEvidence $candidateEvidence -StoredEvidence $storedState
        $attemptId = [guid]::NewGuid().ToString('N')
        $verifyAction = {
            param($Family)
            $candidate = $candidates[$Family]
            if ($retryState.load -ccontains $Family) {
                $response = Invoke-G2eStreamLoad -Family $Family `
                    -MetricRunId $identity.MetricRunId -AttemptId $attemptId `
                    -CandidatePath $candidate.Path
                $spec = Get-G2eTargetSpec -Family $Family
                Assert-G2eStreamLoadResponse -Response $response `
                    -ExpectedRows $candidate.RowCount -TableName $spec.Table
            }
            $storedRows = @(Read-G2eStoredMetricRows `
                -Family $Family -MetricRunId $identity.MetricRunId -EnvFile $envFile)
            return Assert-G2eStoredCandidate -Family $Family -Identity $identity `
                -CandidateRows $candidate.Rows -StoredRows $storedRows
        }.GetNewClosure()
        $publishAction = {
            param($VerifiedEvidence)
            $publishedAt = [datetimeoffset]::UtcNow.ToString(
                "yyyy-MM-dd'T'HH:mm:ss.fff'Z'",
                [Globalization.CultureInfo]::InvariantCulture
            )
            $record = New-G2ePublicationRecord -Identity $identity `
                -Evidence $VerifiedEvidence -PublishedAt $publishedAt
            return Publish-G2eMetadata -Publication $record -EnvFile $envFile
        }.GetNewClosure()
        $publication = Invoke-G2ePublicationSequence `
            -VerifyAction $verifyAction -PublishAction $publishAction
        $report = [ordered]@{
            status = 'published'
            metric_run_id = $identity.MetricRunId
            source_bundle_sha256 = $identity.SourceBundleSha256
            implementation_revision = $identity.ImplementationRevision
            candidates = $candidateReport
            publication = $publication
        }
        $reportPath = Write-G2eRefreshReport `
            -SourceBundleSha256 $identity.SourceBundleSha256 -Report $report
        return [pscustomobject][ordered]@{
            status = 'published'
            metric_run_id = $identity.MetricRunId
            report_path = $reportPath
        }
    } finally {
        if ($null -ne $lock) {
            $lock.Dispose()
            if ([IO.File]::Exists($paths.LockPath)) { [IO.File]::Delete($paths.LockPath) }
        }
    }
}

if ($FunctionsOnly) { return }

if ([string]::IsNullOrWhiteSpace($refreshRequestedManifestPath)) {
    throw 'ManifestPath is required for G2-E metric refresh.'
}
$result = Invoke-G2eRefresh `
    -ManifestPath $refreshRequestedManifestPath -TimeoutSeconds $refreshRequestedTimeout
$result | ConvertTo-Json -Depth 16 -Compress
