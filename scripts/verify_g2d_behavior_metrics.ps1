[CmdletBinding()]
param(
    [ValidateSet('g2c-correctness-subset', 'stable-user-2pct-full')]
    [string]$ExpectedDataScope = 'g2c-correctness-subset',
    [switch]$FunctionsOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$verifierFunctionsOnly = $FunctionsOnly
$verifierProjectRoot = Split-Path $PSScriptRoot -Parent
. (Join-Path $PSScriptRoot 'refresh_g2d_behavior_metrics.ps1') -FunctionsOnly
$FunctionsOnly = $verifierFunctionsOnly

$script:G2dApiBaseUrl = 'http://localhost:8000'
$script:G2dSubsetWarning = 'correctness subset; not the full 2% user sample'
$script:G2dProxyLimitation = 'No quantity, currency, discount, refund, cancellation, or payment state.'
$script:G2dMetricTables = @('overview', 'funnel', 'dimension', 'quality')
$script:G2dApiContracts = @(
    'GET /api/v1/behavior/publication',
    'GET /api/v1/behavior/overview?window=full',
    'GET /api/v1/behavior/funnel?window=full',
    'GET /api/v1/behavior/rankings?dimension=product&window=full&sort_by=purchases&limit=20',
    'GET /api/v1/behavior/quality',
    'GET /api/v1/metrics/definitions?domain=behavior&version=behavior-v1'
)

function Assert-G2dVerificationEvidence {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ExpectedRunId,
        [Parameter(Mandatory = $true)]$ExpectedSnapshotId,
        [Parameter(Mandatory = $true)]$SourceTotal,
        [Parameter(Mandatory = $true)]$DayTotal,
        [Parameter(Mandatory = $true)]$FullTotal,
        [Parameter(Mandatory = $true)]$CleanCount,
        [Parameter(Mandatory = $true)]$LateCount,
        [Parameter(Mandatory = $true)]$DistinctEventCount,
        [Parameter(Mandatory = $true)][string]$ApiRunId,
        [Parameter(Mandatory = $true)]$ApiSourceTotal,
        [string]$DataScope = 'g2c-correctness-subset',
        $ApiSnapshotId,
        [string]$ApiDataScope = '',
        [AllowEmptyCollection()][string[]]$ApiWarnings = @(
            'correctness subset; not the full 2% user sample'
        ),
        [AllowEmptyCollection()][string[]]$ProxyLimitations = @(
            'No quantity, currency, discount, refund, cancellation, or payment state.'
        ),
        [AllowEmptyCollection()][string[]]$ProxyForbiddenClaims = @('GMV', '销售额', '收入'),
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()][string[]]$ValidatedMetricTables,
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()][string[]]$ApiContracts
    )

    $identity = Get-G2dMetricIdentity -SnapshotId $ExpectedSnapshotId `
        -DataScope $DataScope -SourceEventCount $SourceTotal
    if ($ExpectedRunId -cne [string]$identity.MetricRunId) {
        throw 'G2-D publication run does not match its Snapshot identity.'
    }

    $day = [long]$DayTotal
    $full = [long]$FullTotal
    $clean = [long]$CleanCount
    $late = [long]$LateCount
    $distinct = [long]$DistinctEventCount
    $source = [long]$identity.SourceEventCount
    if ($day -ne $source -or $full -ne $source) {
        throw 'G2-D DAY, FULL, and source totals do not reconcile.'
    }
    if ($clean + $late -ne $source) {
        throw 'G2-D clean and late totals do not reconcile.'
    }
    if ($DataScope -ceq 'g2c-correctness-subset' -and ($clean -ne 1001 -or $late -ne 1)) {
        throw 'G2-D correctness subset must contain exactly 1,001 clean and 1 late event.'
    }
    if ($distinct -ne $source) { throw 'G2-D duplicate event IDs were detected.' }

    if (-not $PSBoundParameters.ContainsKey('ApiSnapshotId')) {
        $ApiSnapshotId = $ExpectedSnapshotId
    }
    if (-not $PSBoundParameters.ContainsKey('ApiDataScope')) { $ApiDataScope = $DataScope }
    $apiIdentity = Get-G2dMetricIdentity -SnapshotId $ApiSnapshotId `
        -DataScope $ApiDataScope -SourceEventCount $ApiSourceTotal
    if ($ApiRunId -cne $ExpectedRunId -or
            [string]$apiIdentity.MetricRunId -cne $ExpectedRunId) {
        throw 'G2-D API identity does not match the Doris publication.'
    }

    if ($DataScope -ceq 'g2c-correctness-subset' -and
            ($ApiWarnings.Count -ne 1 -or $ApiWarnings[0] -cne $script:G2dSubsetWarning)) {
        throw 'G2-D API correctness-subset warning is missing or changed.'
    }
    if ($DataScope -ceq 'stable-user-2pct-full' -and $ApiWarnings.Count -ne 0) {
        throw 'G2-D full-scope API response must not carry the subset warning.'
    }

    if ($ProxyLimitations -cnotcontains $script:G2dProxyLimitation) {
        throw 'G2-D purchase_amount_proxy limitation is missing.'
    }
    $claimSet = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($claim in $ProxyForbiddenClaims) {
        if ([string]::IsNullOrWhiteSpace($claim) -or -not $claimSet.Add($claim)) {
            throw 'G2-D purchase_amount_proxy forbidden claims are invalid.'
        }
    }
    foreach ($requiredClaim in @('GMV', '销售额', '收入')) {
        if (-not $claimSet.Contains($requiredClaim)) {
            throw 'G2-D purchase_amount_proxy definitions permit a forbidden business claim.'
        }
    }

    if (($ValidatedMetricTables -join ',') -cne ($script:G2dMetricTables -join ',')) {
        throw 'G2-D verification did not validate every metric table.'
    }
    if (($ApiContracts -join ',') -cne ($script:G2dApiContracts -join ',')) {
        throw 'G2-D verification did not validate all six API contracts.'
    }

    return [pscustomobject][ordered]@{
        status = 'PASS'
        metric_run_id = $ExpectedRunId
        source_snapshot_id = [string]$identity.SourceSnapshotId
        data_scope = $DataScope
        source_event_count = $source
        day_event_count = $day
        full_event_count = $full
        clean_event_count = $clean
        late_event_count = $late
        distinct_event_count = $distinct
    }
}

function Invoke-G2dVerification {
    [CmdletBinding()]
    param(
        [ValidateSet('g2c-correctness-subset', 'stable-user-2pct-full')]
        [string]$ExpectedDataScope = 'g2c-correctness-subset'
    )

    $stopwatch = [Diagnostics.Stopwatch]::StartNew()
    Assert-G2dDependencies
    try {
        $healthResponse = Invoke-WebRequest -UseBasicParsing -Method Get `
            -Uri "$script:G2dApiBaseUrl/health" -TimeoutSec 15
        $health = $healthResponse.Content | ConvertFrom-Json
    } catch {
        throw "G2-D API /health is unavailable. $script:G2dSetupHelp"
    }
    if ([int]$healthResponse.StatusCode -ne 200 -or [string]$health.status -cne 'ok' -or
            [string]$health.service -cne 'realtime-metrics-api') {
        throw 'G2-D API /health returned an unexpected contract.'
    }

    $publicationRows = @(Invoke-G2dDorisQuery -Sql @'
SELECT metric_run_id, dataset_id, metric_version, data_scope, source_snapshot_id,
       source_event_count, window_start, window_end, calculated_at, published_at,
       overview_row_count, overview_sha256, funnel_row_count, funnel_sha256,
       dimension_row_count, dimension_sha256, quality_row_count, quality_sha256, status
FROM analytics.behavior_metric_publications
WHERE status = 'PUBLISHED'
ORDER BY published_at DESC, metric_run_id DESC
LIMIT 1;
'@)
    if ($publicationRows.Count -ne 1) {
        throw 'G2-D requires exactly one latest PUBLISHED Doris run.'
    }
    $publication = $publicationRows[0]
    $identity = Get-G2dMetricIdentity -SnapshotId $publication.source_snapshot_id `
        -DataScope $ExpectedDataScope -SourceEventCount $publication.source_event_count
    if ([string]$publication.metric_run_id -cne [string]$identity.MetricRunId -or
            [string]$publication.dataset_id -cne [string]$identity.DatasetId -or
            [string]$publication.metric_version -cne [string]$identity.MetricVersion -or
            [string]$publication.data_scope -cne [string]$identity.DataScope -or
            [string]$publication.status -cne 'PUBLISHED') {
        throw 'G2-D latest publication identity is invalid.'
    }
    foreach ($name in $script:G2dMetricTables) {
        $countField = "${name}_row_count"
        $hashField = "${name}_sha256"
        if ([long]$publication.$countField -le 0 -or
                [string]$publication.$hashField -notmatch '^[0-9a-f]{64}$') {
            throw "G2-D publication evidence is invalid for '$name'."
        }
    }

    $templatePath = Join-Path $verifierProjectRoot 'jobs/sql/18_g2d_behavior_metrics.sql.template'
    $template = [IO.File]::ReadAllText($templatePath, [Text.Encoding]::UTF8)
    $statements = Split-G2dNamedSql -Sql $template -SnapshotId $identity.SourceSnapshotId
    $sourceRows = @(
        Invoke-G2dTrinoStatement -Name source_identity -Sql $statements.source_identity
    )
    if ($sourceRows.Count -ne 1 -or
            [string]$sourceRows[0].source_snapshot_id -cne [string]$identity.SourceSnapshotId -or
            [long]$sourceRows[0].source_event_count -ne [long]$identity.SourceEventCount -or
            [long]$sourceRows[0].distinct_event_count -ne [long]$identity.SourceEventCount) {
        throw 'G2-D Trino source identity does not match the publication.'
    }
    $sourceIdentity = $sourceRows[0]

    $metricRows = [ordered]@{}
    $metricEvidence = [ordered]@{}
    foreach ($name in $script:G2dMetricTables) {
        $spec = Get-G2dTargetSpec -Name $name
        $rows = @(Read-G2dStoredMetricRows -Name $name -MetricRunId $identity.MetricRunId)
        Assert-G2dStoredIdentity -Identity $identity -Rows $rows -Name $name
        $digest = Get-G2dCanonicalDigest -Rows $rows -Columns $spec.Columns `
            -UniqueKeyColumns $spec.UniqueKeyColumns
        $countField = "${name}_row_count"
        $hashField = "${name}_sha256"
        if ($rows.Count -ne [long]$publication.$countField -or
                $digest -cne [string]$publication.$hashField) {
            throw "G2-D Doris row count or digest mismatch for '$name'."
        }
        $metricRows[$name] = $rows
        $metricEvidence[$name] = [pscustomobject][ordered]@{
            table = $spec.Table
            row_count = [long]$rows.Count
            sha256 = $digest
        }
    }

    $bundle = Assert-G2dMetricBundle -Identity $identity -SourceIdentity $sourceRows `
        -Overview $metricRows.overview -Funnel $metricRows.funnel `
        -Dimension $metricRows.dimension -Quality $metricRows.quality
    $dayRows = @($metricRows.overview | Where-Object { $_.window_type -ceq 'DAY' })
    $fullOverview = @($metricRows.overview | Where-Object { $_.window_type -ceq 'FULL' })[0]
    $dayTotal = [long](($dayRows | Measure-Object -Property event_count -Sum).Sum)
    foreach ($column in @('view_count', 'cart_count', 'purchase_count')) {
        $dayColumnTotal = [long](($dayRows | Measure-Object -Property $column -Sum).Sum)
        if ($dayColumnTotal -ne [long]$fullOverview.$column) {
            throw "G2-D DAY and FULL '$column' values do not reconcile."
        }
    }
    $dayAmount = [decimal]0
    foreach ($row in $dayRows) {
        $dayAmount += [decimal]::Parse(
            [string]$row.purchase_amount_proxy,
            [Globalization.CultureInfo]::InvariantCulture
        )
    }
    $fullAmount = [decimal]::Parse(
        [string]$fullOverview.purchase_amount_proxy,
        [Globalization.CultureInfo]::InvariantCulture
    )
    if ($dayAmount -ne $fullAmount) {
        throw 'G2-D DAY and FULL purchase_amount_proxy values do not reconcile.'
    }
    $knownEventCount = [long]$fullOverview.view_count + [long]$fullOverview.cart_count +
        [long]$fullOverview.purchase_count
    $removeFromCartCount = [long]$fullOverview.event_count - $knownEventCount
    if ($removeFromCartCount -lt 0) { throw 'G2-D event-type counts exceed the FULL total.' }

    $quality = $metricRows.quality[0]
    foreach ($column in @(
            'duplicate_event_count', 'invalid_event_type_count', 'empty_key_id_count',
            'invalid_price_count', 'invalid_derived_date_count')) {
        if ([long]$quality.$column -ne 0) { throw "G2-D quality gate failed for '$column'." }
    }
    if ([long]$quality.clean_event_count + [long]$quality.late_event_count -ne
            [long]$identity.SourceEventCount) {
        throw 'G2-D clean and late counts do not equal the source total.'
    }

    $fullFunnel = @($metricRows.funnel | Where-Object { $_.window_type -ceq 'FULL' })[0]
    if ([long]$fullFunnel.completed_sessions -gt [long]$fullFunnel.view_to_cart_sessions -or
            [long]$fullFunnel.view_to_cart_sessions -gt [long]$fullFunnel.view_sessions) {
        throw 'G2-D FULL funnel is not monotonic.'
    }

    $apiSpecs = @(
        [pscustomobject]@{ Name = 'publication'; Method = 'GET'; Path = '/api/v1/behavior/publication' },
        [pscustomobject]@{ Name = 'overview'; Method = 'GET'; Path = '/api/v1/behavior/overview?window=full' },
        [pscustomobject]@{ Name = 'funnel'; Method = 'GET'; Path = '/api/v1/behavior/funnel?window=full' },
        [pscustomobject]@{ Name = 'rankings'; Method = 'GET'; Path = '/api/v1/behavior/rankings?dimension=product&window=full&sort_by=purchases&limit=20' },
        [pscustomobject]@{ Name = 'quality'; Method = 'GET'; Path = '/api/v1/behavior/quality' },
        [pscustomobject]@{ Name = 'definitions'; Method = 'GET'; Path = '/api/v1/metrics/definitions?domain=behavior&version=behavior-v1' }
    )
    $apiPayloads = [ordered]@{}
    $apiEvidence = [Collections.Generic.List[object]]::new()
    foreach ($spec in $apiSpecs) {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Method $spec.Method `
                -Uri ($script:G2dApiBaseUrl + $spec.Path) -TimeoutSec 15
            $payload = $response.Content | ConvertFrom-Json
        } catch {
            throw "G2-D API contract '$($spec.Name)' is unavailable."
        }
        if ([int]$response.StatusCode -ne 200) {
            throw "G2-D API contract '$($spec.Name)' did not return HTTP 200."
        }
        $apiPayloads[$spec.Name] = $payload
        $apiEvidence.Add([pscustomobject][ordered]@{
            name = $spec.Name
            method = $spec.Method
            path = $spec.Path
            status_code = [int]$response.StatusCode
        })
    }

    foreach ($name in @('publication', 'overview', 'funnel', 'rankings', 'quality')) {
        $meta = $apiPayloads[$name].meta
        $warnings = @($meta.warnings)
        if ([string]$meta.dataset_id -cne [string]$identity.DatasetId -or
                [string]$meta.metric_version -cne [string]$identity.MetricVersion -or
                [string]$meta.metric_run_id -cne [string]$identity.MetricRunId -or
                [string]$meta.source_snapshot_id -cne [string]$identity.SourceSnapshotId -or
                [string]$meta.data_scope -cne [string]$identity.DataScope -or
                [long]$meta.source_event_count -ne [long]$identity.SourceEventCount -or
                $warnings.Count -ne 1 -or $warnings[0] -cne $script:G2dSubsetWarning) {
            throw "G2-D API contract '$name' does not match the published identity."
        }
    }

    $apiPublication = $apiPayloads.publication.data
    if ([string]$apiPublication.status -cne 'PUBLISHED') {
        throw 'G2-D publication API did not return PUBLISHED.'
    }
    foreach ($name in $script:G2dMetricTables) {
        $countField = "${name}_row_count"
        $hashField = "${name}_sha256"
        if ([long]$apiPublication.$countField -ne [long]$publication.$countField -or
                [string]$apiPublication.$hashField -cne [string]$publication.$hashField) {
            throw "G2-D publication API evidence mismatch for '$name'."
        }
    }

    $apiOverviewRows = @($apiPayloads.overview.data)
    if ($apiOverviewRows.Count -ne 1) { throw 'G2-D FULL overview API must return one row.' }
    $apiOverview = $apiOverviewRows[0]
    foreach ($column in @(
            'window_type', 'window_start', 'window_end', 'event_count', 'view_count',
            'cart_count', 'purchase_count', 'unique_user_count', 'session_count',
            'product_count', 'purchase_amount_proxy')) {
        if ([string]$apiOverview.$column -cne [string]$fullOverview.$column) {
            throw "G2-D overview API mismatch for '$column'."
        }
    }

    $apiFunnelRows = @($apiPayloads.funnel.data)
    if ($apiFunnelRows.Count -ne 1) { throw 'G2-D FULL funnel API must return one row.' }
    $apiFunnel = $apiFunnelRows[0]
    foreach ($column in @(
            'window_type', 'window_start', 'window_end', 'missing_session_event_count',
            'view_sessions', 'view_to_cart_sessions', 'completed_sessions')) {
        if ([string]$apiFunnel.$column -cne [string]$fullFunnel.$column) {
            throw "G2-D funnel API mismatch for '$column'."
        }
    }

    $fullProducts = @(
        $metricRows.dimension | Where-Object {
            $_.window_type -ceq 'FULL' -and $_.dimension_type -ceq 'product'
        }
    )
    $apiRankings = @($apiPayloads.rankings.data)
    if ($apiRankings.Count -ne [Math]::Min(20, $fullProducts.Count)) {
        throw 'G2-D rankings API returned an unexpected row count.'
    }
    foreach ($ranking in $apiRankings) {
        $matches = @($fullProducts | Where-Object { $_.dimension_id -ceq $ranking.dimension_id })
        if ($matches.Count -ne 1) { throw 'G2-D rankings API returned an unknown product.' }
        foreach ($column in @(
                'window_type', 'window_start', 'window_end', 'dimension_type', 'dimension_id',
                'dimension_name', 'view_count', 'cart_count', 'purchase_count',
                'unique_user_count', 'purchase_amount_proxy')) {
            if ([string]$ranking.$column -cne [string]$matches[0].$column) {
                throw "G2-D rankings API mismatch for '$column'."
            }
        }
    }

    $apiQuality = $apiPayloads.quality.data
    foreach ($column in @(
            'window_type', 'window_start', 'window_end', 'source_event_count',
            'clean_event_count', 'late_event_count', 'distinct_event_count',
            'duplicate_event_count', 'missing_session_count', 'unknown_category_count',
            'unknown_brand_count', 'invalid_event_type_count', 'empty_key_id_count',
            'invalid_price_count', 'invalid_derived_date_count', 'overview_event_count',
            'reconciliation_status')) {
        if ([string]$apiQuality.$column -cne [string]$quality.$column) {
            throw "G2-D quality API mismatch for '$column'."
        }
    }

    $definitions = $apiPayloads.definitions
    if ([string]$definitions.domain -cne 'behavior' -or
            [string]$definitions.dataset_id -cne [string]$identity.DatasetId -or
            [string]$definitions.metric_version -cne [string]$identity.MetricVersion) {
        throw 'G2-D definitions API identity is invalid.'
    }
    $proxyDefinitions = @(
        $definitions.definitions | Where-Object { $_.metric_name -ceq 'purchase_amount_proxy' }
    )
    if ($proxyDefinitions.Count -ne 1) {
        throw 'G2-D definitions API must contain exactly one purchase_amount_proxy definition.'
    }
    $proxyDefinition = $proxyDefinitions[0]
    $verification = Assert-G2dVerificationEvidence `
        -ExpectedRunId $identity.MetricRunId `
        -ExpectedSnapshotId $identity.SourceSnapshotId `
        -SourceTotal $identity.SourceEventCount `
        -DayTotal $dayTotal `
        -FullTotal $fullOverview.event_count `
        -CleanCount $quality.clean_event_count `
        -LateCount $quality.late_event_count `
        -DistinctEventCount $sourceIdentity.distinct_event_count `
        -ApiRunId $apiPayloads.publication.meta.metric_run_id `
        -ApiSourceTotal $apiPayloads.publication.meta.source_event_count `
        -DataScope $identity.DataScope `
        -ApiSnapshotId $apiPayloads.publication.meta.source_snapshot_id `
        -ApiDataScope $apiPayloads.publication.meta.data_scope `
        -ApiWarnings @($apiPayloads.publication.meta.warnings) `
        -ProxyLimitations @($proxyDefinition.limitations) `
        -ProxyForbiddenClaims @($proxyDefinition.forbidden_claims) `
        -ValidatedMetricTables @($metricEvidence.Keys) `
        -ApiContracts @(
            $apiEvidence | ForEach-Object { "$($_.method) $($_.path)" }
        )

    $stopwatch.Stop()
    $report = [pscustomobject][ordered]@{
        status = $verification.status
        verified_at = [datetimeoffset]::UtcNow.ToString(
            "yyyy-MM-dd'T'HH:mm:ss.fff'Z'",
            [Globalization.CultureInfo]::InvariantCulture
        )
        elapsed_ms = [long]$stopwatch.ElapsedMilliseconds
        source = [pscustomobject][ordered]@{
            table = 'lakehouse.analytics.real_behavior_detail_v1'
            snapshot_id = [string]$identity.SourceSnapshotId
            snapshot_committed_at = [string]$sourceIdentity.snapshot_committed_at
            window_start = [string]$sourceIdentity.window_start
            window_end = [string]$sourceIdentity.window_end
            event_count = [long]$identity.SourceEventCount
            distinct_event_count = [long]$sourceIdentity.distinct_event_count
        }
        publication = [pscustomobject][ordered]@{
            metric_run_id = [string]$identity.MetricRunId
            data_scope = [string]$identity.DataScope
            calculated_at = [string]$publication.calculated_at
            published_at = [string]$publication.published_at
        }
        metric_tables = $metricEvidence
        reconciliation = [pscustomobject][ordered]@{
            day_event_count = [long]$dayTotal
            full_event_count = [long]$fullOverview.event_count
            view_count = [long]$fullOverview.view_count
            cart_count = [long]$fullOverview.cart_count
            remove_from_cart_count = $removeFromCartCount
            purchase_count = [long]$fullOverview.purchase_count
            clean_event_count = [long]$quality.clean_event_count
            late_event_count = [long]$quality.late_event_count
            full_view_sessions = [long]$fullFunnel.view_sessions
            full_view_to_cart_sessions = [long]$fullFunnel.view_to_cart_sessions
            full_completed_sessions = [long]$fullFunnel.completed_sessions
        }
        api_contracts = @($apiEvidence.ToArray())
        definition_guard = [pscustomobject][ordered]@{
            metric_name = 'purchase_amount_proxy'
            limitations = @($proxyDefinition.limitations)
            forbidden_claims = @($proxyDefinition.forbidden_claims)
        }
        capacity_boundary = 'The 1,002-row correctness subset is non-capacity evidence and is not the full 2% user sample.'
    }

    $paths = Initialize-G2dRunDirectory -MetricRunId $identity.MetricRunId
    $reportPath = [IO.Path]::GetFullPath((Join-Path $paths.RunDirectory 'verification.json'))
    $expectedPath = [IO.Path]::GetFullPath((Join-Path $paths.RunDirectory 'verification.json'))
    if (-not $reportPath.Equals($expectedPath, (Get-G2dPathComparison))) {
        throw 'G2-D verification report path is not fixed.'
    }
    $null = Assert-G2dPhysicalContainment -RootPath $paths.PhysicalRunDirectory `
        -CandidatePath $reportPath -Description 'verification report'
    if ([IO.File]::Exists($reportPath)) {
        $item = Get-Item -LiteralPath $reportPath -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw 'G2-D verification report cannot replace a reparse point.'
        }
    }
    $json = $report | ConvertTo-Json -Depth 12 -Compress
    [IO.File]::WriteAllText($reportPath, ($json + "`n"), [Text.UTF8Encoding]::new($false))
    return $report
}

if (-not $FunctionsOnly) {
    $result = Invoke-G2dVerification -ExpectedDataScope $ExpectedDataScope
    $result | ConvertTo-Json -Depth 12 -Compress
}
