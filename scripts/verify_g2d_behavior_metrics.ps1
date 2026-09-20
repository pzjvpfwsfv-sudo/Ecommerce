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
$script:G2dExpectedMetricNames = @(
    'event_count', 'view_count', 'cart_count', 'purchase_count',
    'unique_user_count', 'session_count', 'product_count', 'purchase_amount_proxy',
    'view_sessions', 'view_to_cart_sessions', 'completed_sessions',
    'view_to_cart_rate', 'cart_to_purchase_rate', 'full_conversion_rate',
    'clean_event_count', 'late_event_count', 'distinct_event_count',
    'duplicate_event_count', 'missing_session_count', 'unknown_category_count',
    'unknown_brand_count', 'invalid_event_type_count', 'empty_key_id_count',
    'invalid_price_count', 'invalid_derived_date_count'
)

function Get-G2dVerifierValue {
    param(
        [Parameter(Mandatory = $true)]$Object,
        [Parameter(Mandatory = $true)][string]$Name
    )

    if ($Object -is [Collections.IDictionary]) {
        if (-not $Object.Contains($Name)) { throw "G2-D API field '$Name' is missing." }
        return $Object[$Name]
    }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) { throw "G2-D API field '$Name' is missing." }
    return $property.Value
}

function Assert-G2dVerifierProperties {
    param(
        [Parameter(Mandatory = $true)]$Object,
        [Parameter(Mandatory = $true)][string[]]$Expected,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $actual = if ($Object -is [Collections.IDictionary]) {
        @($Object.Keys | ForEach-Object { [string]$_ })
    } else {
        @($Object.PSObject.Properties.Name)
    }
    $actual = @($actual | Sort-Object -CaseSensitive)
    $wanted = @($Expected | Sort-Object -CaseSensitive)
    if (($actual -join "`u{001F}") -cne ($wanted -join "`u{001F}")) {
        throw "G2-D API '$Name' fields do not match the public contract."
    }
}

function ConvertTo-G2dVerifierDate {
    param([Parameter(Mandatory = $true)]$Value)

    if ($Value -is [datetime]) {
        return $Value.ToString('yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
    }
    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    $parsed = [datetime]::MinValue
    if (-not [datetime]::TryParseExact(
            $text,
            'yyyy-MM-dd',
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::None,
            [ref]$parsed)) {
        throw 'G2-D API date is invalid.'
    }
    return $parsed.ToString('yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
}

function ConvertTo-G2dVerifierTimestamp {
    param([Parameter(Mandatory = $true)]$Value)

    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    $parsed = [datetimeoffset]::MinValue
    if (-not [datetimeoffset]::TryParse(
            $text,
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::AssumeUniversal,
            [ref]$parsed)) {
        throw 'G2-D API timestamp is invalid.'
    }
    return $parsed.ToUniversalTime().ToString(
        "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'",
        [Globalization.CultureInfo]::InvariantCulture
    )
}

function ConvertTo-G2dVerifierBoolean {
    param([Parameter(Mandatory = $true)]$Value)

    if ($Value -is [bool]) { return [bool]$Value }
    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    if ($text -ceq '1' -or $text -ceq 'true' -or $text -ceq 'True') { return $true }
    if ($text -ceq '0' -or $text -ceq 'false' -or $text -ceq 'False') { return $false }
    throw 'G2-D API boolean is invalid.'
}

function ConvertTo-G2dVerifierDecimal {
    param(
        [Parameter(Mandatory = $true)]$Value,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $parsed = 0D
    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    if (-not [decimal]::TryParse(
            $text,
            [Globalization.NumberStyles]::AllowDecimalPoint,
            [Globalization.CultureInfo]::InvariantCulture,
            [ref]$parsed) -or $parsed -lt 0) {
        throw "G2-D $Name is not a nonnegative decimal."
    }
    return $parsed
}

function Format-G2dVerifierRate {
    param(
        [Parameter(Mandatory = $true)]$Numerator,
        [Parameter(Mandatory = $true)]$Denominator
    )

    $numeratorValue = ConvertTo-G2dVerifierDecimal $Numerator 'rate numerator'
    $denominatorValue = ConvertTo-G2dVerifierDecimal $Denominator 'rate denominator'
    if ($denominatorValue -eq 0) { return $null }
    return [Math]::Round(
        ($numeratorValue / $denominatorValue),
        6,
        [MidpointRounding]::AwayFromZero
    ).ToString('0.000000', [Globalization.CultureInfo]::InvariantCulture)
}

function Format-G2dVerifierStoredRate {
    param([Parameter(Mandatory = $true)]$Value)

    $parsed = ConvertTo-G2dVerifierDecimal $Value 'stored quality rate'
    return [Math]::Round(
        $parsed,
        6,
        [MidpointRounding]::AwayFromZero
    ).ToString('0.000000', [Globalization.CultureInfo]::InvariantCulture)
}

function Assert-G2dVerifierFieldEqual {
    param(
        $Expected,
        $Actual,
        [Parameter(Mandatory = $true)][string]$Field,
        [Parameter(Mandatory = $true)][string]$Family
    )

    if ($null -eq $Expected -or $null -eq $Actual) {
        if ($null -ne $Expected -or $null -ne $Actual) {
            throw "G2-D API '$Family' mismatch for '$Field'."
        }
        return
    }
    if ($Field -in @('window_start', 'window_end')) {
        $equal = (ConvertTo-G2dVerifierDate $Expected) -ceq (ConvertTo-G2dVerifierDate $Actual)
    } elseif ($Field -in @('calculated_at', 'published_at')) {
        $equal = (ConvertTo-G2dVerifierTimestamp $Expected) -ceq
            (ConvertTo-G2dVerifierTimestamp $Actual)
    } elseif ($Field -ceq 'is_unknown') {
        $equal = (ConvertTo-G2dVerifierBoolean $Expected) -eq
            (ConvertTo-G2dVerifierBoolean $Actual)
    } else {
        $equal = [Convert]::ToString(
            $Expected,
            [Globalization.CultureInfo]::InvariantCulture
        ) -ceq [Convert]::ToString(
            $Actual,
            [Globalization.CultureInfo]::InvariantCulture
        )
    }
    if (-not $equal) { throw "G2-D API '$Family' mismatch for '$Field'." }
}

function Assert-G2dApiRowsEqual {
    param(
        [Parameter(Mandatory = $true)][object[]]$ExpectedRows,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$ActualRows,
        [Parameter(Mandatory = $true)][string[]]$Fields,
        [Parameter(Mandatory = $true)][string]$Family
    )

    if ($ExpectedRows.Count -ne $ActualRows.Count) {
        throw "G2-D API '$Family' row count does not match Doris."
    }
    for ($index = 0; $index -lt $ExpectedRows.Count; $index++) {
        Assert-G2dVerifierProperties $ActualRows[$index] $Fields "$Family row"
        foreach ($field in $Fields) {
            Assert-G2dVerifierFieldEqual `
                (Get-G2dVerifierValue $ExpectedRows[$index] $field) `
                (Get-G2dVerifierValue $ActualRows[$index] $field) $field $Family
        }
    }
}

function Assert-G2dApiMeta {
    param(
        [Parameter(Mandatory = $true)]$Meta,
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)]$Publication,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $fields = @(
        'dataset_id', 'metric_version', 'metric_run_id', 'source_snapshot_id',
        'window_start', 'window_end', 'calculated_at', 'data_scope',
        'source_event_count', 'warnings'
    )
    Assert-G2dVerifierProperties $Meta $fields "$Name meta"
    $expected = [ordered]@{
        dataset_id = [string]$Identity.DatasetId
        metric_version = [string]$Identity.MetricVersion
        metric_run_id = [string]$Identity.MetricRunId
        source_snapshot_id = [string]$Identity.SourceSnapshotId
        window_start = Get-G2dVerifierValue $Publication 'window_start'
        window_end = Get-G2dVerifierValue $Publication 'window_end'
        calculated_at = Get-G2dVerifierValue $Publication 'calculated_at'
        data_scope = [string]$Identity.DataScope
        source_event_count = [long]$Identity.SourceEventCount
    }
    foreach ($field in $expected.Keys) {
        Assert-G2dVerifierFieldEqual $expected[$field] (Get-G2dVerifierValue $Meta $field) `
            $field "$Name meta"
    }
    $warnings = @(Get-G2dVerifierValue $Meta 'warnings')
    $expectedWarnings = if ([string]$Identity.DataScope -ceq 'g2c-correctness-subset') {
        @($script:G2dSubsetWarning)
    } else {
        @()
    }
    if (($warnings -join "`u{001F}") -cne ($expectedWarnings -join "`u{001F}")) {
        throw "G2-D API '$Name' warnings do not match its data scope."
    }
}

function Assert-G2dDefinitionContracts {
    param(
        [Parameter(Mandatory = $true)]$Actual,
        [Parameter(Mandatory = $true)]$Expected,
        [Parameter(Mandatory = $true)]$Identity
    )

    $documentFields = @('domain', 'dataset_id', 'metric_version', 'definitions')
    Assert-G2dVerifierProperties $Actual $documentFields 'definitions'
    Assert-G2dVerifierProperties $Expected $documentFields 'expected definitions'
    foreach ($field in @('domain', 'dataset_id', 'metric_version')) {
        Assert-G2dVerifierFieldEqual (Get-G2dVerifierValue $Expected $field) `
            (Get-G2dVerifierValue $Actual $field) $field 'definitions'
    }
    if ([string](Get-G2dVerifierValue $Actual 'domain') -cne 'behavior' -or
            [string](Get-G2dVerifierValue $Actual 'dataset_id') -cne [string]$Identity.DatasetId -or
            [string](Get-G2dVerifierValue $Actual 'metric_version') -cne [string]$Identity.MetricVersion) {
        throw 'G2-D definitions API identity is invalid.'
    }

    $expectedDefinitions = @(Get-G2dVerifierValue $Expected 'definitions')
    $actualDefinitions = @(Get-G2dVerifierValue $Actual 'definitions')
    if ($expectedDefinitions.Count -ne $script:G2dExpectedMetricNames.Count -or
            $actualDefinitions.Count -ne $script:G2dExpectedMetricNames.Count) {
        throw 'G2-D definitions API must contain exactly 25 definitions.'
    }
    $definitionFields = @(
        'metric_name', 'display_name', 'formula', 'numerator', 'denominator',
        'source_fields', 'allowed_windows', 'additive', 'null_policy',
        'limitations', 'forbidden_claims'
    )
    $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    for ($index = 0; $index -lt $expectedDefinitions.Count; $index++) {
        $expectedDefinition = $expectedDefinitions[$index]
        $actualDefinition = $actualDefinitions[$index]
        Assert-G2dVerifierProperties $actualDefinition $definitionFields 'definition item'
        Assert-G2dVerifierProperties $expectedDefinition $definitionFields 'expected definition item'
        $name = [string](Get-G2dVerifierValue $actualDefinition 'metric_name')
        if (-not $seen.Add($name) -or $name -cne $script:G2dExpectedMetricNames[$index]) {
            throw 'G2-D definitions API metric identities are duplicated or out of order.'
        }
        foreach ($field in @(
                'metric_name', 'display_name', 'formula', 'numerator', 'denominator',
                'additive', 'null_policy')) {
            Assert-G2dVerifierFieldEqual (Get-G2dVerifierValue $expectedDefinition $field) `
                (Get-G2dVerifierValue $actualDefinition $field) $field 'definitions'
        }
        foreach ($field in @(
                'source_fields', 'allowed_windows', 'limitations', 'forbidden_claims')) {
            $expectedValues = @(Get-G2dVerifierValue $expectedDefinition $field)
            $actualValues = @(Get-G2dVerifierValue $actualDefinition $field)
            if (($expectedValues -join "`u{001F}") -cne ($actualValues -join "`u{001F}")) {
                throw "G2-D definitions API mismatch for '$field'."
            }
        }
        foreach ($limitation in @(Get-G2dVerifierValue $actualDefinition 'limitations')) {
            if ([string]::IsNullOrWhiteSpace([string]$limitation)) {
                throw 'G2-D definitions API contains a blank limitation.'
            }
        }
    }
}

function Assert-G2dApiResponseContracts {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)]$Publication,
        [Parameter(Mandatory = $true)][object[]]$Overview,
        [Parameter(Mandatory = $true)][object[]]$Funnel,
        [Parameter(Mandatory = $true)][object[]]$Dimension,
        [Parameter(Mandatory = $true)]$Quality,
        [Parameter(Mandatory = $true)]$ApiPayloads,
        [Parameter(Mandatory = $true)]$ExpectedDefinitions
    )

    foreach ($name in @('publication', 'overview', 'funnel', 'rankings', 'quality')) {
        $payload = Get-G2dVerifierValue $ApiPayloads $name
        Assert-G2dVerifierProperties $payload @('meta', 'data') $name
        Assert-G2dApiMeta (Get-G2dVerifierValue $payload 'meta') $Identity $Publication $name
    }

    $publicationFields = @(
        'published_at', 'overview_row_count', 'overview_sha256', 'funnel_row_count',
        'funnel_sha256', 'dimension_row_count', 'dimension_sha256',
        'quality_row_count', 'quality_sha256', 'status'
    )
    $apiPublication = Get-G2dVerifierValue `
        (Get-G2dVerifierValue $ApiPayloads 'publication') 'data'
    Assert-G2dVerifierProperties $apiPublication $publicationFields 'publication data'
    foreach ($field in $publicationFields) {
        Assert-G2dVerifierFieldEqual (Get-G2dVerifierValue $Publication $field) `
            (Get-G2dVerifierValue $apiPublication $field) $field 'publication'
    }

    $overviewFields = @(
        'window_type', 'window_start', 'window_end', 'event_count', 'view_count',
        'cart_count', 'purchase_count', 'unique_user_count', 'session_count',
        'product_count', 'purchase_amount_proxy'
    )
    $fullOverview = @($Overview | Where-Object { $_.window_type -ceq 'FULL' })
    Assert-G2dApiRowsEqual $fullOverview `
        @(Get-G2dVerifierValue (Get-G2dVerifierValue $ApiPayloads 'overview') 'data') `
        $overviewFields 'overview'

    $funnelFields = @(
        'window_type', 'window_start', 'window_end', 'missing_session_event_count',
        'view_sessions', 'view_to_cart_sessions', 'completed_sessions',
        'view_to_cart_rate', 'cart_to_purchase_rate', 'full_conversion_rate'
    )
    $fullFunnelRows = @($Funnel | Where-Object { $_.window_type -ceq 'FULL' })
    $expectedFunnel = foreach ($row in $fullFunnelRows) {
        [pscustomobject][ordered]@{
            window_type = $row.window_type
            window_start = $row.window_start
            window_end = $row.window_end
            missing_session_event_count = $row.missing_session_event_count
            view_sessions = $row.view_sessions
            view_to_cart_sessions = $row.view_to_cart_sessions
            completed_sessions = $row.completed_sessions
            view_to_cart_rate = Format-G2dVerifierRate $row.view_to_cart_sessions $row.view_sessions
            cart_to_purchase_rate = Format-G2dVerifierRate $row.completed_sessions $row.view_to_cart_sessions
            full_conversion_rate = Format-G2dVerifierRate $row.completed_sessions $row.view_sessions
        }
    }
    Assert-G2dApiRowsEqual @($expectedFunnel) `
        @(Get-G2dVerifierValue (Get-G2dVerifierValue $ApiPayloads 'funnel') 'data') `
        $funnelFields 'funnel'

    $rankingFields = @(
        'window_type', 'window_start', 'window_end', 'dimension_type', 'dimension_id',
        'dimension_name', 'is_unknown', 'view_count', 'cart_count', 'purchase_count',
        'unique_user_count', 'purchase_amount_proxy'
    )
    $expectedRankings = @(
        $Dimension |
            Where-Object { $_.window_type -ceq 'FULL' -and $_.dimension_type -ceq 'product' } |
            Sort-Object `
                @{ Expression = { [long]$_.purchase_count }; Descending = $true }, `
                @{ Expression = { [string]$_.dimension_id }; Descending = $false }, `
                @{ Expression = { [string]$_.window_start }; Descending = $false } |
            Select-Object -First 20
    )
    Assert-G2dApiRowsEqual $expectedRankings `
        @(Get-G2dVerifierValue (Get-G2dVerifierValue $ApiPayloads 'rankings') 'data') `
        $rankingFields 'rankings'

    $qualityFields = @(
        'window_type', 'window_start', 'window_end', 'source_event_count',
        'clean_event_count', 'late_event_count', 'clean_event_rate', 'late_event_rate',
        'distinct_event_count', 'duplicate_event_count', 'missing_session_count',
        'unknown_category_count', 'unknown_brand_count', 'invalid_event_type_count',
        'empty_key_id_count', 'invalid_price_count', 'invalid_derived_date_count',
        'overview_event_count', 'reconciliation_status'
    )
    $qualityRows = @($Quality)
    if ($qualityRows.Count -ne 1) { throw 'G2-D quality truth must contain one row.' }
    $qualityRow = $qualityRows[0]
    $expectedQuality = [pscustomobject][ordered]@{}
    foreach ($field in $qualityFields) {
        $value = if ($field -ceq 'clean_event_rate') {
            Format-G2dVerifierStoredRate $qualityRow.clean_event_rate
        } elseif ($field -ceq 'late_event_rate') {
            Format-G2dVerifierStoredRate $qualityRow.late_event_rate
        } else {
            Get-G2dVerifierValue $qualityRow $field
        }
        Add-Member -InputObject $expectedQuality -NotePropertyName $field -NotePropertyValue $value
    }
    Assert-G2dApiRowsEqual @($expectedQuality) `
        @((Get-G2dVerifierValue (Get-G2dVerifierValue $ApiPayloads 'quality') 'data')) `
        $qualityFields 'quality'

    Assert-G2dDefinitionContracts `
        (Get-G2dVerifierValue $ApiPayloads 'definitions') $ExpectedDefinitions $Identity
    return @($script:G2dApiContracts)
}

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

    $definitionsPath = Join-Path $verifierProjectRoot 'configs/metrics/behavior-v1.json'
    $expectedDefinitions = [IO.File]::ReadAllText($definitionsPath, [Text.Encoding]::UTF8) |
        ConvertFrom-Json
    $null = Assert-G2dApiResponseContracts -Identity $identity -Publication $publication `
        -Overview $metricRows.overview -Funnel $metricRows.funnel `
        -Dimension $metricRows.dimension -Quality $metricRows.quality `
        -ApiPayloads $apiPayloads -ExpectedDefinitions $expectedDefinitions

    $definitions = $apiPayloads.definitions
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
