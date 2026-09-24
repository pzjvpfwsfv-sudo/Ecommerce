[CmdletBinding()]
param(
    [string]$ManifestPath = '',
    [string]$ApiBaseUrl = 'http://localhost:8000',
    [int]$TimeoutSeconds = 300,
    [switch]$FunctionsOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$acceptanceManifestPath = $ManifestPath
$acceptanceApiBaseUrl = $ApiBaseUrl
$acceptanceTimeoutSeconds = $TimeoutSeconds
$acceptanceFunctionsOnly = [bool]$FunctionsOnly

$script:G2eAcceptanceDatasetId = 'olist-brazilian-ecommerce-v2'
$script:G2eAcceptanceMetricVersion = 'orders-v1'
$script:G2eAcceptanceCurrencyWarning = [Text.Encoding]::UTF8.GetString(
    [Convert]::FromBase64String(
        '5rqQ5pWw5o2u5biB56eN5bCa5pyq5qC46aqM77yM5LiN5bGV56S66LSn5biB56ym5Y+35oiW5omn6KGM5rGH546H5o2i566X44CC'
    )
)
$script:G2eAcceptanceSourceTables = [ordered]@{
    orders = 'orders_src_v1'
    order_items = 'order_items_src_v1'
    order_payments = 'order_payments_src_v1'
    order_reviews = 'order_reviews_src_v1'
    customers = 'customers_src_v1'
    products = 'products_src_v1'
    sellers = 'sellers_src_v1'
    geolocation = 'geolocation_src_v1'
    category_translation = 'category_translation_src_v1'
}
$script:G2eAcceptanceCuratedTables = [string[]]@(
    'customer_dim_v1', 'category_dim_v1', 'product_dim_v1', 'seller_dim_v1',
    'geolocation_dim_v1', 'order_fact_v1', 'order_item_fact_v1', 'payment_fact_v1',
    'review_fact_v1'
)
$script:G2eAcceptanceFamilies = [string[]]@(
    'overview', 'delivery', 'payment', 'ranking', 'review', 'quality'
)
$script:G2eAcceptanceEndpoints = [string[]]@(
    '/api/v1/orders/publication',
    '/api/v1/orders/overview?window=full',
    '/api/v1/orders/delivery?window=full',
    '/api/v1/orders/payments?window=full',
    '/api/v1/orders/rankings?dimension=category&window=full&sort_by=item_value&limit=20',
    '/api/v1/orders/reviews?window=full',
    '/api/v1/orders/quality',
    '/api/v1/metrics/definitions?domain=orders&version=orders-v1'
)
$script:G2eAcceptanceResponseNames = [string[]]@(
    'publication', 'overview', 'delivery', 'payments', 'rankings', 'reviews',
    'quality', 'definitions', 'day_overview', 'month_overview'
)
$script:G2eAcceptanceVerificationNames = [string[]]@(
    'trino_snapshot_identity', 'trino_fact_reconciliation',
    'doris_candidate_digests', 'api_full_contracts', 'api_day_range',
    'api_month_range', 'invalid_requests_422'
)

function Get-G2eAcceptancePropertyNames {
    param([Parameter(Mandatory = $true)]$Object)

    if ($Object -is [Collections.IDictionary]) {
        return [string[]]@($Object.Keys | ForEach-Object { [string]$_ })
    }
    if ($Object -is [pscustomobject]) {
        return [string[]]@($Object.PSObject.Properties | ForEach-Object { [string]$_.Name })
    }
    throw 'G2-E acceptance value must be an object.'
}

function Get-G2eAcceptanceProperty {
    param(
        [Parameter(Mandatory = $true)]$Object,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Label
    )

    foreach ($propertyName in @(Get-G2eAcceptancePropertyNames $Object)) {
        if ($propertyName -ceq $Name) {
            if ($Object -is [Collections.IDictionary]) { return $Object[$propertyName] }
            return $Object.PSObject.Properties[$propertyName].Value
        }
    }
    throw "G2-E acceptance $Label is missing $Name."
}

function Assert-G2eAcceptanceExactKeys {
    param(
        [Parameter(Mandatory = $true)]$Object,
        [Parameter(Mandatory = $true)][string[]]$Expected,
        [Parameter(Mandatory = $true)][string]$Label
    )

    $actual = [string[]]@(Get-G2eAcceptancePropertyNames $Object)
    if ($actual.Count -ne $Expected.Count) { throw "G2-E acceptance $Label key set differs." }
    foreach ($name in $Expected) {
        if (-not ($actual -ccontains $name)) { throw "G2-E acceptance $Label key set differs." }
    }
    return $Object
}

function Assert-G2eAcceptanceSha256 {
    param($Value, [string]$Label)
    if ($Value -isnot [string] -or [string]$Value -cnotmatch '^[0-9a-f]{64}$') {
        throw "G2-E acceptance $Label must be a lowercase SHA-256."
    }
    return [string]$Value
}

function Assert-G2eAcceptanceRevision {
    param($Value)
    if ($Value -isnot [string] -or [string]$Value -cnotmatch '^[0-9a-f]{40}$') {
        throw 'G2-E acceptance implementation revision is invalid.'
    }
    return [string]$Value
}

function Test-G2eAcceptanceInteger {
    param($Value)
    if ($Value -is [bool] -or $null -eq $Value) { return $false }
    return $Value -is [byte] -or $Value -is [sbyte] -or
        $Value -is [int16] -or $Value -is [uint16] -or
        $Value -is [int32] -or $Value -is [uint32] -or
        $Value -is [int64] -or $Value -is [uint64]
}

function Assert-G2eAcceptanceCount {
    param($Value, [string]$Label, [switch]$Positive, [switch]$AllowString)
    [long]$number = 0
    if (Test-G2eAcceptanceInteger $Value) {
        $number = [int64]$Value
    } elseif ($AllowString -and $Value -is [string] -and
            [string]$Value -cmatch '^(?:0|[1-9][0-9]*)$' -and
            [int64]::TryParse(
                [string]$Value,
                [Globalization.NumberStyles]::None,
                [Globalization.CultureInfo]::InvariantCulture,
                [ref]$number
            )) {
    } else {
        throw "G2-E acceptance $Label must be an integer."
    }
    if ($number -lt 0 -or ($Positive -and $number -eq 0)) {
        throw "G2-E acceptance $Label is outside its supported range."
    }
    return $number
}

function Assert-G2eAcceptanceDate {
    param($Value, [string]$Label)
    if ($Value -isnot [string] -or [string]$Value -cnotmatch '^\d{4}-\d{2}-\d{2}$') {
        throw "G2-E acceptance $Label must be an ISO date."
    }
    try {
        $null = [datetime]::ParseExact(
            [string]$Value,
            'yyyy-MM-dd',
            [Globalization.CultureInfo]::InvariantCulture
        )
    } catch { throw "G2-E acceptance $Label must be an ISO date." }
    return [string]$Value
}

function ConvertTo-G2eAcceptanceUtcTimestamp {
    param($Value, [string]$Label)
    if ($Value -is [datetimeoffset]) {
        return $Value.ToUniversalTime().ToString(
            'o', [Globalization.CultureInfo]::InvariantCulture
        )
    }
    if ($Value -is [datetime]) {
        $dateTime = [datetime]$Value
        if ($dateTime.Kind -eq [DateTimeKind]::Unspecified) {
            $dateTime = [datetime]::SpecifyKind($dateTime, [DateTimeKind]::Utc)
        }
        return ([datetimeoffset]$dateTime).ToUniversalTime().ToString(
            'o', [Globalization.CultureInfo]::InvariantCulture
        )
    }
    if ($Value -isnot [string] -or [string]::IsNullOrWhiteSpace([string]$Value)) {
        throw "G2-E acceptance $Label timestamp is invalid."
    }
    $parsed = [datetimeoffset]::MinValue
    if (-not [datetimeoffset]::TryParse(
            [string]$Value,
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::AssumeUniversal,
            [ref]$parsed
        )) {
        throw "G2-E acceptance $Label timestamp is invalid."
    }
    return $parsed.ToUniversalTime().ToString('o', [Globalization.CultureInfo]::InvariantCulture)
}

function Test-G2eAcceptanceObject {
    param($Value)
    return $Value -is [Collections.IDictionary] -or $Value -is [pscustomobject]
}

function Test-G2eAcceptanceSequence {
    param($Value)
    return $null -ne $Value -and $Value -is [Collections.IEnumerable] -and
        $Value -isnot [string] -and -not (Test-G2eAcceptanceObject $Value)
}

function Assert-G2eAcceptanceValueEqual {
    param(
        $Actual,
        $Expected,
        [string]$Path = 'value'
    )

    if ($null -eq $Actual -or $null -eq $Expected) {
        if ($null -ne $Actual -or $null -ne $Expected) {
            throw "G2-E acceptance $Path differs."
        }
        return
    }
    $actualObject = Test-G2eAcceptanceObject $Actual
    $expectedObject = Test-G2eAcceptanceObject $Expected
    if ($actualObject -or $expectedObject) {
        if (-not $actualObject -or -not $expectedObject) {
            throw "G2-E acceptance $Path differs."
        }
        $expectedNames = [string[]]@(Get-G2eAcceptancePropertyNames $Expected)
        $null = Assert-G2eAcceptanceExactKeys $Actual $expectedNames $Path
        foreach ($name in $expectedNames) {
            Assert-G2eAcceptanceValueEqual `
                (Get-G2eAcceptanceProperty $Actual $name $Path) `
                (Get-G2eAcceptanceProperty $Expected $name $Path) "$Path.$name"
        }
        return
    }
    $actualSequence = Test-G2eAcceptanceSequence $Actual
    $expectedSequence = Test-G2eAcceptanceSequence $Expected
    if ($actualSequence -or $expectedSequence) {
        if (-not $actualSequence -or -not $expectedSequence) {
            throw "G2-E acceptance $Path differs."
        }
        $actualItems = @($Actual)
        $expectedItems = @($Expected)
        if ($actualItems.Count -ne $expectedItems.Count) {
            throw "G2-E acceptance $Path differs."
        }
        for ($index = 0; $index -lt $actualItems.Count; $index++) {
            Assert-G2eAcceptanceValueEqual $actualItems[$index] $expectedItems[$index] `
                "$Path[$index]"
        }
        return
    }
    if ($Actual -is [bool] -or $Expected -is [bool]) {
        if ($Actual -isnot [bool] -or $Expected -isnot [bool] -or $Actual -ne $Expected) {
            throw "G2-E acceptance $Path differs."
        }
        return
    }
    if ((Test-G2eAcceptanceInteger $Actual) -or (Test-G2eAcceptanceInteger $Expected)) {
        if (-not (Test-G2eAcceptanceInteger $Actual) -or
                -not (Test-G2eAcceptanceInteger $Expected) -or
                [int64]$Actual -ne [int64]$Expected) {
            throw "G2-E acceptance $Path differs."
        }
        return
    }
    if ($Actual -is [datetime] -or $Actual -is [datetimeoffset] -or
            $Expected -is [datetime] -or $Expected -is [datetimeoffset]) {
        try {
            $actualTimestamp = ConvertTo-G2eAcceptanceUtcTimestamp $Actual $Path
            $expectedTimestamp = ConvertTo-G2eAcceptanceUtcTimestamp $Expected $Path
        } catch { throw "G2-E acceptance $Path differs." }
        if ($actualTimestamp -cne $expectedTimestamp) {
            throw "G2-E acceptance $Path differs."
        }
        return
    }
    if ($Actual -isnot [string] -or $Expected -isnot [string] -or
            [string]$Actual -cne [string]$Expected) {
        throw "G2-E acceptance $Path differs."
    }
}

function ConvertFrom-G2eAcceptanceJsonObject {
    param($Value, [string]$Label)
    if ($Value -isnot [string]) { throw "G2-E acceptance $Label must be JSON text." }
    try { $parsed = [string]$Value | ConvertFrom-Json } catch {
        throw "G2-E acceptance $Label is malformed."
    }
    if (-not (Test-G2eAcceptanceObject $parsed)) {
        throw "G2-E acceptance $Label must be a JSON object."
    }
    return $parsed
}

function Assert-G2eAcceptanceSnapshotMap {
    param(
        [Parameter(Mandatory = $true)]$Map,
        [Parameter(Mandatory = $true)][string[]]$ExpectedTables,
        [Parameter(Mandatory = $true)][string]$Label
    )
    $null = Assert-G2eAcceptanceExactKeys $Map $ExpectedTables $Label
    foreach ($table in $ExpectedTables) {
        $snapshot = Get-G2eAcceptanceProperty $Map $table $Label
        if ($snapshot -isnot [string] -or [string]$snapshot -cnotmatch '^[1-9][0-9]*$') {
            throw "G2-E acceptance $Label contains an invalid Snapshot ID."
        }
    }
    return $Map
}

function Assert-G2eAcceptanceIdentity {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Manifest,
        [Parameter(Mandatory = $true)]$SourceReports,
        [Parameter(Mandatory = $true)]$CuratedReport,
        [Parameter(Mandatory = $true)]$RefreshReport,
        [Parameter(Mandatory = $true)]$Publication
    )

    if ([string](Get-G2eAcceptanceProperty $Manifest 'dataset_id' 'manifest') -cne
            $script:G2eAcceptanceDatasetId) {
        throw 'G2-E acceptance manifest dataset identity differs.'
    }
    $bundle = Assert-G2eAcceptanceSha256 `
        (Get-G2eAcceptanceProperty $Manifest 'source_bundle_sha256' 'manifest') `
        'source bundle'
    $files = @(Get-G2eAcceptanceProperty $Manifest 'files' 'manifest')
    if ($files.Count -ne $script:G2eAcceptanceSourceTables.Count) {
        throw 'G2-E acceptance manifest file set differs.'
    }
    $manifestFiles = @{}
    foreach ($file in $files) {
        $entity = [string](Get-G2eAcceptanceProperty $file 'entity' 'manifest file')
        if (-not ($script:G2eAcceptanceSourceTables.Keys -ccontains $entity) -or
                $manifestFiles.ContainsKey($entity)) {
            throw 'G2-E acceptance manifest entity set differs.'
        }
        $rowCount = Assert-G2eAcceptanceCount `
            (Get-G2eAcceptanceProperty $file 'row_count' "$entity manifest file") `
            "$entity manifest row count" -Positive
        $normalized = Get-G2eAcceptanceProperty $file 'normalized' "$entity manifest file"
        $normalizedRows = Assert-G2eAcceptanceCount `
            (Get-G2eAcceptanceProperty $normalized 'row_count' "$entity normalized file") `
            "$entity normalized row count" -Positive
        if ($rowCount -ne $normalizedRows) {
            throw "G2-E acceptance normalized row count differs for $entity."
        }
        $manifestFiles[$entity] = $file
    }

    $reports = @($SourceReports)
    if ($reports.Count -ne $script:G2eAcceptanceSourceTables.Count) {
        throw 'G2-E acceptance source report set differs.'
    }
    $sourceSnapshots = [ordered]@{}
    $seenReports = @{}
    foreach ($report in $reports) {
        $entity = [string](Get-G2eAcceptanceProperty $report 'entity' 'source report')
        if (-not ($script:G2eAcceptanceSourceTables.Keys -ccontains $entity) -or
                $seenReports.ContainsKey($entity)) {
            throw 'G2-E acceptance source report entity set differs.'
        }
        $seenReports[$entity] = $true
        if ([string](Get-G2eAcceptanceProperty $report 'status' "$entity source report") -cne 'PASS' -or
                [string](Get-G2eAcceptanceProperty $report 'dataset_id' "$entity source report") -cne
                    $script:G2eAcceptanceDatasetId -or
                [string](Get-G2eAcceptanceProperty $report 'source_bundle_sha256' "$entity source report") -cne
                    $bundle) {
            throw "G2-E acceptance source report identity differs for $entity."
        }
        $file = $manifestFiles[$entity]
        $expectedRows = Assert-G2eAcceptanceCount `
            (Get-G2eAcceptanceProperty $file 'row_count' "$entity manifest file") `
            "$entity manifest rows" -Positive
        $reportRows = Assert-G2eAcceptanceCount `
            (Get-G2eAcceptanceProperty $report 'expected_row_count' "$entity source report") `
            "$entity expected rows" -Positive
        $iceberg = Get-G2eAcceptanceProperty $report 'iceberg' "$entity source report"
        $icebergRows = Assert-G2eAcceptanceCount `
            (Get-G2eAcceptanceProperty $iceberg 'row_count' "$entity Iceberg report") `
            "$entity Iceberg rows" -Positive
        if ($expectedRows -ne $reportRows -or $expectedRows -ne $icebergRows) {
            throw "G2-E acceptance source row count differs for $entity."
        }
        $normalized = Get-G2eAcceptanceProperty $file 'normalized' "$entity manifest file"
        foreach ($field in @('normalized_sha256', 'source_row_id_sequence_sha256')) {
            $manifestField = if ($field -ceq 'normalized_sha256') { 'sha256' } else { $field }
            $left = Assert-G2eAcceptanceSha256 `
                (Get-G2eAcceptanceProperty $normalized $manifestField "$entity normalized file") `
                "$entity normalized identity"
            $right = Assert-G2eAcceptanceSha256 `
                (Get-G2eAcceptanceProperty $report $field "$entity source report") `
                "$entity source report identity"
            if ($left -cne $right) {
                throw "G2-E acceptance source normalized identity differs for $entity."
            }
        }
        $target = [string](Get-G2eAcceptanceProperty $report 'target_table' "$entity source report")
        if ($target -cne [string]$script:G2eAcceptanceSourceTables[$entity]) {
            throw "G2-E acceptance source target differs for $entity."
        }
        $snapshot = [string](Get-G2eAcceptanceProperty $iceberg 'snapshot_id' "$entity Iceberg report")
        if ($snapshot -cnotmatch '^[1-9][0-9]*$') {
            throw "G2-E acceptance source snapshot is invalid for $entity."
        }
        $sourceSnapshots[$target] = $snapshot
    }

    if ([string](Get-G2eAcceptanceProperty $CuratedReport 'status' 'curated report') -cne 'PASS' -or
            [string](Get-G2eAcceptanceProperty $CuratedReport 'dataset_id' 'curated report') -cne
                $script:G2eAcceptanceDatasetId -or
            [string](Get-G2eAcceptanceProperty $CuratedReport 'source_bundle_sha256' 'curated report') -cne
                $bundle) {
        throw 'G2-E acceptance curated report identity differs.'
    }
    $curatedSourceSnapshots = Get-G2eAcceptanceProperty `
        $CuratedReport 'source_snapshots' 'curated report'
    $null = Assert-G2eAcceptanceSnapshotMap $curatedSourceSnapshots `
        ([string[]]$script:G2eAcceptanceSourceTables.Values) 'curated source snapshots'
    Assert-G2eAcceptanceValueEqual $curatedSourceSnapshots $sourceSnapshots `
        'curated source snapshot identity'
    $curatedSnapshots = Get-G2eAcceptanceProperty `
        $CuratedReport 'curated_snapshots' 'curated report'
    $null = Assert-G2eAcceptanceSnapshotMap $curatedSnapshots `
        $script:G2eAcceptanceCuratedTables 'curated snapshots'

    $expectedRun = "$($script:G2eAcceptanceMetricVersion)-b$bundle"
    $revision = Assert-G2eAcceptanceRevision `
        (Get-G2eAcceptanceProperty $RefreshReport 'implementation_revision' 'refresh report')
    if ([string](Get-G2eAcceptanceProperty $RefreshReport 'status' 'refresh report') -notin
            @('published', 'already_published') -or
            [string](Get-G2eAcceptanceProperty $RefreshReport 'metric_run_id' 'refresh report') -cne
                $expectedRun -or
            [string](Get-G2eAcceptanceProperty $RefreshReport 'source_bundle_sha256' 'refresh report') -cne
                $bundle) {
        throw 'G2-E acceptance refresh identity differs.'
    }
    $publicationRun = [string](Get-G2eAcceptanceProperty $Publication 'metric_run_id' 'publication')
    if ($publicationRun -cne $expectedRun -or
            [string](Get-G2eAcceptanceProperty $Publication 'dataset_id' 'publication') -cne
                $script:G2eAcceptanceDatasetId -or
            [string](Get-G2eAcceptanceProperty $Publication 'metric_version' 'publication') -cne
                $script:G2eAcceptanceMetricVersion -or
            [string](Get-G2eAcceptanceProperty $Publication 'source_bundle_sha256' 'publication') -cne
                $bundle -or
            [string](Get-G2eAcceptanceProperty $Publication 'implementation_revision' 'publication') -cne
                $revision -or
            [string](Get-G2eAcceptanceProperty $Publication 'status' 'publication') -cne 'PUBLISHED') {
        throw 'G2-E acceptance publication identity differs.'
    }
    $publishedSourceSnapshots = ConvertFrom-G2eAcceptanceJsonObject `
        (Get-G2eAcceptanceProperty $Publication 'source_snapshots_json' 'publication') `
        'publication source snapshots'
    $publishedCuratedSnapshots = ConvertFrom-G2eAcceptanceJsonObject `
        (Get-G2eAcceptanceProperty $Publication 'curated_snapshots_json' 'publication') `
        'publication curated snapshots'
    $null = Assert-G2eAcceptanceSnapshotMap $publishedSourceSnapshots `
        ([string[]]$script:G2eAcceptanceSourceTables.Values) 'publication source snapshots'
    $null = Assert-G2eAcceptanceSnapshotMap $publishedCuratedSnapshots `
        $script:G2eAcceptanceCuratedTables 'publication curated snapshots'
    Assert-G2eAcceptanceValueEqual $publishedSourceSnapshots $sourceSnapshots `
        'publication source snapshot identity'
    Assert-G2eAcceptanceValueEqual $publishedCuratedSnapshots $curatedSnapshots `
        'publication curated snapshot identity'

    $ordersFile = $manifestFiles['orders']
    $sourceOrderCount = Assert-G2eAcceptanceCount `
        (Get-G2eAcceptanceProperty $ordersFile 'row_count' 'orders manifest file') `
        'source order count' -Positive
    $publicationOrderCount = Assert-G2eAcceptanceCount `
        (Get-G2eAcceptanceProperty $Publication 'source_order_count' 'publication') `
        'publication source order count' -Positive -AllowString
    if ($sourceOrderCount -ne $publicationOrderCount) {
        throw 'G2-E acceptance source order count differs.'
    }
    $windowStart = Assert-G2eAcceptanceDate `
        (Get-G2eAcceptanceProperty $Publication 'window_start' 'publication') 'window start'
    $windowEnd = Assert-G2eAcceptanceDate `
        (Get-G2eAcceptanceProperty $Publication 'window_end' 'publication') 'window end'
    if ([datetime]$windowStart -gt [datetime]$windowEnd) {
        throw 'G2-E acceptance publication window is invalid.'
    }
    $calculatedAt = ConvertTo-G2eAcceptanceUtcTimestamp `
        (Get-G2eAcceptanceProperty $Publication 'calculated_at' 'publication') 'calculated_at'

    return [pscustomobject][ordered]@{
        DatasetId = $script:G2eAcceptanceDatasetId
        MetricVersion = $script:G2eAcceptanceMetricVersion
        MetricRunId = $expectedRun
        SourceBundleSha256 = $bundle
        SourceSnapshots = $sourceSnapshots
        CuratedSnapshots = $curatedSnapshots
        SourceOrderCount = $sourceOrderCount
        WindowStart = $windowStart
        WindowEnd = $windowEnd
        CalculatedAt = $calculatedAt
        ImplementationRevision = $revision
    }
}

function Assert-G2eApiMeta {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Meta,
        [Parameter(Mandatory = $true)]$Identity
    )

    foreach ($pair in @(
            @('dataset_id', 'DatasetId'), @('metric_version', 'MetricVersion'),
            @('metric_run_id', 'MetricRunId'), @('source_bundle_sha256', 'SourceBundleSha256'),
            @('window_start', 'WindowStart'), @('window_end', 'WindowEnd'),
            @('implementation_revision', 'ImplementationRevision')
        )) {
        if ([string](Get-G2eAcceptanceProperty $Meta $pair[0] 'API meta') -cne
                [string]$Identity.($pair[1])) {
            throw "G2-E acceptance API meta differs for $($pair[0])."
        }
    }
    $sourceCount = Assert-G2eAcceptanceCount `
        (Get-G2eAcceptanceProperty $Meta 'source_order_count' 'API meta') `
        'API source order count' -Positive
    if ($sourceCount -ne [int64]$Identity.SourceOrderCount) {
        throw 'G2-E acceptance API source order count differs.'
    }
    if ([string](Get-G2eAcceptanceProperty $Meta 'source_timezone' 'API meta') -cne 'unspecified') {
        throw 'G2-E acceptance API source timezone differs.'
    }
    if ($null -ne (Get-G2eAcceptanceProperty $Meta 'source_currency' 'API meta')) {
        throw 'G2-E acceptance API source currency must remain unverified.'
    }
    $warnings = @(Get-G2eAcceptanceProperty $Meta 'warnings' 'API meta')
    if ($warnings.Count -ne 1 -or [string]$warnings[0] -cne
            $script:G2eAcceptanceCurrencyWarning) {
        throw 'G2-E acceptance API currency warning differs.'
    }
    $apiCalculatedAt = ConvertTo-G2eAcceptanceUtcTimestamp `
        (Get-G2eAcceptanceProperty $Meta 'calculated_at' 'API meta') 'API calculated_at'
    if ($apiCalculatedAt -cne [string]$Identity.CalculatedAt) {
        throw 'G2-E acceptance API calculated timestamp differs.'
    }
    $apiSource = Get-G2eAcceptanceProperty $Meta 'source_snapshots' 'API meta'
    $apiCurated = Get-G2eAcceptanceProperty $Meta 'curated_snapshots' 'API meta'
    $null = Assert-G2eAcceptanceSnapshotMap $apiSource `
        ([string[]]$script:G2eAcceptanceSourceTables.Values) 'API source snapshots'
    $null = Assert-G2eAcceptanceSnapshotMap $apiCurated `
        $script:G2eAcceptanceCuratedTables 'API curated snapshots'
    Assert-G2eAcceptanceValueEqual $apiSource $Identity.SourceSnapshots `
        'API source snapshot identity'
    Assert-G2eAcceptanceValueEqual $apiCurated $Identity.CuratedSnapshots `
        'API curated snapshot identity'
    return $Meta
}

function Assert-G2eApiRows {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][ValidateSet(
            'overview', 'delivery', 'payments', 'rankings', 'reviews', 'quality',
            'day_overview', 'month_overview'
        )][string]$Family,
        [Parameter(Mandatory = $true)]$Actual,
        [Parameter(Mandatory = $true)]$Expected
    )

    Assert-G2eAcceptanceValueEqual $Actual $Expected "API rows for $Family"
    return $Actual
}

function Assert-G2eAcceptanceEvidence {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)]$Evidence)

    if ((Assert-G2eAcceptanceCount `
                (Get-G2eAcceptanceProperty $Evidence 'schema_version' 'evidence') `
                'schema version' -Positive) -ne 1 -or
            [string](Get-G2eAcceptanceProperty $Evidence 'status' 'evidence') -cne 'PASS') {
        throw 'G2-E acceptance evidence status is invalid.'
    }
    $manifest = Get-G2eAcceptanceProperty $Evidence 'manifest' 'evidence'
    $sourceReports = Get-G2eAcceptanceProperty $Evidence 'source_reports' 'evidence'
    $curated = Get-G2eAcceptanceProperty $Evidence 'curated' 'evidence'
    $refresh = Get-G2eAcceptanceProperty $Evidence 'refresh' 'evidence'
    $doris = Get-G2eAcceptanceProperty $Evidence 'doris' 'evidence'
    $publication = Get-G2eAcceptanceProperty $doris 'publication' 'Doris evidence'
    $identity = Assert-G2eAcceptanceIdentity -Manifest $manifest `
        -SourceReports $sourceReports -CuratedReport $curated `
        -RefreshReport $refresh -Publication $publication

    foreach ($pair in @(
            @('dataset_id', 'DatasetId'), @('source_bundle_sha256', 'SourceBundleSha256'),
            @('metric_run_id', 'MetricRunId'), @('implementation_revision', 'ImplementationRevision'),
            @('window_start', 'WindowStart'), @('window_end', 'WindowEnd')
        )) {
        if ([string](Get-G2eAcceptanceProperty $Evidence $pair[0] 'evidence') -cne
                [string]$identity.($pair[1])) {
            throw "G2-E acceptance top-level identity differs for $($pair[0])."
        }
    }
    if ((Assert-G2eAcceptanceCount `
                (Get-G2eAcceptanceProperty $Evidence 'source_order_count' 'evidence') `
                'top-level source order count' -Positive) -ne [int64]$identity.SourceOrderCount) {
        throw 'G2-E acceptance top-level source order count differs.'
    }

    $hardGate = Get-G2eAcceptanceProperty $curated 'hard_gate' 'curated report'
    $hardGateNames = @(Get-G2eAcceptancePropertyNames $hardGate)
    if ($hardGateNames.Count -eq 0) { throw 'G2-E acceptance hard gate is empty.' }
    foreach ($name in $hardGateNames) {
        if ((Assert-G2eAcceptanceCount `
                    (Get-G2eAcceptanceProperty $hardGate $name 'curated hard gate') `
                    "hard gate $name" -AllowString) -ne 0) {
            throw 'G2-E acceptance curated hard gate did not pass.'
        }
    }

    $refreshPublication = Get-G2eAcceptanceProperty $refresh 'publication' 'refresh report'
    Assert-G2eAcceptanceValueEqual $refreshPublication $publication 'refresh/Doris publication'
    $refreshCandidates = Get-G2eAcceptanceProperty $refresh 'candidates' 'refresh report'
    $familyEvidence = Get-G2eAcceptanceProperty $doris 'families' 'Doris evidence'
    $null = Assert-G2eAcceptanceExactKeys $refreshCandidates `
        $script:G2eAcceptanceFamilies 'refresh candidate families'
    $null = Assert-G2eAcceptanceExactKeys $familyEvidence `
        $script:G2eAcceptanceFamilies 'Doris candidate families'
    foreach ($family in $script:G2eAcceptanceFamilies) {
        $refreshFamily = Get-G2eAcceptanceProperty $refreshCandidates $family 'refresh candidates'
        $dorisFamily = Get-G2eAcceptanceProperty $familyEvidence $family 'Doris families'
        $rowField = "${family}_row_count"
        $digestField = "${family}_sha256"
        $publishedRows = Assert-G2eAcceptanceCount `
            (Get-G2eAcceptanceProperty $publication $rowField 'publication') `
            "$family publication rows" -Positive -AllowString
        $publishedDigest = Assert-G2eAcceptanceSha256 `
            (Get-G2eAcceptanceProperty $publication $digestField 'publication') `
            "$family publication digest"
        foreach ($candidate in @($refreshFamily, $dorisFamily)) {
            $rows = Assert-G2eAcceptanceCount `
                (Get-G2eAcceptanceProperty $candidate 'row_count' "$family evidence") `
                "$family evidence rows" -Positive
            $digest = Assert-G2eAcceptanceSha256 `
                (Get-G2eAcceptanceProperty $candidate 'sha256' "$family evidence") `
                "$family evidence digest"
            if ($rows -ne $publishedRows -or $digest -cne $publishedDigest) {
                throw "G2-E acceptance row count or digest differs for $family."
            }
        }
    }

    $api = Get-G2eAcceptanceProperty $Evidence 'api' 'evidence'
    $endpoints = @(Get-G2eAcceptanceProperty $api 'endpoints' 'API evidence')
    if ($endpoints.Count -ne $script:G2eAcceptanceEndpoints.Count) {
        throw 'G2-E acceptance API endpoint list differs.'
    }
    for ($index = 0; $index -lt $endpoints.Count; $index++) {
        if ([string]$endpoints[$index] -cne $script:G2eAcceptanceEndpoints[$index]) {
            throw 'G2-E acceptance API endpoint list differs.'
        }
    }
    $responses = Get-G2eAcceptanceProperty $api 'responses' 'API evidence'
    $null = Assert-G2eAcceptanceExactKeys $responses `
        $script:G2eAcceptanceResponseNames 'API responses'
    foreach ($name in @(
            'publication', 'overview', 'delivery', 'payments', 'rankings', 'reviews',
            'quality', 'day_overview', 'month_overview'
        )) {
        $response = Get-G2eAcceptanceProperty $responses $name 'API responses'
        $null = Assert-G2eApiMeta `
            (Get-G2eAcceptanceProperty $response 'meta' "$name response") $identity
    }

    $publicationResponse = Get-G2eAcceptanceProperty $responses 'publication' 'API responses'
    $publicationData = Get-G2eAcceptanceProperty `
        $publicationResponse 'data' 'publication response'
    if ([string](Get-G2eAcceptanceProperty $publicationData 'status' 'publication response') -cne
            'PUBLISHED') {
        throw 'G2-E acceptance API publication status differs.'
    }
    foreach ($family in $script:G2eAcceptanceFamilies) {
        $rowField = "${family}_row_count"
        $actualRows = Assert-G2eAcceptanceCount `
            (Get-G2eAcceptanceProperty $publicationData $rowField 'publication response') `
            "API publication $rowField" -Positive
        $expectedRows = Assert-G2eAcceptanceCount `
            (Get-G2eAcceptanceProperty $publication $rowField 'publication') `
            "Doris publication $rowField" -Positive -AllowString
        if ($actualRows -ne $expectedRows) {
            throw "G2-E acceptance API publication $rowField differs."
        }
        $digestField = "${family}_sha256"
        $actualDigest = Assert-G2eAcceptanceSha256 `
            (Get-G2eAcceptanceProperty $publicationData $digestField 'publication response') `
            "API publication $digestField"
        $expectedDigest = Assert-G2eAcceptanceSha256 `
            (Get-G2eAcceptanceProperty $publication $digestField 'publication') `
            "Doris publication $digestField"
        if ($actualDigest -cne $expectedDigest) {
            throw "G2-E acceptance API publication $digestField differs."
        }
    }
    $apiPublishedAt = ConvertTo-G2eAcceptanceUtcTimestamp `
        (Get-G2eAcceptanceProperty $publicationData 'published_at' 'publication response') `
        'API published_at'
    $storedPublishedAt = ConvertTo-G2eAcceptanceUtcTimestamp `
        (Get-G2eAcceptanceProperty $publication 'published_at' 'publication') `
        'Doris published_at'
    if ($apiPublishedAt -cne $storedPublishedAt) {
        throw 'G2-E acceptance API publication timestamp differs.'
    }

    $expectedRows = Get-G2eAcceptanceProperty $doris 'api_rows' 'Doris evidence'
    $rowResponseNames = [string[]]@(
        'overview', 'delivery', 'payments', 'rankings', 'reviews', 'quality',
        'day_overview', 'month_overview'
    )
    $null = Assert-G2eAcceptanceExactKeys $expectedRows `
        $rowResponseNames 'Doris API row projections'
    foreach ($name in $rowResponseNames) {
        $actualResponse = Get-G2eAcceptanceProperty $responses $name 'API responses'
        $actualData = Get-G2eAcceptanceProperty $actualResponse 'data' "$name response"
        $expectedData = Get-G2eAcceptanceProperty $expectedRows $name 'Doris API row projections'
        $null = Assert-G2eApiRows -Family $name -Actual $actualData -Expected $expectedData
    }
    $qualityResponse = Get-G2eAcceptanceProperty $responses 'quality' 'API responses'
    $qualityData = Get-G2eAcceptanceProperty $qualityResponse 'data' 'quality response'
    if ([string](Get-G2eAcceptanceProperty $qualityData 'reconciliation_status' 'quality response') -cne
            'PASS') {
        throw 'G2-E acceptance quality status did not pass.'
    }

    $definitions = Get-G2eAcceptanceProperty $responses 'definitions' 'API responses'
    if ([string](Get-G2eAcceptanceProperty $definitions 'domain' 'definitions response') -cne
            'orders' -or
            [string](Get-G2eAcceptanceProperty $definitions 'dataset_id' 'definitions response') -cne
                $identity.DatasetId -or
            [string](Get-G2eAcceptanceProperty $definitions 'metric_version' 'definitions response') -cne
                $identity.MetricVersion -or
            @(Get-G2eAcceptanceProperty $definitions 'definitions' 'definitions response').Count -eq 0) {
        throw 'G2-E acceptance definitions response differs.'
    }

    $verification = Get-G2eAcceptanceProperty $Evidence 'verification' 'evidence'
    $null = Assert-G2eAcceptanceExactKeys $verification `
        $script:G2eAcceptanceVerificationNames 'verification gates'
    foreach ($name in $script:G2eAcceptanceVerificationNames) {
        $value = Get-G2eAcceptanceProperty $verification $name 'verification gates'
        if ($value -isnot [bool] -or -not $value) {
            throw "G2-E acceptance verification gate did not pass: $name."
        }
    }
    $measured = Get-G2eAcceptanceProperty $Evidence 'measured' 'evidence'
    foreach ($name in @('day_range', 'month_range')) {
        $range = Get-G2eAcceptanceProperty $measured $name 'measured ranges'
        $start = Assert-G2eAcceptanceDate `
            (Get-G2eAcceptanceProperty $range 'start_date' "$name range") "$name start"
        $end = Assert-G2eAcceptanceDate `
            (Get-G2eAcceptanceProperty $range 'end_date' "$name range") "$name end"
        if ([datetime]$start -gt [datetime]$end -or
                [datetime]$start -lt [datetime]$identity.WindowStart -or
                [datetime]$end -gt [datetime]$identity.WindowEnd) {
            throw "G2-E acceptance measured $name is outside the publication window."
        }
    }
    $null = ConvertTo-G2eAcceptanceUtcTimestamp `
        (Get-G2eAcceptanceProperty $Evidence 'verified_at' 'evidence') 'verified_at'
    return $Evidence
}

function Assert-G2eAcceptanceNoReparsePoint {
    param(
        [Parameter(Mandatory = $true)][string]$RootPath,
        [Parameter(Mandatory = $true)][string]$CandidatePath
    )

    $root = [IO.Path]::GetFullPath($RootPath).TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    )
    $candidate = [IO.Path]::GetFullPath($CandidatePath)
    $comparison = if ($env:OS -ceq 'Windows_NT') {
        [StringComparison]::OrdinalIgnoreCase
    } else {
        [StringComparison]::Ordinal
    }
    $prefix = $root + [IO.Path]::DirectorySeparatorChar
    if (-not $candidate.StartsWith($prefix, $comparison)) {
        throw 'G2-E acceptance path escapes the repository root.'
    }
    $current = $candidate
    while ($current.StartsWith($prefix, $comparison) -or $current.Equals($root, $comparison)) {
        if ([IO.File]::Exists($current) -or [IO.Directory]::Exists($current)) {
            $item = Get-Item -LiteralPath $current -Force
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw 'G2-E acceptance path contains a reparse point.'
            }
        }
        if ($current.Equals($root, $comparison)) { break }
        $current = [IO.Path]::GetDirectoryName($current)
        if ([string]::IsNullOrWhiteSpace($current)) { break }
    }
    return $candidate
}

function Get-G2eAcceptancePath {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string]$SourceBundleSha256
    )

    if (-not [IO.Path]::IsPathRooted($RepositoryRoot) -or
            -not [IO.Directory]::Exists($RepositoryRoot)) {
        throw 'G2-E acceptance repository root must be an existing absolute directory.'
    }
    $bundle = Assert-G2eAcceptanceSha256 $SourceBundleSha256 'report source bundle'
    $root = [IO.Path]::GetFullPath($RepositoryRoot)
    $path = [IO.Path]::GetFullPath((Join-Path $root `
        "tmp/graduation/g2e/$bundle/acceptance.json"))
    return Assert-G2eAcceptanceNoReparsePoint $root $path
}

function Write-G2eAcceptanceEvidence {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)]$Evidence
    )

    $null = Assert-G2eAcceptanceEvidence $Evidence
    $bundle = [string](Get-G2eAcceptanceProperty $Evidence 'source_bundle_sha256' 'evidence')
    $path = Get-G2eAcceptancePath $RepositoryRoot $bundle
    $directory = [IO.Path]::GetDirectoryName($path)
    $null = New-Item -ItemType Directory -Force -Path $directory
    $null = Assert-G2eAcceptanceNoReparsePoint $RepositoryRoot $directory
    if ([IO.File]::Exists($path)) {
        throw 'G2-E acceptance evidence already exists and will not be overwritten.'
    }
    $temporary = Join-Path $directory `
        ('.acceptance-' + [guid]::NewGuid().ToString('N') + '.tmp')
    try {
        $json = ($Evidence | ConvertTo-Json -Depth 32) + "`n"
        [IO.File]::WriteAllText($temporary, $json, [Text.UTF8Encoding]::new($false))
        [IO.File]::Move($temporary, $path)
    } finally {
        if ([IO.File]::Exists($temporary)) { [IO.File]::Delete($temporary) }
    }
    return $path
}

function ConvertTo-G2eAcceptanceIntFromText {
    param($Value, [string]$Label)
    $number = [int64]0
    if ($null -eq $Value -or -not [int64]::TryParse(
            [string]$Value,
            [Globalization.NumberStyles]::None,
            [Globalization.CultureInfo]::InvariantCulture,
            [ref]$number
        ) -or $number -lt 0) {
        throw "G2-E acceptance stored $Label is not a nonnegative integer."
    }
    return $number
}

function ConvertTo-G2eAcceptanceBoolFromText {
    param($Value, [string]$Label)
    if ([string]$Value -ceq '1') { return $true }
    if ([string]$Value -ceq '0') { return $false }
    throw "G2-E acceptance stored $Label is not a strict boolean."
}

function ConvertTo-G2eAcceptanceProjectedRow {
    param(
        [Parameter(Mandatory = $true)][string]$Family,
        [Parameter(Mandatory = $true)]$Row
    )

    $fields = switch ($Family) {
        'overview' { [string[]]@(
            'window_type', 'window_start', 'window_end', 'order_count',
            'delivered_order_count', 'canceled_order_count', 'unavailable_order_count',
            'status_eligible_order_count', 'status_excluded_order_count', 'delivered_rate',
            'canceled_rate', 'unique_customer_count', 'repeat_customer_count',
            'repeat_customer_rate', 'item_row_count', 'item_value_sum',
            'freight_value_sum', 'payment_value_sum', 'items_per_order_avg'
        ) }
        'delivery' { [string[]]@(
            'window_type', 'window_start', 'window_end', 'delivery_eligible_order_count',
            'delivery_excluded_order_count', 'delivery_days_avg', 'delivery_days_p50',
            'delivery_days_p90', 'late_delivery_order_count',
            'late_delivery_eligible_order_count', 'late_delivery_excluded_order_count',
            'late_delivery_rate'
        ) }
        'payment' { [string[]]@(
            'window_type', 'window_start', 'window_end', 'payment_type', 'is_all',
            'global_order_count', 'payment_order_count', 'payment_row_count',
            'installment_order_count', 'payment_value_sum'
        ) }
        'ranking' { [string[]]@(
            'window_type', 'window_start', 'window_end', 'dimension_type', 'dimension_id',
            'dimension_name', 'is_unknown', 'ranking_order_count', 'ranking_item_row_count',
            'ranking_customer_count', 'ranking_item_value_sum', 'ranking_freight_value_sum',
            'ranking_payment_value_sum', 'ranking_late_delivery_order_count',
            'ranking_late_delivery_eligible_order_count', 'ranking_late_delivery_rate',
            'payment_value_is_additive'
        ) }
        'review' { [string[]]@(
            'window_type', 'window_start', 'window_end', 'review_row_count',
            'reviewed_order_count', 'all_order_count', 'review_coverage_rate',
            'review_score_avg', 'low_score_order_count', 'low_score_rate',
            'multi_review_order_count'
        ) }
        default { throw 'G2-E acceptance projection family is unsupported.' }
    }
    $integerFields = [string[]]@(
        'order_count', 'delivered_order_count', 'canceled_order_count',
        'unavailable_order_count', 'status_eligible_order_count', 'status_excluded_order_count',
        'unique_customer_count', 'repeat_customer_count', 'item_row_count',
        'delivery_eligible_order_count', 'delivery_excluded_order_count',
        'late_delivery_order_count', 'late_delivery_eligible_order_count',
        'late_delivery_excluded_order_count', 'global_order_count', 'payment_order_count',
        'payment_row_count', 'installment_order_count', 'ranking_order_count',
        'ranking_item_row_count', 'ranking_customer_count',
        'ranking_late_delivery_order_count', 'ranking_late_delivery_eligible_order_count',
        'review_row_count', 'reviewed_order_count', 'all_order_count',
        'low_score_order_count', 'multi_review_order_count'
    )
    $booleanFields = [string[]]@('is_all', 'is_unknown', 'payment_value_is_additive')
    $result = [ordered]@{}
    foreach ($field in $fields) {
        $storedField = if ($Family -ceq 'payment' -and $field -ceq 'payment_value_sum') {
            'payment_type_value_sum'
        } else { $field }
        $value = Get-G2eAcceptanceProperty $Row $storedField "$Family stored row"
        if ($null -eq $value) { $result[$field] = $null }
        elseif ($integerFields -ccontains $field) {
            $result[$field] = ConvertTo-G2eAcceptanceIntFromText $value "$Family.$field"
        } elseif ($booleanFields -ccontains $field) {
            $result[$field] = ConvertTo-G2eAcceptanceBoolFromText $value "$Family.$field"
        } else { $result[$field] = [string]$value }
    }
    return [pscustomobject]$result
}

function ConvertTo-G2eAcceptanceQualityProjection {
    param([Parameter(Mandatory = $true)]$Row)

    $integerFields = [string[]]@(
        'source_row_count', 'iceberg_row_count', 'duplicate_key_count', 'orphan_key_count',
        'invalid_value_count', 'temporal_anomaly_count', 'amount_comparable_order_count',
        'amount_reconciled_order_count', 'amount_mismatch_order_count'
    )
    $decimalFields = [string[]]@(
        'amount_reconciliation_rate', 'payment_item_freight_abs_difference_avg',
        'payment_item_freight_abs_difference_p50', 'payment_item_freight_abs_difference_p90'
    )
    $result = [ordered]@{
        window_type = [string](Get-G2eAcceptanceProperty $Row 'window_type' 'quality row')
        window_start = [string](Get-G2eAcceptanceProperty $Row 'window_start' 'quality row')
        window_end = [string](Get-G2eAcceptanceProperty $Row 'window_end' 'quality row')
    }
    foreach ($field in $integerFields) {
        $result[$field] = ConvertTo-G2eAcceptanceIntFromText `
            (Get-G2eAcceptanceProperty $Row $field 'quality row') "quality.$field"
    }
    foreach ($field in $decimalFields) {
        $value = Get-G2eAcceptanceProperty $Row $field 'quality row'
        $result[$field] = if ($null -eq $value) { $null } else { [string]$value }
    }
    foreach ($mapping in @(
            @('raw_row_counts_json', 'raw_row_counts'),
            @('normalized_row_counts_json', 'normalized_row_counts'),
            @('iceberg_row_counts_json', 'iceberg_row_counts'),
            @('normalized_sha256_json', 'normalized_sha256'),
            @('source_snapshots_json', 'source_snapshots'),
            @('curated_snapshots_json', 'curated_snapshots'),
            @('fact_reconciliations_json', 'fact_reconciliations'),
            @('reportable_quality_json', 'reportable_quality')
        )) {
        $result[$mapping[1]] = ConvertFrom-G2eAcceptanceJsonObject `
            (Get-G2eAcceptanceProperty $Row $mapping[0] 'quality row') `
            "quality $($mapping[1])"
    }
    $result.reconciliation_status = [string](
        Get-G2eAcceptanceProperty $Row 'reconciliation_status' 'quality row'
    )
    return [pscustomobject]$result
}

function Invoke-G2eAcceptanceJsonGet {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds
    )
    $response = Invoke-WebRequest -UseBasicParsing -Method Get `
        -Uri ($BaseUrl.TrimEnd('/') + $Path) -TimeoutSec $TimeoutSeconds
    if ([int]$response.StatusCode -ne 200) { throw "G2-E acceptance API failed for $Path." }
    try { return [string]$response.Content | ConvertFrom-Json } catch {
        throw "G2-E acceptance API returned malformed JSON for $Path."
    }
}

function Assert-G2eAcceptanceHttpStatus {
    param(
        [Parameter(Mandatory = $true)][string]$Uri,
        [Parameter(Mandatory = $true)][int]$ExpectedStatus,
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds
    )
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Method Get -Uri $Uri `
            -TimeoutSec $TimeoutSeconds
        $status = [int]$response.StatusCode
    } catch {
        $status = if ($null -ne $_.Exception.Response) {
            [int]$_.Exception.Response.StatusCode
        } else { 0 }
    }
    if ($status -ne $ExpectedStatus) {
        throw "G2-E acceptance expected HTTP $ExpectedStatus."
    }
}

if ($acceptanceFunctionsOnly) { return }

if ([string]::IsNullOrWhiteSpace($acceptanceManifestPath)) {
    throw 'ManifestPath is required for G2-E order-domain acceptance.'
}
if ($acceptanceTimeoutSeconds -lt 1 -or $acceptanceTimeoutSeconds -gt 3600) {
    throw 'G2-E acceptance timeout must be between 1 and 3600 seconds.'
}
if ($acceptanceApiBaseUrl -cnotmatch '^http://(?:localhost|127\.0\.0\.1):[1-9][0-9]{0,4}/?$') {
    throw 'G2-E acceptance API base URL must be a local HTTP endpoint.'
}

$repositoryRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'refresh_g2e_order_metrics.ps1') -FunctionsOnly
$envFile = Get-G2eRefreshEnvFile
Initialize-G2eDorisCredentials -EnvFile $envFile
$runtimeEvidence = Get-G2eRefreshEvidence `
    -ManifestPath $acceptanceManifestPath -EnvFile $envFile
$bundle = [string]$runtimeEvidence.SourceIdentity.SourceBundleSha256
$metricsPaths = Get-G2eMetricsPaths -SourceBundleSha256 $bundle
if (-not [IO.File]::Exists($metricsPaths.ReportPath)) {
    throw 'G2-E acceptance refresh report is missing.'
}
try {
    $refreshReport = [IO.File]::ReadAllText(
        $metricsPaths.ReportPath,
        [Text.Encoding]::UTF8
    ) | ConvertFrom-Json
} catch { throw 'G2-E acceptance refresh report is malformed.' }

$script:G2eDockerExecutable = Get-G2eDockerExecutable
$dockerVersion = @(& $script:G2eDockerExecutable version --format '{{.Server.Version}}' 2>&1)
if ($LASTEXITCODE -ne 0 -or $dockerVersion.Count -ne 1) {
    throw 'Docker Engine is unavailable for G2-E acceptance.'
}
Wait-G2eTrinoDependency -EnvFile $envFile -TimeoutSeconds $acceptanceTimeoutSeconds
Wait-G2eDorisDependency -EnvFile $envFile -TimeoutSeconds $acceptanceTimeoutSeconds
$liveSnapshots = Get-G2eLiveSnapshotMap -Evidence $runtimeEvidence -EnvFile $envFile
$window = Get-G2eMetricWindow -Evidence $runtimeEvidence -EnvFile $envFile
$metricRunId = "$($script:G2eAcceptanceMetricVersion)-b$bundle"
$publicationRows = @(Read-G2ePublicationRows -MetricRunId $metricRunId -EnvFile $envFile)
if ($publicationRows.Count -ne 1) {
    throw 'G2-E acceptance requires exactly one Doris publication row.'
}
$publication = $publicationRows[0]
$storedEvidence = Get-G2eStoredEvidence `
    -Identity ([pscustomobject]@{
        MetricRunId = $metricRunId
        DatasetId = $script:G2eAcceptanceDatasetId
        MetricVersion = $script:G2eAcceptanceMetricVersion
    }) -EnvFile $envFile

$storedRows = [ordered]@{}
foreach ($family in $script:G2eAcceptanceFamilies) {
    $storedRows[$family] = @(Read-G2eStoredMetricRows `
        -Family $family -MetricRunId $metricRunId -EnvFile $envFile)
}
$fullOverviewRows = @($storedRows.overview | Where-Object { $_.window_type -ceq 'FULL' })
$fullDeliveryRows = @($storedRows.delivery | Where-Object { $_.window_type -ceq 'FULL' })
$fullPaymentRows = @($storedRows.payment | Where-Object { $_.window_type -ceq 'FULL' } |
    Sort-Object @{ Expression = { [int]$_.is_all }; Descending = $true }, payment_type)
$fullRankingRows = @($storedRows.ranking | Where-Object {
        $_.window_type -ceq 'FULL' -and $_.dimension_type -ceq 'category'
    } | Sort-Object `
        @{ Expression = { [decimal]$_.ranking_item_value_sum }; Descending = $true },
        dimension_id, window_start | Select-Object -First 20)
$fullReviewRows = @($storedRows.review | Where-Object { $_.window_type -ceq 'FULL' })
$fullQualityRows = @($storedRows.quality | Where-Object { $_.window_type -ceq 'FULL' })
$dayRows = @($storedRows.overview | Where-Object { $_.window_type -ceq 'DAY' } |
    Sort-Object window_start | Select-Object -First 1)
$monthRows = @($storedRows.overview | Where-Object { $_.window_type -ceq 'MONTH' } |
    Sort-Object window_start | Select-Object -First 1)
if ($dayRows.Count -ne 1 -or $monthRows.Count -ne 1 -or $fullQualityRows.Count -ne 1) {
    throw 'G2-E acceptance measured DAY, MONTH, or quality row is missing.'
}

$apiRows = [ordered]@{
    overview = @($fullOverviewRows | ForEach-Object {
        ConvertTo-G2eAcceptanceProjectedRow overview $_
    })
    delivery = @($fullDeliveryRows | ForEach-Object {
        ConvertTo-G2eAcceptanceProjectedRow delivery $_
    })
    payments = @($fullPaymentRows | ForEach-Object {
        ConvertTo-G2eAcceptanceProjectedRow payment $_
    })
    rankings = @($fullRankingRows | ForEach-Object {
        ConvertTo-G2eAcceptanceProjectedRow ranking $_
    })
    reviews = @($fullReviewRows | ForEach-Object {
        ConvertTo-G2eAcceptanceProjectedRow review $_
    })
    quality = ConvertTo-G2eAcceptanceQualityProjection $fullQualityRows[0]
    day_overview = @($dayRows | ForEach-Object {
        ConvertTo-G2eAcceptanceProjectedRow overview $_
    })
    month_overview = @($monthRows | ForEach-Object {
        ConvertTo-G2eAcceptanceProjectedRow overview $_
    })
}

$responses = [ordered]@{}
for ($index = 0; $index -lt $script:G2eAcceptanceEndpoints.Count; $index++) {
    $name = $script:G2eAcceptanceResponseNames[$index]
    $responses[$name] = Invoke-G2eAcceptanceJsonGet `
        -BaseUrl $acceptanceApiBaseUrl -Path $script:G2eAcceptanceEndpoints[$index] `
        -TimeoutSeconds $acceptanceTimeoutSeconds
}
$dayStart = [string]$dayRows[0].window_start
$dayEnd = [string]$dayRows[0].window_end
$monthStart = [string]$monthRows[0].window_start
$monthEnd = [string]$monthRows[0].window_end
$responses.day_overview = Invoke-G2eAcceptanceJsonGet -BaseUrl $acceptanceApiBaseUrl `
    -Path "/api/v1/orders/overview?window=day&start_date=$dayStart&end_date=$dayEnd" `
    -TimeoutSeconds $acceptanceTimeoutSeconds
$responses.month_overview = Invoke-G2eAcceptanceJsonGet -BaseUrl $acceptanceApiBaseUrl `
    -Path "/api/v1/orders/overview?window=month&start_date=$monthStart&end_date=$monthEnd" `
    -TimeoutSeconds $acceptanceTimeoutSeconds
$base = $acceptanceApiBaseUrl.TrimEnd('/')
Assert-G2eAcceptanceHttpStatus `
    "$base/api/v1/orders/rankings?dimension=category&window=full&sort_by=late_rate" `
    422 $acceptanceTimeoutSeconds
Assert-G2eAcceptanceHttpStatus `
    "$base/api/v1/orders/overview?window=full&start_date=$dayStart&end_date=$dayEnd" `
    422 $acceptanceTimeoutSeconds

$familyEvidence = [ordered]@{}
foreach ($family in $script:G2eAcceptanceFamilies) {
    $item = $storedEvidence[$family]
    $familyEvidence[$family] = [ordered]@{
        row_count = [int64]$item.RowCount
        sha256 = [string]$item.Sha256
    }
}
$refreshCandidates = [ordered]@{}
foreach ($family in $script:G2eAcceptanceFamilies) {
    $item = Get-G2eAcceptanceProperty `
        (Get-G2eAcceptanceProperty $refreshReport 'candidates' 'refresh report') `
        $family 'refresh candidates'
    $refreshCandidates[$family] = [ordered]@{
        row_count = [int64](Get-G2eAcceptanceProperty $item 'row_count' "$family candidate")
        sha256 = [string](Get-G2eAcceptanceProperty $item 'sha256' "$family candidate")
        file_sha256 = [string](Get-G2eAcceptanceProperty $item 'file_sha256' "$family candidate")
    }
}
$refreshSummary = [ordered]@{
    status = [string](Get-G2eAcceptanceProperty $refreshReport 'status' 'refresh report')
    metric_run_id = [string](Get-G2eAcceptanceProperty $refreshReport 'metric_run_id' 'refresh report')
    source_bundle_sha256 = [string](Get-G2eAcceptanceProperty $refreshReport 'source_bundle_sha256' 'refresh report')
    implementation_revision = [string](Get-G2eAcceptanceProperty $refreshReport 'implementation_revision' 'refresh report')
    candidates = $refreshCandidates
    publication = Get-G2eAcceptanceProperty $refreshReport 'publication' 'refresh report'
}

$acceptance = [ordered]@{
    schema_version = 1
    status = 'PASS'
    dataset_id = $script:G2eAcceptanceDatasetId
    source_bundle_sha256 = $bundle
    metric_run_id = $metricRunId
    implementation_revision = [string]$publication.implementation_revision
    window_start = [string]$window.WindowStart
    window_end = [string]$window.WindowEnd
    source_order_count = [int64]$window.SourceOrderCount
    manifest = $runtimeEvidence.Manifest
    source_reports = @($runtimeEvidence.SourceReports)
    curated = $runtimeEvidence.CuratedReport
    refresh = $refreshSummary
    doris = [ordered]@{
        publication = $publication
        families = $familyEvidence
        api_rows = $apiRows
    }
    api = [ordered]@{
        endpoints = $script:G2eAcceptanceEndpoints
        responses = $responses
    }
    verification = [ordered]@{
        trino_snapshot_identity = ($liveSnapshots.Count -eq 18)
        trino_fact_reconciliation = $true
        doris_candidate_digests = $true
        api_full_contracts = $true
        api_day_range = $true
        api_month_range = $true
        invalid_requests_422 = $true
    }
    measured = [ordered]@{
        day_range = [ordered]@{ start_date = $dayStart; end_date = $dayEnd }
        month_range = [ordered]@{ start_date = $monthStart; end_date = $monthEnd }
    }
    verified_at = [DateTimeOffset]::UtcNow.ToString('o')
}
$savedPath = Write-G2eAcceptanceEvidence `
    -RepositoryRoot $repositoryRoot -Evidence ([pscustomobject]$acceptance)
[ordered]@{
    status = 'PASS'
    metric_run_id = $metricRunId
    source_bundle_sha256 = $bundle
    acceptance_path = $savedPath
    endpoints = $script:G2eAcceptanceEndpoints.Count
} | ConvertTo-Json -Compress
