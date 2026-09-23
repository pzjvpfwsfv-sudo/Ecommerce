Set-StrictMode -Version Latest

$script:G2eMetricFamilies = [string[]]@('overview', 'delivery', 'payment', 'ranking', 'review', 'quality')
$script:G2eSourceTables = [ordered]@{
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
$script:G2eCuratedTables = [string[]]@(
    'customer_dim_v1', 'category_dim_v1', 'product_dim_v1', 'seller_dim_v1',
    'geolocation_dim_v1', 'order_fact_v1', 'order_item_fact_v1', 'payment_fact_v1',
    'review_fact_v1'
)
$script:G2eTargetColumns = [ordered]@{
    overview = [string[]]@(
        'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
        'window_end', 'order_count', 'delivered_order_count', 'canceled_order_count',
        'unavailable_order_count', 'status_eligible_order_count', 'status_excluded_order_count',
        'delivered_rate', 'canceled_rate', 'unique_customer_count', 'repeat_customer_count',
        'repeat_customer_rate', 'item_row_count', 'item_value_sum', 'freight_value_sum',
        'payment_value_sum', 'items_per_order_avg'
    )
    delivery = [string[]]@(
        'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
        'window_end', 'delivery_eligible_order_count', 'delivery_excluded_order_count',
        'delivery_days_avg', 'delivery_days_p50', 'delivery_days_p90',
        'late_delivery_order_count', 'late_delivery_eligible_order_count',
        'late_delivery_excluded_order_count', 'late_delivery_rate'
    )
    payment = [string[]]@(
        'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
        'window_end', 'payment_type', 'is_all', 'global_order_count', 'payment_order_count',
        'payment_row_count', 'installment_order_count', 'payment_type_order_count',
        'payment_type_value_sum'
    )
    ranking = [string[]]@(
        'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
        'window_end', 'dimension_type', 'dimension_id', 'dimension_name', 'is_unknown',
        'ranking_order_count', 'ranking_item_row_count', 'ranking_customer_count',
        'ranking_item_value_sum', 'ranking_freight_value_sum', 'ranking_payment_value_sum',
        'ranking_late_delivery_order_count', 'ranking_late_delivery_eligible_order_count',
        'ranking_late_delivery_rate', 'payment_value_is_additive'
    )
    review = [string[]]@(
        'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
        'window_end', 'review_row_count', 'reviewed_order_count', 'all_order_count',
        'review_coverage_rate', 'review_score_avg', 'low_score_order_count',
        'low_score_rate', 'multi_review_order_count'
    )
    quality = [string[]]@(
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
}
$script:G2eUniqueKeyColumns = [ordered]@{
    overview = [string[]]@('metric_run_id', 'window_type', 'window_start')
    delivery = [string[]]@('metric_run_id', 'window_type', 'window_start')
    payment = [string[]]@('metric_run_id', 'window_type', 'window_start', 'payment_type')
    ranking = [string[]]@('metric_run_id', 'window_type', 'window_start', 'dimension_type', 'dimension_id')
    review = [string[]]@('metric_run_id', 'window_type', 'window_start')
    quality = [string[]]@('metric_run_id')
}

function Get-G2eOrderProperty {
    param(
        [Parameter(Mandatory = $true)]$Value,
        [Parameter(Mandatory = $true)][string]$Name
    )

    if ($Value -is [Collections.IDictionary]) {
        $matches = @($Value.Keys | Where-Object { [string]$_ -ceq $Name })
        if ($matches.Count -ne 1) { throw "Required property '$Name' is missing or has wrong casing." }
        return $Value[$matches[0]]
    }
    $matches = @($Value.PSObject.Properties | Where-Object { $_.Name -ceq $Name })
    if ($matches.Count -ne 1) { throw "Required property '$Name' is missing or has wrong casing." }
    return $matches[0].Value
}

function Test-G2eOrderProperty {
    param($Value, [string]$Name)
    if ($null -eq $Value) { return $false }
    if ($Value -is [Collections.IDictionary]) {
        return @($Value.Keys | Where-Object { [string]$_ -ceq $Name }).Count -eq 1
    }
    return @($Value.PSObject.Properties | Where-Object { $_.Name -ceq $Name }).Count -eq 1
}

function Get-G2eOrderMapKeys {
    param([Parameter(Mandatory = $true)]$Map)
    if ($Map -is [Collections.IDictionary]) { return [string[]]@($Map.Keys) }
    return [string[]]@($Map.PSObject.Properties.Name)
}

function Assert-G2eOrderExactKeys {
    param(
        [Parameter(Mandatory = $true)]$Map,
        [Parameter(Mandatory = $true)][string[]]$Expected,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $actual = [string[]](Get-G2eOrderMapKeys $Map)
    $expectedCopy = [string[]]$Expected.Clone()
    [Array]::Sort($actual, [StringComparer]::Ordinal)
    [Array]::Sort($expectedCopy, [StringComparer]::Ordinal)
    if (($actual -join "`n") -cne ($expectedCopy -join "`n")) {
        throw "$Name keys are not exact."
    }
}

function Get-G2eOrderMapValue {
    param($Map, [string]$Key)
    return Get-G2eOrderProperty $Map $Key
}

function ConvertTo-G2eOrderCount {
    param($Value, [string]$Name, [switch]$Positive)
    if ($null -eq $Value) { throw "$Name must be an integer." }
    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    if ($text -notmatch '^(0|[1-9][0-9]*)$') { throw "$Name must be an integer." }
    $parsed = 0L
    if (-not [long]::TryParse($text, [Globalization.NumberStyles]::None,
            [Globalization.CultureInfo]::InvariantCulture, [ref]$parsed)) {
        throw "$Name is outside the Int64 range."
    }
    if ($Positive -and $parsed -le 0) { throw "$Name must be positive." }
    return $parsed
}

function ConvertTo-G2eOrderDecimal {
    param($Value, [string]$Name, [switch]$AllowNull)
    if ($null -eq $Value -or [string]::IsNullOrEmpty([string]$Value)) {
        if ($AllowNull) { return $null }
        throw "$Name must be a decimal."
    }
    $parsed = 0D
    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    if (-not [decimal]::TryParse($text, [Globalization.NumberStyles]::AllowDecimalPoint,
            [Globalization.CultureInfo]::InvariantCulture, [ref]$parsed)) {
        throw "$Name must be a decimal."
    }
    if ($parsed -lt 0) { throw "$Name must be nonnegative." }
    return $parsed
}

function ConvertTo-G2eOrderBoolean {
    param($Value, [string]$Name, [switch]$AllowNull)
    if ($null -eq $Value -or [string]::IsNullOrEmpty([string]$Value)) {
        if ($AllowNull) { return $null }
        throw "$Name must be a boolean."
    }
    if ($Value -is [bool]) { return [bool]$Value }
    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    if ($text -ceq '1' -or $text -ceq 'true' -or $text -ceq 'True') { return $true }
    if ($text -ceq '0' -or $text -ceq 'false' -or $text -ceq 'False') { return $false }
    throw "$Name must be a boolean or 0/1."
}

function ConvertTo-G2eOrderDate {
    param($Value, [string]$Name)
    $parsed = [datetime]::MinValue
    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    if (-not [datetime]::TryParseExact($text, 'yyyy-MM-dd',
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::None, [ref]$parsed)) {
        throw "$Name must use yyyy-MM-dd."
    }
    return $parsed.ToString('yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
}

function ConvertTo-G2eOrderTimestamp {
    param($Value, [string]$Name)
    $parsed = [datetimeoffset]::MinValue
    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    if (-not [datetimeoffset]::TryParse($text, [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::AssumeUniversal, [ref]$parsed)) {
        throw "$Name must be an ISO-8601 timestamp."
    }
    return $parsed.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'",
        [Globalization.CultureInfo]::InvariantCulture)
}

function Assert-G2eOrderSha256 {
    param($Value, [string]$Name)
    $text = [string]$Value
    if ($text -cnotmatch '^[0-9a-f]{64}$') { throw "$Name must be a lowercase SHA-256." }
    return $text
}

function Assert-G2eOrderSnapshot {
    param($Value, [string]$Name)
    $text = [string]$Value
    if ($text -notmatch '^[1-9][0-9]*$') { throw "$Name must be a positive Snapshot ID." }
    $null = ConvertTo-G2eOrderCount $text $Name -Positive
    return $text
}

function ConvertTo-G2eOrderCanonicalMapJson {
    param([Parameter(Mandatory = $true)]$Map)
    $keys = [string[]](Get-G2eOrderMapKeys $Map)
    [Array]::Sort($keys, [StringComparer]::Ordinal)
    $pairs = foreach ($key in $keys) {
        $value = Get-G2eOrderMapValue $Map $key
        $keyJson = ConvertTo-Json -InputObject $key -Compress
        if ($null -eq $value) {
            $valueJson = 'null'
        } elseif ($value -is [bool]) {
            $valueJson = if ($value) { 'true' } else { 'false' }
        } elseif ($value -is [byte] -or $value -is [int16] -or $value -is [int32] -or
                $value -is [int64] -or $value -is [uint16] -or $value -is [uint32]) {
            $valueJson = [Convert]::ToString($value, [Globalization.CultureInfo]::InvariantCulture)
        } else {
            $valueJson = ConvertTo-Json -InputObject ([string]$value) -Compress
        }
        "$keyJson`:$valueJson"
    }
    return '{' + ($pairs -join ',') + '}'
}

function Copy-G2eOrderSortedSnapshotMap {
    param($Map, [string[]]$Expected, [string]$Name)
    Assert-G2eOrderExactKeys $Map $Expected $Name
    $keys = [string[]]$Expected.Clone()
    [Array]::Sort($keys, [StringComparer]::Ordinal)
    $result = [ordered]@{}
    foreach ($key in $keys) {
        $result[$key] = Assert-G2eOrderSnapshot (Get-G2eOrderMapValue $Map $key) "$Name.$key"
    }
    return $result
}

function Get-G2eOrderEvidenceContext {
    param(
        [Parameter(Mandatory = $true)]$Manifest,
        [Parameter(Mandatory = $true)][object[]]$SourceReports,
        [Parameter(Mandatory = $true)]$CuratedReport
    )

    if ([string](Get-G2eOrderProperty $Manifest 'dataset_id') -cne 'olist-brazilian-ecommerce-v2') {
        throw 'G2-E order manifest dataset identity is invalid.'
    }
    $bundle = Assert-G2eOrderSha256 (Get-G2eOrderProperty $Manifest 'source_bundle_sha256') 'manifest bundle'
    $files = @((Get-G2eOrderProperty $Manifest 'files'))
    if ($files.Count -ne $script:G2eSourceTables.Count) { throw 'Manifest must contain exactly nine source files.' }
    $rawCounts = [ordered]@{}
    $normalizedCounts = [ordered]@{}
    $normalizedHashes = [ordered]@{}
    foreach ($file in $files) {
        $entity = [string](Get-G2eOrderProperty $file 'entity')
        if (-not $script:G2eSourceTables.Contains($entity) -or $rawCounts.Contains($entity)) {
            throw 'Manifest contains an unknown or duplicate entity.'
        }
        $raw = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $file 'row_count') "manifest.$entity.row_count" -Positive
        $normalized = Get-G2eOrderProperty $file 'normalized'
        $normalizedCount = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $normalized 'row_count') "manifest.$entity.normalized.row_count" -Positive
        if ($raw -ne $normalizedCount) { throw "Manifest raw and normalized counts differ for $entity." }
        $rawCounts[$entity] = $raw
        $normalizedCounts[$entity] = $normalizedCount
        $normalizedHashes[$entity] = Assert-G2eOrderSha256 (Get-G2eOrderProperty $normalized 'sha256') "manifest.$entity.normalized.sha256"
    }
    Assert-G2eOrderExactKeys $rawCounts ([string[]]$script:G2eSourceTables.Keys) 'manifest entities'

    if ($SourceReports.Count -ne $script:G2eSourceTables.Count) {
        throw 'Exactly nine source reports are required.'
    }
    $sourceSnapshots = [ordered]@{}
    $icebergCounts = [ordered]@{}
    $seenReports = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($report in $SourceReports) {
        if ([string](Get-G2eOrderProperty $report 'status') -cne 'PASS' -or
                [string](Get-G2eOrderProperty $report 'dataset_id') -cne 'olist-brazilian-ecommerce-v2') {
            throw 'Source report status or dataset identity is invalid.'
        }
        $entity = [string](Get-G2eOrderProperty $report 'entity')
        if (-not $script:G2eSourceTables.Contains($entity) -or -not $seenReports.Add($entity)) {
            throw 'Source reports contain an unknown or duplicate entity.'
        }
        if ((Assert-G2eOrderSha256 (Get-G2eOrderProperty $report 'source_bundle_sha256') "source report $entity bundle") -cne $bundle) {
            throw 'Source reports do not match the manifest bundle.'
        }
        $table = [string]$script:G2eSourceTables[$entity]
        if ([string](Get-G2eOrderProperty $report 'target_table') -cne $table) {
            throw "Source report target is invalid for $entity."
        }
        $expected = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $report 'expected_row_count') "$entity expected count" -Positive
        $iceberg = Get-G2eOrderProperty $report 'iceberg'
        $actual = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $iceberg 'row_count') "$entity Iceberg count" -Positive
        if ($expected -ne $rawCounts[$entity] -or $actual -ne $expected) {
            throw "Source counts do not reconcile for $entity."
        }
        $sourceSnapshots[$table] = Assert-G2eOrderSnapshot (Get-G2eOrderProperty $iceberg 'snapshot_id') "$entity source snapshot"
        $icebergCounts[$table] = $actual
        $reportHash = Assert-G2eOrderSha256 (Get-G2eOrderProperty $report 'normalized_sha256') "$entity source normalized hash"
        if ($reportHash -cne $normalizedHashes[$entity]) { throw "Normalized hash differs for $entity." }
    }

    if ([string](Get-G2eOrderProperty $CuratedReport 'status') -cne 'PASS' -or
            [string](Get-G2eOrderProperty $CuratedReport 'dataset_id') -cne 'olist-brazilian-ecommerce-v2' -or
            (Assert-G2eOrderSha256 (Get-G2eOrderProperty $CuratedReport 'source_bundle_sha256') 'curated report bundle') -cne $bundle) {
        throw 'Curated report identity is invalid.'
    }
    $sourceTableNames = [string[]]$script:G2eSourceTables.Values
    $reportedSource = Copy-G2eOrderSortedSnapshotMap (Get-G2eOrderProperty $CuratedReport 'source_snapshots') $sourceTableNames 'curated source snapshots'
    $expectedSource = Copy-G2eOrderSortedSnapshotMap $sourceSnapshots $sourceTableNames 'source report snapshots'
    if ((ConvertTo-G2eOrderCanonicalMapJson $reportedSource) -cne (ConvertTo-G2eOrderCanonicalMapJson $expectedSource)) {
        throw 'Curated report source snapshots differ from source reports.'
    }
    $curatedSnapshots = Copy-G2eOrderSortedSnapshotMap (Get-G2eOrderProperty $CuratedReport 'curated_snapshots') $script:G2eCuratedTables 'curated snapshots'
    $sourceOrderCount = [long]$rawCounts.orders
    $grain = Get-G2eOrderProperty $CuratedReport 'grain_reconciliation'
    foreach ($name in @('order_fact_expected_count', 'order_fact_row_count')) {
        if ((ConvertTo-G2eOrderCount (Get-G2eOrderProperty $grain $name) "curated.$name") -ne $sourceOrderCount) {
            throw 'Curated order grain differs from the source order count.'
        }
    }
    $anti = Get-G2eOrderProperty $CuratedReport 'anti_fanout'
    $entityEvidence = [ordered]@{
        orders = @('source_order_count', 'fact_order_count')
        order_items = @('source_item_count', 'fact_item_count')
        order_payments = @('source_payment_count', 'fact_payment_count')
        order_reviews = @('source_review_count', 'fact_review_count')
    }
    foreach ($entity in $entityEvidence.Keys) {
        foreach ($field in $entityEvidence[$entity]) {
            if ((ConvertTo-G2eOrderCount (Get-G2eOrderProperty $anti $field) "curated.$field") -ne $rawCounts[$entity]) {
                throw "Curated anti-fanout count differs for $entity."
            }
        }
    }
    return [pscustomobject][ordered]@{
        Bundle = $bundle
        RawCounts = $rawCounts
        NormalizedCounts = $normalizedCounts
        NormalizedHashes = $normalizedHashes
        IcebergCounts = $icebergCounts
        SourceSnapshots = $expectedSource
        CuratedSnapshots = $curatedSnapshots
        SourceOrderCount = $sourceOrderCount
        Grain = $grain
        AntiFanout = $anti
    }
}

function Get-G2eMetricIdentity {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Manifest,
        [Parameter(Mandatory = $true)][object[]]$SourceReports,
        [Parameter(Mandatory = $true)]$CuratedReport,
        [Parameter(Mandatory = $true)][string]$WindowStart,
        [Parameter(Mandatory = $true)][string]$WindowEnd,
        [Parameter(Mandatory = $true)][string]$CalculatedAt,
        [Parameter(Mandatory = $true)][string]$ImplementationRevision,
        [string]$MetricRunId = ''
    )

    $context = Get-G2eOrderEvidenceContext $Manifest $SourceReports $CuratedReport
    $start = ConvertTo-G2eOrderDate $WindowStart 'WindowStart'
    $end = ConvertTo-G2eOrderDate $WindowEnd 'WindowEnd'
    if ($start -cgt $end) { throw 'Metric window is inverted.' }
    $calculated = ConvertTo-G2eOrderTimestamp $CalculatedAt 'CalculatedAt'
    if ($ImplementationRevision -cnotmatch '^[0-9a-f]{40}$') {
        throw 'ImplementationRevision must be a clean lowercase 40-character Git revision.'
    }
    $expectedRunId = "orders-v1-b$($context.Bundle)"
    if (-not [string]::IsNullOrEmpty($MetricRunId) -and $MetricRunId -cne $expectedRunId) {
        throw 'MetricRunId does not match the source bundle.'
    }
    return [pscustomobject][ordered]@{
        MetricRunId = $expectedRunId
        DatasetId = 'olist-brazilian-ecommerce-v2'
        MetricVersion = 'orders-v1'
        SourceBundleSha256 = $context.Bundle
        SourceSnapshots = $context.SourceSnapshots
        CuratedSnapshots = $context.CuratedSnapshots
        SourceOrderCount = $context.SourceOrderCount
        WindowStart = $start
        WindowEnd = $end
        CalculatedAt = $calculated
        ImplementationRevision = $ImplementationRevision
    }
}

function Split-G2eNamedSql {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [Parameter(Mandatory = $true)]$CuratedSnapshots
    )

    $snapshots = Copy-G2eOrderSortedSnapshotMap $CuratedSnapshots $script:G2eCuratedTables 'metric curated snapshots'
    $tokens = [ordered]@{}
    foreach ($table in $script:G2eCuratedTables) {
        $base = $table.Substring(0, $table.Length - 3).ToUpperInvariant()
        $tokens["__$base`_SNAPSHOT__"] = [string]$snapshots[$table]
    }
    $matches = [regex]::Matches($Sql, '__[A-Z_]+_SNAPSHOT__')
    if ($matches.Count -eq 0) { throw 'G2-E order metric SQL contains no Snapshot placeholders.' }
    foreach ($match in $matches) {
        if (-not $tokens.Contains($match.Value)) { throw 'G2-E order metric SQL contains an unknown placeholder.' }
    }
    foreach ($token in $tokens.Keys) {
        if ($Sql.IndexOf($token, [StringComparison]::Ordinal) -lt 0) {
            throw "G2-E order metric SQL is missing $token."
        }
    }

    $markers = [regex]::Matches($Sql, '(?m)^-- result:([a-z_]+)\r?$')
    if ($markers.Count -ne $script:G2eMetricFamilies.Count) {
        throw 'G2-E order metric SQL must contain exactly six result markers.'
    }
    $result = [ordered]@{}
    for ($index = 0; $index -lt $markers.Count; $index++) {
        $name = $markers[$index].Groups[1].Value
        if ($name -cne $script:G2eMetricFamilies[$index]) {
            throw 'G2-E order metric result markers are missing, duplicated, or out of order.'
        }
        $start = $markers[$index].Index + $markers[$index].Length
        $end = if ($index + 1 -lt $markers.Count) { $markers[$index + 1].Index } else { $Sql.Length }
        $statement = $Sql.Substring($start, $end - $start).Trim()
        if (-not $statement.EndsWith(';', [StringComparison]::Ordinal) -or
                [regex]::Matches($statement, ';').Count -ne 1) {
            throw 'Each G2-E order metric result must contain one semicolon-terminated statement.'
        }
        foreach ($token in $tokens.Keys) { $statement = $statement.Replace($token, $tokens[$token]) }
        $result[$name] = $statement
    }
    return $result
}

function ConvertFrom-G2eCsv {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][AllowEmptyString()][string]$CsvText)

    $headers = $null
    $result = [Collections.Generic.List[object]]::new()
    $fields = [Collections.Generic.List[string]]::new()
    $quotedFields = [Collections.Generic.List[bool]]::new()
    $field = [Text.StringBuilder]::new()
    $inQuotes = $false
    $afterQuote = $false
    $fieldQuoted = $false
    $rowStarted = $false

    for ($index = 0; $index -le $CsvText.Length; $index++) {
        $syntheticEnd = $index -eq $CsvText.Length
        if ($syntheticEnd) {
            if ($inQuotes) { throw 'Malformed CSV contains an unterminated quoted field.' }
            if (-not ($afterQuote -or $rowStarted -or $fields.Count -gt 0 -or $field.Length -gt 0)) {
                break
            }
            $character = "`n"
        } else {
            $character = $CsvText[$index]
        }

        if ($inQuotes) {
            if ($character -eq '"') {
                if ($index + 1 -lt $CsvText.Length -and $CsvText[$index + 1] -eq '"') {
                    $null = $field.Append('"'); $index++
                } else {
                    $inQuotes = $false; $afterQuote = $true
                }
            } else {
                $null = $field.Append($character)
            }
            continue
        }

        $recordEnded = $false
        if ($afterQuote) {
            if ($character -eq ',') {
                $fields.Add($field.ToString()); $quotedFields.Add($fieldQuoted)
                $null = $field.Clear(); $afterQuote = $false; $fieldQuoted = $false
                $rowStarted = $true
            } elseif ($character -eq "`r" -or $character -eq "`n") {
                $recordEnded = $true
            } else {
                throw 'Malformed CSV follows a closing quote.'
            }
        } elseif ($character -eq '"') {
            if ($field.Length -ne 0 -or $fieldQuoted) {
                throw 'Malformed CSV contains a quote in an unquoted field.'
            }
            $inQuotes = $true; $fieldQuoted = $true; $rowStarted = $true
        } elseif ($character -eq ',') {
            $fields.Add($field.ToString()); $quotedFields.Add($fieldQuoted)
            $null = $field.Clear(); $fieldQuoted = $false; $rowStarted = $true
        } elseif ($character -eq "`r" -or $character -eq "`n") {
            $recordEnded = $true
        } else {
            $null = $field.Append($character); $rowStarted = $true
        }

        if (-not $recordEnded) { continue }

        $fields.Add($field.ToString()); $quotedFields.Add($fieldQuoted)
        $values = [string[]]$fields.ToArray()
        $quoted = [bool[]]$quotedFields.ToArray()
        if ($null -eq $headers) {
            $headers = $values
            $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
            foreach ($header in $headers) {
                if ([string]::IsNullOrWhiteSpace($header) -or -not $seen.Add($header)) {
                    throw 'G2-E CSV headers must be nonempty and unique.'
                }
            }
        } else {
            if ($values.Count -ne $headers.Count) {
                throw 'G2-E CSV row has an inconsistent column count.'
            }
            $row = [ordered]@{}
            for ($column = 0; $column -lt $headers.Count; $column++) {
                $row[$headers[$column]] = if (
                    -not $quoted[$column] -and $values[$column].Length -eq 0
                ) {
                    $null
                } else {
                    $values[$column]
                }
            }
            $result.Add([pscustomobject]$row)
        }

        $fields = [Collections.Generic.List[string]]::new()
        $quotedFields = [Collections.Generic.List[bool]]::new()
        $null = $field.Clear()
        $afterQuote = $false; $fieldQuoted = $false; $rowStarted = $false
        if (-not $syntheticEnd -and $character -eq "`r" -and
                $index + 1 -lt $CsvText.Length -and $CsvText[$index + 1] -eq "`n") {
            $index++
        }
    }

    if ($null -eq $headers -or $result.Count -lt 1) {
        throw 'G2-E CSV must contain a header and at least one row.'
    }
    return @($result.ToArray())
}

function Merge-G2eQualityEvidence {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][object[]]$SqlQuality,
        [Parameter(Mandatory = $true)]$Manifest,
        [Parameter(Mandatory = $true)][object[]]$SourceReports,
        [Parameter(Mandatory = $true)]$CuratedReport
    )

    if ($SqlQuality.Count -ne 1) { throw 'SQL quality must contain exactly one row.' }
    $context = Get-G2eOrderEvidenceContext $Manifest $SourceReports $CuratedReport
    $row = $SqlQuality[0]
    $sourceCount = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $row 'source_row_count') 'quality.source_row_count' -Positive
    $icebergCount = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $row 'iceberg_row_count') 'quality.iceberg_row_count' -Positive
    if ($sourceCount -ne $context.SourceOrderCount -or $icebergCount -ne $context.SourceOrderCount) {
        throw 'SQL quality order counts differ from formal evidence.'
    }
    $hardGate = Get-G2eOrderProperty $CuratedReport 'hard_gate'
    foreach ($name in Get-G2eOrderMapKeys $hardGate) {
        if ((ConvertTo-G2eOrderCount (Get-G2eOrderMapValue $hardGate $name) "hard_gate.$name") -ne 0) {
            throw 'Curated hard-gate evidence is not clean.'
        }
    }
    $anti = $context.AntiFanout
    foreach ($pair in @(
            @('source_item_value_sum', 'fact_item_value_sum'),
            @('source_freight_value_sum', 'fact_freight_value_sum'),
            @('source_payment_value_sum', 'fact_payment_value_sum'))) {
        $left = ConvertTo-G2eOrderDecimal (Get-G2eOrderProperty $anti $pair[0]) "anti_fanout.$($pair[0])"
        $right = ConvertTo-G2eOrderDecimal (Get-G2eOrderProperty $anti $pair[1]) "anti_fanout.$($pair[1])"
        if ($left -ne $right) { throw 'Curated anti-fanout monetary evidence differs.' }
    }
    $comparable = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $row 'amount_comparable_order_count') 'quality.amount_comparable_order_count'
    $reconciled = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $row 'amount_reconciled_order_count') 'quality.amount_reconciled_order_count'
    $mismatch = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $row 'amount_mismatch_order_count') 'quality.amount_mismatch_order_count'
    if ($reconciled + $mismatch -ne $comparable) { throw 'Quality amount counts do not reconcile.' }
    Assert-G2eOrderRate (Get-G2eOrderProperty $row 'amount_reconciliation_rate') $reconciled $comparable 'quality.amount_reconciliation_rate'
    if ([string](Get-G2eOrderProperty $row 'reconciliation_status') -cne 'PASS') {
        throw 'SQL quality reconciliation status is not PASS.'
    }
    $reportable = Get-G2eOrderProperty $CuratedReport 'reportable_quality'
    foreach ($name in Get-G2eOrderMapKeys $reportable) {
        $null = ConvertTo-G2eOrderCount (Get-G2eOrderMapValue $reportable $name) "reportable_quality.$name"
    }
    $result = [ordered]@{}
    foreach ($name in @(
            'window_type', 'window_start', 'window_end', 'source_row_count', 'iceberg_row_count',
            'duplicate_key_count', 'orphan_key_count', 'invalid_value_count',
            'temporal_anomaly_count', 'amount_comparable_order_count',
            'amount_reconciled_order_count', 'amount_mismatch_order_count',
            'amount_reconciliation_rate', 'payment_item_freight_abs_difference_avg',
            'payment_item_freight_abs_difference_p50', 'payment_item_freight_abs_difference_p90')) {
        $result[$name] = Get-G2eOrderProperty $row $name
    }
    $result.window_start = ConvertTo-G2eOrderDate $result.window_start 'quality.window_start'
    $result.window_end = ConvertTo-G2eOrderDate $result.window_end 'quality.window_end'
    foreach ($name in @(
            'source_row_count', 'iceberg_row_count', 'duplicate_key_count', 'orphan_key_count',
            'invalid_value_count', 'temporal_anomaly_count', 'amount_comparable_order_count',
            'amount_reconciled_order_count', 'amount_mismatch_order_count')) {
        $result[$name] = ConvertTo-G2eOrderCount $result[$name] "quality.$name"
    }
    foreach ($name in @(
            'amount_reconciliation_rate', 'payment_item_freight_abs_difference_avg',
            'payment_item_freight_abs_difference_p50',
            'payment_item_freight_abs_difference_p90')) {
        $decimal = ConvertTo-G2eOrderDecimal $result[$name] "quality.$name" -AllowNull
        $result[$name] = if ($null -eq $decimal) {
            $null
        } else {
            $decimal.ToString('0.000000', [Globalization.CultureInfo]::InvariantCulture)
        }
    }
    $result['raw_row_counts_json'] = ConvertTo-G2eOrderCanonicalMapJson $context.RawCounts
    $result['normalized_row_counts_json'] = ConvertTo-G2eOrderCanonicalMapJson $context.NormalizedCounts
    $result['iceberg_row_counts_json'] = ConvertTo-G2eOrderCanonicalMapJson $context.IcebergCounts
    $result['normalized_sha256_json'] = ConvertTo-G2eOrderCanonicalMapJson $context.NormalizedHashes
    $result['source_snapshots_json'] = ConvertTo-G2eOrderCanonicalMapJson $context.SourceSnapshots
    $result['curated_snapshots_json'] = ConvertTo-G2eOrderCanonicalMapJson $context.CuratedSnapshots
    $result['fact_reconciliations_json'] = ConvertTo-G2eOrderCanonicalMapJson $context.Grain
    $result['reportable_quality_json'] = ConvertTo-G2eOrderCanonicalMapJson $reportable
    $result['reconciliation_status'] = 'PASS'
    return [pscustomobject]$result
}

function Assert-G2eOrderRate {
    param($Value, [long]$Numerator, [long]$Denominator, [string]$Name)
    if ($Denominator -eq 0) {
        if ($null -ne $Value -and -not [string]::IsNullOrEmpty([string]$Value)) {
            throw "$Name must be null for a zero denominator."
        }
        return
    }
    if ($Numerator -gt $Denominator) { throw "$Name numerator exceeds its denominator." }
    $actual = ConvertTo-G2eOrderDecimal $Value $Name
    $expected = [Math]::Round(([decimal]$Numerator / [decimal]$Denominator), 6,
        [MidpointRounding]::AwayFromZero)
    if ($actual -ne $expected) { throw "$Name does not match its numerator and denominator." }
}

function Get-G2eOrderWindowKey {
    param($Row, [string]$Family)
    $type = [string](Get-G2eOrderProperty $Row 'window_type')
    if (@('DAY', 'MONTH', 'FULL') -cnotcontains $type) { throw "$Family has an invalid window type." }
    $start = ConvertTo-G2eOrderDate (Get-G2eOrderProperty $Row 'window_start') "$Family.window_start"
    $end = ConvertTo-G2eOrderDate (Get-G2eOrderProperty $Row 'window_end') "$Family.window_end"
    if ($start -cgt $end) { throw "$Family contains an inverted window." }
    if ($type -ceq 'DAY' -and $start -cne $end) { throw "$Family DAY window must cover one date." }
    if ($type -ceq 'MONTH' -and $start.Substring(0, 7) -cne $end.Substring(0, 7)) {
        throw "$Family MONTH window crosses a calendar month."
    }
    return "$type$([char]31)$start$([char]31)$end"
}

function Assert-G2eOrderOptionalIdentity {
    param($Row, $Identity, [string]$Family)
    $fields = [string[]]@('metric_run_id', 'dataset_id', 'metric_version')
    $present = @($fields | Where-Object { Test-G2eOrderProperty $Row $_ })
    if ($present.Count -eq 0) { return }
    if ($present.Count -ne 3) { throw "$Family row identity is incomplete." }
    $expected = @($Identity.MetricRunId, $Identity.DatasetId, $Identity.MetricVersion)
    for ($index = 0; $index -lt 3; $index++) {
        if ([string](Get-G2eOrderProperty $Row $fields[$index]) -cne [string]$expected[$index]) {
            throw "$Family row identity differs from the metric run."
        }
    }
}

function Assert-G2eOrderUniqueKeys {
    param([object[]]$Rows, [string[]]$Columns, [string]$Family)
    $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($row in $Rows) {
        $parts = foreach ($column in $Columns) {
            $value = Get-G2eOrderProperty $row $column
            if ($null -eq $value -or [string]::IsNullOrEmpty([string]$value)) {
                throw "$Family key '$column' cannot be null or empty."
            }
            [string]$value
        }
        if (-not $seen.Add(($parts -join ([char]31)))) { throw "Duplicate $Family key detected." }
    }
}

function Assert-G2eOrderWindowCoverage {
    param([object[]]$Rows, $Identity, [string]$Family, [switch]$RequireFull)
    $keys = [Collections.Generic.List[string]]::new()
    $fullCount = 0
    foreach ($row in $Rows) {
        Assert-G2eOrderOptionalIdentity $row $Identity $Family
        $key = Get-G2eOrderWindowKey $row $Family
        $keys.Add($key)
        $start = ConvertTo-G2eOrderDate (Get-G2eOrderProperty $row 'window_start') "$Family.window_start"
        $end = ConvertTo-G2eOrderDate (Get-G2eOrderProperty $row 'window_end') "$Family.window_end"
        if ($start -clt $Identity.WindowStart -or $end -cgt $Identity.WindowEnd) {
            throw "$Family window escapes the publication range."
        }
        if ([string](Get-G2eOrderProperty $row 'window_type') -ceq 'FULL') {
            $fullCount++
            if ($start -cne $Identity.WindowStart -or $end -cne $Identity.WindowEnd) {
                throw "$Family FULL window differs from the publication range."
            }
        }
    }
    if ($RequireFull -and $fullCount -ne 1) { throw "$Family must contain exactly one FULL row." }
    return [string[]]$keys.ToArray()
}

function Assert-G2eOrderSameSet {
    param([string[]]$Expected, [string[]]$Actual, [string]$Name)
    $left = [Collections.Generic.HashSet[string]]::new($Expected, [StringComparer]::Ordinal)
    $right = [Collections.Generic.HashSet[string]]::new($Actual, [StringComparer]::Ordinal)
    if ($left.Count -ne $right.Count -or -not $left.SetEquals($right)) { throw "$Name sets differ." }
}

function Assert-G2eOrderAdditiveWindows {
    param([object[]]$Rows, [string[]]$Columns, [string]$Family)
    $full = @($Rows | Where-Object { [string](Get-G2eOrderProperty $_ 'window_type') -ceq 'FULL' })
    if ($full.Count -ne 1) { throw "$Family must contain one FULL row." }
    foreach ($type in @('DAY', 'MONTH')) {
        $parts = @($Rows | Where-Object { [string](Get-G2eOrderProperty $_ 'window_type') -ceq $type })
        if ($parts.Count -eq 0) { throw "$Family must contain $type rows." }
        foreach ($column in $Columns) {
            $expected = ConvertTo-G2eOrderDecimal (Get-G2eOrderProperty $full[0] $column) "$Family.$column"
            $actual = 0D
            foreach ($row in $parts) { $actual += ConvertTo-G2eOrderDecimal (Get-G2eOrderProperty $row $column) "$Family.$column" }
            if ($actual -ne $expected) { throw "$Family $type '$column' does not reconcile to FULL." }
        }
    }
}

function Assert-G2eMetricBundle {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)][object[]]$Overview,
        [Parameter(Mandatory = $true)][object[]]$Delivery,
        [Parameter(Mandatory = $true)][object[]]$Payment,
        [Parameter(Mandatory = $true)][object[]]$Ranking,
        [Parameter(Mandatory = $true)][object[]]$Review,
        [Parameter(Mandatory = $true)][object[]]$Quality
    )

    foreach ($family in @{
            overview=$Overview; delivery=$Delivery; payment=$Payment;
            ranking=$Ranking; review=$Review; quality=$Quality
        }.GetEnumerator()) {
        if ($family.Value.Count -eq 0) { throw "Metric family '$($family.Key)' is empty." }
    }
    Assert-G2eOrderUniqueKeys $Overview @('window_type', 'window_start') 'overview'
    Assert-G2eOrderUniqueKeys $Delivery @('window_type', 'window_start') 'delivery'
    Assert-G2eOrderUniqueKeys $Payment @('window_type', 'window_start', 'payment_type') 'payment'
    Assert-G2eOrderUniqueKeys $Ranking @('window_type', 'window_start', 'dimension_type', 'dimension_id') 'ranking'
    Assert-G2eOrderUniqueKeys $Review @('window_type', 'window_start') 'review'

    $overviewKeys = Assert-G2eOrderWindowCoverage $Overview $Identity 'overview' -RequireFull
    $deliveryKeys = Assert-G2eOrderWindowCoverage $Delivery $Identity 'delivery' -RequireFull
    $reviewKeys = Assert-G2eOrderWindowCoverage $Review $Identity 'review' -RequireFull
    Assert-G2eOrderSameSet $overviewKeys $deliveryKeys 'overview/delivery windows'
    Assert-G2eOrderSameSet $overviewKeys $reviewKeys 'overview/review windows'

    foreach ($row in $Overview) {
        $orders = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $row 'order_count') 'overview.order_count' -Positive
        $eligible = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $row 'status_eligible_order_count') 'overview.status_eligible_order_count'
        $excluded = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $row 'status_excluded_order_count') 'overview.status_excluded_order_count'
        if ($eligible + $excluded -ne $orders) { throw 'Overview status counts do not reconcile.' }
        $delivered = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $row 'delivered_order_count') 'overview.delivered_order_count'
        $canceled = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $row 'canceled_order_count') 'overview.canceled_order_count'
        $null = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $row 'unavailable_order_count') 'overview.unavailable_order_count'
        Assert-G2eOrderRate (Get-G2eOrderProperty $row 'delivered_rate') $delivered $eligible 'overview.delivered_rate'
        Assert-G2eOrderRate (Get-G2eOrderProperty $row 'canceled_rate') $canceled $eligible 'overview.canceled_rate'
        $unique = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $row 'unique_customer_count') 'overview.unique_customer_count' -Positive
        $repeat = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $row 'repeat_customer_count') 'overview.repeat_customer_count'
        if ($unique -gt $orders -or $repeat -gt $unique) { throw 'Overview customer counts exceed their bounds.' }
        Assert-G2eOrderRate (Get-G2eOrderProperty $row 'repeat_customer_rate') $repeat $unique 'overview.repeat_customer_rate'
        $itemRows = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $row 'item_row_count') 'overview.item_row_count'
        foreach ($name in @('item_value_sum', 'freight_value_sum', 'payment_value_sum')) {
            $null = ConvertTo-G2eOrderDecimal (Get-G2eOrderProperty $row $name) "overview.$name"
        }
        $expectedItems = [Math]::Round(([decimal]$itemRows / [decimal]$orders), 6, [MidpointRounding]::AwayFromZero)
        if ((ConvertTo-G2eOrderDecimal (Get-G2eOrderProperty $row 'items_per_order_avg') 'overview.items_per_order_avg') -ne $expectedItems) {
            throw 'Overview items-per-order average does not match its counts.'
        }
    }
    $fullOverview = @($Overview | Where-Object { $_.window_type -ceq 'FULL' })[0]
    if ((ConvertTo-G2eOrderCount $fullOverview.order_count 'overview.FULL.order_count') -ne [long]$Identity.SourceOrderCount) {
        throw 'FULL overview order count differs from source identity.'
    }
    Assert-G2eOrderAdditiveWindows $Overview @(
        'order_count', 'delivered_order_count', 'canceled_order_count', 'unavailable_order_count',
        'status_eligible_order_count', 'status_excluded_order_count', 'item_row_count',
        'item_value_sum', 'freight_value_sum', 'payment_value_sum'
    ) 'overview'

    foreach ($row in $Delivery) {
        $key = Get-G2eOrderWindowKey $row 'delivery'
        $matchingOverview = @($Overview | Where-Object { (Get-G2eOrderWindowKey $_ 'overview') -ceq $key })
        if ($matchingOverview.Count -ne 1) { throw 'Delivery window has no exact overview row.' }
        $orders = ConvertTo-G2eOrderCount $matchingOverview[0].order_count 'overview.order_count'
        $eligible = ConvertTo-G2eOrderCount $row.delivery_eligible_order_count 'delivery.delivery_eligible_order_count'
        $excluded = ConvertTo-G2eOrderCount $row.delivery_excluded_order_count 'delivery.delivery_excluded_order_count'
        $late = ConvertTo-G2eOrderCount $row.late_delivery_order_count 'delivery.late_delivery_order_count'
        $lateEligible = ConvertTo-G2eOrderCount $row.late_delivery_eligible_order_count 'delivery.late_delivery_eligible_order_count'
        $lateExcluded = ConvertTo-G2eOrderCount $row.late_delivery_excluded_order_count 'delivery.late_delivery_excluded_order_count'
        if ($eligible + $excluded -ne $orders -or $lateEligible + $lateExcluded -ne $orders) {
            throw 'Delivery eligibility counts do not reconcile to orders.'
        }
        Assert-G2eOrderRate $row.late_delivery_rate $late $lateEligible 'delivery.late_delivery_rate'
        foreach ($name in @('delivery_days_avg', 'delivery_days_p50', 'delivery_days_p90')) {
            $null = ConvertTo-G2eOrderDecimal $row.$name "delivery.$name" -AllowNull
        }
    }
    Assert-G2eOrderAdditiveWindows $Delivery @(
        'delivery_eligible_order_count', 'delivery_excluded_order_count',
        'late_delivery_order_count', 'late_delivery_eligible_order_count',
        'late_delivery_excluded_order_count'
    ) 'delivery'

    $paymentWindowKeys = [string[]]@($Payment | ForEach-Object { Get-G2eOrderWindowKey $_ 'payment' } | Select-Object -Unique)
    Assert-G2eOrderSameSet $overviewKeys $paymentWindowKeys 'overview/payment windows'
    foreach ($key in $overviewKeys) {
        $rows = @($Payment | Where-Object { (Get-G2eOrderWindowKey $_ 'payment') -ceq $key })
        $allRows = @($rows | Where-Object { ConvertTo-G2eOrderBoolean $_.is_all 'payment.is_all' })
        if ($allRows.Count -ne 1 -or [string]$allRows[0].payment_type -cne '__ALL__') {
            throw 'Each payment window must contain one __ALL__ row.'
        }
        $matchingOverview = @($Overview | Where-Object { (Get-G2eOrderWindowKey $_ 'overview') -ceq $key })[0]
        $all = $allRows[0]
        if ((ConvertTo-G2eOrderCount $all.global_order_count 'payment.global_order_count') -ne
                (ConvertTo-G2eOrderCount $matchingOverview.order_count 'overview.order_count')) {
            throw 'Payment global order count differs from overview.'
        }
        if ((ConvertTo-G2eOrderDecimal $all.payment_type_value_sum 'payment.payment_type_value_sum') -ne
                (ConvertTo-G2eOrderDecimal $matchingOverview.payment_value_sum 'overview.payment_value_sum')) {
            throw 'Payment __ALL__ value differs from overview.'
        }
        $typed = @($rows | Where-Object { -not (ConvertTo-G2eOrderBoolean $_.is_all 'payment.is_all') })
        $typedRows = 0L; $typedValue = 0D
        foreach ($row in $rows) {
            $orderCount = ConvertTo-G2eOrderCount $row.payment_order_count 'payment.payment_order_count'
            $typeOrderCount = ConvertTo-G2eOrderCount $row.payment_type_order_count 'payment.payment_type_order_count'
            if ($orderCount -ne $typeOrderCount) { throw 'Payment type and payment order counts differ.' }
            $null = ConvertTo-G2eOrderCount $row.installment_order_count 'payment.installment_order_count'
        }
        foreach ($row in $typed) {
            $typedRows += ConvertTo-G2eOrderCount $row.payment_row_count 'payment.payment_row_count'
            $typedValue += ConvertTo-G2eOrderDecimal $row.payment_type_value_sum 'payment.payment_type_value_sum'
        }
        if ($typedRows -ne (ConvertTo-G2eOrderCount $all.payment_row_count 'payment.payment_row_count') -or
                $typedValue -ne (ConvertTo-G2eOrderDecimal $all.payment_type_value_sum 'payment.payment_type_value_sum')) {
            throw 'Payment-type rows do not reconcile to __ALL__.'
        }
    }

    $rankingWindowKeys = [string[]]@($Ranking | ForEach-Object {
        Get-G2eOrderWindowKey $_ 'ranking'
    } | Select-Object -Unique)
    Assert-G2eOrderSameSet $overviewKeys $rankingWindowKeys 'overview/ranking windows'
    foreach ($row in $Ranking) {
        Assert-G2eOrderOptionalIdentity $row $Identity 'ranking'
        $null = Get-G2eOrderWindowKey $row 'ranking'
        $type = [string](Get-G2eOrderProperty $row 'dimension_type')
        if (@('product', 'category', 'seller', 'customer_state', 'seller_state') -cnotcontains $type) {
            throw 'Ranking dimension type is invalid.'
        }
        $null = ConvertTo-G2eOrderBoolean (Get-G2eOrderProperty $row 'is_unknown') 'ranking.is_unknown'
        $null = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $row 'ranking_order_count') 'ranking.ranking_order_count' -Positive
        $null = ConvertTo-G2eOrderCount (Get-G2eOrderProperty $row 'ranking_customer_count') 'ranking.ranking_customer_count' -Positive
        if ($type -ceq 'seller_state') {
            if ((ConvertTo-G2eOrderBoolean $row.payment_value_is_additive 'ranking.payment_value_is_additive') -ne $false) {
                throw 'Seller-state payment values must be non-additive.'
            }
            $null = ConvertTo-G2eOrderDecimal $row.ranking_payment_value_sum 'ranking.ranking_payment_value_sum'
        } elseif ($type -ceq 'customer_state') {
            if ((ConvertTo-G2eOrderBoolean $row.payment_value_is_additive 'ranking.payment_value_is_additive') -ne $true) {
                throw 'Customer-state payment values must be additive.'
            }
            if ($null -ne $row.ranking_item_row_count -or $null -ne $row.ranking_item_value_sum -or
                    $null -ne $row.ranking_freight_value_sum) {
                throw 'Customer-state item measures must be null.'
            }
        } else {
            if ($null -ne $row.ranking_payment_value_sum -or $null -ne $row.payment_value_is_additive -or
                    $null -ne $row.ranking_late_delivery_rate) {
                throw 'Item-derived ranking rows contain inapplicable order measures.'
            }
        }
        if ($null -ne $row.ranking_late_delivery_eligible_order_count) {
            $late = ConvertTo-G2eOrderCount $row.ranking_late_delivery_order_count 'ranking.ranking_late_delivery_order_count'
            $eligible = ConvertTo-G2eOrderCount $row.ranking_late_delivery_eligible_order_count 'ranking.ranking_late_delivery_eligible_order_count'
            Assert-G2eOrderRate $row.ranking_late_delivery_rate $late $eligible 'ranking.ranking_late_delivery_rate'
        }
    }

    foreach ($row in $Review) {
        $key = Get-G2eOrderWindowKey $row 'review'
        $matchingOverview = @($Overview | Where-Object { (Get-G2eOrderWindowKey $_ 'overview') -ceq $key })[0]
        $all = ConvertTo-G2eOrderCount $row.all_order_count 'review.all_order_count' -Positive
        $reviewed = ConvertTo-G2eOrderCount $row.reviewed_order_count 'review.reviewed_order_count'
        $reviewRows = ConvertTo-G2eOrderCount $row.review_row_count 'review.review_row_count'
        $low = ConvertTo-G2eOrderCount $row.low_score_order_count 'review.low_score_order_count'
        $multi = ConvertTo-G2eOrderCount $row.multi_review_order_count 'review.multi_review_order_count'
        if ($all -ne (ConvertTo-G2eOrderCount $matchingOverview.order_count 'overview.order_count') -or
                $reviewed -gt $all -or $reviewRows -lt $reviewed -or $low -gt $reviewed -or $multi -gt $reviewed) {
            throw 'Review counts exceed their semantic bounds.'
        }
        Assert-G2eOrderRate $row.review_coverage_rate $reviewed $all 'review.review_coverage_rate'
        Assert-G2eOrderRate $row.low_score_rate $low $reviewed 'review.low_score_rate'
        $score = ConvertTo-G2eOrderDecimal $row.review_score_avg 'review.review_score_avg' -AllowNull
        if ($null -ne $score -and ($score -lt 1 -or $score -gt 5)) { throw 'Review score average is outside 1 through 5.' }
    }
    Assert-G2eOrderAdditiveWindows $Review @(
        'review_row_count', 'reviewed_order_count', 'all_order_count',
        'low_score_order_count', 'multi_review_order_count'
    ) 'review'

    if ($Quality.Count -ne 1) { throw 'Quality must contain exactly one row.' }
    $qualityRow = $Quality[0]
    Assert-G2eOrderOptionalIdentity $qualityRow $Identity 'quality'
    $null = Assert-G2eOrderWindowCoverage @($qualityRow) $Identity 'quality' -RequireFull
    if ((ConvertTo-G2eOrderCount $qualityRow.source_row_count 'quality.source_row_count') -ne [long]$Identity.SourceOrderCount -or
            (ConvertTo-G2eOrderCount $qualityRow.iceberg_row_count 'quality.iceberg_row_count') -ne [long]$Identity.SourceOrderCount) {
        throw 'Quality order counts differ from the metric identity.'
    }
    if ((ConvertTo-G2eOrderCount $qualityRow.duplicate_key_count 'quality.duplicate_key_count') -ne 0 -or
            (ConvertTo-G2eOrderCount $qualityRow.orphan_key_count 'quality.orphan_key_count') -ne 0) {
        throw 'Quality hard gates must be zero.'
    }
    $comparable = ConvertTo-G2eOrderCount $qualityRow.amount_comparable_order_count 'quality.amount_comparable_order_count'
    $reconciled = ConvertTo-G2eOrderCount $qualityRow.amount_reconciled_order_count 'quality.amount_reconciled_order_count'
    $mismatch = ConvertTo-G2eOrderCount $qualityRow.amount_mismatch_order_count 'quality.amount_mismatch_order_count'
    if ($reconciled + $mismatch -ne $comparable) { throw 'Quality amount counts do not reconcile.' }
    Assert-G2eOrderRate $qualityRow.amount_reconciliation_rate $reconciled $comparable 'quality.amount_reconciliation_rate'
    foreach ($name in @('raw_row_counts_json', 'normalized_row_counts_json', 'iceberg_row_counts_json',
            'normalized_sha256_json', 'source_snapshots_json', 'curated_snapshots_json',
            'fact_reconciliations_json', 'reportable_quality_json')) {
        $text = [string](Get-G2eOrderProperty $qualityRow $name)
        try { $map = $text | ConvertFrom-Json } catch { throw "Quality $name is malformed JSON." }
        if ((ConvertTo-G2eOrderCanonicalMapJson $map) -cne $text) { throw "Quality $name is not canonical JSON." }
    }
    if ([string]$qualityRow.reconciliation_status -cne 'PASS') { throw 'Quality status is not PASS.' }
    return [pscustomobject][ordered]@{
        Status = 'PASS'
        OverviewRowCount = $Overview.Count
        DeliveryRowCount = $Delivery.Count
        PaymentRowCount = $Payment.Count
        RankingRowCount = $Ranking.Count
        ReviewRowCount = $Review.Count
        QualityRowCount = 1
    }
}

function ConvertTo-G2eCanonicalValue {
    param($Value, [string]$Column)
    if ($null -eq $Value) { return $null }
    if (@('is_all', 'is_unknown', 'payment_value_is_additive') -ccontains $Column) {
        $boolean = ConvertTo-G2eOrderBoolean $Value $Column -AllowNull
        if ($null -eq $boolean) { return $null }
        return $(if ($boolean) { '1' } else { '0' })
    }
    if ($Column -match '(_count|_row_count)$') {
        return (ConvertTo-G2eOrderCount $Value $Column).ToString([Globalization.CultureInfo]::InvariantCulture)
    }
    if ($Column -match '(item_value_sum|freight_value_sum|payment_value_sum|payment_type_value_sum)$' -or
            $Column -match '^ranking_(item|freight|payment)_value_sum$') {
        $decimal = ConvertTo-G2eOrderDecimal $Value $Column -AllowNull
        if ($null -eq $decimal) { return $null }
        return $decimal.ToString('0.00', [Globalization.CultureInfo]::InvariantCulture)
    }
    if ($Column -match '(_rate|_avg|_p50|_p90)$' -or $Column -ceq 'items_per_order_avg' -or
            $Column -ceq 'review_score_avg') {
        $decimal = ConvertTo-G2eOrderDecimal $Value $Column -AllowNull
        if ($null -eq $decimal) { return $null }
        return $decimal.ToString('0.000000', [Globalization.CultureInfo]::InvariantCulture)
    }
    if ($Column -ceq 'window_start' -or $Column -ceq 'window_end' -or $Column -match '_date$') {
        return ConvertTo-G2eOrderDate $Value $Column
    }
    if ($Column -match '_at$' -or $Column -match '_timestamp$') {
        return ConvertTo-G2eOrderTimestamp $Value $Column
    }
    return [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
}

function ConvertTo-G2eCsvCell {
    param($Value, [string]$Column)
    $normalized = ConvertTo-G2eCanonicalValue $Value $Column
    if ($null -eq $normalized) { return '\N' }
    $text = [string]$normalized
    if ($text.Length -eq 0) { return '""' }
    if ($text -ceq '\N' -or $text.IndexOfAny([char[]]@(',', '"', "`r", "`n")) -ge 0) {
        return '"' + $text.Replace('"', '""') + '"'
    }
    return $text
}

function Get-G2eCanonicalCsvPayload {
    param([object[]]$Rows, [string[]]$Columns, [string[]]$UniqueKeyColumns)
    if ($Rows.Count -eq 0 -or $Columns.Count -eq 0 -or $UniqueKeyColumns.Count -eq 0) {
        throw 'Canonical rows, columns and unique keys are required.'
    }
    foreach ($key in $UniqueKeyColumns) {
        if ($Columns -cnotcontains $key) { throw 'Canonical key is absent from the fixed columns.' }
    }
    $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $sortable = [Collections.Generic.List[object]]::new()
    foreach ($row in $Rows) {
        $cells = [ordered]@{}
        foreach ($column in $Columns) {
            $cells[$column] = ConvertTo-G2eCsvCell (Get-G2eOrderProperty $row $column) $column
        }
        $keyCells = foreach ($column in $UniqueKeyColumns) {
            if ($cells[$column] -ceq '\N' -or $cells[$column] -ceq '""') { throw 'Canonical keys cannot be null or empty.' }
            $cells[$column]
        }
        $sortKey = $keyCells -join ([char]31)
        if (-not $seen.Add($sortKey)) { throw 'Duplicate normalized canonical key detected.' }
        $sortable.Add([pscustomobject]@{ SortKey=$sortKey; Line=([string[]]$cells.Values -join ',') })
    }
    $sortKeys = [string[]]@($sortable | ForEach-Object { $_.SortKey })
    $lines = [string[]]@($sortable | ForEach-Object { $_.Line })
    [Array]::Sort($sortKeys, $lines, [StringComparer]::Ordinal)
    return (($Columns -join ',') + "`n" + ($lines -join "`n") + "`n")
}

function Get-G2eCanonicalDigest {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][object[]]$Rows,
        [Parameter(Mandatory = $true)][string[]]$Columns,
        [Parameter(Mandatory = $true)][string[]]$UniqueKeyColumns
    )
    $payload = Get-G2eCanonicalCsvPayload $Rows $Columns $UniqueKeyColumns
    $bytes = [Text.UTF8Encoding]::new($false).GetBytes($payload)
    $algorithm = [Security.Cryptography.SHA256]::Create()
    try { $digest = $algorithm.ComputeHash($bytes) } finally { $algorithm.Dispose() }
    return ([BitConverter]::ToString($digest)).Replace('-', '').ToLowerInvariant()
}

function Export-G2eCandidateCsv {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Target,
        [Parameter(Mandatory = $true)][object[]]$Rows,
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)][string]$OutputDirectory,
        [Parameter(Mandatory = $true)][string]$OutputPath
    )
    if ($script:G2eMetricFamilies -cnotcontains $Target) { throw 'G2-E candidate target is unknown.' }
    if ($Rows.Count -eq 0) { throw 'G2-E candidate export requires rows.' }
    $root = [IO.Path]::GetFullPath($OutputDirectory)
    if (-not [IO.Directory]::Exists($root)) { throw 'G2-E output directory does not exist.' }
    $path = [IO.Path]::GetFullPath($OutputPath)
    $comparison = if ([IO.Path]::DirectorySeparatorChar -eq '\') { [StringComparison]::OrdinalIgnoreCase } else { [StringComparison]::Ordinal }
    $prefix = $root.TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
    if (-not $path.StartsWith($prefix, $comparison) -or [IO.Path]::GetDirectoryName($path) -cne $root) {
        throw 'G2-E candidate path must be a direct child of the output directory.'
    }
    foreach ($candidate in @($root, $path)) {
        if ([IO.File]::Exists($candidate) -or [IO.Directory]::Exists($candidate)) {
            $item = Get-Item -LiteralPath $candidate -Force
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw 'G2-E candidate path cannot be a reparse point.'
            }
        }
    }
    $columns = [string[]]$script:G2eTargetColumns[$Target]
    $enriched = [Collections.Generic.List[object]]::new()
    foreach ($row in $Rows) {
        $copy = [ordered]@{}
        foreach ($column in $columns) {
            if ($column -ceq 'metric_run_id') { $value = [string]$Identity.MetricRunId }
            elseif ($column -ceq 'dataset_id') { $value = [string]$Identity.DatasetId }
            elseif ($column -ceq 'metric_version') { $value = [string]$Identity.MetricVersion }
            else { $value = Get-G2eOrderProperty $row $column }
            if ((Test-G2eOrderProperty $row $column) -and @('metric_run_id','dataset_id','metric_version') -ccontains $column -and
                    [string](Get-G2eOrderProperty $row $column) -cne [string]$value) {
                throw 'Candidate row identity differs from the metric identity.'
            }
            $copy[$column] = $value
        }
        $enriched.Add([pscustomobject]$copy)
    }
    $payload = Get-G2eCanonicalCsvPayload $enriched.ToArray() $columns ([string[]]$script:G2eUniqueKeyColumns[$Target])
    $temporary = Join-Path $root ('.g2e-' + [guid]::NewGuid().ToString('N') + '.tmp')
    try {
        [IO.File]::WriteAllText($temporary, $payload, [Text.UTF8Encoding]::new($false))
        [IO.File]::Move($temporary, $path)
    } finally {
        if ([IO.File]::Exists($temporary)) { [IO.File]::Delete($temporary) }
    }
    return $path
}

Export-ModuleMember -Function @(
    'Get-G2eMetricIdentity',
    'Split-G2eNamedSql',
    'ConvertFrom-G2eCsv',
    'Merge-G2eQualityEvidence',
    'Assert-G2eMetricBundle',
    'Get-G2eCanonicalDigest',
    'Export-G2eCandidateCsv'
)
