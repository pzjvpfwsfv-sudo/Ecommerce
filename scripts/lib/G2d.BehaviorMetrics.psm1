Set-StrictMode -Version Latest

$script:G2dSqlResultNames = @('source_identity', 'overview', 'funnel', 'dimension', 'quality')
$script:G2dPublicationOrder = @('overview', 'funnel', 'dimension', 'quality', 'publication')
$script:G2dTargetColumns = [ordered]@{
    overview = @(
        'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
        'window_end', 'event_count', 'view_count', 'cart_count', 'purchase_count',
        'unique_user_count', 'session_count', 'product_count', 'purchase_amount_proxy'
    )
    funnel = @(
        'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
        'window_end', 'missing_session_event_count', 'view_sessions',
        'view_to_cart_sessions', 'completed_sessions'
    )
    dimension = @(
        'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
        'window_end', 'dimension_type', 'dimension_id', 'dimension_name', 'is_unknown',
        'view_count', 'cart_count', 'purchase_count', 'unique_user_count',
        'purchase_amount_proxy'
    )
    quality = @(
        'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
        'window_end', 'source_event_count', 'clean_event_count', 'late_event_count',
        'clean_event_rate', 'late_event_rate', 'distinct_event_count',
        'duplicate_event_count', 'missing_session_count', 'unknown_category_count',
        'unknown_brand_count', 'invalid_event_type_count', 'empty_key_id_count',
        'invalid_price_count', 'invalid_derived_date_count', 'overview_event_count',
        'reconciliation_status'
    )
}

function ConvertTo-G2dInt64 {
    param(
        [Parameter(Mandatory = $true)]$Value,
        [Parameter(Mandatory = $true)][string]$Name,
        [switch]$Positive
    )

    if ($null -eq $Value) { throw "$Name must be an integer." }
    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    if ($text -notmatch '^(0|[1-9][0-9]*)$') { throw "$Name must be an integer." }
    $parsed = 0L
    if (-not [long]::TryParse(
            $text,
            [Globalization.NumberStyles]::None,
            [Globalization.CultureInfo]::InvariantCulture,
            [ref]$parsed)) {
        throw "$Name is outside the Int64 range."
    }
    if ($Positive -and $parsed -le 0) { throw "$Name must be positive." }
    return $parsed
}

function Get-G2dPropertyValue {
    param(
        [Parameter(Mandatory = $true)]$Row,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $property = $Row.PSObject.Properties[$Name]
    if ($null -eq $property) { throw "Required column '$Name' is missing." }
    return $property.Value
}

function ConvertTo-G2dDateText {
    param(
        [Parameter(Mandatory = $true)]$Value,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    $parsed = [datetime]::MinValue
    if (-not [datetime]::TryParseExact(
            $text,
            'yyyy-MM-dd',
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::None,
            [ref]$parsed)) {
        throw "$Name must use yyyy-MM-dd."
    }
    return $parsed.ToString('yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
}

function ConvertTo-G2dTimestampText {
    param(
        [Parameter(Mandatory = $true)]$Value,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    $parsed = [datetimeoffset]::MinValue
    if (-not [datetimeoffset]::TryParse(
            $text,
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::AssumeUniversal,
            [ref]$parsed)) {
        throw "$Name must be an ISO-8601 timestamp."
    }
    return $parsed.ToUniversalTime().ToString(
        "yyyy-MM-dd'T'HH:mm:ss.fff'Z'",
        [Globalization.CultureInfo]::InvariantCulture
    )
}

function ConvertTo-G2dBooleanInt {
    param(
        [Parameter(Mandatory = $true)]$Value,
        [Parameter(Mandatory = $true)][string]$Name
    )

    if ($Value -is [bool]) { return [int][bool]$Value }
    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    if ($text -ceq '1' -or $text -ceq 'true' -or $text -ceq 'True') { return 1 }
    if ($text -ceq '0' -or $text -ceq 'false' -or $text -ceq 'False') { return 0 }
    throw "$Name must be a boolean or 0/1."
}

function Assert-G2dAmount {
    param(
        [Parameter(Mandatory = $true)]$Value,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    if ($text -notmatch '^[0-9]+\.[0-9]{2}$') {
        throw "$Name must be a nonnegative amount with two decimal places."
    }
    $parsed = 0D
    if (-not [decimal]::TryParse(
            $text,
            [Globalization.NumberStyles]::AllowDecimalPoint,
            [Globalization.CultureInfo]::InvariantCulture,
            [ref]$parsed)) {
        throw "$Name is not a valid decimal amount."
    }
}

function Assert-G2dRate {
    param(
        [Parameter(Mandatory = $true)]$Value,
        [Parameter(Mandatory = $true)][string]$Name
    )

    if ($null -eq $Value) { return }
    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    $parsed = 0D
    if (-not [decimal]::TryParse(
            $text,
            [Globalization.NumberStyles]::AllowDecimalPoint,
            [Globalization.CultureInfo]::InvariantCulture,
            [ref]$parsed) -or $parsed -lt 0) {
        throw "$Name must be a nonnegative decimal."
    }
}

function Assert-G2dUniqueKeys {
    param(
        [Parameter(Mandatory = $true)][object[]]$Rows,
        [Parameter(Mandatory = $true)][string[]]$Columns,
        [Parameter(Mandatory = $true)][string]$Family
    )

    $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($row in $Rows) {
        $parts = foreach ($column in $Columns) {
            $value = Get-G2dPropertyValue -Row $row -Name $column
            if ($null -eq $value) { throw "$Family key '$column' cannot be null." }
            [Convert]::ToString($value, [Globalization.CultureInfo]::InvariantCulture)
        }
        $key = $parts -join "`u{001F}"
        if (-not $seen.Add($key)) { throw "Duplicate $Family composite key detected." }
    }
}

function Assert-G2dWindowRows {
    param(
        [Parameter(Mandatory = $true)][object[]]$Rows,
        [Parameter(Mandatory = $true)][string]$Family,
        [Parameter(Mandatory = $true)][string]$WindowStart,
        [Parameter(Mandatory = $true)][string]$WindowEnd,
        [switch]$RequireFull
    )

    $fullCount = 0
    foreach ($row in $Rows) {
        $windowType = [string](Get-G2dPropertyValue -Row $row -Name 'window_type')
        $start = ConvertTo-G2dDateText (Get-G2dPropertyValue -Row $row -Name 'window_start') "$Family.window_start"
        $end = ConvertTo-G2dDateText (Get-G2dPropertyValue -Row $row -Name 'window_end') "$Family.window_end"
        if ($windowType -ceq 'FULL') {
            $fullCount++
            if ($start -cne $WindowStart -or $end -cne $WindowEnd) {
                throw "$Family FULL window does not match source window."
            }
        } elseif ($windowType -ceq 'DAY') {
            if ($start -cne $end -or $start -clt $WindowStart -or $start -cgt $WindowEnd) {
                throw "$Family DAY window is invalid."
            }
        } else {
            throw "$Family has an unknown window type."
        }
    }
    if ($RequireFull -and $fullCount -ne 1) { throw "$Family must contain exactly one FULL row." }
}

function Get-G2dMetricIdentity {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$SnapshotId,
        [Parameter(Mandatory = $true)][string]$DataScope,
        [Parameter(Mandatory = $true)]$SourceEventCount
    )

    $snapshot = ConvertTo-G2dInt64 -Value $SnapshotId -Name 'G2-D snapshot ID' -Positive
    $sourceCount = ConvertTo-G2dInt64 -Value $SourceEventCount -Name 'G2-D source event count'
    if ($DataScope -ceq 'g2c-correctness-subset') {
        $expected = 1002L
    } elseif ($DataScope -ceq 'stable-user-2pct-full') {
        $expected = 2199938L
    } else {
        throw 'G2-D data scope is unknown.'
    }
    if ($sourceCount -ne $expected) { throw 'G2-D data scope does not match source count.' }

    return [pscustomobject][ordered]@{
        MetricRunId = "behavior-v1-s$snapshot"
        DatasetId = 'rees46-multicategory'
        MetricVersion = 'behavior-v1'
        DataScope = $DataScope
        SourceSnapshotId = $snapshot
        SourceEventCount = $sourceCount
    }
}

function Get-G2dPublicationOrder {
    [CmdletBinding()]
    param([string[]]$Order)

    if ($PSBoundParameters.ContainsKey('Order')) {
        if ($Order.Count -ne $script:G2dPublicationOrder.Count) {
            throw 'G2-D publication order is incomplete.'
        }
        for ($index = 0; $index -lt $script:G2dPublicationOrder.Count; $index++) {
            if ($Order[$index] -cne $script:G2dPublicationOrder[$index]) {
                throw 'G2-D publication must follow all candidate targets.'
            }
        }
    }
    return @($script:G2dPublicationOrder)
}

function Split-G2dNamedSql {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [Parameter(Mandatory = $true)]$SnapshotId
    )

    $snapshot = ConvertTo-G2dInt64 -Value $SnapshotId -Name 'G2-D snapshot ID' -Positive
    $placeholderMatches = [regex]::Matches($Sql, '__[A-Za-z0-9_]+__')
    if ($placeholderMatches.Count -eq 0) { throw 'G2-D SQL snapshot placeholder is missing.' }
    foreach ($match in $placeholderMatches) {
        if ($match.Value -cne '__SNAPSHOT_ID__') { throw 'G2-D SQL contains an unknown placeholder.' }
    }

    $markerMatches = [regex]::Matches($Sql, '(?m)^-- result:([a-z_]+)\r?$')
    if ($markerMatches.Count -ne $script:G2dSqlResultNames.Count) {
        throw 'G2-D SQL must contain exactly five result markers.'
    }
    for ($index = 0; $index -lt $script:G2dSqlResultNames.Count; $index++) {
        if ($markerMatches[$index].Groups[1].Value -cne $script:G2dSqlResultNames[$index]) {
            throw 'G2-D SQL result markers are missing, duplicated, or out of order.'
        }
    }

    $result = [ordered]@{}
    for ($index = 0; $index -lt $markerMatches.Count; $index++) {
        $start = $markerMatches[$index].Index + $markerMatches[$index].Length
        $end = if ($index + 1 -lt $markerMatches.Count) {
            $markerMatches[$index + 1].Index
        } else {
            $Sql.Length
        }
        $statement = $Sql.Substring($start, $end - $start).Trim()
        if (-not $statement.EndsWith(';', [StringComparison]::Ordinal) -or
                ([regex]::Matches($statement, ';')).Count -ne 1) {
            throw 'Each G2-D SQL result must contain exactly one semicolon-terminated statement.'
        }
        $name = $script:G2dSqlResultNames[$index]
        $result[$name] = $statement.Replace(
            '__SNAPSHOT_ID__',
            $snapshot.ToString([Globalization.CultureInfo]::InvariantCulture)
        )
    }
    return $result
}

function ConvertFrom-G2dCsv {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][AllowEmptyString()][string]$CsvText)

    $records = [Collections.Generic.List[object]]::new()
    $fields = [Collections.Generic.List[string]]::new()
    $field = [Text.StringBuilder]::new()
    $inQuotes = $false
    $afterQuote = $false
    $rowStarted = $false

    for ($index = 0; $index -lt $CsvText.Length; $index++) {
        $character = $CsvText[$index]
        if ($inQuotes) {
            if ($character -eq '"') {
                if ($index + 1 -lt $CsvText.Length -and $CsvText[$index + 1] -eq '"') {
                    $null = $field.Append('"')
                    $index++
                } else {
                    $inQuotes = $false
                    $afterQuote = $true
                }
            } else {
                $null = $field.Append($character)
            }
            continue
        }

        if ($afterQuote) {
            if ($character -eq ',') {
                $fields.Add($field.ToString())
                $null = $field.Clear()
                $afterQuote = $false
                $rowStarted = $true
            } elseif ($character -eq "`r" -or $character -eq "`n") {
                $fields.Add($field.ToString())
                $records.Add([string[]]$fields.ToArray())
                $fields = [Collections.Generic.List[string]]::new()
                $null = $field.Clear()
                $afterQuote = $false
                $rowStarted = $false
                if ($character -eq "`r" -and $index + 1 -lt $CsvText.Length -and $CsvText[$index + 1] -eq "`n") {
                    $index++
                }
            } else {
                throw 'Malformed CSV follows a closing quote.'
            }
            continue
        }

        if ($character -eq '"') {
            if ($field.Length -ne 0) { throw 'Malformed CSV contains a quote in an unquoted field.' }
            $inQuotes = $true
            $rowStarted = $true
        } elseif ($character -eq ',') {
            $fields.Add($field.ToString())
            $null = $field.Clear()
            $rowStarted = $true
        } elseif ($character -eq "`r" -or $character -eq "`n") {
            $fields.Add($field.ToString())
            $records.Add([string[]]$fields.ToArray())
            $fields = [Collections.Generic.List[string]]::new()
            $null = $field.Clear()
            $rowStarted = $false
            if ($character -eq "`r" -and $index + 1 -lt $CsvText.Length -and $CsvText[$index + 1] -eq "`n") {
                $index++
            }
        } else {
            $null = $field.Append($character)
            $rowStarted = $true
        }
    }

    if ($inQuotes) { throw 'Malformed CSV contains an unterminated quoted field.' }
    if ($afterQuote -or $rowStarted -or $fields.Count -gt 0 -or $field.Length -gt 0) {
        $fields.Add($field.ToString())
        $records.Add([string[]]$fields.ToArray())
    }
    if ($records.Count -lt 2) { throw 'G2-D CSV must contain one header and at least one data row.' }

    $headers = [string[]]$records[0]
    $seenHeaders = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($header in $headers) {
        if ([string]::IsNullOrWhiteSpace($header) -or -not $seenHeaders.Add($header)) {
            throw 'G2-D CSV headers must be nonempty and unique.'
        }
    }

    $result = [Collections.Generic.List[object]]::new()
    for ($rowIndex = 1; $rowIndex -lt $records.Count; $rowIndex++) {
        $values = [string[]]$records[$rowIndex]
        if ($values.Count -ne $headers.Count) { throw 'G2-D CSV row has an inconsistent column count.' }
        $row = [ordered]@{}
        for ($columnIndex = 0; $columnIndex -lt $headers.Count; $columnIndex++) {
            $row[$headers[$columnIndex]] = $values[$columnIndex]
        }
        $result.Add([pscustomobject]$row)
    }
    return @($result.ToArray())
}

function Assert-G2dMetricBundle {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)][object[]]$SourceIdentity,
        [Parameter(Mandatory = $true)][object[]]$Overview,
        [Parameter(Mandatory = $true)][object[]]$Funnel,
        [Parameter(Mandatory = $true)][object[]]$Dimension,
        [Parameter(Mandatory = $true)][object[]]$Quality
    )

    if ($SourceIdentity.Count -ne 1) { throw 'Source identity must contain exactly one row.' }
    if ($Overview.Count -eq 0 -or $Funnel.Count -eq 0 -or $Dimension.Count -eq 0 -or $Quality.Count -eq 0) {
        throw 'Every G2-D metric family must contain rows.'
    }

    $identitySourceCount = ConvertTo-G2dInt64 $Identity.SourceEventCount 'Identity.SourceEventCount'
    $identitySnapshot = ConvertTo-G2dInt64 $Identity.SourceSnapshotId 'Identity.SourceSnapshotId' -Positive
    $sourceRow = $SourceIdentity[0]
    $sourceCount = ConvertTo-G2dInt64 (Get-G2dPropertyValue $sourceRow 'source_event_count') 'source_identity.source_event_count'
    $sourceDistinct = ConvertTo-G2dInt64 (Get-G2dPropertyValue $sourceRow 'distinct_event_count') 'source_identity.distinct_event_count'
    $sourceSnapshot = ConvertTo-G2dInt64 (Get-G2dPropertyValue $sourceRow 'source_snapshot_id') 'source_identity.source_snapshot_id' -Positive
    $windowStart = ConvertTo-G2dDateText (Get-G2dPropertyValue $sourceRow 'window_start') 'source_identity.window_start'
    $windowEnd = ConvertTo-G2dDateText (Get-G2dPropertyValue $sourceRow 'window_end') 'source_identity.window_end'
    $null = ConvertTo-G2dTimestampText (Get-G2dPropertyValue $sourceRow 'snapshot_committed_at') 'source_identity.snapshot_committed_at'
    if ($windowStart -cgt $windowEnd) { throw 'Source window is inverted.' }
    if ($sourceCount -ne $identitySourceCount -or $sourceSnapshot -ne $identitySnapshot) {
        throw 'Source identity does not match metric identity.'
    }
    if ($sourceDistinct -ne $sourceCount) { throw 'Duplicate event IDs detected.' }

    Assert-G2dWindowRows $Overview 'overview' $windowStart $windowEnd -RequireFull
    Assert-G2dWindowRows $Funnel 'funnel' $windowStart $windowEnd -RequireFull
    Assert-G2dWindowRows $Dimension 'dimension' $windowStart $windowEnd
    Assert-G2dWindowRows $Quality 'quality' $windowStart $windowEnd -RequireFull
    Assert-G2dUniqueKeys $Overview @('window_type', 'window_start') 'overview'
    Assert-G2dUniqueKeys $Funnel @('window_type', 'window_start') 'funnel'
    Assert-G2dUniqueKeys $Dimension @('window_type', 'window_start', 'dimension_type', 'dimension_id') 'dimension'
    Assert-G2dUniqueKeys $Quality @('window_type', 'window_start') 'quality'

    $overviewIntegerColumns = @(
        'event_count', 'view_count', 'cart_count', 'purchase_count', 'unique_user_count',
        'session_count', 'product_count'
    )
    foreach ($row in $Overview) {
        foreach ($column in $overviewIntegerColumns) {
            $null = ConvertTo-G2dInt64 (Get-G2dPropertyValue $row $column) "overview.$column"
        }
        Assert-G2dAmount (Get-G2dPropertyValue $row 'purchase_amount_proxy') 'overview.purchase_amount_proxy'
    }

    $funnelIntegerColumns = @(
        'missing_session_event_count', 'view_sessions', 'view_to_cart_sessions', 'completed_sessions'
    )
    foreach ($row in $Funnel) {
        $counts = @{}
        foreach ($column in $funnelIntegerColumns) {
            $counts[$column] = ConvertTo-G2dInt64 (Get-G2dPropertyValue $row $column) "funnel.$column"
        }
        if ($counts.completed_sessions -gt $counts.view_to_cart_sessions -or
                $counts.view_to_cart_sessions -gt $counts.view_sessions) {
            throw 'Funnel stages are not monotonic.'
        }
    }

    $dimensionIntegerColumns = @('view_count', 'cart_count', 'purchase_count', 'unique_user_count')
    foreach ($row in $Dimension) {
        $dimensionType = [string](Get-G2dPropertyValue $row 'dimension_type')
        if (@('product', 'category', 'brand') -cnotcontains $dimensionType) {
            throw 'Unknown dimension type.'
        }
        $dimensionId = [string](Get-G2dPropertyValue $row 'dimension_id')
        if ([string]::IsNullOrEmpty($dimensionId)) { throw 'Dimension ID cannot be empty.' }
        $null = ConvertTo-G2dBooleanInt (Get-G2dPropertyValue $row 'is_unknown') 'dimension.is_unknown'
        foreach ($column in $dimensionIntegerColumns) {
            $null = ConvertTo-G2dInt64 (Get-G2dPropertyValue $row $column) "dimension.$column"
        }
        Assert-G2dAmount (Get-G2dPropertyValue $row 'purchase_amount_proxy') 'dimension.purchase_amount_proxy'
    }

    $qualityIntegerColumns = @(
        'source_event_count', 'clean_event_count', 'late_event_count', 'distinct_event_count',
        'duplicate_event_count', 'missing_session_count', 'unknown_category_count',
        'unknown_brand_count', 'invalid_event_type_count', 'empty_key_id_count',
        'invalid_price_count', 'invalid_derived_date_count', 'overview_event_count'
    )
    foreach ($row in $Quality) {
        foreach ($column in $qualityIntegerColumns) {
            $null = ConvertTo-G2dInt64 (Get-G2dPropertyValue $row $column) "quality.$column"
        }
        Assert-G2dRate (Get-G2dPropertyValue $row 'clean_event_rate') 'quality.clean_event_rate'
        Assert-G2dRate (Get-G2dPropertyValue $row 'late_event_rate') 'quality.late_event_rate'
    }

    $fullOverview = @($Overview | Where-Object { $_.window_type -ceq 'FULL' })[0]
    $fullFunnel = @($Funnel | Where-Object { $_.window_type -ceq 'FULL' })[0]
    $fullQuality = @($Quality | Where-Object { $_.window_type -ceq 'FULL' })[0]
    $fullEventCount = ConvertTo-G2dInt64 $fullOverview.event_count 'overview.event_count'
    if ($fullEventCount -ne $identitySourceCount) { throw 'Source total mismatch.' }
    $knownEventTotal =
        (ConvertTo-G2dInt64 $fullOverview.view_count 'overview.view_count') +
        (ConvertTo-G2dInt64 $fullOverview.cart_count 'overview.cart_count') +
        (ConvertTo-G2dInt64 $fullOverview.purchase_count 'overview.purchase_count')
    if ($knownEventTotal -gt $fullEventCount) { throw 'Event type totals exceed the source total.' }

    $dayTotal = 0L
    $dayRows = @($Overview | Where-Object { $_.window_type -ceq 'DAY' })
    if ($dayRows.Count -eq 0) { throw 'Overview must contain DAY rows.' }
    foreach ($row in $dayRows) {
        $dayTotal += ConvertTo-G2dInt64 $row.event_count 'overview.event_count'
    }
    if ($dayTotal -ne $fullEventCount) { throw 'DAY and FULL totals do not reconcile.' }

    $qualityDistinct = ConvertTo-G2dInt64 $fullQuality.distinct_event_count 'quality.distinct_event_count'
    $qualityDuplicateCount = ConvertTo-G2dInt64 $fullQuality.duplicate_event_count 'quality.duplicate_event_count'
    if ($qualityDistinct -ne $fullEventCount -or $qualityDuplicateCount -ne 0) {
        throw 'Duplicate event IDs detected.'
    }
    if ((ConvertTo-G2dInt64 $fullQuality.source_event_count 'quality.source_event_count') -ne $fullEventCount -or
            (ConvertTo-G2dInt64 $fullQuality.overview_event_count 'quality.overview_event_count') -ne $fullEventCount) {
        throw 'Quality totals do not reconcile.'
    }
    if ((ConvertTo-G2dInt64 $fullQuality.invalid_event_type_count 'quality.invalid_event_type_count') -ne 0) {
        throw 'Invalid event types detected.'
    }
    if ([string]$fullQuality.reconciliation_status -cne 'PASS') {
        throw "Quality reconciliation status must be 'PASS'."
    }

    return [pscustomobject][ordered]@{
        WindowStart = $windowStart
        WindowEnd = $windowEnd
        OverviewRowCount = $Overview.Count
        FunnelRowCount = $Funnel.Count
        DimensionRowCount = $Dimension.Count
        QualityRowCount = $Quality.Count
    }
}

function ConvertTo-G2dCanonicalToken {
    param(
        $Value,
        [Parameter(Mandatory = $true)][string]$Column
    )

    if ($null -eq $Value) { return 'null' }
    if ($Column -ceq 'is_unknown') {
        return (ConvertTo-G2dBooleanInt $Value $Column).ToString([Globalization.CultureInfo]::InvariantCulture)
    }
    if ($Column -ceq 'purchase_amount_proxy') {
        $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
        $parsed = 0D
        if (-not [decimal]::TryParse($text, [Globalization.NumberStyles]::Number, [Globalization.CultureInfo]::InvariantCulture, [ref]$parsed) -or $parsed -lt 0) {
            throw "$Column must be a nonnegative decimal."
        }
        return $parsed.ToString('0.00', [Globalization.CultureInfo]::InvariantCulture)
    }
    if ($Column -match '(^|_)count$' -or $Column -ceq 'source_snapshot_id') {
        return (ConvertTo-G2dInt64 $Value $Column).ToString([Globalization.CultureInfo]::InvariantCulture)
    }
    if ($Column -ceq 'window_start' -or $Column -ceq 'window_end' -or $Column -match '_date$') {
        $normalized = ConvertTo-G2dDateText $Value $Column
        return ConvertTo-Json -InputObject $normalized -Compress
    }
    if ($Column -match '_at$' -or $Column -match '_timestamp$') {
        $normalized = ConvertTo-G2dTimestampText $Value $Column
        return ConvertTo-Json -InputObject $normalized -Compress
    }
    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    return ConvertTo-Json -InputObject $text -Compress
}

function Get-G2dCanonicalDigest {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][object[]]$Rows,
        [Parameter(Mandatory = $true)][string[]]$Columns,
        [Parameter(Mandatory = $true)][string[]]$UniqueKeyColumns
    )

    if ($Columns.Count -eq 0 -or $UniqueKeyColumns.Count -eq 0) {
        throw 'Canonical digest columns and unique keys are required.'
    }
    foreach ($keyColumn in $UniqueKeyColumns) {
        if ($Columns -cnotcontains $keyColumn) { throw 'Canonical unique-key columns must be in the fixed column list.' }
    }

    $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $canonicalRows = [Collections.Generic.List[object]]::new()
    foreach ($row in $Rows) {
        $tokens = [ordered]@{}
        foreach ($column in $Columns) {
            $tokens[$column] = ConvertTo-G2dCanonicalToken (Get-G2dPropertyValue $row $column) $column
        }
        $keyTokens = foreach ($keyColumn in $UniqueKeyColumns) { $tokens[$keyColumn] }
        $sortKey = '[' + ($keyTokens -join ',') + ']'
        if (-not $seen.Add($sortKey)) { throw 'Duplicate normalized canonical key detected.' }
        $rowTokens = foreach ($column in $Columns) { $tokens[$column] }
        $canonicalRows.Add([pscustomobject]@{
            SortKey = $sortKey
            Json = '[' + ($rowTokens -join ',') + ']'
        })
    }

    $orderedRows = @($canonicalRows | Sort-Object -Property @{ Expression = 'SortKey'; Ascending = $true })
    $payload = (@($orderedRows | ForEach-Object { $_.Json }) -join "`n")
    $bytes = [Text.UTF8Encoding]::new($false).GetBytes($payload)
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        $hash = $sha.ComputeHash($bytes)
    } finally {
        $sha.Dispose()
    }
    return ([BitConverter]::ToString($hash) -replace '-', '').ToLowerInvariant()
}

function ConvertTo-G2dCsvCell {
    param($Value, [string]$Column)

    if ($null -eq $Value) { return '' }
    if ($Column -ceq 'is_unknown') {
        $text = (ConvertTo-G2dBooleanInt $Value $Column).ToString([Globalization.CultureInfo]::InvariantCulture)
    } else {
        $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    }
    if ($text.IndexOfAny([char[]]@(',', '"', "`r", "`n")) -ge 0) {
        return '"' + $text.Replace('"', '""') + '"'
    }
    return $text
}

function Export-G2dCandidateCsv {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Target,
        [Parameter(Mandatory = $true)][object[]]$Rows,
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)][string]$OutputDirectory,
        [Parameter(Mandatory = $true)][string]$OutputPath
    )

    if (@('overview', 'funnel', 'dimension', 'quality') -cnotcontains $Target) {
        throw 'G2-D candidate target is unknown.'
    }
    if ($Rows.Count -eq 0) { throw 'G2-D candidate export requires rows.' }
    if (-not [IO.Directory]::Exists($OutputDirectory)) { throw 'G2-D output directory does not exist.' }

    $root = [IO.Path]::GetFullPath($OutputDirectory).TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    ) + [IO.Path]::DirectorySeparatorChar
    $path = [IO.Path]::GetFullPath($OutputPath)
    if (-not $path.StartsWith($root, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'G2-D candidate path must remain under the output directory.'
    }

    $columns = [string[]]$script:G2dTargetColumns[$Target]
    $identityValues = [ordered]@{
        metric_run_id = [string]$Identity.MetricRunId
        dataset_id = [string]$Identity.DatasetId
        metric_version = [string]$Identity.MetricVersion
    }
    foreach ($value in $identityValues.Values) {
        if ([string]::IsNullOrEmpty([string]$value)) { throw 'G2-D metric identity is incomplete.' }
    }

    $lines = [Collections.Generic.List[string]]::new()
    $lines.Add(($columns -join ','))
    foreach ($row in $Rows) {
        $cells = foreach ($column in $columns) {
            if ($identityValues.Contains($column)) {
                ConvertTo-G2dCsvCell $identityValues[$column] $column
            } else {
                ConvertTo-G2dCsvCell (Get-G2dPropertyValue $row $column) $column
            }
        }
        $lines.Add(($cells -join ','))
    }
    [IO.File]::WriteAllText($path, (($lines.ToArray() -join "`n") + "`n"), [Text.UTF8Encoding]::new($false))
    return $path
}

Export-ModuleMember -Function @(
    'Get-G2dMetricIdentity',
    'Split-G2dNamedSql',
    'ConvertFrom-G2dCsv',
    'Assert-G2dMetricBundle',
    'Get-G2dCanonicalDigest',
    'Get-G2dPublicationOrder',
    'Export-G2dCandidateCsv'
)
