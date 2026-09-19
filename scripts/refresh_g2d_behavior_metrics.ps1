[CmdletBinding()]
param(
    [ValidateSet('g2c-correctness-subset', 'stable-user-2pct-full')]
    [string]$DataScope = 'g2c-correctness-subset',
    [switch]$PlanOnly,
    [switch]$FunctionsOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$script:G2dProjectRoot = Split-Path $PSScriptRoot -Parent
$script:G2dSqlTemplatePath = Join-Path $script:G2dProjectRoot 'jobs/sql/18_g2d_behavior_metrics.sql.template'
$script:G2dDdlPath = Join-Path $script:G2dProjectRoot 'infra/compose/doris/init/02_create_behavior_metrics.sql'
$script:G2dOutputRoot = Join-Path $script:G2dProjectRoot 'tmp/graduation/g2d'
$script:G2dTrinoContainer = 'ecom-trino'
$script:G2dDorisContainer = 'ecom-doris-fe'
$script:G2dDorisStreamLoadUrl = 'http://localhost:8040'
$script:G2dSetupHelp = 'Start the existing services with .\scripts\bootstrap_chapter_10_5.ps1 and see docs\chapter-10-5-engineering-hardening-runbook.md.'

Import-Module (Join-Path $PSScriptRoot 'lib/G2d.BehaviorMetrics.psm1') -Force

function Get-G2dTargetSpec {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet('overview', 'funnel', 'dimension', 'quality')]
        [string]$Name
    )

    $specs = [ordered]@{
        overview = [pscustomobject]@{
            Name = 'overview'
            Table = 'behavior_overview_metrics'
            Columns = @(
                'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
                'window_end', 'event_count', 'view_count', 'cart_count', 'purchase_count',
                'unique_user_count', 'session_count', 'product_count', 'purchase_amount_proxy'
            )
            UniqueKeyColumns = @('metric_run_id', 'window_type', 'window_start')
        }
        funnel = [pscustomobject]@{
            Name = 'funnel'
            Table = 'behavior_funnel_metrics'
            Columns = @(
                'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
                'window_end', 'missing_session_event_count', 'view_sessions',
                'view_to_cart_sessions', 'completed_sessions'
            )
            UniqueKeyColumns = @('metric_run_id', 'window_type', 'window_start')
        }
        dimension = [pscustomobject]@{
            Name = 'dimension'
            Table = 'behavior_dimension_metrics'
            Columns = @(
                'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
                'window_end', 'dimension_type', 'dimension_id', 'dimension_name', 'is_unknown',
                'view_count', 'cart_count', 'purchase_count', 'unique_user_count',
                'purchase_amount_proxy'
            )
            UniqueKeyColumns = @(
                'metric_run_id', 'window_type', 'window_start', 'dimension_type', 'dimension_id'
            )
        }
        quality = [pscustomobject]@{
            Name = 'quality'
            Table = 'behavior_quality_metrics'
            Columns = @(
                'metric_run_id', 'dataset_id', 'metric_version', 'window_type', 'window_start',
                'window_end', 'source_event_count', 'clean_event_count', 'late_event_count',
                'clean_event_rate', 'late_event_rate', 'distinct_event_count',
                'duplicate_event_count', 'missing_session_count', 'unknown_category_count',
                'unknown_brand_count', 'invalid_event_type_count', 'empty_key_id_count',
                'invalid_price_count', 'invalid_derived_date_count', 'overview_event_count',
                'reconciliation_status'
            )
            UniqueKeyColumns = @('metric_run_id', 'window_type', 'window_start')
        }
    }
    return $specs[$Name]
}

function Get-G2dRefreshPlan {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][string]$MetricRunId)

    if ($MetricRunId -notmatch '^behavior-v1-s([1-9][0-9]*)$') {
        throw 'G2-D metric run ID is unsafe.'
    }
    $snapshot = 0L
    if (-not [long]::TryParse(
            $Matches[1],
            [Globalization.NumberStyles]::None,
            [Globalization.CultureInfo]::InvariantCulture,
            [ref]$snapshot) -or $snapshot -le 0) {
        throw 'G2-D metric run ID contains an invalid snapshot ID.'
    }

    $order = @(Get-G2dPublicationOrder)
    $plan = @(
        [pscustomobject]@{ Name = 'overview'; Target = 'behavior_overview_metrics' },
        [pscustomobject]@{ Name = 'funnel'; Target = 'behavior_funnel_metrics' },
        [pscustomobject]@{ Name = 'dimension'; Target = 'behavior_dimension_metrics' },
        [pscustomobject]@{ Name = 'quality'; Target = 'behavior_quality_metrics' },
        [pscustomobject]@{ Name = 'publication'; Target = 'behavior_metric_publications' }
    )
    $null = Get-G2dPublicationOrder -Order @($plan | ForEach-Object { $_.Name })
    if (($order -join ',') -cne (($plan | ForEach-Object { $_.Name }) -join ',')) {
        throw 'G2-D refresh plan does not match the reviewed publication order.'
    }
    return $plan
}

function Assert-G2dTrinoResultSet {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][Collections.IDictionary]$Results)

    $expected = @('source_identity', 'overview', 'funnel', 'dimension', 'quality')
    $keys = @($Results.Keys)
    if ($keys.Count -ne $expected.Count) {
        throw 'G2-D Trino execution must contain exactly five results.'
    }
    for ($index = 0; $index -lt $expected.Count; $index++) {
        if ([string]$keys[$index] -cne $expected[$index]) {
            throw 'G2-D Trino result names are missing, extra, or out of order.'
        }
    }
}

function Assert-G2dStreamLoadResponse {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Response,
        [Parameter(Mandatory = $true)][long]$ExpectedRows,
        [Parameter(Mandatory = $true)][string]$TableName
    )

    if ([string]$Response.Status -cne 'Success' -or
            [long]$Response.NumberLoadedRows -ne $ExpectedRows -or
            [long]$Response.NumberFilteredRows -ne 0) {
        throw "G2-D Stream Load failed for $TableName."
    }
}

function Assert-G2dStoredCandidate {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet('overview', 'funnel', 'dimension', 'quality')]
        [string]$Name,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$CandidateRows,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$StoredRows
    )

    $spec = Get-G2dTargetSpec -Name $Name
    if ($StoredRows.Count -ne $CandidateRows.Count) {
        throw "G2-D stored row count mismatch for $($spec.Table)."
    }
    $candidateDigest = Get-G2dCanonicalDigest -Rows $CandidateRows `
        -Columns $spec.Columns -UniqueKeyColumns $spec.UniqueKeyColumns
    $storedDigest = Get-G2dCanonicalDigest -Rows $StoredRows `
        -Columns $spec.Columns -UniqueKeyColumns $spec.UniqueKeyColumns
    if ($storedDigest -cne $candidateDigest) {
        throw "G2-D stored digest mismatch for $($spec.Table)."
    }
    return [pscustomobject][ordered]@{
        RowCount = [long]$StoredRows.Count
        Sha256 = $storedDigest
    }
}

function Assert-G2dStoredIdentity {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$Rows,
        [Parameter(Mandatory = $true)]
        [ValidateSet('overview', 'funnel', 'dimension', 'quality')]
        [string]$Name
    )

    if ($Rows.Count -eq 0) { throw "G2-D stored $Name rows are empty." }
    foreach ($row in $Rows) {
        if ([string]$row.metric_run_id -cne [string]$Identity.MetricRunId -or
                [string]$row.dataset_id -cne [string]$Identity.DatasetId -or
                [string]$row.metric_version -cne [string]$Identity.MetricVersion) {
            throw "G2-D stored $Name row identity does not match the publication identity."
        }
    }
}

function New-G2dAttemptId {
    [CmdletBinding()]
    param()

    return [guid]::NewGuid().ToString('N')
}

function Get-G2dUnpublishedCandidateState {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][Collections.IDictionary]$Counts,
        [Parameter(Mandatory = $true)][string]$AttemptId
    )

    if ($AttemptId -notmatch '^[0-9a-f]{32}$') { throw 'G2-D attempt ID is unsafe.' }
    $normalized = [ordered]@{}
    foreach ($name in @('overview', 'funnel', 'dimension', 'quality')) {
        if (-not $Counts.Contains($name)) { throw 'G2-D candidate count set is incomplete.' }
        $value = 0L
        $text = [Convert]::ToString($Counts[$name], [Globalization.CultureInfo]::InvariantCulture)
        if ($text -notmatch '^(0|[1-9][0-9]*)$' -or -not [long]::TryParse($text, [ref]$value)) {
            throw 'G2-D candidate counts must be nonnegative integers.'
        }
        $normalized[$name] = $value
    }
    if ($Counts.Count -ne 4) { throw 'G2-D candidate count set contains an unknown table.' }
    return [pscustomobject][ordered]@{
        status = 'continue'
        attempt_id = $AttemptId
        counts = [pscustomobject]$normalized
    }
}

function Assert-G2dTimestamp {
    param([Parameter(Mandatory = $true)]$Value, [Parameter(Mandatory = $true)][string]$Name)

    $parsed = [datetimeoffset]::MinValue
    if (-not [datetimeoffset]::TryParse(
            [string]$Value,
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::AssumeUniversal,
            [ref]$parsed)) {
        throw "$Name must be a valid timestamp."
    }
    return $parsed.ToUniversalTime()
}

function Assert-G2dExistingPublication {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)]$SourceIdentity,
        [Parameter(Mandatory = $true)]$Publication,
        [Parameter(Mandatory = $true)][Collections.IDictionary]$StoredEvidence
    )

    $expected = [ordered]@{
        metric_run_id = [string]$Identity.MetricRunId
        dataset_id = [string]$Identity.DatasetId
        metric_version = [string]$Identity.MetricVersion
        data_scope = [string]$Identity.DataScope
        source_snapshot_id = [string]$Identity.SourceSnapshotId
        source_event_count = [string]$Identity.SourceEventCount
        window_start = [string]$SourceIdentity.window_start
        window_end = [string]$SourceIdentity.window_end
        status = 'PUBLISHED'
    }
    foreach ($field in $expected.Keys) {
        $property = $Publication.PSObject.Properties[$field]
        if ($null -eq $property -or [string]$property.Value -cne [string]$expected[$field]) {
            throw "G2-D existing publication field '$field' does not match."
        }
    }

    $calculated = Assert-G2dTimestamp -Value $Publication.calculated_at -Name 'calculated_at'
    $published = Assert-G2dTimestamp -Value $Publication.published_at -Name 'published_at'
    if ($published -lt $calculated) { throw 'G2-D publication timestamp precedes calculation.' }

    foreach ($name in @('overview', 'funnel', 'dimension', 'quality')) {
        if (-not $StoredEvidence.Contains($name)) {
            throw 'G2-D stored publication evidence is incomplete.'
        }
        $evidence = $StoredEvidence[$name]
        $countField = "${name}_row_count"
        $hashField = "${name}_sha256"
        if ([long]$evidence.RowCount -le 0 -or
                [string]$Publication.$countField -cne [string]$evidence.RowCount -or
                [string]$Publication.$hashField -cne [string]$evidence.Sha256 -or
                [string]$evidence.Sha256 -notmatch '^[0-9a-f]{64}$') {
            throw "G2-D existing publication evidence does not match $name rows."
        }
    }
    if ($StoredEvidence.Count -ne 4) { throw 'G2-D stored publication evidence contains an unknown table.' }

    return [pscustomobject][ordered]@{
        status = 'already_published'
        metric_run_id = [string]$Identity.MetricRunId
    }
}

function Invoke-G2dPublicationSequence {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][scriptblock]$VerifyAction,
        [Parameter(Mandatory = $true)][scriptblock]$PublishAction
    )

    $order = @(Get-G2dPublicationOrder)
    $evidence = [ordered]@{}
    foreach ($name in $order) {
        if ($name -ceq 'publication') { break }
        $evidence[$name] = & $VerifyAction $name
    }
    if ($evidence.Count -ne 4) { throw 'G2-D publication attempted before all metric checks passed.' }
    return & $PublishAction $evidence
}

function Enable-G2dDockerCli {
    [CmdletBinding()]
    param()

    $dockerFallback = 'D:\DockerProgram\Docker\resources\bin'
    if (-not (Get-Command docker -ErrorAction SilentlyContinue) -and
            (Test-Path (Join-Path $dockerFallback 'docker.exe'))) {
        $env:PATH = "$dockerFallback;$env:PATH"
    }
    $command = Get-Command docker -ErrorAction SilentlyContinue
    if ($null -eq $command) { throw "Docker CLI is unavailable. $script:G2dSetupHelp" }
    return $command.Source
}

function Get-G2dTrinoEndpoints {
    [CmdletBinding()]
    param()

    return [pscustomobject][ordered]@{
        HostHealthBaseUrl = 'http://localhost:8088'
        ContainerCliBaseUrl = 'http://localhost:8080'
    }
}

function Invoke-G2dDorisSql {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [switch]$NoHeaders
    )

    $arguments = @(
        'exec', '-i', $script:G2dDorisContainer, 'mysql', '-h127.0.0.1', '-P9030',
        '-uroot', '--batch'
    )
    if ($NoHeaders) { $arguments += '--skip-column-names' }
    $output = @($Sql | & docker @arguments 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "Doris SQL failed. $script:G2dSetupHelp Output: $($output -join ' ')"
    }
    return @($output | ForEach-Object { [string]$_ })
}

function ConvertFrom-G2dMysqlCell {
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

function ConvertFrom-G2dMysqlBatch {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][AllowEmptyCollection()][string[]]$Lines)

    if ($Lines.Count -eq 0) { return @() }
    $headers = @($Lines[0].Split([char[]]@("`t"), [StringSplitOptions]::None))
    $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($header in $headers) {
        if ([string]::IsNullOrWhiteSpace($header) -or -not $seen.Add($header)) {
            throw 'Doris result headers must be nonempty and unique.'
        }
    }
    $rows = [Collections.Generic.List[object]]::new()
    for ($rowIndex = 1; $rowIndex -lt $Lines.Count; $rowIndex++) {
        $cells = @($Lines[$rowIndex].Split([char[]]@("`t"), [StringSplitOptions]::None))
        if ($cells.Count -ne $headers.Count) { throw 'Doris result has an inconsistent column count.' }
        $row = [ordered]@{}
        for ($columnIndex = 0; $columnIndex -lt $headers.Count; $columnIndex++) {
            $row[$headers[$columnIndex]] = ConvertFrom-G2dMysqlCell -Text $cells[$columnIndex]
        }
        $rows.Add([pscustomobject]$row)
    }
    return @($rows.ToArray())
}

function Invoke-G2dDorisQuery {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][string]$Sql)

    $lines = @(Invoke-G2dDorisSql -Sql $Sql)
    return @(ConvertFrom-G2dMysqlBatch -Lines $lines)
}

function Invoke-G2dTrinoStatement {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Sql
    )

    if (@('preflight_table', 'latest_snapshot', 'source_identity', 'overview', 'funnel', 'dimension', 'quality') -cnotcontains $Name) {
        throw 'G2-D Trino statement name is unknown.'
    }
    $trinoEndpoints = Get-G2dTrinoEndpoints
    $output = @(& docker exec -e TERM=dumb $script:G2dTrinoContainer trino `
        --server $trinoEndpoints.ContainerCliBaseUrl --catalog lakehouse --schema analytics `
        --output-format CSV_HEADER_UNQUOTED --execute $Sql 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "Trino statement '$Name' failed. $script:G2dSetupHelp Output: $($output -join ' ')"
    }
    return @(ConvertFrom-G2dCsv -CsvText ($output -join "`n"))
}

function Assert-G2dDependencies {
    [CmdletBinding()]
    param()

    $null = Enable-G2dDockerCli
    $dockerOutput = @(& docker version --format '{{.Server.Version}}' 2>&1)
    if ($LASTEXITCODE -ne 0 -or $dockerOutput.Count -ne 1 -or [string]::IsNullOrWhiteSpace([string]$dockerOutput[0])) {
        throw "Docker Engine is unavailable. $script:G2dSetupHelp"
    }
    try {
        $trinoEndpoints = Get-G2dTrinoEndpoints
        $null = Invoke-RestMethod -Method Get `
            -Uri "$($trinoEndpoints.HostHealthBaseUrl)/v1/info" -TimeoutSec 10
    } catch {
        throw "Trino /v1/info is unavailable. $script:G2dSetupHelp"
    }
    $probe = @(Invoke-G2dDorisSql -Sql 'SELECT 1 AS ready;' -NoHeaders)
    if ($probe.Count -ne 1 -or [string]$probe[0] -cne '1') {
        throw "Doris SELECT 1 failed. $script:G2dSetupHelp"
    }
    if ($null -eq (Get-Command curl.exe -ErrorAction SilentlyContinue)) {
        throw 'curl.exe is required for byte-preserving Doris Stream Load.'
    }
}

function Get-G2dLatestSnapshotSql {
    [CmdletBinding()]
    param()

    return @'
SELECT snapshot_id AS source_snapshot_id
FROM lakehouse.analytics."real_behavior_detail_v1$snapshots"
WHERE snapshot_id > 0
ORDER BY committed_at DESC, snapshot_id DESC
LIMIT 1;
'@
}

function Get-G2dLatestSnapshotId {
    [CmdletBinding()]
    param()

    $tableRows = @(Invoke-G2dTrinoStatement -Name preflight_table -Sql @'
SELECT table_name
FROM lakehouse.information_schema.tables
WHERE table_schema = 'analytics' AND table_name = 'real_behavior_detail_v1';
'@)
    if ($tableRows.Count -ne 1 -or [string]$tableRows[0].table_name -cne 'real_behavior_detail_v1') {
        throw "The fixed G2-C source table is unavailable. $script:G2dSetupHelp"
    }
    $snapshotRows = @(
        Invoke-G2dTrinoStatement -Name latest_snapshot -Sql (Get-G2dLatestSnapshotSql)
    )
    if ($snapshotRows.Count -ne 1 -or [string]$snapshotRows[0].source_snapshot_id -notmatch '^[1-9][0-9]*$') {
        throw 'The fixed G2-C source table has no positive Snapshot ID.'
    }
    $snapshot = 0L
    if (-not [long]::TryParse([string]$snapshotRows[0].source_snapshot_id, [ref]$snapshot) -or $snapshot -le 0) {
        throw 'The latest G2-C Snapshot ID is outside the supported Int64 range.'
    }
    return $snapshot
}

function Initialize-G2dDorisSchema {
    [CmdletBinding()]
    param()

    $ddl = [IO.File]::ReadAllText($script:G2dDdlPath, [Text.Encoding]::UTF8)
    $null = Invoke-G2dDorisSql -Sql $ddl -NoHeaders
}

function Assert-G2dDorisTables {
    [CmdletBinding()]
    param()

    $rows = @(Invoke-G2dDorisQuery -Sql @'
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'analytics'
  AND table_name IN (
    'behavior_metric_publications', 'behavior_overview_metrics',
    'behavior_funnel_metrics', 'behavior_dimension_metrics', 'behavior_quality_metrics'
  )
ORDER BY table_name;
'@)
    $expected = @(
        'behavior_dimension_metrics', 'behavior_funnel_metrics', 'behavior_metric_publications',
        'behavior_overview_metrics', 'behavior_quality_metrics'
    )
    $actual = @($rows | ForEach-Object { [string]$_.table_name })
    if ($actual.Count -ne $expected.Count -or ($actual -join ',') -cne ($expected -join ',')) {
        throw 'G2-D Doris initialization did not produce all five fixed tables.'
    }
}

function ConvertTo-G2dSqlString {
    param([Parameter(Mandatory = $true)][AllowEmptyString()][string]$Value)

    if ($Value.IndexOf([char]0) -ge 0) { throw 'G2-D SQL value contains a NUL character.' }
    return "'" + $Value.Replace("'", "''") + "'"
}

function Read-G2dStoredMetricRows {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet('overview', 'funnel', 'dimension', 'quality')]
        [string]$Name,
        [Parameter(Mandatory = $true)][string]$MetricRunId
    )

    $null = Get-G2dRefreshPlan -MetricRunId $MetricRunId
    $spec = Get-G2dTargetSpec -Name $Name
    $columns = $spec.Columns -join ', '
    $order = $spec.UniqueKeyColumns -join ', '
    $runLiteral = ConvertTo-G2dSqlString -Value $MetricRunId
    $sql = "SELECT $columns FROM analytics.$($spec.Table) " +
        "WHERE metric_run_id = $runLiteral ORDER BY $order;"
    return @(Invoke-G2dDorisQuery -Sql $sql)
}

function Read-G2dPublicationRows {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][string]$MetricRunId)

    $null = Get-G2dRefreshPlan -MetricRunId $MetricRunId
    $runLiteral = ConvertTo-G2dSqlString -Value $MetricRunId
    $columns = @(
        'metric_run_id', 'dataset_id', 'metric_version', 'data_scope', 'source_snapshot_id',
        'source_event_count', 'window_start', 'window_end', 'calculated_at', 'published_at',
        'overview_row_count', 'overview_sha256', 'funnel_row_count', 'funnel_sha256',
        'dimension_row_count', 'dimension_sha256', 'quality_row_count', 'quality_sha256', 'status'
    ) -join ', '
    return @(Invoke-G2dDorisQuery -Sql (
        "SELECT $columns FROM analytics.behavior_metric_publications " +
        "WHERE metric_run_id = $runLiteral ORDER BY metric_run_id;"
    ))
}

function Get-G2dStoredEvidence {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Identity
    )

    $evidence = [ordered]@{}
    foreach ($name in @('overview', 'funnel', 'dimension', 'quality')) {
        $spec = Get-G2dTargetSpec -Name $name
        $rows = @(Read-G2dStoredMetricRows -Name $name -MetricRunId $Identity.MetricRunId)
        Assert-G2dStoredIdentity -Identity $Identity -Rows $rows -Name $name
        $evidence[$name] = [pscustomobject][ordered]@{
            RowCount = [long]$rows.Count
            Sha256 = Get-G2dCanonicalDigest -Rows $rows -Columns $spec.Columns `
                -UniqueKeyColumns $spec.UniqueKeyColumns
        }
    }
    return $evidence
}

function Get-G2dExistingPublicationResult {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)]$SourceIdentity
    )

    $rows = @(Read-G2dPublicationRows -MetricRunId $Identity.MetricRunId)
    if ($rows.Count -eq 0) { return $null }
    if ($rows.Count -ne 1) { throw 'G2-D metric run has multiple publication rows.' }
    $evidence = Get-G2dStoredEvidence -Identity $Identity
    return Assert-G2dExistingPublication -Identity $Identity -SourceIdentity $SourceIdentity `
        -Publication $rows[0] -StoredEvidence $evidence
}

function Get-G2dCandidateCounts {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][string]$MetricRunId)

    $null = Get-G2dRefreshPlan -MetricRunId $MetricRunId
    $runLiteral = ConvertTo-G2dSqlString -Value $MetricRunId
    $counts = [ordered]@{}
    foreach ($name in @('overview', 'funnel', 'dimension', 'quality')) {
        $spec = Get-G2dTargetSpec -Name $name
        $rows = @(Invoke-G2dDorisQuery -Sql (
            "SELECT count(*) AS row_count FROM analytics.$($spec.Table) " +
            "WHERE metric_run_id = $runLiteral;"
        ))
        if ($rows.Count -ne 1 -or [string]$rows[0].row_count -notmatch '^(0|[1-9][0-9]*)$') {
            throw "G2-D could not read candidate count for $($spec.Table)."
        }
        $counts[$name] = [long]$rows[0].row_count
    }
    return $counts
}

function Get-G2dPathComparison {
    if ([IO.Path]::DirectorySeparatorChar -eq [char]92) {
        return [StringComparison]::OrdinalIgnoreCase
    }
    return [StringComparison]::Ordinal
}

function Test-G2dPathContained {
    param(
        [Parameter(Mandatory = $true)][string]$RootPath,
        [Parameter(Mandatory = $true)][string]$CandidatePath
    )

    $root = [IO.Path]::GetFullPath($RootPath).TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    )
    $candidate = [IO.Path]::GetFullPath($CandidatePath)
    $comparison = Get-G2dPathComparison
    if ($candidate.Equals($root, $comparison)) { return $true }
    return $candidate.StartsWith(
        ($root + [IO.Path]::DirectorySeparatorChar),
        $comparison
    )
}

function Resolve-G2dPhysicalOrProjectedPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    $fullPath = [IO.Path]::GetFullPath($Path)
    $pathRoot = [IO.Path]::GetPathRoot($fullPath)
    $current = $pathRoot
    $relativePath = $fullPath.Substring($pathRoot.Length)
    $separators = [char[]]@(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    )
    $segments = $relativePath.Split($separators, [StringSplitOptions]::RemoveEmptyEntries)
    foreach ($segment in $segments) {
        $candidate = Join-Path $current $segment
        if (-not [IO.File]::Exists($candidate) -and -not [IO.Directory]::Exists($candidate)) {
            $current = $candidate
            continue
        }

        $item = Get-Item -LiteralPath $candidate -Force -ErrorAction Stop
        $isReparsePoint = ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0
        if (-not $isReparsePoint) {
            $current = $candidate
            continue
        }

        $targetItem = $null
        if ($null -ne $item.PSObject.Methods['ResolveLinkTarget']) {
            $targetItem = $item.ResolveLinkTarget($true)
        }
        if ($null -eq $targetItem) {
            $targetProperty = $item.PSObject.Properties['Target']
            $targets = if ($null -eq $targetProperty) { @() } else { @($targetProperty.Value) }
            if ($targets.Count -ne 1 -or [string]::IsNullOrWhiteSpace([string]$targets[0])) {
                throw 'G2-D cannot resolve an output path reparse point.'
            }
            $targetPath = [string]$targets[0]
            if (-not [IO.Path]::IsPathRooted($targetPath)) {
                $targetPath = Join-Path (Split-Path $candidate -Parent) $targetPath
            }
            $targetItem = Get-Item -LiteralPath ([IO.Path]::GetFullPath($targetPath)) `
                -Force -ErrorAction Stop
        }
        $current = $targetItem.FullName
    }
    return [IO.Path]::GetFullPath($current)
}

function Assert-G2dPhysicalContainment {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$RootPath,
        [Parameter(Mandatory = $true)][string]$CandidatePath,
        [Parameter(Mandatory = $true)][string]$Description
    )

    $physicalRoot = Resolve-G2dPhysicalOrProjectedPath -Path $RootPath
    $physicalCandidate = Resolve-G2dPhysicalOrProjectedPath -Path $CandidatePath
    if (-not (Test-G2dPathContained -RootPath $physicalRoot -CandidatePath $physicalCandidate)) {
        throw "G2-D $Description escapes its fixed physical root."
    }
    return $physicalCandidate
}

function Assert-G2dFixedOutputPath {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$MetricRunId,
        [switch]$RequireRunDirectory
    )

    $null = Get-G2dRefreshPlan -MetricRunId $MetricRunId
    $outputRoot = [IO.Path]::GetFullPath($script:G2dOutputRoot)
    $runDirectory = [IO.Path]::GetFullPath((Join-Path $outputRoot $MetricRunId))
    $physicalOutputRoot = Assert-G2dPhysicalContainment `
        -RootPath $script:G2dProjectRoot -CandidatePath $outputRoot `
        -Description 'output root'
    $physicalRunDirectory = Assert-G2dPhysicalContainment `
        -RootPath $physicalOutputRoot -CandidatePath $runDirectory `
        -Description 'run directory'

    if ([IO.File]::Exists($outputRoot)) { throw 'G2-D output root is not a directory.' }
    if ([IO.File]::Exists($runDirectory)) { throw 'G2-D run path is not a directory.' }
    if ($RequireRunDirectory -and -not [IO.Directory]::Exists($runDirectory)) {
        throw 'G2-D fixed run directory does not exist.'
    }
    return [pscustomobject][ordered]@{
        OutputRoot = $outputRoot
        RunDirectory = $runDirectory
        PhysicalOutputRoot = $physicalOutputRoot
        PhysicalRunDirectory = $physicalRunDirectory
    }
}

function Initialize-G2dRunDirectory {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][string]$MetricRunId)

    $paths = Assert-G2dFixedOutputPath -MetricRunId $MetricRunId
    if (-not [IO.Directory]::Exists($paths.OutputRoot)) {
        $null = New-Item -ItemType Directory -Path $paths.OutputRoot
    }
    $paths = Assert-G2dFixedOutputPath -MetricRunId $MetricRunId
    if (-not [IO.Directory]::Exists($paths.RunDirectory)) {
        $null = New-Item -ItemType Directory -Path $paths.RunDirectory
    }
    return Assert-G2dFixedOutputPath -MetricRunId $MetricRunId -RequireRunDirectory
}

function Invoke-G2dStreamLoad {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet('overview', 'funnel', 'dimension', 'quality')]
        [string]$Name,
        [Parameter(Mandatory = $true)][string]$MetricRunId,
        [Parameter(Mandatory = $true)][string]$AttemptId,
        [Parameter(Mandatory = $true)][string]$CandidatePath
    )

    $null = Get-G2dRefreshPlan -MetricRunId $MetricRunId
    if ($AttemptId -notmatch '^[0-9a-f]{32}$') { throw 'G2-D attempt ID is unsafe.' }
    $fullPath = [IO.Path]::GetFullPath($CandidatePath)
    $paths = Assert-G2dFixedOutputPath -MetricRunId $MetricRunId -RequireRunDirectory
    $null = Assert-G2dPhysicalContainment -RootPath $paths.PhysicalRunDirectory `
        -CandidatePath $fullPath -Description 'Stream Load candidate'
    if (-not [IO.File]::Exists($fullPath)) {
        throw 'G2-D Stream Load candidate must be an existing file in the fixed run directory.'
    }

    $spec = Get-G2dTargetSpec -Name $Name
    $label = "$MetricRunId-$($spec.Table)-$AttemptId"
    if ($label -notmatch '^[a-z0-9_-]+$') { throw 'G2-D Stream Load label is unsafe.' }
    $arguments = @(
        '--silent', '--show-error', '--location-trusted', '--user', 'root:', '--request', 'PUT',
        '--header', 'Expect:100-continue', '--header', "label:$label", '--header', 'format:csv',
        '--header', 'column_separator:,',
        '--header', 'skip_lines:1', '--header', 'strict_mode:true',
        '--header', ('columns:' + ($spec.Columns -join ',')), '--upload-file', $fullPath,
        "$script:G2dDorisStreamLoadUrl/api/analytics/$($spec.Table)/_stream_load"
    )
    $output = @(& curl.exe @arguments 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "G2-D Stream Load request failed for $($spec.Table)." }
    try {
        return (($output -join "`n") | ConvertFrom-Json)
    } catch {
        throw "G2-D Stream Load returned invalid JSON for $($spec.Table)."
    }
}

function New-G2dPublicationRecord {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)]$SourceIdentity,
        [Parameter(Mandatory = $true)][Collections.IDictionary]$Evidence,
        [Parameter(Mandatory = $true)][string]$CalculatedAt,
        [Parameter(Mandatory = $true)][string]$PublishedAt
    )

    $null = Assert-G2dTimestamp -Value $CalculatedAt -Name 'calculated_at'
    $null = Assert-G2dTimestamp -Value $PublishedAt -Name 'published_at'
    $record = [ordered]@{
        metric_run_id = [string]$Identity.MetricRunId
        dataset_id = [string]$Identity.DatasetId
        metric_version = [string]$Identity.MetricVersion
        data_scope = [string]$Identity.DataScope
        source_snapshot_id = [string]$Identity.SourceSnapshotId
        source_event_count = [string]$Identity.SourceEventCount
        window_start = [string]$SourceIdentity.window_start
        window_end = [string]$SourceIdentity.window_end
        calculated_at = $CalculatedAt
        published_at = $PublishedAt
    }
    foreach ($name in @('overview', 'funnel', 'dimension', 'quality')) {
        if (-not $Evidence.Contains($name)) { throw 'G2-D publication evidence is incomplete.' }
        $record["${name}_row_count"] = [string]$Evidence[$name].RowCount
        $record["${name}_sha256"] = [string]$Evidence[$name].Sha256
    }
    $record.status = 'PUBLISHED'
    return [pscustomobject]$record
}

function ConvertTo-G2dDorisTimestampLiteral {
    param([Parameter(Mandatory = $true)][string]$Value)

    $parsed = Assert-G2dTimestamp -Value $Value -Name 'publication timestamp'
    return ConvertTo-G2dSqlString -Value $parsed.ToString(
        'yyyy-MM-dd HH:mm:ss.fff',
        [Globalization.CultureInfo]::InvariantCulture
    )
}

function New-G2dPublicationInsertSql {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)]$Publication)

    $textFields = @(
        'metric_run_id', 'dataset_id', 'metric_version', 'data_scope', 'window_start', 'window_end',
        'overview_sha256', 'funnel_sha256', 'dimension_sha256', 'quality_sha256', 'status'
    )
    foreach ($field in @('overview_sha256', 'funnel_sha256', 'dimension_sha256', 'quality_sha256')) {
        if ([string]$Publication.$field -notmatch '^[0-9a-f]{64}$') {
            throw "G2-D publication field '$field' is invalid."
        }
    }
    if ([string]$Publication.status -cne 'PUBLISHED') { throw 'G2-D publication status must be PUBLISHED.' }
    $values = [ordered]@{}
    foreach ($field in $textFields) {
        $values[$field] = ConvertTo-G2dSqlString -Value ([string]$Publication.$field)
    }
    foreach ($field in @(
            'source_snapshot_id', 'source_event_count', 'overview_row_count', 'funnel_row_count',
            'dimension_row_count', 'quality_row_count')) {
        $text = [string]$Publication.$field
        if ($text -notmatch '^(0|[1-9][0-9]*)$') { throw "G2-D publication field '$field' is invalid." }
        $values[$field] = $text
    }
    $values.calculated_at = ConvertTo-G2dDorisTimestampLiteral -Value ([string]$Publication.calculated_at)
    $values.published_at = ConvertTo-G2dDorisTimestampLiteral -Value ([string]$Publication.published_at)
    $columns = @(
        'metric_run_id', 'dataset_id', 'metric_version', 'data_scope', 'source_snapshot_id',
        'source_event_count', 'window_start', 'window_end', 'calculated_at', 'published_at',
        'overview_row_count', 'overview_sha256', 'funnel_row_count', 'funnel_sha256',
        'dimension_row_count', 'dimension_sha256', 'quality_row_count', 'quality_sha256', 'status'
    )
    $sqlValues = @($columns | ForEach-Object { $values[$_] }) -join ', '
    return "INSERT INTO analytics.behavior_metric_publications ($($columns -join ', ')) VALUES ($sqlValues);"
}

function Assert-G2dPublicationReadback {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Expected,
        [Parameter(Mandatory = $true)]$Actual
    )

    $fields = @(
        'metric_run_id', 'dataset_id', 'metric_version', 'data_scope', 'source_snapshot_id',
        'source_event_count', 'window_start', 'window_end', 'overview_row_count',
        'overview_sha256', 'funnel_row_count', 'funnel_sha256', 'dimension_row_count',
        'dimension_sha256', 'quality_row_count', 'quality_sha256', 'status'
    )
    foreach ($field in $fields) {
        if ([string]$Actual.$field -cne [string]$Expected.$field) {
            throw "G2-D publication readback mismatch for '$field'."
        }
    }
    foreach ($field in @('calculated_at', 'published_at')) {
        $expectedTime = Assert-G2dTimestamp -Value $Expected.$field -Name $field
        $actualTime = Assert-G2dTimestamp -Value $Actual.$field -Name $field
        if ($actualTime -ne $expectedTime) { throw "G2-D publication readback mismatch for '$field'." }
    }
    return $Actual
}

function Publish-G2dMetadata {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)]$Publication)

    $before = @(Read-G2dPublicationRows -MetricRunId $Publication.metric_run_id)
    if ($before.Count -ne 0) { throw 'G2-D publication appeared before final metadata insertion.' }
    $sql = New-G2dPublicationInsertSql -Publication $Publication
    $null = Invoke-G2dDorisSql -Sql $sql -NoHeaders
    $after = @(Read-G2dPublicationRows -MetricRunId $Publication.metric_run_id)
    if ($after.Count -ne 1) { throw 'G2-D publication readback did not return exactly one row.' }
    return Assert-G2dPublicationReadback -Expected $Publication -Actual $after[0]
}

function Assert-G2dReportFilePath {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$RunDirectory,
        [Parameter(Mandatory = $true)][string]$ReportPath
    )

    $runPath = [IO.Path]::GetFullPath($RunDirectory)
    $reportFile = [IO.Path]::GetFullPath($ReportPath)
    $expectedFile = [IO.Path]::GetFullPath((Join-Path $runPath 'refresh-report.json'))
    $comparison = Get-G2dPathComparison
    if (-not $reportFile.Equals($expectedFile, $comparison)) {
        throw 'G2-D report path is not the fixed refresh-report.json path.'
    }
    return Assert-G2dPhysicalContainment -RootPath $runPath -CandidatePath $reportFile `
        -Description 'report file'
}

function Write-G2dRefreshReport {
    param(
        [Parameter(Mandatory = $true)][string]$OutputDirectory,
        [Parameter(Mandatory = $true)]$Report
    )

    $metricRunId = [string]$Report.metric_run_id
    $json = $Report | ConvertTo-Json -Depth 12
    $paths = Assert-G2dFixedOutputPath -MetricRunId $metricRunId -RequireRunDirectory
    $comparison = Get-G2dPathComparison
    if (-not [IO.Path]::GetFullPath($OutputDirectory).Equals($paths.RunDirectory, $comparison)) {
        throw 'G2-D report output directory is not the fixed run directory.'
    }
    $path = Join-Path $paths.RunDirectory 'refresh-report.json'
    $null = Assert-G2dReportFilePath -RunDirectory $paths.RunDirectory -ReportPath $path
    [IO.File]::WriteAllText($path, ($json + "`n"), [Text.UTF8Encoding]::new($false))
    return $path
}

function Invoke-G2dRefresh {
    [CmdletBinding()]
    param(
        [ValidateSet('g2c-correctness-subset', 'stable-user-2pct-full')]
        [string]$DataScope = 'g2c-correctness-subset',
        [switch]$PlanOnly
    )

    if ($PlanOnly) {
        return [pscustomobject][ordered]@{
            status = 'planned'
            data_scope = $DataScope
            plan = @(Get-G2dRefreshPlan -MetricRunId 'behavior-v1-s1')
        }
    }

    Assert-G2dDependencies
    $snapshotId = Get-G2dLatestSnapshotId
    $template = [IO.File]::ReadAllText($script:G2dSqlTemplatePath, [Text.Encoding]::UTF8)
    $statements = Split-G2dNamedSql -Sql $template -SnapshotId $snapshotId
    Assert-G2dTrinoResultSet -Results $statements

    $sourceRows = @(Invoke-G2dTrinoStatement -Name source_identity -Sql $statements.source_identity)
    if ($sourceRows.Count -ne 1 -or [string]$sourceRows[0].source_snapshot_id -cne [string]$snapshotId) {
        throw 'G2-D source identity did not echo the selected Snapshot ID.'
    }
    $sourceIdentity = $sourceRows[0]
    $identity = Get-G2dMetricIdentity -SnapshotId $snapshotId -DataScope $DataScope `
        -SourceEventCount $sourceIdentity.source_event_count
    $null = Get-G2dRefreshPlan -MetricRunId $identity.MetricRunId
    $attemptId = New-G2dAttemptId
    $outputPaths = Initialize-G2dRunDirectory -MetricRunId $identity.MetricRunId
    $outputDirectory = $outputPaths.RunDirectory
    $sqlSha256 = (Get-FileHash -LiteralPath $script:G2dSqlTemplatePath -Algorithm SHA256).Hash.ToLowerInvariant()

    $report = [ordered]@{
        status = 'running'
        metric_run_id = $identity.MetricRunId
        attempt_id = $attemptId
        data_scope = $identity.DataScope
        source_snapshot_id = $identity.SourceSnapshotId
        source_event_count = $identity.SourceEventCount
        sql_template_sha256 = $sqlSha256
        candidate_counts_before = $null
        candidates = [ordered]@{}
        publication = $null
    }

    try {
        Initialize-G2dDorisSchema
        Assert-G2dDorisTables
        $existing = Get-G2dExistingPublicationResult -Identity $identity -SourceIdentity $sourceIdentity
        if ($null -ne $existing) {
            $report.status = 'already_published'
            $report.publication = $existing
            $reportPath = Write-G2dRefreshReport -OutputDirectory $outputDirectory -Report $report
            return [pscustomobject][ordered]@{
                status = 'already_published'
                metric_run_id = $identity.MetricRunId
                attempt_id = $attemptId
                report_path = $reportPath
            }
        }

        $counts = Get-G2dCandidateCounts -MetricRunId $identity.MetricRunId
        $candidateState = Get-G2dUnpublishedCandidateState -Counts $counts -AttemptId $attemptId
        $report.candidate_counts_before = $candidateState.counts

        $results = [ordered]@{ source_identity = $sourceRows }
        foreach ($name in @('overview', 'funnel', 'dimension', 'quality')) {
            $results[$name] = @(Invoke-G2dTrinoStatement -Name $name -Sql $statements[$name])
        }
        Assert-G2dTrinoResultSet -Results $results
        $bundle = Assert-G2dMetricBundle -Identity $identity -SourceIdentity $results.source_identity `
            -Overview $results.overview -Funnel $results.funnel `
            -Dimension $results.dimension -Quality $results.quality

        $calculatedAt = [datetimeoffset]::UtcNow.ToString(
            "yyyy-MM-dd'T'HH:mm:ss.fff'Z'",
            [Globalization.CultureInfo]::InvariantCulture
        )
        $candidates = [ordered]@{}
        foreach ($name in @('overview', 'funnel', 'dimension', 'quality')) {
            $outputPaths = Assert-G2dFixedOutputPath `
                -MetricRunId $identity.MetricRunId -RequireRunDirectory
            $outputDirectory = $outputPaths.RunDirectory
            $path = Join-Path $outputDirectory "$name.csv"
            $null = Export-G2dCandidateCsv -Target $name -Rows $results[$name] -Identity $identity `
                -OutputDirectory $outputDirectory -OutputPath $path
            $candidateRows = @(ConvertFrom-G2dCsv -CsvText (
                [IO.File]::ReadAllText($path, [Text.Encoding]::UTF8)
            ))
            $spec = Get-G2dTargetSpec -Name $name
            $canonicalSha256 = Get-G2dCanonicalDigest -Rows $candidateRows `
                -Columns $spec.Columns -UniqueKeyColumns $spec.UniqueKeyColumns
            $fileSha256 = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
            $candidates[$name] = [pscustomobject][ordered]@{
                Path = $path
                Rows = $candidateRows
                RowCount = [long]$candidateRows.Count
                CanonicalSha256 = $canonicalSha256
                FileSha256 = $fileSha256
            }
            $report.candidates[$name] = [ordered]@{
                path = $path
                row_count = [long]$candidateRows.Count
                canonical_sha256 = $canonicalSha256
                file_sha256 = $fileSha256
            }
        }
        $report.bundle = $bundle
        $null = Write-G2dRefreshReport -OutputDirectory $outputDirectory -Report $report

        $verifyAction = {
            param($Name)
            $candidate = $candidates[$Name]
            $spec = Get-G2dTargetSpec -Name $Name
            $response = Invoke-G2dStreamLoad -Name $Name -MetricRunId $identity.MetricRunId `
                -AttemptId $attemptId -CandidatePath $candidate.Path
            Assert-G2dStreamLoadResponse -Response $response -ExpectedRows $candidate.RowCount `
                -TableName $spec.Table
            $storedRows = @(Read-G2dStoredMetricRows -Name $Name -MetricRunId $identity.MetricRunId)
            return Assert-G2dStoredCandidate -Name $Name -CandidateRows $candidate.Rows `
                -StoredRows $storedRows
        }
        $publishAction = {
            param($Evidence)
            $publishedAt = [datetimeoffset]::UtcNow.ToString(
                "yyyy-MM-dd'T'HH:mm:ss.fff'Z'",
                [Globalization.CultureInfo]::InvariantCulture
            )
            $publication = New-G2dPublicationRecord -Identity $identity -SourceIdentity $sourceIdentity `
                -Evidence $Evidence -CalculatedAt $calculatedAt -PublishedAt $publishedAt
            return Publish-G2dMetadata -Publication $publication
        }
        $publicationResult = Invoke-G2dPublicationSequence `
            -VerifyAction $verifyAction -PublishAction $publishAction
        $report.status = 'PUBLISHED'
        $report.publication = $publicationResult
        $reportPath = Write-G2dRefreshReport -OutputDirectory $outputDirectory -Report $report
        return [pscustomobject][ordered]@{
            status = 'PUBLISHED'
            metric_run_id = $identity.MetricRunId
            attempt_id = $attemptId
            report_path = $reportPath
        }
    } catch {
        $report.status = 'FAILED'
        $report.error = $_.Exception.Message
        $null = Write-G2dRefreshReport -OutputDirectory $outputDirectory -Report $report
        throw
    }
}

if ($FunctionsOnly) { return }

$result = Invoke-G2dRefresh -DataScope $DataScope -PlanOnly:$PlanOnly
$result | ConvertTo-Json -Depth 12 -Compress
