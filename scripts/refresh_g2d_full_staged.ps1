[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][ValidateSet('Prepare', 'Publish')][string]$Phase,
    [Parameter(Mandatory = $true)][string]$SourceRunId,
    [string]$PythonExecutable = 'python',
    [switch]$PlanOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$requestedRunId = $SourceRunId
$requestedPlanOnly = [bool]$PlanOnly
. (Join-Path $PSScriptRoot 'refresh_g2d_behavior_metrics.ps1') -FunctionsOnly
$SourceRunId = $requestedRunId
$PlanOnly = $requestedPlanOnly

if ($SourceRunId -cne 'g5-full-01') {
    throw 'The staged full refresh is fixed to the verified g5-full-01 source run.'
}
$sourceTable = Resolve-G2dSourceTable -RunId $SourceRunId
if ($PlanOnly) {
    [pscustomobject]@{
        phases = @('Prepare', 'Publish')
        source_table = $sourceTable
        data_scope = 'stable-user-2pct-full'
        note = 'Prepare uses Trino only; Publish uses Doris only.'
    } | ConvertTo-Json -Compress
    return
}

function Assert-G2dFullCapacity {
    $c = (Get-PSDrive -Name C).Free
    $d = (Get-PSDrive -Name D).Free
    if ($c -lt 5GB -or $d -lt 20GB) {
        throw 'Full-sample capacity gate failed: C must have 5 GiB and D 20 GiB free.'
    }
}

function Wait-G2dTrino {
    $url = (Get-G2dTrinoEndpoints).HostHealthBaseUrl + '/v1/info'
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        try {
            $null = Invoke-RestMethod -Uri $url -TimeoutSec 3
        } catch { Start-Sleep -Seconds 2 }
        $probe = @(& docker exec -e TERM=dumb ecom-trino trino `
            --server http://localhost:8080 --catalog lakehouse --schema analytics `
            --output-format CSV_HEADER_UNQUOTED --execute 'SELECT 1 AS ready' 2>$null)
        if ($LASTEXITCODE -eq 0 -and $probe.Count -eq 2 -and
                [string]$probe[0] -ceq 'ready' -and [string]$probe[1] -ceq '1') {
            return
        }
        Start-Sleep -Seconds 2
    }
    throw 'Trino did not become ready.'
}

function Wait-G2dDoris {
    for ($attempt = 0; $attempt -lt 90; $attempt++) {
        try {
            $ready = @(Invoke-G2dDorisSql -Sql 'SELECT 1 AS ready;' -NoHeaders)
            if ($ready.Count -eq 1 -and [string]$ready[0] -ceq '1') {
                $backends = @(Invoke-G2dDorisQuery -Sql 'SHOW BACKENDS;')
                if (@($backends | Where-Object { [string]$_.Alive -ieq 'true' }).Count -ge 1) {
                    return
                }
            }
        } catch { }
        Start-Sleep -Seconds 2
    }
    throw 'Doris FE and BE did not become ready.'
}

function Invoke-G2dFullPython {
    param([string]$Mode, [string]$InputPath, [string]$ConfigPath, [string]$OutputPath)

    $arguments = @((Join-Path $PSScriptRoot 'g2d_full_stream.py'), $Mode,
        '--input', $InputPath, '--config', $ConfigPath)
    if ($Mode -ceq 'prepare') { $arguments += @('--output', $OutputPath) }
    $lines = @(& $PythonExecutable @arguments 2>&1)
    if ($LASTEXITCODE -ne 0 -or $lines.Count -ne 1) {
        throw "Full dimension $Mode audit failed: $($lines -join ' ')"
    }
    return ($lines[0] | ConvertFrom-Json)
}

function Get-G2dSmallCandidate {
    param([string]$Name, [object[]]$Rows, $Identity, [string]$Directory)

    $path = Join-Path $Directory "$Name.csv"
    $null = Export-G2dCandidateCsv -Target $Name -Rows $Rows -Identity $Identity `
        -OutputDirectory $Directory -OutputPath $path
    $candidateRows = @(ConvertFrom-G2dCsv -CsvText ([IO.File]::ReadAllText($path)))
    $spec = Get-G2dTargetSpec -Name $Name
    return [pscustomobject]@{
        path = $path
        row_count = [long]$candidateRows.Count
        canonical_sha256 = Get-G2dCanonicalDigest -Rows $candidateRows `
            -Columns $spec.Columns -UniqueKeyColumns $spec.UniqueKeyColumns
        file_sha256 = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
    }
}

function Assert-G2dFullManifest {
    param($Manifest, [string]$Directory)

    if ([string]$Manifest.status -cne 'PREPARED' -or
            [string]$Manifest.source_table -cne $sourceTable -or
            [string]$Manifest.data_scope -cne 'stable-user-2pct-full' -or
            [string]$Manifest.sql_template_sha256 -cne
                (Get-FileHash -LiteralPath $script:G2dSqlTemplatePath -Algorithm SHA256).Hash.ToLowerInvariant()) {
        throw 'Full refresh manifest identity or SQL version does not match.'
    }
    $identity = Get-G2dMetricIdentity -SnapshotId $Manifest.source_snapshot_id `
        -DataScope 'stable-user-2pct-full' -SourceEventCount $Manifest.source_event_count
    if ([string]$Manifest.metric_run_id -cne $identity.MetricRunId -or
            [string]$Manifest.source_identity.source_snapshot_id -cne [string]$identity.SourceSnapshotId -or
            [string]$Manifest.source_identity.source_event_count -cne [string]$identity.SourceEventCount -or
            [string]$Manifest.source_identity.window_start -cne '2019-10-01' -or
            [string]$Manifest.source_identity.window_end -cne '2019-11-30' -or
            [string]$Manifest.source_identity.distinct_event_count -cne '2199938') {
        throw 'Full refresh manifest source identity is invalid.'
    }
    foreach ($name in @('overview', 'funnel', 'dimension', 'quality')) {
        $candidate = $Manifest.candidates.$name
        $expectedPath = Join-Path $Directory "$name.csv"
        if ([IO.Path]::GetFullPath([string]$candidate.path) -cne
                [IO.Path]::GetFullPath($expectedPath) -or
                [string]$candidate.canonical_sha256 -notmatch '^[0-9a-f]{64}$' -or
                [long]$candidate.row_count -le 0 -or
                (Get-FileHash -LiteralPath $expectedPath -Algorithm SHA256).Hash.ToLowerInvariant() -cne
                    [string]$candidate.file_sha256) {
            throw "Full refresh candidate $name changed after preparation."
        }
    }
    return $identity
}

$null = Enable-G2dDockerCli
Assert-G2dFullCapacity
$env:TEMP = 'D:\EcommerceDev\temp'
$env:TMP = 'D:\EcommerceDev\temp'
$paths = $null

if ($Phase -ceq 'Prepare') {
    $startedTrino = $false
    try {
        $null = & docker start ecom-trino
        if ($LASTEXITCODE -ne 0) { throw 'Could not start Trino.' }
        $startedTrino = $true
        Wait-G2dTrino
        $snapshot = Get-G2dLatestSnapshotId -SourceTable $sourceTable
        $template = [IO.File]::ReadAllText($script:G2dSqlTemplatePath, [Text.Encoding]::UTF8)
        $sql = Split-G2dNamedSql -Sql $template -SnapshotId $snapshot -SourceTable $sourceTable
        Assert-G2dTrinoResultSet -Results $sql
        $sourceRows = @(Invoke-G2dTrinoStatement -Name source_identity -Sql $sql.source_identity)
        if ($sourceRows.Count -ne 1 -or
                [string]$sourceRows[0].source_snapshot_id -cne [string]$snapshot -or
                [string]$sourceRows[0].window_start -cne '2019-10-01' -or
                [string]$sourceRows[0].window_end -cne '2019-11-30' -or
                [string]$sourceRows[0].distinct_event_count -cne '2199938') {
            throw 'Full source snapshot, window, or distinct event count changed.'
        }
        $identity = Get-G2dMetricIdentity -SnapshotId $snapshot `
            -DataScope 'stable-user-2pct-full' -SourceEventCount $sourceRows[0].source_event_count
        $paths = Initialize-G2dRunDirectory -MetricRunId $identity.MetricRunId
        $directory = $paths.RunDirectory
        $overview = @(Invoke-G2dTrinoStatement -Name overview -Sql $sql.overview)
        $funnel = @(Invoke-G2dTrinoStatement -Name funnel -Sql $sql.funnel)
        $quality = @(Invoke-G2dTrinoStatement -Name quality -Sql $sql.quality)

        $null = & docker restart ecom-trino
        if ($LASTEXITCODE -ne 0) { throw 'Could not restart Trino before the dimension query.' }
        Wait-G2dTrino
        $raw = Join-Path $directory 'dimension-trino-raw.csv'
        $errorFile = Join-Path $directory 'dimension-trino-error.txt'
        & docker exec -e TERM=dumb ecom-trino trino --server http://localhost:8080 `
            --catalog lakehouse --schema analytics --output-format CSV_HEADER_UNQUOTED `
            --execute $sql.dimension > $raw 2> $errorFile
        if ($LASTEXITCODE -ne 0) {
            throw "Full dimension Trino query failed: $([IO.File]::ReadAllText($errorFile))"
        }
        $configPath = Join-Path $directory 'dimension-audit-config.json'
        $config = [ordered]@{
            metric_run_id = $identity.MetricRunId
            dataset_id = $identity.DatasetId
            metric_version = $identity.MetricVersion
            overview_rows = $overview
            window_start = [string]$sourceRows[0].window_start
            window_end = [string]$sourceRows[0].window_end
        }
        [IO.File]::WriteAllText($configPath, ($config | ConvertTo-Json -Depth 8),
            [Text.UTF8Encoding]::new($false))
        $dimensionEvidence = Invoke-G2dFullPython -Mode prepare -InputPath $raw `
            -ConfigPath $configPath -OutputPath (Join-Path $directory 'dimension.csv')
        $dimension = @($dimensionEvidence.summary_rows)
        $bundle = Assert-G2dMetricBundle -Identity $identity -SourceIdentity $sourceRows `
            -Overview $overview -Funnel $funnel -Dimension $dimension -Quality $quality
        $candidates = [ordered]@{}
        foreach ($name in @('overview', 'funnel', 'quality')) {
            $rows = if ($name -ceq 'overview') { $overview } elseif ($name -ceq 'funnel') { $funnel } else { $quality }
            $candidates[$name] = Get-G2dSmallCandidate -Name $name -Rows $rows `
                -Identity $identity -Directory $directory
        }
        $candidates.dimension = [pscustomobject]@{
            path = Join-Path $directory 'dimension.csv'
            row_count = [long]$dimensionEvidence.row_count
            canonical_sha256 = [string]$dimensionEvidence.canonical_sha256
            file_sha256 = [string]$dimensionEvidence.file_sha256
        }
        $manifest = [ordered]@{
            status = 'PREPARED'
            metric_run_id = $identity.MetricRunId
            data_scope = $identity.DataScope
            source_table = $sourceTable
            source_snapshot_id = [string]$identity.SourceSnapshotId
            source_event_count = [string]$identity.SourceEventCount
            source_identity = $sourceRows[0]
            calculated_at = [datetimeoffset]::UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'")
            sql_template_sha256 = (Get-FileHash -LiteralPath $script:G2dSqlTemplatePath -Algorithm SHA256).Hash.ToLowerInvariant()
            candidates = $candidates
            dimension_summary_rows = $dimension
            bundle = $bundle
        }
        $manifestPath = Join-Path $directory 'staged-full.json'
        [IO.File]::WriteAllText($manifestPath, ($manifest | ConvertTo-Json -Depth 12),
            [Text.UTF8Encoding]::new($false))
        [pscustomobject]@{ status = 'PREPARED'; metric_run_id = $identity.MetricRunId;
            dimension_rows = $dimensionEvidence.row_count; manifest_path = $manifestPath } |
            ConvertTo-Json -Compress
    } finally {
        if ($startedTrino) { $null = & docker stop -t 15 ecom-trino }
    }
    return
}

$metricRunId = 'behavior-v1-s5556722256660191067'
$paths = Assert-G2dFixedOutputPath -MetricRunId $metricRunId -RequireRunDirectory
$directory = $paths.RunDirectory
$manifestPath = Join-Path $directory 'staged-full.json'
if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
    throw 'Run Prepare before Publish; no verified manifest exists.'
}
$manifest = Get-Content -LiteralPath $manifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
$identity = Assert-G2dFullManifest -Manifest $manifest -Directory $directory
$overview = @(ConvertFrom-G2dCsv -CsvText ([IO.File]::ReadAllText((Join-Path $directory 'overview.csv'))))
$funnel = @(ConvertFrom-G2dCsv -CsvText ([IO.File]::ReadAllText((Join-Path $directory 'funnel.csv'))))
$quality = @(ConvertFrom-G2dCsv -CsvText ([IO.File]::ReadAllText((Join-Path $directory 'quality.csv'))))
$null = Assert-G2dMetricBundle -Identity $identity -SourceIdentity @($manifest.source_identity) `
    -Overview $overview -Funnel $funnel -Dimension @($manifest.dimension_summary_rows) -Quality $quality
if ([string](& docker inspect -f '{{.State.Running}}' ecom-trino).Trim() -cne 'false') {
    throw 'Stop Trino before starting Doris for the full publication.'
}
$null = & docker start ecom-doris-fe ecom-doris-be
if ($LASTEXITCODE -ne 0) { throw 'Could not start Doris FE/BE.' }
Wait-G2dDoris
Assert-G2dFullCapacity
Initialize-G2dDorisSchema
Assert-G2dDorisTables
if (@(Read-G2dPublicationRows -MetricRunId $identity.MetricRunId).Count -ne 0) {
    throw 'The selected full run is already published; refusing to overwrite it.'
}
$attemptId = New-G2dAttemptId
$evidence = [ordered]@{}
foreach ($name in @('overview', 'funnel', 'dimension', 'quality')) {
    Assert-G2dFullCapacity
    $candidate = $manifest.candidates.$name
    $spec = Get-G2dTargetSpec -Name $name
    $count = [long](Get-G2dCandidateCounts -MetricRunId $identity.MetricRunId)[$name]
    if ($count -ne 0 -and $count -ne [long]$candidate.row_count) {
        throw "Existing $name candidate count differs from the verified candidate."
    }
    if ($count -eq 0) {
        $response = Invoke-G2dStreamLoad -Name $name -MetricRunId $identity.MetricRunId `
            -AttemptId $attemptId -CandidatePath $candidate.path
        Assert-G2dStreamLoadResponse -Response $response -ExpectedRows $candidate.row_count `
            -TableName $spec.Table
    }
    if ($name -ceq 'dimension') {
        $runLiteral = ConvertTo-G2dSqlString -Value $identity.MetricRunId
        $sql = "SELECT $($spec.Columns -join ', ') FROM analytics.$($spec.Table) " +
            "WHERE metric_run_id = $runLiteral ORDER BY $($spec.UniqueKeyColumns -join ', ');"
        $raw = Join-Path $directory 'dimension-doris-readback.tsv'
        $errorFile = Join-Path $directory 'dimension-doris-error.txt'
        $sql | & docker exec -i ecom-doris-fe mysql --host=127.0.0.1 --port=9030 --user=root --batch `
            > $raw 2> $errorFile
        if ($LASTEXITCODE -ne 0) {
            throw "Full dimension Doris readback failed: $([IO.File]::ReadAllText($errorFile))"
        }
        $stored = Invoke-G2dFullPython -Mode readback -InputPath $raw `
            -ConfigPath (Join-Path $directory 'dimension-audit-config.json') -OutputPath ''
        if ([long]$stored.row_count -ne [long]$candidate.row_count -or
                [string]$stored.canonical_sha256 -cne [string]$candidate.canonical_sha256) {
            throw 'Full dimension Doris row count or canonical digest differs from the candidate.'
        }
        $evidence[$name] = [pscustomobject]@{ RowCount = [long]$stored.row_count;
            Sha256 = [string]$stored.canonical_sha256 }
    } else {
        $stored = @(Read-G2dStoredMetricRows -Name $name -MetricRunId $identity.MetricRunId)
        $candidateRows = if ($name -ceq 'overview') { $overview } elseif ($name -ceq 'funnel') { $funnel } else { $quality }
        $evidence[$name] = Assert-G2dStoredCandidate -Name $name `
            -CandidateRows $candidateRows -StoredRows $stored
    }
}
$publishedAt = [datetimeoffset]::UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'")
$publication = New-G2dPublicationRecord -Identity $identity `
    -SourceIdentity $manifest.source_identity -SourceTable $sourceTable -Evidence $evidence `
    -CalculatedAt $manifest.calculated_at -PublishedAt $publishedAt
$published = Publish-G2dMetadata -Publication $publication
$report = [ordered]@{ status = 'PUBLISHED'; metric_run_id = $identity.MetricRunId;
    data_scope = $identity.DataScope; source_table = $sourceTable;
    source_snapshot_id = $identity.SourceSnapshotId; candidates = $manifest.candidates;
    publication = $published }
$reportPath = Write-G2dRefreshReport -OutputDirectory $directory -Report $report
[pscustomobject]@{ status = 'PUBLISHED'; metric_run_id = $identity.MetricRunId;
    report_path = $reportPath } | ConvertTo-Json -Compress
