[CmdletBinding()]
param(
    [string]$ManifestPath = '',
    [int]$TimeoutSeconds = 600,
    [switch]$FunctionsOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$curatedVerifyManifestPath = $ManifestPath
$curatedVerifyTimeout = $TimeoutSeconds
$curatedVerifierFunctionsOnly = [bool]$FunctionsOnly
. (Join-Path $PSScriptRoot 'build_g2e_olist_curated.ps1') -FunctionsOnly
$FunctionsOnly = $curatedVerifierFunctionsOnly

function Get-G2eCuratedSnapshotsFromReports {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)]$Identity
    )

    $snapshots = [ordered]@{}
    $reports = [ordered]@{}
    $registry = Get-G2eCuratedRegistry
    foreach ($stage in Get-G2eCuratedStageOrder) {
        $path = Get-G2eCuratedStageReportPath $RepositoryRoot $Identity.SourceBundleSha256 $stage
        $report = Get-G2eRecordedCuratedStage $path $stage $Identity
        if ($null -eq $report) { throw "G2-E curated stage report is missing for $stage." }
        $table = [string]$registry[$stage].TargetTable
        $snapshots[$table] = Assert-G2ePositiveSnapshotId (Get-G2ePropertyValue $report 'snapshot_id') "$stage report"
        $reports[$stage] = $report
    }
    Assert-G2eExactMapKeys $snapshots (Get-G2eCuratedSnapshotTables) 'curated Snapshot reports'
    return [pscustomobject][ordered]@{ Snapshots = $snapshots; Reports = $reports }
}

function Invoke-G2eCuratedVerificationResults {
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [Parameter(Mandatory = $true)][string]$ComposeFile,
        [Parameter(Mandatory = $true)][string]$EnvFile
    )

    $results = [ordered]@{}
    foreach ($part in Split-G2eCuratedVerificationSql $Sql) {
        $results[$part.Name] = Invoke-G2eTrinoStatement $part.Sql $ComposeFile $EnvFile
    }
    return $results
}

function Assert-G2eCuratedSnapshotIdentity {
    param(
        [Parameter(Mandatory = $true)]$Row,
        [Parameter(Mandatory = $true)]$Identity,
        [Parameter(Mandatory = $true)]$CuratedSnapshots
    )

    if ([string](Get-G2ePropertyValue $Row 'source_bundle_sha256') -cne [string]$Identity.SourceBundleSha256 -or
            [string](Get-G2ePropertyValue $Row 'source_snapshot_set_sha256') -cne [string]$Identity.SourceSnapshotSetSha256) {
        throw 'G2-E curated verification identity differs from its sources.'
    }
    try {
        $sourceMap = [string](Get-G2ePropertyValue $Row 'source_snapshots_json') | ConvertFrom-Json
        $curatedMap = [string](Get-G2ePropertyValue $Row 'curated_snapshots_json') | ConvertFrom-Json
    } catch { throw 'G2-E curated Snapshot identity JSON is malformed.' }
    $sourceTables = [string[]]@((Get-G2eRegistry).Values | ForEach-Object { [string]$_.TargetTable })
    $actualSource = ConvertTo-G2eCanonicalSnapshotJson $sourceMap $sourceTables
    $expectedSource = ConvertTo-G2eCanonicalSnapshotJson $Identity.SourceSnapshots $sourceTables
    $curatedTables = Get-G2eCuratedSnapshotTables
    $actualCurated = ConvertTo-G2eCanonicalSnapshotJson $curatedMap $curatedTables
    $expectedCurated = ConvertTo-G2eCanonicalSnapshotJson $CuratedSnapshots $curatedTables
    if ($actualSource -cne $expectedSource -or $actualCurated -cne $expectedCurated) {
        throw 'G2-E curated Snapshot maps differ from their recorded identities.'
    }
    return $Row
}

if ($FunctionsOnly) { return }

if ([string]::IsNullOrWhiteSpace($curatedVerifyManifestPath)) { throw 'ManifestPath is required for G2-E curated verification.' }
if ($curatedVerifyTimeout -lt 1 -or $curatedVerifyTimeout -gt 3600) { throw 'G2-E curated timeout must be between 1 and 3600 seconds.' }

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repositoryRoot 'infra/docker-compose.yml'
$envFile = Join-Path $repositoryRoot 'infra/.env.example'
$identity = Get-G2eCuratedSourceContext $curatedVerifyManifestPath $repositoryRoot $envFile
$recorded = Get-G2eCuratedSnapshotsFromReports $repositoryRoot $identity
$curatedSnapshots = $recorded.Snapshots

$null = Enable-G2eDockerCli
$null = Invoke-G2eChecked { docker version --format '{{.Server.Version}}' } 'Docker daemon is unavailable for G2-E curated verification.'
$startup = Get-G2eServiceStartupPlan
$lakehouseServices = [string[]]$startup.Lakehouse
$null = Invoke-G2eChecked {
    docker compose --env-file $envFile -f $composeFile --profile lakehouse up -d @lakehouseServices
} 'Failed to start G2-E curated verification services.'
Wait-G2eHttpReady 'http://localhost:8088/v1/info' $curatedVerifyTimeout

foreach ($stage in Get-G2eCuratedStageOrder) {
    $spec = (Get-G2eCuratedRegistry)[$stage]
    $report = $recorded.Reports[$stage]
    $expectedRows = Get-G2eCuratedExpectedRowCount $stage $identity $composeFile $envFile
    if ((ConvertTo-G2eCount (Get-G2ePropertyValue $report 'row_count') "$stage report rows") -ne $expectedRows) {
        throw "G2-E curated stage report row count differs for $stage."
    }
    $expected = [pscustomobject][ordered]@{
        Stage = $stage
        ExpectedRowCount = $expectedRows
        SourceBundleSha256 = $identity.SourceBundleSha256
        SourceSnapshotSetSha256 = $identity.SourceSnapshotSetSha256
        RecordedSnapshotId = [string]$recorded.Snapshots[[string]$spec.TargetTable]
    }
    $state = Assert-G2eCuratedStageState (Get-G2eCuratedStageState $stage $composeFile $envFile) $expected
    if ($state.Kind -cne 'Verified') { throw "G2-E curated stage is not exact: $stage." }
}

$template = Get-Content -LiteralPath (Join-Path $repositoryRoot 'jobs/sql/22_olist_curated_verify.sql.template') -Raw -Encoding UTF8
$sql = Render-G2eCuratedSqlCore $template $identity $curatedSnapshots -RequireCompleteCurated
$results = Invoke-G2eCuratedVerificationResults $sql $composeFile $envFile
$null = Assert-G2eZeroHardGate $results.hard_gate
$quality = Assert-G2eReportableQuality $results.reportable_quality
$null = Assert-G2eGrainReconciliation $results.grain_reconciliation
$antiFanout = Assert-G2eAntiFanout $results.anti_fanout
$null = Assert-G2eCuratedSnapshotIdentity $results.snapshot_identity $identity $curatedSnapshots

$report = [ordered]@{
    status = 'PASS'
    verified_at = [DateTimeOffset]::UtcNow.ToString('o')
    dataset_id = $identity.DatasetId
    source_bundle_sha256 = $identity.SourceBundleSha256
    source_snapshot_set_sha256 = $identity.SourceSnapshotSetSha256
    source_snapshots = $identity.SourceSnapshots
    curated_snapshots = $curatedSnapshots
    hard_gate = $results.hard_gate
    reportable_quality = $quality.Counts
    grain_reconciliation = $results.grain_reconciliation
    anti_fanout = $antiFanout
    snapshot_identity = $results.snapshot_identity
}
$reportPath = Join-Path $repositoryRoot "tmp/graduation/g2e/$($identity.SourceBundleSha256)/curated/verification.json"
$savedPath = Write-G2eSourceReport $reportPath $report
$saved = Get-Content -LiteralPath $savedPath -Raw -Encoding UTF8 | ConvertFrom-Json
$output = [ordered]@{}
foreach ($property in $saved.PSObject.Properties) { $output[$property.Name] = $property.Value }
$output.report_path = $savedPath
$output | ConvertTo-Json -Depth 16 -Compress
