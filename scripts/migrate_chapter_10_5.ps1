param(
    [switch]$TrafficPaused,
    [switch]$ConfirmRealtimeReset,
    [switch]$FunctionsOnly
)

$chapter105MigrationFunctionsOnly = [bool]$FunctionsOnly
if (-not $chapter105MigrationFunctionsOnly -and
    (-not $TrafficPaused -or -not $ConfirmRealtimeReset)) {
    throw 'Migration requires -TrafficPaused and -ConfirmRealtimeReset.'
}

. (Join-Path $PSScriptRoot 'reset_chapter_10_5_realtime.ps1') -FunctionsOnly

$script:Chapter105LastMigrationReport = $null

function Invoke-Chapter105Migration {
    param(
        [switch]$TrafficPaused,
        [switch]$ConfirmRealtimeReset,
        [Parameter(Mandatory = $true)][string]$RepositoryRoot
    )

    if (-not $TrafficPaused -or -not $ConfirmRealtimeReset) {
        throw 'Migration requires -TrafficPaused and -ConfirmRealtimeReset.'
    }

    $report = [pscustomobject][ordered]@{
        kind = 'chapter10_5_migration'
        status = 'running'
        started_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
        completed_at_utc = $null
        traffic_paused = $true
        reset_confirmed = $true
        plan = [pscustomobject][ordered]@{
            sequence = @(
                'record_lake_evidence',
                'stop_old_realtime_services',
                'reset_allowlisted_realtime_state',
                'start_postgresql_metastore',
                'restore_fixed_catalog',
                'start_chapter9_job',
                'run_strict_acceptance',
                'record_lake_evidence'
            )
            protected = @('warehouse', 'infra/compose/minio/data')
        }
        reset = $null
        evidence = [pscustomobject][ordered]@{ before = $null; after = $null }
        diagnostic_command = $script:Chapter105DiagnosticCommand
    }
    $script:Chapter105LastMigrationReport = $report
    Write-Host ("[plan] migration sequence: " + ($report.plan.sequence -join ' -> '))
    Write-Host '[protected] warehouse bucket; infra/compose/minio/data'

    try {
        $composeContext = New-Chapter105ControlledComposeContext -RepositoryRoot $RepositoryRoot
        Assert-Chapter105ControlledComposeContext -Context $composeContext `
            -RepositoryRoot $RepositoryRoot
        $composePrefix = @($composeContext.prefix)
        $report.evidence.before = Get-Chapter105LakeEvidence -ComposePrefix $composePrefix
        $report.reset = Invoke-Chapter105RealtimeReset -ConfirmReset `
            -RepositoryRoot $RepositoryRoot -ComposeContext $composeContext `
            -BeforeEvidence $report.evidence.before

        Invoke-Chapter105Native -FilePath 'powershell' -Arguments @(
            '-NoProfile', '-File', (Join-Path $RepositoryRoot 'scripts\bootstrap_chapter_10_5.ps1'),
            '-EnvFile', (Join-Path $RepositoryRoot 'infra\.env'),
            '-ReportPath', 'tmp/chapter-10-5/migration-bootstrap-report.json',
            '-ComposeProjectName', ([string]$composeContext.project_name)
        ) -FailureMessage 'Chapter 10.5 strict bootstrap acceptance failed.' | Out-Null

        $report.evidence.after = Get-Chapter105LakeEvidence -ComposePrefix $composePrefix
        Assert-Chapter105LakeEvidencePreserved -Before $report.evidence.before -After $report.evidence.after
        $report.status = 'passed'
        return $report
    } catch {
        $report.status = 'failed'
        Write-Host "[diagnostic] $($script:Chapter105DiagnosticCommand)"
        throw 'Chapter 10.5 migration failed. State was preserved; run the fixed diagnostic command.'
    } finally {
        $report.completed_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
        $script:Chapter105LastMigrationReport = $report
    }
}

if ($chapter105MigrationFunctionsOnly) { return }

$chapter105Root = Get-Chapter105PrimaryRepositoryRoot -StartPath $PSScriptRoot
$chapter105ReportPath = Join-Path $chapter105Root 'tmp/chapter-10-5/migration-report.json'
$chapter105PreviousMinioDataDir = [Environment]::GetEnvironmentVariable('MINIO_DATA_DIR', 'Process')
$chapter105Failure = $null
try {
    [Environment]::SetEnvironmentVariable(
        'MINIO_DATA_DIR',
        (Get-Chapter105StableMinioDataPath -RepositoryRoot $chapter105Root),
        'Process'
    )
    Invoke-Chapter105Migration -TrafficPaused:$TrafficPaused `
        -ConfirmRealtimeReset:$ConfirmRealtimeReset -RepositoryRoot $chapter105Root | Out-Null
} catch {
    $chapter105Failure = $_.Exception.Message
} finally {
    [Environment]::SetEnvironmentVariable('MINIO_DATA_DIR', $chapter105PreviousMinioDataDir, 'Process')
    if ($null -eq $script:Chapter105LastMigrationReport) {
        $script:Chapter105LastMigrationReport = [pscustomobject][ordered]@{
            kind = 'chapter10_5_migration'
            status = 'failed'
            diagnostic_command = $script:Chapter105DiagnosticCommand
        }
    }
    try {
        Write-Chapter105BootstrapReport -Report $script:Chapter105LastMigrationReport `
            -Path $chapter105ReportPath
    } catch {
        if ($null -eq $chapter105Failure) {
            $chapter105Failure = 'Chapter 10.5 migration report could not be written safely.'
        }
    }
}
if ($null -ne $chapter105Failure) { throw $chapter105Failure }
