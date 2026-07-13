$ErrorActionPreference = "Stop"

function Invoke-CheckedCommand {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$Command,
        [Parameter(Mandatory = $true)]
        [string]$FailureMessage
    )

    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw $FailureMessage
    }
}

function New-ReadbackSqlFile {
    $combinedSqlFile = "tmp/chapter_5_readback_validation.sql"
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    $sqlFiles = @(
        "jobs/sql/06_create_iceberg_catalog.sql",
        "jobs/sql/10_readback_iceberg_user_behavior.sql"
    )

    [System.IO.File]::WriteAllText((Join-Path (Resolve-Path 'tmp') 'chapter_5_readback_validation.sql'), "", $utf8NoBom)
    foreach ($sqlFile in $sqlFiles) {
        [System.IO.File]::AppendAllText((Join-Path (Resolve-Path 'tmp') 'chapter_5_readback_validation.sql'), (Get-Content -Raw $sqlFile), $utf8NoBom)
        [System.IO.File]::AppendAllText((Join-Path (Resolve-Path 'tmp') 'chapter_5_readback_validation.sql'), "`r`n`r`n", $utf8NoBom)
    }

    return $combinedSqlFile
}

$endToEndScript = Join-Path $PSScriptRoot "verify_chapter_5_end_to_end.ps1"
& $endToEndScript
if ($LASTEXITCODE -ne 0) {
    throw "Failed to finish Chapter 5 end-to-end validation before readback."
}

$combinedSqlFile = New-ReadbackSqlFile
$containerSqlPath = "/workspace/tmp/chapter_5_readback_validation.sql"
$output = docker exec ecom-flink-sql-client /opt/flink/bin/sql-client.sh -f $containerSqlPath
if ($LASTEXITCODE -ne 0) {
    throw "Failed to execute Chapter 5 readback validation query."
}

$output | Write-Host
$outputText = $output -join "`n"

if ($outputText -notmatch "event_count") {
    throw "Readback validation did not print event_count."
}

if ($outputText -notmatch "1 row in set|Received a total of") {
    throw "Readback validation did not produce query result output."
}
