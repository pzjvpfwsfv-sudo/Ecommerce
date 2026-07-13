[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot "infra/docker-compose.yml"
$envFile = Join-Path $repoRoot "infra/.env.example"
$chapter5Verify = Join-Path $repoRoot "scripts/verify_chapter_5_end_to_end.ps1"
$querySqlPath = Join-Path $repoRoot "jobs/sql/11_trino_read_iceberg_user_behavior.sql"
$trinoBaseUrl = "http://localhost:8088"
$trinoContainerName = "ecom-trino"
$hiveMetastoreContainerName = "ecom-hive-metastore"

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

function Wait-ForTrinoReady {
    param([int]$TimeoutSeconds = 90)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $info = Invoke-RestMethod -Method Get -Uri "$trinoBaseUrl/v1/info"
            if ($info.nodeVersion.version) {
                return
            }
        } catch {
            Start-Sleep -Seconds 3
        }
    }

    throw "Timed out waiting for Trino readiness.`nTrino diagnostics:`n$(Get-TrinoDiagnosticSummary)"
}

function Get-TrinoContainerLogs {
    try {
        return docker logs --tail 200 $trinoContainerName 2>&1 | Out-String
    } catch {
        return ""
    }
}

function Get-TrinoDiagnosticSummary {
    $logs = Get-TrinoContainerLogs
    if (-not $logs) {
        return "No Trino logs captured."
    }

    $tailLines = $logs.Split([Environment]::NewLine) | Select-Object -Last 40
    return ($tailLines -join [Environment]::NewLine)
}

function Wait-ForContainerRunning {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ContainerName,
        [int]$TimeoutSeconds = 60
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $status = docker inspect -f "{{.State.Status}}" $ContainerName 2>$null
        if ($LASTEXITCODE -eq 0 -and $status -eq "running") {
            return
        }

        Start-Sleep -Seconds 2
    }

    throw "Timed out waiting for container $ContainerName to be running."
}

function Wait-ForTrinoStatementReady {
    param([int]$TimeoutSeconds = 90)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $probe = Invoke-RestMethod `
                -Method Post `
                -Uri "$trinoBaseUrl/v1/statement" `
                -Headers @{
                    "X-Trino-User" = "codex"
                    "X-Trino-Source" = "chapter-6-validation"
                    "X-Trino-Catalog" = "lakehouse"
                    "X-Trino-Schema" = "analytics"
                } `
                -Body "SELECT 1" `
                -ContentType "text/plain"

            if ($probe.error) {
                if ($probe.error.message -match "still initializing") {
                    if ((Get-Date) -ge $deadline) {
                        throw "Timed out waiting for Trino statement readiness.`nTrino diagnostics:`n$(Get-TrinoDiagnosticSummary)"
                    }

                    Start-Sleep -Seconds 3
                    continue
                }

                throw "Trino statement probe failed: $($probe.error.message)"
            }

            return
        } catch {
            if ($_.Exception.Message -match "still initializing") {
                Start-Sleep -Seconds 3
                continue
            }

            Start-Sleep -Seconds 3
        }
    }

    throw "Timed out waiting for Trino statement readiness.`nTrino diagnostics:`n$(Get-TrinoDiagnosticSummary)"
}

function Invoke-TrinoStatement {
    param(
        [string]$Sql,
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ($true) {
        try {
            $response = Invoke-RestMethod `
                -Method Post `
                -Uri "$trinoBaseUrl/v1/statement" `
                -Headers @{
                    "X-Trino-User" = "codex"
                    "X-Trino-Source" = "chapter-6-validation"
                    "X-Trino-Catalog" = "lakehouse"
                    "X-Trino-Schema" = "analytics"
                } `
                -Body $Sql `
                -ContentType "text/plain"
        } catch {
            if ((Get-Date) -ge $deadline) {
                throw
            }

            if ($_.Exception.Message -match "still initializing") {
                Start-Sleep -Seconds 3
                continue
            }

            throw
        }

        if ($response.error) {
            if ($response.error.message -match "still initializing") {
                if ((Get-Date) -ge $deadline) {
                    throw "Timed out waiting for Trino statement execution readiness.`nTrino diagnostics:`n$(Get-TrinoDiagnosticSummary)"
                }

                Start-Sleep -Seconds 3
                continue
            }

            throw "Trino query failed: $($response.error.message)"
        }

        break
    }

    $rows = @()
    while ($true) {
        if ($response.error) {
            if ($response.error.message -match "still initializing") {
                if ((Get-Date) -ge $deadline) {
                    throw "Timed out waiting for Trino statement execution readiness.`nTrino diagnostics:`n$(Get-TrinoDiagnosticSummary)"
                }

                Start-Sleep -Seconds 3
                continue
            }

            throw "Trino query failed: $($response.error.message)"
        }

        if ($response.data) {
            $rows += $response.data
        }
        if (-not $response.nextUri) {
            break
        }

        try {
            $response = Invoke-RestMethod -Method Get -Uri $response.nextUri
        } catch {
            throw
        }
    }

    return $rows
}

function ConvertTo-TrinoTableRows {
    param(
        [Parameter(Mandatory = $true)]
        $Rows
    )

    $tableRows = New-Object System.Collections.Generic.List[object[]]
    foreach ($row in [System.Collections.IEnumerable]$Rows) {
        if ($null -eq $row) {
            continue
        }

        if ($row -is [System.Array]) {
            [void]$tableRows.Add(([object[]]@($row)))
            continue
        }

        if ($row -is [System.ValueType] -or $row -is [string]) {
            [void]$tableRows.Add(([object[]]@($row)))
            continue
        }

        $properties = @($row.PSObject.Properties)
        if ($properties.Count -gt 0) {
            [void]$tableRows.Add(([object[]]@($properties | ForEach-Object { $_.Value })))
            continue
        }

        [void]$tableRows.Add(([object[]]@($row)))
    }

    return [object[][]]$tableRows.ToArray()
}

function Get-TrinoScalarFromFirstRow {
    param(
        [Parameter(Mandatory = $true)]
        [object[][]]$Rows
    )

    if ($Rows.Count -eq 0 -or $Rows[0].Count -eq 0) {
        return $null
    }

    return $Rows[0][0]
}

function Wait-ForTrinoCountResult {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Sql,
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $rows = @(ConvertTo-TrinoTableRows -Rows (Invoke-TrinoStatement -Sql $Sql -TimeoutSeconds $TimeoutSeconds))
        if ($rows.Count -gt 0) {
            $value = [int](Get-TrinoScalarFromFirstRow -Rows $rows)
            if ($value -gt 0) {
                return @{
                    Rows = $rows
                    Count = $value
                }
            }
        }

        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)

    return @{
        Rows = $rows
        Count = 0
    }
}

Write-Host "[chapter6-verify] preparing iceberg data through chapter 5 verification..."
& $chapter5Verify

Write-Host "[chapter6-verify] starting trino service..."
Invoke-CheckedCommand `
    -Command { docker compose --env-file $envFile -f $composeFile --profile lakehouse up -d hive-metastore trino | Out-Null } `
    -FailureMessage "Failed to start Trino with docker compose."

Write-Host "[chapter6-verify] waiting for trino readiness..."
Wait-ForContainerRunning -ContainerName $hiveMetastoreContainerName -TimeoutSeconds 60
Wait-ForTrinoReady
Wait-ForTrinoStatementReady

$sqlText = Get-Content -Path $querySqlPath -Raw
$statements = $sqlText -split ";"
$statements = $statements | ForEach-Object { $_.Trim() } | Where-Object { $_ }
if ($statements.Count -ne 2) {
    throw "Expected exactly 2 non-empty SQL statements in $querySqlPath but found $($statements.Count)."
}

$countResult = Wait-ForTrinoCountResult -Sql $statements[0] -TimeoutSeconds 90
$countRows = @($countResult.Rows)
$countValue = [int]$countResult.Count

if (-not $countRows -or $countValue -le 0) {
    throw "Trino count query returned zero rows."
}

$groupRows = @(ConvertTo-TrinoTableRows -Rows (Invoke-TrinoStatement -Sql $statements[1]))
if (-not $groupRows -or $groupRows.Count -lt 1) {
    throw "Trino group query returned no event_type rows."
}

$topGroupRow = [object[]]$groupRows[0]

Write-Host "[chapter6-verify] event_count=$countValue"
Write-Host "[chapter6-verify] top_event_type=$($topGroupRow[0]) count=$($topGroupRow[1])"
