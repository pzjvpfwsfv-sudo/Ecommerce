[CmdletBinding()]
param(
    [string]$RunId = "",
    [string]$CleanTopic = "",
    [string]$LateTopic = "",
    [string]$TableName = "",
    [switch]$PlanOnly,
    [switch]$FunctionsOnly
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Assert-G2cRunId {
    param([Parameter(Mandatory = $true)][string]$Value)

    if ($Value -cnotmatch "^[a-z0-9][a-z0-9-]{0,31}$") {
        throw "G2-C run-id is invalid."
    }
    return $Value
}

function Get-G2cDeployment {
    param(
        [Parameter(Mandatory = $true)][string]$RunId,
        [string]$CleanTopic = "",
        [string]$LateTopic = "",
        [string]$TableName = ""
    )

    $safeRunId = Assert-G2cRunId -Value $RunId
    $expectedRaw = "real_behavior_events_v1_$safeRunId"
    $expectedClean = "real_behavior_clean_v1_$safeRunId"
    $expectedLate = "real_behavior_late_v1_$safeRunId"
    $expectedTable = "real_behavior_detail_v1_$($safeRunId.Replace('-', '_'))"

    if ([string]::IsNullOrWhiteSpace($CleanTopic)) { $CleanTopic = $expectedClean }
    if ([string]::IsNullOrWhiteSpace($LateTopic)) { $LateTopic = $expectedLate }
    if ([string]::IsNullOrWhiteSpace($TableName)) { $TableName = $expectedTable }

    if ($CleanTopic -cne $expectedClean) { throw "Clean topic does not match the G2-C run-id." }
    if ($LateTopic -cne $expectedLate) { throw "Late topic does not match the G2-C run-id." }
    if ($TableName -cne "real_behavior_detail_v1" -and $TableName -cne $expectedTable) {
        throw "Table name is outside the G2-C namespace."
    }

    $identity = "graduation-g2c-$safeRunId"
    return [pscustomobject][ordered]@{
        RunId = $safeRunId
        RawTopic = $expectedRaw
        CleanTopic = $CleanTopic
        LateTopic = $LateTopic
        TableName = $TableName
        ConsumerGroup = $identity
        PipelineName = $identity
    }
}

function Render-G2cSql {
    param(
        [Parameter(Mandatory = $true)][string]$Template,
        [Parameter(Mandatory = $true)]$Deployment
    )

    $rendered = $Template.Replace("__RUN_ID__", [string]$Deployment.RunId)
    $rendered = $rendered.Replace("__CLEAN_TOPIC__", [string]$Deployment.CleanTopic)
    $rendered = $rendered.Replace("__LATE_TOPIC__", [string]$Deployment.LateTopic)
    $rendered = $rendered.Replace("__TABLE_NAME__", [string]$Deployment.TableName)
    if ($rendered -cmatch "__[A-Z0-9_]+__") {
        throw "Unresolved G2-C SQL placeholder."
    }
    return $rendered
}

function Assert-G2cRenderedSql {
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [Parameter(Mandatory = $true)]$Deployment
    )

    foreach ($required in @(
        "'topic' = '$($Deployment.CleanTopic)'",
        "'topic' = '$($Deployment.LateTopic)'",
        "'properties.group.id' = '$($Deployment.ConsumerGroup)'",
        "SET 'pipeline.name' = '$($Deployment.PipelineName)'",
        "CREATE TABLE IF NOT EXISTS lakehouse.analytics.$($Deployment.TableName)",
        "PARTITIONED BY (event_date)",
        "UNION ALL",
        "'clean' AS quality_route",
        "'late' AS quality_route",
        "'properties.isolation.level' = 'read_committed'",
        "'json.fail-on-missing-field' = 'true'",
        "'json.ignore-parse-errors' = 'false'"
    )) {
        if (-not $Sql.Contains($required)) {
            throw "Rendered G2-C SQL is missing a required contract fragment."
        }
    }

    if ($Sql -cmatch "__[A-Z0-9_]+__") {
        throw "Rendered G2-C SQL contains an unresolved placeholder."
    }
    if ($Sql -match "real_behavior_dlq|user_behavior_") {
        throw "Rendered G2-C SQL references a forbidden topic namespace."
    }

    $insertPattern = "(?im)^[ \t]*INSERT[ \t]+INTO[ \t]+lakehouse\.analytics\." +
        [regex]::Escape([string]$Deployment.TableName) + "[ \t]*$"
    if ([regex]::Matches($Sql, $insertPattern).Count -ne 1) {
        throw "Rendered G2-C SQL must contain exactly one target INSERT."
    }
}

function Get-G2cSha256 {
    param([Parameter(Mandatory = $true)][string]$Value)

    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $digest = $sha.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($Value))
        return -join ($digest | ForEach-Object { $_.ToString("x2") })
    } finally {
        $sha.Dispose()
    }
}

function Assert-G2cSourceTopics {
    param(
        [Parameter(Mandatory = $true)]$Deployment,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][string[]]$AvailableTopics
    )

    $topics = [System.Collections.Generic.HashSet[string]]::new(
        [string[]]$AvailableTopics,
        [System.StringComparer]::Ordinal
    )
    foreach ($required in @($Deployment.CleanTopic, $Deployment.LateTopic)) {
        if (-not $topics.Contains([string]$required)) {
            throw "Required G2-C source topic does not exist: $required"
        }
    }
}

function Enable-G2cDockerCli {
    param([string]$FallbackBin = "D:\DockerProgram\Docker\resources\bin")

    $current = @(Get-Command docker -CommandType Application -All -ErrorAction SilentlyContinue) |
        Where-Object { $_.Source -match "docker\.exe$" } | Select-Object -First 1
    if ($null -ne $current) { return [string]$current.Source }

    $candidate = Join-Path $FallbackBin "docker.exe"
    if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) {
        throw "Docker CLI is not available on PATH or in the configured D-drive fallback."
    }
    $env:PATH = "$FallbackBin;$env:PATH"
    $resolved = @(Get-Command docker -CommandType Application -All -ErrorAction Stop) |
        Where-Object { $_.Source -match "docker\.exe$" } | Select-Object -First 1
    if ($null -eq $resolved) { throw "Docker CLI fallback did not resolve docker.exe." }
    return [string]$resolved.Source
}

function Invoke-G2cChecked {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Command,
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )

    $output = & $Command
    if ($LASTEXITCODE -ne 0) { throw $FailureMessage }
    return $output
}

function Assert-G2cRuntimeFiles {
    param([Parameter(Mandatory = $true)][string]$RepositoryRoot)

    $required = @(
        "infra/compose/flink/lib/flink-sql-connector-kafka-3.3.0-1.19.jar",
        "infra/compose/flink/lib/flink-sql-connector-hive-3.1.3_2.12-1.19.2.jar",
        "infra/compose/flink/lib/iceberg-flink-runtime-1.19-1.6.1.jar",
        "infra/compose/flink/lib/iceberg-aws-bundle-1.6.1.jar",
        "infra/compose/flink/lib/hadoop-client-api-3.3.6.jar",
        "infra/compose/flink/lib/hadoop-client-runtime-3.3.6.jar",
        "infra/compose/flink/lib/hadoop-aws-3.3.6.jar",
        "infra/compose/flink/lib/aws-java-sdk-bundle-1.12.262.jar",
        "infra/compose/hive-metastore/lib/postgresql-42.7.4.jar"
    )
    $missing = @($required | Where-Object { -not (Test-Path -LiteralPath (Join-Path $RepositoryRoot $_)) })
    if ($missing.Count -gt 0) {
        throw "G2-C runtime dependencies are missing. Run scripts/install_runtime_dependencies.ps1 first: $($missing -join ', ')"
    }
}

function Wait-G2cSourceTopics {
    param(
        [Parameter(Mandatory = $true)]$Deployment,
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $topics = @(& docker exec ecom-kafka kafka-topics --bootstrap-server kafka:29092 --list 2>$null)
        if ($LASTEXITCODE -eq 0) {
            try {
                Assert-G2cSourceTopics -Deployment $Deployment -AvailableTopics $topics
                return
            } catch {
                if ((Get-Date) -ge $deadline) { throw }
            }
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)

    throw "Timed out waiting for G2-C source topics."
}

function Wait-G2cHiveMetastore {
    param([int]$TimeoutSeconds = 90)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        & docker exec ecom-flink-sql-client bash -lc "echo > /dev/tcp/hive-metastore/9083" 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { return }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)

    throw "Timed out waiting for Hive Metastore from the Flink SQL client."
}

function Wait-G2cTrinoReady {
    param(
        [int]$TimeoutSeconds = 120,
        [string]$BaseUrl = "http://localhost:8088"
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $info = Invoke-RestMethod -Method Get -Uri "$BaseUrl/v1/info" -TimeoutSec 10
            if (-not [string]::IsNullOrWhiteSpace([string]$info.nodeVersion.version)) { return }
        } catch {
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "Timed out waiting for Trino readiness."
}

function Invoke-G2cTrinoScalar {
    param([Parameter(Mandatory = $true)][string]$Sql)

    $lines = @(& docker exec ecom-trino trino --server http://localhost:8080 `
        --catalog lakehouse --schema analytics --output-format TSV --execute $Sql 2>$null)
    if ($LASTEXITCODE -ne 0) { throw "G2-C Trino safety query failed." }
    $content = @($lines | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($content.Count -ne 1 -or [string]$content[0] -cnotmatch "^[0-9]+$") {
        throw "G2-C Trino safety query returned an invalid scalar."
    }
    return [long]$content[0]
}

function Get-G2cTargetRowCount {
    param([Parameter(Mandatory = $true)][string]$TableName)

    if ($TableName -cne "real_behavior_detail_v1" -and
            $TableName -cnotmatch "^real_behavior_detail_v1_[a-z0-9][a-z0-9_]{0,31}$") {
        throw "Table name is outside the G2-C namespace."
    }
    $existsSql = "SELECT count(*) FROM lakehouse.information_schema.tables " +
        "WHERE table_schema = 'analytics' AND table_name = '$TableName'"
    $exists = Invoke-G2cTrinoScalar -Sql $existsSql
    if ($exists -eq 0) { return 0L }
    if ($exists -ne 1) { throw "G2-C target table identity is ambiguous." }
    return Invoke-G2cTrinoScalar -Sql "SELECT count(*) FROM lakehouse.analytics.$TableName"
}

function Get-G2cFlinkJobs {
    param([string]$FlinkRestUrl = "http://localhost:8081")

    $response = Invoke-RestMethod -Method Get -Uri "$FlinkRestUrl/jobs/overview" -TimeoutSec 10
    return @($response.jobs)
}

function Get-G2cActivePipelineJobs {
    param(
        [Parameter(Mandatory = $true)][string]$PipelineName,
        [object[]]$Jobs = @()
    )

    $activeStates = @("CREATED", "RUNNING", "FAILING", "CANCELLING", "RESTARTING", "RECONCILING", "INITIALIZING")
    return @($Jobs | Where-Object { $_.name -ceq $PipelineName -and $_.state -in $activeStates })
}

function Assert-G2cSubmissionSafety {
    param(
        [Parameter(Mandatory = $true)][string]$PipelineName,
        [object[]]$Jobs = @(),
        [Parameter(Mandatory = $true)][long]$TargetRowCount
    )

    $history = @($Jobs | Where-Object { $_.name -ceq $PipelineName })
    if ($history.Count -gt 0) {
        throw "G2-C run-id has already been submitted; use a fresh run-id."
    }
    if ($TargetRowCount -ne 0) {
        throw "G2-C target table is not empty; refusing replay into an existing fact table."
    }
}

function Wait-G2cJobRunning {
    param(
        [Parameter(Mandatory = $true)][string]$PipelineName,
        [int]$TimeoutSeconds = 90,
        [string]$FlinkRestUrl = "http://localhost:8081"
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $jobs = @(Get-G2cFlinkJobs -FlinkRestUrl $FlinkRestUrl)
            $matches = @(Get-G2cActivePipelineJobs -PipelineName $PipelineName -Jobs $jobs)
            if ($matches.Count -gt 1) { throw "Multiple Flink jobs use pipeline name $PipelineName." }
            if ($matches.Count -eq 1) {
                if ($matches[0].state -ceq "RUNNING") { return $matches[0] }
            }
        } catch {
            if ($_.Exception.Message -match "Multiple Flink jobs") { throw }
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)

    throw "Timed out waiting for G2-C Flink job $PipelineName."
}

function Write-G2cSqlFile {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)]$Deployment,
        [Parameter(Mandatory = $true)][string]$Sql
    )

    $directory = Join-Path $RepositoryRoot "tmp/graduation/g2c/$($Deployment.RunId)"
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
    $path = Join-Path $directory "lakehouse.sql"
    [System.IO.File]::WriteAllText($path, $Sql, [System.Text.UTF8Encoding]::new($false))
    return $path
}

if ($FunctionsOnly) { return }

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$deployment = Get-G2cDeployment -RunId $RunId -CleanTopic $CleanTopic -LateTopic $LateTopic -TableName $TableName
$templatePath = Join-Path $repositoryRoot "jobs/sql/16_real_behavior_to_iceberg.sql.template"
$template = Get-Content -LiteralPath $templatePath -Raw -Encoding UTF8
$sql = Render-G2cSql -Template $template -Deployment $deployment
Assert-G2cRenderedSql -Sql $sql -Deployment $deployment

if ($PlanOnly) {
    [ordered]@{
        status = "planned"
        run_id = $deployment.RunId
        raw_topic = $deployment.RawTopic
        clean_topic = $deployment.CleanTopic
        late_topic = $deployment.LateTopic
        table_name = $deployment.TableName
        consumer_group = $deployment.ConsumerGroup
        pipeline_name = $deployment.PipelineName
        sql_sha256 = Get-G2cSha256 -Value $sql
    } | ConvertTo-Json -Compress
    return
}

$composeFile = Join-Path $repositoryRoot "infra/docker-compose.yml"
$envFile = Join-Path $repositoryRoot "infra/.env.example"
Enable-G2cDockerCli | Out-Null
Assert-G2cRuntimeFiles -RepositoryRoot $repositoryRoot
Invoke-G2cChecked -Command { docker version --format "{{.Server.Version}}" } `
    -FailureMessage "Docker daemon is unavailable for G2-C." | Out-Null

Push-Location $repositoryRoot
try {
    Invoke-G2cChecked -Command {
        docker compose --env-file $envFile -f $composeFile --profile flink --profile lakehouse up -d `
            kafka-controller kafka-broker flink-jobmanager flink-taskmanager flink-sql-client `
            minio minio-init metastore-postgres hive-metastore trino
    } -FailureMessage "Failed to start the G2-C Kafka/Flink/lakehouse services." | Out-Null

    Wait-G2cHiveMetastore
    Wait-G2cTrinoReady
    Wait-G2cSourceTopics -Deployment $deployment
    $jobs = @(Get-G2cFlinkJobs)
    $targetRowCount = Get-G2cTargetRowCount -TableName $deployment.TableName
    Assert-G2cSubmissionSafety -PipelineName $deployment.PipelineName -Jobs $jobs `
        -TargetRowCount $targetRowCount

    $hostSqlPath = Write-G2cSqlFile -RepositoryRoot $repositoryRoot -Deployment $deployment -Sql $sql
    $containerSqlPath = "/workspace/tmp/graduation/g2c/$($deployment.RunId)/lakehouse.sql"
    $submissionOutput = @(Invoke-G2cChecked -Command {
        docker exec ecom-flink-sql-client /opt/flink/bin/sql-client.sh -f $containerSqlPath 2>&1
    } -FailureMessage "Flink SQL submission failed for G2-C.")
    if (($submissionOutput -join "`n") -match "\[ERROR\]") {
        throw "Flink SQL client reported an error for G2-C."
    }

    $job = Wait-G2cJobRunning -PipelineName $deployment.PipelineName
    [ordered]@{
        status = "running"
        run_id = $deployment.RunId
        job_id = [string]$job.jid
        pipeline_name = $deployment.PipelineName
        raw_topic = $deployment.RawTopic
        clean_topic = $deployment.CleanTopic
        late_topic = $deployment.LateTopic
        table_name = $deployment.TableName
        rendered_sql = $hostSqlPath
        sql_sha256 = Get-G2cSha256 -Value $sql
    } | ConvertTo-Json -Compress
} finally {
    Pop-Location
}
