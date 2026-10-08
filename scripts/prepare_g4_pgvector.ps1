param(
    [string]$EnvFile = 'infra/.env.example',
    [string]$BackupDirectory = 'D:\EcommerceDev\backups\g4',
    [switch]$PreflightOnly
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repo 'infra/docker-compose.yml'
$migrationFile = Join-Path $repo 'infra/compose/app-postgres/migrations/002_knowledge.sql'
$authFile = Join-Path $repo 'infra/compose/app-postgres/init/001_auth.sql'
$envPath = if ([IO.Path]::IsPathRooted($EnvFile)) { $EnvFile } else { Join-Path $repo $EnvFile }

function Invoke-Docker([string[]]$Arguments) {
    $result = & docker @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed: $($Arguments[0..([Math]::Min(2, $Arguments.Count - 1))] -join ' ')" }
    return $result
}

function Get-VolumeState([string]$Volume) {
    $mount = "type=volume,source=$Volume,target=/data,readonly"
    $state = Invoke-Docker @('run', '--rm', '--network', 'none', '--mount', $mount,
        '--entrypoint', 'sh', 'postgres:16.4-alpine', '-c',
        'if test -s /data/PG_VERSION; then printf DATA; else printf EMPTY; fi')
    if ($state -notin @('DATA', 'EMPTY')) { throw "Cannot verify volume state: $Volume" }
    return $state
}

function Wait-Database([string]$Container, [string]$User, [string]$Database) {
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        & docker exec $Container pg_isready -h 127.0.0.1 -U $User -d $Database *> $null
        if ($LASTEXITCODE -eq 0) { return }
        Start-Sleep -Seconds 2
    }
    throw "PostgreSQL did not become ready: $Container"
}

function Get-AuthFingerprint([string]$Container, [string]$User, [string]$Database) {
    $sql = "SELECT (SELECT count(*) FROM app_users)::text || ':' || " +
        "(SELECT count(*) FROM app_sessions)::text || ':' || " +
        "(SELECT count(*) FROM auth_audit)::text || ':' || " +
        "coalesce((SELECT md5(string_agg(username || ':' || password_hash || ':' || role, ',' ORDER BY username)) FROM app_users), '')"
    return (Invoke-Docker @('exec', $Container, 'psql', '-v', 'ON_ERROR_STOP=1',
        '-U', $User, '-d', $Database, '-Atqc', $sql)).Trim()
}

function Stop-Temporary([string]$Container) {
    & docker stop $Container *> $null
}

if (-not (Test-Path -LiteralPath $envPath)) { throw "Env file not found: $envPath" }
$configText = Invoke-Docker @('compose', '--env-file', $envPath, '-f', $composeFile,
    '--profile', 'serving', 'config', '--format', 'json')
$config = ($configText -join "`n") | ConvertFrom-Json
$project = $config.name
$database = $config.services.'app-postgres'.environment.POSTGRES_DB
$user = $config.services.'app-postgres'.environment.POSTGRES_USER
$password = $config.services.'app-postgres'.environment.POSTGRES_PASSWORD
if (-not $project -or -not $database -or -not $user) { throw 'Compose app-postgres configuration is incomplete' }
$oldVolume = "${project}_app-postgres-data"
$newVolume = "${project}_app-postgres-g4-data"
$volumes = @(Invoke-Docker @('volume', 'ls', '--format', '{{.Name}}'))
$oldExists = $volumes -contains $oldVolume
$newExists = $volumes -contains $newVolume
$oldState = if ($oldExists) { Get-VolumeState $oldVolume } else { 'ABSENT' }
if ($oldState -eq 'DATA') {
    $users = @(Invoke-Docker @('ps', '--filter', "volume=$oldVolume", '--format', '{{.ID}}'))
    if ($users.Count -gt 0 -and $users[0]) { throw 'Old application database is running; stop it before migration' }
}

if ($PreflightOnly) {
    if ($oldState -eq 'DATA') { throw 'Old application data exists; backup and restore must be verified before switching' }
    Write-Output "Read-only preflight: old=$oldState new_volume_exists=$newExists"
    return
}

if ($newExists) { throw "New volume already exists; inspect it manually before retrying: $newVolume" }
if (-not $password) { throw 'APP_DB_PASSWORD must be set in the selected environment before migration' }
$backupPath = [IO.Path]::GetFullPath($BackupDirectory)
if (-not $backupPath.StartsWith('D:\', [StringComparison]::OrdinalIgnoreCase)) {
    throw 'BackupDirectory must be on D: to avoid filling C:'
}
$drive = Get-PSDrive -Name D
if ($drive.Free -lt 2GB) { throw 'D: needs at least 2 GiB free before migration' }
& docker image inspect 'pgvector/pgvector:0.8.7-pg16-bookworm' *> $null
if ($LASTEXITCODE -ne 0) { throw 'Pinned pgvector image is not local; pull it and rerun preflight' }

$backupFile = $null
$oldFingerprint = $null
if ($oldState -eq 'DATA') {
    New-Item -ItemType Directory -Force -Path $backupPath | Out-Null
    $backupFile = Join-Path $backupPath ("app-auth-{0}.dump" -f (Get-Date -Format 'yyyyMMdd-HHmmss'))
    $oldContainer = "g4-old-$PID"
    try {
        Invoke-Docker @('run', '--rm', '-d', '--name', $oldContainer, '--network', 'none',
            '--mount', "type=volume,source=$oldVolume,target=/var/lib/postgresql/data",
            '--mount', "type=bind,source=$backupPath,target=/backup",
            'postgres:16.4-alpine') | Out-Null
        Wait-Database $oldContainer $user $database
        $oldFingerprint = Get-AuthFingerprint $oldContainer $user $database
        Invoke-Docker @('exec', $oldContainer, 'pg_dump', '-U', $user, '-d', $database,
            '-Fc', '-f', "/backup/$(Split-Path -Leaf $backupFile)") | Out-Null
        Invoke-Docker @('exec', $oldContainer, 'pg_restore', '-l',
            "/backup/$(Split-Path -Leaf $backupFile)") | Out-Null
    } finally {
        Stop-Temporary $oldContainer
    }
    if (-not (Test-Path -LiteralPath $backupFile) -or (Get-Item $backupFile).Length -eq 0) {
        throw 'Logical backup is missing or empty; new volume was not created'
    }
    $backupSha = (Get-FileHash -Algorithm SHA256 -LiteralPath $backupFile).Hash
    Write-Output "Backup verified: $backupFile SHA256=$backupSha"
}

Invoke-Docker @('volume', 'create', $newVolume) | Out-Null
$newContainer = "g4-new-$PID"
$originalDb = [Environment]::GetEnvironmentVariable('POSTGRES_DB')
$originalUser = [Environment]::GetEnvironmentVariable('POSTGRES_USER')
$originalPassword = [Environment]::GetEnvironmentVariable('POSTGRES_PASSWORD')
try {
    $env:POSTGRES_DB = $database
    $env:POSTGRES_USER = $user
    $env:POSTGRES_PASSWORD = $password
    $runArgs = @('run', '--rm', '-d', '--name', $newContainer, '--network', 'none',
        '--mount', "type=volume,source=$newVolume,target=/var/lib/postgresql/data",
        '--mount', "type=bind,source=$migrationFile,target=/migration/002_knowledge.sql,readonly",
        '-e', 'POSTGRES_DB', '-e', 'POSTGRES_USER', '-e', 'POSTGRES_PASSWORD')
    if ($backupFile) {
        $runArgs += @('--mount', "type=bind,source=$backupPath,target=/backup,readonly")
    } else {
        $runArgs += @('--mount', "type=bind,source=$authFile,target=/docker-entrypoint-initdb.d/001_auth.sql,readonly")
    }
    $runArgs += 'pgvector/pgvector:0.8.7-pg16-bookworm'
    Invoke-Docker $runArgs | Out-Null
} finally {
    [Environment]::SetEnvironmentVariable('POSTGRES_DB', $originalDb)
    [Environment]::SetEnvironmentVariable('POSTGRES_USER', $originalUser)
    [Environment]::SetEnvironmentVariable('POSTGRES_PASSWORD', $originalPassword)
}

try {
    Wait-Database $newContainer $user $database
    if ($backupFile) {
        Invoke-Docker @('exec', $newContainer, 'pg_restore', '-U', $user, '-d', $database,
            '--no-owner', '--no-acl', "/backup/$(Split-Path -Leaf $backupFile)") | Out-Null
        $newFingerprint = Get-AuthFingerprint $newContainer $user $database
        if ($newFingerprint -ne $oldFingerprint) { throw 'Auth rows or password hashes differ after restore' }
    }
    for ($i = 0; $i -lt 2; $i++) {
        Invoke-Docker @('exec', $newContainer, 'psql', '-v', 'ON_ERROR_STOP=1', '-U', $user,
            '-d', $database, '-f', '/migration/002_knowledge.sql') | Out-Null
    }
    $extension = (Invoke-Docker @('exec', $newContainer, 'psql', '-U', $user, '-d', $database,
        '-Atqc', "SELECT extversion FROM pg_extension WHERE extname='vector'")).Trim()
    if (-not $extension) { throw 'pgvector extension verification failed' }
    $originalPgPassword = [Environment]::GetEnvironmentVariable('PGPASSWORD')
    try {
        $env:PGPASSWORD = $password
        $login = (Invoke-Docker @('exec', '-e', 'PGPASSWORD', $newContainer, 'psql', '-h', '127.0.0.1',
            '-U', $user, '-d', $database, '-Atqc', 'SELECT 1')).Trim()
    } finally {
        [Environment]::SetEnvironmentVariable('PGPASSWORD', $originalPgPassword)
    }
    if ($login -ne '1') { throw 'Application database password login failed' }
    Write-Output "G4 database ready: volume=$newVolume vector=$extension auth=$($oldFingerprint ?? 'fresh')"
} finally {
    Stop-Temporary $newContainer
}
