function Test-DependencyHash {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$ExpectedHash
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { return $false }
    $stream = [System.IO.File]::OpenRead($Path)
    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    try {
        $actual = ([System.BitConverter]::ToString($sha256.ComputeHash($stream)) -replace '-', '').ToLowerInvariant()
        return $actual -eq $ExpectedHash
    } finally {
        $sha256.Dispose()
        $stream.Dispose()
    }
}

function Test-AllowedDependencyPath {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string[]]$AllowedRoots
    )

    foreach ($root in $AllowedRoots) {
        if ($Path.StartsWith($root + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase)) {
            return $true
        }
    }
    return $false
}

function Assert-DependencyPathSafe {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$RepositoryRoot
    )

    $root = [System.IO.Path]::GetFullPath($RepositoryRoot).TrimEnd('\', '/')
    $candidate = [System.IO.Path]::GetFullPath($Path)
    if (-not $candidate.StartsWith($root + [System.IO.Path]::DirectorySeparatorChar,
            [System.StringComparison]::OrdinalIgnoreCase)) {
        throw 'Runtime dependency destination is outside the repository.'
    }

    $current = $root
    $relative = $candidate.Substring($root.Length).TrimStart('\', '/')
    $segments = @($relative.Split(@('\', '/'), [System.StringSplitOptions]::RemoveEmptyEntries))
    foreach ($segment in (@('') + $segments)) {
        if ($segment) { $current = Join-Path $current $segment }
        if (-not (Test-Path -LiteralPath $current)) { break }
        $item = Get-Item -LiteralPath $current -Force
        if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw 'Runtime dependency path contains a reparse point.'
        }
    }
}

function Get-RuntimeDependencyArtifacts {
    param(
        [Parameter(Mandatory = $true)][string]$LockFile,
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [switch]$AllowInsecureHttpForTest
    )

    if (-not (Test-Path -LiteralPath $LockFile -PathType Leaf)) { throw 'Runtime dependency lock file is missing.' }
    $lock = Get-Content -LiteralPath $LockFile -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($null -eq $lock -or $lock.version -ne 1 -or $null -eq $lock.artifacts) { throw 'Runtime dependency lock schema is invalid.' }

    $repositoryRoot = [System.IO.Path]::GetFullPath($RepositoryRoot)
    $allowedRoots = @(
        [System.IO.Path]::GetFullPath((Join-Path $repositoryRoot 'infra/compose/flink/lib')),
        [System.IO.Path]::GetFullPath((Join-Path $repositoryRoot 'infra/compose/hive-metastore/lib'))
    )
    $destinations = @{}
    $validated = @()

    foreach ($artifact in @($lock.artifacts)) {
        $properties = @($artifact.PSObject.Properties.Name)
        if ($properties.Count -ne 4 -or @(@('name', 'url', 'destination', 'sha256') | Where-Object { $_ -notin $properties }).Count -gt 0) {
            throw 'Runtime dependency artifact schema is invalid.'
        }
        $name = [string]$artifact.name
        $url = [string]$artifact.url
        $destination = [string]$artifact.destination
        $sha256 = [string]$artifact.sha256
        if ($name -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]*\.jar$' -or $sha256 -notmatch '^[0-9a-f]{64}$') {
            throw 'Runtime dependency artifact name or SHA-256 is invalid.'
        }
        $uri = $null
        if (-not [System.Uri]::TryCreate($url, [System.UriKind]::Absolute, [ref]$uri) -or $uri.UserInfo) {
            throw 'Runtime dependency URL is invalid.'
        }
        $isFixtureUrl = $AllowInsecureHttpForTest -and $uri.Scheme -eq 'http' -and $uri.IsLoopback
        if ($uri.Scheme -ne 'https' -and -not $isFixtureUrl) { throw 'Runtime dependency URL must use HTTPS.' }
        if ($destination -notmatch '^infra/compose/(flink|hive-metastore)/lib/[A-Za-z0-9][A-Za-z0-9._-]*\.jar$' -or
            [System.IO.Path]::GetFileName($destination) -ne $name) {
            throw 'Runtime dependency destination is invalid.'
        }
        if ($destinations.ContainsKey($destination)) { throw 'Runtime dependency destinations must be unique.' }
        $destinations[$destination] = $true
        $destinationPath = [System.IO.Path]::GetFullPath((Join-Path $repositoryRoot $destination.Replace('/', '\')))
        if (-not (Test-AllowedDependencyPath -Path $destinationPath -AllowedRoots $allowedRoots)) {
            throw 'Runtime dependency destination is outside the allowed roots.'
        }
        $validated += [pscustomobject]@{ Name = $name; Url = $url; Destination = $destinationPath; Sha256 = $sha256 }
    }
    return @($validated)
}

function Install-RuntimeDependencies {
    param(
        [Parameter(Mandatory = $true)][string]$LockFile,
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [switch]$AllowInsecureHttpForTest
    )

    $artifacts = Get-RuntimeDependencyArtifacts -LockFile $LockFile -RepositoryRoot $RepositoryRoot `
        -AllowInsecureHttpForTest:$AllowInsecureHttpForTest
    foreach ($artifact in $artifacts) {
        Assert-DependencyPathSafe -Path $artifact.Destination -RepositoryRoot $RepositoryRoot
        if (Test-DependencyHash -Path $artifact.Destination -ExpectedHash $artifact.Sha256) {
            Write-Output "$($artifact.Name) cached"
            continue
        }

        $directory = Split-Path -Parent $artifact.Destination
        Assert-DependencyPathSafe -Path $directory -RepositoryRoot $RepositoryRoot
        New-Item -ItemType Directory -Path $directory -Force | Out-Null
        Assert-DependencyPathSafe -Path $directory -RepositoryRoot $RepositoryRoot
        $partial = "$($artifact.Destination).partial.$([guid]::NewGuid().ToString('N'))"
        try {
            Assert-DependencyPathSafe -Path $partial -RepositoryRoot $RepositoryRoot
            try {
                Invoke-WebRequest -UseBasicParsing -Uri $artifact.Url -OutFile $partial -MaximumRedirection 0 -ErrorAction Stop
            } catch {
                throw "Runtime dependency download failed for $($artifact.Name)."
            }
            if (-not (Test-DependencyHash -Path $partial -ExpectedHash $artifact.Sha256)) {
                throw "Downloaded dependency hash validation failed for $($artifact.Name)."
            }
            Assert-DependencyPathSafe -Path $partial -RepositoryRoot $RepositoryRoot
            Assert-DependencyPathSafe -Path $artifact.Destination -RepositoryRoot $RepositoryRoot
            Move-Item -LiteralPath $partial -Destination $artifact.Destination -Force
            Write-Output "$($artifact.Name) installed"
        } finally {
            Assert-DependencyPathSafe -Path $partial -RepositoryRoot $RepositoryRoot
            if ((Test-Path -LiteralPath $partial -PathType Leaf) -and
                (Test-AllowedDependencyPath -Path $partial -AllowedRoots @($directory))) {
                Remove-Item -LiteralPath $partial -Force
            }
        }
    }
}

function Read-Chapter105EnvFile {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw 'Environment file is missing.'
    }

    $values = [ordered]@{}
    $lineNumber = 0
    foreach ($line in Get-Content -LiteralPath $Path -Encoding UTF8) {
        $lineNumber += 1
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith('#')) { continue }
        $match = [regex]::Match($line, '^\s*(?<key>[A-Za-z_][A-Za-z0-9_]*)=(?<value>.*)$')
        if (-not $match.Success) {
            throw "Environment file has an invalid entry at line $lineNumber."
        }
        $key = $match.Groups['key'].Value
        if ($values.Contains($key)) {
            throw "Environment file contains a duplicate key at line $lineNumber."
        }
        $values[$key] = $match.Groups['value'].Value
    }
    return $values
}

function Get-Chapter105PrimaryRepositoryRoot {
    param([Parameter(Mandatory = $true)][string]$StartPath)

    $originalLocation = Get-Location
    try {
        Set-Location -LiteralPath $StartPath
        $commonDirectory = @(& git rev-parse --path-format=absolute --git-common-dir 2>$null)
        if ($LASTEXITCODE -eq 0 -and $commonDirectory.Count -eq 1) {
            $commonPath = [System.IO.Path]::GetFullPath([string]$commonDirectory[0]).TrimEnd('\', '/')
            if ([System.IO.Path]::GetFileName($commonPath) -eq '.git') {
                $candidate = Split-Path -Parent $commonPath
                $isRepository = @(& git -C $candidate rev-parse --is-inside-work-tree 2>$null)
                if ($LASTEXITCODE -eq 0 -and $isRepository.Count -eq 1 -and $isRepository[0] -eq 'true') {
                    return [System.IO.Path]::GetFullPath($candidate).TrimEnd('\', '/')
                }
            }
        }
        $topLevel = @(& git rev-parse --path-format=absolute --show-toplevel 2>$null)
        if ($LASTEXITCODE -eq 0 -and $topLevel.Count -eq 1) {
            return [System.IO.Path]::GetFullPath([string]$topLevel[0]).TrimEnd('\', '/')
        }
        throw 'Unable to resolve the primary repository root.'
    } finally {
        Set-Location -LiteralPath $originalLocation
    }
}

function Get-Chapter105StableMinioDataPath {
    param([Parameter(Mandatory = $true)][string]$RepositoryRoot)

    $root = [System.IO.Path]::GetFullPath($RepositoryRoot).TrimEnd('\', '/')
    if ($root -match '(?i)[\\/]\.worktrees(?:[\\/]|$)') {
        throw 'The primary repository root must not be a worktree.'
    }
    return Join-Path $root 'infra\compose\minio\data'
}

function Resolve-Chapter105RepositoryPath {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$AllowedRelativeRoot
    )

    $root = [System.IO.Path]::GetFullPath($RepositoryRoot).TrimEnd('\', '/')
    if (-not (Test-Path -LiteralPath $root -PathType Container) -or
        ((Get-Item -LiteralPath $root -Force).Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw 'Repository root is missing or is a reparse point.'
    }
    $allowedRoot = [System.IO.Path]::GetFullPath((Join-Path $root $AllowedRelativeRoot)).TrimEnd('\', '/')
    $segments = @($Path.Split(@('\', '/'), [System.StringSplitOptions]::RemoveEmptyEntries))
    if ($segments -contains '..') { throw 'Repository path traversal is not allowed.' }
    $candidate = if ([System.IO.Path]::IsPathRooted($Path)) {
        [System.IO.Path]::GetFullPath($Path)
    } else {
        [System.IO.Path]::GetFullPath((Join-Path $root $Path))
    }
    if (-not $candidate.StartsWith($allowedRoot + [System.IO.Path]::DirectorySeparatorChar,
            [System.StringComparison]::OrdinalIgnoreCase)) {
        throw 'Repository path is outside its allowed root.'
    }

    $current = $root
    $relative = $candidate.Substring($root.Length).TrimStart('\', '/')
    foreach ($segment in $relative.Split(@('\', '/'), [System.StringSplitOptions]::RemoveEmptyEntries)) {
        $current = Join-Path $current $segment
        if (-not (Test-Path -LiteralPath $current)) { break }
        $item = Get-Item -LiteralPath $current -Force
        if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw 'Repository path contains a reparse point.'
        }
    }
    return $candidate
}

function Invoke-Chapter105Native {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = @(& $FilePath @Arguments 2>&1)
        $exitCode = $LASTEXITCODE
    } catch {
        throw $FailureMessage
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($exitCode -ne 0) { throw $FailureMessage }
    return @($output | ForEach-Object { [string]$_ })
}

function Invoke-Chapter105Retry {
    param(
        [Parameter(Mandatory = $true)][ValidateRange(1, 1350)][int]$Attempts,
        [Parameter(Mandatory = $true)][ValidateRange(0, 60)][int]$SleepSeconds,
        [Parameter(Mandatory = $true)][string]$FailureMessage,
        [Parameter(Mandatory = $true)][scriptblock]$Action
    )

    for ($attempt = 1; $attempt -le $Attempts; $attempt += 1) {
        try { return & $Action } catch {
            if ($attempt -eq $Attempts) { throw $FailureMessage }
            if ($SleepSeconds -gt 0) { Start-Sleep -Seconds $SleepSeconds }
        }
    }
    throw $FailureMessage
}

function ConvertTo-Chapter105RedactedValue {
    param([AllowNull()][object]$Value)

    if ($null -eq $Value) { return $null }
    if ($Value -is [string]) {
        if ($Value -match '(?i)password|secret|api[_-]?key|token') { return '[REDACTED]' }
        return $Value
    }
    if ($Value -is [System.Collections.IDictionary]) {
        $result = [ordered]@{}
        foreach ($key in $Value.Keys) {
            $safeKey = [string]$key
            if ($safeKey -match '(?i)password|secret|api[_-]?key|token') {
                $result['[REDACTED]'] = '[REDACTED]'
            } else {
                $result[$safeKey] = ConvertTo-Chapter105RedactedValue -Value $Value[$key]
            }
        }
        return $result
    }
    if ($Value -is [System.Management.Automation.PSCustomObject]) {
        $result = [ordered]@{}
        foreach ($property in $Value.PSObject.Properties) {
            $safeKey = [string]$property.Name
            if ($safeKey -match '(?i)password|secret|api[_-]?key|token') {
                $result['[REDACTED]'] = '[REDACTED]'
            } else {
                $result[$safeKey] = ConvertTo-Chapter105RedactedValue -Value $property.Value
            }
        }
        return $result
    }
    if ($Value -is [System.Collections.IEnumerable]) {
        return @($Value | ForEach-Object { ConvertTo-Chapter105RedactedValue -Value $_ })
    }
    return $Value
}

function Write-Chapter105BootstrapReport {
    param(
        [Parameter(Mandatory = $true)][object]$Report,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $destination = [System.IO.Path]::GetFullPath($Path)
    $directory = Split-Path -Parent $destination
    [System.IO.Directory]::CreateDirectory($directory) | Out-Null
    $temporary = "$destination.partial.$([Guid]::NewGuid().ToString('N'))"
    try {
        $safeReport = ConvertTo-Chapter105RedactedValue -Value $Report
        $json = $safeReport | ConvertTo-Json -Depth 20
        [System.IO.File]::WriteAllText($temporary, $json, [System.Text.UTF8Encoding]::new($false))
        if (Test-Path -LiteralPath $destination -PathType Leaf) {
            [System.IO.File]::Replace($temporary, $destination, $null)
        } else {
            [System.IO.File]::Move($temporary, $destination)
        }
    } finally {
        if (Test-Path -LiteralPath $temporary -PathType Leaf) {
            Remove-Item -LiteralPath $temporary -Force
        }
    }
}

if ($MyInvocation.InvocationName -ne '.') {
    Export-ModuleMember -Function @(
        'Install-RuntimeDependencies',
        'Read-Chapter105EnvFile',
        'Get-Chapter105PrimaryRepositoryRoot',
        'Get-Chapter105StableMinioDataPath',
        'Resolve-Chapter105RepositoryPath',
        'Invoke-Chapter105Native',
        'Invoke-Chapter105Retry',
        'ConvertTo-Chapter105RedactedValue',
        'Write-Chapter105BootstrapReport'
    )
}
