Set-StrictMode -Version Latest

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
        if (Test-DependencyHash -Path $artifact.Destination -ExpectedHash $artifact.Sha256) {
            Write-Output "$($artifact.Name) cached"
            continue
        }

        $directory = Split-Path -Parent $artifact.Destination
        New-Item -ItemType Directory -Path $directory -Force | Out-Null
        $partial = "$($artifact.Destination).partial.$([guid]::NewGuid().ToString('N'))"
        try {
            Invoke-WebRequest -UseBasicParsing -Uri $artifact.Url -OutFile $partial -MaximumRedirection 0 -ErrorAction Stop
            if (-not (Test-DependencyHash -Path $partial -ExpectedHash $artifact.Sha256)) {
                throw "Downloaded dependency hash validation failed for $($artifact.Name)."
            }
            Move-Item -LiteralPath $partial -Destination $artifact.Destination -Force
            Write-Output "$($artifact.Name) installed"
        } finally {
            if ((Test-Path -LiteralPath $partial -PathType Leaf) -and
                (Test-AllowedDependencyPath -Path $partial -AllowedRoots @($directory))) {
                Remove-Item -LiteralPath $partial -Force
            }
        }
    }
}

Export-ModuleMember -Function Install-RuntimeDependencies
