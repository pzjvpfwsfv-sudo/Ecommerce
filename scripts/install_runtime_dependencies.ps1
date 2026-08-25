param(
    [string]$LockFile = (Join-Path $PSScriptRoot '..\infra\runtime-dependencies.lock.json'),
    [string]$RepositoryRoot = (Join-Path $PSScriptRoot '..'),
    [switch]$AllowInsecureHttpForTest
)

$ErrorActionPreference = 'Stop'
Import-Module (Join-Path $PSScriptRoot 'lib\Chapter105.Common.psm1') -Force
Install-RuntimeDependencies -LockFile $LockFile -RepositoryRoot $RepositoryRoot `
    -AllowInsecureHttpForTest:$AllowInsecureHttpForTest
