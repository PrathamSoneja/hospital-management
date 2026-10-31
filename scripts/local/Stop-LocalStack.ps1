<#
.SYNOPSIS  Stop local containers. Volumes and databases are kept (no data deletion).
#>
. (Join-Path $PSScriptRoot 'LocalCommon.ps1')
Push-Location $RepoRoot
try { Invoke-Native { docker compose stop } } finally { Pop-Location }
