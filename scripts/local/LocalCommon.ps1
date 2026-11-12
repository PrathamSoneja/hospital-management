# Shared helpers for scripts/local/Sandbox.ps1.
$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$Python = Join-Path $RepoRoot '.venv\Scripts\python.exe'

function Invoke-Native {
    # Windows PowerShell 5.1 turns native stderr into errors under 'Stop'; judge by exit code instead.
    param([Parameter(Mandatory)][scriptblock]$Command, [switch]$AllowFailure)
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try { & $Command 2>&1 | ForEach-Object { "$_" } } finally { $ErrorActionPreference = $previous }
    if (-not $AllowFailure -and $LASTEXITCODE -ne 0) { throw "Command failed with exit code ${LASTEXITCODE}: $Command" }
}
