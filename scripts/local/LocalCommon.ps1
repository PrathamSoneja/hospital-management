# Shared helpers for local scripts. Reads the ignored .env.local; never prints secret values.
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

function Import-LocalEnv {
    $path = Join-Path $RepoRoot '.env.local'
    if (-not (Test-Path $path)) { throw "Create .env.local from .env.local.example first (it is ignored by Git)." }
    foreach ($line in Get-Content $path) {
        if ($line -match '^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*(#.*)?$') {
            [Environment]::SetEnvironmentVariable($Matches[1], $Matches[2], 'Process')
        }
    }
    if ($env:ENVIRONMENT -ne 'local') { throw ".env.local must set ENVIRONMENT=local" }
}

function Assert-DisposableDatabase([string]$Url) {
    $uri = [Uri]($Url -replace '^postgresql\+psycopg2', 'postgresql')
    if ($uri.Host -notin @('localhost', '127.0.0.1')) { throw "Only local PostgreSQL is allowed" }
    $db = $uri.AbsolutePath.TrimStart('/')
    if (-not $db.EndsWith('_test')) { throw "Database '$db' is not a disposable *_test database" }
    return $db
}
