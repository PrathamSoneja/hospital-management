<#
.SYNOPSIS  Start local PostgreSQL + Firestore emulator, create and migrate the disposable DB
           named in .env.local (must end in _test).
.EXAMPLE   .\scripts\local\Start-LocalStack.ps1
#>
. (Join-Path $PSScriptRoot 'LocalCommon.ps1')
Import-LocalEnv
$db = Assert-DisposableDatabase $env:DATABASE_URL
Push-Location $RepoRoot
try {
    Invoke-Native { docker compose up -d postgres firestore-emulator }
    for ($i = 0; $i -lt 30; $i++) {
        Invoke-Native -AllowFailure { docker exec hospital-postgres pg_isready -U postgres } | Out-Null
        if ($LASTEXITCODE -eq 0) { break }
        Start-Sleep 1
    }
    $exists = Invoke-Native { docker exec hospital-postgres psql -U postgres -Atc "select 1 from pg_database where datname='$db'" }
    if ("$exists".Trim() -ne '1') { Invoke-Native { docker exec hospital-postgres createdb -U postgres $db } }
    Invoke-Native { & $Python -m alembic upgrade head } | Select-Object -Last 1
    Invoke-Native { & $Python -m alembic current } | Select-Object -Last 1
    Write-Host "Local stack ready: database $db migrated; Firestore emulator on $env:FIRESTORE_EMULATOR_HOST"
} finally { Pop-Location }
