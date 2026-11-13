<#
.SYNOPSIS  Canonical local sandbox workflow (same Python CLI that CI runs). TEST-ONLY; no cloud access.
.EXAMPLE   .\scripts\local\Sandbox.ps1 infra-up          # disposable Postgres/Firestore/Pub/Sub/Kafka/Prometheus/Grafana
.EXAMPLE   .\scripts\local\Sandbox.ps1 verify            # full pytest suite; any skipped integration test fails
.EXAMPLE   .\scripts\local\Sandbox.ps1 load -Duration 2m # reset -> seed -> k6 -> invariant audit -> evidence
.EXAMPLE   .\scripts\local\Sandbox.ps1 up                # interactive: real Lock+Bed, stand-ins, Triage, 2 allocators
.EXAMPLE   .\scripts\local\Sandbox.ps1 infra-down        # removes only the hospital-sandbox Compose project
#>
param(
    [Parameter(Mandatory, Position = 0)][ValidateSet('infra-up', 'infra-down', 'verify', 'load', 'up', 'env')][string]$Command,
    [string]$Duration = '2m',
    [int]$Vus = 20,
    [ValidateSet('contention', 'multipool')][string]$Fixture = 'contention',
    [Parameter(ValueFromRemainingArguments)][string[]]$PytestArgs
)
. (Join-Path $PSScriptRoot 'LocalCommon.ps1')
Push-Location $RepoRoot
try {
    $arguments = @('-m', 'tests.sandbox.cli', $Command)
    switch ($Command) {
        'load' { $arguments += @('--duration', $Duration, '--vus', $Vus) }
        'up' { $arguments += @('--fixture', $Fixture) }
        'verify' { if ($PytestArgs) { $arguments += $PytestArgs } }
    }
    Invoke-Native { & $Python @arguments }
} finally { Pop-Location }
