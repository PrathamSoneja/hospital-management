# UNEXECUTED cloud helpers. Every cloud script requires explicit targets and a typed
# confirmation, refuses to run if the active gcloud/kubectl target differs, and uses ADC
# (gcloud auth application-default login) - never exported tokens or key files.
$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path

function Invoke-Native {
    # Windows PowerShell 5.1 turns native stderr into errors under 'Stop'; judge by exit code instead.
    param([Parameter(Mandatory)][scriptblock]$Command)
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try { & $Command 2>&1 | ForEach-Object { "$_" } } finally { $ErrorActionPreference = $previous }
    if ($LASTEXITCODE -ne 0) { throw "Command failed with exit code ${LASTEXITCODE}: $Command" }
}

function Assert-CloudTarget {
    param([Parameter(Mandatory)][string]$ProjectId, [string]$KubeContext, [Parameter(Mandatory)][string]$Action)
    if ($env:GOOGLE_APPLICATION_CREDENTIALS) { throw "Unset GOOGLE_APPLICATION_CREDENTIALS; use ADC or Workload Identity." }
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $active = "$(gcloud config get-value project 2>$null)".Trim()
        $ctx = if ($KubeContext) { "$(kubectl config current-context 2>$null)".Trim() } else { '' }
    } finally { $ErrorActionPreference = $previous }
    if ($active -ne $ProjectId) { throw "Active gcloud project '$active' does not match -ProjectId '$ProjectId'." }
    if ($KubeContext -and $ctx -ne $KubeContext) { throw "Current kube context '$ctx' does not match -KubeContext '$KubeContext'." }
    Write-Host "About to: $Action" -ForegroundColor Yellow
    Write-Host "Project: $ProjectId  Kube context: $KubeContext"
    $typed = Read-Host "Type the project ID to confirm"
    if ($typed -ne $ProjectId) { throw "Confirmation did not match; nothing was changed." }
}
