<#
.SYNOPSIS  Run the full tracked suite against disposable local services (skips become failures),
           plus OpenAPI, protobuf, and Kustomize render checks.
.EXAMPLE   .\scripts\local\Invoke-LocalTests.ps1
#>
. (Join-Path $PSScriptRoot 'LocalCommon.ps1')
Import-LocalEnv
Assert-DisposableDatabase $env:TEST_POSTGRES_URL | Out-Null
$env:REQUIRE_INTEGRATION = '1'
Push-Location $RepoRoot
try {
    Invoke-Native { & $Python -m pytest -p no:cacheprovider -q } | Select-Object -Last 1
    foreach ($spec in Get-ChildItem contracts\openapi\*.yaml) { Invoke-Native { & $Python -m openapi_spec_validator $spec.FullName } }
    Invoke-Native { & $Python scripts\compile_proto.py } | Out-Null
    Invoke-Native { git diff --exit-code -- shared/generated }
    Invoke-Native { kubectl kustomize k8s/overlays/staging } | Out-Null
    Invoke-Native { kubectl kustomize k8s/jobs/allocation-migrate } | Out-Null
    Write-Host "Local verification passed"
} finally { Pop-Location }
