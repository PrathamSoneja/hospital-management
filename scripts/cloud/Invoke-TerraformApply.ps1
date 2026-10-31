<#
.SYNOPSIS  UNEXECUTED. Apply ONLY a previously reviewed saved plan (tfplan.bin) whose SHA-256
           was approved. Owner: Ruchir. Billable.
.EXAMPLE   .\scripts\cloud\Invoke-TerraformApply.ps1 -ProjectId <project> -ApprovedPlanSha256 <hash printed by plan>
#>
param([Parameter(Mandatory)][string]$ProjectId, [Parameter(Mandatory)][string]$ApprovedPlanSha256)
. (Join-Path $PSScriptRoot 'CloudCommon.ps1')
$plan = Join-Path $RepoRoot 'infra\terraform\tfplan.bin'
if (-not (Test-Path $plan)) { throw "No saved plan; run Invoke-TerraformPlan.ps1 first." }
$actual = (Get-FileHash $plan -Algorithm SHA256).Hash.ToLower()
if ($actual -ne $ApprovedPlanSha256.ToLower()) { throw "Plan hash differs from the approved plan; re-review." }
Assert-CloudTarget -ProjectId $ProjectId -Action "terraform apply tfplan.bin (sha256 $actual) - creates billable resources"
Push-Location (Join-Path $RepoRoot 'infra\terraform')
try { terraform apply tfplan.bin } finally { Pop-Location }
