<#
.SYNOPSIS  UNEXECUTED. Plan Ruchir's existing Terraform stack (infra/terraform) into a saved plan
           file for review. Owner: Ruchir. Applies nothing.
.EXAMPLE   .\scripts\cloud\Invoke-TerraformPlan.ps1 -ProjectId <project> -BackendConfig backend.hcl -VarFile staging.tfvars
           (both files live, ignored, in infra\terraform; see backend.hcl.example)
#>
param([Parameter(Mandatory)][string]$ProjectId, [Parameter(Mandatory)][string]$BackendConfig, [Parameter(Mandatory)][string]$VarFile)
. (Join-Path $PSScriptRoot 'CloudCommon.ps1')
Assert-CloudTarget -ProjectId $ProjectId -Action "terraform init + plan in infra/terraform (reads state, changes nothing)"
Push-Location (Join-Path $RepoRoot 'infra\terraform')
try {
    Invoke-Native { terraform init "-backend-config=$BackendConfig" }
    Invoke-Native { terraform validate }
    Invoke-Native { terraform plan "-var-file=$VarFile" -out=tfplan.bin }
    Invoke-Native { terraform show -no-color tfplan.bin } | Out-File -Encoding utf8 tfplan.txt
    $hash = (Get-FileHash tfplan.bin -Algorithm SHA256).Hash.ToLower()
    Write-Host "Review infra\terraform\tfplan.txt; approve plan sha256 $hash before Invoke-TerraformApply.ps1."
} finally { Pop-Location }
