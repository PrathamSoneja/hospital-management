<#
.SYNOPSIS  UNEXECUTED. Run an APPROVED Allocation migration by manually syncing the dedicated
           ArgoCD Application (k8s/jobs/allocation-migrate). Never kubectl apply.
.EXAMPLE   .\scripts\cloud\Start-ApprovedMigration.ps1 -ProjectId <p> -KubeContext <ctx> -Revision 20261031_03
#>
param([Parameter(Mandatory)][string]$ProjectId, [Parameter(Mandatory)][string]$KubeContext, [Parameter(Mandatory)][string]$Revision,
      [string]$Application = 'hospital-allocation-migrations-staging')
. (Join-Path $PSScriptRoot 'CloudCommon.ps1')
$python = Join-Path $RepoRoot '.venv\Scripts\python.exe'
Push-Location $RepoRoot
try { $heads = (& $python -m alembic heads 2>$null) -join ' ' } finally { Pop-Location }
if ($heads -notmatch [regex]::Escape($Revision)) { throw "Revision $Revision is not the Alembic head ($heads)." }
Assert-CloudTarget -ProjectId $ProjectId -KubeContext $KubeContext -Action "argocd sync $Application (alembic upgrade to $Revision with the migration user)"
Invoke-Native { argocd app diff $Application }
Invoke-Native { argocd app sync $Application --prune=false }
Invoke-Native { argocd app wait $Application --health --timeout 900 }
Invoke-Native { kubectl get jobs -l app.kubernetes.io/part-of=hospital-bed-management }