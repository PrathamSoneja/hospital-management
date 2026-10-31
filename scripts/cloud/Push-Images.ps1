<#
.SYNOPSIS  UNEXECUTED. Build Allocation/Triage images tagged with the Git SHA and push them to
           Artifact Registry. Prints immutable digests for scripts\gitops\Set-ImageDigest.ps1.
.EXAMPLE   .\scripts\cloud\Push-Images.ps1 -ProjectId <project> -Region <region> -Repository <ar-repo>
#>
param([Parameter(Mandatory)][string]$ProjectId, [Parameter(Mandatory)][string]$Region, [Parameter(Mandatory)][string]$Repository)
. (Join-Path $PSScriptRoot 'CloudCommon.ps1')
Push-Location $RepoRoot
try {
    if (git status --porcelain --untracked-files=no) { throw "Commit changes first; images must map to one commit." }
    $sha = "$(git rev-parse --short=12 HEAD)".Trim()
    $registry = "$Region-docker.pkg.dev/$ProjectId/$Repository"
    Assert-CloudTarget -ProjectId $ProjectId -Action "push bed-allocation:$sha and triage:$sha to $registry"
    Invoke-Native { gcloud auth configure-docker "$Region-docker.pkg.dev" --quiet }
    $services = @(
        @{ Name = 'bed-allocation'; File = 'services/bed_allocation/Dockerfile' },
        @{ Name = 'triage'; File = 'services/triage/Dockerfile' }
    )
    foreach ($svc in $services) {
        $ref = "$registry/$($svc.Name):$sha"
        Invoke-Native { docker build -f $svc.File -t $ref . }
        Invoke-Native { docker push $ref }
        $digest = docker inspect --format '{{index .RepoDigests 0}}' $ref
        Write-Host "$($svc.Name) => $digest"
    }
} finally { Pop-Location }
