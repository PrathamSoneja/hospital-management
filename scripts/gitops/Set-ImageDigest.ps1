<#
.SYNOPSIS  Local Git change only: pin an overlay image to an immutable Artifact Registry digest.
           Commit and review the change; ArgoCD deploys it after the approved manual sync.
.EXAMPLE   .\scripts\gitops\Set-ImageDigest.ps1 -Image bed-allocation -Reference us-central1-docker.pkg.dev/p/r/bed-allocation@sha256:<64 hex>
#>
param([Parameter(Mandatory)][ValidateSet('bed-allocation', 'triage')][string]$Image,
      [Parameter(Mandatory)][string]$Reference,
      [ValidateSet('staging')][string]$Overlay = 'staging')
$ErrorActionPreference = 'Stop'
$referencePattern = '^(?<name>[a-z0-9-]+-docker\.pkg\.dev/[a-z0-9-]+/[a-z0-9_-]+/' + [regex]::Escape($Image) + ')@(?<digest>sha256:[0-9a-f]{64})$'
$match = [regex]::Match($Reference, $referencePattern)
if (-not $match.Success) { throw "Reference must be <region>-docker.pkg.dev/<project>/<repo>/$Image@sha256:<digest> (digests only, no tags)." }
$files = @(Join-Path $PSScriptRoot "..\..\k8s\overlays\$Overlay\kustomization.yaml")
if ($Image -eq 'bed-allocation') { $files += Join-Path $PSScriptRoot '..\..\k8s\jobs\allocation-migrate\kustomization.yaml' }
foreach ($file in $files) {
    $text = Get-Content $file -Raw
    $block = '(?m)(  - name: ' + [regex]::Escape($Image) + '\r?\n    newName: )[^\r\n]+(\r?\n    digest: )sha256:[0-9a-f]{64}'
    if (-not [regex]::IsMatch($text, $block)) { throw "Image block for $Image not found in $file" }
    $text = [regex]::Replace($text, $block, '${1}' + $match.Groups['name'].Value + '${2}' + $match.Groups['digest'].Value)
    [IO.File]::WriteAllText((Resolve-Path $file), $text)
}
Write-Host "Pinned $Image. Review with: git diff -- k8s/"
