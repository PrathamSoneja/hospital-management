<#
.SYNOPSIS  UNEXECUTED. Read-only post-deploy evidence for Allocation/Triage: rollout, images,
           HPA/PDB, probes and a DB-backed 404 via port-forward, Cloud Logging request-ID
           correlation, and outbox backlog. -WatchHpa records HPA changes during a k6 run.
.EXAMPLE   .\scripts\cloud\Invoke-PostDeployChecks.ps1 -ProjectId <p> -KubeContext <ctx> -Namespace <ns>
#>
param([Parameter(Mandatory)][string]$ProjectId, [Parameter(Mandatory)][string]$KubeContext, [Parameter(Mandatory)][string]$Namespace,
      [switch]$WatchHpa, [string]$EvidenceDir)
. (Join-Path $PSScriptRoot 'CloudCommon.ps1')
if (-not $EvidenceDir) { $EvidenceDir = Join-Path $RepoRoot ("evidence\" + (Get-Date -Format 'yyyyMMdd-HHmm')) }
Assert-CloudTarget -ProjectId $ProjectId -KubeContext $KubeContext -Action "read-only checks in namespace $Namespace"
New-Item -ItemType Directory -Force $EvidenceDir | Out-Null
$ns = @('-n', $Namespace)
Invoke-Native { kubectl @ns rollout status deploy/bed-allocation-service --timeout=300s }
Invoke-Native { kubectl @ns rollout status deploy/triage-service --timeout=300s }
Invoke-Native { kubectl @ns get deploy,hpa,pdb,pods -o wide } | Tee-Object (Join-Path $EvidenceDir 'workloads.txt')
$imagesPath = 'jsonpath={range .items[*]}{.metadata.name}{" "}{.spec.template.spec.containers[0].image}{"\n"}{end}'
Invoke-Native { kubectl @ns get deploy -l app.kubernetes.io/part-of=hospital-bed-management -o $imagesPath } |
    Tee-Object (Join-Path $EvidenceDir 'images.txt')

$forward = Start-Process kubectl -ArgumentList @('-n', $Namespace, 'port-forward', 'svc/bed-allocation-service', '18102:8000') -PassThru -WindowStyle Hidden
try {
    Start-Sleep 3
    $requestId = "postdeploy-$(Get-Random)"
    foreach ($path in @('/healthz', '/readyz')) {
        "$path " + (Invoke-WebRequest "http://localhost:18102$path" -UseBasicParsing).StatusCode | Tee-Object -Append (Join-Path $EvidenceDir 'probes.txt')
    }
    try {
        Invoke-WebRequest 'http://localhost:18102/api/v1/allocations/postdeploy-missing' -Headers @{ 'X-Request-ID' = $requestId } -UseBasicParsing | Out-Null
    } catch {
        "missing-allocation " + $_.Exception.Response.StatusCode.value__ | Tee-Object -Append (Join-Path $EvidenceDir 'probes.txt')
    }
    Start-Sleep 20
    $filter = 'resource.type="k8s_container" AND jsonPayload.request_id="' + $requestId + '"'
    Invoke-Native { gcloud logging read $filter --project $ProjectId --freshness 10m --limit 20 --format json } |
        Out-File -Encoding utf8 (Join-Path $EvidenceDir 'log-correlation.json')
    (Invoke-WebRequest 'http://localhost:18102/metrics' -UseBasicParsing).Content -split "`n" |
        Where-Object { $_ -match '^outbox_pending_events' } | Tee-Object (Join-Path $EvidenceDir 'outbox-backlog.txt')
} finally {
    Stop-Process $forward.Id -ErrorAction SilentlyContinue
}
if ($WatchHpa) {
    Write-Host 'Start k6 from a pod or VM with private access to the staging Services now; Ctrl+C to stop.' -ForegroundColor Yellow
    kubectl @ns get hpa -w | Tee-Object (Join-Path $EvidenceDir 'hpa-watch.txt')
}
Write-Host "Evidence written to $EvidenceDir"
