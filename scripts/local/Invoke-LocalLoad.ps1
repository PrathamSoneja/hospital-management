<#
.SYNOPSIS  Run k6 (grafana/k6 container) against LOCAL load targets started from the ignored
           dev-local/load_target.py launcher. Results are MOCK-TESTED evidence only.
.EXAMPLE   .\scripts\local\Invoke-LocalLoad.ps1 -Scenario contention
#>
param([ValidateSet('contention', 'sustained')][string]$Scenario = 'contention', [string]$Duration = '2m')
. (Join-Path $PSScriptRoot 'LocalCommon.ps1')
$k6 = 'grafana/k6:0.54.0'
$scripts = "$(Join-Path $RepoRoot 'k6'):/scripts:ro"
if ($Scenario -eq 'contention') {
    Invoke-Native { docker run --rm -v $scripts -e BASE_URL=http://host.docker.internal:18010 -e POOL_ID=POOL-K6 `
        -e ADMISSIONS=50 -e EXPECT_ALLOCATED=10 -e ADMISSION_PREFIX=adm-k6 $k6 run /scripts/contention.js }
} else {
    Invoke-Native { docker run --rm -v $scripts -e ALLOCATION_URL=http://host.docker.internal:18010 -e TRIAGE_URL=http://host.docker.internal:18011 `
        -e POOL_IDS=POOL-A,POOL-B,POOL-C -e VUS=20 -e DURATION=$Duration $k6 run /scripts/sustained-multi-pool.js }
}
