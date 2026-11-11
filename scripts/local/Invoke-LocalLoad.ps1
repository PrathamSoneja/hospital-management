<#
.SYNOPSIS  Run the repeatable local load procedure (reset -> seed -> readiness -> k6 -> audit -> evidence)
           against the tracked sandbox. Results are local MOCK/emulator evidence only, never GKE/HPA evidence.
.EXAMPLE   .\scripts\local\Invoke-LocalLoad.ps1 -Duration 2m
#>
param([string]$Duration = '2m', [int]$Vus = 20)
& (Join-Path $PSScriptRoot 'Sandbox.ps1') infra-up
& (Join-Path $PSScriptRoot 'Sandbox.ps1') load -Duration $Duration -Vus $Vus
