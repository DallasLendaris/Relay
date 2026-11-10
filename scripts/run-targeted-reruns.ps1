<#
.SYNOPSIS
  Reruns only the suite tests that measure the audit fixes, one after another, on Windows. About 3 hours.

  In order:  t2 (scenarios a,b,c,d,f), t4, t5, t7 (30 minutes), then t8 and t9 if they are
  registered in loadtest\src\suite\tests\index.ts. Each test runs through run-scale-suite.ps1
  (same prerequisites, npm cache, data dir and keep-awake logic) in its own result folder.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts\run-targeted-reruns.ps1
  powershell -ExecutionPolicy Bypass -File scripts\run-targeted-reruns.ps1 -Scale smoke

.PARAMETER Scale    full (default) or smoke (tiny sizes, to check the setup).
.PARAMETER Workers  Worker processes per test (default: run-scale-suite.ps1's, logical CPUs - 2).
.PARAMETER OutDir   Results root (default: <repo parent>\results\rerun-<timestamp>, on the repo's drive,
                    e.g. D:\relay-audit\results\rerun-20260101-1200). One subfolder per test.
.PARAMETER DataDir  Scratch data folder (default: <drive>\relay-suite-data, shared with run-scale-suite.ps1
                    so cached seeds are reused).
#>
[CmdletBinding()]
param(
  [ValidateSet('full', 'smoke')][string]$Scale = 'full',
  [int]$Workers = 0,
  [string]$OutDir = '',
  [string]$DataDir = ''
)

$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$stamp = Get-Date -Format 'yyyyMMdd-HHmm'
if (-not $OutDir) { $OutDir = Join-Path (Join-Path (Split-Path -Parent $repo) 'results') "rerun-$stamp" }
$runner = Join-Path $PSScriptRoot 'run-scale-suite.ps1'
$psExe = (Get-Process -Id $PID).Path            # the same PowerShell that is running this script

function Fail($msg) { Write-Host "ERROR: $msg" -ForegroundColor Red; exit 1 }
if (-not (Test-Path $runner)) { Fail "run-scale-suite.ps1 not found next to this script ($runner)" }

# t8 and t9 are included only when they are registered in the suite at run time.
$index = Join-Path $repo 'loadtest\src\suite\tests\index.ts'
$registered = ''
if (Test-Path $index) { $registered = Get-Content -Raw $index }

# One entry per test: id, environment variables for that test only, what it measures.
$plan = @(
  @{ Id = 't2'; Env = @{ T2_SCENARIOS = 'a,b,c,d,f' }; What = 'fan-out a,b,c,d + dense city cell f' },
  @{ Id = 't4'; Env = @{}; What = 'databases (drainer at the real cadence)' },
  @{ Id = 't5'; Env = @{}; What = 'failure under load (misconfiguration d, sender crash e)' },
  @{ Id = 't7'; Env = @{ RELAY_SOAK_MINUTES = '30' }; What = 'soak, 30 minutes' }
)
foreach ($n in 't8', 't9') {
  if ($registered -match "from\s+'\./$n-") {
    $plan += @{ Id = $n; Env = @{}; What = "suite test $n" }
  }
  else {
    Write-Host "Note: $n is not registered in tests\index.ts, skipping it."
  }
}

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
Write-Host "Repo:    $repo"
Write-Host "Results: $OutDir  (one folder per test)"
Write-Host "Scale:   $Scale"
Write-Host "Tests:   $(($plan | ForEach-Object { $_.Id }) -join ', ')"
Write-Host ""

$rows = @()
$sw = [System.Diagnostics.Stopwatch]::StartNew()
foreach ($t in $plan) {
  $testOut = Join-Path $OutDir $t.Id
  $runArgs = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $runner,
    '-Tests', $t.Id, '-Scale', $Scale, '-OutDir', $testOut)
  if ($DataDir) { $runArgs += @('-DataDir', $DataDir) }
  if ($Workers -gt 0) { $runArgs += @('-Workers', "$Workers") }

  Write-Host "=== $($t.Id): $($t.What) ===" -ForegroundColor Cyan
  $saved = @{}
  foreach ($k in $t.Env.Keys) {
    $saved[$k] = [Environment]::GetEnvironmentVariable($k)
    Set-Item -Path "Env:$k" -Value $t.Env[$k]
  }
  $one = [System.Diagnostics.Stopwatch]::StartNew()
  # A child process: run-scale-suite.ps1 ends with `exit`, and the environment above is inherited.
  & $psExe @runArgs
  $code = $LASTEXITCODE
  $one.Stop()
  foreach ($k in $t.Env.Keys) {
    if ($null -eq $saved[$k]) { Remove-Item -Path "Env:$k" -ErrorAction SilentlyContinue }
    else { Set-Item -Path "Env:$k" -Value $saved[$k] }
  }
  $rows += [pscustomobject]@{ Test = $t.Id; Exit = $code; Time = ('{0:hh\:mm\:ss}' -f $one.Elapsed); Results = $testOut }
  Write-Host ""
}
$sw.Stop()

Write-Host "================ Targeted rerun finished ================" -ForegroundColor Cyan
$rows | Format-Table -AutoSize | Out-String | Write-Host
Write-Host ("Total time: {0:hh\:mm\:ss}" -f $sw.Elapsed)
Write-Host "Results:    $OutDir  (each test folder has SUMMARY.md, <id>.md and <id>.json)"
$failed = @($rows | Where-Object { $_.Exit -ne 0 })
if ($failed.Count -gt 0) {
  Write-Host "FAILED: $(($failed | ForEach-Object { $_.Test }) -join ', ')  (see each folder's SUMMARY.md)" -ForegroundColor Red
  exit 1
}
Write-Host "All tests passed or were skipped." -ForegroundColor Green
exit 0
