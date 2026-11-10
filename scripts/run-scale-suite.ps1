<#
.SYNOPSIS
  Runs the Relay scalability suite (loadtest/SUITE.md) natively on Windows. One command, 4-5 hours.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts\run-scale-suite.ps1
  powershell -ExecutionPolicy Bypass -File scripts\run-scale-suite.ps1 -Tests t1,t3 -Scale smoke

.PARAMETER Tests    Comma-separated test ids (default: all, t1..t7).
.PARAMETER Scale    full (default) or smoke (tiny sizes, about a minute, to check the setup).
.PARAMETER Workers  Worker processes per test (default: logical CPUs - 2).
.PARAMETER OutDir   Results folder (default: <repo parent>\results\suite-<timestamp>, on the repo's drive).
.PARAMETER DataDir  Scratch data folder for big generated files (default: <drive>\relay-suite-data).
#>
[CmdletBinding()]
param(
  [string]$Tests = '',
  [ValidateSet('full', 'smoke')][string]$Scale = 'full',
  [int]$Workers = 0,
  [string]$OutDir = '',
  [string]$DataDir = ''
)

$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$drive = Split-Path -Qualifier $repo            # e.g. D:
$stamp = Get-Date -Format 'yyyyMMdd-HHmm'
if (-not $DataDir) { $DataDir = Join-Path $drive 'relay-suite-data' }
if (-not $OutDir) { $OutDir = Join-Path (Join-Path (Split-Path -Parent $repo) 'results') "suite-$stamp" }
$npmCache = Join-Path $drive 'npm-cache'

function Fail($msg) { Write-Host "ERROR: $msg" -ForegroundColor Red; exit 1 }

# ---------- prerequisites ----------
$nodeVer = $null
try { $nodeVer = (& node --version) 2>$null } catch { }
if (-not $nodeVer) {
  Fail "Node.js is not installed. Install it with:  winget install OpenJS.NodeJS.LTS   (then open a new terminal)"
}
$nodeMajor = [int](($nodeVer -replace '^v', '') -split '\.')[0]
if ($nodeMajor -lt 22) {
  Fail "Node $nodeVer is too old, need >= 22. Upgrade with:  winget install OpenJS.NodeJS.LTS"
}
$gitOk = $null -ne (Get-Command git -ErrorAction SilentlyContinue)
if (-not $gitOk) { Fail "Git is not installed. Install it with:  winget install Git.Git" }

$dockerReady = $false
if (Get-Command docker -ErrorAction SilentlyContinue) {
  $prev = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
  & docker info *> $null
  if ($LASTEXITCODE -eq 0) { $dockerReady = $true }
  $ErrorActionPreference = $prev
}
$needsDocker = (-not $Tests) -or ($Tests -match '(^|,)\s*t4\s*(,|$)')
if ($needsDocker -and -not $dockerReady) {
  Write-Host "WARNING: Docker Desktop is not installed or not running. Test t4 (real databases) will be SKIPPED." -ForegroundColor Yellow
  Write-Host "         Start Docker Desktop and wait until it says 'running' to include t4, or continue without it."
  $answer = Read-Host "Continue without Docker? [y/N]"
  if ($answer -notmatch '^(y|yes)$') { Fail "Aborted. Start Docker Desktop and run this script again." }
}

# ---------- environment (this session only) ----------
New-Item -ItemType Directory -Force -Path $npmCache, $DataDir, $OutDir | Out-Null
$env:npm_config_cache = $npmCache
$env:RELAY_SUITE = '1'

# ---------- keep the PC awake while the suite runs ----------
if (-not ('Relay.Awake' -as [type])) {
  Add-Type -Namespace Relay -Name Awake -MemberDefinition @'
[System.Runtime.InteropServices.DllImport("kernel32.dll")]
public static extern uint SetThreadExecutionState(uint esFlags);
'@
}
$ES_CONTINUOUS = [uint32]2147483648
$ES_SYSTEM_REQUIRED = [uint32]1
$null = [Relay.Awake]::SetThreadExecutionState($ES_CONTINUOUS -bor $ES_SYSTEM_REQUIRED)

$exit = 1
$sw = [System.Diagnostics.Stopwatch]::StartNew()
Start-Transcript -Path (Join-Path $OutDir 'suite-log.txt') -Force | Out-Null
try {
  Set-Location $repo
  Write-Host "Repo:    $repo"
  Write-Host "Node:    $nodeVer   npm cache: $npmCache"
  Write-Host "Data:    $DataDir"
  Write-Host "Results: $OutDir"
  Write-Host "Docker:  $(if ($dockerReady) { 'running' } else { 'not available (t4 will be skipped)' })"

  $ErrorActionPreference = 'Continue'    # native stderr output must not abort under Windows PowerShell 5.1
  if (-not (Test-Path (Join-Path $repo 'node_modules'))) {
    Write-Host "node_modules missing: running npm ci"
    & npm ci --no-audit --no-fund
    if ($LASTEXITCODE -ne 0) { throw "npm ci failed (exit $LASTEXITCODE)" }
  }

  $suiteArgs = @('--scale', $Scale, '--out', $OutDir, '--data', $DataDir)
  if ($Tests) { $suiteArgs += @('--tests', ($Tests -replace '\s', '')) }
  if ($Workers -gt 0) { $suiteArgs += @('--workers', "$Workers") }

  # Windows PowerShell 5.1 transcripts miss native program output, so tee it into the log too.
  & npm run suite --workspace loadtest -- @suiteArgs 2>&1 | ForEach-Object { "$_" } | Tee-Object -FilePath (Join-Path $OutDir 'suite-output.txt') -Append
  $exit = $LASTEXITCODE
}
catch {
  Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
  $exit = 1
}
finally {
  $null = [Relay.Awake]::SetThreadExecutionState($ES_CONTINUOUS)   # allow sleep again
  $sw.Stop()
  Write-Host ""
  Write-Host ("Total time: {0:hh\:mm\:ss}" -f $sw.Elapsed)
  Write-Host "Results:    $OutDir  (open SUMMARY.md)"
  Write-Host "Log:        $(Join-Path $OutDir 'suite-output.txt')"
  Write-Host "Exit code:  $exit  ($(if ($exit -eq 0) { 'all tests passed or were skipped' } else { 'at least one test FAILED, see SUMMARY.md' }))"
  try { Stop-Transcript | Out-Null } catch { }
}
exit $exit
