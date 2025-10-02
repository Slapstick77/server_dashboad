<#!
.SYNOPSIS
  Incremental backward backfill for SCHLabor: downloads one missing prior day each run.

.DESCRIPTION
  Designed for use with Windows Task Scheduler (run every N minutes/hours). It:
    1. Scans existing SCHLabor_yyyyMMdd.csv files in the working directory.
    2. If none exist, starts at -StartDate (default 2025-08-21).
    3. Otherwise finds the EARLIEST date already downloaded and targets previous day.
    4. Skips if a STOP file exists (STOP_BACKFILL.txt) so you can halt cleanly.
    5. Honors -MinDate (won't go earlier).

  This avoids needing a never-ending PowerShell session; the scheduler re-invokes until you drop STOP_BACKFILL.txt
  or disable the task. Works if the laptop lid is closed ONLY if the machine does not sleep (adjust power settings) or better run on an always-on server.

.PARAMETER ReportServerRoot
  Base ReportServer endpoint.
.PARAMETER ReportPath
  SCHLabor report path.
.PARAMETER StartDate
  First (latest) date to begin if no files exist yet.
.PARAMETER MinDate
  Earliest date allowed (inclusive). If target would be earlier, script exits.
.PARAMETER OutputFolder
  Folder to store CSV files (default current directory).
.PARAMETER Quiet
  Suppress info output (good for scheduled task logs).
#>
[CmdletBinding()]param(
  [string]$ReportServerRoot = 'http://c201m580/ReportServer',
  [string]$ReportPath = '/Custom/Production Control/SCHLabor',
  [datetime]$StartDate = '2025-08-21',
  [datetime]$MinDate,
  [string]$OutputFolder = (Get-Location).Path,
  [switch]$Quiet,
  [switch]$Continuous,            # Loop backward automatically until MinDate or STOP file present
  [int]$SleepSeconds = 5,         # Delay between iterations when -Continuous
  [int]$MaxIterations = 0         # Safety cap when -Continuous (0 = unlimited)
)

function Write-Info($m){ if(-not $Quiet){ Write-Host "[INFO ] $m" -ForegroundColor Cyan } }
function Write-Warn($m){ if(-not $Quiet){ Write-Warning $m } }
function Write-Err($m){ Write-Error $m }

$powershellScript = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) 'Get-SSRSReport.ps1'
if(-not (Test-Path $powershellScript)){ Write-Err "Cannot locate Get-SSRSReport.ps1 at $powershellScript"; exit 1 }

$iteration = 0
do {
  $stopFile = Join-Path $OutputFolder 'STOP_BACKFILL.txt'
  if(Test-Path $stopFile){ Write-Info 'STOP_BACKFILL.txt present; exiting.'; break }

  $existing = Get-ChildItem -Path $OutputFolder -Filter 'SCHLabor_*.csv' -File | Where-Object { $_.BaseName -match '^SCHLabor_\d{8}$' }

  if(-not $existing){
    $targetDate = $StartDate.Date
    Write-Info "No existing files. Starting at $($targetDate.ToString('yyyy-MM-dd'))"
  } else {
    $dates = $existing | ForEach-Object {
      if($_.BaseName -match 'SCHLabor_(\d{4})(\d{2})(\d{2})'){ [datetime]::ParseExact($Matches[1]+$Matches[2]+$Matches[3],'yyyyMMdd',$null) }
    } | Sort-Object
    $earliest = $dates[0]
    $targetDate = $earliest.AddDays(-1)
    Write-Info "Earliest existing: $($earliest.ToString('yyyy-MM-dd')); next target back: $($targetDate.ToString('yyyy-MM-dd'))"
  }

  if($MinDate){
    if($targetDate -lt $MinDate.Date){ Write-Info "Target $($targetDate.ToString('yyyy-MM-dd')) earlier than MinDate $($MinDate.ToString('yyyy-MM-dd')); exiting."; break }
  }

  if($targetDate -gt (Get-Date).Date){ Write-Warn 'Target date is in the future; exiting.'; break }

  $outFile = Join-Path $OutputFolder ("SCHLabor_{0}.csv" -f $targetDate.ToString('yyyyMMdd'))
  if(Test-Path $outFile){ Write-Info "Already have $outFile"; if(-not $Continuous){ break } else { Start-Sleep -Seconds $SleepSeconds; continue } }

  $dStr = $targetDate.ToString('yyyy-MM-dd')
  Write-Info "Downloading SCHLabor for $dStr -> $outFile"
  try {
    powershell -NoLogo -NoProfile -ExecutionPolicy Bypass -File $powershellScript -ReportServerRoot $ReportServerRoot -ReportPath $ReportPath -OutputFormat CSV -OutputFile $outFile -Param DateStart=$dStr,DateEnd=$dStr | Out-Null
    if(Test-Path $outFile){
      $size = (Get-Item $outFile).Length
      Write-Info "Saved $outFile ($size bytes)"
    } else { Write-Warn 'Download command completed but file missing.' }
  } catch {
    Write-Warn ('Download failed: ' + $_.Exception.Message)
    if(-not $Continuous){ break } else { Start-Sleep -Seconds $SleepSeconds; continue }
  }

  $iteration++
  if($Continuous){
    if($MaxIterations -gt 0 -and $iteration -ge $MaxIterations){ Write-Info "Reached MaxIterations $MaxIterations; exiting."; break }
    Start-Sleep -Seconds $SleepSeconds
  }
} while($Continuous)
