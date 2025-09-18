<#!
.SYNOPSIS
  Continuously backfill daily SCHLabor CSV files going backwards in time until you stop (Ctrl+C).

.DESCRIPTION
  Starts at a given -StartDate (default 2025-08-21) and for each day:
    - Renders SCHLabor for that date (DateStart=DateEnd=the day)
    - Saves to SCHLabor_yyyyMMdd.csv (skips if already exists unless -Force)
    - Decrements date and repeats
  Stops only when you cancel (Ctrl+C) or optionally when -StopDate is reached.

.PARAMETER ReportServerRoot
  Base ReportServer endpoint.

.PARAMETER ReportPath
  Report catalog path.

.PARAMETER StartDate
  First (latest) date to pull (will go backward from here). Not today per your requirement.

.PARAMETER StopDate
  Optional earliest date to stop (inclusive). If omitted runs until you cancel.

.PARAMETER DelaySeconds
  Optional pause between requests (default 0) to reduce server load.

.PARAMETER Force
  Re-download even if the target CSV already exists.
#>
[CmdletBinding()]param(
  [string]$ReportServerRoot = 'http://c201m580/ReportServer',
  [string]$ReportPath = '/Custom/Production Control/SCHLabor',
  [datetime]$StartDate = '2025-08-21',
  [datetime]$StopDate,
  [int]$DelaySeconds = 0,
  [switch]$Force
)

function Write-Info($m){ Write-Host "[INFO ] $m" -ForegroundColor Cyan }
function Write-Warn($m){ Write-Warning $m }
function Write-Err($m){ Write-Error $m }

$script = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) 'Get-SSRSReport.ps1'
if(-not (Test-Path $script)){ Write-Err "Get-SSRSReport.ps1 not found at $script"; exit 1 }

$current = $StartDate.Date
if($StopDate){ $StopDate = $StopDate.Date }

Write-Info "Backfill starting at $($current.ToString('yyyy-MM-dd')) going backward. Press Ctrl+C to stop."

try {
  while($true){
    if($StopDate -and $current -lt $StopDate){ Write-Info "Reached StopDate $($StopDate.ToString('yyyy-MM-dd')). Exiting."; break }
    $tag = $current.ToString('yyyyMMdd')
    $outFile = "SCHLabor_$tag.csv"
    if((Test-Path $outFile) -and -not $Force){
      Write-Info "Skip existing $outFile"
    } else {
      $dStr = $current.ToString('yyyy-MM-dd')
      Write-Info "Fetching $dStr -> $outFile"
      try {
        powershell -NoLogo -NoProfile -ExecutionPolicy Bypass -File $script -ReportServerRoot $ReportServerRoot -ReportPath $ReportPath -OutputFormat CSV -OutputFile $outFile -Param DateStart=$dStr,DateEnd=$dStr | Out-Null
        if(Test-Path $outFile){
          $size = (Get-Item $outFile).Length
          Write-Info "Saved $outFile ($size bytes)"
        } else { Write-Warn "No file created for $dStr" }
      } catch {
  $errMsg = $_.Exception.Message
  Write-Warn ('Failed ' + $dStr + ': ' + $errMsg)
      }
    }
    $current = $current.AddDays(-1)
    if($DelaySeconds -gt 0){ Start-Sleep -Seconds $DelaySeconds }
  }
} catch {
  Write-Warn "Stopped by user or error: $($_.Exception.Message)"
}

Write-Info 'Done.'
