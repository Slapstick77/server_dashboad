[CmdletBinding()]param(
  [datetime]$FromDate = '2025-08-13',   # latest date
  [datetime]$ToDate   = '2025-08-01',   # earliest date
  [string]$ReportServerRoot = 'http://c201m580/ReportServer',
  [string]$ReportPath = '/Custom/Production Control/SCHLabor',
  [string]$OutputFolder = (Get-Location).Path,
  [switch]$Overwrite,
  [switch]$Quiet
)
function Write-Info($m){ if(-not $Quiet){ Write-Host "[INFO ] $m" -ForegroundColor Cyan } }
function Write-Warn($m){ if(-not $Quiet){ Write-Warning $m } }

if($ToDate -gt $FromDate){ throw "ToDate ($ToDate) must be earlier than or equal to FromDate ($FromDate)" }

$driver = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) 'Get-SSRSReport.ps1'
if(-not (Test-Path $driver)){ throw "Cannot find Get-SSRSReport.ps1 at $driver" }

for($d = $FromDate.Date; $d -ge $ToDate.Date; $d = $d.AddDays(-1)){
  $tag = $d.ToString('yyyyMMdd')
  $outFile = Join-Path $OutputFolder ("SCHLabor_{0}.csv" -f $tag)
  if( (Test-Path $outFile) -and (-not $Overwrite) ) { Write-Info "Skip existing $tag"; continue }
  $iso = $d.ToString('yyyy-MM-dd')
  Write-Info "Downloading $iso -> $outFile";
  try {
    powershell -NoLogo -NoProfile -ExecutionPolicy Bypass -File $driver -ReportServerRoot $ReportServerRoot -ReportPath $ReportPath -OutputFormat CSV -OutputFile $outFile -Param DateStart=$iso,DateEnd=$iso | Out-Null
    if(Test-Path $outFile){
      $size=(Get-Item $outFile).Length
      Write-Info "Saved $tag ($size bytes)"
    } else { Write-Warn "Failed to produce file for $iso" }
  } catch {
    Write-Warn ('Error ' + $iso + ': ' + $_.Exception.Message)
  }
}
