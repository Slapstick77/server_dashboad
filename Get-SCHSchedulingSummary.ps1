[CmdletBinding()]param(
  [Parameter(Mandatory)] [datetime]$StartDate,
  [Parameter(Mandatory)] [datetime]$EndDate,
  [string]$ReportServerRoot = $env:SSRS_REPORTSERVER_ROOT,
  [string]$ReportPath = '/Custom/Production Control/SCHSchedulingSummaryReport',
  [string]$StartParamName = 'SHIP_DATE_START',
  [string]$EndParamName   = 'SHIP_DATE_END',
  [switch]$OpenAfter,
  [switch]$ListOnly
)
# Optional centralized constants
$constFile = Join-Path $PSScriptRoot 'ReportConstants.ps1'
if(Test-Path $constFile){
  . $constFile
  if($Global:REPORT_CONSTANTS){
    if(-not $PSBoundParameters.ContainsKey('ReportServerRoot') -and $Global:REPORT_CONSTANTS.ReportServerRoot){ $ReportServerRoot = $Global:REPORT_CONSTANTS.ReportServerRoot }
    if(-not $PSBoundParameters.ContainsKey('ReportPath')){ $ReportPath = $Global:REPORT_CONSTANTS.SchedulingSummary.ReportPath }
    if(-not $PSBoundParameters.ContainsKey('StartParamName')){ $StartParamName = $Global:REPORT_CONSTANTS.SchedulingSummary.StartParamName }
    if(-not $PSBoundParameters.ContainsKey('EndParamName')){ $EndParamName = $Global:REPORT_CONSTANTS.SchedulingSummary.EndParamName }
  }
}
function Fail($m){ throw $m }
if(-not $ReportServerRoot){ Fail 'Set -ReportServerRoot or $env:SSRS_REPORTSERVER_ROOT first.' }

if($ListOnly){
  & (Join-Path $PSScriptRoot 'Get-SSRSReport.ps1') -ReportServerRoot $ReportServerRoot -ListFolder (Split-Path $ReportPath -Parent) -Recursive
  return
}
if($EndDate -lt $StartDate){ Fail 'EndDate must be >= StartDate' }

$startStr = $StartDate.ToString('yyyy-MM-dd')
$endStr   = $EndDate.ToString('yyyy-MM-dd')
$outFile  = "SCHSchedulingSummaryReport_${startStr}_${endStr}.csv"

Write-Host "Downloading scheduling summary $startStr to $endStr -> $outFile" -ForegroundColor Cyan

## Note: Do NOT dot-source Get-SSRSReport.ps1 because it has mandatory parameters and will prompt.
## We only need to invoke it directly.

# Build param strings (use separate -Param occurrences)
 $paramHash = @{ $StartParamName = $startStr; $EndParamName = $endStr }
 & (Join-Path $PSScriptRoot 'Get-SSRSReport.ps1') -ReportServerRoot $ReportServerRoot -ReportPath $ReportPath -Format CSV -OutFile $outFile -Parameters $paramHash
if($LASTEXITCODE -and $LASTEXITCODE -ne 0){ Fail "Download failed (exit $LASTEXITCODE)" }
if(Test-Path $outFile){
  Write-Host "Saved $outFile (size: $((Get-Item $outFile).Length) bytes)" -ForegroundColor Green
  if($OpenAfter){ Invoke-Item $outFile }
} else { Fail 'Output file not created.' }
