param(
  [string]$ReportServerRoot = 'http://c201m580/ReportServer',
  [string]$ReportPath = '/Custom/Production Control/SCHLabor',
  [datetime]$StartDate = '2025-08-21',
  [datetime]$EndDate = (Get-Date).Date,  # inclusive
  [string]$StartParamName = 'DateStart',
  [string]$EndParamName   = 'DateEnd'
)
# Optional centralized constants
$constFile = Join-Path $PSScriptRoot 'ReportConstants.ps1'
if(Test-Path $constFile){
  . $constFile
  if($Global:REPORT_CONSTANTS){
    if(-not $PSBoundParameters.ContainsKey('ReportServerRoot')){ $ReportServerRoot = $Global:REPORT_CONSTANTS.ReportServerRoot }
    if(-not $PSBoundParameters.ContainsKey('ReportPath')){ $ReportPath = $Global:REPORT_CONSTANTS.Labor.ReportPath }
    if(-not $PSBoundParameters.ContainsKey('StartParamName')){ $StartParamName = $Global:REPORT_CONSTANTS.Labor.StartParamName }
    if(-not $PSBoundParameters.ContainsKey('EndParamName')){ $EndParamName = $Global:REPORT_CONSTANTS.Labor.EndParamName }
  }
}

Write-Host "[INFO ] Downloading SCHLabor daily CSV from $($StartDate.ToString('yyyy-MM-dd')) to $($EndDate.ToString('yyyy-MM-dd'))" -ForegroundColor Cyan
if($EndDate -lt $StartDate){ Write-Error 'EndDate earlier than StartDate'; exit 1 }

$script = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) 'Get-SSRSReport.ps1'
if(-not (Test-Path $script)){ Write-Error "Cannot find Get-SSRSReport.ps1 at $script"; exit 1 }

$cur = $StartDate.Date
while($cur -le $EndDate){
  $d = $cur.ToString('yyyy-MM-dd')
  $outName = "SCHLabor_$($cur.ToString('yyyyMMdd')).csv"
  Write-Host "[INFO ] $d -> $outName" -ForegroundColor Green
  try {
  powershell -NoLogo -NoProfile -ExecutionPolicy Bypass -File $script -ReportServerRoot $ReportServerRoot -ReportPath $ReportPath -OutputFormat CSV -OutputFile $outName -Param "$StartParamName=$d,$EndParamName=$d" | Out-Null
    if(Test-Path $outName){ Write-Host "[OK   ] Saved $outName (Size: $((Get-Item $outName).Length) bytes)" -ForegroundColor Yellow }
  } catch {
    Write-Warning "Failed $d : $($_.Exception.Message)"
  }
  $cur = $cur.AddDays(1)
}

Write-Host '[DONE ] Complete.' -ForegroundColor Cyan
