[CmdletBinding()]param(
  [Parameter(Mandatory)][datetime]$StartDate,
  [Parameter(Mandatory)][datetime]$EndDate,
  [string]$ReportServerRoot = 'http://c201m580/ReportServer',
  [string]$ReportPath = '/Custom/Production Control/SCHSchedulingSummaryReport',
  [string]$OutFile = 'SCHSchedulingSummaryReport_range.csv',
  [string]$StartParamName = 'SHIP_DATE_START',
  [string]$EndParamName   = 'SHIP_DATE_END',
  [switch]$Force
)
# Optional centralized constants
$constFile = Join-Path $PSScriptRoot 'ReportConstants.ps1'
if(Test-Path $constFile){
  . $constFile
  if($Global:REPORT_CONSTANTS){
    if(-not $PSBoundParameters.ContainsKey('ReportServerRoot')){ $ReportServerRoot = $Global:REPORT_CONSTANTS.ReportServerRoot }
    if(-not $PSBoundParameters.ContainsKey('ReportPath')){ $ReportPath = $Global:REPORT_CONSTANTS.SchedulingSummary.ReportPath }
    if(-not $PSBoundParameters.ContainsKey('StartParamName')){ $StartParamName = $Global:REPORT_CONSTANTS.SchedulingSummary.StartParamName }
    if(-not $PSBoundParameters.ContainsKey('EndParamName')){ $EndParamName = $Global:REPORT_CONSTANTS.SchedulingSummary.EndParamName }
  }
}
function Info($m){ Write-Host "[INFO ] $m" -ForegroundColor Cyan }
function Warn($m){ Write-Warning $m }
function Fail($m){ throw $m }
if($EndDate -lt $StartDate){ Fail 'EndDate must be >= StartDate' }
if((Test-Path $OutFile) -and -not $Force){ Fail "OutFile exists: $OutFile (use -Force)" }
if(Test-Path $OutFile){ Remove-Item $OutFile -Force }

# SOAP GetItemParameters (can't rely on main script because of mandatory param binding when dot-sourcing)
$svc = ($ReportServerRoot.TrimEnd('/') + '/ReportService2010.asmx')
$env = @"
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:rs="http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer">
  <soap:Body>
    <GetItemParameters xmlns="http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer">
      <ItemPath>$ReportPath</ItemPath>
      <HistoryID xsi:nil="true" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" />
      <ForRendering>true</ForRendering>
      <Values />
    </GetItemParameters>
  </soap:Body>
</soap:Envelope>
"@
try {
  $resp = Invoke-WebRequest -Uri $svc -Method Post -ContentType 'text/xml; charset=utf-8' -Headers @{SOAPAction='http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer/GetItemParameters'} -Body $env -UseDefaultCredentials -ErrorAction Stop
  [xml]$xml = $resp.Content
  $nsMgr = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
  $nsMgr.AddNamespace('rs','http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer')
  $pNodes = $xml.SelectNodes('//rs:ReportParameter',$nsMgr)
} catch {
  Warn "Parameter metadata fetch failed: $($_.Exception.Message)"
  $pNodes = @()
}
$paramMeta = @()
foreach($n in $pNodes){
  $defaults = @()
  if($n.DefaultValues -and $n.DefaultValues.Value){ $defaults = @($n.DefaultValues.Value | ForEach-Object { $_.'#text' }) }
  $paramMeta += [pscustomobject]@{ Name=$n.Name; Type=$n.Type; Multi=($n.MultiValue -eq 'true'); Defaults=$defaults }
}
if($paramMeta.Count -gt 0){ Info "Parameters: " + ($paramMeta | ForEach-Object { "${($_.Name)}(${($_.Type)})" } -join ', ') }

$dateParams = $paramMeta | Where-Object { $_.Type -eq 'DateTime' }

# Determine strategy
$singleDayMode = $false
$startName = $null; $endName = $null
# If explicit param names provided (defaults now set to discovered SHIP_DATE_START/SHIP_DATE_END) skip discovery
if($StartParamName -and $EndParamName){
  $startName = $StartParamName; $endName = $EndParamName; Info "Using provided parameter names: $startName / $endName"
} else {
  if($dateParams.Count -ge 2){
    $startName = ($dateParams | Where-Object { $_.Name -match '(?i)^(Start|From|Begin|DateFrom|WeekStart|PeriodStart|SHIP_)' } | Select-Object -First 1).Name
    $endName   = ($dateParams | Where-Object { $_.Name -match '(?i)^(End|To|Finish|DateTo|WeekEnd|PeriodEnd|SHIP_)' } | Select-Object -First 1).Name
    if(-not $startName -or -not $endName){
      $startName = $dateParams[0].Name
      $endName = ($dateParams | Where-Object { $_.Name -ne $startName } | Select-Object -First 1).Name
    }
    Info "Using date parameters: $startName / $endName"
  } elseif($dateParams.Count -eq 1){
    $singleDayMode = $true
    $startName = $dateParams[0].Name
    Info "Single-date parameter detected: $startName (will loop each day)"
  } else {
    Fail 'No date parameters discovered and none supplied.'
  }
}

# Helper to invoke main downloader
function Invoke-Download($paramsHash,$outfile){
  & (Join-Path $PSScriptRoot 'Get-SSRSReport.ps1') -ReportServerRoot $ReportServerRoot -ReportPath $ReportPath -Format CSV -OutFile $outfile -Parameters $paramsHash
}

if(-not $singleDayMode){
  $p = @{ $startName = $StartDate.ToString('yyyy-MM-dd'); $endName = $EndDate.ToString('yyyy-MM-dd') }
  $tmp = "$OutFile.tmp"
  if(Test-Path $tmp){ Remove-Item $tmp -Force }
  Info "Downloading full range via $startName/$endName"
  Invoke-Download $p $tmp
  if(-not (Test-Path $tmp) -or (Get-Item $tmp).Length -lt 50){ Fail 'Download failed or empty.' }
  Move-Item $tmp $OutFile -Force
  Info "Saved $OutFile (size: $((Get-Item $OutFile).Length) bytes)"
} else {
  Info 'Looping daily...'
  $headerWritten=$false
  $cur=$StartDate.Date
  while($cur -le $EndDate.Date){
    $p=@{ $startName = $cur.ToString('yyyy-MM-dd') }
    $dayFile = Join-Path $env:TEMP ("schsched_" + $cur.ToString('yyyyMMdd') + '.csv')
    if(Test-Path $dayFile){ Remove-Item $dayFile -Force }
    Invoke-Download $p $dayFile
    if(Test-Path $dayFile -and (Get-Item $dayFile).Length -gt 30){
      $lines = Get-Content $dayFile -Encoding UTF8
      if(-not $headerWritten){ $lines[0] | Add-Content -Path $OutFile; $headerWritten=$true }
      $lines | Select-Object -Skip 1 | Add-Content -Path $OutFile
      Info "Appended $($cur.ToString('yyyy-MM-dd'))"
    } else { Warn "No data for $($cur.ToString('yyyy-MM-dd'))" }
    $cur = $cur.AddDays(1)
  }
  if(-not (Test-Path $OutFile)){ Fail 'No data collected.' }
  Info "Saved aggregated $OutFile (size: $((Get-Item $OutFile).Length) bytes)"
}
