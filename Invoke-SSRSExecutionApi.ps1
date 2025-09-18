<#!
.SYNOPSIS
  Render an SSRS report via the ReportExecution2005 SOAP API (more direct parameter handling).

.PARAMETER ReportServerRoot
  Base ReportServer endpoint (e.g. http://c201m580/ReportServer)

.PARAMETER ReportPath
  Catalog path (e.g. /Custom/Production Control/SCHLabor)

.PARAMETER OutputFormat
  PDF, EXCEL, WORDOPENXML, CSV, etc. Default PDF.

.PARAMETER Param
  One or more Name=Value pairs for report parameters.

.PARAMETER OutputFile
  Destination file path. Auto-generated if omitted.

.PARAMETER Credential
  Optional PSCredential for explicit auth.
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$ReportServerRoot,
  [Parameter(Mandatory=$true)][string]$ReportPath,
  [ValidateSet('PDF','EXCEL','WORDOPENXML','WORD','EXCELOPENXML','CSV','XML','MHTML','IMAGE')][string]$OutputFormat='PDF',
  [string[]]$Param,
  [string]$OutputFile,
  [System.Management.Automation.PSCredential]$Credential
)
function Write-Info($m){ Write-Host "[INFO ] $m" -ForegroundColor Cyan }
if(-not $OutputFile){
  $name = (Split-Path $ReportPath -Leaf) -replace '[\\/:*?"<>|]','_'
  $ext = switch($OutputFormat){ 'PDF'{'.pdf'}'EXCEL'{'.xls'}'EXCELOPENXML'{'.xlsx'}'WORDOPENXML'{'.docx'}'WORD'{'.doc'}'CSV'{'.csv'} default{'.dat'} }
  $OutputFile = Join-Path (Get-Location) ($name+$ext)
}
$wsdl = ("{0}/ReportExecution2005.asmx?wsdl" -f $ReportServerRoot.TrimEnd('/'))
Write-Info "Connecting to Execution API: $wsdl"
try {
  if($Credential){ $proxy = New-WebServiceProxy -Uri $wsdl -UseDefaultCredential:$false -Credential $Credential -Namespace ReportExecution2005 -Class ReportExec }
  else { $proxy = New-WebServiceProxy -Uri $wsdl -UseDefaultCredential -Namespace ReportExecution2005 -Class ReportExec }
} catch { throw "Failed to create web service proxy: $($_.Exception.Message)" }

Write-Info "LoadReport: $ReportPath"
$execInfo = $proxy.LoadReport($ReportPath, $null)
$parameters = @()
if($Param){
  foreach($p in $Param){
    if($p -notmatch '='){ Write-Warning "Skipping invalid param '$p'"; continue }
    $n,$v = $p.Split('=',2)
    $pv = New-Object ReportExecution2005.ParameterValue
    $pv.Name = $n; $pv.Value = $v
    $parameters += $pv
  }
  if($parameters.Count -gt 0){
    $display = ($parameters | ForEach-Object { "{0}={1}" -f $_.Name,$_.Value }) -join ', '
    Write-Info "Setting parameters: $display"
    $null = $proxy.SetExecutionParameters($parameters, 'en-US')
  }
}

Write-Info "Rendering $OutputFormat"
$mime=[ref]'';$enc=[ref]'';$ext=[ref]''
# Prepare strongly typed arrays for warnings and stream IDs after call
try { $result = $proxy.Render($OutputFormat,$null,[ref]$ext,[ref]$mime,[ref]$enc,[ref]([ReportExecution2005.Warning[]]$null),[ref]([string[]]$null)) }
catch { throw "Render failed: $($_.Exception.Message)" }
[IO.File]::WriteAllBytes($OutputFile,$result)
Write-Info "Saved: $OutputFile (Size: $((Get-Item $OutputFile).Length) bytes)"
