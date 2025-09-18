<#!
.SYNOPSIS
  Discover the SQL Server and database used by an SSRS report (shared or embedded data source).

.DESCRIPTION
  Calls the SSRS ReportService2010 SOAP API (GetItemDataSources) to retrieve data source definitions
  for a given report path. Parses the connection string to expose DataSource (server/instance) and
  Initial Catalog (database) so you can query the database directly.

.PARAMETER ReportServerRoot
  Base ReportServer endpoint (e.g. http://c201m580/ReportServer).

.PARAMETER ReportPath
  Catalog path of the report (e.g. /Custom/Production Control/SCHLabor).

.PARAMETER Credential
  (Optional) PSCredential for explicit auth; otherwise uses current user (Integrated Security).

.EXAMPLE
  ./Get-SSRSDataSource.ps1 -ReportServerRoot http://c201m580/ReportServer -ReportPath "/Custom/Production Control/SCHLabor"

.OUTPUTS
  PSCustomObject with: ReportPath, Name, ConnectString, DataSourceServer, Database, Extension, IntegratedSecurity, UserName.
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$ReportServerRoot,
  [Parameter(Mandatory=$true)][string]$ReportPath,
  [System.Management.Automation.PSCredential]$Credential
)

function Write-Info($m){ Write-Host "[INFO ] $m" -ForegroundColor Cyan }
function Write-Warn($m){ Write-Warning $m }

function Join-Url([string]$a,[string]$b){ ("{0}/{1}" -f $a.TrimEnd('/'), $b.TrimStart('/')) }

# Build SOAP envelope
$svc = Join-Url $ReportServerRoot 'ReportService2010.asmx'
$envelope = @"
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetItemDataSources xmlns="http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer">
      <ItemPath>$ReportPath</ItemPath>
    </GetItemDataSources>
  </soap:Body>
</soap:Envelope>
"@

$invokeParams = @{
  Uri        = $svc
  Method     = 'Post'
  ContentType= 'text/xml; charset=utf-8'
  Body       = $envelope
  Headers    = @{ SOAPAction='http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer/GetItemDataSources' }
}
if($Credential){ $invokeParams.Credential = $Credential } else { $invokeParams.UseDefaultCredentials = $true }

Write-Info "Requesting data sources for $ReportPath"
try {
  $resp = Invoke-WebRequest @invokeParams -ErrorAction Stop
} catch {
  Write-Error "SOAP request failed: $($_.Exception.Message)"; return
}

[xml]$xml = $resp.Content
$nsMgr = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
$nsMgr.AddNamespace('rs','http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer')
$dsNodes = $xml.SelectNodes('//rs:DataSource',$nsMgr)
if(-not $dsNodes -or $dsNodes.Count -eq 0){ Write-Warn 'No data sources found (check permissions or path).'; return }

$results = foreach($node in $dsNodes){
  $name = $node.Name
  $def = $node.SelectSingleNode('.//rs:DataSourceDefinition',$nsMgr)
  $connect = $def.ConnectString
  $extension = $def.Extension
  $intSec = ($def.IntegratedSecurity -eq 'true')
  $user = $def.UserName
  # Parse connection string
  $server = $null; $db = $null
  if($connect){
    $pairs = $connect -split ';' | Where-Object { $_ -match '=' }
    foreach($p in $pairs){
      $k,$v = $p.Split('=',2)
      switch -Regex ($k.Trim()){ '^(Data Source|Server|Address|Addr|Network Address)$' { $server = $v.Trim() } '^(Initial Catalog|Database)$' { $db = $v.Trim() } }
    }
  }
  [pscustomobject]@{
    ReportPath = $ReportPath
    Name = $name
    ConnectString = $connect
    DataSourceServer = $server
    Database = $db
    Extension = $extension
    IntegratedSecurity = $intSec
    UserName = $user
  }
}

$results | Format-Table -AutoSize

Write-Info 'Tip: Use DataSourceServer (instance) and Database with Get-SQLData.ps1 to query directly.'
