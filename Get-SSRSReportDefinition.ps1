<#!
.SYNOPSIS
  Download and parse an SSRS report (RDL) to reveal data sources and dataset queries.

.PARAMETER ReportServerRoot
  Base ReportServer endpoint (e.g. http://c201m580/ReportServer)

.PARAMETER ReportPath
  Catalog path (e.g. /Custom/Production Control/SCHLabor)

.PARAMETER OutFile
  Optional path to save raw RDL. If omitted saves beside script with sanitized name.

.PARAMETER Credential
  Optional PSCredential; otherwise uses integrated security.

.OUTPUTS
  Writes summary objects to console and (optionally) saves the RDL file.
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$ReportServerRoot,
  [Parameter(Mandatory=$true)][string]$ReportPath,
  [string]$OutFile,
  [System.Management.Automation.PSCredential]$Credential
)
function Write-Info($m){ Write-Host "[INFO ] $m" -ForegroundColor Cyan }
function Join-Url([string]$a,[string]$b){ ("{0}/{1}" -f $a.TrimEnd('/'), $b.TrimStart('/')) }

if(-not $OutFile){
  $name = (Split-Path $ReportPath -Leaf) -replace '[\\/:*?"<>|]','_'
  $OutFile = Join-Path (Get-Location) ($name + '.rdl')
}

$svc = Join-Url $ReportServerRoot 'ReportService2010.asmx'
$envelope = @"
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetItemDefinition xmlns="http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer">
      <ItemPath>$ReportPath</ItemPath>
    </GetItemDefinition>
  </soap:Body>
</soap:Envelope>
"@
$invokeParams = @{
  Uri = $svc; Method='Post'; ContentType='text/xml; charset=utf-8'; Body=$envelope;
  Headers=@{SOAPAction='http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer/GetItemDefinition'}
}
if($Credential){ $invokeParams.Credential=$Credential } else { $invokeParams.UseDefaultCredentials=$true }
Write-Info "Requesting RDL definition..."
try{ $resp = Invoke-WebRequest @invokeParams -ErrorAction Stop }catch{ Write-Error "GetItemDefinition failed: $($_.Exception.Message)"; exit 1 }
[xml]$soap = $resp.Content
$rdlNode = $soap.SelectSingleNode('//*[local-name()="GetItemDefinitionResult"]')
if(-not $rdlNode){ Write-Error 'No RDL payload found.'; exit 1 }
$rdlXml = [xml]$rdlNode.InnerText
$rdlXml.Save($OutFile)
Write-Info "Saved RDL: $OutFile"

# Namespaces (handle classic vs 2016+)
$nsMgr = New-Object System.Xml.XmlNamespaceManager($rdlXml.NameTable)
$nsMgr.AddNamespace('rdl','http://schemas.microsoft.com/sqlserver/reporting/2008/01/reportdefinition')
$nsMgr.AddNamespace('rdl12','http://schemas.microsoft.com/sqlserver/reporting/2016/01/reportdefinition')

# DataSources section
$dataSources = @()
foreach($ns in 'rdl','rdl12'){
  $nodes = $rdlXml.SelectNodes("//${ns}:DataSources/${ns}:DataSource", $nsMgr)
  if($nodes){
    foreach($ds in $nodes){
      $name = $ds.Name
      $conn = $ds.SelectSingleNode("${ns}:ConnectionProperties", $nsMgr)
      if($conn){
        $connectString = $conn.ConnectString
        $ext = $conn.DataProvider
        # Parse connection string
        $server=$null;$db=$null
        $pairs = $connectString -split ';' | Where-Object { $_ -match '=' }
        foreach($p in $pairs){ $k,$v=$p.Split('=',2); switch -Regex ($k.Trim()){ '^(Data Source|Server|Address|Addr|Network Address)$'{ $server=$v.Trim() } '^(Initial Catalog|Database)$'{ $db=$v.Trim() } } }
        $dataSources += [pscustomobject]@{ Name=$name; Server=$server; Database=$db; Provider=$ext; ConnectionString=$connectString }
      }
    }
  }
}

Write-Info 'Data Sources:'
$dataSources | Format-Table -AutoSize

# Datasets
$dataSets = @()
foreach($ns in 'rdl','rdl12'){
  $nodes = $rdlXml.SelectNodes("//${ns}:DataSets/${ns}:DataSet", $nsMgr)
  if($nodes){
    foreach($ds in $nodes){
      $dsName = $ds.Name
      $query = $ds.SelectSingleNode("${ns}:Query/${ns}:CommandText", $nsMgr)
      $dsRef = $ds.SelectSingleNode("${ns}:Query/${ns}:DataSourceName", $nsMgr)
      $text = $query.InnerText
      $dataSets += [pscustomobject]@{ DataSet=$dsName; DataSourceRef=$dsRef.InnerText; CommandText=$text }
    }
  }
}
Write-Info 'Data Sets (trimmed 200 chars):'
$dataSets | ForEach-Object { [pscustomobject]@{ DataSet=$_.DataSet; DataSourceRef=$_.DataSourceRef; CommandSnippet=( if($_.CommandText.Length -gt 200){ $_.CommandText.Substring(0,200)+'...' } else { $_.CommandText }) } } | Format-Table -AutoSize

Write-Info 'Next: Use Server+Database with Get-SQLData.ps1 and adapt the CommandText (replace @params with real values).'
