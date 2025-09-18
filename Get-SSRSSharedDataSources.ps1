<#!
.SYNOPSIS
  List shared SSRS data sources in a folder (default /Data Sources) and extract server & database.

.DESCRIPTION
  Uses ReportService2010 SOAP ListChildren + GetItemDefinition. For each DataSource item (.rds),
  parses the definition to output connection info you can use for direct SQL queries.

.PARAMETER ReportServerRoot
  Base ReportServer endpoint, e.g. http://c201m580/ReportServer

.PARAMETER FolderPath
  Folder containing shared data sources. Default: /Data Sources

.PARAMETER Credential
  Optional PSCredential; if omitted current Windows credentials are used.

.EXAMPLE
  ./Get-SSRSSharedDataSources.ps1 -ReportServerRoot http://c201m580/ReportServer

.EXAMPLE
  $cred = Get-Credential
  ./Get-SSRSSharedDataSources.ps1 -ReportServerRoot http://c201m580/ReportServer -Credential $cred
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$ReportServerRoot,
  [string]$FolderPath = '/Data Sources',
  [switch]$Recursive,
  [System.Management.Automation.PSCredential]$Credential
)
function Write-Info($m){ Write-Host "[INFO ] $m" -ForegroundColor Cyan }
function Write-Warn($m){ Write-Warning $m }
function Join-Url([string]$a,[string]$b){ ("{0}/{1}" -f $a.TrimEnd('/'), $b.TrimStart('/')) }

$svc = Join-Url $ReportServerRoot 'ReportService2010.asmx'

# SOAP helper
function Invoke-Soap([string]$action,[string]$body){
  $envelope = @"
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    $body
  </soap:Body>
</soap:Envelope>
"@
  $p = @{ Uri=$svc; Method='Post'; ContentType='text/xml; charset=utf-8'; Body=$envelope; Headers=@{SOAPAction=$action} }
  if($Credential){ $p.Credential=$Credential } else { $p.UseDefaultCredentials=$true }
  try { Invoke-WebRequest @p -ErrorAction Stop } catch { throw "SOAP $action failed: $($_.Exception.Message)" }
}

Write-Info "Listing folder: $FolderPath (Recursive=$($Recursive.IsPresent))"
$listBody = @"
<ListChildren xmlns="http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer">
  <ItemPath>$FolderPath</ItemPath>
  <Recursive>$($Recursive.IsPresent.ToString().ToLower())</Recursive>
</ListChildren>
"@
try { $resp = Invoke-Soap 'http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer/ListChildren' $listBody } catch { Write-Error $_; exit 1 }
[xml]$xml = $resp.Content
$nsMgr = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
$nsMgr.AddNamespace('rs','http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer')
$items = $xml.SelectNodes('//rs:CatalogItem',$nsMgr)
if(-not $items){ Write-Warn 'No items returned.'; exit }
$dataSources = $items | Where-Object { $_.TypeName -eq 'DataSource' }
if(-not $dataSources){ Write-Warn 'No DataSource items found.'; exit }

$result = @()
foreach($ds in $dataSources){
  $path = $ds.Path
  $name = $ds.Name
  Write-Info "Fetching definition: $path"
  $body = @"
<GetItemDefinition xmlns="http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer">
  <ItemPath>$path</ItemPath>
</GetItemDefinition>
"@
  try { $defResp = Invoke-Soap 'http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer/GetItemDefinition' $body } catch { Write-Warn $_; continue }
  [xml]$soap = $defResp.Content
  $defNode = $soap.SelectSingleNode('//*[local-name()="GetItemDefinitionResult"]')
  if(-not $defNode){ Write-Warn "No definition node for $path"; continue }
  [xml]$defXml = $defNode.InnerText
  $connProps = $defXml.SelectSingleNode('//*[local-name()="ConnectionProperties"]')
  if(-not $connProps){ Write-Warn "No ConnectionProperties for $path"; continue }
  $connectString = $connProps.ConnectString
  $provider = $connProps.DataProvider
  $intSec = ($connProps.IntegratedSecurity -eq 'true')
  $server = $null; $db = $null
  if($connectString){
    $pairs = $connectString -split ';' | Where-Object { $_ -match '=' }
    foreach($p in $pairs){
      $k,$v = $p.Split('=',2)
      switch -Regex ($k.Trim()){
        '^(Data Source|Server|Address|Addr|Network Address)$' { $server = $v.Trim() }
        '^(Initial Catalog|Database)$' { $db = $v.Trim() }
      }
    }
  }
  $result += [pscustomobject]@{
    Name=$name; Path=$path; Server=$server; Database=$db; Provider=$provider; IntegratedSecurity=$intSec; ConnectionString=$connectString
  }
}

if($result.Count -gt 0){ $result | Format-Table -AutoSize }
Write-Info 'Pick the Server & Database that match the SCHLabor data; then build a direct SQL query.'
