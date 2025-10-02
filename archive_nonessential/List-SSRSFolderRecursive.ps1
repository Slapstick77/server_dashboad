[CmdletBinding()]param(
  [Parameter(Mandatory)][string]$ReportServerRoot,
  [Parameter(Mandatory)][string]$Folder
)
# Simple SOAP ListChildren recursive
function SoapList($path){
  $svc = ($ReportServerRoot.TrimEnd('/') + '/ReportService2010.asmx')
  $envelope = @"
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:rs="http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer">
  <soap:Body>
    <ListChildren xmlns="http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer">
      <ItemPath>$path</ItemPath>
      <Recursive>true</Recursive>
    </ListChildren>
  </soap:Body>
</soap:Envelope>
"@
  $resp = Invoke-WebRequest -Uri $svc -Method Post -ContentType 'text/xml; charset=utf-8' -Body $envelope -Headers @{SOAPAction='http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer/ListChildren'} -UseDefaultCredentials
  [xml]$xml = $resp.Content
  $nsMgr = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
  $nsMgr.AddNamespace('rs','http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer')
  $nodes = $xml.SelectNodes('//rs:CatalogItem',$nsMgr)
  $nodes | ForEach-Object { [pscustomobject]@{ Name=$_.Name; Path=$_.Path; Type=$_.TypeName } }
}
SoapList $Folder | Sort-Object Path | Format-Table -AutoSize
