<#!
.SYNOPSIS
    Download (render) a SQL Server Reporting Services (SSRS) report to a local file automatically.

.DESCRIPTION
    Uses the SSRS "/ReportServer" endpoint (URL access) or the REST API (if available) to:
      - List reports in a folder
      - Download a specific report in a chosen format (PDF, EXCEL, WORD, CSV, XML, JSON, etc.)
    Supports Windows Integrated Security (default) or explicit Network Credentials.

.PARAMETER ReportServerRoot
    Base URL to the SSRS ReportServer endpoint (NOT the /Reports portal). Example: http://c201m580/ReportServer

.PARAMETER ReportPath
    Full catalog path to the report, e.g. /Custom/Production Control/My Daily Production Report

.PARAMETER OutputFormat
    Target render format. Common: PDF, EXCEL, WORDOPENXML, CSV. Default: PDF.

.PARAMETER OutputFile
    Destination file path. If omitted, will auto-generate in current directory based on report name + extension.

.PARAMETER ListFolder
    If provided, lists items (reports & folders) under that folder path instead of downloading.

.PARAMETER UseRest
    Force using REST API (if supported) to render. By default URL access is used because it's widely compatible.

.PARAMETER Credential
    PSCredential for explicit auth. If omitted, current user (Integrated) credentials are used.

.EXAMPLE
    # List reports in folder
    .\Get-SSRSReport.ps1 -ReportServerRoot http://c201m580/ReportServer -ListFolder "/Custom/Production Control"

.EXAMPLE
    # Download a report as PDF automatically naming the file
    .\Get-SSRSReport.ps1 -ReportServerRoot http://c201m580/ReportServer -ReportPath "/Custom/Production Control/Daily Output" -OutputFormat PDF

.EXAMPLE
    # Download with explicit credentials and custom output path
    $cred = Get-Credential
    .\Get-SSRSReport.ps1 -ReportServerRoot http://c201m580/ReportServer -ReportPath "/Custom/Production Control/Daily Output" -OutputFormat EXCEL -OutputFile C:\Reports\DailyOutput.xlsx -Credential $cred

.NOTES
    Author: (auto-generated)
    Schedule via Windows Task Scheduler by invoking: powershell.exe -File "C:\Path\Get-SSRSReport.ps1" <params>

#>
[CmdletBinding(DefaultParameterSetName='Download')]
param(
    # ReportServerRoot can now come from env var SSRS_REPORTSERVER_ROOT if not passed explicitly
    [Parameter(Position=0)]
    [string]$ReportServerRoot = $env:SSRS_REPORTSERVER_ROOT,

    [Parameter(Mandatory=$true, ParameterSetName='Download')]
    [string]$ReportPath,

    [Parameter(ParameterSetName='Download')]
    [Alias('Format')]
    [ValidateSet('PDF','EXCEL','WORD','WORDOPENXML','EXCELOPENXML','CSV','XML','MHTML','IMAGE','JSON','ATOM','HTML4.0','HTML5')]
    [string]$OutputFormat = 'PDF',

    [Parameter(ParameterSetName='Download')]
    [Alias('OutFile')]
    [string]$OutputFile,

    # Convenience: -Date 2025-08-23 (maps to single date param, or Start/End same day)
    [Parameter(ParameterSetName='Download')]
    [Alias('Date')]
    [string]$SingleDate,

    # Optional explicit report parameters (name/value hashtable) for rendering
    [Parameter(ParameterSetName='Download')]
    [hashtable]$Parameters,

    # Easier CLI: one or more Name=Value strings, e.g. -Param DateStart=2025-08-21 -Param DateEnd=2025-08-21
    [Parameter(ParameterSetName='Download')]
    [string[]]$Param,

    [Parameter(Mandatory=$true, ParameterSetName='List')]
    [string]$ListFolder,

    [Parameter(ParameterSetName='List')]
    [switch]$Recursive,

    [switch]$UseRest,

    [System.Management.Automation.PSCredential]$Credential
)

    # Validate we have a server root one way or another
    if(-not $ReportServerRoot -or $ReportServerRoot.Trim() -eq ''){
        throw "ReportServerRoot not supplied and environment variable SSRS_REPORTSERVER_ROOT not set. Provide -ReportServerRoot or set SSRS_REPORTSERVER_ROOT."
    }

function Write-Info($msg){ Write-Host "[INFO ] $msg" -ForegroundColor Cyan }
function Write-Warn($msg){ Write-Warning $msg }
function Write-Err ($msg){ Write-Error $msg }

function Join-Url([string]$a,[string]$b){
    return ("{0}/{1}" -f $a.TrimEnd('/'), $b.TrimStart('/'))
}

function Encode-Path([string]$p){
    # SSRS URL access wants spaces encoded as %20 but preserve forward slashes as path separators
    return ($p -split '/' | ForEach-Object { [uri]::EscapeDataString($_) }) -join '/'
}

function Get-ExtensionForFormat($fmt){
    switch($fmt){
        'PDF' {'.pdf'}
        'EXCEL' {'.xls'}
        'EXCELOPENXML' {'.xlsx'}
        'WORD' {'.doc'}
        'WORDOPENXML' {'.docx'}
        'CSV' {'.csv'}
        'XML' {'.xml'}
        'MHTML' {'.mhtml'}
        'IMAGE' {'.tif'}
        'JSON' {'.json'}
        'ATOM' {'.atom'}
        default {'.dat'}
    }
}

function Get-WebClientParams(){
    if($Credential){ return @{ Credential = $Credential } }
    else { return @{ UseDefaultCredentials = $true } }
}

function List-SSRSFolderUrlAccess([string]$folder){
    # Fallback to SOAP or REST would give richer metadata; simple approach uses REST if available
    if(Test-SSRSRestAvailable){
        return List-SSRSFolderRest $folder
    } else {
        Write-Warn 'REST API not available; attempting SOAP ListChildren.'
        return List-SSRSFolderSoap $folder
    }
}

function Test-SSRSRestAvailable(){
    $restUrl = Join-Url $ReportServerRoot '../Reports/api/v2.0' # derive portal relative
    $ping = Join-Url $restUrl 'Folders(Path=/)'
    try {
        $auth = Get-WebClientParams
        $resp = Invoke-RestMethod -Uri $ping -Method Get -ErrorAction Stop @auth
        return $true
    } catch { return $false }
}

function List-SSRSFolderRest([string]$folder){
    $restBase = Join-Url $ReportServerRoot '../Reports/api/v2.0'
    $encoded = [uri]::EscapeDataString($folder)
    $url = Join-Url $restBase "CatalogItems?path=$encoded"
    $auth = Get-WebClientParams
    $items = Invoke-RestMethod -Uri $url -Method Get @auth
    $items | Select-Object Name,Path,Type,CreatedDate,ModifiedDate
}

function List-SSRSFolderSoap([string]$folder){
    $svc = Join-Url $ReportServerRoot 'ReportService2010.asmx'
    $envelope = @"
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <ListChildren xmlns="http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer">
      <ItemPath>$folder</ItemPath>
      <Recursive>false</Recursive>
    </ListChildren>
  </soap:Body>
</soap:Envelope>
"@
    $auth = Get-WebClientParams
    $resp = Invoke-WebRequest -Uri $svc -Method Post -ContentType 'text/xml; charset=utf-8' -Body $envelope -Headers @{SOAPAction='http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer/ListChildren'} @auth
    [xml]$xml = $resp.Content
    # Build namespace manager properly
    $nsMgr = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
    $nsMgr.AddNamespace('rs','http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer')
    $nodes = $xml.SelectNodes('//rs:CatalogItem',$nsMgr)
    if(-not $nodes -or $nodes.Count -eq 0){
        Write-Warn 'No catalog items returned (check folder path or permissions).'
        return @()
    }
    $nodes | ForEach-Object {
        [pscustomobject]@{
            Name = $_.Name
            Path = $_.Path
            Type = $_.TypeName
            CreatedDate = $_.CreationDate
            ModifiedDate = $_.ModifiedDate
        }
    }
}

# Retrieve report parameters metadata via SOAP GetItemParameters
function Get-SSRSReportParameters([string]$path){
    $svc = Join-Url $ReportServerRoot 'ReportService2010.asmx'
    $envelope = @"
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetItemParameters xmlns="http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer">
      <ItemPath>$path</ItemPath>
      <HistoryID xsi:nil="true" />
      <ForRendering>true</ForRendering>
      <Values />
    </GetItemParameters>
  </soap:Body>
</soap:Envelope>
"@
    try {
        $auth = Get-WebClientParams
        $resp = Invoke-WebRequest -Uri $svc -Method Post -ContentType 'text/xml; charset=utf-8' -Body $envelope -Headers @{SOAPAction='http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer/GetItemParameters'} @auth -ErrorAction Stop
        [xml]$xml = $resp.Content
        $nsMgr = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
        $nsMgr.AddNamespace('rs','http://schemas.microsoft.com/sqlserver/reporting/2010/03/01/ReportServer')
        $params = $xml.SelectNodes('//rs:ReportParameter',$nsMgr)
        return $params | ForEach-Object {
            $defaultVals = @()
            if($_.DefaultValues -and $_.DefaultValues.Value){ $defaultVals = @($_.DefaultValues.Value | ForEach-Object { $_.'#text' }) }
            [pscustomobject]@{
                Name = $_.Name
                Type = $_.Type
                Nullable = [bool]([string]::IsNullOrEmpty($_.Nullable) -or $_.Nullable -eq 'true')
                MultiValue = ($_.MultiValue -eq 'true')
                AllowBlank = ($_.AllowBlank -eq 'true')
                Prompt = $_.Prompt
                DefaultValues = $defaultVals
            }
        }
    } catch {
        Write-Warn "Failed to retrieve parameters: $($_.Exception.Message)"
        return @()
    }
}

function Build-ParameterQuery([hashtable]$paramHash){
    if(-not $paramHash -or $paramHash.Count -eq 0){ return '' }
    $pairs = @()
    foreach($k in $paramHash.Keys){
        $val = $paramHash[$k]
        if($val -is [System.Collections.IEnumerable] -and -not ($val -is [string])){
            foreach($single in $val){
                $pairs += ('&{0}={1}' -f [uri]::EscapeDataString($k), [uri]::EscapeDataString([string]$single))
            }
        } else {
            $pairs += ('&{0}={1}' -f [uri]::EscapeDataString($k), [uri]::EscapeDataString([string]$val))
        }
    }
    return ($pairs -join '')
}

function Download-SSRSReportUrlAccess(){
    $encPath = Encode-Path $ReportPath
    $paramQuery = Build-ParameterQuery $Parameters
    $renderUrl = Join-Url $ReportServerRoot ("?{0}&rs:Command=Render&rs:Format={1}{2}" -f $encPath, $OutputFormat, $paramQuery)
    Write-Info "Rendering via URL access: $renderUrl"
    $auth = Get-WebClientParams
        try {
            $bytes = Invoke-WebRequest -Uri $renderUrl -Method Get -OutFile $OutputFile @auth -PassThru
            if(Test-Path $OutputFile){ Write-Info "Saved: $OutputFile (Size: $((Get-Item $OutputFile).Length) bytes)" }
        } catch {
            $msg = $_.Exception.Message
            if($msg -match 'rsReportParameterTypeMismatch' -and $Parameters){
                Write-Warn 'Parameter type mismatch detected. Attempting alternate date formats...'
                $dateKeys = @($Parameters.Keys | Where-Object { $_ -match '(?i)date' })
                $original = @{}
                foreach($k in $dateKeys){ $original[$k] = $Parameters[$k] }
                $variants = 'yyyy-MM-dd','MM/dd/yyyy','M/d/yyyy','yyyy-MM-dd 00:00:00','yyyy-MM-ddT00:00:00'
                foreach($fmt in $variants){
                    $changed = $false
                    foreach($k in $dateKeys){
                        $val = $original[$k]
                        [DateTime]$dt = $null
                        if([DateTime]::TryParse($val, [ref]$dt)){
                            $Parameters[$k] = $dt.ToString($fmt)
                            $changed = $true
                        }
                    }
                    if($changed){
                        $paramQuery = Build-ParameterQuery $Parameters
                        $retryUrl = Join-Url $ReportServerRoot ("?{0}&rs:Command=Render&rs:Format={1}{2}" -f $encPath, $OutputFormat, $paramQuery)
                        Write-Info "Retry with format '$fmt': $retryUrl"
                        try {
                            Invoke-WebRequest -Uri $retryUrl -Method Get -OutFile $OutputFile @auth -PassThru | Out-Null
                            if(Test-Path $OutputFile){ Write-Info "Saved after retry format '$fmt': $OutputFile"; return }
                        } catch { Write-Warn "Retry format '$fmt' failed: $($_.Exception.Message)" }
                    }
                }
                throw
            } else { throw }
        }
}

function Download-SSRSReportRest(){
    $restBase = Join-Url $ReportServerRoot '../Reports/api/v2.0'
    $encoded = [uri]::EscapeDataString($ReportPath)
    $findUrl = Join-Url $restBase "CatalogItems(Path=$encoded)"
    $auth = Get-WebClientParams
    $item = Invoke-RestMethod -Uri $findUrl -Method Get @auth
    if(-not $item){ throw "Report not found: $ReportPath" }
    $id = $item.Id
    $renderUrl = Join-Url $restBase "Reports($id)/Export/$OutputFormat"
    Write-Info "Rendering via REST: $renderUrl"
    Invoke-WebRequest -Uri $renderUrl -Method Get -OutFile $OutputFile @auth | Out-Null
    if(Test-Path $OutputFile){ Write-Info "Saved: $OutputFile (Size: $((Get-Item $OutputFile).Length) bytes)" }
}

# MAIN
if($PSCmdlet.ParameterSetName -eq 'List'){
    Write-Info "Listing folder: $ListFolder (Recursive=$($Recursive.IsPresent))"
    $items = List-SSRSFolderUrlAccess $ListFolder
    if($Recursive){
        function Get-ItemsRec([string]$path){
            $children = List-SSRSFolderUrlAccess $path
            foreach($c in $children){
                $c
                if(($c.Type -eq 'Folder') -or ($c.TypeName -eq 'Folder')){
                    try { Get-ItemsRec $c.Path } catch { Write-Warn "Failed descend into $($c.Path): $($_.Exception.Message)" }
                }
            }
        }
        $all = @()
        if($ListFolder -ne '/'){
            $all += [pscustomobject]@{ Name = (Split-Path $ListFolder -Leaf); Path = $ListFolder; Type='Folder' }
        }
        $all += $items
        foreach($f in ($items | Where-Object { $_.Type -eq 'Folder' -or $_.TypeName -eq 'Folder' })){
            $all += Get-ItemsRec $f.Path
        }
        $all | Sort-Object Path,Type | Format-Table -AutoSize
    } else {
        $items | Format-Table -AutoSize
    }
    return
}

# Download path
if(-not $OutputFile){
    $name = Split-Path $ReportPath -Leaf
    $ext = Get-ExtensionForFormat $OutputFormat
    $sanitized = ($name -replace '[\\/:*?"<>|]','_')
    $OutputFile = Join-Path (Get-Location) ("{0}{1}" -f $sanitized, $ext)
}

# Convert -Param strings to hashtable (merged with -Parameters if both provided)
if($Param){
    if(-not $Parameters){ $Parameters = @{} } else { $Parameters = @{} + $Parameters }
    foreach($raw in $Param){
        $parts = $raw -split ',' | Where-Object { $_ -and $_.Trim() -ne '' }
        foreach($p in $parts){
            if($p -notmatch '='){ Write-Warn "Ignoring parameter '$p' (missing =)"; continue }
            $name,$val = $p.Split('=',2)
            $Parameters[$name] = $val
        }
    }
}

# Auto parameter inference if none supplied
if($PSCmdlet.ParameterSetName -eq 'Download' -and -not $Parameters){
    $reportParams = Get-SSRSReportParameters $ReportPath
    if($reportParams.Count -gt 0){
        $Parameters = @{}
        $yesterday = (Get-Date).Date.AddDays(-1)
        $yStr = $yesterday.ToString('yyyy-MM-dd')
        # Single required DateTime param
        $dateParams = $reportParams | Where-Object { $_.Type -eq 'DateTime' }
        $requiredDateParams = $dateParams | Where-Object { $_.DefaultValues.Count -eq 0 }
        if($requiredDateParams.Count -eq 1){
            $Parameters[$requiredDateParams[0].Name] = $yStr
            Write-Info "Auto-set parameter $($requiredDateParams[0].Name)=$yStr"
        } elseif($requiredDateParams.Count -ge 2) {
            $start = $requiredDateParams | Where-Object { $_.Name -match '(?i)^(Start|From)' } | Select-Object -First 1
            $end = $requiredDateParams | Where-Object { $_.Name -match '(?i)^(End|To)' } | Select-Object -First 1
            if($start -and $end){
                $Parameters[$start.Name] = $yStr
                $Parameters[$end.Name] = $yStr
                Write-Info "Auto-set parameters $($start.Name) & $($end.Name) to $yStr"
            }
        }
        if($Parameters.Count -eq 0){
            # If no auto detection, drop back to $null so download attempts defaults
            $Parameters = $null
            Write-Info 'No auto parameter values determined; using report defaults.'
            Write-Info ("Parameters: " + (($reportParams | Select-Object Name,Type) | Out-String).Trim())
        }
    }
}

# If -SingleDate provided, override/augment parameters accordingly
if($SingleDate){
    [DateTime]$dt=$null
    if(-not [DateTime]::TryParse($SingleDate,[ref]$dt)){
        throw "Invalid -Date value '$SingleDate'"
    }
    $dstr = $dt.ToString('yyyy-MM-dd')
    if(-not $Parameters){ $Parameters=@{} }
    $keys = @($Parameters.Keys)
    $dateKeys = $keys | Where-Object { $_ -match '(?i)date' }
    if($dateKeys.Count -eq 1){
        $Parameters[$dateKeys[0]] = $dstr
        Write-Info "Set $($dateKeys[0])=$dstr from -Date"
    } else {
        $startKey = $keys | Where-Object { $_ -match '(?i)^(DateStart|StartDate|FromDate|DateFrom)' } | Select-Object -First 1
        $endKey   = $keys | Where-Object { $_ -match '(?i)^(DateEnd|EndDate|ToDate|DateTo)' } | Select-Object -First 1
        if($startKey -and $endKey){
            $Parameters[$startKey]=$dstr; $Parameters[$endKey]=$dstr
            Write-Info "Set $startKey & $endKey to $dstr from -Date"
        } elseif($startKey){
            $Parameters[$startKey]=$dstr
            Write-Info "Set $startKey=$dstr from -Date"
        } elseif($endKey){
            $Parameters[$endKey]=$dstr
            Write-Info "Set $endKey=$dstr from -Date"
        } else {
            # No existing keys, create generic Date parameter
            $Parameters['Date']=$dstr
            Write-Info "Added Date=$dstr (inferred)"
        }
    }
}

if($UseRest){
    if(-not (Test-SSRSRestAvailable)){
        Write-Warn 'REST not available; falling back to URL access.'
        Download-SSRSReportUrlAccess
    } else { Download-SSRSReportRest }
} else {
    try { Download-SSRSReportUrlAccess }
    catch {
        Write-Warn "URL access failed: $($_.Exception.Message). Trying REST..."
        if(Test-SSRSRestAvailable){ Download-SSRSReportRest } else { throw }
    }
}
