<#!
.SYNOPSIS
  Run a SQL query (or stored procedure) directly and export results (CSV or JSON).

.DESCRIPTION
  Bypasses SSRS by executing SQL against a target database using either Windows Integrated Security
  (default) or an explicit SQL/Login credential. Supports simple token replacement for @DateStart and
  @DateEnd in the query text when you provide -DateStart/-DateEnd parameters.

.PARAMETER Server
  SQL Server instance name (e.g. SERVERNAME or SERVERNAME\\INSTANCE).

.PARAMETER Database
  Database name hosting the data you need.

.PARAMETER Query
  Inline T-SQL query text OR stored procedure exec statement (mutually exclusive with -QueryFile).

.PARAMETER QueryFile
  Path to a .sql file containing the query (mutually exclusive with -Query).

.PARAMETER DateStart
  Optional date (yyyy-MM-dd) replacing token @DateStart in query text before execution.

.PARAMETER DateEnd
  Optional date (yyyy-MM-dd) replacing token @DateEnd in query text before execution.

.PARAMETER OutputFile
  Destination file; extension determines format: .csv (default) or .json.
  If omitted, auto-named with timestamp.

.PARAMETER Credential
  PSCredential for SQL Authentication. If omitted, uses Integrated Security.

.EXAMPLE
  # Yesterday's range inline query
  $y = (Get-Date).Date.AddDays(-1).ToString('yyyy-MM-dd')
  ./Get-SQLData.ps1 -Server PRODDB -Database ERP -Query "SELECT * FROM Labor WHERE WorkDate BETWEEN @DateStart AND @DateEnd" -DateStart $y -DateEnd $y

.EXAMPLE
  # Using a .sql file and explicit SQL credential
  $cred = Get-Credential  # (SQL login)
  ./Get-SQLData.ps1 -Server PRODDB -Database ERP -QueryFile .\SCHLabor.sql -DateStart 2025-08-21 -DateEnd 2025-08-21 -Credential $cred -OutputFile Labor_20250821.csv

.NOTES
  Keep queries parameterized when possible. This helper only performs token substitution for convenience.
#>
[CmdletBinding(DefaultParameterSetName='Inline')]
param(
  [Parameter(Mandatory=$true)][string]$Server,
  [Parameter(Mandatory=$true)][string]$Database,
  [Parameter(Mandatory=$true, ParameterSetName='Inline')][string]$Query,
  [Parameter(Mandatory=$true, ParameterSetName='File')][string]$QueryFile,
  [string]$DateStart,
  [string]$DateEnd,
  [string]$OutputFile,
  [System.Management.Automation.PSCredential]$Credential
)

function Write-Info($m){ Write-Host "[INFO ] $m" -ForegroundColor Cyan }
function Write-Warn($m){ Write-Warning $m }
function Fail($m){ throw $m }

# Load query
if($PSCmdlet.ParameterSetName -eq 'File'){
  if(-not (Test-Path $QueryFile)){ Fail "QueryFile not found: $QueryFile" }
  $Query = Get-Content -Raw -Path $QueryFile
}

# Basic token substitution (NOT SQL parameters). Use only with trusted input.
if($DateStart){ $Query = $Query -replace '@DateStart', ($DateStart) }
if($DateEnd){ $Query = $Query -replace '@DateEnd', ($DateEnd) }

if(-not $OutputFile){
  $stamp = (Get-Date).ToString('yyyyMMdd_HHmmss')
  $OutputFile = Join-Path (Get-Location) ("SQLData_$stamp.csv")
}

$ext = [IO.Path]::GetExtension($OutputFile).ToLowerInvariant()
if($ext -notin @('.csv','.json')){ Write-Warn 'Unknown extension; defaulting to .csv'; $OutputFile += '.csv'; $ext = '.csv' }

# Build connection string
if($Credential){
  $user = $Credential.UserName
  $pwd = $Credential.GetNetworkCredential().Password
  $connString = "Server=$Server;Database=$Database;User ID=$user;Password=$pwd;TrustServerCertificate=True;" # Add Encrypt=Yes if needed
} else {
  $connString = "Server=$Server;Database=$Database;Integrated Security=SSPI;TrustServerCertificate=True;"
}

Add-Type -AssemblyName System.Data
$dt = New-Object System.Data.DataTable

Write-Info 'Executing query...'
$sw = [System.Diagnostics.Stopwatch]::StartNew()
try {
  $conn = New-Object System.Data.SqlClient.SqlConnection $connString
  $conn.Open()
  try {
    $cmd = $conn.CreateCommand()
    $cmd.CommandTimeout = 600
    $cmd.CommandText = $Query
    $rdr = $cmd.ExecuteReader()
    $dt.Load($rdr)
    $rdr.Close()
  } finally { $conn.Close() }
} catch {
  Fail "SQL execution failed: $($_.Exception.Message)"
}
$sw.Stop()
Write-Info ("Rows: {0}  Cols: {1}  Time: {2} ms" -f $dt.Rows.Count,$dt.Columns.Count,$sw.ElapsedMilliseconds)

# Export
switch($ext){
  '.csv' {
    Write-Info "Writing CSV: $OutputFile"
    $sb = New-Object System.Text.StringBuilder
    # Header
    [void]$sb.AppendLine(($dt.Columns | ForEach-Object ColumnName | ForEach-Object { '"' + ($_ -replace '"','""') + '"' }) -join ',')
    foreach($row in $dt.Rows){
      $cells = foreach($col in $dt.Columns){
        $val = $row[$col.ColumnName]
        if($null -eq $val){ '""' } else {
          $s = $val.ToString()
          '"' + ($s -replace '"','""') + '"'
        }
      }
      [void]$sb.AppendLine(($cells -join ','))
    }
    [IO.File]::WriteAllText($OutputFile, $sb.ToString(), [Text.UTF8Encoding]::new($false))
  }
  '.json' {
    Write-Info "Writing JSON: $OutputFile"
    $rows = foreach($row in $dt.Rows){
      $h = @{}
      foreach($col in $dt.Columns){ $h[$col.ColumnName] = $row[$col.ColumnName] }
      [pscustomobject]$h
    }
    $rows | ConvertTo-Json -Depth 5 | Out-File -Encoding utf8 -FilePath $OutputFile
  }
}

Write-Info "Saved output: $OutputFile"
