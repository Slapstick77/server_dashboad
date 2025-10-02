<#!
.SYNOPSIS
  Build a SQLite database (SCHLabor.db) from all SCHLabor_YYYYMMDD.csv files.

.DESCRIPTION
  Parses each SCHLabor_*.csv, skipping the first non-data lines until the real header line
  starting with LoggedDate,. Cleans and normalizes values:
    - LoggedDate converted to ISO yyyy-MM-dd
    - ActualHours to REAL (decimal) when possible
  Produces (a) a SQLite database if sqlite3 CLI is available or (b) an import SQL script if not.

.PARAMETER Folder
  Source folder containing SCHLabor_*.csv files.
.PARAMETER Database
  Output SQLite database file path (default SCHLabor.db under Folder).
.PARAMETER SqlFile
  Output SQL file path (default SCHLabor_import.sql under Folder) always generated.
.PARAMETER Force
  Overwrite existing DB / SQL files.
.PARAMETER MaxFiles
  Optional limit for testing (process only first N files by date).
.PARAMETER Quiet
  Reduce console output.
#>
[CmdletBinding()]param(
  [string]$Folder = (Get-Location).Path,
  [string]$Database = (Join-Path (Get-Location).Path 'SCHLabor.db'),
  [string]$SqlFile = (Join-Path (Get-Location).Path 'SCHLabor_import.sql'),
  [switch]$Force,
  [int]$MaxFiles,
  [switch]$Quiet
)
function Write-Info($m){ if(-not $Quiet){ Write-Host "[INFO ] $m" -ForegroundColor Cyan } }
function Write-Warn($m){ if(-not $Quiet){ Write-Warning $m } }
function Write-Err($m){ Write-Error $m }

if(-not (Test-Path $Folder)){ throw "Folder not found: $Folder" }

$csvFiles = Get-ChildItem -Path $Folder -Filter 'SCHLabor_*.csv' -File | Where-Object { $_.BaseName -match '^SCHLabor_\d{8}$' } | Sort-Object Name
if(-not $csvFiles){ Write-Err 'No SCHLabor_*.csv files found.'; exit 1 }
if($MaxFiles -gt 0){ $csvFiles = $csvFiles | Select-Object -First $MaxFiles }

if(Test-Path $SqlFile){ if($Force){ Remove-Item $SqlFile -Force } else { Write-Err "SQL file exists: $SqlFile (use -Force)"; exit 1 } }
if(Test-Path $Database){ if($Force){ Remove-Item $Database -Force } else { Write-Warn "Database exists: $Database (will append if sqlite3 is used)" } }

$headerColumns = 'LoggedDate','COMNumber','EmployeeName','EmployeeNumber','DepartmentNumber','Area','ActualHours','Reference'

Add-Content -Path $SqlFile -Value "BEGIN;"
Add-Content -Path $SqlFile -Value @'
CREATE TABLE IF NOT EXISTS SCHLabor (
  Id INTEGER PRIMARY KEY,
  LoggedDate TEXT NOT NULL,
  COMNumber INTEGER,
  EmployeeName TEXT,
  EmployeeNumber INTEGER,
  DepartmentNumber TEXT,
  Area TEXT,
  ActualHours REAL,
  Reference TEXT
);
CREATE INDEX IF NOT EXISTS IX_SCHLabor_Date ON SCHLabor(LoggedDate);
CREATE INDEX IF NOT EXISTS IX_SCHLabor_Emp ON SCHLabor(EmployeeNumber);
'@

[int]$rowCount = 0
[int]$fileCount = 0
function Esc($v){ if($null -eq $v -or $v -eq ''){ return $null }; return ($v -replace "'","''") }

foreach($file in $csvFiles){
  $fileCount++
  Write-Info "Processing $($file.Name) ($fileCount of $($csvFiles.Count))"
  $lines = Get-Content -Path $file.FullName -Raw -ErrorAction SilentlyContinue -Encoding UTF8
  if(-not $lines){ Write-Warn "Empty file: $($file.Name)"; continue }
  $lineArr = $lines -split "`r?`n"
  # find header line
  $idx = 0
  while($idx -lt $lineArr.Length -and ($lineArr[$idx] -notmatch '^LoggedDate,')) { $idx++ }
  if($idx -ge $lineArr.Length){ Write-Warn "Header not found in $($file.Name)"; continue }
  $dataStart = $idx + 1
  if($dataStart -ge $lineArr.Length){ Write-Warn "No data rows in $($file.Name)"; continue }
  $dataLines = @()
  for($i=$dataStart; $i -lt $lineArr.Length; $i++){
    $ln = $lineArr[$i].Trim()
    if([string]::IsNullOrWhiteSpace($ln)){ continue }
    $dataLines += $ln
  }
  if(-not $dataLines){ Write-Warn "No non-empty data rows in $($file.Name)"; continue }
  $objects = $dataLines | ConvertFrom-Csv -Header $headerColumns
  foreach($o in $objects){
    # normalize date; skip row if missing LoggedDate
    $rawDate = $o.LoggedDate
    if([string]::IsNullOrWhiteSpace($rawDate)){ continue }
  $iso = $rawDate
  $parsed = [datetime]::MinValue
  if([datetime]::TryParse($rawDate, [ref]$parsed)) { $iso = $parsed.ToString('yyyy-MM-dd') }
    # sanitize text fields
    $empName = Esc $o.EmployeeName
    $area = Esc $o.Area
    $ref = Esc $o.Reference
    # numeric conversions
    $com = $null
    $tmp = 0
    if([int]::TryParse($o.COMNumber, [ref]$tmp)){ $com = $tmp }
    $empNum = $null
    $tmp2 = 0
    if([int]::TryParse($o.EmployeeNumber, [ref]$tmp2)){ $empNum = $tmp2 }
    $dept = $o.DepartmentNumber  # keep as text to preserve leading zeros
  $hours = $null
  $hTmp = 0.0
  if([double]::TryParse($o.ActualHours, [ref]$hTmp)){ $hours = $hTmp }
    $valList = @()
    $valList += ('"{0}"' -f $iso)
    $valList += ($(if($com -ne $null){ $com } else {'NULL'}))
    $valList += ($(if($empName){ '"' + $empName + '"' } else {'NULL'}))
    $valList += ($(if($empNum -ne $null){ $empNum } else {'NULL'}))
    $valList += ($(if($dept){ '"' + ($dept -replace '"','""') + '"' } else {'NULL'}))
    $valList += ($(if($area){ '"' + $area + '"' } else {'NULL'}))
    $valList += ($(if($hours -ne $null){ $hours } else {'NULL'}))
    $valList += ($(if($ref){ '"' + $ref + '"' } else {'NULL'}))
    $insert = 'INSERT INTO SCHLabor (LoggedDate,COMNumber,EmployeeName,EmployeeNumber,DepartmentNumber,Area,ActualHours,Reference) VALUES ({0});' -f ($valList -join ',')
    Add-Content -Path $SqlFile -Value $insert
    $rowCount++
  }
}
Add-Content -Path $SqlFile -Value 'COMMIT;'
Write-Info "Generated SQL file $SqlFile with $rowCount rows from $fileCount files."

$sqlite = Get-Command sqlite3 -ErrorAction SilentlyContinue
if($sqlite){
  Write-Info "sqlite3 found: $($sqlite.Source). Importing into $Database"
  Get-Content -Path $SqlFile | & $sqlite.Source $Database
  if($LASTEXITCODE -eq 0){ Write-Info 'Import completed.' } else { Write-Warn "sqlite3 exit code $LASTEXITCODE (database may be incomplete)" }
} else {
  Write-Warn 'sqlite3 CLI not found. Install (winget install SQLite.SQLite or choco install sqlite) then run:'
  Write-Host "  sqlite3 `"$Database`" < `"$SqlFile`"" -ForegroundColor Yellow
}
