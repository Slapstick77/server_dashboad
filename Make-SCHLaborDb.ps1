[CmdletBinding()]param(
  [string]$Folder = (Get-Location).Path,
  [string]$MasterCsv = (Join-Path (Get-Location).Path 'SCHLabor_master.csv'),
  [string]$Database = (Join-Path (Get-Location).Path 'SCHLabor.db'),
  [switch]$Force,
  [switch]$Quiet,
  [switch]$AutoInstall   # Attempt to install sqlite3 via winget if not found
)
function Write-Info($m){ if(-not $Quiet){ Write-Host "[INFO ] $m" -ForegroundColor Cyan } }
function Write-Warn($m){ if(-not $Quiet){ Write-Warning $m } }
function Write-Err($m){ Write-Error $m }

if(-not (Test-Path $MasterCsv)){ Write-Err "Master CSV not found: $MasterCsv. Run Build-SCHLaborMasterCsv.ps1 first."; exit 1 }
if(Test-Path $Database){ if($Force){ Remove-Item $Database -Force } else { Write-Warn "Database exists. Use -Force to overwrite."; exit 1 } }

$sqlite = Get-Command sqlite3 -ErrorAction SilentlyContinue
if(-not $sqlite -and $AutoInstall){
  Write-Info 'Attempting winget install of SQLite.SQLite (may require acceptance).'
  if(Get-Command winget -ErrorAction SilentlyContinue){
    try {
      winget install -e --id SQLite.SQLite -h --accept-package-agreements --accept-source-agreements | Out-Null
    } catch { Write-Warn ('winget install failed: ' + $_.Exception.Message) }
    Start-Sleep -Seconds 3
    $sqlite = Get-Command sqlite3 -ErrorAction SilentlyContinue
  } else { Write-Warn 'winget not available; cannot auto-install.' }
}
if(-not $sqlite){
  Write-Warn 'sqlite3 not found. Install then re-run or use -AutoInstall if winget available.'
  Write-Host 'Manual install options:' -ForegroundColor Yellow
  Write-Host '  winget install SQLite.SQLite' -ForegroundColor Yellow
  Write-Host '  choco install sqlite' -ForegroundColor Yellow
  exit 1
}

Write-Info "Using sqlite3 at $($sqlite.Source)"

# Build command script
$cmds = @(
  'PRAGMA journal_mode=WAL;',
  'PRAGMA synchronous=OFF;',
  'DROP TABLE IF EXISTS SCHLabor;',
  'CREATE TABLE SCHLabor (' +
     'LoggedDate TEXT NOT NULL,' +
     'COMNumber INTEGER,' +
     'EmployeeName TEXT,' +
     'EmployeeNumber INTEGER,' +
     'DepartmentNumber TEXT,' +
     'Area TEXT,' +
     'ActualHours REAL,' +
     'Reference TEXT' +
  ');',
  '.mode csv',
  # Use just the file name to avoid path space escaping issues with sqlite3 shell
  ('.import --skip 1 "' + (Split-Path $MasterCsv -Leaf) + '" SCHLabor'),
  'CREATE INDEX IF NOT EXISTS IX_SCHLabor_Date ON SCHLabor(LoggedDate);',
  'CREATE INDEX IF NOT EXISTS IX_SCHLabor_Emp ON SCHLabor(EmployeeNumber);',
  'CREATE INDEX IF NOT EXISTS IX_SCHLabor_Area ON SCHLabor(Area);',
  'ANALYZE;',
  'VACUUM;',
  '.quit'
)

Write-Info 'Importing data (this may take a minute)...'
$cmdText = ($cmds -join "`n")
# Capture output & errors for diagnostics
$importOutput = New-Object System.Collections.Generic.List[string]
try {
  $cmdText | & $sqlite.Source $Database 2>&1 | ForEach-Object { $importOutput.Add([string]$_) }
} catch {
  Write-Warn "Exception during sqlite execution: $($_.Exception.Message)"
}
if($LASTEXITCODE -ne 0){
  Write-Err "sqlite3 exited with code $LASTEXITCODE"
  $diagFile = Join-Path (Get-Location) 'sqlite_import_diagnostics.txt'
  $importOutput | Set-Content -Path $diagFile -Encoding UTF8
  Write-Warn "Captured sqlite output to $diagFile (showing last 20 lines):"
  $importOutput | Select-Object -Last 20 | ForEach-Object { Write-Host $_ }
  exit $LASTEXITCODE
}

Write-Info "Database created: $Database"
Write-Info 'Counting rows...'
$countCmds = @(
  '.mode list',
  'SELECT COUNT(*) FROM SCHLabor;',
  '.quit'
)
$rowCount = ($countCmds -join "`n") | & $sqlite.Source $Database
Write-Host "Row count output:" -ForegroundColor Green
Write-Host $rowCount
