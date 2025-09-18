[CmdletBinding()]param(
  [datetime]$StartDate = '2021-05-14',
  [datetime]$EndDate = (Get-Date).Date,
  [int]$PrimaryMonths = 4,
  [int]$OverlapMonths = 1,
  [string]$ReportServerRoot = 'http://c201m580/ReportServer',
  [string]$ReportPath = '/Custom/Production Control/SCHSchedulingSummaryReport',
  [string]$StartParamName = 'SHIP_DATE_START',
  [string]$EndParamName = 'SHIP_DATE_END',
  [switch]$WhatIfChunks,
  [switch]$Recreate
)
function Info($m){ Write-Host "[INFO ] $m" -ForegroundColor Cyan }
function Warn($m){ Write-Warning $m }
function Fail($m){ throw $m }
if($EndDate -lt $StartDate){ Fail 'EndDate must be >= StartDate' }
$advance = $PrimaryMonths - $OverlapMonths
if($advance -lt 1){ $advance = 1 }
# Resolve python executable (prefer local .venv)
$pythonExe = Join-Path $PSScriptRoot '.venv/Scripts/python.exe'
if(-not (Test-Path $pythonExe)){
  $pythonExe = 'python'
}
Info "Historical backfill Start=$($StartDate.ToString('yyyy-MM-dd')) End=$($EndDate.ToString('yyyy-MM-dd')) Window=${PrimaryMonths}m Overlap=${OverlapMonths}m Advance=${advance}m"

if($Recreate){
  Info 'Dropping existing SCHSchedulingSummary / ChangeLog / RunLog tables...'
  $dropPy = @"
import sqlite3
conn=sqlite3.connect('SCHLabor.db')
cur=conn.cursor()
for t in ('SCHSchedulingSummary','ChangeLog','RunLog'):
    cur.execute(f'DROP TABLE IF EXISTS {t}')
conn.commit(); conn.close()
print('Dropped (if existed).')
"@
  & $pythonExe -c $dropPy
}
$cur = $StartDate.Date
$chunkIndex = 0
$errors = @()
while($cur -le $EndDate){
  $chunkIndex++
  $chunkEnd = $cur.AddMonths($PrimaryMonths).AddDays(-1)
  if($chunkEnd -gt $EndDate){ $chunkEnd = $EndDate }
  $tag = '{0:yyyy-MM-dd}_{1:yyyy-MM-dd}' -f $cur,$chunkEnd
  $out = "SCHSchedulingSummaryReport_${tag}.csv"
  if($WhatIfChunks){ Write-Host "[PLAN ] $tag -> $out"; $cur = $cur.AddMonths($advance); continue }
  Info "Chunk $chunkIndex $tag downloading..."
  try {
    .\Get-SCHSchedulingSummaryRange.ps1 -StartDate $cur -EndDate $chunkEnd -OutFile $out -Force -ReportServerRoot $ReportServerRoot -ReportPath $ReportPath -StartParamName $StartParamName -EndParamName $EndParamName | Out-Null
    if(-not (Test-Path $out) -or (Get-Item $out).Length -lt 100){ Warn "Empty or missing $out"; $errors += $tag; $cur = $cur.AddMonths($advance); continue }
    Info "Chunk $chunkIndex $tag cleaning + ingest..."
  $pyCode = @"
import sys, os, csv
raw=sys.argv[1]
clean='cleaned_file.csv'
def fallback_clean(inp, outp):
  # Minimal: lower-case headers, strip spaces, write out
  with open(inp,'r',encoding='utf-8',errors='ignore') as f, open(outp,'w',newline='',encoding='utf-8') as g:
    rdr = csv.reader(f)
    rows=list(rdr)
    if not rows: return 0
    hdr = [h.strip().lower().replace(' ','_') for h in rows[0]]
    w = csv.writer(g)
    w.writerow(hdr)
    for r in rows[1:]:
      w.writerow(r)
    return len(rows)-1
rows_count = 0
try:
  from clean import convert_file1_to_cleaned
  convert_file1_to_cleaned(raw, clean)
except Exception as e:
  rows_count = fallback_clean(raw, clean)
from report_update_service import parse_cleaned_csv, upsert_sched_rows, ensure_change_log_tables
ensure_change_log_tables()
rows=parse_cleaned_csv(clean)
changes=upsert_sched_rows(rows)
new=sum(1 for c in changes if c['column']=='*NEW*')
print(f'INGEST {os.path.basename(raw)} rows={len(rows)} new={new} updates={len(changes)-new}')
"@
  & $pythonExe -c $pyCode $out
  } catch {
    Warn "Error $tag : $($_.Exception.Message)"
    $errors += $tag
  }
  $cur = $cur.AddMonths($advance)
}
Info "Backfill complete. Chunks=$chunkIndex Errors=$($errors.Count)"
if($errors.Count -gt 0){ Write-Host "Failed/empty tags: $($errors -join ', ')" -ForegroundColor Yellow }
# Summary counts
$py2 = @"
import sqlite3
c=sqlite3.connect('SCHLabor.db');cur=c.cursor()
try:
 cur.execute('SELECT COUNT(DISTINCT comnumber1) FROM SCHSchedulingSummary'); print('Distinct COMNumber1:', cur.fetchone()[0])
 cur.execute('SELECT COUNT(*) FROM SCHSchedulingSummary'); print('Total rows:', cur.fetchone()[0])
finally:
 c.close()
"@
& $pythonExe -c $py2
