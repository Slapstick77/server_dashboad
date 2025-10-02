[CmdletBinding()]param(
  [string]$Folder = (Get-Location).Path
)
$files = Get-ChildItem -Path $Folder -Filter 'SCHLabor_*.csv' -File | Where-Object { $_.BaseName -match '^SCHLabor_\d{8}$' }
if(-not $files){ Write-Host 'No SCHLabor_*.csv files found.' -ForegroundColor Yellow; exit }
$dates = foreach($f in $files){
  if($f.BaseName -match 'SCHLabor_(\d{4})(\d{2})(\d{2})'){
    [datetime]::ParseExact($Matches[1]+$Matches[2]+$Matches[3],'yyyyMMdd',$null)
  }
}
$dates = $dates | Sort-Object
$earliest = $dates[0]
$latest = $dates[-1]
$allSet = [System.Collections.Generic.HashSet[string]]::new()
foreach($d in $dates){ [void]$allSet.Add($d.ToString('yyyy-MM-dd')) }
# Detect gaps between earliest and latest
$missing = New-Object System.Collections.Generic.List[string]
for($d = $earliest; $d -le $latest; $d = $d.AddDays(1)){
  $k = $d.ToString('yyyy-MM-dd')
  if(-not $allSet.Contains($k)){ $missing.Add($k) }
}
$nextTarget = $earliest.AddDays(-1)
Write-Host "Total files: $($dates.Count)" -ForegroundColor Cyan
Write-Host "Latest date present: $($latest.ToString('yyyy-MM-dd'))" -ForegroundColor Cyan
Write-Host "Earliest date present: $($earliest.ToString('yyyy-MM-dd'))" -ForegroundColor Cyan
if($missing.Count -eq 0){ Write-Host 'No gaps between earliest and latest.' -ForegroundColor Green } else { Write-Host ("Gaps (missing days between earliest and latest): {0}" -f ($missing -join ', ')) -ForegroundColor Yellow }
Write-Host "Next target (one earlier): $($nextTarget.ToString('yyyy-MM-dd'))" -ForegroundColor Magenta
