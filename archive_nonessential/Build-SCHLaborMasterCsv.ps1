[CmdletBinding()]param(
  [string]$Folder = (Get-Location).Path,
  [string]$Output = (Join-Path (Get-Location).Path 'SCHLabor_master.csv'),
  [switch]$Force,
  [switch]$NoDedupe,
  [switch]$Quiet
)
function Write-Info($m){ if(-not $Quiet){ Write-Host "[INFO ] $m" -ForegroundColor Cyan } }
function Write-Warn($m){ if(-not $Quiet){ Write-Warning $m } }

$files = Get-ChildItem -Path $Folder -Filter 'SCHLabor_*.csv' -File | Where-Object { $_.BaseName -match '^SCHLabor_\d{8}$' } | Sort-Object Name
if(-not $files){ Write-Warn 'No SCHLabor_*.csv files found.'; exit 1 }

if(Test-Path $Output){ if($Force){ Remove-Item $Output -Force } else { Write-Warn "Output exists. Use -Force to overwrite."; exit 1 } }

$header = 'LoggedDate,COMNumber,EmployeeName,EmployeeNumber,DepartmentNumber,Area,ActualHours,Reference'
Add-Content -Path $Output -Value $header

$seen = if($NoDedupe){ $null } else { New-Object System.Collections.Generic.HashSet[string] }
[int]$total = 0
[int]$written = 0
foreach($f in $files){
  Write-Info "Processing $($f.Name)"
  $lines = Get-Content -Path $f.FullName -Encoding UTF8
  # find header line
  $idx = ($lines | Select-String -Pattern '^LoggedDate,' | Select-Object -First 1).LineNumber
  if(-not $idx){ continue }
  for($i = $idx; $i -lt $lines.Length; $i++){
    $ln = $lines[$i].Trim()
    if([string]::IsNullOrWhiteSpace($ln)){ continue }
    if($ln -match '^LoggedDate,'){ continue } # skip repeated headers
    # normalize EmployeeNumber1 -> EmployeeNumber (pre-header rename already forces order, so just pass line)
    $total++
    if($seen){
      if($seen.Add($ln)){
        Add-Content -Path $Output -Value $ln
        $written++
      }
    } else {
      Add-Content -Path $Output -Value $ln
      $written++
    }
  }
}
Write-Info "Master CSV built: $Output (rows written: $written, source rows seen: $total, dedupe: $([bool]$seen))"
