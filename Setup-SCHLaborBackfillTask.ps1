<#!
.SYNOPSIS
  Creates/updates a Windows Scheduled Task to run incremental SCHLabor backfill regularly.

.DESCRIPTION
  Sets up a task running as the current user (or specified user) every hour (default) executing
  Get-SCHLaborBackfillIncremental.ps1. Adjust triggers as needed. Ensures 'Run task as soon as possible after a scheduled start is missed'
  and 'Wake the computer to run this task' options when possible.

.PARAMETER TaskName
  Name of the scheduled task.
.PARAMETER IntervalMinutes
  Trigger repetition interval in minutes.
.PARAMETER StartBoundary
  When the trigger first starts (default: now + 1 minute).
.PARAMETER ScriptDirectory
  Directory where scripts & outputs reside.
.PARAMETER MinDate
  Earliest date to fetch (passed to script).
.PARAMETER StartDate
  StartDate passed if no files yet.
#>
[CmdletBinding()]param(
  [string]$TaskName = 'SCHLabor Backfill Incremental',
  [int]$IntervalMinutes = 60,
  [datetime]$StartBoundary = (Get-Date).AddMinutes(1),
  [string]$ScriptDirectory = (Get-Location).Path,
  [datetime]$MinDate,
  [datetime]$StartDate = '2025-08-21'
)

$incScript = Join-Path $ScriptDirectory 'Get-SCHLaborBackfillIncremental.ps1'
if(-not (Test-Path $incScript)){ throw "Script not found: $incScript" }

$actionArgs = @('-NoLogo','-NoProfile','-ExecutionPolicy','Bypass','-File', $incScript, '-StartDate', $StartDate.ToString('yyyy-MM-dd'))
if($MinDate){ $actionArgs += @('-MinDate', $MinDate.ToString('yyyy-MM-dd')) }
$argString = $actionArgs -join ' '

Write-Host "Creating/Updating task '$TaskName' running: powershell $argString" -ForegroundColor Cyan

$trigger = New-ScheduledTaskTrigger -Once -At $StartBoundary -RepetitionInterval ([TimeSpan]::FromMinutes($IntervalMinutes)) -RepetitionDuration ([TimeSpan]::MaxValue)
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType S4U -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -WakeToRun -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $argString -WorkingDirectory $ScriptDirectory

$task = New-ScheduledTask -Action $action -Trigger $trigger -Principal $principal -Settings $settings
Register-ScheduledTask -TaskName $TaskName -InputObject $task -Force | Out-Null
Write-Host "Task '$TaskName' registered." -ForegroundColor Green

Write-Host 'To stop further downloads without deleting task, create STOP_BACKFILL.txt in the script directory.' -ForegroundColor Yellow
