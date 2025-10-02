# Example: Schedule daily SCHLabor report (yesterday's dates) via Task Scheduler
# Creates/updates a Windows Scheduled Task running under the current user context.

$taskName = 'SCHLabor Daily PDF'
$scriptPath = 'C:\Project p\SQRS\Get-SSRSReport.ps1'
$action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument "-NoLogo -NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`" -ReportServerRoot http://c201m580/ReportServer -ReportPath `"/Custom/Production Control/SCHLabor`" -OutputFormat PDF -Param DateStart=$((Get-Date).AddDays(-1).ToString('yyyy-MM-dd')),DateEnd=$((Get-Date).AddDays(-1).ToString('yyyy-MM-dd'))"
$trigger = New-ScheduledTaskTrigger -Daily -At 6:00am
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Force
Write-Host "Scheduled task '$taskName' created/updated." -ForegroundColor Green
