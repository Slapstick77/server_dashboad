# Start desktop sync app in headless mode with logging
# For use with Windows Scheduled Tasks or startup

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

$logFile = Join-Path $scriptDir "auto_sync_log.txt"

# Run headless and log output
python desktop_sync_app.py --headless 2>&1 | Tee-Object -FilePath $logFile -Append
