# Auto-Refresh Feature for Desktop Sync App

## Overview

The desktop sync app now supports **automatic polling** for all sync operations:
- Labor Backfill
- Scheduling Summary  
- Parts Tracker
- DR Polling (2d, 7d, 30d windows)

Each task can have its own interval, be enabled/disabled independently, and settings persist across restarts.

## Features

✅ **Individual Intervals** - Each task has its own configurable interval (5 minutes to 1 week)  
✅ **Persistent Config** - Settings saved to `auto_sync_config.json`  
✅ **GUI Control** - Easy-to-use settings dialog  
✅ **Headless Mode** - Run on server without GUI (perfect for startup tasks)  
✅ **Status Display** - Shows next run time for enabled tasks  
✅ **Test Button** - Run tasks immediately to test configuration  

## Usage

### With GUI

1. Launch the app normally: `python desktop_sync_app.py`
2. Click **"Auto-Refresh..."** button
3. Enable desired tasks and set intervals
4. Click **"Save & Apply"**

The app will:
- Save settings to `auto_sync_config.json`
- Start timers for enabled tasks
- Show next run times in status bar
- Continue running even when minimized

### Headless Mode (Server/Background)

Run without GUI for automated server deployments:

```powershell
python desktop_sync_app.py --headless
```

This will:
- Load config from `auto_sync_config.json`
- Start all enabled tasks
- Run in background
- Log output to console
- Stop with Ctrl+C

**Perfect for Windows startup tasks** or server deployments where GUI is not needed.

### Windows Scheduled Task (Startup)

To run on server startup:

1. Configure intervals in GUI mode first
2. Create scheduled task:
   - **Program**: `C:\Path\To\python.exe`
   - **Arguments**: `C:\Path\To\desktop_sync_app.py --headless`
   - **Trigger**: At startup
   - **Options**: Run whether user logged in or not

## Configuration File

Settings are stored in `auto_sync_config.json`:

```json
{
  "labor": {
    "enabled": false,
    "interval_minutes": 1440
  },
  "scheduling": {
    "enabled": false,
    "interval_minutes": 480
  },
  "parts": {
    "enabled": false,
    "interval_minutes": 720
  },
  "dr_2d": {
    "enabled": false,
    "interval_minutes": 60
  },
  "dr_7d": {
    "enabled": true,
    "interval_minutes": 60
  },
  "dr_30d": {
    "enabled": false,
    "interval_minutes": 180
  }
}
```

### Recommended Intervals

| Task | Interval | Reason |
|------|----------|--------|
| **Labor Backfill** | 1440 min (daily) | Full reprocess, run once per day |
| **Scheduling Summary** | 480 min (8 hours) | Moderate frequency for updates |
| **Parts Tracker** | 720 min (12 hours) | CSV sync, twice daily is sufficient |
| **DR Polling (2d)** | 60 min (hourly) | Recent DRs need frequent checks |
| **DR Polling (7d)** | 60-120 min | Balance between freshness and load |
| **DR Polling (30d)** | 180-360 min | Historical data, less frequent is OK |

## Credentials for Headless Mode

For DR polling in headless mode, credentials are loaded from:

1. **Windows Credential Manager** (preferred)
   - Service: `SQRS_DR`
   - Username/Password stored securely

2. **Environment Variables** (fallback)
   - `DR_USERNAME`
   - `DR_PASSWORD`

Set environment variables for headless server deployment:

```powershell
[System.Environment]::SetEnvironmentVariable('DR_USERNAME', 'your_user', 'Machine')
[System.Environment]::SetEnvironmentVariable('DR_PASSWORD', 'your_pass', 'Machine')
```

## Status Display

The main window shows next run times for enabled tasks:

```
dr_7d: 2:45 PM | scheduling: 6:30 PM
```

Status updates every 30 seconds automatically.

## Test Feature

The **"Test (Run Now)"** button in settings dialog lets you:
- Run a task immediately (bypasses timer)
- Verify credentials and configuration
- Check for errors before enabling long intervals

## Stopping Auto-Refresh

### In GUI Mode
- Open **"Auto-Refresh..."** dialog
- Uncheck tasks to disable
- Click **"Save & Apply"**

### In Headless Mode
- Press **Ctrl+C** to stop gracefully
- Or kill the process

## Troubleshooting

**Tasks not running?**
- Check log output for errors
- Verify credentials are set
- Check config file syntax
- Ensure intervals are >= 5 minutes

**Headless mode exits immediately?**
- Check for credential errors
- Verify config file exists and is valid
- Check database path is correct

**Status not updating?**
- Status bar refreshes every 30 seconds
- Check if tasks are actually enabled in config

## Migration from Old Auto-Refresh

If you were using the old DR-only auto-refresh:
- Old settings are not migrated automatically
- Open **"Auto-Refresh..."** dialog
- Re-configure your preferred intervals
- New system is more flexible with all tasks supported

## Example Workflows

### Development Machine
- Enable DR 7d with 60-minute interval
- Disable labor/scheduling (run manually as needed)
- Keep GUI open

### Production Server
- Enable all tasks with appropriate intervals
- Run in headless mode at startup
- Set environment variables for credentials
- Monitor logs

### Testing/Staging
- Enable DR 2d with 30-minute interval
- Enable scheduling with 4-hour interval
- Run in GUI mode to see logs
