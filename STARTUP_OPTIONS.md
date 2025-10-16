# Desktop Sync App - Startup Options Guide

## Quick Answer

**YES** - If you start the app normally, it will keep running (and auto-polling) until:
- You close the window
- PC shuts down
- You log out (if running as user)

## Three Ways to Run

### 1. GUI Mode (Recommended for Desktop)

**Start:**
```powershell
python desktop_sync_app.py
```
or double-click: `start_auto_sync_gui.bat`

**Behavior:**
- ✅ Window opens with log panel
- ✅ Auto-scheduler starts enabled tasks
- ✅ Can minimize window (keeps running)
- ✅ See all output in real-time
- ✅ Can manually click buttons anytime
- ❌ Stops if you close window or log out

**Best for:**
- Development/testing
- Desktop PC where you stay logged in
- Want to see logs in GUI

### 2. Headless Mode (Recommended for Server)

**Start:**
```powershell
python desktop_sync_app.py --headless
```
or double-click: `start_auto_sync_headless.bat`

**Behavior:**
- ✅ No window (pure background)
- ✅ Auto-scheduler starts enabled tasks
- ✅ Runs even when no one logged in
- ✅ Output logged to file
- ❌ Can't see GUI (use log file)
- ❌ Must stop with Ctrl+C or Task Manager

**Best for:**
- Server/production environment
- Windows startup task (no user login needed)
- Running as background service

### 3. Hybrid (GUI at Startup)

**Setup:**
1. Press `Win+R`, type `shell:startup`, press Enter
2. Copy `start_auto_sync_gui.bat` to that folder
3. On next login, app auto-starts with GUI
4. Minimize the window and forget about it

**Behavior:**
- ✅ Auto-starts on login
- ✅ Has GUI (can check logs)
- ✅ Runs until you close it or log out
- ❌ Only runs when you're logged in

## Will It Run Forever?

| Mode | Runs Forever? | Conditions |
|------|---------------|------------|
| **GUI** | While window open | Must stay logged in, don't close window |
| **Headless** | Until stopped | Can run without login, true background service |
| **GUI at Startup** | Until logout | Auto-starts each login, runs while logged in |

## Auto-Scheduler Status

**In both modes**, the auto-scheduler will:
- ✅ Load `auto_sync_config.json` on startup
- ✅ Start all enabled tasks
- ✅ Run tasks at configured intervals
- ✅ Keep running until app stops
- ✅ Not require any interaction

**Example:** If DR 7d is enabled with 60-minute interval:
- First poll: Immediately on startup
- Next poll: 60 minutes later
- Continues: Every 60 minutes forever

## Recommendation for Your Use Case

Based on your question about "control app starting desktop_sync_app", I recommend:

### If Server/Unattended:
```powershell
# Use headless mode with Windows Task Scheduler
python desktop_sync_app.py --headless
```
**Setup Windows Task:**
- Trigger: At startup
- Run whether user logged in: Yes
- Result: True background service

### If Desktop/Workstation:
```powershell
# Use GUI mode, put shortcut in startup folder
# Just start normally and minimize
python desktop_sync_app.py
```
**Setup:**
- Copy `start_auto_sync_gui.bat` to startup folder
- Window opens on login, minimize it
- Result: Runs all day, can see logs

## Current Status

After starting the app (either mode):
- Check log for: `[AUTO] task_name: scheduled for X:XX PM`
- Status bar shows next run times
- Tasks execute automatically
- No interaction needed

## Log Locations

| Mode | Log Location |
|------|--------------|
| **GUI** | In-app log panel (real-time) |
| **Headless** | `auto_sync_headless.log` (file) |

## Troubleshooting

**GUI keeps closing?**
- Don't close the window, minimize instead
- Check if crash/error in log

**Tasks not running?**
- Check `auto_sync_config.json` - tasks must be enabled
- Verify intervals are set
- Check credentials for DR tasks

**Need to stop?**
- GUI: Close window
- Headless: Press Ctrl+C or kill process
