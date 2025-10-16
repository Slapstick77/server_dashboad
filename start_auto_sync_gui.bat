@echo off
REM Start desktop sync app with GUI (normal mode)
REM Tasks will auto-run based on config
REM Window can be minimized but must stay open

cd /d "%~dp0"
start "Desktop Sync Auto-Scheduler" python desktop_sync_app.py

REM Note: Window opens and can be minimized
REM Auto-scheduler runs as long as window is open
