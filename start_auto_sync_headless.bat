@echo off
REM Start desktop sync app in headless mode (no GUI)
REM This is for use with Windows Scheduled Tasks or startup

cd /d "%~dp0"
python desktop_sync_app.py --headless

REM If you want to see output, add: >> sync_log.txt 2>&1
REM python desktop_sync_app.py --headless >> sync_log.txt 2>&1
