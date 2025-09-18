# SSRS Report Automation

PowerShell script `Get-SSRSReport.ps1` to list and download (render) SQL Server Reporting Services (SSRS) reports automatically.

## Quick Start

1. Identify the ReportServer endpoint (NOT the web portal `/Reports`):
   - Portal URL you visit: `http://c201m580/Reports/browse/Custom/Production%20Control`
   - Corresponding ReportServer endpoint: `http://c201m580/ReportServer`
2. Determine the catalog path of the report as shown in the portal (use spaces exactly): e.g. `/Custom/Production Control/Daily Output`
3. Open PowerShell in this folder.

### List reports in a folder
```powershell
./Get-SSRSReport.ps1 -ReportServerRoot http://c201m580/ReportServer -ListFolder "/Custom/Production Control"
```

### Download a report as PDF
```powershell
./Get-SSRSReport.ps1 -ReportServerRoot http://c201m580/ReportServer -ReportPath "/Custom/Production Control/Daily Output" -OutputFormat PDF
```
Result will be saved in the current directory with an auto-generated filename.

### Specify output path & format (Excel)
```powershell
$cred = Get-Credential  # Only if you need to supply different credentials
./Get-SSRSReport.ps1 -ReportServerRoot http://c201m580/ReportServer -ReportPath "/Custom/Production Control/Daily Output" -OutputFormat EXCEL -OutputFile C:\Reports\DailyOutput.xls -Credential $cred
```

### Force REST API (if SSRS 2017+)
```powershell
./Get-SSRSReport.ps1 -ReportServerRoot http://c201m580/ReportServer -ReportPath "/Custom/Production Control/Daily Output" -UseRest
```
(Will fallback automatically if REST is not available.)

## Supported Formats
`PDF, EXCEL, EXCELOPENXML, WORD, WORDOPENXML, CSV, XML, MHTML, IMAGE, JSON, ATOM, HTML4.0, HTML5`

Note: Not all instances enable every renderer. If a format fails, try PDF first.

## Scheduling (Windows Task Scheduler)
1. Create a basic task.
2. Action: Start a Program.
3. Program/script:
   ```
   powershell.exe
   ```
4. Add arguments (example):
   ```
   -NoLogo -NoProfile -ExecutionPolicy Bypass -File "C:\Path\To\Get-SSRSReport.ps1" -ReportServerRoot http://c201m580/ReportServer -ReportPath "/Custom/Production Control/Daily Output" -OutputFormat PDF -OutputFile "C:\Reports\DailyOutput.pdf"
   ```
5. Set triggers (daily, hourly, etc.).

## Troubleshooting
- 401 Unauthorized: Run PowerShell as a user with SSRS Browser permissions, or pass `-Credential`.
- 404 Not Found: Confirm you used `/ReportServer` not `/Reports` in `-ReportServerRoot`, and that `-ReportPath` matches exactly (case-insensitive but spaces matter).
- Empty / incorrect file: Ensure the report renders in the browser first. Some reports require parameters; this basic script doesn't yet handle parameters.

## Extending: Adding Parameters
URL access example with parameters:
```
?%2fCustom%2fProduction%20Control%2fDaily%20Output&rs:Command=Render&rs:Format=PDF&StartDate=2025-08-01&EndDate=2025-08-22
```
Enhancement idea: Add `-Parameters @{ StartDate='2025-08-01'; EndDate='2025-08-22' }` handling that appends `&Name=Value` pairs.

## Next Improvements (if needed)
- Report parameter support.
- Retry logic / logging to file.
- Zip & email results.
- Parallel downloading of multiple reports.

---
Generated helper script for automating SSRS report pulls.

## Report Update Service (Python)

`report_update_service.py` adds automated logic for:

1. Labor Backfill: Downloads missing daily SCHLabor CSVs since the last LoggedDate and inserts only new rows (unique index prevents duplicates).
2. Scheduling Summary Upsert: Pulls 90-day window (past 60 / future 30), runs `clean.py`, then upserts rows into `SCHSchedulingSummary` keyed on `comnumber1`, tracking changed columns.
3. Change Logging: Tables `RunLog` and `ChangeLog` capture each run and per-COM modifications.

Run manually:
```powershell
python report_update_service.py
```
Or call functions (`labor_backfill()`, `update_scheduling_summary()`) from elsewhere (e.g., a Flask route or a Windows Scheduled Task wrapper).

Future: integrate with Flask UI to trigger runs & display last changes.
