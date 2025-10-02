# SSRS Report Automation

PowerShell script `Get-SSRSReport.ps1` to list and download (render) SQL Server Reporting Service## Dept-day inclusion rules (applied before counting d## Averages displayed

**Only fully complete units** (all 11 applicable departments at 100%) are included in averages:

- **Last 10 Complete Units:** Most recent 10 by unit last day
- **Last 90 Calendar Days:** Complete units with last day within last 90 days

For each set:

- Average actual days worked: mean of `Unit Actual Days Worked`
- Average total time span: mean of `Unit Time Span`
- Average overall efficiency: mean of per-unit overall efficiency (see below)
Department metrics (First/Last/Days/Span) are computed **only when department completion = 100%**.

1. **Minimum hours threshold** OR **Employee Override**
    - Keep a department-day if:
      - Total hours ≥ `MIN_DAY_HOURS` (default: 2.0), OR
      - Distinct employees ≥ 2 (Employee Override)
    - Days below threshold with <2 employees are dropped.

2. **Exclusion Employees**
    - Configured employees (e.g., ID `1205797`) are excluded from day selection logic.
    - They don't count toward hours or distinct employee counts for filtering decisions.

3. **Gap Filtering (Outlier Caps)**
    - **First Day:** ALWAYS apply gap filtering. Keep if gap to next charge ≤ cap OR ≥2 employees.
    - **Last Day:** Apply ONLY when department is 100% complete. Incomplete departments keep all days.
    - **Caps (days):** Assembly 12, BaseFormPaint 7, Crating 7, DoorFab 7, Electrical 7, Fab 13, FanAssyTest 22, InsulWallFab 11, Paint 7, Pipe 7, Welding 8, Test 22.
    - **Note:** Assembly cap applies to both ASSY (0260) and FLOW (0280) codes.

4. **Gantt Chart Behavior**
    - Shows **all** raw charged days for **all** departments including Test.
    - **No filtering applied** – purely for visibility and troubleshooting.orts automatically.

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

---

# Production Dashboard Metrics – Definitions

This section documents how the web dashboard computes span, active days, and efficiency so we stay consistent.

## Data sources and normalization

- Labor table: `SCHLabor`
   - Day per charge: `iso_logged_date` if set; else first 10 chars of `LoggedDate`. Normalized as `YYYY-MM-DD`.
   - COM normalization: last 5 digits from any `COMNumber` value (e.g., "COM 123456" → "23456").
   - Department rows use raw codes resolved via `DepartmentCode` table, then mapped to canonical departments (see below).
- Scheduling summary: `SCHSchedulingSummary`
   - Provides per-department Standard (std), Actual (act), Completion %, and Efficiency % for 11 tracked departments.
   - Used for completion status determination only; days/span metrics come from filtered `SCHLabor` data.

## Department Mapping (Labor Codes → Canonical Departments)

Raw labor codes from `SCHLabor.DepartmentNumber` are resolved via `DepartmentCode` table:

- `0120 FAB` → **Fab**
- `0140 WELD` → **Welding**
- `0180 FOAM/PAINT` → **BaseFormPaint**
- `0200 FAN` → **FanAssyTest**
- `0220 WALL FAB` → **InsulWallFab**
- `0230 PIPE` → **Pipe**
- `0260 ASSY` + `0280 FLOW` → **Assembly** (both codes map to single department)
- `0270 DOOR` → **DoorFab**
- `0300 ELEC` → **Electrical**
- `0340 PAINT` → **Paint**
- `0360 TEST` → **Test** (Gantt display only; not used for completion/metrics)
- `0380 FINISH` → **Crating**

**Completion Tracking:** 11 departments (excludes Test): Fab, Welding, BaseFormPaint, FanAssyTest, InsulWallFab, DoorFab, Electrical, Pipe, Paint, Crating, Assembly.

## Dept-day inclusion rules (applied before counting days)

1. Minimum hours threshold
    - Keep a department-day only if total hours that day ≥ `MIN_DAY_HOURS`.
    - Current: `MIN_DAY_HOURS = 2.0` hours.

2. Electrical-only worker exclusion
    - If department is Electrical and the only worker that day is employee `1205797`(Chris Rawlins), drop that day.

3. Outlier edge gap trimming (first/last only)
    - For each department’s ordered days, check first→second and previous→last gaps. If a gap exceeds the per-dept cap AND that day has <2 distinct employees, drop the edge day. If ≥2 employees, keep regardless of gap.
    - Caps (days): Assembly 12, BaseFormPaint 7, Crating 7, DoorFab 7, Electrical 7, Fab 13, FanAssyTest 22, InsulWallFab 11, Paint 7, Pipe 7, Welding 8.

## Unit (COM) Completion & Metrics

**Unit Complete:** A unit is complete when **all** 11 completion-tracked departments with std > 0 have completion = 100%.

**Unit Metrics** (computed only when unit complete):

- **Unit First Day:** MIN(Department First Day) across all eligible departments
- **Unit Last Day:** MAX(Department Last Day) across all eligible departments  
- **Unit Actual Days Worked:** Count of distinct kept charge dates where any eligible department had charges (after filtering)
- **Unit Time Span:** Calendar span inclusive: `(Unit Last Day - Unit First Day) + 1`

**Department Metrics** (computed only when department completion = 100%):

- **Department First/Last Day:** Earliest/latest kept charge date after applying filtering rules
- **Department Actual Days Worked:** Count of distinct kept charge dates
- **Department Time Span:** Calendar span inclusive between first and last day

If department is 100% complete but has no kept labor rows after filtering, all metrics are NULL.

## Averages displayed

- Two COM sets:
   - Last 10 “completed” units (approximated by most recent last-day).
   - Last 90 calendar days: COMs whose last-day is within the last 90 days.

For each set:

- Average actual days worked: mean of `active_days` per COM.
- Average total time span: mean of `span_days` per COM.
- Average overall efficiency: mean of per‑COM overall efficiency (see below).

## Overall efficiency per COM (rollup from summary)

Per department:

- Completion fallback when missing/invalid (`comp ≤ 0` or `comp > 100`) and `std > 0`:
  
   `comp = min(100, (act/std)*100)`

- Earned hours: `EH = (comp/100) * std`

Per COM:

- `overall_efficiency = (sum(EH) / sum(act)) * 100`

Notes:
- Completion fallback is capped at 100%. Efficiency is not explicitly capped.
- Departments absent in the summary contribute zero to both std and act.

## COM page Gantt (quick reference)

- Rows: one per department code that has any charges for the COM.
- Columns: every day from earliest to latest charge.
- Cells: lit on charged days; color intensity scales with total hours that day.
- Hover: shows department, date, total hours, number of charges, and distinct employees.


