import os
import csv
import sqlite3
import subprocess
import json
import calendar
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Tuple

DB_PATH = os.path.join(os.path.dirname(__file__), 'SCHLabor.db')
BASE_DIR = os.path.dirname(__file__)
ALLOW_SCHLABOR_SCHEMA_MUTATIONS = (os.environ.get('ALLOW_SCHLABOR_SCHEMA_MUTATIONS','0') == '1')
LABOR_REPROCESS_LAST_DAY = (os.environ.get('LABOR_REPROCESS_LAST_DAY','0') == '1')  # legacy flag
LABOR_REPROCESS_TRAILING_DAYS = max(1, int(os.environ.get('LABOR_REPROCESS_TRAILING_DAYS','1')))  # number of most recent days to always reprocess (default 1)
# Optional runtime hook for GUI progress if caller does not pass explicit callback.
LABOR_PROGRESS_CALLBACK = None  # signature: (phase:str, info:dict) -> None

SCH_SCHED_SCRIPT = os.path.join(BASE_DIR, 'Get-SCHSchedulingSummary.ps1')
SCH_LABOR_RANGE_SCRIPT = os.path.join(BASE_DIR, 'Get-SCHLaborRange.ps1')

# ---------------- DB Helpers ---------------- #

def get_conn():
    return sqlite3.connect(DB_PATH)

def _qi(name: str) -> str:
    """Quote an identifier for SQLite (handles spaces and special characters)."""
    s = str(name).replace('"', '""')
    return f'"{s}"'

def ensure_change_log_tables():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS RunLog (
              id INTEGER PRIMARY KEY,
              run_started TEXT NOT NULL,
              run_completed TEXT,
              run_type TEXT NOT NULL, -- 'SchedulingSummary' | 'Labor'
              success INTEGER DEFAULT 0,
              message TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ChangeLog (
              id INTEGER PRIMARY KEY,
              run_id INTEGER NOT NULL,
              comnumber1 INTEGER NOT NULL,
              column_name TEXT NOT NULL,
              old_value TEXT,
              new_value TEXT,
              FOREIGN KEY(run_id) REFERENCES RunLog(id)
            );
        """)
        if ALLOW_SCHLABOR_SCHEMA_MUTATIONS:
            _ensure_labor_unique_index(cur)
    conn.commit()

# --------------- Helpers --------------- #

def _parse_any_date(s: str):
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    for fmt in ('%Y-%m-%d','%m/%d/%Y','%Y/%m/%d','%m/%d/%y'):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except Exception:
            pass
    return None

def _get_report_server_root() -> str | None:
    # Priority: environment variable, then config.json {"report_server_root": "http://host/ReportServer"}
    root = os.environ.get('SSRS_REPORTSERVER_ROOT') or os.environ.get('REPORT_SERVER_ROOT')
    if root:
        return root
    cfg = os.path.join(BASE_DIR, 'config.json')
    if os.path.isfile(cfg):
        try:
            with open(cfg, 'r', encoding='utf-8') as f:
                data = json.load(f)
            root = data.get('report_server_root') or data.get('ReportServerRoot')
            if root:
                return root
        except Exception:
            pass
    return None

# ---------------- Scheduling Summary Upsert ---------------- #

def _discover_latest_sched_file(start: date, end: date) -> str:
    expected = f"SCHSchedulingSummaryReport_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.csv"
    path = os.path.join(BASE_DIR, expected)
    return path if os.path.isfile(path) else ''

def _run_powershell(script_path: str, args: List[str]) -> Tuple[int, str]:
    cmd = ["powershell", "-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", script_path] + args
    try:
        completed = subprocess.run(cmd, capture_output=True, text=True, cwd=BASE_DIR, timeout=60*30)
        return completed.returncode, (completed.stdout + '\n' + completed.stderr)
    except Exception as e:
        return 1, str(e)

def ensure_sched_table(columns: List[str]):
    # comnumber1 must be first column; create table with columns if needed, add missing columns otherwise
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='SCHSchedulingSummary'")
        exists = cur.fetchone() is not None
        if not exists:
            col_defs = []
            for c in columns:
                if c == 'comnumber1':
                    col_defs.append(f'{_qi("comnumber1")} INTEGER PRIMARY KEY')
                else:
                    # heuristic numeric vs text
                    if c.endswith('hrs') or c in ('height','sqft','code','shipmonth','indoor','outdoor','flowline','emb_new','flow_new','med_new','ol_new','mmp','sppp','lau','vfd','alum','airflow','leaktest','deflection'):
                        col_defs.append(f'{_qi(c)} REAL')
                    else:
                        col_defs.append(f'{_qi(c)} TEXT')
            ddl = 'CREATE TABLE SCHSchedulingSummary (' + ','.join(col_defs) + ')'
            cur.execute(ddl)
        else:
            # add any missing columns
            cur.execute("PRAGMA table_info(SCHSchedulingSummary)")
            existing_cols = {r[1] for r in cur.fetchall()}
            for c in columns:
                if c not in existing_cols:
                    cur.execute(f'ALTER TABLE SCHSchedulingSummary ADD COLUMN {_qi(c)} TEXT')
            # Ensure a unique index (or PK) exists on comnumber1 so upsert works
            cur.execute("PRAGMA table_info(SCHSchedulingSummary)")
            pk_present = any(r[1] == 'comnumber1' and r[5] == 1 for r in cur.fetchall())  # r[5]=pk flag
            if not pk_present:
                # Check for existing unique index
                cur.execute("PRAGMA index_list(SCHSchedulingSummary)")
                indexes = cur.fetchall() or []
                has_unique = False
                for ix in indexes:
                    # ix: (seq, name, unique, origin, partial)
                    if len(ix) >= 3 and ix[2] == 1:
                        # inspect columns of index
                        iname = ix[1]
                        cur.execute(f"PRAGMA index_info('{iname}')")
                        cols = [c[2] for c in cur.fetchall()]
                        if cols == ['comnumber1']:
                            has_unique = True
                            break
                if not has_unique:
                    # Deduplicate before creating unique index (keep lowest rowid per comnumber1)
                    try:
                        cur.execute("DELETE FROM SCHSchedulingSummary WHERE rowid NOT IN (SELECT MIN(rowid) FROM SCHSchedulingSummary GROUP BY comnumber1)")
                        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_sched_comnumber1 ON SCHSchedulingSummary(\"comnumber1\")")
                    except sqlite3.OperationalError:
                        # As fallback, ignore; subsequent upserts will still fail but we tried
                        pass
        conn.commit()

def upsert_sched_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """(Legacy) Retained for backward compatibility; now delegates to upsert_sched_rows_with_stats.

    Returns list of change records (new rows -> one '*NEW*' record each; updated rows -> per changed column).
    """
    res = upsert_sched_rows_with_stats(rows)
    return res['change_records']

def upsert_sched_rows_with_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Upsert rows with per-row change detection.

    Returns dict with keys:
      change_records: list (for ChangeLog)
      new_rows: int (count of brand new COMs inserted)
      updated_rows: int (count of existing COMs where at least one column changed)
      skipped_rows: int (count of existing COMs where no column changed)
      changed_columns: int (total individual column updates across all COMs)
    """
    if not rows:
        return {'change_records': [], 'new_rows': 0, 'updated_rows': 0, 'skipped_rows': 0, 'changed_columns': 0}

    # Allow override strategy: diff (default) or bulk (always update existing rows)
    update_mode = (os.environ.get('SCHSUMMARY_UPDATE_MODE') or 'diff').lower().strip()
    if update_mode not in ('diff','bulk'):
        update_mode = 'diff'

    def _canonical(val: Any) -> str:
        """Return a canonical string for comparison so '0' == '0.0', trimming float noise.

        - None/'' -> ''
        - Numeric strings coerced to float then formatted; integers keep no decimal.
        - Other values stripped of outer whitespace.
        """
        if val is None:
            return ''
        s = str(val).strip()
        if s == '':
            return ''
        # Fast path: digits / optional decimal / optional sign
        try:
            # Reject if contains letters
            if any(c.isalpha() for c in s):
                return s
            f = float(s)
            if abs(f - int(f)) < 1e-9:
                return str(int(f))
            # Limit precision to avoid binary float artifacts, strip trailing zeros
            return ('{0:.6f}'.format(f)).rstrip('0').rstrip('.')
        except Exception:
            return s
    columns = list(rows[0].keys())
    ensure_sched_table(columns)
    change_records: List[Dict[str, Any]] = []
    new_rows = 0
    updated_rows = 0
    skipped_rows = 0
    changed_columns = 0
    with get_conn() as conn:
        cur = conn.cursor()
        col_list_sql = ','.join([_qi(c) for c in columns])
        # Build dynamic UPDATE only when needed (we'll issue UPDATE manually instead of using ON CONFLICT to be able to skip unchanged rows)
        select_sql = f'SELECT {col_list_sql} FROM SCHSchedulingSummary WHERE {_qi("comnumber1")}=?'
        insert_sql = f'INSERT INTO SCHSchedulingSummary ({col_list_sql}) VALUES ({','.join(['?']*len(columns))})'
        # Prebuild update set clause
        update_clause = ','.join([f'{_qi(c)}=?' for c in columns if c != 'comnumber1'])
    update_cols = [c for c in columns if c != 'comnumber1']
    update_sql = f'UPDATE SCHSchedulingSummary SET {update_clause} WHERE {_qi("comnumber1")}=?'
    for r in rows:
            key = r['comnumber1']
            cur.execute(select_sql, (key,))
            before = cur.fetchone()
            if not before:
                # New row
                values = [r[c] for c in columns]
                cur.execute(insert_sql, values)
                change_records.append({'comnumber1': key, 'column': '*NEW*', 'old': None, 'new': 'INSERTED'})
                new_rows += 1
                continue
            before_map = dict(zip(columns, before))
            # Detect diffs
            row_changes = []
            for c in columns:
                if c == 'comnumber1':
                    continue
                old_val = before_map.get(c)
                new_val = r[c]
                if _canonical(old_val) != _canonical(new_val):
                    row_changes.append((c, old_val, new_val))
            if update_mode == 'bulk':
                # Always update existing rows regardless of diff (treat as updated if any canonical diff vs none?)
                update_vals = [r[c] for c in update_cols] + [key]  # full set update
                cur.execute(update_sql, update_vals)
                if row_changes:
                    updated_rows += 1
                    for c, old_val, new_val in row_changes:
                        changed_columns += 1
                        change_records.append({'comnumber1': key, 'column': c, 'old': old_val, 'new': new_val})
                else:
                    skipped_rows += 1  # no actual value change
                continue
            # diff mode
            if not row_changes:
                skipped_rows += 1
                continue  # no DB write required
            # Perform UPDATE only for changed columns
            set_clause = ','.join([f"{_qi(c)}=?" for c, _, _ in row_changes])
            dyn_sql = f"UPDATE SCHSchedulingSummary SET {set_clause} WHERE {_qi('comnumber1')}=?"
            update_vals = [r[c] for c, _, _ in row_changes] + [key]
            cur.execute(dyn_sql, update_vals)
            updated_rows += 1
            for c, old_val, new_val in row_changes:
                changed_columns += 1
                change_records.append({'comnumber1': key, 'column': c, 'old': old_val, 'new': new_val})
    conn.commit()
    return {
        'change_records': change_records,
        'new_rows': new_rows,
        'updated_rows': updated_rows,
        'skipped_rows': skipped_rows,
        'changed_columns': changed_columns,
        'update_mode': update_mode
    }

def parse_cleaned_csv(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, newline='', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            # leave values as-is; could normalize numeric
            rows.append(row)
    return rows

def update_scheduling_summary() -> Dict[str, Any]:
    """Pull a 150-day window (-90 to +60 days) and upsert with detailed stats.

    Behavior:
      - For each COMNumber (comnumber1) in the window: insert if new; if exists compare each column.
      - Only perform UPDATE when at least one column changed.
      - Track counts: new_rows, updated_rows, skipped_rows (unchanged existing), changed_columns.
      - Record per-column changes in ChangeLog; new rows recorded with a single '*NEW*' entry.
    """
    ensure_change_log_tables()
    start = date.today() - timedelta(days=90)
    end = date.today() + timedelta(days=60)
    run_id = None
    run_started = datetime.utcnow().isoformat()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO RunLog(run_started, run_type, success) VALUES(?,?,0)", (run_started, 'SchedulingSummary'))
        run_id = cur.lastrowid
        conn.commit()
    # fetch report
    server_root = _get_report_server_root()
    if not server_root:
        msg = 'ReportServerRoot not configured. Set env SSRS_REPORTSERVER_ROOT or create config.json.'
        _finalize_run(run_id, False, msg)
        return {'ok': False, 'error': msg}
    # Explicitly pass correct path & param names (latest known) to avoid stale script defaults
    args = [
        "-StartDate", start.strftime('%Y-%m-%d'),
        "-EndDate", end.strftime('%Y-%m-%d'),
        '-ReportPath', '/Custom/Production Control/SCHSchedulingSummaryReport',
        '-StartParamName', 'SHIP_DATE_START',
        '-EndParamName', 'SHIP_DATE_END',
        '-ReportServerRoot', server_root
    ]
    rc, output = _run_powershell(SCH_SCHED_SCRIPT, args)
    if rc != 0:
        _finalize_run(run_id, False, f'PS error: {output[:500]}')
        return {'ok': False, 'error': output}
    report_file = _discover_latest_sched_file(start, end)
    if not report_file:
        _finalize_run(run_id, False, 'Report file not found after download.')
        return {'ok': False, 'error': 'Report file missing'}
    # run cleaning
    try:
        from clean import convert_file1_to_cleaned
        raw_clean_out = os.path.join(BASE_DIR, 'cleaned_file.csv')
        convert_file1_to_cleaned(report_file, raw_clean_out)
    except Exception as e:
        _finalize_run(run_id, False, f'clean.py failed: {e}')
        return {'ok': False, 'error': str(e)}
    # load cleaned and upsert with stats
    try:
        rows = parse_cleaned_csv(os.path.join(BASE_DIR, 'cleaned_file.csv'))
        upsert_res = upsert_sched_rows_with_stats(rows)
        changes = upsert_res['change_records']
        _record_changes(run_id, changes)
        summary_msg = (f"rows={len(rows)} new={upsert_res['new_rows']} "
                       f"updated={upsert_res['updated_rows']} skipped={upsert_res['skipped_rows']} "
                       f"changed_cols={upsert_res['changed_columns']}")
        _finalize_run(run_id, True, summary_msg)
        return {
            'ok': True,
            'rows': len(rows),
            'new_rows': upsert_res['new_rows'],
            'updated_rows': upsert_res['updated_rows'],
            'skipped_rows': upsert_res['skipped_rows'],
            'changed_columns': upsert_res['changed_columns'],
            'changes': changes[:200]
        }
    except Exception as e:
        _finalize_run(run_id, False, f'Upsert failed: {e}')
        return {'ok': False, 'error': str(e)}

def _record_changes(run_id: int, changes: List[Dict[str, Any]]):
    if not changes:
        return
    with get_conn() as conn:
        cur = conn.cursor()
        cur.executemany("INSERT INTO ChangeLog(run_id, comnumber1, column_name, old_value, new_value) VALUES (?,?,?,?,?)",
                        [(run_id, c['comnumber1'], c['column'], c['old'], c['new']) for c in changes])
        conn.commit()

def _finalize_run(run_id: int, success: bool, message: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE RunLog SET run_completed=?, success=?, message=? WHERE id=?",
                    (datetime.utcnow().isoformat(), 1 if success else 0, message, run_id))
        conn.commit()

# ---------------- Labor Backfill ---------------- #

def labor_backfill(stop_event=None, progress=None, commit_every: int = 7) -> Dict[str, Any]:
    """Backfill SCHLabor new daily CSV files.

    Enhancements:
      - Uses iso_logged_date (if present) to determine the last ingested day.
      - Commits every `commit_every` days for resilience.
      - Emits explicit flag when a daily CSV is missing.

    stop_event: threading.Event (optional) to allow cooperative cancellation.
    progress: callback(phase:str, info:dict).
    commit_every: commit frequency in days (>=1).
    """
    cb = progress or LABOR_PROGRESS_CALLBACK
    def emit(phase, **info):
        if cb:
            try:
                cb(phase, info)
            except Exception:
                pass
    ensure_change_log_tables()
    run_started = datetime.utcnow().isoformat()
    with get_conn() as conn:
        cur = conn.cursor()
        if ALLOW_SCHLABOR_SCHEMA_MUTATIONS:
            _ensure_labor_table(cur)
        cur.execute("INSERT INTO RunLog(run_started, run_type, success) VALUES(?,?,0)", (run_started, 'Labor'))
        run_id = cur.lastrowid
        last = None
        # Prefer fast iso_logged_date lookup
        try:
            cur.execute("SELECT MAX(iso_logged_date) FROM SCHLabor WHERE iso_logged_date IS NOT NULL")
            r = cur.fetchone()
            if r and r[0]:
                last = datetime.strptime(r[0], '%Y-%m-%d').date()
        except Exception:
            last = None
        # Fallback to robust parse of LoggedDate if iso not available
        if not last:
            try:
                cur.execute("SELECT DISTINCT LoggedDate FROM SCHLabor WHERE LoggedDate IS NOT NULL")
                parsed_dates = []
                for (dval,) in cur.fetchall():
                    try:
                        pd = _parse_any_date(dval)
                        if pd:
                            parsed_dates.append(pd)
                    except Exception:
                        pass
                if parsed_dates:
                    last = max(parsed_dates)
            except Exception:
                last = None
    today = date.today()
    # Determine start. We must allow incomplete recent days to fill in.
    # Strategy:
    #   Always reprocess the most recent N days (LABOR_REPROCESS_TRAILING_DAYS, default 1).
    #   Backward compat: LABOR_REPROCESS_LAST_DAY=1 forces at least 1 trailing day (already covered).
    #   If no prior data, seed a 60-day historical window (non-overlapping reprocessing not needed yet).
    if last:
        trailing = max(1, LABOR_REPROCESS_TRAILING_DAYS)  # ensure >=1
        # start at (last - trailing + 1) so we include 'last' day and (trailing-1) previous days
        start = last - timedelta(days=trailing - 1)
        if start > today:  # safety
            start = today
    else:
        start = today - timedelta(days=60)
    end = today
    reprocessing_today_only = (start == today and last == today)
    if start > end:  # defensive, should not occur
        _finalize_run(run_id, True, 'No new days to backfill.')
        emit('noop', start=start.isoformat(), end=end.isoformat())
        return {'ok': True, 'inserted': 0, 'message': 'Up to date', 'start': start.isoformat(), 'end': end.isoformat(), 'reprocess_today': False}
    emit('init', start=start.isoformat(), end=end.isoformat(), last_iso=last.isoformat() if last else None, reprocess_today=reprocessing_today_only)
    rc, output = _run_powershell(SCH_LABOR_RANGE_SCRIPT, ["-StartDate", start.strftime('%Y-%m-%d'), "-EndDate", end.strftime('%Y-%m-%d')])
    if rc != 0:
        _finalize_run(run_id, False, f'PS error: {output[:500]}')
        emit('error', message='PowerShell failure', detail=output[:500])
        return {'ok': False, 'error': output}
    inserted = 0
    cur_day = start
    day_index = 0
    commit_every = max(1, int(commit_every))
    with get_conn() as conn:
        cur = conn.cursor()
        # Ensure unique index for robust dedupe (idempotent)
        try:
            cur.execute("PRAGMA index_list('SCHLabor')")
            existing = {r[1] for r in cur.fetchall()}
            if 'UX_SCHLabor_FullRow' not in existing:
                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS UX_SCHLabor_FullRow
                    ON SCHLabor(LoggedDate, COMNumber, EmployeeName, EmployeeNumber, DepartmentNumber, Area, ActualHours, Reference)
                """)
        except Exception:
            pass
    first_day = True
    while cur_day <= end:
            day_index += 1
            if stop_event and stop_event.is_set():
                emit('stopped', date=cur_day.isoformat())
                conn.commit()
                _finalize_run(run_id, True, f'Stopped early; inserted {inserted}')
                return {'ok': True, 'inserted': inserted, 'stopped': True, 'start': start.isoformat(), 'end': end.isoformat()}
            fn = os.path.join(BASE_DIR, f'SCHLabor_{cur_day.strftime("%Y%m%d")}.csv')
            existed = os.path.isfile(fn)
            added = 0
            if existed:
                # Always reprocess each day's file (including past days and today). Full-row dedupe in _import_labor_csv prevents duplicates.
                added = _import_labor_csv(cur, fn)
                inserted += added
            else:
                # Missing file explicitly noted
                emit('missing-file', date=cur_day.isoformat(), file=fn)
            emit('day', date=cur_day.isoformat(), file=fn if existed else None, missing=(not existed), inserted_today=added, total_inserted=inserted)
            if day_index % commit_every == 0:
                conn.commit()
                emit('checkpoint', date=cur_day.isoformat(), committed=True, total_inserted=inserted, day_index=day_index)
            first_day = False
            cur_day += timedelta(days=1)
    conn.commit()
    _finalize_run(run_id, True, f'Inserted {inserted} new labor rows (commit_every={commit_every})')
    emit('done', inserted=inserted, commit_every=commit_every, reprocess_today=reprocessing_today_only)
    return {'ok': True, 'inserted': inserted, 'start': start.isoformat(), 'end': end.isoformat(), 'commit_every': commit_every, 'reprocess_today': reprocessing_today_only}

def _import_labor_csv(cur: sqlite3.Cursor, path: str) -> int:
    _ensure_iso_column(cur)
    try:
        with open(path, encoding='utf-8') as f:
            lines = f.read().splitlines()
        # find header line starting with LoggedDate
        header_idx = None
        for i, ln in enumerate(lines):
            if ln.startswith('LoggedDate'):
                header_idx = i
                break
        if header_idx is None or header_idx == len(lines)-1:
            return 0
        raw_header = [h.strip() for h in lines[header_idx].split(',')]
        header = raw_header[:]
        emp_col = _labor_emp_col(cur)
        file_emp_col = 'EmployeeNumber1' if 'EmployeeNumber1' in header else ('EmployeeNumber' if 'EmployeeNumber' in header else None)
        inserted = 0
        for line in lines[header_idx+1:]:
            if not line.strip():
                continue
            parts = list(csv.reader([line]))[0]
            if len(parts) != len(header):
                continue
            row = dict(zip(header, parts))
            raw_date = row.get('LoggedDate')
            try:
                dt = datetime.strptime(raw_date[:10], '%Y-%m-%d') if '-' in raw_date[:10] else datetime.strptime(raw_date[:10], '%m/%d/%Y')
                iso = dt.strftime('%Y-%m-%d')
            except Exception:
                iso = None
            try:
                hrs = float(row.get('ActualHours')) if row.get('ActualHours') not in (None,'') else None
            except ValueError:
                hrs = None
            # Canonicalize text fields (strip whitespace) so identical rows are recognized
            emp_val = (row.get(file_emp_col) or '').strip() if file_emp_col else None
            employee_name = (row.get('EmployeeName') or '').strip() or None
            dept_val = (row.get('DepartmentNumber') or '').strip() or None
            area_val = (row.get('Area') or '').strip() or None
            ref_val = (row.get('Reference') or '').strip() or None
            insert_cols = [
                'LoggedDate','COMNumber','EmployeeName',emp_col,'DepartmentNumber','Area','ActualHours','Reference','iso_logged_date'
            ]
            vals = [iso if iso else raw_date[:10], (row.get('COMNumber') or '').strip() or None, employee_name, emp_val or None,
                    dept_val, area_val, hrs, ref_val, iso]
            placeholders = ','.join(['?']*len(insert_cols))
            col_list = ','.join(insert_cols)
            # Use TRIM on text columns to avoid duplicates from whitespace differences; numeric compare for hours.
            cur.execute(f"""
                INSERT INTO SCHLabor ({col_list})
                SELECT {placeholders}
                WHERE NOT EXISTS (
                  SELECT 1 FROM SCHLabor WHERE
                    LoggedDate = ? AND
                    IFNULL(TRIM(COMNumber),'') = IFNULL(TRIM(?), '') AND
                    IFNULL(TRIM(EmployeeName),'') = IFNULL(TRIM(?), '') AND
                    IFNULL(TRIM({emp_col}),'') = IFNULL(TRIM(?), '') AND
                    IFNULL(TRIM(DepartmentNumber),'') = IFNULL(TRIM(?), '') AND
                    IFNULL(TRIM(Area),'') = IFNULL(TRIM(?), '') AND
                    ( (ActualHours IS NULL AND ? IS NULL) OR (ActualHours = ?) ) AND
                    IFNULL(TRIM(Reference),'') = IFNULL(TRIM(?), '')
                )
            """, vals + [
                vals[0],  # LoggedDate
                vals[1],  # COMNumber
                vals[2],  # EmployeeName
                vals[3],  # EmployeeNumber / EmployeeNumber1
                vals[4],  # DepartmentNumber
                vals[5],  # Area
                vals[6],  # ActualHours for NULL compare
                vals[6],  # ActualHours for equality compare
                vals[7],  # Reference
            ])
            if cur.rowcount == 1:
                inserted += 1
        return inserted
    except Exception:
        return 0

def ensure_labor_unique_index():
    """Create a full-row unique index (excluding iso_logged_date) to guarantee dedupe at the DB layer.
    NOTE: NULLs are treated as distinct by SQLite UNIQUE; we mitigate by trimming and leaving real NULLs only for truly missing.
    """
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("PRAGMA index_list('SCHLabor')")
            existing = {r[1] for r in cur.fetchall()}
            if 'UX_SCHLabor_FullRow' not in existing:
                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS UX_SCHLabor_FullRow
                    ON SCHLabor(LoggedDate, COMNumber, EmployeeName, EmployeeNumber, DepartmentNumber, Area, ActualHours, Reference)
                """)
                conn.commit()
    except Exception:
        pass

def _ensure_iso_column(cur: sqlite3.Cursor):
    try:
        cur.execute("PRAGMA table_info(SCHLabor)")
        cols=[r[1] for r in cur.fetchall()]
        if 'iso_logged_date' not in cols:
            cur.execute("ALTER TABLE SCHLabor ADD COLUMN iso_logged_date TEXT")
    except Exception:
        pass

def reimport_existing_labor_files(start: date, end: date, progress=None) -> int:
    """Re-scan existing daily SCHLabor_YYYYMMDD.csv files between start and end (inclusive)
    and attempt to insert any rows missed previously (e.g., due to header mismatch EmployeeNumber1).
    Returns number of rows newly inserted.
    """
    cb = progress
    def emit(day, added):
        if cb:
            try:
                cb('reimport-day', {'date': day.isoformat(), 'added': added})
            except Exception:
                pass
    inserted = 0
    with get_conn() as conn:
        cur = conn.cursor()
        d = start
        while d <= end:
            fn = os.path.join(BASE_DIR, f'SCHLabor_{d.strftime("%Y%m%d")}.csv')
            if os.path.isfile(fn):
                added = _import_labor_csv(cur, fn)
                inserted += added
                emit(d, added)
            d += timedelta(days=1)
        conn.commit()
    return inserted

def reimport_all_labor_files(progress=None, commit_every: int = 25) -> dict:
    """Scan the working directory for all SCHLabor_YYYYMMDD.csv files and (re)import
    any rows not already present. Does NOT delete anything; purely additive with
    full-row dedupe. Returns a dict with counts.

    progress callback(phase, info) phases:
      'scan'  -> info={'files': N}
      'file'  -> info={'file': name, 'date': 'YYYY-MM-DD', 'added': x, 'index': i, 'total_files': N, 'total_inserted': T}
      'done'  -> info={'files': N, 'inserted': T}
    """
    import re
    pat = re.compile(r'^SCHLabor_(\d{8})\.csv$')
    cb = progress
    def emit(phase, **info):
        if cb:
            try:
                cb(phase, info)
            except Exception:
                pass
    files = []
    for fn in os.listdir(BASE_DIR):
        m = pat.match(fn)
        if m:
            try:
                d = datetime.strptime(m.group(1), '%Y%m%d').date()
            except Exception:
                continue
            files.append((d, fn))
    files.sort()
    emit('scan', files=len(files))
    inserted_total = 0
    processed = 0
    with get_conn() as conn:
        cur = conn.cursor()
        for idx, (d, fn) in enumerate(files, start=1):
            path = os.path.join(BASE_DIR, fn)
            added = _import_labor_csv(cur, path)
            inserted_total += added
            processed += 1
            emit('file', file=fn, date=d.isoformat(), added=added, index=idx, total_files=len(files), total_inserted=inserted_total)
            if idx % commit_every == 0:
                conn.commit()
        conn.commit()
    emit('done', files=processed, inserted=inserted_total)
    return {'files': processed, 'inserted': inserted_total}

# --------------- Labor Unique Index / Duplicate Cleanup --------------- #

def _ensure_labor_unique_index(cur: sqlite3.Cursor):
    """Ensure composite unique index exists; if duplicates present, collapse them first.

    Uniqueness key treats NULL as '' so we build the index on expressions using IFNULL.
    If duplicates already exist (from historical loads prior to uniqueness enforcement), we
    retain the earliest row (lowest rowid) and delete the rest for each duplicate key.
    """
    # If table missing, nothing to do yet
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='SCHLabor'")
    if not cur.fetchone():
        return  # table absent; skip (won't auto-create unless flag true earlier)
    # Check if index already exists
    cur.execute("PRAGMA index_list('SCHLabor')")
    for row in cur.fetchall():
        # row format: seq, name, unique, origin, partial (SQLite 3.8+) possibly more in future
        name = row[1]
        if name == 'UX_SCHLabor_Row':
            return  # already enforced
    # Detect duplicates under the intended key (only if we are allowed to mutate)
    cur.execute("""
        SELECT LoggedDate,
               IFNULL(EmployeeNumber,'') AS Emp,
               IFNULL(COMNumber,'')      AS Com,
               IFNULL(DepartmentNumber,'') AS Dept,
               IFNULL(Reference,'')      AS Ref,
               COUNT(*) c
        FROM SCHLabor
        GROUP BY 1,2,3,4,5
        HAVING c > 1
    """)
    dups = cur.fetchall()
    if dups and ALLOW_SCHLABOR_SCHEMA_MUTATIONS:
        for logged, emp, com, dept, ref, _c in dups:
            # Keep the smallest rowid, delete others
            cur.execute("""
                SELECT rowid FROM SCHLabor
                WHERE LoggedDate=?
                  AND IFNULL(EmployeeNumber,'')=?
                  AND IFNULL(COMNumber,'')=?
                  AND IFNULL(DepartmentNumber,'')=?
                  AND IFNULL(Reference,'')=?
                ORDER BY rowid
            """, (logged, emp, com, dept, ref))
            rowids = [r[0] for r in cur.fetchall()]
            keep = rowids[0]
            for rid in rowids[1:]:
                cur.execute("DELETE FROM SCHLabor WHERE rowid=?", (rid,))
    # Now safe to create unique index
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS UX_SCHLabor_Row
        ON SCHLabor(LoggedDate, IFNULL(EmployeeNumber,''), IFNULL(COMNumber,''), IFNULL(DepartmentNumber,''), IFNULL(Reference,''));
    """)

def _ensure_labor_table(cur: sqlite3.Cursor):
    """Create SCHLabor table if it doesn't exist."""
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='SCHLabor'")
    if cur.fetchone():
        return
    cur.execute("""
        CREATE TABLE SCHLabor (
          Id INTEGER PRIMARY KEY,
          LoggedDate TEXT NOT NULL,
          COMNumber INTEGER,
          EmployeeName TEXT,
          EmployeeNumber INTEGER,
          DepartmentNumber TEXT,
            Area TEXT,
          ActualHours REAL,
          Reference TEXT
        )
    """)

def labor_diagnostics() -> Dict[str, Any]:
    """Return row count, date range, duplicate count, and whether unique index exists."""
    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute("SELECT COUNT(*), MIN(LoggedDate), MAX(LoggedDate) FROM SCHLabor")
            count, min_d, max_d = cur.fetchone()
        except sqlite3.OperationalError:
            return {'exists': False}
        # duplicates by composite key
        emp_col = _detect_emp_col(cur)
        cur.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT LoggedDate, IFNULL({emp_col},''), IFNULL(COMNumber,''), IFNULL(DepartmentNumber,''), IFNULL(Reference,''), COUNT(*) c
                FROM SCHLabor GROUP BY 1,2,3,4,5 HAVING c>1
            )
        """)
        dup_groups = cur.fetchone()[0]
        # index existence
        cur.execute("PRAGMA index_list('SCHLabor')")
        idx_exists = any(r[1] == 'UX_SCHLabor_Row' for r in cur.fetchall())
    return {
        'exists': True,
        'rows': count,
        'min_date': min_d,
        'max_date': max_d,
        'duplicate_key_groups': dup_groups,
        'unique_index': idx_exists
    }

# Helper to detect employee number column name
def _labor_emp_col(cur: sqlite3.Cursor) -> str:
    try:
        cur.execute("PRAGMA table_info(SCHLabor)")
        cols = [r[1] for r in cur.fetchall()]
        if 'EmployeeNumber1' in cols:
            return 'EmployeeNumber1'
        return 'EmployeeNumber'
    except Exception:
        return 'EmployeeNumber'

def _detect_emp_col(cur: sqlite3.Cursor) -> str:
    return _labor_emp_col(cur)

def _add_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    day = min(d.day, calendar.monthrange(y, m)[1])
    return date(y, m, day)

def batch_download_sched_summary(start: date, end: date, months_per_chunk: int = 3, skip_existing: bool = True, progress=None, stop_event=None) -> dict:
    """Download scheduling summary CSVs in multi-month chunks without loading into DB.

    Returns dict: { 'ok': bool, 'chunks': N, 'files': [...], 'skipped': K }
    progress callback phases: chunk-start, chunk-skip, chunk-done, error, all-done
    """
    if months_per_chunk < 1:
        months_per_chunk = 3
    cb = progress
    def emit(phase, **info):
        if cb:
            try: cb(phase, info)
            except Exception: pass
    files = []
    skipped = 0
    cur_start = start
    server_root = _get_report_server_root()
    while cur_start <= end:
        if stop_event and stop_event.is_set():
            emit('stopped', start=cur_start.isoformat())
            break
        # chunk end is one day before the date produced by adding months_per_chunk
        tentative = _add_months(cur_start, months_per_chunk)
        # ensure contiguous coverage: last day of chunk = tentative - 1 day
        chunk_end = tentative - timedelta(days=1)
        if chunk_end > end:
            chunk_end = end
        out_name = f"SCHSchedulingSummaryReport_{cur_start.strftime('%Y-%m-%d')}_{chunk_end.strftime('%Y-%m-%d')}.csv"
        out_path = os.path.join(BASE_DIR, out_name)
        if skip_existing and os.path.isfile(out_path):
            skipped += 1
            emit('chunk-skip', start=cur_start.isoformat(), end=chunk_end.isoformat(), file=out_name)
        else:
            emit('chunk-start', start=cur_start.isoformat(), end=chunk_end.isoformat())
            args = ["-StartDate", cur_start.strftime('%Y-%m-%d'), "-EndDate", chunk_end.strftime('%Y-%m-%d')]
            if server_root:
                args += ['-ReportServerRoot', server_root]
            rc, output = _run_powershell(SCH_SCHED_SCRIPT, args)
            if rc != 0 or not os.path.isfile(out_path):
                emit('error', start=cur_start.isoformat(), end=chunk_end.isoformat(), rc=rc, detail=output[:400])
                return {'ok': False, 'error': f'Failure {cur_start}..{chunk_end}', 'detail': output[:500], 'files': files}
            files.append(out_path)
            emit('chunk-done', start=cur_start.isoformat(), end=chunk_end.isoformat(), file=out_name)
        cur_start = chunk_end + timedelta(days=1)
    emit('all-done', files=len(files), skipped=skipped)
    return {'ok': True, 'chunks': len(files)+skipped, 'files': files, 'downloaded': len(files), 'skipped': skipped}

def backfill_sched_summary_overlapping(start: date, end: date, primary_months: int = 4, overlap_months: int = 1, progress=None, stop_event=None) -> dict:
    """Download, clean, and upsert scheduling summary in overlapping chunks.

    For each chunk:
      - Download a window of `primary_months` months (start .. endExclusive-1 day)
      - Next chunk start advances by (primary_months - overlap_months) months, giving overlap.
      - Clean and upsert rows; later chunks overwrite earlier COMNumbers (latest wins).

    Returns summary dict with chunk stats.
    Progress phases: chunk-start, download-done, clean-done, upsert-done, chunk-done, stopped, error, all-done.
    """
    if primary_months < 1: primary_months = 4
    if overlap_months < 0: overlap_months = 1
    stride_months = max(1, primary_months - overlap_months)  # months to advance per loop
    cb = progress
    def emit(phase, **info):
        if cb:
            try: cb(phase, info)
            except Exception: pass
    server_root = _get_report_server_root()
    cur_start = start
    chunks = []
    total_new = 0
    total_updates = 0
    total_changes = 0
    chunk_index = 0
    while cur_start <= end:
        if stop_event and stop_event.is_set():
            emit('stopped', next_start=cur_start.isoformat())
            break
        chunk_index += 1
        tentative = _add_months(cur_start, primary_months)
        chunk_end = tentative - timedelta(days=1)
        if chunk_end > end:
            chunk_end = end
        emit('chunk-start', index=chunk_index, start=cur_start.isoformat(), end=chunk_end.isoformat())
        # Download CSV for this chunk
        args = ["-StartDate", cur_start.strftime('%Y-%m-%d'), "-EndDate", chunk_end.strftime('%Y-%m-%d')]
        if server_root:
            args += ['-ReportServerRoot', server_root]
        rc, output = _run_powershell(SCH_SCHED_SCRIPT, args)
        out_file = f"SCHSchedulingSummaryReport_{cur_start.strftime('%Y-%m-%d')}_{chunk_end.strftime('%Y-%m-%d')}.csv"
        out_path = os.path.join(BASE_DIR, out_file)
        if rc != 0 or not os.path.isfile(out_path):
            emit('error', index=chunk_index, start=cur_start.isoformat(), end=chunk_end.isoformat(), rc=rc, detail=output[:400])
            return {
                'ok': False,
                'error': f'Download failed chunk {chunk_index} {cur_start}..{chunk_end}',
                'rc': rc,
                'detail': output[:500],
                'chunks_completed': chunk_index - 1
            }
        emit('download-done', index=chunk_index, file=out_file)
        # Clean file
        try:
            from clean import convert_file1_to_cleaned
            cleaned_tmp = os.path.join(BASE_DIR, 'cleaned_file.csv')  # reused temp
            convert_file1_to_cleaned(out_path, cleaned_tmp)
            emit('clean-done', index=chunk_index, cleaned=cleaned_tmp)
        except Exception as e:
            emit('error', index=chunk_index, phase='clean', message=str(e))
            return {'ok': False, 'error': f'Clean failed chunk {chunk_index}: {e}', 'chunks_completed': chunk_index - 1}
        # Parse & upsert
        try:
            rows = parse_cleaned_csv(cleaned_tmp)
            change_recs = upsert_sched_rows(rows)
            # classify changes
            new_rows = sum(1 for c in change_recs if c.get('column') == '*NEW*')
            updates = len(change_recs) - new_rows
            total_new += new_rows
            total_updates += updates
            total_changes += len(change_recs)
            emit('upsert-done', index=chunk_index, rows=len(rows), new=new_rows, updates=updates, changes=len(change_recs))
        except Exception as e:
            emit('error', index=chunk_index, phase='upsert', message=str(e))
            return {'ok': False, 'error': f'Upsert failed chunk {chunk_index}: {e}', 'chunks_completed': chunk_index - 1}
        chunks.append({'index': chunk_index, 'start': cur_start.isoformat(), 'end': chunk_end.isoformat(), 'rows': len(rows)})
        emit('chunk-done', index=chunk_index, start=cur_start.isoformat(), end=chunk_end.isoformat())
        # Advance start by stride months (ensuring overlap)
        cur_start = _add_months(cur_start, stride_months)
    emit('all-done', chunks=len(chunks), total_new=total_new, total_updates=total_updates, total_changes=total_changes)
    return {
        'ok': True,
        'chunks': chunks,
        'total_chunks': len(chunks),
        'new_rows': total_new,
        'updated_rows': total_updates,
        'total_changes': total_changes,
        'primary_months': primary_months,
        'overlap_months': overlap_months,
        'stride_months': stride_months
    }

if __name__ == '__main__':
    ensure_change_log_tables()
    print('Labor backfill:', labor_backfill())
    print('Scheduling summary update:', update_scheduling_summary())
