"""
NEW standalone data sync application for:
  1. SCHLabor incremental backfill (no duplicates)
  2. SCHSchedulingSummary 90-day rolling (past 60 / future 30) pull + upsert + change detection

USAGE (run from project root):
  python new_data_sync_app.py labor            # backfill SCHLabor from last date to today
  python new_data_sync_app.py labor --days 90  # force backfill window length if table empty
  python new_data_sync_app.py sched            # 90-day scheduling summary upsert
  python new_data_sync_app.py both             # run labor then scheduling summary

Designed to be scheduled via Windows Task Scheduler without touching the existing Flask UI.
"""
from __future__ import annotations
import argparse, os, csv, sqlite3, subprocess, sys
from datetime import date, datetime, timedelta
from typing import List, Dict, Any

ROOT = os.path.dirname(__file__)
DB_PATH = os.path.join(ROOT, 'SCHLabor.db')
SCH_LABOR_RANGE_PS = os.path.join(ROOT, 'Get-SCHLaborRange.ps1')
SCH_SCHED_PS = os.path.join(ROOT, 'Get-SCHSchedulingSummary.ps1')

# ---------------- Utility ---------------- #
def ps_run(script: str, args: List[str], timeout_sec: int = 1800) -> tuple[int,str]:
    cmd = ["powershell","-NoLogo","-NoProfile","-ExecutionPolicy","Bypass","-File", script] + args
    try:
        cp = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT, timeout=timeout_sec)
        return cp.returncode, cp.stdout + cp.stderr
    except Exception as e:
        return 1, f"EXCEPTION {e}"

def connect():
    if not os.path.isfile(DB_PATH):
        raise SystemExit(f"DB not found: {DB_PATH}")
    return sqlite3.connect(DB_PATH)

# ---------------- Labor Backfill ---------------- #
def labor_last_date(cur: sqlite3.Cursor) -> date | None:
    cur.execute("SELECT MAX(LoggedDate) FROM SCHLabor")
    val = cur.fetchone()[0]
    if not val:
        return None
    try:
        return date.fromisoformat(val[:10])
    except Exception:
        return None

def labor_backfill(window_if_empty: int = 60) -> Dict[str,Any]:
    inserted_total = 0
    with connect() as conn:
        cur = conn.cursor()
        last = labor_last_date(cur)
    if last:
        start = last + timedelta(days=1)
    else:
        start = date.today() - timedelta(days=window_if_empty)
    end = date.today()
    if start > end:
        return {'ok': True, 'inserted': 0, 'message': 'Already current'}
    rc, out = ps_run(SCH_LABOR_RANGE_PS, ["-StartDate", start.strftime('%Y-%m-%d'), "-EndDate", end.strftime('%Y-%m-%d')])
    if rc != 0:
        return {'ok': False, 'error': out[:600]}
    cur_day = start
    with connect() as conn:
        cur = conn.cursor()
        _ensure_labor_unique_index(cur)
        while cur_day <= end:
            fn = os.path.join(ROOT, f"SCHLabor_{cur_day.strftime('%Y%m%d')}.csv")
            if os.path.isfile(fn):
                inserted_total += _import_labor(cur, fn)
            cur_day += timedelta(days=1)
        conn.commit()
    return {'ok': True, 'inserted': inserted_total, 'start': start.isoformat(), 'end': end.isoformat()}

def _ensure_labor_unique_index(cur: sqlite3.Cursor):
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS UX_SCHLabor_Row
                 ON SCHLabor(LoggedDate, COALESCE(EmployeeNumber,''), COALESCE(COMNumber,''), COALESCE(DepartmentNumber,''), COALESCE(Reference,''))""")

def _import_labor(cur: sqlite3.Cursor, path: str) -> int:
    try:
        with open(path, encoding='utf-8') as f:
            lines = f.read().splitlines()
        header_idx = next((i for i,l in enumerate(lines) if l.startswith('LoggedDate,')), None)
        if header_idx is None: return 0
        header = lines[header_idx].split(',')
        inserted_before = cur.rowcount
        for raw in lines[header_idx+1:]:
            if not raw.strip():
                continue
            parts = list(csv.reader([raw]))[0]
            if len(parts) != len(header):
                continue
            row = dict(zip(header, parts))
            iso = _normalize_date(row.get('LoggedDate',''))
            vals = [iso, row.get('COMNumber') or None, row.get('EmployeeName') or None, row.get('EmployeeNumber') or None,
                    row.get('DepartmentNumber') or None, row.get('Area') or None, row.get('ActualHours') or None,
                    row.get('Reference') or None]
            cur.execute("""INSERT OR IGNORE INTO SCHLabor
                        (LoggedDate, COMNumber, EmployeeName, EmployeeNumber, DepartmentNumber, Area, ActualHours, Reference)
                        VALUES (?,?,?,?,?,?,?,?)""", vals)
        return 0 if cur.rowcount == -1 else max(cur.rowcount - (inserted_before if inserted_before != -1 else 0), 0)
    except Exception:
        return 0

def _normalize_date(s: str) -> str:
    s = (s or '')[:10]
    for fmt in ('%Y-%m-%d','%m/%d/%Y','%Y/%m/%d'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except Exception:
            pass
    return s

# ---------------- Scheduling Summary 90-day Upsert ---------------- #
def sched_update() -> Dict[str,Any]:
    past_days = 60
    future_days = 30
    start = date.today() - timedelta(days=past_days)
    end = date.today() + timedelta(days=future_days)
    rc, out = ps_run(SCH_SCHED_PS, ["-StartDate", start.strftime('%Y-%m-%d'), "-EndDate", end.strftime('%Y-%m-%d')])
    if rc != 0:
        return {'ok': False, 'error': out[:600]}
    report_name = f"SCHSchedulingSummaryReport_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.csv"
    report_path = os.path.join(ROOT, report_name)
    if not os.path.isfile(report_path):
        return {'ok': False, 'error': 'Report CSV not found'}
    try:
        from clean import convert_file1_to_cleaned
        convert_file1_to_cleaned(report_path, os.path.join(ROOT, 'cleaned_file.csv'))
    except Exception as e:
        return {'ok': False, 'error': f'clean.py failed: {e}'}
    cleaned_path = os.path.join(ROOT, 'cleaned_file.csv')
    if not os.path.isfile(cleaned_path):
        return {'ok': False, 'error': 'cleaned_file.csv missing'}
    rows = _read_csv(cleaned_path)
    if not rows:
        return {'ok': True, 'rows': 0, 'changes': 0}
    columns = list(rows[0].keys())
    with connect() as conn:
        cur = conn.cursor()
        _ensure_sched_schema(cur, columns)
        _ensure_sched_unique(cur)
        existing_map = _load_existing_sched(cur, columns)
        changes = 0
        inserted = 0
        updated = 0
        for r in rows:
            key = r['comnumber1']
            prev = existing_map.get(key)
            placeholders = ','.join(['?']*len(columns))
            assign = ','.join([f"{c}=excluded.{c}" for c in columns if c != 'comnumber1'])
            sql = f"INSERT INTO SCHSchedulingSummary ({','.join(columns)}) VALUES ({placeholders}) ON CONFLICT(comnumber1) DO UPDATE SET {assign}"
            cur.execute(sql, [r[c] for c in columns])
            if prev is None:
                inserted += 1
            else:
                for c in columns:
                    if c == 'comnumber1':
                        continue
                    if (prev.get(c) or '') != (r.get(c) or ''):
                        changes += 1
                updated += 1
        conn.commit()
    return {'ok': True, 'rows': len(rows), 'inserted': inserted, 'updated': updated, 'changed_fields': changes}

def _ensure_sched_schema(cur: sqlite3.Cursor, columns: List[str]):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='SCHSchedulingSummary'")
    exists = cur.fetchone() is not None
    if not exists:
        col_defs = []
        for c in columns:
            if c == 'comnumber1':
                col_defs.append('comnumber1 INTEGER PRIMARY KEY')
            else:
                if c.endswith('hrs') or c in {'code','height','sqft','shipmonth','indoor','outdoor','flowline','emb_new','flow_new','med_new','ol_new','mmp','sppp','lau','vfd','alum','airflow','leaktest','deflection'}:
                    col_defs.append(f'{c} REAL')
                else:
                    col_defs.append(f'{c} TEXT')
        cur.execute('CREATE TABLE SCHSchedulingSummary (' + ','.join(col_defs) + ')')
    else:
        cur.execute('PRAGMA table_info(SCHSchedulingSummary)')
        existing = {r[1] for r in cur.fetchall()}
        for c in columns:
            if c not in existing:
                cur.execute(f'ALTER TABLE SCHSchedulingSummary ADD COLUMN {c} TEXT')

def _ensure_sched_unique(cur: sqlite3.Cursor):
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS UX_SCHSchedulingSummary_COM ON SCHSchedulingSummary(comnumber1)")
    cur.execute("SELECT comnumber1, COUNT(*) c FROM SCHSchedulingSummary GROUP BY comnumber1 HAVING c>1")
    dups = cur.fetchall()
    for com, _ in dups:
        cur.execute("SELECT rowid FROM SCHSchedulingSummary WHERE comnumber1=? ORDER BY rowid", (com,))
        rows = [r[0] for r in cur.fetchall()]
        for rid in rows[:-1]:
            cur.execute("DELETE FROM SCHSchedulingSummary WHERE rowid=?", (rid,))

def _load_existing_sched(cur: sqlite3.Cursor, columns: List[str]) -> Dict[str,Dict[str,str]]:
    cur.execute(f"SELECT {','.join(columns)} FROM SCHSchedulingSummary")
    out: Dict[str,Dict[str,str]] = {}
    for row in cur.fetchall():
        m = dict(zip(columns, row))
        out[m['comnumber1']] = m
    return out

def _read_csv(path: str):
    with open(path, newline='', encoding='utf-8') as f:
        import csv as _csv
        rdr = _csv.DictReader(f)
        return list(rdr)

# ---------------- CLI ---------------- #
def main():
    ap = argparse.ArgumentParser(description="NEW Data Sync App (labor backfill + scheduling summary upsert)")
    ap.add_argument('mode', choices=['labor','sched','both'], help='Which sync to run')
    ap.add_argument('--days', type=int, default=60, help='Backfill window if SCHLabor empty (default 60)')
    args = ap.parse_args()
    if args.mode in ('labor','both'):
        res = labor_backfill(args.days)
        print('[LABOR]', res)
        if not res.get('ok'): sys.exit(1)
    if args.mode in ('sched','both'):
        res2 = sched_update()
        print('[SCHED]', res2)
        if not res2.get('ok'): sys.exit(1)

if __name__ == '__main__':
    main()
"""
NEW standalone data sync application for:
  1. SCHLabor incremental backfill (no duplicates)
  2. SCHSchedulingSummary 90-day rolling (past 60 / future 30) pull + upsert + change detection

USAGE (run from project root):
  python new_data_sync_app.py labor            # backfill SCHLabor from last date to today
  python new_data_sync_app.py labor --days 90  # force backfill window length if table empty
  python new_data_sync_app.py sched            # 90-day scheduling summary upsert
  python new_data_sync_app.py both             # run labor then scheduling summary

Designed to be scheduled via Windows Task Scheduler without touching the existing Flask UI.
"""
from __future__ import annotations
import argparse, os, csv, sqlite3, subprocess, sys
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Iterable

ROOT = os.path.dirname(__file__)
DB_PATH = os.path.join(ROOT, 'SCHLabor.db')
SCH_LABOR_RANGE_PS = os.path.join(ROOT, 'Get-SCHLaborRange.ps1')
SCH_SCHED_PS = os.path.join(ROOT, 'Get-SCHSchedulingSummary.ps1')

# ---------------- Utility ---------------- #
def ps_run(script: str, args: List[str], timeout_sec: int = 1800) -> tuple[int,str]:
    cmd = ["powershell","-NoLogo","-NoProfile","-ExecutionPolicy","Bypass","-File", script] + args
    try:
        cp = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT, timeout=timeout_sec)
        return cp.returncode, cp.stdout + cp.stderr
    except Exception as e:
        return 1, f"EXCEPTION {e}"

def connect():
    if not os.path.isfile(DB_PATH):
        raise SystemExit(f"DB not found: {DB_PATH}")
    return sqlite3.connect(DB_PATH)

# ---------------- Labor Backfill ---------------- #
def labor_last_date(cur: sqlite3.Cursor) -> date | None:
    cur.execute("SELECT MAX(LoggedDate) FROM SCHLabor")
    val = cur.fetchone()[0]
    if not val:
        return None
    try:
        return date.fromisoformat(val[:10])
    except Exception:
        return None

def labor_backfill(window_if_empty: int = 60) -> Dict[str,Any]:
    inserted_total = 0
    with connect() as conn:
        cur = conn.cursor()
        last = labor_last_date(cur)
    if last:
        start = last + timedelta(days=1)
    else:
        start = date.today() - timedelta(days=window_if_empty)
    end = date.today()
    if start > end:
        return {'ok': True, 'inserted': 0, 'message': 'Already current'}
    rc, out = ps_run(SCH_LABOR_RANGE_PS, ["-StartDate", start.strftime('%Y-%m-%d'), "-EndDate", end.strftime('%Y-%m-%d')])
    if rc != 0:
        return {'ok': False, 'error': out[:600]}
    cur_day = start
    with connect() as conn:
        cur = conn.cursor()
        _ensure_labor_unique_index(cur)
        while cur_day <= end:
            fn = os.path.join(ROOT, f"SCHLabor_{cur_day.strftime('%Y%m%d')}.csv")
            if os.path.isfile(fn):
                inserted_total += _import_labor(cur, fn)
            cur_day += timedelta(days=1)
        conn.commit()
    return {'ok': True, 'inserted': inserted_total, 'start': start.isoformat(), 'end': end.isoformat()}

def _ensure_labor_unique_index(cur: sqlite3.Cursor):
    # Unique composite to prevent dup rows
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS UX_SCHLabor_Row
                 ON SCHLabor(LoggedDate, COALESCE(EmployeeNumber,''), COALESCE(COMNumber,''), COALESCE(DepartmentNumber,''), COALESCE(Reference,''))""")

def _import_labor(cur: sqlite3.Cursor, path: str) -> int:
    try:
        with open(path, encoding='utf-8') as f:
            lines = f.read().splitlines()
        header_idx = next((i for i,l in enumerate(lines) if l.startswith('LoggedDate,')), None)
        if header_idx is None: return 0
        header = lines[header_idx].split(',')
        inserted_before = cur.rowcount
        for raw in lines[header_idx+1:]:
            if not raw.strip():
                continue
            parts = list(csv.reader([raw]))[0]
            if len(parts) != len(header):
                continue
            row = dict(zip(header, parts))
            iso = _normalize_date(row.get('LoggedDate',''))
            vals = [iso, row.get('COMNumber') or None, row.get('EmployeeName') or None, row.get('EmployeeNumber') or None,
                    row.get('DepartmentNumber') or None, row.get('Area') or None, row.get('ActualHours') or None,
                    row.get('Reference') or None]
            cur.execute("""INSERT OR IGNORE INTO SCHLabor
                        (LoggedDate, COMNumber, EmployeeName, EmployeeNumber, DepartmentNumber, Area, ActualHours, Reference)
                        VALUES (?,?,?,?,?,?,?,?)""", vals)
        return 0 if cur.rowcount == -1 else max(cur.rowcount - (inserted_before if inserted_before != -1 else 0), 0)
    except Exception:
        return 0

def _normalize_date(s: str) -> str:
    s = (s or '')[:10]
    for fmt in ('%Y-%m-%d','%m/%d/%Y','%Y/%m/%d'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except Exception:
            pass
    return s

# ---------------- Scheduling Summary 90-day Upsert ---------------- #
def sched_update() -> Dict[str,Any]:
    past_days = 60
    future_days = 30
    start = date.today() - timedelta(days=past_days)
    end = date.today() + timedelta(days=future_days)
    rc, out = ps_run(SCH_SCHED_PS, ["-StartDate", start.strftime('%Y-%m-%d'), "-EndDate", end.strftime('%Y-%m-%d')])
    if rc != 0:
        return {'ok': False, 'error': out[:600]}
    report_name = f"SCHSchedulingSummaryReport_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.csv"
    report_path = os.path.join(ROOT, report_name)
    if not os.path.isfile(report_path):
        return {'ok': False, 'error': 'Report CSV not found'}
    # clean
    try:
        from clean import convert_file1_to_cleaned
        convert_file1_to_cleaned(report_path, os.path.join(ROOT, 'cleaned_file.csv'))
    except Exception as e:
        return {'ok': False, 'error': f'clean.py failed: {e}'}
    cleaned_path = os.path.join(ROOT, 'cleaned_file.csv')
    if not os.path.isfile(cleaned_path):
        return {'ok': False, 'error': 'cleaned_file.csv missing'}
    rows = _read_csv(cleaned_path)
    if not rows:
        return {'ok': True, 'rows': 0, 'changes': 0}
    columns = list(rows[0].keys())
    with connect() as conn:
        cur = conn.cursor()
        _ensure_sched_schema(cur, columns)
        _ensure_sched_unique(cur)
        existing_map = _load_existing_sched(cur, columns)
        changes = 0
        inserted = 0
        updated = 0
        for r in rows:
            key = r['comnumber1']
            prev = existing_map.get(key)
            placeholders = ','.join(['?']*len(columns))
            assign = ','.join([f"{c}=excluded.{c}" for c in columns if c != 'comnumber1'])
            sql = f"INSERT INTO SCHSchedulingSummary ({','.join(columns)}) VALUES ({placeholders}) ON CONFLICT(comnumber1) DO UPDATE SET {assign}"
            cur.execute(sql, [r[c] for c in columns])
            if prev is None:
                inserted += 1
            else:
                # detect column level changes
                for c in columns:
                    if c == 'comnumber1':
                        continue
                    if (prev.get(c) or '') != (r.get(c) or ''):
                        changes += 1
                updated += 1
        conn.commit()
    return {'ok': True, 'rows': len(rows), 'inserted': inserted, 'updated': updated, 'changed_fields': changes}

def _ensure_sched_schema(cur: sqlite3.Cursor, columns: List[str]):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='SCHSchedulingSummary'")
    exists = cur.fetchone() is not None
    if not exists:
        col_defs = []
        for c in columns:
            if c == 'comnumber1':
                col_defs.append('comnumber1 INTEGER PRIMARY KEY')
            else:
                # heuristics: hours or numeric flags
                if c.endswith('hrs') or c in {'code','height','sqft','shipmonth','indoor','outdoor','flowline','emb_new','flow_new','med_new','ol_new','mmp','sppp','lau','vfd','alum','airflow','leaktest','deflection'}:
                    col_defs.append(f'{c} REAL')
                else:
                    col_defs.append(f'{c} TEXT')
        cur.execute('CREATE TABLE SCHSchedulingSummary (' + ','.join(col_defs) + ')')
    else:
        cur.execute('PRAGMA table_info(SCHSchedulingSummary)')
        existing = {r[1] for r in cur.fetchall()}
        for c in columns:
            if c not in existing:
                cur.execute(f'ALTER TABLE SCHSchedulingSummary ADD COLUMN {c} TEXT')

def _ensure_sched_unique(cur: sqlite3.Cursor):
    # if table created earlier with surrogate id primary key, add unique index
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS UX_SCHSchedulingSummary_COM ON SCHSchedulingSummary(comnumber1)")
    # Optional: collapse duplicates if any
    cur.execute("SELECT comnumber1, COUNT(*) c FROM SCHSchedulingSummary GROUP BY comnumber1 HAVING c>1")
    dups = cur.fetchall()
    for com, _ in dups:
        cur.execute("SELECT rowid FROM SCHSchedulingSummary WHERE comnumber1=? ORDER BY rowid", (com,))
        rows = [r[0] for r in cur.fetchall()]
        # keep last
        for rid in rows[:-1]:
            cur.execute("DELETE FROM SCHSchedulingSummary WHERE rowid=?", (rid,))

def _load_existing_sched(cur: sqlite3.Cursor, columns: List[str]) -> Dict[str,Dict[str,str]]:
    cur.execute(f"SELECT {','.join(columns)} FROM SCHSchedulingSummary")
    out: Dict[str,Dict[str,str]] = {}
    for row in cur.fetchall():
        m = dict(zip(columns, row))
        out[m['comnumber1']] = m
    return out

def _read_csv(path: str) -> List[Dict[str,str]]:
    with open(path, newline='', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        return list(rdr)

# ---------------- CLI ---------------- #
def main():
    ap = argparse.ArgumentParser(description="NEW Data Sync App (labor backfill + scheduling summary upsert)")
    ap.add_argument('mode', choices=['labor','sched','both'], help='Which sync to run')
    ap.add_argument('--days', type=int, default=60, help='Backfill window if SCHLabor empty (default 60)')
    args = ap.parse_args()
    if args.mode in ('labor','both'):
        res = labor_backfill(args.days)
        print('[LABOR]', res)
        if not res.get('ok'): sys.exit(1)
    if args.mode in ('sched','both'):
        res2 = sched_update()
        print('[SCHED]', res2)
        if not res2.get('ok'): sys.exit(1)

if __name__ == '__main__':
    main()
