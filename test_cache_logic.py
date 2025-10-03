"""
Quick test: Check what span is being calculated for COM 19044 in the metrics cache logic.
"""
import sqlite3
import os
import sys
from datetime import date, timedelta
from collections import defaultdict

ROOT = os.path.dirname(__file__)
DB_PATH = os.path.join(ROOT, 'SCHLabor.db')

sys.path.insert(0, os.path.join(ROOT, 'webapp'))
from blueprints.utils import (
    TRACKED_DEPARTMENTS, COMPLETION_CHECK_DEPARTMENTS,
    normalize_com, fnum, _resolve_department_days
)

def get_conn():
    return sqlite3.connect(DB_PATH)

print("=" * 80)
print("VERIFY: COM 19044 Span in Metrics Cache Logic")
print("=" * 80)

# Load labor for COM 19044 only
raw_code_to_label = {
    '0120':'Fab','0140':'Welding','0180':'BaseFormPaint','0200':'FanAssyTest','0220':'InsulWallFab',
    '0230':'Pipe','0260':'Assembly','0270':'DoorFab','0280':'Assembly','0300':'Electrical','0320':'Pipe',
    '0340':'Paint','0360':'Test','0380':'Crating',
}
tracked_codes = sorted(raw_code_to_label.keys())
codes_sql = ','.join(f"'{c}'" for c in tracked_codes)

with get_conn() as conn:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Load labor data
    cur.execute(
        f"""
        SELECT CAST(COMNumber AS TEXT) com,
               DepartmentNumber dept,
               strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10))) day,
               EmployeeNumber1 emp,
               COALESCE(ActualHours,0) hrs
        FROM SCHLabor
        WHERE COALESCE(ActualHours,0) > 0
          AND DepartmentNumber IN ({codes_sql})
          AND CAST(COMNumber AS TEXT) IN ('19044', '019044')
    """
    )
    labor_rows = cur.fetchall()
    
    # Get completion status
    cur.execute('PRAGMA table_info(SCHSchedulingSummary)')
    colset = {r[1] for r in cur.fetchall()}
    base_needed = ['comnumber1']
    for _, stdc, actc, compc, effc in TRACKED_DEPARTMENTS:
        base_needed.extend([stdc, actc, compc, effc])
    present = [c for c in base_needed if c in colset]
    cols_sql = ','.join(f'"{c}"' for c in present) if present else 'comnumber1'
    
    cur.execute(
        f'SELECT {cols_sql} FROM SCHSchedulingSummary WHERE CAST(comnumber1 AS TEXT) IN (\'19044\', \'019044\')'
    )
    sched_rows = [dict(r) for r in cur.fetchall()]

print(f"Loaded {len(labor_rows)} labor rows")

# Build day_emp structure
day_emp = {}
for com, dept, day, emp, hrs in labor_rows:
    label = raw_code_to_label.get(dept)
    if not label or day is None:
        continue
    key = ('19044', label)
    dm = day_emp.setdefault(key, {})
    rec = dm.setdefault(day, {'emps': set(), 'hours': 0.0})
    if emp:
        rec['emps'].add(str(emp).strip())
    try:
        rec['hours'] += float(hrs) if hrs is not None else 0.0
    except:
        pass

# Build dept completion map
dept_completion_map = {}
for r in sched_rows:
    com = '19044'
    
    for label, stdc, actc, compc, effc in COMPLETION_CHECK_DEPARTMENTS:
        std = fnum(r.get(stdc)) if stdc in r else 0.0
        if std <= 0:
            dept_completion_map[(com, label)] = False
            continue
        
        comp = fnum(r.get(compc)) if compc in r else 0.0
        dept_complete = True
        if comp < 100.0:
            act = fnum(r.get(actc)) if actc in r else 0.0
            calc_comp = min(100.0, (act / std) * 100.0) if act > 0 else 0.0
            if calc_comp < 100.0:
                dept_complete = False
        
        dept_completion_map[(com, label)] = dept_complete

# Calculate unit metrics using SAME logic as _compute_trailing_trend_charts
tmp_by_com = defaultdict(lambda: {'days': set(), 'first': None, 'last': None})
com = '19044'

print(f"\n{'='*80}")
print(f"PROCESSING EACH DEPARTMENT:")
print(f"{'='*80}")

for (unit_com, label), daymap in day_emp.items():
    is_dept_complete = dept_completion_map.get((unit_com, label), False)
    
    # Apply filtering logic
    use_days, meta = _resolve_department_days(label, daymap, is_complete=is_dept_complete)
    
    print(f"\n{label}:")
    print(f"  Complete: {is_dept_complete}")
    print(f"  Raw days: {len(daymap)}")
    print(f"  Filtered days: {len(use_days)}")
    
    if not use_days:
        print(f"  ⚠️  All days filtered out!")
        continue
    
    first_day = min(use_days)
    last_day = max(use_days)
    
    print(f"  First: {first_day}")
    print(f"  Last: {last_day}")
    
    # Track for unit-level calc
    com_rec = tmp_by_com[com]
    com_rec['days'].update(use_days)
    if com_rec['first'] is None or first_day < com_rec['first']:
        com_rec['first'] = first_day
    if com_rec['last'] is None or last_day > com_rec['last']:
        com_rec['last'] = last_day

# Calculate final unit metrics
rec = tmp_by_com[com]
ds = sorted(rec['days'])
active_days = len(ds)
try:
    span_days = (date.fromisoformat(rec['last']) - date.fromisoformat(rec['first'])).days + 1
except:
    span_days = 0

print(f"\n{'='*80}")
print(f"FINAL UNIT METRICS (COM 19044):")
print(f"{'='*80}")
print(f"Total active days: {active_days}")
print(f"First day (across all depts): {rec['first']}")
print(f"Last day (across all depts): {rec['last']}")
print(f"Span: {span_days} days")

print(f"\n{'='*80}")
if span_days < 100:
    print(f"✅ SUCCESS: Span is {span_days} days (expected ~63 with gap filter)")
else:
    print(f"⚠️  PROBLEM: Span is {span_days} days (expected ~63 with gap filter)")
print(f"{'='*80}")
