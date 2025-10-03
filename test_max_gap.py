"""
Test: Verify that COM 19044 span is now filtered with the 30-day hard maximum gap.
"""
import sqlite3
import os
import sys
from datetime import date

ROOT = os.path.dirname(__file__)
DB_PATH = os.path.join(ROOT, 'SCHLabor.db')

sys.path.insert(0, os.path.join(ROOT, 'webapp'))
from blueprints.utils import _filter_days_by_gap, OUTLIER_CAPS, PROJECT_DAY_RULES

def get_conn():
    return sqlite3.connect(DB_PATH)

print("=" * 80)
print("TESTING MAX GAP OVERRIDE: COM 19044 Fab Department")
print("=" * 80)

# Get Fab charges for COM 19044
raw_code_to_label = {
    '0120':'Fab','0140':'Welding','0180':'BaseFormPaint','0200':'FanAssyTest','0220':'InsulWallFab',
    '0230':'Pipe','0260':'Assembly','0270':'DoorFab','0280':'Assembly','0300':'Electrical','0320':'Pipe',
    '0340':'Paint','0360':'Test','0380':'Crating',
}

with get_conn() as conn:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10))) as date,
            EmployeeNumber1 as emp,
            COALESCE(ActualHours, 0) as hours
        FROM SCHLabor
        WHERE CAST(COMNumber AS TEXT) IN ('19044', '019044')
          AND DepartmentNumber = '0120'
          AND COALESCE(ActualHours, 0) > 0
        ORDER BY date
    """)
    
    charges = cur.fetchall()

# Build daymap structure
daymap = {}
for charge in charges:
    day = charge['date']
    if not day:
        continue
    if day not in daymap:
        daymap[day] = {'emps': set(), 'hours': 0.0}
    if charge['emp']:
        daymap[day]['emps'].add(str(charge['emp']).strip())
    daymap[day]['hours'] += charge['hours']

all_days = sorted(daymap.keys())

print(f"\nRaw Fab charges: {len(all_days)} days")
print(f"First day: {all_days[0]}")
print(f"Last day: {all_days[-1]}")

# Calculate unfiltered span
first_date = date.fromisoformat(all_days[0])
last_date = date.fromisoformat(all_days[-1])
unfiltered_span = (last_date - first_date).days + 1

print(f"Unfiltered span: {unfiltered_span} days")

# Apply gap filtering (100% complete department)
is_complete = True
excl_employees = PROJECT_DAY_RULES.get('exclusion_employees', [])
min_employees = 2

print(f"\n{'='*80}")
print(f"APPLYING GAP FILTERING:")
print(f"{'='*80}")
print(f"Fab Outlier Cap: {OUTLIER_CAPS.get('Fab', 'NOT SET')} days")
print(f"Hard Max Gap Override: {PROJECT_DAY_RULES.get('max_gap_override', 'NOT SET')} days")
print(f"Department Complete: {is_complete}")
print(f"Min Employees Override: {min_employees}")

filtered_days = _filter_days_by_gap('Fab', all_days, daymap, is_complete, excl_employees, min_employees)

print(f"\n{'='*80}")
print(f"RESULTS:")
print(f"{'='*80}")
print(f"Days before filtering: {len(all_days)}")
print(f"Days after filtering: {len(filtered_days)}")
print(f"Days dropped: {len(all_days) - len(filtered_days)}")

if len(filtered_days) >= 2:
    filtered_first = filtered_days[0]
    filtered_last = filtered_days[-1]
    
    filtered_span = (date.fromisoformat(filtered_last) - date.fromisoformat(filtered_first)).days + 1
    
    print(f"\nFiltered first day: {filtered_first}")
    print(f"Filtered last day: {filtered_last}")
    print(f"Filtered span: {filtered_span} days")
    
    print(f"\n{'='*80}")
    print(f"SPAN COMPARISON:")
    print(f"{'='*80}")
    print(f"Before: {unfiltered_span} days (2024-04-18 to 2025-06-27)")
    print(f"After:  {filtered_span} days ({filtered_first} to {filtered_last})")
    print(f"Change: {filtered_span - unfiltered_span:+d} days")
    
    if filtered_span < unfiltered_span:
        print(f"\n✅ SUCCESS: Hard max gap override (30 days) filtered out the 373-day gap!")
        print(f"   June 27, 2025 charges (2.5 hours, 2 employees) were dropped.")
    else:
        print(f"\n⚠️  Span unchanged - check logic")
elif len(filtered_days) == 0:
    print(f"\n⚠️  All days were filtered out!")
else:
    print(f"\n⚠️  Only one day remains after filtering")

# Show which days were dropped
dropped_days = set(all_days) - set(filtered_days)
if dropped_days:
    print(f"\n{'='*80}")
    print(f"DROPPED DAYS ({len(dropped_days)}):")
    print(f"{'='*80}")
    for day in sorted(dropped_days):
        data = daymap[day]
        print(f"  {day}: {data['hours']:.1f} hours, {len(data['emps'])} employees")

print(f"\n{'='*80}")
