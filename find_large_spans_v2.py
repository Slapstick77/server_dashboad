import sqlite3
import sys
sys.path.insert(0, r'c:\Project p\SQRS')

from webapp.blueprints.utils import _resolve_department_days, normalize_com
from datetime import datetime

db_path = r'c:\Project p\SQRS\SCHLabor.db'

# Get all complete units
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute('SELECT com_number FROM UnitCompletion ORDER BY com_number')
complete_coms = [row[0] for row in cursor.fetchall()]

print(f"Analyzing {len(complete_coms)} complete units for large filtered spans...\n")

# Department mapping
raw_code_to_label = {
    '0120':'Fab','0140':'Welding','0180':'BaseFormPaint','0200':'FanAssyTest','0220':'InsulWallFab',
    '0230':'Pipe','0260':'Assembly','0270':'DoorFab','0280':'Assembly','0300':'Electrical','0320':'Pipe',
    '0340':'Paint','0360':'Test','0380':'Crating',
}

large_spans = []

for com in complete_coms:
    # Get labor data for this unit
    query = """
    SELECT DepartmentNumber, iso_logged_date, EmployeeNumber1, ActualHours
    FROM SCHLabor
    WHERE COMNumber = ?
    ORDER BY iso_logged_date
    """
    cursor.execute(query, (int(com.replace('COM', '')),))
    rows = cursor.fetchall()
    
    if not rows:
        continue
    
    # Build department daymaps
    day_emp = {}
    for dept, day, emp, hrs in rows:
        label = raw_code_to_label.get(dept)
        if not label or day is None:
            continue
        dm = day_emp.setdefault(label, {})
        rec = dm.setdefault(day, {'emps': set(), 'hours': 0.0})
        if emp:
            rec['emps'].add(str(emp).strip())
        try:
            rec['hours'] += float(hrs) if hrs is not None else 0.0
        except Exception:
            pass
    
    # Apply filtering for each department
    unit_first = None
    unit_last = None
    total_filtered_days = 0
    
    for label, daymap in day_emp.items():
        use_days, meta = _resolve_department_days(label, daymap, is_complete=True)
        if not use_days:
            continue
        
        total_filtered_days += len(use_days)
        first_day = min(use_days)
        last_day = max(use_days)
        
        if unit_first is None or first_day < unit_first:
            unit_first = first_day
        if unit_last is None or last_day > unit_last:
            unit_last = last_day
    
    if unit_first and unit_last:
        span_days = (datetime.fromisoformat(unit_last) - datetime.fromisoformat(unit_first)).days + 1
        if span_days > 100:  # Only show units with >100 day span
            large_spans.append({
                'com': com,
                'span': span_days,
                'first': unit_first,
                'last': unit_last,
                'days': total_filtered_days
            })

conn.close()

# Sort by span descending
large_spans.sort(key=lambda x: x['span'], reverse=True)

print("=" * 80)
print(f"Units with filtered span > 100 days: {len(large_spans)}")
print("=" * 80)
print(f"{'COM':<10} {'Span':<8} {'Days':<6} {'First Date':<12} {'Last Date':<12}")
print("-" * 80)

for unit in large_spans[:20]:  # Show top 20
    print(f"{unit['com']:<10} {unit['span']:>6}d  {unit['days']:>4}   {unit['first']:<12} {unit['last']:<12}")

if len(large_spans) > 20:
    print(f"\n... and {len(large_spans) - 20} more units")
