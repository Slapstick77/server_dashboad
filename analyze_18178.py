import sqlite3
import sys
sys.path.insert(0, r'c:\Project p\SQRS')

from webapp.blueprints.utils import _resolve_department_days
from datetime import datetime

db_path = r'c:\Project p\SQRS\SCHLabor.db'

raw_code_to_label = {
    '0120':'Fab','0140':'Welding','0180':'BaseFormPaint','0200':'FanAssyTest','0220':'InsulWallFab',
    '0230':'Pipe','0260':'Assembly','0270':'DoorFab','0280':'Assembly','0300':'Electrical','0320':'Pipe',
    '0340':'Paint','0360':'Test','0380':'Crating',
}

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

com_num = 18178

query = """
SELECT DepartmentNumber, iso_logged_date, EmployeeNumber1, EmployeeName, ActualHours
FROM SCHLabor
WHERE COMNumber = ?
ORDER BY iso_logged_date, DepartmentNumber
"""

cursor.execute(query, (com_num,))
rows = cursor.fetchall()

print(f"COM {com_num} - Analyzing filtered spans by department\n")
print("=" * 100)

# Build department daymaps
day_emp = {}
for dept, day, emp_num, emp_name, hrs in rows:
    label = raw_code_to_label.get(dept)
    if not label or day is None:
        continue
    dm = day_emp.setdefault(label, {})
    rec = dm.setdefault(day, {'emps': set(), 'hours': 0.0})
    if emp_num:
        rec['emps'].add(str(emp_num).strip())
    rec['hours'] += float(hrs) if hrs is not None else 0.0

# Process each department
dept_results = []
for label in sorted(day_emp.keys()):
    daymap = day_emp[label]
    raw_days = sorted(daymap.keys())
    use_days, meta = _resolve_department_days(label, daymap, is_complete=True)
    
    if use_days:
        first = min(use_days)
        last = max(use_days)
        span = (datetime.fromisoformat(last) - datetime.fromisoformat(first)).days + 1
        dept_results.append({
            'label': label,
            'raw': len(raw_days),
            'filtered': len(use_days),
            'first': first,
            'last': last,
            'span': span,
            'dropped': meta.get('dropped_for_gap', [])
        })

# Sort by span
dept_results.sort(key=lambda x: x['span'], reverse=True)

for dept in dept_results:
    print(f"\n{dept['label']}:")
    print(f"  Raw days: {dept['raw']}, Filtered days: {dept['filtered']}")
    print(f"  Date range: {dept['first']} → {dept['last']}")
    print(f"  Span: {dept['span']} days")
    if dept['dropped']:
        print(f"  Dropped for gap: {dept['dropped']}")

# Overall unit span
if dept_results:
    unit_first = min(d['first'] for d in dept_results)
    unit_last = max(d['last'] for d in dept_results)
    unit_span = (datetime.fromisoformat(unit_last) - datetime.fromisoformat(unit_first)).days + 1
    print("\n" + "=" * 100)
    print(f"UNIT OVERALL SPAN: {unit_first} → {unit_last} = {unit_span} days")
    print(f"This is from aggregating {len(dept_results)} departments")

conn.close()
