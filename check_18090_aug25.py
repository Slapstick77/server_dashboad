import sqlite3
import sys
sys.path.insert(0, r'c:\Project p\SQRS')

from webapp.blueprints.utils import _resolve_department_days

db_path = r'c:\Project p\SQRS\SCHLabor.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Check the 2023-08-25 charges for COM 18090
query = """
SELECT iso_logged_date, EmployeeName, EmployeeNumber1, DepartmentNumber, ActualHours
FROM SCHLabor
WHERE COMNumber = 18090
  AND iso_logged_date IN ('2023-08-24', '2023-08-25')
ORDER BY iso_logged_date, DepartmentNumber
"""

cursor.execute(query)
charges = cursor.fetchall()

print("COM 18090 - Charges on 2023-08-24 and 2023-08-25:")
print("=" * 80)
for date, emp_name, emp_num, dept, hrs in charges:
    print(f"{date} - {emp_name} - Dept {dept} - {hrs} hrs")

# Now check Crating department (0380) specifically
print("\n" + "=" * 80)
print("Department 0380 (Crating) - Full analysis:")
print("=" * 80)

raw_code_to_label = {
    '0120':'Fab','0140':'Welding','0180':'BaseFormPaint','0200':'FanAssyTest','0220':'InsulWallFab',
    '0230':'Pipe','0260':'Assembly','0270':'DoorFab','0280':'Assembly','0300':'Electrical','0320':'Pipe',
    '0340':'Paint','0360':'Test','0380':'Crating',
}

query2 = """
SELECT iso_logged_date, EmployeeNumber1, ActualHours
FROM SCHLabor
WHERE COMNumber = 18090
  AND DepartmentNumber = '0380'
ORDER BY iso_logged_date
"""

cursor.execute(query2)
dept_charges = cursor.fetchall()

# Build daymap
daymap = {}
for date_str, emp_num, hrs in dept_charges:
    if date_str not in daymap:
        daymap[date_str] = {'emps': set(), 'hours': 0.0}
    if emp_num:
        daymap[date_str]['emps'].add(str(emp_num).strip())
    daymap[date_str]['hours'] += float(hrs) if hrs else 0.0

print(f"\nRaw days: {len(daymap)}")
for day in sorted(daymap.keys()):
    marker = " ← 2023-08-25" if day == '2023-08-25' else ""
    print(f"  {day}: {len(daymap[day]['emps'])} emp, {daymap[day]['hours']:.2f} hrs{marker}")

# Check gaps
from datetime import datetime
days_list = sorted(daymap.keys())
print("\nGaps:")
for i in range(len(days_list) - 1):
    d1 = days_list[i]
    d2 = days_list[i+1]
    gap = (datetime.fromisoformat(d2) - datetime.fromisoformat(d1)).days
    marker = " ← includes 2023-08-25" if d2 == '2023-08-25' or d1 == '2023-08-25' else ""
    print(f"  {d1} → {d2}: {gap} days{marker}")

# Apply filtering
use_days, meta = _resolve_department_days('Crating', daymap, is_complete=True)

print(f"\nFiltered days: {len(use_days)}")
for day in sorted(use_days):
    marker = " ✓ VALID" if day == '2023-08-25' else ""
    print(f"  {day}{marker}")

if meta.get('filter_reasons'):
    print("\nDropped days:")
    for day, reason in sorted(meta['filter_reasons'].items()):
        marker = " ✗ FILTERED" if day == '2023-08-25' else ""
        print(f"  {day}: {reason}{marker}")

if '2023-08-25' in use_days:
    print("\n⚠️  2023-08-25 was KEPT as VALID")
    print("    3.00 hours, ? employees, Walley, Felix L")
else:
    print("\n✓  2023-08-25 was FILTERED OUT")

conn.close()
