import sqlite3
import sys
sys.path.insert(0, r'c:\Project p\SQRS')

from webapp.blueprints.utils import _resolve_department_days

db_path = r'c:\Project p\SQRS\SCHLabor.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get charges for COM 18084 dept 0200
query = """
SELECT iso_logged_date, EmployeeName, EmployeeNumber1, DepartmentNumber, ActualHours
FROM SCHLabor
WHERE COMNumber = 18084
  AND DepartmentNumber = '0200'
ORDER BY iso_logged_date
"""

cursor.execute(query)
charges = cursor.fetchall()

print("Building daymap for FanAssyTest...")
daymap = {}
for date_str, emp_name, emp_num, dept, hrs in charges:
    if date_str not in daymap:
        daymap[date_str] = {'emps': set(), 'hours': 0.0}
    if emp_num:
        daymap[date_str]['emps'].add(str(emp_num).strip())
    daymap[date_str]['hours'] += float(hrs) if hrs else 0.0

print(f"\nRaw daymap: {len(daymap)} days")
for day in sorted(daymap.keys()):
    print(f"  {day}: {len(daymap[day]['emps'])} emp, {daymap[day]['hours']} hrs")

# Call the filter with is_complete=True (assume complete)
print("\nCalling _resolve_department_days with is_complete=True...")
use_days, meta = _resolve_department_days('FanAssyTest', daymap, is_complete=True)

print(f"\nFiltered days: {len(use_days)}")
for day in sorted(use_days):
    print(f"  {day}: KEPT")

print(f"\nDropped days:")
if meta.get('filter_reasons'):
    for day, reason in sorted(meta['filter_reasons'].items()):
        print(f"  {day}: {reason}")

if '2024-05-15' in use_days:
    print("\n❌ 2024-05-15 was KEPT (WRONG)")
else:
    print("\n✅ 2024-05-15 was FILTERED (CORRECT)")

conn.close()
