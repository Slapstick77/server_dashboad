import sqlite3
import sys
sys.path.insert(0, r'c:\Project p\SQRS')

from webapp.blueprints.utils import _resolve_department_days

db_path = r'c:\Project p\SQRS\SCHLabor.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

query = """
SELECT iso_logged_date, EmployeeNumber1, ActualHours
FROM SCHLabor
WHERE COMNumber = 18178
  AND DepartmentNumber IN ('0260', '0280')
ORDER BY iso_logged_date
"""

cursor.execute(query)
charges = cursor.fetchall()

daymap = {}
for date_str, emp_num, hrs in charges:
    if date_str not in daymap:
        daymap[date_str] = {'emps': set(), 'hours': 0.0}
    if emp_num:
        daymap[date_str]['emps'].add(str(emp_num).strip())
    daymap[date_str]['hours'] += float(hrs) if hrs else 0.0

print("COM 18178 - Assembly Department")
print("=" * 60)
print("RAW DAYS:")
for day in sorted(daymap.keys()):
    print(f"  {day}: {len(daymap[day]['emps'])} emp, {daymap[day]['hours']:.2f} hrs")

use_days, meta = _resolve_department_days('Assembly', daymap, is_complete=True)

print("\nFILTERED DAYS:")
for day in sorted(use_days):
    print(f"  {day}: KEPT")

print("\nDROPPED DAYS:")
for day, reason in sorted(meta.get('filter_reasons', {}).items()):
    print(f"  {day}: {reason}")

print("\nGap check:")
days_list = sorted(daymap.keys())
from datetime import datetime
for i in range(len(days_list) - 1):
    d1 = days_list[i]
    d2 = days_list[i+1]
    gap = (datetime.fromisoformat(d2) - datetime.fromisoformat(d1)).days
    print(f"  {d1} → {d2}: {gap} days")

conn.close()
