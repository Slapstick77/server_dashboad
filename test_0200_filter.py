import sqlite3
import sys
sys.path.insert(0, r'c:\Project p\SQRS')

from webapp.blueprints.utils import _resolve_department_days

db_path = r'c:\Project p\SQRS\SCHLabor.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get all charges for COM 18084 dept 0200
query = """
SELECT iso_logged_date, EmployeeName, DepartmentNumber, ActualHours
FROM SCHLabor
WHERE COMNumber = 18084
  AND DepartmentNumber = '0200'
ORDER BY iso_logged_date
"""

cursor.execute(query)
charges = cursor.fetchall()

print("=" * 80)
print("RAW CHARGES for COM 18084 - Department 0200:")
print("=" * 80)
for date_str, emp, dept, hrs in charges:
    print(f"{date_str}: {emp} - {hrs} hours - Dept {dept}")

# Build daymap for filtering
daymap = {}
for date_str, emp, dept, hrs in charges:
    if date_str not in daymap:
        daymap[date_str] = {'emps': set(), 'hours': 0}
    daymap[date_str]['emps'].add(emp)
    daymap[date_str]['hours'] += hrs

print("\n" + "=" * 80)
print("DAYMAP before filtering:")
print("=" * 80)
for date in sorted(daymap.keys()):
    print(f"{date}: {len(daymap[date]['emps'])} emp, {daymap[date]['hours']} hrs")

# Apply filtering (is_complete=True since COM 18084 is completed)
filtered_days, meta = _resolve_department_days('0200', daymap, is_complete=True)

print("\n" + "=" * 80)
print("FILTERED DAYS after _resolve_department_days:")
print("=" * 80)
if filtered_days:
    for date in sorted(filtered_days):
        print(f"{date}: KEPT")
else:
    print("NO DAYS KEPT")

print("\n" + "=" * 80)
print("FILTER METADATA:")
print("=" * 80)
print(f"Total days seen: {meta['total_days_seen']}")
print(f"Dropped for hours: {meta.get('dropped_for_hours', [])}")
print(f"Dropped for exclusion: {meta.get('dropped_for_exclusion', [])}")
print(f"Dropped for gap: {meta.get('dropped_for_gap', [])}")
if meta.get('filter_reasons'):
    print("\nFilter reasons:")
    for day, reason in sorted(meta['filter_reasons'].items()):
        print(f"  {day}: {reason}")

print("\n" + "=" * 80)
print("ANALYSIS:")
print("=" * 80)
all_dates = sorted(daymap.keys())
print(f"Raw dates: {all_dates}")
print(f"Filtered dates: {sorted(filtered_days) if filtered_days else 'None'}")

if '2024-05-15' in filtered_days:
    print("\n⚠️  2024-05-15 was KEPT (but should be filtered)")
    print("   Gap: 482 days, 1 employee, 1.0 hours")
    print("   This exceeds max_gap_override of 30 days")
else:
    print("\n✓  2024-05-15 was FILTERED OUT (correct)")
    print("   Gap: 482 days exceeds max_gap_override of 30 days")

conn.close()
