import sqlite3
import sys
sys.path.insert(0, r'c:\Project p\SQRS')

from webapp.blueprints.utils import _resolve_department_days

db_path = r'c:\Project p\SQRS\SCHLabor.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Check the 2023-08-25 charges
query = """
SELECT iso_logged_date, EmployeeName, EmployeeNumber1, DepartmentNumber, ActualHours
FROM SCHLabor
WHERE COMNumber = 18084
  AND iso_logged_date = '2023-08-25'
ORDER BY DepartmentNumber, EmployeeName
"""

cursor.execute(query)
charges = cursor.fetchall()

print("COM 18084 - Charges on 2023-08-25:")
print("=" * 80)
for date, emp_name, emp_num, dept, hrs in charges:
    print(f"{date} - {emp_name} - Dept {dept} - {hrs} hrs - Emp #{emp_num}")

# Check each department separately
raw_code_to_label = {
    '0120':'Fab','0140':'Welding','0180':'BaseFormPaint','0200':'FanAssyTest','0220':'InsulWallFab',
    '0230':'Pipe','0260':'Assembly','0270':'DoorFab','0280':'Assembly','0300':'Electrical','0320':'Pipe',
    '0340':'Paint','0360':'Test','0380':'Crating',
}

for dept_code in ['0120', '0220', '0380']:
    label = raw_code_to_label.get(dept_code)
    if not label:
        continue
    
    print(f"\n{'=' * 80}")
    print(f"Department {dept_code} ({label}):")
    print("=" * 80)
    
    # Get all charges for this department
    query2 = """
    SELECT iso_logged_date, EmployeeNumber1, ActualHours
    FROM SCHLabor
    WHERE COMNumber = 18084
      AND DepartmentNumber = ?
    ORDER BY iso_logged_date
    """
    
    cursor.execute(query2, (dept_code,))
    dept_charges = cursor.fetchall()
    
    # Build daymap
    daymap = {}
    for date_str, emp_num, hrs in dept_charges:
        if date_str not in daymap:
            daymap[date_str] = {'emps': set(), 'hours': 0.0}
        if emp_num:
            daymap[date_str]['emps'].add(str(emp_num).strip())
        daymap[date_str]['hours'] += float(hrs) if hrs else 0.0
    
    print(f"Raw days: {len(daymap)}")
    for day in sorted(daymap.keys()):
        print(f"  {day}: {len(daymap[day]['emps'])} emp, {daymap[day]['hours']:.2f} hrs")
    
    # Apply filtering
    use_days, meta = _resolve_department_days(label, daymap, is_complete=True)
    
    print(f"\nFiltered days: {len(use_days)}")
    for day in sorted(use_days):
        if day == '2023-08-25':
            print(f"  {day}: KEPT ✓")
        else:
            print(f"  {day}: kept")
    
    if meta.get('filter_reasons'):
        print("\nDropped days:")
        for day, reason in sorted(meta['filter_reasons'].items()):
            if day == '2023-08-25':
                print(f"  {day}: {reason} ✗")
            else:
                print(f"  {day}: {reason}")

conn.close()
