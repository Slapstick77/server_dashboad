import sys
import sqlite3
sys.path.insert(0, r'c:\Project p\SQRS\webapp')

from blueprints.utils import _resolve_department_days
from collections import defaultdict
from datetime import datetime

db = sqlite3.connect(r'c:\Project p\SQRS\SCHLabor.db')
cursor = db.cursor()

# Get all charges for 18084
cursor.execute("""
    SELECT LoggedDate, DepartmentNumber, EmployeeNumber1, EmployeeName, ActualHours
    FROM SCHLabor
    WHERE COMNumber = 18084
    ORDER BY LoggedDate
""")

charges = cursor.fetchall()

print("=" * 100)
print(f"COM 18084 - ALL CHARGES ({len(charges)} total)")
print("=" * 100)
print(f"{'Date':<12} {'Dept':<10} {'Emp#':<8} {'Employee':<20} {'Hours':>8}")
print("-" * 100)

for date, dept, emp_num, emp_name, hours in charges:
    print(f"{date:<12} {dept:<10} {emp_num:<8} {emp_name:<20} {hours:>8.1f}")

# Group by department to see the filtering
print("\n" + "=" * 100)
print("ANALYZING BY DEPARTMENT")
print("=" * 100)

day_emp = defaultdict(lambda: defaultdict(lambda: {'emps': set(), 'hours': 0.0}))
for date, dept, emp, emp_name, hours in charges:
    day_emp[(18084, dept)][date]['emps'].add(emp)
    day_emp[(18084, dept)][date]['hours'] += hours

for (com, dept), daymap in day_emp.items():
    print(f"\n{dept} Department:")
    print(f"  Raw days: {len(daymap)}")
    
    # Show the actual days
    sorted_days = sorted(daymap.keys())
    print(f"  First day: {sorted_days[0]}")
    print(f"  Last day: {sorted_days[-1]}")
    
    # Calculate gaps between consecutive days
    print(f"\n  Checking gaps:")
    for i in range(len(sorted_days)):
        day = sorted_days[i]
        emps = daymap[day]['emps']
        hours = daymap[day]['hours']
        
        if i > 0:
            prev_day = sorted_days[i-1]
            try:
                curr_dt = datetime.strptime(day, '%Y-%m-%d')
                prev_dt = datetime.strptime(prev_day, '%Y-%m-%d')
            except:
                curr_dt = datetime.strptime(day, '%m/%d/%Y')
                prev_dt = datetime.strptime(prev_day, '%m/%d/%Y')
            
            gap = (curr_dt - prev_dt).days
            
            if gap > 7:  # Show large gaps
                print(f"    ⚠️ GAP {gap} days: {prev_day} → {day} ({len(emps)} emp, {hours:.1f}h)")
        else:
            print(f"    FIRST: {day} ({len(emps)} emp, {hours:.1f}h)")
    
    # Now run through the actual filter
    filtered_days, meta = _resolve_department_days(dept, daymap, is_complete=True)
    print(f"\n  After filtering: {len(filtered_days)} days")
    
    if len(filtered_days) != len(daymap):
        print(f"  ❌ DROPPED {len(daymap) - len(filtered_days)} days!")
        dropped = set(daymap.keys()) - set(filtered_days)
        print(f"  Dropped days: {sorted(dropped)}")

db.close()
