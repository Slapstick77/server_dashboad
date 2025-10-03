import sys
import sqlite3
sys.path.insert(0, r'c:\Project p\SQRS\webapp')

from blueprints.utils import _resolve_department_days
from collections import defaultdict

# Get COM 19044 data using the ACTUAL code
db = sqlite3.connect(r'c:\Project p\SQRS\SCHLabor.db')
cursor = db.cursor()

# Get all charges for COM 19044
cursor.execute("""
    SELECT DepartmentNumber, LoggedDate, EmployeeNumber1, ActualHours
    FROM SCHLabor
    WHERE COMNumber = 19044
    ORDER BY LoggedDate
""")

charges = cursor.fetchall()
print(f"Found {len(charges)} charges for COM 19044\n")

if charges:
    print(f"Date range: {charges[0][1]} to {charges[-1][1]}")
    
    # Group by department like the real code does
    day_emp = defaultdict(lambda: defaultdict(set))
    
    for dept, date, emp, hours in charges:
        day_emp[(19044, dept)][date].add(emp)
    
    # Now check the Fab department specifically
    fab_key = None
    for key in day_emp.keys():
        if 'Fab' in str(key[1]):
            fab_key = key
            break
    
    if fab_key:
        print(f"\nFab department key: {fab_key}")
        print(f"Fab dates: {sorted(day_emp[fab_key].keys())}")
        
        # Use the ACTUAL filtering function
        filtered_days, meta = _resolve_department_days('Fab', day_emp[fab_key], is_complete=True)
        
        print(f"\nFab filtering results:")
        print(f"  Raw days: {len(day_emp[fab_key])}")
        print(f"  Filtered days: {len(filtered_days)}")
        print(f"  Metadata: {meta}")
        
        if filtered_days:
            sorted_filtered = sorted(filtered_days.keys())
            print(f"  First filtered date: {sorted_filtered[0]}")
            print(f"  Last filtered date: {sorted_filtered[-1]}")

db.close()
