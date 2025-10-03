import sqlite3
from datetime import datetime

db_path = r'c:\Project p\SQRS\SCHLabor.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get all charges for COM 18084 in dept 0200 around May 15, 2024
query = """
SELECT iso_logged_date, EmployeeName, DepartmentNumber, ActualHours, COMNumber
FROM SCHLabor
WHERE COMNumber = 18084
  AND DepartmentNumber = '0200'
ORDER BY iso_logged_date
"""

cursor.execute(query)
charges = cursor.fetchall()

print(f"COM 18084 - Department 0200 charges:")
print(f"{'Date':<12} {'Employee':<20} {'Dept':<6} {'Hours':<6}")
print("=" * 60)

dates_list = []
for row in charges:
    date_str, emp, dept, hrs, com = row
    print(f"{date_str:<12} {emp:<20} {dept:<6} {hrs:<6}")
    dates_list.append(date_str)

print("\n" + "=" * 60)
print("\nDate gaps in 0200 department:")
print("=" * 60)

# Convert dates and calculate gaps
from datetime import datetime
date_objects = []
for d in dates_list:
    try:
        # Try both formats
        try:
            dt = datetime.strptime(d, '%Y-%m-%d')
        except:
            dt = datetime.strptime(d, '%m/%d/%Y')
        date_objects.append((d, dt))
    except Exception as e:
        print(f"Error parsing date {d}: {e}")

date_objects.sort(key=lambda x: x[1])

for i in range(len(date_objects) - 1):
    current_date_str, current_date = date_objects[i]
    next_date_str, next_date = date_objects[i + 1]
    gap = (next_date - current_date).days
    print(f"{current_date_str} → {next_date_str}: {gap} days")

print("\n" + "=" * 60)
print("\nChecking employee count and hours for 2024-05-15:")
print("=" * 60)

# Check the specific charge on 2024-05-15
query2 = """
SELECT iso_logged_date, EmployeeName, DepartmentNumber, ActualHours
FROM SCHLabor
WHERE COMNumber = 18084
  AND DepartmentNumber = '0200'
  AND (iso_logged_date = '2024-05-15' OR LoggedDate = '05/15/2024')
"""

cursor.execute(query2)
may15_charges = cursor.fetchall()

total_hours = 0
total_employees = set()
for row in may15_charges:
    date_str, emp, dept, hrs = row
    print(f"{date_str}: {emp} - {hrs} hours")
    total_hours += hrs
    total_employees.add(emp)

print(f"\nTotal on 2024-05-15: {len(total_employees)} employee(s), {total_hours} hours")

# Check the previous charge
print("\n" + "=" * 60)
print("\nPrevious charge before 2024-05-15:")
print("=" * 60)

query3 = """
SELECT iso_logged_date, EmployeeName, DepartmentNumber, ActualHours
FROM SCHLabor
WHERE COMNumber = 18084
  AND DepartmentNumber = '0200'
ORDER BY iso_logged_date DESC
"""

cursor.execute(query3)
all_charges = cursor.fetchall()

# Find the charge before 2024-05-15
from datetime import datetime
target = datetime(2024, 5, 15)
prev_charge = None

for row in all_charges:
    date_str = row[0]
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
    except:
        dt = datetime.strptime(date_str, '%m/%d/%Y')
    
    if dt < target:
        prev_charge = (date_str, dt, row[1], row[3])
        break

if prev_charge:
    date_str, dt, emp, hrs = prev_charge
    gap = (target - dt).days
    print(f"Previous: {date_str} ({emp}, {hrs} hrs)")
    print(f"Gap to 2024-05-15: {gap} days")
    print(f"\nWhy it was VALID:")
    if gap <= 30:
        print(f"  ✓ Gap of {gap} days is within max_gap_override of 30 days")
    else:
        print(f"  ✗ Gap of {gap} days exceeds max_gap_override of 30 days")
        if len(total_employees) >= 2:
            print(f"  ✓ BUT: {len(total_employees)} employees on 2024-05-15 (≥2 employee override)")
        else:
            print(f"  ✗ Only {len(total_employees)} employee on 2024-05-15")

conn.close()
