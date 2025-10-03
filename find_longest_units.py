import sys
import sqlite3
sys.path.insert(0, r'c:\Project p\SQRS\webapp')

from blueprints.utils import _resolve_department_days
from collections import defaultdict
from datetime import datetime

db = sqlite3.connect(r'c:\Project p\SQRS\SCHLabor.db')
cursor = db.cursor()

# Get all COM numbers
cursor.execute("SELECT DISTINCT COMNumber FROM SCHLabor ORDER BY COMNumber DESC LIMIT 200")
com_numbers = [row[0] for row in cursor.fetchall()]

units_with_spans = []

for com in com_numbers:
    cursor.execute("""
        SELECT DepartmentNumber, LoggedDate, EmployeeNumber1, ActualHours
        FROM SCHLabor
        WHERE COMNumber = ?
        ORDER BY LoggedDate
    """, (com,))
    
    charges = cursor.fetchall()
    if not charges or len(charges) < 5:  # Skip tiny units
        continue
    
    # Calculate raw span
    try:
        first_date = datetime.strptime(charges[0][1], '%Y-%m-%d')
        last_date = datetime.strptime(charges[-1][1], '%Y-%m-%d')
    except:
        try:
            first_date = datetime.strptime(charges[0][1], '%m/%d/%Y')
            last_date = datetime.strptime(charges[-1][1], '%m/%d/%Y')
        except:
            continue
    
    raw_span = (last_date - first_date).days
    
    if raw_span > 25:  # Only look at units with >25 day raw span
        units_with_spans.append((f"COM {com}", first_date, last_date, raw_span, len(charges)))

# Sort by span
units_with_spans.sort(key=lambda x: x[3], reverse=True)

print("=" * 100)
print("TOP UNITS WITH LONGEST RAW SPAN (>25 days)")
print("=" * 100)
print(f"{'COM':<12} {'First Date':<12} {'Last Date':<12} {'Span':>6} {'Charges':>8}")
print("-" * 100)

for com, first, last, span, charges in units_with_spans[:30]:
    print(f"{com:<12} {first.strftime('%Y-%m-%d'):<12} {last.strftime('%Y-%m-%d'):<12} {span:>6} {charges:>8}")

db.close()
