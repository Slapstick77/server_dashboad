import sys
import sqlite3
sys.path.insert(0, r'c:\Project p\SQRS\webapp')

from blueprints.utils import _resolve_department_days
from collections import defaultdict
from datetime import datetime

db = sqlite3.connect(r'c:\Project p\SQRS\SCHLabor.db')
cursor = db.cursor()

# Get all completed units
cursor.execute("SELECT com_number FROM UnitCompletion WHERE last_day_unfiltered IS NOT NULL")
com_numbers = [row[0] for row in cursor.fetchall()]

print(f"Analyzing {len(com_numbers)} completed units...\n")

units_with_filtered_spans = []

for com in com_numbers:
    # Get all charges for this unit
    cursor.execute("""
        SELECT DepartmentNumber, LoggedDate, EmployeeNumber1, ActualHours
        FROM SCHLabor
        WHERE COMNumber = ?
        ORDER BY LoggedDate
    """, (int(com),))
    
    charges = cursor.fetchall()
    if not charges:
        continue
    
    # Group by department like the real code does
    day_emp = defaultdict(lambda: defaultdict(lambda: {'emps': set(), 'hours': 0.0}))
    for dept, date, emp, hours in charges:
        day_emp[(com, dept)][date]['emps'].add(emp)
        day_emp[(com, dept)][date]['hours'] += hours
    
    # Calculate filtered span for each department
    max_filtered_span = 0
    for (unit_com, dept), daymap in day_emp.items():
        # Use the ACTUAL filtering function with is_complete=True
        filtered_days, meta = _resolve_department_days(dept, daymap, is_complete=True)
        
        if filtered_days and len(filtered_days) > 0:
            # filtered_days is a list of date strings
            sorted_dates = sorted(filtered_days)
            
            # Try both date formats
            try:
                first = datetime.strptime(sorted_dates[0], '%Y-%m-%d')
                last = datetime.strptime(sorted_dates[-1], '%Y-%m-%d')
            except:
                try:
                    first = datetime.strptime(sorted_dates[0], '%m/%d/%Y')
                    last = datetime.strptime(sorted_dates[-1], '%m/%d/%Y')
                except:
                    continue
            
            filtered_span = (last - first).days
            
            if filtered_span > max_filtered_span:
                max_filtered_span = filtered_span
    
    if max_filtered_span > 20:  # Only units with >20 day filtered span
        units_with_filtered_spans.append((com, max_filtered_span))

# Sort by filtered span
units_with_filtered_spans.sort(key=lambda x: x[1], reverse=True)

print("=" * 80)
print("TOP UNITS BY FILTERED SPAN (using your actual gap filtering logic)")
print("=" * 80)
print(f"{'COM':<12} {'Filtered Span':>15}")
print("-" * 80)

for com, span in units_with_filtered_spans[:30]:
    highlight = " ⚠️ HUGE!" if span > 100 else (" 🔥 HIGH!" if span > 50 else "")
    print(f"{com:<12} {span:>15} days{highlight}")

print("\n" + "=" * 80)
print(f"Total units with filtered span > 20 days: {len(units_with_filtered_spans)}")

db.close()
