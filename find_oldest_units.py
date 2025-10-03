import sqlite3
from datetime import datetime

db_path = r'c:\Project p\SQRS\SCHLabor.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get units with completion dates, calculate their spans
cursor.execute("""
    SELECT 
        uc.com_number,
        MIN(sl.LoggedDate) as first_date,
        MAX(sl.LoggedDate) as last_date,
        uc.last_day_unfiltered,
        COUNT(DISTINCT sl.LoggedDate) as num_days,
        COUNT(*) as num_charges
    FROM UnitCompletion uc
    JOIN SCHLabor sl ON uc.com_number = CAST(sl.COMNumber AS TEXT)
    WHERE uc.last_day_unfiltered IS NOT NULL
    GROUP BY uc.com_number
    ORDER BY first_date ASC
    LIMIT 50
""")

units = cursor.fetchall()

print("=" * 100)
print("UNITS WITH EARLIEST START DATES (likely longest spans)")
print("=" * 100)
print(f"{'COM':<10} {'First Date':<12} {'Last Date':<12} {'Span':>6} {'Days':>5} {'Charges':>8}")
print("-" * 100)

for com, first, last, unfiltered, days, charges in units:
    try:
        first_dt = datetime.strptime(first, '%Y-%m-%d')
        last_dt = datetime.strptime(last, '%Y-%m-%d')
        span = (last_dt - first_dt).days
        
        highlight = " ⚠️ LONG!" if span > 100 else ""
        print(f"{com:<10} {first:<12} {last:<12} {span:>6} {days:>5} {charges:>8}{highlight}")
    except:
        print(f"{com:<10} {first:<12} {last:<12} {'???':>6} {days:>5} {charges:>8}")

conn.close()
