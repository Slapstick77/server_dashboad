import sqlite3

db_path = r'c:\Project p\SQRS\SCHLabor.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Calculate span directly in SQL
cursor.execute("""
    SELECT 
        uc.com_number,
        MIN(sl.LoggedDate) as first_date,
        MAX(sl.LoggedDate) as last_date,
        JULIANDAY(MAX(sl.LoggedDate)) - JULIANDAY(MIN(sl.LoggedDate)) as span_days,
        COUNT(DISTINCT sl.LoggedDate) as num_days,
        COUNT(*) as num_charges
    FROM UnitCompletion uc
    JOIN SCHLabor sl ON uc.com_number = CAST(sl.COMNumber AS TEXT)
    WHERE uc.last_day_unfiltered IS NOT NULL
    GROUP BY uc.com_number
    HAVING span_days > 0
    ORDER BY span_days DESC
    LIMIT 50
""")

units = cursor.fetchall()

print("=" * 100)
print("TOP 50 UNITS BY LONGEST RAW SPAN")
print("=" * 100)
print(f"{'COM':<10} {'First Date':<12} {'Last Date':<12} {'Span':>6} {'Days':>5} {'Charges':>8}")
print("-" * 100)

for com, first, last, span, days, charges in units:
    highlight = " ⚠️ HUGE!" if span > 200 else (" 🔥 BIG!" if span > 100 else "")
    print(f"{com:<10} {first:<12} {last:<12} {span:>6.0f} {days:>5} {charges:>8}{highlight}")

conn.close()
