import sqlite3
from datetime import datetime

db_path = r'c:\Project p\SQRS\SCHLabor.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get all completed units with their date ranges and span
query = """
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
HAVING span_days > 20
ORDER BY span_days DESC
LIMIT 50
"""

cursor.execute(query)
results = cursor.fetchall()

print("=" * 100)
print("TOP 50 UNITS WITH HIGHEST SPAN (>20 days)")
print("=" * 100)
print(f"{'COM':<12} {'First Date':<12} {'Last Date':<12} {'Span':>6} {'Days':>5} {'Charges':>8} {'Gap Info'}")
print("-" * 100)

for row in results:
    com, first_date, last_date, span, num_days, num_charges = row
    
    # Calculate average gap
    if num_days > 1:
        avg_gap = (span / (num_days - 1)) if num_days > 1 else 0
    else:
        avg_gap = 0
    
    # Find max gap between consecutive dates for this unit
    cursor.execute("""
        WITH dates AS (
            SELECT DISTINCT LoggedDate, JULIANDAY(LoggedDate) as jd
            FROM SCHLabor
            WHERE CAST(COMNumber AS TEXT) = ?
            ORDER BY LoggedDate
        ),
        gaps AS (
            SELECT 
                LoggedDate,
                jd - LAG(jd) OVER (ORDER BY jd) as gap
            FROM dates
        )
        SELECT MAX(gap) as max_gap
        FROM gaps
        WHERE gap IS NOT NULL
    """, (com,))
    
    max_gap_result = cursor.fetchone()
    max_gap = max_gap_result[0] if max_gap_result and max_gap_result[0] else 0
    
    gap_info = f"Max gap: {max_gap:.0f}d"
    if max_gap > 30:
        gap_info += " ⚠️ EXCEEDS 30-DAY LIMIT"
    
    print(f"{com:<12} {first_date:<12} {last_date:<12} {span:>6.0f} {num_days:>5} {num_charges:>8} {gap_info}")

print("=" * 100)

# Now check which of these would be filtered by max_gap_override
print("\n" + "=" * 100)
print("UNITS AFFECTED BY 30-DAY MAX GAP FILTER")
print("=" * 100)

filtered_count = 0
for row in results:
    com = row[0]
    
    # Check if this unit has any gap > 30 days
    cursor.execute("""
        WITH dates AS (
            SELECT DISTINCT LoggedDate, JULIANDAY(LoggedDate) as jd
            FROM SCHLabor
            WHERE CAST(COMNumber AS TEXT) = ?
            ORDER BY LoggedDate
        ),
        gaps AS (
            SELECT 
                LoggedDate,
                jd - LAG(jd) OVER (ORDER BY jd) as gap
            FROM dates
        )
        SELECT MAX(gap) as max_gap
        FROM gaps
        WHERE gap IS NOT NULL
    """, (com,))
    
    max_gap_result = cursor.fetchone()
    max_gap = max_gap_result[0] if max_gap_result and max_gap_result[0] else 0
    
    if max_gap > 30:
        filtered_count += 1
        print(f"{com:<12} - Max gap: {max_gap:.0f} days - WOULD BE FILTERED")

print(f"\nTotal units with gaps > 30 days: {filtered_count} out of {len(results)}")

conn.close()
