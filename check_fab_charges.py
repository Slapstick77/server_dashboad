"""
Show ALL 37 Fab charges for COM 19044 chronologically.
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'SCHLabor.db')

def get_conn():
    return sqlite3.connect(DB_PATH)

print("=" * 80)
print("ALL FAB CHARGES FOR COM 19044 (Chronological Order)")
print("=" * 80)

with get_conn() as conn:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Get all Fab charges for COM 19044
    cur.execute("""
        SELECT 
            strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10))) as date,
            COALESCE(ActualHours, 0) as hours,
            LoggedDate,
            iso_logged_date
        FROM SCHLabor
        WHERE CAST(COMNumber AS TEXT) IN ('19044', '019044')
          AND DepartmentNumber = '0120'
          AND COALESCE(ActualHours, 0) > 0
        ORDER BY date, LoggedDate
    """)
    
    fab_charges = cur.fetchall()

print(f"\nTotal Fab charges: {len(fab_charges)}")
print(f"\n{'#':<4} {'Date':<12} {'Hours':<8} {'Month':<12}")
print("-" * 50)

# Track by month
from collections import defaultdict
monthly_totals = defaultdict(lambda: {'charges': 0, 'hours': 0})

for i, charge in enumerate(fab_charges, 1):
    date = charge['date']
    hours = charge['hours']
    month = date[:7] if date else 'Unknown'
    
    print(f"{i:<4} {date:<12} {hours:<8.1f} {month:<12}")
    
    monthly_totals[month]['charges'] += 1
    monthly_totals[month]['hours'] += hours

# Summary by month
print(f"\n{'=' * 80}")
print(f"MONTHLY SUMMARY:")
print(f"{'=' * 80}")
print(f"\n{'Month':<12} {'Charges':<10} {'Total Hours':<12}")
print("-" * 40)

total_charges = 0
total_hours = 0
for month in sorted(monthly_totals.keys()):
    data = monthly_totals[month]
    print(f"{month:<12} {data['charges']:<10} {data['hours']:<12.1f}")
    total_charges += data['charges']
    total_hours += data['hours']

print("-" * 40)
print(f"{'TOTAL':<12} {total_charges:<10} {total_hours:<12.1f}")

# Gap analysis
print(f"\n{'=' * 80}")
print(f"GAP ANALYSIS:")
print(f"{'=' * 80}")

dates = [charge['date'] for charge in fab_charges]
from datetime import date, timedelta

print(f"\nLongest gaps between charges:")
gaps = []
for i in range(1, len(dates)):
    prev_date = date.fromisoformat(dates[i-1])
    curr_date = date.fromisoformat(dates[i])
    gap_days = (curr_date - prev_date).days
    if gap_days > 1:
        gaps.append({
            'from': dates[i-1],
            'to': dates[i],
            'days': gap_days
        })

# Sort by gap size
gaps.sort(key=lambda x: x['days'], reverse=True)

print(f"\n{'From Date':<12} {'To Date':<12} {'Gap (days)':<12}")
print("-" * 40)
for gap in gaps[:10]:  # Top 10 gaps
    print(f"{gap['from']:<12} {gap['to']:<12} {gap['days']:<12}")

print(f"\n{'=' * 80}")
