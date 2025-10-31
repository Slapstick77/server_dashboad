import sqlite3
from datetime import datetime, date, timedelta

conn = sqlite3.connect('SCHLabor.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("\n" + "="*80)
print("LABOR DATA DIAGNOSIS:")
print("="*80)

# Check date format in LoggedDate
print("\n📅 Sample LoggedDate values (last 10 rows):")
cur.execute("SELECT LoggedDate FROM SCHLabor ORDER BY ROWID DESC LIMIT 10")
samples = cur.fetchall()
for s in samples:
    print(f"   {s['LoggedDate']}")

# Try to find most recent dates
print("\n📊 Finding most recent data:")
cur.execute("""
    SELECT LoggedDate, COUNT(*) as cnt
    FROM SCHLabor
    GROUP BY LoggedDate
    ORDER BY LoggedDate DESC
    LIMIT 20
""")
recent = cur.fetchall()
for r in recent:
    print(f"   {r['LoggedDate']}: {r['cnt']:,} rows")

# Check if there's data from last 90 days
print("\n🔍 Checking for recent data (various date formats):")

# Try ISO format (YYYY-MM-DD)
today_iso = date.today().isoformat()
ninety_days_ago_iso = (date.today() - timedelta(days=90)).isoformat()
cur.execute(f"""
    SELECT COUNT(*) as cnt 
    FROM SCHLabor 
    WHERE LoggedDate >= '{ninety_days_ago_iso}'
""")
iso_count = cur.fetchone()['cnt']
print(f"   ISO format (YYYY-MM-DD) - Last 90 days: {iso_count:,} rows")

# Try M/D/YYYY format
today_mdy = date.today().strftime('%-m/%-d/%Y') if hasattr(date.today(), 'strftime') else None
if today_mdy:
    cur.execute(f"""
        SELECT COUNT(*) as cnt 
        FROM SCHLabor 
        WHERE LoggedDate LIKE '%/2024' OR LoggedDate LIKE '%/2025'
    """)
    slash_count = cur.fetchone()['cnt']
    print(f"   Slash format (M/D/YYYY) - 2024/2025 data: {slash_count:,} rows")

# Check what complete units are being calculated from
print("\n✅ Complete Units Analysis:")
cur.execute("""
    SELECT 
        COUNT(DISTINCT COMNumber) as total_coms,
        COUNT(DISTINCT LoggedDate) as unique_dates,
        MIN(LoggedDate) as earliest,
        MAX(LoggedDate) as latest
    FROM SCHLabor
""")
summary = cur.fetchone()
print(f"   Total COMs: {summary['total_coms']:,}")
print(f"   Date range: {summary['earliest']} to {summary['latest']}")
print(f"   Unique dates: {summary['unique_dates']:,}")

# Check SCHSchedulingSummary for completion status
print("\n📋 SCHSchedulingSummary Completion Status:")
cur.execute("""
    SELECT 
        COUNT(*) as total_units,
        SUM(CASE WHEN CAST("Assembly Completion" AS REAL) = 100 THEN 1 ELSE 0 END) as assy_complete
    FROM SCHSchedulingSummary
""")
sched = cur.fetchone()
print(f"   Total units in schedule: {sched['total_units']:,}")
print(f"   Assembly 100% complete: {sched['assy_complete']:,}")

conn.close()

print("\n" + "="*80)
print("💡 DIAGNOSIS:")
print("="*80)
print("If you see:")
print("  - Recent dates (Oct 2025) in the data → Good, data is fresh")
print("  - Only old dates (Sept 2024 or earlier) → Need to sync new data")
print("  - Mixed date formats → Might cause issues with date comparisons")
print("  - Low complete unit count → Not enough data to show trends")
