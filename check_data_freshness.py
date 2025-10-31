import sqlite3
from datetime import datetime, date

conn = sqlite3.connect('SCHLabor.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("\n" + "="*80)
print("DATA FRESHNESS CHECK:")
print("="*80)

# Check SCHLabor table
print("\n📊 SCHLabor Table:")
cur.execute("SELECT MAX(LoggedDate) as latest_date, COUNT(*) as total_rows FROM SCHLabor")
row = cur.fetchone()
print(f"   Latest LoggedDate: {row['latest_date']}")
print(f"   Total rows: {row['total_rows']:,}")

if row['latest_date']:
    latest = datetime.strptime(row['latest_date'], '%Y-%m-%d').date()
    today = date.today()
    days_behind = (today - latest).days
    
    if days_behind == 0:
        print(f"   ✅ Data is CURRENT (today's date)")
    elif days_behind == 1:
        print(f"   ⚠️  Data is 1 day behind")
    else:
        print(f"   ❌ Data is {days_behind} DAYS BEHIND!")

# Check recent data volume
print("\n📅 Recent Data Volume (last 10 days):")
cur.execute("""
    SELECT LoggedDate, COUNT(*) as row_count
    FROM SCHLabor
    WHERE LoggedDate >= date('now', '-10 days')
    GROUP BY LoggedDate
    ORDER BY LoggedDate DESC
""")

recent = cur.fetchall()
if recent:
    for r in recent:
        print(f"   {r['LoggedDate']}: {r['row_count']:,} rows")
else:
    print("   ❌ No data in last 10 days!")

# Check SCHSchedulingSummary
print("\n📋 SCHSchedulingSummary Table:")
cur.execute("SELECT COUNT(*) as total_rows FROM SCHSchedulingSummary")
row = cur.fetchone()
print(f"   Total rows: {row['total_rows']:,}")

# Check complete units
print("\n✅ Complete Units (last 30 days):")
cur.execute("""
    SELECT 
        LoggedDate as work_date,
        COUNT(DISTINCT COMNumber) as unit_count
    FROM SCHLabor
    WHERE LoggedDate >= date('now', '-30 days')
    GROUP BY LoggedDate
    ORDER BY LoggedDate DESC
    LIMIT 10
""")

completions = cur.fetchall()
if completions:
    for c in completions:
        print(f"   {c['work_date']}: {c['unit_count']} COMs worked")
else:
    print("   ❌ No labor data in last 30 days!")

# Check RunLog for sync history
print("\n🔄 Data Sync History (RunLog):")
cur.execute("""
    SELECT run_started, run_completed, status
    FROM RunLog
    ORDER BY run_started DESC
    LIMIT 5
""")

syncs = cur.fetchall()
if syncs:
    for s in syncs:
        print(f"   {s['run_started']} -> {s['run_completed']} [{s['status']}]")
else:
    print("   ⚠️  No RunLog entries found")

conn.close()
