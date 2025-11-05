import sqlite3
from datetime import datetime, timezone

conn = sqlite3.connect('SCHLabor.db')
cursor = conn.cursor()

# Check table schema
cursor.execute("PRAGMA table_info(DRStaticMetadata)")
columns = cursor.fetchall()
print("DRStaticMetadata columns:")
for col in columns:
    print(f"  {col[1]} ({col[2]})")

# Get recent DRs
cursor.execute("""
    SELECT deviation_number, date_created 
    FROM DRStaticMetadata 
    ORDER BY date_created DESC 
    LIMIT 5
""")
rows = cursor.fetchall()
print("\nRecent DRs:")
for row in rows:
    print(f"  DR {row[0]}: {row[1]}")

# Check timezone
print(f"\nCurrent time (UTC): {datetime.now(timezone.utc)}")
print(f"Current time (local): {datetime.now()}")

# Test timestamp conversion
if rows:
    test_date = rows[0][1]
    print(f"\nTest conversion for: {test_date}")
    dt = datetime.fromisoformat(test_date.split('.')[0])
    dt_utc = dt.replace(tzinfo=timezone.utc)
    created_ms = int(dt_utc.timestamp() * 1000)
    print(f"  Parsed as UTC: {dt_utc}")
    print(f"  Milliseconds: {created_ms}")
    
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    print(f"  Now (ms): {now_ms}")
    age_ms = now_ms - created_ms
    print(f"  Age (ms): {age_ms}")
    age_hours = age_ms / (1000 * 60 * 60)
    print(f"  Age (hours): {age_hours:.2f}")

conn.close()
