"""Check PartsTracker table for DR-related data"""
import sqlite3

conn = sqlite3.connect('SCHLabor.db')
cursor = conn.cursor()

# Check column names
cursor.execute("PRAGMA table_info(PartsTracker)")
columns = cursor.fetchall()
print("PartsTracker columns:")
for col in columns:
    print(f"  {col[1]} ({col[2]})")

print("\n" + "="*80)
print("Sample PartsTracker data (first 10 rows):")
print("="*80)

cursor.execute("SELECT * FROM PartsTracker LIMIT 10")
rows = cursor.fetchall()
col_names = [desc[0] for desc in cursor.description]
print(f"\nColumns: {col_names}\n")

for i, row in enumerate(rows, 1):
    print(f"\nRow {i}:")
    for col_name, value in zip(col_names, row):
        print(f"  {col_name}: {value}")

# Check if there's any DR# data in the part field
print("\n" + "="*80)
print("Checking for DR# patterns in 'part' column:")
print("="*80)
cursor.execute("""
    SELECT DISTINCT part 
    FROM PartsTracker 
    WHERE part LIKE '%DR%' OR part LIKE '%dr%'
    LIMIT 20
""")
dr_parts = cursor.fetchall()
if dr_parts:
    print(f"Found {len(dr_parts)} parts with 'DR' in name:")
    for part in dr_parts:
        print(f"  {part[0]}")
else:
    print("No parts found with 'DR' in the name")

# Check total record count
cursor.execute("SELECT COUNT(*) FROM PartsTracker")
total = cursor.fetchone()[0]
print(f"\nTotal records in PartsTracker: {total:,}")

# Check distinct COMs
cursor.execute("SELECT COUNT(DISTINCT com) FROM PartsTracker")
distinct_coms = cursor.fetchone()[0]
print(f"Distinct COMs in PartsTracker: {distinct_coms:,}")

conn.close()
