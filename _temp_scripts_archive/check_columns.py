import sqlite3

conn = sqlite3.connect('SCHLabor.db')
cur = conn.cursor()

# Check what columns exist in DRItemSnapshot
cur.execute("PRAGMA table_info(DRItemSnapshot)")
cols = cur.fetchall()

print("Current DRItemSnapshot columns:")
for col in cols:
    print(f"  {col[1]} ({col[2]})")

# Check if the unwanted columns exist
unwanted = ['urgency', 'deviation_type', 'defect', 'part_number']
existing_unwanted = [col[1] for col in cols if col[1] in unwanted]

if existing_unwanted:
    print(f"\n❌ Found unwanted columns: {existing_unwanted}")
    print("These should be removed from DRItemSnapshot (they belong only in DRStaticMetadata)")
else:
    print("\n✅ No unwanted columns found!")

conn.close()
