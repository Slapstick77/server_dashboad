import sqlite3

db_path = r'c:\Project p\SQRS\SCHLabor.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Check UnitCompletion table for COM 19044 (with space!)
cursor.execute("SELECT * FROM UnitCompletion WHERE com_number LIKE '%19044%'")
result = cursor.fetchone()

if result:
    print(f"✅ Found in UnitCompletion:")
    print(f"  com_number: {result[0]}")
    print(f"  last_day_unfiltered: {result[1]}")
    print(f"  last_updated: {result[2]}")
else:
    print("❌ NOT found - checking format...")
    
    # Show some examples of what's actually in there
    cursor.execute("SELECT com_number, last_day_unfiltered FROM UnitCompletion LIMIT 10")
    examples = cursor.fetchall()
    print("\nExample com_numbers in table:")
    for ex in examples:
        print(f"  '{ex[0]}' -> {ex[1]}")

# Now get top units by last_day_unfiltered date
cursor.execute("""
    SELECT com_number, last_day_unfiltered 
    FROM UnitCompletion 
    WHERE last_day_unfiltered IS NOT NULL
    ORDER BY last_day_unfiltered DESC
    LIMIT 20
""")

print("\n" + "=" * 80)
print("TOP 20 UNITS BY LAST UNFILTERED DAY (most recent completions)")
print("=" * 80)
results = cursor.fetchall()
for row in results:
    print(f"{row[0]:<15} -> Last unfiltered day: {row[1]}")

conn.close()
