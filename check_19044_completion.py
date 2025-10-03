import sqlite3

db_path = r'c:\Project p\SQRS\SCHLabor.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Check if COM 19044 is in UnitCompletion
cursor.execute("SELECT com_number, last_day_unfiltered FROM UnitCompletion WHERE com_number = 'COM 19044'")
result = cursor.fetchone()

if result:
    print(f"✅ COM 19044 found in UnitCompletion: {result}")
else:
    print("❌ COM 19044 NOT in UnitCompletion table")
    
# Check raw charges for COM 19044
cursor.execute("""
    SELECT MIN(LoggedDate), MAX(LoggedDate), COUNT(DISTINCT LoggedDate), COUNT(*)
    FROM SCHLabor 
    WHERE CAST(COMNumber AS TEXT) = 'COM 19044'
""")
raw_data = cursor.fetchone()
print(f"\nRaw SCHLabor data for COM 19044:")
print(f"  First date: {raw_data[0]}")
print(f"  Last date: {raw_data[1]}")
print(f"  Distinct days: {raw_data[2]}")
print(f"  Total charges: {raw_data[3]}")

conn.close()
