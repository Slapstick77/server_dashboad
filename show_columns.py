import sqlite3

db_path = r'c:\Project p\SQRS\SCHLabor.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("PRAGMA table_info(SCHLabor)")
cols = cursor.fetchall()

print("SCHLabor columns:")
for col in cols[:15]:
    print(f"  {col[1]} ({col[2]})")

conn.close()
