import sqlite3

conn = sqlite3.connect('SCHLabor.db')
cur = conn.cursor()

# Get all tables
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [row[0] for row in cur.fetchall()]

print("Tables in database:")
for table in tables:
    print(f"\n{table}:")
    cur.execute(f"PRAGMA table_info({table})")
    cols = cur.fetchall()
    print("  Columns:", [col[1] for col in cols])

conn.close()
