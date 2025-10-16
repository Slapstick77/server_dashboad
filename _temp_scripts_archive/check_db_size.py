import sqlite3
import os

db_path = 'SCHLabor.db'
file_size_mb = os.path.getsize(db_path) / (1024 * 1024)
print(f"Database file size: {file_size_mb:.2f} MB\n")

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in cursor.fetchall()]

print("Tables and row counts:")
table_info = []
for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    table_info.append((table, count))
    print(f"  {table}: {count:,} rows")

# Check page size and page count
cursor.execute("PRAGMA page_size")
page_size = cursor.fetchone()[0]
cursor.execute("PRAGMA page_count")
page_count = cursor.fetchone()[0]

print(f"\nDatabase stats:")
print(f"  Page size: {page_size:,} bytes")
print(f"  Page count: {page_count:,}")
print(f"  Calculated size: {(page_size * page_count) / (1024 * 1024):.2f} MB")

# Check for fragmentation
cursor.execute("PRAGMA freelist_count")
freelist = cursor.fetchone()[0]
print(f"  Free pages: {freelist:,} ({(freelist * page_size) / (1024 * 1024):.2f} MB unused)")

conn.close()
