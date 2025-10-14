import sqlite3

conn = sqlite3.connect('SCHLabor.db')
cursor = conn.cursor()

# Get table structure
cursor.execute('PRAGMA table_info(UnitCompletion)')
print('UnitCompletion table structure:')
print('=' * 80)
columns = cursor.fetchall()
for col in columns:
    print(f"{col[1]:20s} {col[2]:10s} {'NOT NULL' if col[3] else ''} {'PRIMARY KEY' if col[5] else ''}")

# Get row count
cursor.execute('SELECT COUNT(*) FROM UnitCompletion')
print(f'\n\nTotal rows: {cursor.fetchone()[0]}')

# Get sample data
cursor.execute('SELECT * FROM UnitCompletion LIMIT 5')
print('\nSample data (first 5 rows):')
print('=' * 80)
for row in cursor.fetchall():
    print(row)

# Check when it was last updated
cursor.execute('SELECT MAX(last_updated) FROM UnitCompletion')
last_update = cursor.fetchone()[0]
print(f'\n\nLast updated: {last_update}')

conn.close()
