import sqlite3

conn = sqlite3.connect('SCHLabor.db')
cur = conn.cursor()

cur.execute('PRAGMA table_info(DRStaticMetadata)')
cols = [r[1] for r in cur.fetchall()]

print('DRStaticMetadata columns:')
for c in cols:
    print(f'  - {c}')

conn.close()
