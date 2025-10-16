import sqlite3

conn = sqlite3.connect('SCHLabor.db')
cur = conn.cursor()

# Count rows
cur.execute('SELECT COUNT(*) FROM DRStaticMetadata')
count = cur.fetchone()[0]
print(f'DRStaticMetadata rows: {count}')

# Show samples
cur.execute('''
    SELECT deviation_number, urgency, defect_description, charged_to_dept 
    FROM DRStaticMetadata 
    LIMIT 10
''')
print('\nSample metadata:')
for row in cur.fetchall():
    print(f'  DR {row[0]}: urgency={row[1][:30] if row[1] else None}, defect={row[2][:30] if row[2] else None}, dept={row[3]}')

conn.close()
