import sqlite3
from datetime import datetime

conn = sqlite3.connect('SCHLabor.db')
cursor = conn.cursor()
cursor.execute('''
    SELECT s.deviation_number, m.date_created, s.latest_routing_touched 
    FROM DRItemSnapshot s 
    JOIN DRStaticMetadata m ON s.deviation_number = m.deviation_number 
    WHERE s.deviation_number IN (49326, 49325, 49324, 49323, 49322, 49318)
    ORDER BY s.id DESC LIMIT 10
''')
rows = cursor.fetchall()

seen = set()
for r in rows:
    if r[0] not in seen:
        seen.add(r[0])
        created = r[1]
        touched = r[2]
        if created and touched:
            created_dt = datetime.fromisoformat(created.split('.')[0])
            touched_dt = datetime.fromisoformat(touched.split('.')[0])
            diff = (touched_dt - created_dt).total_seconds() / 60  # minutes
            print(f'DR {r[0]}:')
            print(f'  created: {created}')
            print(f'  touched: {touched}')
            print(f'  diff: {diff:.1f} minutes')
            print()

conn.close()
