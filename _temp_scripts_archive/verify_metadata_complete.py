import sqlite3

conn = sqlite3.connect('SCHLabor.db')
cur = conn.cursor()

print('='*80)
print('DRStaticMetadata - With Deviation Type and Component')
print('='*80)

cur.execute('SELECT COUNT(*) FROM DRStaticMetadata')
print(f'\nTotal DRs with metadata: {cur.fetchone()[0]}')

cur.execute('''
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN deviation_type IS NOT NULL THEN 1 END) as has_dev_type,
        COUNT(CASE WHEN component IS NOT NULL THEN 1 END) as has_component,
        COUNT(CASE WHEN urgency IS NOT NULL THEN 1 END) as has_urgency,
        COUNT(CASE WHEN defect_description IS NOT NULL THEN 1 END) as has_defect,
        COUNT(CASE WHEN charged_to_dept IS NOT NULL THEN 1 END) as has_charged_to
    FROM DRStaticMetadata
''')
row = cur.fetchone()
print(f'\nField coverage:')
print(f'  Deviation Type: {row[1]}/{row[0]}')
print(f'  Component: {row[2]}/{row[0]}')
print(f'  Urgency: {row[3]}/{row[0]}')
print(f'  Defect: {row[4]}/{row[0]}')
print(f'  Charged To: {row[5]}/{row[0]}')

print(f'\n' + '='*80)
print('Sample DRs with all fields:')
print('='*80)

cur.execute('''
    SELECT 
        deviation_number, 
        urgency,
        defect_description,
        charged_to_dept,
        deviation_type,
        component
    FROM DRStaticMetadata
    WHERE deviation_type IS NOT NULL
    ORDER BY deviation_number DESC
    LIMIT 10
''')

for row in cur.fetchall():
    dn, urg, defect, charged, dev_type, comp = row
    urg_short = urg[:30] + '...' if urg and len(urg) > 30 else urg
    defect_short = defect[:30] + '...' if defect and len(defect) > 30 else defect
    print(f'\nDR {dn}:')
    print(f'  Urgency: {urg_short}')
    print(f'  Defect: {defect_short}')
    print(f'  Charged To: {charged or "None"}')
    print(f'  Deviation Type: {dev_type}')
    print(f'  Component: {comp}')

conn.close()
