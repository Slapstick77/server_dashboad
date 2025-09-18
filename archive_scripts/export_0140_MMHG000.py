import sqlite3, os, csv, sys
DB='SCHLabor.db'
if not os.path.isfile(DB):
    print('DB missing'); sys.exit(1)
conn=sqlite3.connect(DB)
cur=conn.cursor()
cur.execute('PRAGMA table_info(SCHLabor)')
cols=[r[1] for r in cur.fetchall()]
emp_col='EmployeeNumber1' if 'EmployeeNumber1' in cols else 'EmployeeNumber'
cur.execute('SELECT DISTINCT DepartmentNumber FROM SCHLabor WHERE COMNumber=? ORDER BY DepartmentNumber', ('MMHG000',))
print('Distinct departments for COM MMHG000:')
for d, in cur.fetchall():
    print('  ', d)
query=f"""
SELECT LoggedDate,
       {emp_col} as EmployeeNumber,
       EmployeeName,
       COMNumber,
       DepartmentNumber,
       ActualHours,
       Reference
FROM SCHLabor
WHERE (TRIM(DepartmentNumber)='0140' OR DepartmentNumber='0140')
  AND COMNumber='MMHG000'
ORDER BY LoggedDate, EmployeeNumber
"""
cur.execute(query)
rows=cur.fetchall()
out='export_0140_MMHG000.csv'
with open(out,'w',newline='',encoding='utf-8') as f:
    w=csv.writer(f)
    w.writerow(['LoggedDate','EmployeeNumber','EmployeeName','COMNumber','DepartmentNumber','ActualHours','Reference'])
    w.writerows(rows)
print('Exported', len(rows), 'rows to', out)
if rows:
    for r in rows[:5]:
        print('PREVIEW', r)
conn.close()
