import sqlite3, csv, os, sys
DB='SCHLabor.db'
if not os.path.isfile(DB):
    print('ERROR: DB file not found:', DB)
    sys.exit(1)
conn=sqlite3.connect(DB)
cur=conn.cursor()
cur.execute('PRAGMA table_info(SCHLabor)')
cols=[r[1] for r in cur.fetchall()]
emp_col='EmployeeNumber1' if 'EmployeeNumber1' in cols else 'EmployeeNumber'
query=f"""
SELECT LoggedDate,
       {emp_col} as EmployeeNumber,
       EmployeeName,
       COMNumber,
       DepartmentNumber,
       ActualHours,
       Reference
FROM SCHLabor
WHERE TRIM(DepartmentNumber)=? AND COMNumber=?
ORDER BY LoggedDate, EmployeeNumber
"""
params=('WEL 0140','MMHG000')
cur.execute(query, params)
rows=cur.fetchall()
out_file='export_WEL0140_MMHG000.csv'
with open(out_file,'w',newline='',encoding='utf-8') as f:
    w=csv.writer(f)
    w.writerow(['LoggedDate','EmployeeNumber','EmployeeName','COMNumber','DepartmentNumber','ActualHours','Reference'])
    w.writerows(rows)
print('Exported', len(rows), 'rows to', out_file)
if rows:
    for r in rows[:5]:
        print('PREVIEW', r)
conn.close()
