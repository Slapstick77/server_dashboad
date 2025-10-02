import sqlite3, os, csv, sys
DB='SCHLabor.db'
if not os.path.isfile(DB):
    print('DB missing'); sys.exit(1)
conn=sqlite3.connect(DB)
cur=conn.cursor()
cur.execute('PRAGMA table_info(SCHLabor)')
cols=[r[1] for r in cur.fetchall()]
emp_col='EmployeeNumber1' if 'EmployeeNumber1' in cols else 'EmployeeNumber'
probes=['COMNumber','Reference','EmployeeName','DepartmentNumber']
for col in probes:
    try:
        cur.execute(f"SELECT COUNT(*) FROM SCHLabor WHERE {col} LIKE '%MMHG000%'")
        print(f'COL {col} hits={cur.fetchone()[0]}')
    except Exception as e:
        print('ERR probe', col, e)
# attempt export referencing order number in any of COMNumber (exact) or Reference contains
cur.execute(f"""
SELECT LoggedDate,
       {emp_col} as EmployeeNumber,
       EmployeeName,
       COMNumber,
       DepartmentNumber,
       ActualHours,
       Reference
FROM SCHLabor
WHERE (REPLACE(DepartmentNumber,' ','') IN ('WEL0140','WEL140'))
  AND (
        COMNumber='MMHG000' OR Reference LIKE '%MMHG000%'
      )
ORDER BY LoggedDate, EmployeeNumber
""")
rows=cur.fetchall()
out='export_WEL0140_MMHG000_any.csv'
with open(out,'w',newline='',encoding='utf-8') as f:
    w=csv.writer(f)
    w.writerow(['LoggedDate','EmployeeNumber','EmployeeName','COMNumber','DepartmentNumber','ActualHours','Reference'])
    w.writerows(rows)
print('EXPORTED_ROWS', len(rows), 'FILE', out)
if rows:
    for r in rows[:5]:
        print('PREVIEW', r)
conn.close()
