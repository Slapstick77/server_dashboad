import sqlite3, os, itertools, csv
DB='SCHLabor.db'
conn=sqlite3.connect(DB)
cur=conn.cursor()
print('--- SCHLabor schema ---')
try:
    cur.execute("PRAGMA table_info(SCHLabor)")
    for r in cur.fetchall():
        print(r)
except Exception as e:
    print('Schema read error',e)
print('\nTotal row count:')
try:
    cur.execute('select count(*) from SCHLabor')
    print(cur.fetchone()[0])
except Exception as e:
    print('Count error',e)
fn='SCHLabor_20250822.csv'
if os.path.isfile(fn):
    with open(fn,'r',encoding='utf-8') as f:
        lines=[l.rstrip('\n') for l in f]
    hi=None
    for i,l in enumerate(lines):
        if l.startswith('LoggedDate,'):
            hi=i;break
    if hi is not None:
        data_lines=[l for l in lines[hi+1:] if l.strip()]
        print(f'File {fn} data rows: {len(data_lines)}')
        print('Sample parsed first 3:')
        for l in itertools.islice(data_lines,3):
            print(next(csv.reader([l])))
    else:
        print('Header not found in file',fn)
try:
    cur.execute("select count(*) from SCHLabor where LoggedDate='2025-08-22'")
    print('Rows in DB for 2025-08-22:', cur.fetchone()[0])
except Exception as e:
    print('Date count error', e)
try:
    cur.execute("select LoggedDate, COMNumber, EmployeeName, EmployeeNumber, DepartmentNumber, Area, ActualHours from SCHLabor where LoggedDate='2025-08-22' limit 10")
    for r in cur.fetchall():
        print(r)
except Exception as e:
    print('Select error', e)
conn.close()
