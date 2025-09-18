import sqlite3, os
DB=os.path.abspath(os.path.join(os.path.dirname(__file__),'..','SCHLabor.db'))
conn=sqlite3.connect(DB)
cur=conn.cursor()
cur.execute("SELECT COMNumber, DepartmentNumber, EmployeeNumber1, strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10))) d, ActualHours FROM SCHLabor WHERE CAST(COMNumber AS TEXT)='19733' AND DepartmentNumber='0270' AND COALESCE(ActualHours,0)>0 ORDER BY d, EmployeeNumber1")
rows=cur.fetchall()
print('Rows (Electrical raw) count=', len(rows))
for r in rows: print(r)
from collections import defaultdict
emp_by_day=defaultdict(set)
for com,dept,emp,d,hrs in rows:
    emp_by_day[d].add(str(emp).strip())
print('\nPer-day employees:')
for d in sorted(emp_by_day):
    print(d, emp_by_day[d])
filtered_days=[d for d,emps in sorted(emp_by_day.items()) if not (emps=={'1205797'})]
print('\nFiltered days (drop only-1205797):', filtered_days)
if filtered_days:
    print('First:', filtered_days[0], 'Last:', filtered_days[-1], 'Count:', len(filtered_days))
else:
    print('No filtered days remain after filtering')
