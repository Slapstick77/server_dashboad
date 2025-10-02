import sqlite3, os
DB=os.path.abspath(os.path.join(os.path.dirname(__file__),'..','SCHLabor.db'))
conn=sqlite3.connect(DB)
cur=conn.cursor()
cur.execute("SELECT EmployeeNumber1, EmployeeName FROM SCHLabor WHERE EmployeeNumber1=3079985 AND EmployeeName IS NOT NULL LIMIT 1")
row=cur.fetchone()
print(row)
