import sqlite3
conn=sqlite3.connect('SCHLabor.db')
cur=conn.cursor()
for d in ('2025-08-22','2025-08-23'):
  cur.execute('select count(*) from SCHLabor where LoggedDate=?',(d,))
  print(d, cur.fetchone()[0])
cur.execute('select count(*) from SCHLabor')
print('Total rows now', cur.fetchone()[0])
conn.close()
