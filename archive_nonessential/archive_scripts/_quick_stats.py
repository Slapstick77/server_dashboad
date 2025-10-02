import sqlite3, collections
conn=sqlite3.connect('SCHLabor.db')
cur=conn.cursor()
cur.execute("select count(*) from SCHLabor")
rows=cur.fetchone()[0]
cur.execute("select min(LoggedDate), max(LoggedDate) from SCHLabor")
min_d,max_d=cur.fetchone()
# daily counts
cur.execute("select LoggedDate, count(*) c from SCHLabor group by LoggedDate order by LoggedDate")
daily=cur.fetchall()
avg = sum(c for _,c in daily)/len(daily) if daily else 0
# distinct by composite key (should equal total since we enforced dedupe logic)
cur.execute("select count(*) from (select LoggedDate, IFNULL(EmployeeNumber,''), IFNULL(COMNumber,''), IFNULL(DepartmentNumber,''), IFNULL(Reference,'') from SCHLabor group by 1,2,3,4,5)")
distinct_rows=cur.fetchone()[0]
# top 5 busiest days
top5=sorted(daily, key=lambda x:x[1], reverse=True)[:5]
print({'total_rows':rows,'distinct_composite':distinct_rows,'min_date':min_d,'max_date':max_d,'days':len(daily),'avg_per_day':round(avg,2),'top5':top5})
conn.close()