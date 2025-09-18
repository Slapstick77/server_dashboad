import sqlite3, json
conn=sqlite3.connect('SCHLabor.db')
cur=conn.cursor()
# Group by all data columns (treat NULL as '') for duplicate detection
cur.execute("""
SELECT COUNT(*) dup_groups
FROM (
  SELECT LoggedDate,
         IFNULL(COMNumber,'') as COMNumber,
         IFNULL(EmployeeName,'') as EmployeeName,
         IFNULL(EmployeeNumber,'') as EmployeeNumber,
         IFNULL(DepartmentNumber,'') as DepartmentNumber,
         IFNULL(Area,'') as Area,
         IFNULL(ActualHours,'') as ActualHours,
         IFNULL(Reference,'') as Reference,
         COUNT(*) c
  FROM SCHLabor
  GROUP BY 1,2,3,4,5,6,7,8
  HAVING c>1
)
""")
row=cur.fetchone()
dup_groups = row[0] if row else 0
print('Duplicate groups:', dup_groups)
if dup_groups:
    # Show up to 10 sample duplicate signatures with their counts
    cur.execute("""
    SELECT LoggedDate,
           IFNULL(COMNumber,''),
           IFNULL(EmployeeName,''),
           IFNULL(EmployeeNumber,''),
           IFNULL(DepartmentNumber,''),
           IFNULL(Area,''),
           IFNULL(ActualHours,''),
           IFNULL(Reference,''),
           COUNT(*) c
    FROM SCHLabor
    GROUP BY 1,2,3,4,5,6,7,8
    HAVING c>1
    ORDER BY c DESC, LoggedDate
    LIMIT 10
    """)
    samples = cur.fetchall()
    print('Sample duplicate signatures (up to 10):')
    for s in samples:
        print(s)
print('Done')
conn.close()
