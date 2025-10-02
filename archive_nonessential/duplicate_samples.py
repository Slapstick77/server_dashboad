import sqlite3, json

con = sqlite3.connect('SCHLabor.db')
cur = con.cursor()

# Sample exact full-row duplicate groups (all 8 columns identical)
cur.execute("""
WITH d AS (
  SELECT LoggedDate, COMNumber, EmployeeName, EmployeeNumber1, DepartmentNumber, Area,
         ActualHours, IFNULL(Reference,'') AS Reference, COUNT(*) c
  FROM SCHLabor_rebuild
  GROUP BY 1,2,3,4,5,6,7,8
  HAVING c>1
)
SELECT LoggedDate, EmployeeNumber1, COMNumber, DepartmentNumber, Area, ActualHours, Reference, c
FROM d
LIMIT 5
""")
exact_groups = cur.fetchall()

exact_details = []
for g in exact_groups:
    lg, emp, com, dept, area, hours, ref, c = g
    cur2 = con.cursor()
    cur2.execute("""
        SELECT LoggedDate, COMNumber, EmployeeName, EmployeeNumber1, DepartmentNumber, Area, ActualHours, IFNULL(Reference,'')
        FROM SCHLabor_rebuild
        WHERE LoggedDate=? AND EmployeeNumber1=? AND COMNumber=? AND DepartmentNumber=? AND Area=? AND IFNULL(Reference,'')=? AND ActualHours=?
        LIMIT 10
    """, (lg, emp, com, dept, area, ref, hours))
    rows = cur2.fetchall()
    exact_details.append({'group': {'LoggedDate': lg, 'EmployeeNumber1': emp, 'COMNumber': com, 'DepartmentNumber': dept, 'Area': area, 'ActualHours': hours, 'Reference': ref, 'row_count': c}, 'rows': rows})

# Sample hour-variant groups (differ only by ActualHours among otherwise identical 7 fields)
cur.execute("""
WITH v AS (
  SELECT LoggedDate, COMNumber, EmployeeName, EmployeeNumber1, DepartmentNumber, Area, IFNULL(Reference,'') AS Reference,
         COUNT(*) c, COUNT(DISTINCT ActualHours) dh
  FROM SCHLabor_rebuild
  GROUP BY 1,2,3,4,5,6,7
  HAVING dh>1
)
SELECT LoggedDate, EmployeeNumber1, COMNumber, DepartmentNumber, Area, Reference, c
FROM v
LIMIT 5
""")
variant_groups = cur.fetchall()

variant_details = []
for g in variant_groups:
    lg, emp, com, dept, area, ref, c = g
    cur2 = con.cursor()
    cur2.execute("""
        SELECT LoggedDate, COMNumber, EmployeeName, EmployeeNumber1, DepartmentNumber, Area, ActualHours, IFNULL(Reference,'')
        FROM SCHLabor_rebuild
        WHERE LoggedDate=? AND EmployeeNumber1=? AND COMNumber=? AND DepartmentNumber=? AND Area=? AND IFNULL(Reference,'')=?
        ORDER BY ActualHours
        LIMIT 12
    """, (lg, emp, com, dept, area, ref))
    rows = cur2.fetchall()
    variant_details.append({'group': {'LoggedDate': lg, 'EmployeeNumber1': emp, 'COMNumber': com, 'DepartmentNumber': dept, 'Area': area, 'Reference': ref, 'row_count': c}, 'rows': rows})

print(json.dumps({'exact_full_row_duplicates': exact_details, 'hour_variant_duplicates': variant_details}, indent=2)[:8000])
