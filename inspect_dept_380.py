import sqlite3, os, sys
DB='SCHLabor.db'
START='2024-08-31'
END='2025-08-31'
if not os.path.isfile(DB):
    print('DB missing'); sys.exit(1)
conn=sqlite3.connect(DB)
cur=conn.cursor()
print('Distinct DepartmentNumber values containing 380 (any time):')
for row in cur.execute("SELECT DISTINCT DepartmentNumber FROM SCHLabor WHERE DepartmentNumber LIKE '%380%' ORDER BY 1"):
    print(repr(row[0]))
print('\nCounts by exact trimmed department (date window):')
cur.execute("SELECT REPLACE(TRIM(DepartmentNumber),'  ',' ') as d, COUNT(DISTINCT COMNumber) FROM SCHLabor WHERE LoggedDate BETWEEN ? AND ? GROUP BY d ORDER BY 2 DESC", (START,END))
rows=cur.fetchall()
for d,cnt in rows:
    if d and '380' in d:
        print(f"{d!r}: {cnt}")
print('\nHeuristic COM count using pattern variants (date window):')
patterns=["%380%","0380%","%380","380"]
for p in patterns:
    cur.execute("SELECT COUNT(DISTINCT COMNumber) FROM SCHLabor WHERE LoggedDate BETWEEN ? AND ? AND DepartmentNumber LIKE ?", (START,END,p))
    print(p, cur.fetchone()[0])
# Attempt normalizing approach
cur.execute("SELECT COUNT(DISTINCT COMNumber) FROM SCHLabor WHERE LoggedDate BETWEEN ? AND ? AND REPLACE(REPLACE(DepartmentNumber,'0',''),' ','') LIKE '%38%'", (START,END))
print("Normalized rough (remove zeros/spaces) contains '38':", cur.fetchone()[0])
conn.close()
