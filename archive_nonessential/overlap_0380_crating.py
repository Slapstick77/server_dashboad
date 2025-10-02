import sqlite3, os
DB='SCHLabor.db'
if not os.path.isfile(DB): raise SystemExit('DB missing')
conn=sqlite3.connect(DB)
cur=conn.cursor()
cur.execute("SELECT DISTINCT COMNumber FROM SCHLabor WHERE iso_logged_date BETWEEN '2024-08-31' AND '2025-08-31' AND TRIM(DepartmentNumber)='0380'")
coms=[r[0] for r in cur.fetchall()]
print('0380 labor COMs:', len(coms))
if not coms: raise SystemExit
ph=','.join(['?']*len(coms))
cur.execute(f"SELECT COUNT(*) FROM SCHSchedulingSummary WHERE comnumber1 IN ({ph})", coms)
print('Have scheduling row:', cur.fetchone()[0])
cur.execute(f"SELECT COUNT(*) FROM SCHSchedulingSummary WHERE comnumber1 IN ({ph}) AND (cratingstdhrs IS NOT NULL OR cratingacthrs IS NOT NULL)", coms)
print('Have any crating values (nullable check):', cur.fetchone()[0])
cur.execute(f"SELECT comnumber1, cratingstdhrs, cratingacthrs FROM SCHSchedulingSummary WHERE comnumber1 IN ({ph}) AND (cratingstdhrs IS NOT NULL OR cratingacthrs IS NOT NULL) LIMIT 10", coms)
print('Sample with crating values:')
for r in cur.fetchall():
    print(r)
conn.close()
