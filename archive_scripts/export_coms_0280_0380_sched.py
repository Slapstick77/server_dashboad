import sqlite3, os, csv, sys
DB='SCHLabor.db'
START='2024-08-31'
END='2025-08-31'
if not os.path.isfile(DB):
    print('DB missing'); sys.exit(1)
conn=sqlite3.connect(DB)
cur=conn.cursor()
# Get COMs with dept 0280
cur.execute("""
SELECT DISTINCT COMNumber FROM SCHLabor
WHERE LoggedDate BETWEEN ? AND ?
  AND COMNumber LIKE '1%'
  AND TRIM(DepartmentNumber)='0280'
  AND COMNumber IS NOT NULL AND COMNumber!=''
""", (START,END))
coms=[r[0] for r in cur.fetchall()]
print('COMS_0280', len(coms))
# Hours for dept 0380
hours={}
if coms:
    cur.execute(f"""
        SELECT COMNumber, SUM(ActualHours) FROM SCHLabor
        WHERE LoggedDate BETWEEN ? AND ?
          AND TRIM(DepartmentNumber)='0380'
          AND COMNumber IN ({','.join(['?']*len(coms))})
        GROUP BY COMNumber
    """, [START,END,*coms])
    for c,h in cur.fetchall():
        hours[c]=h or 0
# Scheduling summary lookup
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='SCHSchedulingSummary'")
if cur.fetchone():
    cur.execute('PRAGMA table_info(SCHSchedulingSummary)')
    scols=[r[1] for r in cur.fetchall()]
    def find(*cands):
        cl=[c.lower() for c in scols]
        for cand in cands:
            if cand.lower() in cl:
                return scols[cl.index(cand.lower())]
        return None
    actual=find('ActualHours','Actual_Hours','HoursActual')
    std=find('StdHours','StandardHours','Std_Hours','StdHours1')
    select_cols=['comnumber1']
    if actual: select_cols.append(actual)
    if std: select_cols.append(std)
    if coms:
        cur.execute(f"SELECT {','.join(select_cols)} FROM SCHSchedulingSummary WHERE comnumber1 IN ({','.join(['?']*len(coms))})", coms)
        sched_rows=cur.fetchall()
    else:
        sched_rows=[]
else:
    actual=std=None
    sched_rows=[]
# map sched
sched_map={}
for row in sched_rows:
    sched_map[row[0]]=row[1:]
# export
out='export_COMs_0280to0380_sched.csv'
with open(out,'w',newline='',encoding='utf-8') as f:
    w=csv.writer(f)
    w.writerow(['COMNumber','HoursDept0380','SchedActual','SchedStd'])
    for com in sorted(coms):
        a=None; s=None
        if com in sched_map:
            if len(sched_map[com])>0: a=sched_map[com][0]
            if len(sched_map[com])>1: s=sched_map[com][1]
        w.writerow([com, hours.get(com,0), a, s])
print('Exported to', out)
conn.close()
