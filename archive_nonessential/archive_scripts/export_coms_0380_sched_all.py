import sqlite3, os, csv, sys
DB='SCHLabor.db'
START='2024-08-31'
END='2025-08-31'
ONLY_PREFIX='1'  # adjust if you want all COMs regardless of starting digit
if not os.path.isfile(DB):
    print('DB missing'); sys.exit(1)
conn=sqlite3.connect(DB)
cur=conn.cursor()
# 1. All COMs worked on (any department) in window (optionally filter prefix)
if ONLY_PREFIX:
    cur.execute("""
        SELECT DISTINCT COMNumber FROM SCHLabor
        WHERE LoggedDate BETWEEN ? AND ?
          AND COMNumber LIKE ? || '%'
          AND COMNumber IS NOT NULL AND COMNumber!=''
    """, (START, END, ONLY_PREFIX))
else:
    cur.execute("""
        SELECT DISTINCT COMNumber FROM SCHLabor
        WHERE LoggedDate BETWEEN ? AND ?
          AND COMNumber IS NOT NULL AND COMNumber!=''
    """, (START, END))
coms=[r[0] for r in cur.fetchall()]
print('TOTAL_COMS_IN_WINDOW', len(coms))
# 2. Hours for Dept 0380 only
hours={}
if coms:
    cur.execute(f"""
        SELECT COMNumber, SUM(ActualHours) FROM SCHLabor
        WHERE LoggedDate BETWEEN ? AND ?
          AND TRIM(DepartmentNumber)='0380'
          AND COMNumber IN ({','.join(['?']*len(coms))})
        GROUP BY COMNumber
    """, [START, END, *coms])
    for c,h in cur.fetchall():
        hours[c]=h or 0.0
# 3. Scheduling summary pull
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='SCHSchedulingSummary'")
actual=std=None
sched_rows=[]
if cur.fetchone():
    cur.execute('PRAGMA table_info(SCHSchedulingSummary)')
    scols=[r[1] for r in cur.fetchall()]
    def find(*cands):
        lower=[c.lower() for c in scols]
        for cand in cands:
            if cand.lower() in lower:
                return scols[lower.index(cand.lower())]
        return None
    actual=find('ActualHours','Actual_Hours','HoursActual')
    std=find('StdHours','StandardHours','Std_Hours','StdHours1')
    if coms:
        cur.execute(f"SELECT comnumber1{', ' + actual if actual else ''}{', ' + std if std else ''} FROM SCHSchedulingSummary WHERE comnumber1 IN ({','.join(['?']*len(coms))})", coms)
        sched_rows=cur.fetchall()
# map
sched_map={}
for row in sched_rows:
    sched_map[row[0]]=row[1:]
# 4. Export
out='export_COMs_Dept0380_sched.csv'
with open(out,'w',newline='',encoding='utf-8') as f:
    w=csv.writer(f)
    w.writerow(['COMNumber','HoursDept0380','SchedActual','SchedStd'])
    for com in sorted(coms):
        a=s=None
        if com in sched_map:
            if len(sched_map[com])>0: a=sched_map[com][0]
            if len(sched_map[com])>1: s=sched_map[com][1]
        w.writerow([com, hours.get(com,0.0), a, s])
print('EXPORTED', out)
conn.close()
