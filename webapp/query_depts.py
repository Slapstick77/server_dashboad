import sqlite3, os
DB=os.path.abspath(os.path.join(os.path.dirname(__file__),'..','SCHLabor.db'))
conn=sqlite3.connect(DB)
cur=conn.cursor()
cur.execute('PRAGMA table_info(SCHLabor)')
cols=[r[1] for r in cur.fetchall()]
print('COLUMNS:', cols)
print()
# dept like
cands=[c for c in cols if 'dept' in c.lower() or 'department' in c.lower()]
print('Dept-like candidates:', cands)
for c in cands:
    try:
        cur.execute(f"SELECT {c}, COUNT(*) c FROM SCHLabor WHERE COALESCE(ActualHours,0)>0 GROUP BY 1 ORDER BY c DESC LIMIT 12")
        rows=cur.fetchall()
        print('\nSample', c, rows[:12])
    except Exception as e:
        print('ERR', c, e)
