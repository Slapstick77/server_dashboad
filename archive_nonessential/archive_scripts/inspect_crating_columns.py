import sqlite3, os
DB='SCHLabor.db'
if not os.path.isfile(DB):
    raise SystemExit('DB missing')
conn=sqlite3.connect(DB)
cur=conn.cursor()
cur.execute('PRAGMA table_info(SCHSchedulingSummary)')
cols=[r[1] for r in cur.fetchall()]
print('Columns (count=',len(cols),')')
print(cols)
# detect any column containing 'crat'
crat_cols=[c for c in cols if 'crat' in c.lower()]
print('\nColumns containing crat:', crat_cols)
# sample non-null counts
for c in crat_cols:
    cur.execute(f"SELECT COUNT(1) FROM SCHSchedulingSummary WHERE {c} IS NOT NULL AND {c}!='' AND {c}!=0")
    print(c, 'non-null/non-zero rows:', cur.fetchone()[0])
# show top 5 rows with any crating value
if crat_cols:
    conditions=' OR '.join([f"({c} IS NOT NULL AND {c}!=0)" for c in crat_cols])
    cur.execute(f"SELECT comnumber1, {', '.join(crat_cols)} FROM SCHSchedulingSummary WHERE {conditions} LIMIT 10")
    print('\nSample rows with crating values:')
    for row in cur.fetchall():
        print(row)
conn.close()
