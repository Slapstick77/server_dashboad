"""Normalize SCHLabor.LoggedDate values to ISO YYYY-MM-DD.
Preview by default (no changes). Set APPLY=True below to perform updates.
"""
import sqlite3, datetime, sys
APPLY = False  # change to True to apply updates
DB='SCHLabor.db'
conn=sqlite3.connect(DB)
cur=conn.cursor()
cur.execute("SELECT COUNT(*) FROM SCHLabor")
_total = cur.fetchone()[0]
cur.execute("SELECT COUNT(* ) FROM SCHLabor WHERE LoggedDate LIKE '%/%'")
slash = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM SCHLabor WHERE LoggedDate LIKE '____-__-__'")
iso = cur.fetchone()[0]
print(f'Total rows: {_total}\nSlash format rows: {slash}\nISO format rows: {iso}')
cur.execute("SELECT DISTINCT LoggedDate FROM SCHLabor WHERE LoggedDate LIKE '%/%' LIMIT 20")
print('Sample slash dates:', [r[0] for r in cur.fetchall()])
if not APPLY:
    print('APPLY is False -> no changes made. Set APPLY=True in script to normalize.')
    sys.exit(0)
cur.execute("SELECT DISTINCT LoggedDate FROM SCHLabor WHERE LoggedDate LIKE '%/%'")
vals=[r[0] for r in cur.fetchall()]
updated=0
for v in vals:
    try:
        dt = datetime.datetime.strptime(v,'%m/%d/%Y')
    except Exception:
        try:
            dt = datetime.datetime.strptime(v,'%m/%d/%y')
        except Exception:
            print('Skip unparsable', v)
            continue
    iso = dt.strftime('%Y-%m-%d')
    cur.execute('UPDATE SCHLabor SET LoggedDate=? WHERE LoggedDate=?', (iso, v))
    updated += cur.rowcount
conn.commit()
print('Rows updated:', updated)
conn.close()
