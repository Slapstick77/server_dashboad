import sqlite3, datetime

DB='SCHLabor.db'
COL='iso_logged_date'

con=sqlite3.connect(DB)
cur=con.cursor()

# Detect if column already exists
cur.execute("PRAGMA table_info(SCHLabor)")
cols=[r[1].lower() for r in cur.fetchall()]
if COL.lower() not in cols:
    cur.execute(f"ALTER TABLE SCHLabor ADD COLUMN {COL} TEXT")
    added=True
else:
    added=False

# Backfill any NULLs
cur.execute(f"SELECT COUNT(*) FROM SCHLabor WHERE {COL} IS NULL OR {COL}='' ")
remaining=cur.fetchone()[0]
if remaining:
    # Process in chunks to avoid huge single transaction memory
    cur.execute(f"SELECT rowid, LoggedDate FROM SCHLabor WHERE {COL} IS NULL OR {COL}='' ")
    rows=cur.fetchall()
    for rowid, ld in rows:
        try:
            m,d,y = ld.split('/')
            iso = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
        except Exception:
            iso = None
        if iso:
            cur.execute(f"UPDATE SCHLabor SET {COL}=? WHERE rowid=?", (iso,rowid))
    con.commit()

# Create index if not exists
cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_schlabor_iso_logged_date'")
if not cur.fetchone():
    cur.execute(f"CREATE INDEX idx_schlabor_iso_logged_date ON SCHLabor({COL})")
    con.commit()

# Verify chronological min/max using new column
cur.execute(f"SELECT MIN({COL}), MAX({COL}), COUNT(DISTINCT {COL}) FROM SCHLabor")
min_iso,max_iso,distinct_iso = cur.fetchone()

# Compare to textual MIN/MAX for illustration
cur.execute("SELECT MIN(LoggedDate), MAX(LoggedDate) FROM SCHLabor")
text_min,text_max = cur.fetchone()

print({
    'column_added': added,
    'iso_min': min_iso,
    'iso_max': max_iso,
    'text_min': text_min,
    'text_max': text_max,
    'distinct_iso_dates': distinct_iso
})
