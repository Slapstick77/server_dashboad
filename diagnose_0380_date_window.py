import sqlite3, os, sys
DB='SCHLabor.db'
START='2024-08-31'
END='2025-08-31'
if not os.path.isfile(DB):
    print('DB missing'); sys.exit(1)
conn=sqlite3.connect(DB)
cur=conn.cursor()
cur.execute('PRAGMA table_info(SCHLabor)')
cols=[r[1] for r in cur.fetchall()]
print('Columns:', cols)
iso_col='iso_logged_date' if 'iso_logged_date' in cols else None
# Max / min dates
for col in [c for c in ['iso_logged_date','LoggedDate'] if c in cols]:
    cur.execute(f"SELECT MIN({col}), MAX({col}) FROM SCHLabor")
    print(f"Date range in {col}:", cur.fetchone())
# Distinct COM counts by department 0380 for each date column
for col in [c for c in ['iso_logged_date','LoggedDate'] if c in cols]:
    cur.execute(f"SELECT COUNT(DISTINCT COMNumber) FROM SCHLabor WHERE {col} BETWEEN ? AND ? AND TRIM(DepartmentNumber)='0380'", (START,END))
    print(f"Dept 0380 distinct COMs using {col}:", cur.fetchone()[0])
# Total distinct COMs any department in window
for col in [c for c in ['iso_logged_date','LoggedDate'] if c in cols]:
    cur.execute(f"SELECT COUNT(DISTINCT COMNumber) FROM SCHLabor WHERE {col} BETWEEN ? AND ?", (START,END))
    print(f"All departments distinct COMs using {col}:", cur.fetchone()[0])
# Sample latest 5 rows
for col in [c for c in ['iso_logged_date','LoggedDate'] if c in cols]:
    cur.execute(f"SELECT {col}, COMNumber, DepartmentNumber, ActualHours FROM SCHLabor ORDER BY {col} DESC LIMIT 5")
    print(f"Latest 5 rows by {col}:")
    for row in cur.fetchall():
        print(row)
conn.close()
