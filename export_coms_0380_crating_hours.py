import sqlite3, csv, os, sys, math
DB='SCHLabor.db'
START='2024-08-31'
END='2025-08-31'
OUT='export_COMs_Dept0380_CratingHours.csv'
if not os.path.isfile(DB):
    print('Missing DB'); sys.exit(1)
conn=sqlite3.connect(DB)
cur=conn.cursor()
# Decide which date column to use. Prefer iso_logged_date if present.
cur.execute("PRAGMA table_info(SCHLabor)")
cols=[r[1] for r in cur.fetchall()]
date_col='iso_logged_date' if 'iso_logged_date' in cols else 'LoggedDate'
print('Using date column:', date_col)
date_filter=f"{date_col} BETWEEN ? AND ?"

# 1. COMs with at least one 0380 charge in window (collect plus aggregate hours & first/last dates)
cur.execute(f"""
        SELECT COMNumber,
                     SUM(ActualHours) as total_hours,
                     MIN({date_col}) as first_date,
                     MAX({date_col}) as last_date
        FROM SCHLabor
        WHERE {date_filter}
            AND TRIM(DepartmentNumber)='0380'
            AND COMNumber IS NOT NULL AND COMNumber!=''
        GROUP BY COMNumber
""", (START, END))
rows=cur.fetchall()
raw_coms=[r[0] for r in rows]
coms=[str(c) for c in raw_coms]
print('Distinct COMs with Dept 0380 charges in window:', len(coms))
if len(coms) < 100:
        print('WARNING: COM count (<100) lower than expected 500-600; verify filtering criteria or source data.')
# 2. Discover scheduling summary columns (explicit requirement: cratingstdhrs, cratingacthrs)
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='SCHSchedulingSummary'")
have_sched=cur.fetchone() is not None
crate_std_col='cratingstdhrs'
crate_act_col='cratingacthrs'
if have_sched and coms:
    cur.execute('PRAGMA table_info(SCHSchedulingSummary)')
    cols=[r[1].lower() for r in cur.fetchall()]
    if crate_std_col not in cols:
        print('WARNING: cratingstdhrs column not found; values will be blank')
        crate_std_col=None
    if crate_act_col not in cols:
        print('WARNING: cratingacthrs column not found; values will be blank')
        crate_act_col=None
print('Using crating columns:', crate_std_col, crate_act_col)
# 3. Pull scheduling data
sched_map={}
if have_sched and coms and (crate_std_col or crate_act_col):
    select_cols=['comnumber1']
    if crate_std_col: select_cols.append(crate_std_col)
    if crate_act_col: select_cols.append(crate_act_col)
    placeholders=','.join(['?']*len(coms))
    cur.execute(f"SELECT {', '.join(select_cols)} FROM SCHSchedulingSummary WHERE comnumber1 IN ({placeholders})", coms)
    for row in cur.fetchall():
        com=str(row[0])
        std_val=None
        act_val=None
        # ordering based on which cols included
        idx=1
        if crate_std_col:
            std_val=row[idx]; idx+=1
        if crate_act_col:
            act_val=row[idx] if crate_act_col else None
        sched_map[com]=(std_val, act_val)
# 4. Export
with open(OUT,'w',newline='',encoding='utf-8') as f:
    w=csv.writer(f)
    # Only required columns
    w.writerow(['COMNumber','cratingstdhrs','cratingacthrs'])
    for com in sorted(coms, key=lambda x: (len(x), x)):
        std_val, act_val = sched_map.get(com,(0,0))
        w.writerow([com, std_val if std_val is not None else 0, act_val if act_val is not None else 0])
print('Exported', OUT)
conn.close()
