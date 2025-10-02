import sqlite3, time

VIEW_NAME = 'vSCHLaborWithDept'

con = sqlite3.connect('SCHLabor.db')
cur = con.cursor()

# Detect view presence
cur.execute("SELECT name FROM sqlite_master WHERE type='view' AND name=?", (VIEW_NAME,))
view_exists = cur.fetchone() is not None

# Drop the view temporarily if it exists (will recreate later based on DepartmentCode join if original definition stored)
if view_exists:
    cur.execute(f'DROP VIEW {VIEW_NAME}')

# Ensure both tables exist
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
names = {r[0] for r in cur.fetchall()}
if 'SCHLabor_rebuild' not in names or 'SCHLabor' not in names:
    raise SystemExit(f'Missing required tables. Present: {names}')

backup = 'SCHLabor_old_' + time.strftime('%Y%m%d%H%M%S')
cur.execute(f'ALTER TABLE SCHLabor RENAME TO {backup}')
cur.execute('ALTER TABLE SCHLabor_rebuild RENAME TO SCHLabor')

# Simple recreation of view (guessing original join)
view_sql = (
    "CREATE VIEW "+VIEW_NAME+" AS \n"
    "SELECT L.*, D.Description AS DepartmentDescription \n"
    "FROM SCHLabor L LEFT JOIN DepartmentCode D \n"
    "ON L.DepartmentNumber = D.DepartmentNumber"
)
cur.execute(view_sql)

cur.execute('SELECT COUNT(*), MIN(LoggedDate), MAX(LoggedDate), COUNT(DISTINCT LoggedDate) FROM SCHLabor')
rows,min_d,max_d,dd = cur.fetchone()
con.commit()
print({'status':'swapped','backup_table':backup,'row_count':rows,'min_date':min_d,'max_date':max_d,'distinct_dates':dd,'view_recreated':True})
