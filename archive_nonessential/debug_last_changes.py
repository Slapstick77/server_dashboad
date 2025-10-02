import os, sqlite3, sys
DB = os.path.join(os.path.dirname(__file__), 'SCHLabor.db')
com = int(sys.argv[1]) if len(sys.argv) > 1 else 19720
conn = sqlite3.connect(DB)
cur = conn.cursor()
cur.execute("SELECT id, run_started, run_completed, message FROM RunLog WHERE run_type='SchedulingSummary' ORDER BY id DESC LIMIT 1")
row = cur.fetchone()
if not row:
    print('No SchedulingSummary run found'); sys.exit(0)
run_id = row[0]
print(f'Last run: id={run_id} started={row[1]} completed={row[2]} msg={row[3]}')
cur.execute("SELECT comnumber1, column_name, old_value, new_value FROM ChangeLog WHERE run_id=? AND comnumber1=? ORDER BY id", (run_id, com))
changes = cur.fetchall()
print(f'Changes for COM {com}: {len(changes)}')
for (c, col, oldv, newv) in changes:
    print(f'{col}: {oldv} -> {newv}')
