import sqlite3, os, json
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'SCHLabor.db'))
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute('PRAGMA table_info(SCHSchedulingSummary)')
cols = [r[1] for r in cur.fetchall()]
tracked_std = ['fabstdhrs','weldingstdhrs','baseformpaintstdhrs','fanassyteststdhrs','insulwallfabstdhrs','doorfabstdhrs','electricalstdhrs','pipestdhrs','paintstdhrs','cratingstdhrs','assystdhrs']
tracked_act = ['fabacthrs','weldingacthrs','baseformpaintacthrs','fanassytestacthrs','insulwallfabacthrs','doorfabacthrs','electricalacthrs','pipeacthrs','paintacthrs','cratingacthrs','assyacthrs']
cur.execute('SELECT COUNT(*) FROM SCHSchedulingSummary')
row_count = cur.fetchone()[0]
missing_std = [c for c in tracked_std if c not in cols]
missing_act = [c for c in tracked_act if c not in cols]
std_sum = ' + '.join([f'COALESCE({c},0)' for c in tracked_std if c in cols]) or '0'
act_sum = ' + '.join([f'COALESCE({c},0)' for c in tracked_act if c in cols]) or '0'
cur.execute(f'SELECT SUM(CASE WHEN ({std_sum})=0 THEN 1 ELSE 0 END), SUM(CASE WHEN ({act_sum})=0 THEN 1 ELSE 0 END) FROM SCHSchedulingSummary')
zero_std_rows, zero_act_rows = cur.fetchone()
cur.execute(f'SELECT COUNT(*) FROM SCHSchedulingSummary WHERE ({std_sum})>0 AND ({act_sum}) < ({std_sum})')
approx_incomplete = cur.fetchone()[0]
# How many have act = 0 but std > 0
cur.execute(f'SELECT COUNT(*) FROM SCHSchedulingSummary WHERE ({std_sum})>0 AND ({act_sum})=0')
not_started = cur.fetchone()[0]
print(json.dumps({
  'row_count': row_count,
  'zero_std_rows': zero_std_rows,
  'zero_act_rows': zero_act_rows,
  'approx_incomplete_rows': approx_incomplete,
  'not_started_rows': not_started,
  'missing_std_columns': missing_std,
  'missing_act_columns': missing_act,
  'present_std_columns': [c for c in tracked_std if c in cols],
  'present_act_columns': [c for c in tracked_act if c in cols]
}, indent=2))
