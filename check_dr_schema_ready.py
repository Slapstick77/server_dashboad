from __future__ import annotations
import sqlite3, os

ROOT = os.path.dirname(__file__)
DB_PATH = os.path.join(ROOT, 'SCHLabor.db')

need_tables = {'DRPollRun','DRPollTimingEvent','DRItemSnapshot','DRStateMeta','DRState'}

if not os.path.isfile(DB_PATH):
    print('DB missing:', DB_PATH)
    raise SystemExit(2)

import dr_schema

conn = sqlite3.connect(DB_PATH)
try:
    dr_schema.ensure_dr_tables(conn)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {r[0] for r in cur.fetchall()}
    ok = need_tables.issubset(tables)
    cur.execute("SELECT name FROM sqlite_master WHERE type='view'")
    views = [r[0] for r in cur.fetchall()]
    cur.execute('PRAGMA table_info(DRItemSnapshot)')
    snapshot_cols = [r[1] for r in cur.fetchall()]
    cur.execute('SELECT last_run_utc FROM DRStateMeta WHERE id=1')
    last_run = cur.fetchone()[0]
    print('ready:', ok)
    print('tables:', sorted(tables))
    print('views:', views)
    print('DRItemSnapshot columns count:', len(snapshot_cols))
    print('last_run_utc:', last_run)
finally:
    conn.close()
