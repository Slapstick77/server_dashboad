import sqlite3, csv, os

DB_PATH = 'SCHLabor.db'
CSV_PATH = 'cleaned_file.csv'
TABLE = 'SCHSchedulingSummary'

schema = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  id INTEGER PRIMARY KEY,
  comnumber1 INTEGER,
  jobname TEXT,
  contractnumber TEXT,
  emb_new INTEGER,
  flow_new INTEGER,
  med_new INTEGER,
  ol_new INTEGER,
  detailingstdhrs REAL,
  progstdhrs REAL,
  fabstdhrs REAL,
  fabacthrs REAL,
  weldingstdhrs REAL,
  weldingacthrs REAL,
  baseformpaintstdhrs REAL,
  baseformpaintacthrs REAL,
  fanassyteststdhrs REAL,
  fanassytestacthrs REAL,
  insulwallfabstdhrs REAL,
  insulwallfabacthrs REAL,
  assystdhrs REAL,
  assyacthrs REAL,
  doorfabstdhrs REAL,
  doorfabacthrs REAL,
  electricalstdhrs REAL,
  electricalacthrs REAL,
  pipestdhrs REAL,
  pipeacthrs REAL,
  paintstdhrs REAL,
  paintacthrs REAL,
  cratingstdhrs REAL,
  cratingacthrs REAL,
  mmp INTEGER,
  sppp INTEGER,
  lau INTEGER,
  vfd INTEGER,
  alum INTEGER,
  airflow INTEGER,
  leaktest INTEGER,
  deflection INTEGER,
  indoor INTEGER,
  outdoor INTEGER,
  code INTEGER,
  height REAL,
  sqft REAL,
  flowline INTEGER,
  shipmonth INTEGER
);
CREATE INDEX IF NOT EXISTS IX_{TABLE}_COM ON {TABLE}(comnumber1);
"""

if not os.path.isfile(DB_PATH):
    raise SystemExit(f'Database not found: {DB_PATH}')
if not os.path.isfile(CSV_PATH):
    raise SystemExit(f'CSV not found: {CSV_PATH}')

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.executescript(schema)

with open(CSV_PATH, newline='', encoding='utf-8') as f:
    rdr = csv.reader(f)
    header = next(rdr)
    cols = ','.join(header)
    placeholders = ','.join(['?']*len(header))
    insert = f'INSERT INTO {TABLE} ({cols}) VALUES ({placeholders})'
    rows = list(rdr)
    cur.executemany(insert, rows)

conn.commit()
print(f'Inserted {cur.rowcount if cur.rowcount != -1 else len(rows)} rows into {TABLE}.')
# Show count
count = cur.execute(f'SELECT COUNT(*) FROM {TABLE}').fetchone()[0]
print('Total rows now in table:', count)
conn.close()
