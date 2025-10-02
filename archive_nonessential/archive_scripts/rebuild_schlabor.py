import os
import sqlite3
import csv
from glob import glob
from typing import List, Tuple

DB_PATH = 'SCHLabor.db'
NEW_TABLE = 'SCHLabor_rebuild'

SCHEMA_SQL = f"""
DROP TABLE IF EXISTS {NEW_TABLE};
CREATE TABLE {NEW_TABLE} (
    LoggedDate TEXT NOT NULL,
    COMNumber INTEGER,
    EmployeeName TEXT,
    EmployeeNumber1 INTEGER,
    DepartmentNumber TEXT,
    Area TEXT,
    ActualHours REAL,
    Reference TEXT
);
"""

def find_csv_files() -> List[str]:
    files = sorted(glob('SCHLabor_*.csv'))
    return files

def extract_rows(path: str) -> List[Tuple]:
    rows: List[Tuple] = []
    with open(path, 'r', encoding='utf-8-sig', newline='') as f:
        lines = f.readlines()
    # Find header line
    header_idx = None
    for i, line in enumerate(lines):
        if line.startswith('LoggedDate,'):
            header_idx = i
            break
    if header_idx is None:
        return rows  # skip malformed file
    data_lines = [l.strip('\n').rstrip('\r') for l in lines[header_idx:]]
    rdr = csv.reader(data_lines)
    header = next(rdr, [])
    # Expect EmployeeNumber1 or EmployeeNumber
    # Normalize: retain EmployeeNumber1 column in target schema
    try:
        emp_idx = header.index('EmployeeNumber1')
    except ValueError:
        try:
            emp_idx = header.index('EmployeeNumber')
        except ValueError:
            emp_idx = None
    col_map = {name: idx for idx, name in enumerate(header)}
    required = ['LoggedDate','COMNumber','EmployeeName','DepartmentNumber','Area','ActualHours','Reference']
    for r in rdr:
        if not r or len(r) < 3:
            continue
        def gv(col):
            idx = col_map.get(col)
            return r[idx].strip() if idx is not None and idx < len(r) else ''
        logged = gv('LoggedDate')
        # Keep date as-is (raw slash format) assuming rebuild wants original format
        com = gv('COMNumber') or None
        name = gv('EmployeeName') or None
        emp = r[emp_idx].strip() if emp_idx is not None and emp_idx < len(r) else None
        emp = emp or None
        dept = gv('DepartmentNumber') or None
        area = gv('Area') or None
        hours_txt = gv('ActualHours')
        try:
            hours = float(hours_txt) if hours_txt else None
        except ValueError:
            hours = None
        ref = gv('Reference') or None
        rows.append((logged, com, name, emp, dept, area, hours, ref))
    return rows

def rebuild():
    files = find_csv_files()
    if not files:
        print('No SCHLabor_*.csv files found.')
        return
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    print('Creating new table schema (fresh start)...')
    for stmt in SCHEMA_SQL.strip().split(';'):
        s = stmt.strip()
        if s:
            cur.execute(s)
    # Speed PRAGMAs (safe because this is a disposable build step; main DB still backed up externally if needed)
    cur.execute("PRAGMA synchronous=OFF")
    cur.execute("PRAGMA journal_mode=MEMORY")
    cur.execute("PRAGMA temp_store=MEMORY")
    cur.execute("PRAGMA locking_mode=EXCLUSIVE")
    con.commit()

    total_rows = 0
    batch_size_files = 25
    pending_files = 0
    cur.execute('BEGIN')
    for idx, path in enumerate(files, 1):
        rows = extract_rows(path)
        if rows:
            cur.executemany(
                f"INSERT INTO {NEW_TABLE} (LoggedDate, COMNumber, EmployeeName, EmployeeNumber1, DepartmentNumber, Area, ActualHours, Reference) VALUES (?,?,?,?,?,?,?,?)",
                rows
            )
            total_rows += len(rows)
        pending_files += 1
        if pending_files >= batch_size_files:
            con.commit()
            pending_files = 0
            print(f'[{idx}/{len(files)}] {os.path.basename(path)} -> cumulative rows: {total_rows}')
            cur.execute('BEGIN')
    # Final commit
    con.commit()
    # Summary (no dedupe done; trusting data set has no duplicates)
    cur.execute(f'SELECT COUNT(*) FROM {NEW_TABLE}')
    total = cur.fetchone()[0]
    # Quick structural duplicate check on key subset (can skip full-row heavy check for speed)
    cur.execute(f"""
        SELECT COUNT(*) FROM (
          SELECT LoggedDate, IFNULL(EmployeeNumber1,''), IFNULL(COMNumber,''), IFNULL(DepartmentNumber,''), IFNULL(Reference,''), COUNT(*) c
          FROM {NEW_TABLE}
          GROUP BY 1,2,3,4 HAVING c>1
        )
    """)
    key_dup_groups = cur.fetchone()[0]
    cur.execute(f'SELECT MIN(LoggedDate), MAX(LoggedDate) FROM {NEW_TABLE}')
    min_d, max_d = cur.fetchone()
    con.close()
    print('Rebuild complete (no dedupe logic applied).')
    print(f'Rows loaded: {total}')
    print(f'Key duplicate groups (LoggedDate+EmpNum+COM+Dept+Ref): {key_dup_groups}')
    print(f'Date range (raw/raw): {min_d} -> {max_d}')
    print('\nNext step to swap tables (manual):')
    print('  1. Backup current SCHLabor.db file.')
    print(f"  2. In sqlite: ALTER TABLE SCHLabor RENAME TO SCHLabor_old; ALTER TABLE {NEW_TABLE} RENAME TO SCHLabor;")
    print('  3. Optionally create a unique index after swap if you want ongoing protection.')

if __name__ == '__main__':
    rebuild()
