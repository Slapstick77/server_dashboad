import os
import sqlite3
import pandas as pd
from datetime import datetime

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, 'SCHLabor.db')
RAW_CSV = os.path.join(BASE_DIR, 'download_archive', 'SCHSchedulingSummaryReport_2025-07-17_2025-11-14.csv')
OUT_CSV = os.path.join(BASE_DIR, 'download_archive', 'cleaned_verify.csv')

def canonical(v):
    if v is None:
        return ''
    s = str(v).strip()
    if s == '' or s.lower() == 'nan':
        return ''
    # try numeric
    try:
        if any(c.isalpha() for c in s):
            return s
        f = float(s)
        if abs(f - int(f)) < 1e-9:
            return str(int(f))
        return ('{0:.6f}'.format(f)).rstrip('0').rstrip('.')
    except Exception:
        return s

def load_db_df(columns):
    # Only select columns that exist in DB
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(SCHSchedulingSummary)")
        cols = [r[1] for r in cur.fetchall()]
        have = ['comnumber1'] + [c for c in columns if c != 'comnumber1' and c in cols]
        if not have:
            raise RuntimeError('SCHSchedulingSummary missing or has no comparable columns')
        col_list = ','.join([f'"{c}"' for c in have])
        df_db = pd.read_sql_query(f'SELECT {col_list} FROM SCHSchedulingSummary', conn)
        return df_db, have

def main():
    # Clean the specified CSV to a temp verification file
    from clean import convert_file1_to_cleaned
    if not os.path.isfile(RAW_CSV):
        print(f"ERROR: Source CSV not found: {RAW_CSV}")
        return 2
    convert_file1_to_cleaned(RAW_CSV, OUT_CSV)
    df_clean = pd.read_csv(OUT_CSV, dtype=str, low_memory=False)
    if 'comnumber1' not in df_clean.columns:
        print('ERROR: cleaned file missing comnumber1')
        return 3
    # Load DB subset
    df_db, comparable_cols = load_db_df(df_clean.columns.tolist())
    # Key as string for robustness
    df_clean['comnumber1'] = df_clean['comnumber1'].astype(str).str.strip()
    df_db['comnumber1'] = df_db['comnumber1'].astype(str).str.strip()
    # Index by key
    cidx = df_clean.set_index('comnumber1')
    didx = df_db.set_index('comnumber1')
    # Coverage
    clean_keys = set(cidx.index)
    db_keys = set(didx.index)
    only_in_clean = sorted(list(clean_keys - db_keys))
    only_in_db = sorted(list(db_keys - clean_keys))
    print(f"Keys: clean={len(clean_keys)} db={len(db_keys)} only_in_clean={len(only_in_clean)} only_in_db={len(only_in_db)}")
    if only_in_clean[:10]:
        print('Sample only_in_clean:', only_in_clean[:10])
    if only_in_db[:10]:
        print('Sample only_in_db:', only_in_db[:10])
    # Compare shared keys
    shared = sorted(list(clean_keys & db_keys))
    mismatches = []
    cols_to_check = [c for c in comparable_cols if c != 'comnumber1']
    for k in shared:
        rc = cidx.loc[k]
        rd = didx.loc[k]
        # If duplicates, take first row (should not happen for DB due to unique index)
        if isinstance(rc, pd.DataFrame):
            rc = rc.iloc[0]
        if isinstance(rd, pd.DataFrame):
            rd = rd.iloc[0]
        for col in cols_to_check:
            vc = canonical(rc.get(col))
            vd = canonical(rd.get(col))
            if vc != vd:
                mismatches.append((k, col, rc.get(col), rd.get(col)))
    print(f"Shared keys: {len(shared)}  Column mismatches: {len(mismatches)}")
    if mismatches:
        print('First 30 mismatches:')
        for k, col, vc, vd in mismatches[:30]:
            print(f"  COM {k} | {col}: CSV='{vc}' DB='{vd}'")
    return 0

if __name__ == '__main__':
    code = main()
    raise SystemExit(code)
