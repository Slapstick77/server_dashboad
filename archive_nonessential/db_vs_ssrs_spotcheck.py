import os
import re
import csv
from typing import Dict, List, Optional, Tuple
import sqlite3
import pandas as pd

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, 'SCHLabor.db')
RAW_CSV = os.path.join(BASE_DIR, 'download_archive', 'SCHSchedulingSummaryReport_2025-07-17_2025-11-14.csv')


def sniff_delimiter(path: str) -> str:
    with open(path, 'r', newline='', encoding='utf-8', errors='ignore') as f:
        sample = f.read(4096)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
        return dialect.delimiter
    except Exception:
        return ','


def normalize_name(s: str) -> str:
    return re.sub(r'[^a-z0-9]', '', s.lower())


def find_column(cols: List[str], candidates: List[str]) -> Optional[str]:
    lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    # fuzzy contains
    for c in cols:
        lc = normalize_name(c)
        for cand in candidates:
            if normalize_name(cand) in lc:
                return c
    return None


def find_hours_column(cols: List[str], dept: str) -> Optional[str]:
    # Prefer columns indicating Actual hours for given dept, avoid standard/std
    target_map = {
        'fab': ['fab', 'fabrication'],
        'weld': ['weld', 'welding'],
        'paint': ['paint'],
        'electrical': ['electrical', 'elec'],
        'doorfab': ['doorfab', 'door fab'],
        'pipe': ['pipe', 'piping'],
        'crating': ['crating', 'crate'],
        'baseformpaint': ['baseformpaint', 'base form', 'base/foam', 'baseform'],
        'fanassytest': ['fanassytest', 'fan assy', 'fan test', 'fanassy'],
        'insulwallfab': ['insulwallfab', 'insul wall', 'insulwall']
    }
    target_tokens = target_map[dept]
    norm_cols = [(c, normalize_name(c)) for c in cols]
    picks: List[Tuple[str, str]] = []
    for orig, lc in norm_cols:
        if any(tok in lc for tok in target_tokens):
            if ('actual' in lc or 'acthrs' in lc or ('act' in lc and 'hrs' in lc)) and not ('std' in lc or 'standard' in lc):
                picks.append((orig, lc))
    if not picks:
        # as fallback, allow contains 'act' only
        for orig, lc in norm_cols:
            if any(tok in lc for tok in target_tokens) and 'act' in lc and not ('std' in lc or 'standard' in lc):
                picks.append((orig, lc))
    # choose the shortest normalized name (most specific)
    if picks:
        picks.sort(key=lambda x: len(x[1]))
        return picks[0][0]
    return None


def independent_parse(description: str) -> Tuple[Optional[float], Optional[float]]:
    """Parse height and sqft independently from description using HxW patterns.
    Uses the first two dimensions in an 'x' separated pattern.
    """
    if not isinstance(description, str):
        description = ''
    desc = description.strip()
    m = re.search(r'(\d+(?:\.\d+)?)\s*[xX]\s*(\d+(?:\.\d+)?)(?:\s*[xX]\s*(\d+(?:\.\d+)?))?', desc)
    if not m:
        return None, None
    try:
        h = float(m.group(1))
        w = float(m.group(2))
        sqft = h * w
        return h, sqft
    except Exception:
        return None, None


def canonical_num(v) -> str:
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ''
        f = float(v)
        if abs(f - int(f)) < 1e-9:
            return str(int(f))
        return ('{0:.4f}'.format(f)).rstrip('0').rstrip('.')
    except Exception:
        s = str(v).strip()
        return '' if s.lower() in ('', 'nan', 'none') else s


def load_db_subset(cols: List[str]) -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(SCHSchedulingSummary)")
        db_cols = [r[1] for r in cur.fetchall()]
        select_cols = [c for c in cols if c in db_cols]
        if 'comnumber1' not in select_cols:
            select_cols = ['comnumber1'] + select_cols
        col_list = ','.join([f'"{c}"' for c in select_cols])
        df = pd.read_sql_query(f'SELECT {col_list} FROM SCHSchedulingSummary', conn)
        return df


def main() -> int:
    if not os.path.isfile(DB_PATH):
        print(f'ERROR: DB not found: {DB_PATH}')
        return 2
    if not os.path.isfile(RAW_CSV):
        print(f'ERROR: Raw CSV not found: {RAW_CSV}')
        return 3

    delim = sniff_delimiter(RAW_CSV)
    df_raw = pd.read_csv(RAW_CSV, sep=delim, dtype=str, low_memory=False, encoding_errors='ignore')

    # Identify columns in raw
    com_col = find_column(df_raw.columns.tolist(), ['comnumber1', 'com number', 'comnumber', 'com'])
    desc_col = find_column(df_raw.columns.tolist(), ['description', 'item description', 'line description', 'product description'])
    if not com_col:
        print('ERROR: COM column not found in raw file.')
        print('Columns:', list(df_raw.columns))
        return 4
    if not desc_col:
        print('WARN: Description column not found; height/sqft checks will be skipped.')

    # Candidate actual-hours columns in raw
    fab_raw = find_hours_column(df_raw.columns.tolist(), 'fab')
    weld_raw = find_hours_column(df_raw.columns.tolist(), 'weld')

    # Load DB subset
    desired_db_cols = ['height', 'sqft', 'fabacthrs', 'weldingacthrs', 'paintacthrs', 'electricalacthrs', 'doorfabacthrs', 'pipeacthrs', 'cratingacthrs', 'baseformpaintacthrs', 'fanassytestacthrs', 'insulwallfabacthrs']
    df_db = load_db_subset(desired_db_cols)

    # Normalize keys
    df_raw['__com__'] = df_raw[com_col].astype(str).str.strip()
    df_db['__com__'] = df_db['comnumber1'].astype(str).str.strip()

    # Build sample set: prefer 19720, 19627 plus first 3 shared
    shared = sorted(set(df_raw['__com__']) & set(df_db['__com__']))
    preferred = [k for k in ['19720', '19627'] if k in shared]
    samples = preferred + [k for k in shared if k not in preferred][:3]
    if not samples:
        print('No shared COMs between DB and SSRS CSV.')
        return 0

    raw_idx = df_raw.set_index('__com__')
    db_idx = df_db.set_index('__com__')

    print('Checking COMs:', ', '.join(samples))
    mismatches = 0
    for com in samples:
        rrow = raw_idx.loc[com]
        drow = db_idx.loc[com]
        if isinstance(rrow, pd.DataFrame):
            rrow = rrow.iloc[0]
        if isinstance(drow, pd.DataFrame):
            drow = drow.iloc[0]

        print(f'COM {com}:')
        # Height/Sqft via independent parse
        if desc_col:
            desc = str(rrow.get(desc_col, ''))
            ih, isq = independent_parse(desc)
            dh = drow.get('height') if 'height' in drow else None
            dsq = drow.get('sqft') if 'sqft' in drow else None
            ch = canonical_num(ih) == canonical_num(dh)
            cs = canonical_num(isq) == canonical_num(dsq)
            status = 'OK' if ch and cs else 'DIFF'
            if not (ch and cs):
                mismatches += 1
            print(f"  Height/Sqft [{status}] desc='{desc[:100]}'")
            print(f"    Raw-> height={canonical_num(ih)} sqft={canonical_num(isq)}  |  DB-> height={canonical_num(dh)} sqft={canonical_num(dsq)}")
        else:
            print('  Skipped Height/Sqft (no description column).')

        # Actual-hours comparisons (exclude Assembly/Flow)
        dept_specs = [
            ('fab', 'fabacthrs', 'Fab Actual'),
            ('weld', 'weldingacthrs', 'Welding Actual'),
            ('paint', 'paintacthrs', 'Paint Actual'),
            ('electrical', 'electricalacthrs', 'Electrical Actual'),
            ('doorfab', 'doorfabacthrs', 'DoorFab Actual'),
            ('pipe', 'pipeacthrs', 'Pipe Actual'),
            ('crating', 'cratingacthrs', 'Crating Actual'),
            ('baseformpaint', 'baseformpaintacthrs', 'BaseFormPaint Actual'),
            ('fanassytest', 'fanassytestacthrs', 'FanAssyTest Actual'),
            ('insulwallfab', 'insulwallfabacthrs', 'InsulWallFab Actual'),
        ]
        # Pre-resolve raw columns for depts
        raw_map: Dict[str, Optional[str]] = {}
        for dept, _, _ in dept_specs:
            raw_map[dept] = find_hours_column(list(rrow.index), dept)
        for dept, db_col, label in dept_specs:
            raw_col = raw_map.get(dept)
            if raw_col and db_col in drow:
                rv = rrow.get(raw_col)
                dv = drow.get(db_col)
                ok = canonical_num(rv) == canonical_num(dv)
                if not ok:
                    mismatches += 1
                print(f"  {label} [{'OK' if ok else 'DIFF'}] Raw={canonical_num(rv)} DB={canonical_num(dv)} (raw col '{raw_col}')")
            else:
                print(f"  {label} skipped (column not found).")

    print(f'Completed. Total mismatches across checks: {mismatches} for {len(samples)} COMs.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
