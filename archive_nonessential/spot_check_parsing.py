import os
import re
import csv
from typing import Optional, Tuple, List
import pandas as pd

BASE_DIR = os.path.dirname(__file__)
RAW_CSV = os.path.join(BASE_DIR, 'download_archive', 'SCHSchedulingSummaryReport_2025-07-17_2025-11-14.csv')
CLEANED_CSV = os.path.join(BASE_DIR, 'download_archive', 'cleaned_file.csv')


def sniff_delimiter(path: str) -> str:
    with open(path, 'r', newline='', encoding='utf-8', errors='ignore') as f:
        sample = f.read(4096)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
        return dialect.delimiter
    except Exception:
        return ','


def find_column(cols: List[str], candidates: List[str]) -> Optional[str]:
    lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    # fuzzy contains
    for c in cols:
        lc = c.lower().replace(' ', '')
        for cand in candidates:
            if cand.lower().replace(' ', '') in lc:
                return c
    return None


def independent_parse(description: str) -> Tuple[int, int, str, Optional[float], Optional[float]]:
    """
    Return (indoor, outdoor, code, height, sqft) using lightweight heuristics:
    - indoor/outdoor detected by keywords: 'indoor', 'outdoor', '(id)', '(od)'
    - code captured as first token like letters+digits with optional dash (e.g., F-16, AF12)
    - height parsed from size tokens like '6x5x16' or '6 x 5', uses first two numbers; sqft = h*w
    """
    if not isinstance(description, str):
        description = ''
    desc = description.strip()
    dlow = desc.lower()
    indoor = 1 if ('indoor' in dlow or '(id)' in dlow or ' id ' in f' {dlow} ') else 0
    outdoor = 1 if ('outdoor' in dlow or '(od)' in dlow or ' od ' in f' {dlow} ') else 0

    # code: letters/dash+digits (not too greedy)
    code_match = re.search(r'\b([A-Z]{1,4}-?\d{1,3})\b', desc)
    code = code_match.group(1) if code_match else ''

    # sizes: collect numeric tokens around x
    size_match = re.search(r'(\d+(?:\.\d+)?)\s*[xX]\s*(\d+(?:\.\d+)?)(?:\s*[xX]\s*(\d+(?:\.\d+)?))?', desc)
    height = None
    sqft = None
    if size_match:
        try:
            h = float(size_match.group(1))
            w = float(size_match.group(2))
            height = h
            sqft = h * w
        except Exception:
            pass
    return indoor, outdoor, code, height, sqft


def canonical_num(v):
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ''
        f = float(v)
        if abs(f - int(f)) < 1e-9:
            return str(int(f))
        return ('{0:.4f}'.format(f)).rstrip('0').rstrip('.')
    except Exception:
        return str(v).strip()


def main():
    if not os.path.isfile(RAW_CSV):
        print(f'ERROR: Raw CSV not found: {RAW_CSV}')
        return 2
    if not os.path.isfile(CLEANED_CSV):
        print(f'ERROR: Cleaned CSV not found: {CLEANED_CSV}')
        return 3

    delim = sniff_delimiter(RAW_CSV)
    df_raw = pd.read_csv(RAW_CSV, sep=delim, dtype=str, low_memory=False, encoding_errors='ignore')
    df_clean = pd.read_csv(CLEANED_CSV, dtype=str, low_memory=False)

    # Identify key and description columns in raw
    com_col = find_column(df_raw.columns.tolist(), ['comnumber1', 'com number', 'comnumber', 'com'])
    desc_col = find_column(df_raw.columns.tolist(), ['description', 'item description', 'line description', 'product description'])
    if not com_col or not desc_col:
        print('ERROR: Could not find COM or Description columns in raw file')
        print('Columns:', list(df_raw.columns))
        return 4

    # Normalize keys
    df_raw['__com__'] = df_raw[com_col].astype(str).str.strip()
    df_clean['__com__'] = df_clean['comnumber1'].astype(str).str.strip()

    # Choose sample COMs: prefer known ones plus 3 others
    preferred = ['19720', '19627']
    shared_keys = sorted(set(df_raw['__com__']) & set(df_clean['__com__']))
    samples: List[str] = [k for k in preferred if k in shared_keys]
    for k in shared_keys:
        if len(samples) >= 5:
            break
        if k not in samples:
            samples.append(k)

    if not samples:
        print('No shared COMs found between raw and cleaned files.')
        return 0

    # Set index for quick lookup
    raw_idx = df_raw.set_index('__com__')
    cln_idx = df_clean.set_index('__com__')

    print(f'Spot-checking {len(samples)} COMs: {", ".join(samples)}')
    mismatches = 0
    for com in samples:
        rrow = raw_idx.loc[com]
        crow = cln_idx.loc[com]
        # If duplicates in raw, take first occurrence
        if isinstance(rrow, pd.DataFrame):
            rrow = rrow.iloc[0]
        if isinstance(crow, pd.DataFrame):
            crow = crow.iloc[0]
        desc = str(rrow.get(desc_col, ''))
        rin, rout, rcode, rheight, rsqft = independent_parse(desc)

        cin = crow.get('indoor')
        cout = crow.get('outdoor')
        ccode = crow.get('code')
        cheight = crow.get('height')
        csqft = crow.get('sqft')

        m_in = str(rin) == str(cin)
        m_out = str(rout) == str(cout)
        m_code = (str(rcode).strip() == str(ccode).strip()) or (not rcode and not str(ccode).strip())
        m_h = canonical_num(rheight) == canonical_num(cheight)
        m_sq = canonical_num(rsqft) == canonical_num(csqft)

        ok = all([m_in, m_out, m_code, m_h, m_sq])
        status = 'OK' if ok else 'DIFF'
        if not ok:
            mismatches += 1
        print(f"COM {com} [{status}] Desc='{desc[:120]}'")
        print(f"  Raw-> indoor={rin} outdoor={rout} code='{rcode}' height={canonical_num(rheight)} sqft={canonical_num(rsqft)}")
        print(f"  Cln-> indoor={cin} outdoor={cout} code='{ccode}' height={canonical_num(cheight)} sqft={canonical_num(csqft)}")

    print(f'Completed. Mismatched rows: {mismatches} of {len(samples)} checked.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
