"""Incremental DR poller (read-only) for periodic (e.g. hourly) execution.

Features:
    * Pull DRs within a rolling time window (default 7 days) using DisplayItemForDateRange.
    * Optionally include closed DRs (when requested, only closed variants are invoked which return all).
    * For each DR gather routing history; derive latest routing step AND latest non-empty comment.
    * Maintain a local state file of last seen routing timestamp, routing step count, and last non-empty comment.
    * Flag three change indicators: Updated (any), UpdatedRouting, UpdatedComment.
    * Output summary CSV & JSON (full plus optionally only-updated) with change flags.
    * Purely read-only: only safe operations invoked.

Usage example (hourly Task Scheduler job):
    python poll_drs_incremental.py --user 01962 --password 01962 --window-days 7 \
            --state-file dr_state.json --out-csv dr_incremental.csv --out-json dr_incremental.json \
            --include-closed --include-history --verbose

Scheduling (Windows Task Scheduler) suggestion:
    Program: powershell
    Arguments: -NoProfile -ExecutionPolicy Bypass -Command "python 'c:/Project p/MOM/poll_drs_incremental.py' --user XXXXX --password XXXXX --window-days 7 --include-closed --state-file 'c:/Project p/MOM/dr_state.json' --out-csv 'c:/Project p/MOM/dr_incremental.csv' --out-json 'c:/Project p/MOM/dr_incremental.json'"
"""
from __future__ import annotations
import os
import argparse
import json
import csv
import datetime as dt
import random
from typing import Any, Dict, List, Optional
import time

try:
    from zeep import Client  # type: ignore
    from zeep.transports import Transport  # type: ignore
    from zeep.cache import InMemoryCache  # type: ignore
except ImportError:
    raise SystemExit("Please install zeep: pip install zeep")

DEFAULT_BASE_URL = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc'
UNSAFE_PREFIXES = ('Save','Delete','Approve','Void','Import','MonthEnd','Lock','Unlock','Upload')

# ---------------- Utility / safety -----------------

def safe_ops(client: Client) -> List[str]:
    ops = set()
    for svc in client.wsdl.services.values():
        for port in svc.ports.values():
            binding = getattr(port, 'binding', None)
            if binding:
                ops.update(binding._operations.keys())  # type: ignore
    return sorted(ops)


def login(client: Client, user: str, pwd: str, app_name: str):
    ops = safe_ops(client)
    if 'RegisterSession' in ops:
        try: client.service.RegisterSession(username=user, password=pwd)
        except Exception: pass
    if 'GetApplicationPK' in ops:
        try: client.service.GetApplicationPK(applicationKey=app_name, username=user, password=pwd)
        except Exception: pass
    if 'MOM_Login' in ops:
        try: client.service.MOM_Login(username=user, password=pwd)
        except Exception: pass
    dev_user = None
    if 'GetUser' in ops:
        try: dev_user = client.service.GetUser(username=user, password=pwd)
        except Exception: pass
    return dev_user

# --------------- Routing helpers -------------------

def extract_routing_list(routing_list: Any) -> List[Dict[str,Any]]:
    out: List[Dict[str,Any]] = []
    if not routing_list:
        return out
    try:
        iterable = list(routing_list)
    except Exception:
        iterable = []
    for step in iterable:
        vals = getattr(step,'__values__',{})
        if not isinstance(vals, dict):
            continue
        row: Dict[str,Any] = {}
        for k,v in vals.items():
            import datetime as _dt
            if isinstance(v,(str,int,float,bool)):
                row[k]=v
            elif isinstance(v,_dt.datetime):
                row[k]=v.isoformat()
        out.append(row)
    # Sort by DateTouched if present
    def parse_ts(s: Any):
        from datetime import datetime
        if not isinstance(s,str): return None
        for fmt in ('%Y-%m-%dT%H:%M:%S.%f','%Y-%m-%dT%H:%M:%S'):
            try: return datetime.strptime(s.split('+')[0], fmt)
            except Exception: continue
        return None
    out.sort(key=lambda r: parse_ts(r.get('DateTouched')) or dt.datetime.min)
    return out


def latest_step(routing_steps: List[Dict[str,Any]]) -> Optional[Dict[str,Any]]:
    if not routing_steps: return None
    return routing_steps[-1]


def latest_non_empty_comment_step(routing_steps: List[Dict[str,Any]]) -> Optional[Dict[str,Any]]:
    for step in reversed(routing_steps):
        comment = step.get('UserComments')
        if comment and isinstance(comment, str) and comment.strip():
            return step
    return None

# --------------- Notes helper -----------------------

def count_notes(client: Client, dev_user: Any, master_obj: Any) -> int:
    try:
        notes = client.service.NotesForMaster(devUser=dev_user, master=master_obj)
        return len(list(notes)) if notes else 0
    except Exception:
        return -1  # sentinel for failure

# --------------- State persistence ------------------

def load_state(path: str) -> Dict[str,Any]:
    if not path or not os.path.exists(path):
        return {"last_run": None, "drs": {}}
    try:
        with open(path,'r',encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {"last_run": None, "drs": {}}


def save_state(path: str, state: Dict[str,Any]):
    tmp = path + '.tmp'
    with open(tmp,'w',encoding='utf-8') as f:
        json.dump(state,f,indent=2,default=str)
    os.replace(tmp, path)

# --------------- Main polling logic -----------------

def poll(args):
    # Optional jitter to spread concurrent scheduled runs
    if getattr(args, 'jitter_seconds', 0):
        sleep_for = random.uniform(0, args.jitter_seconds)
        print(f"[INFO] Jitter sleep {sleep_for:.2f}s (max {args.jitter_seconds}s)")
        time.sleep(sleep_for)

    poll_start_monotonic = time.perf_counter()
    timing_events: List[Dict[str, Any]] = []

    wsdl = args.base_url + '?wsdl'
    client = Client(wsdl=wsdl, transport=Transport(cache=InMemoryCache(), timeout=25))
    ops = safe_ops(client)
    dev_user = login(client, args.user, args.password, args.app_name)
    if dev_user is None:
        raise SystemExit('Login/GetUser failed: devUser context unavailable.')

    # Determine window (supports timezone offset to approximate plant local time)
    tz_offset = getattr(args, 'tz_offset_hours', 0)
    # Use timezone-aware UTC now then apply naive offset for local approximation
    now_utc = dt.datetime.now(dt.timezone.utc)
    now = (now_utc + dt.timedelta(hours=tz_offset)).replace(tzinfo=None)
    # Floor start to midnight (local offset) and end to now (or end-of-day if --eod flag added later)
    today_local = now.date()
    to_dt = now
    from_dt = (to_dt - dt.timedelta(days=args.window_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    print(f"[INFO] Window (floored start): {from_dt.isoformat()} -> {to_dt.isoformat()} (offset {tz_offset}h, span={args.window_days}d)")

    # Collect displays from queries
    displays: Dict[int, Any] = {}

    def add_results(result):
        if not result:
            return
        try:
            iterable = list(result)
        except Exception:
            return
        for disp in iterable:
            vals = getattr(disp, '__values__', {})
            if not isinstance(vals, dict):
                continue
            dn = vals.get('DeviationNumber')
            if isinstance(dn, int):
                displays[dn] = disp

    if 'DisplayItemForDateRange' not in ops:
        raise SystemExit('DisplayItemForDateRange not available')

    def call_with_retries(fn, description: str):
        for attempt in range(1, args.retries + 1):
            t0 = time.perf_counter()
            try:
                result = fn()
                elapsed = time.perf_counter() - t0
                timing_events.append({
                    'description': description,
                    'attempts': attempt,
                    'successful': True,
                    'elapsed_seconds': round(elapsed, 6)
                })
                return result
            except Exception as e:
                elapsed = time.perf_counter() - t0
                timing_events.append({
                    'description': description,
                    'attempts': attempt,
                    'successful': False,
                    'error': str(e),
                    'elapsed_seconds': round(elapsed, 6)
                })
                if attempt == args.retries:
                    print(f"[ERROR] {description} failed after {args.retries} attempts: {e}")
                else:
                    wait = args.retry_wait_seconds * (2 ** (attempt - 1))
                    print(f"[WARN] {description} attempt {attempt} failed: {e}; retrying in {wait:.1f}s")
                    time.sleep(wait)
        return None

    def open_variants():
        variants = [
            {'devUser': dev_user, 'fromDate': from_dt, 'toDate': to_dt},
            {'fromDate': from_dt, 'toDate': to_dt, 'devUser': dev_user},
        ]
        for i, kwargs in enumerate(variants, 1):
            res = call_with_retries(lambda kw=kwargs: client.service.DisplayItemForDateRange(**kw), f'Open variant {i}')
            if res is not None:
                add_results(res)
                if args.verbose:
                    try:
                        print(f"[DEBUG] Open variant {i} returned {len(list(res)) if res else 0} items")
                    except Exception:
                        pass

    def closed_variants():
        variants = [
            {'devUser': dev_user, 'fromDate': from_dt, 'toDate': to_dt, 'closed': True},
            {'fromDate': from_dt, 'toDate': to_dt, 'closed': True, 'devUser': dev_user},
        ]
        if getattr(args, 'single_closed_variant', False):
            variants = variants[:1]
        for i, kwargs in enumerate(variants, 1):
            res = call_with_retries(lambda kw=kwargs: client.service.DisplayItemForDateRange(**kw), f'Closed variant {i}')
            if res is not None:
                add_results(res)
                if args.verbose:
                    try:
                        print(f"[DEBUG] Closed variant {i} returned {len(list(res)) if res else 0} items")
                    except Exception:
                        pass

    # Normalized: if include_closed, closed variants alone appear to return all (open+closed)
    if args.include_closed:
        closed_variants()
    else:
        open_variants()

    print(f"[INFO] Retrieved {len(displays)} unique DR display records in window.")

    # Expectation check for specific DR numbers
    if getattr(args, 'expect_numbers', None):
        expected = {int(x) for x in args.expect_numbers if isinstance(x, int) or (isinstance(x, str) and x.strip().isdigit())}
        present = set(displays.keys())
        missing = sorted(expected - present)
        if missing:
            print(f"[WARN] Expected DR(s) missing from query results: {', '.join(str(m) for m in missing)}")
        else:
            print("[INFO] All expected DR numbers present.")

    state = load_state(args.state_file)
    dr_state: Dict[str,Any] = state.get('drs', {})

    output_records: List[Dict[str,Any]] = []

    for dn, disp in displays.items():
        dvals = getattr(disp,'__values__',{}) if disp else {}
        master_obj = dvals.get('Master') if isinstance(dvals, dict) else None
        # Routing history
        routing_steps: List[Dict[str,Any]] = []
        if 'RoutingsForMaster' in ops and master_obj is not None:
            try:
                rt0 = time.perf_counter()
                rlist = client.service.RoutingsForMaster(devUser=dev_user, master=master_obj)
                rt_elapsed = time.perf_counter() - rt0
                # Timing event per DR routing fetch
                timing_events.append({
                    'description': f'RoutingsForMaster DR {dn}',
                    'attempts': 1,
                    'successful': True,
                    'elapsed_seconds': round(rt_elapsed, 6)
                })
                routing_steps = extract_routing_list(rlist)
            except Exception as e:
                print(f'[WARN] RoutingsForMaster failed for DR {dn}: {e}')
                timing_events.append({
                    'description': f'RoutingsForMaster DR {dn}',
                    'attempts': 1,
                    'successful': False,
                    'error': str(e),
                    'elapsed_seconds': None
                })
        latest = latest_step(routing_steps)
        latest_non_empty = latest_non_empty_comment_step(routing_steps)
        notes_count = None
        if args.include_notes and 'NotesForMaster' in ops and master_obj is not None:
            notes_count = count_notes(client, dev_user, master_obj)
        # Prior state
        prev = dr_state.get(str(dn), {})
        prev_ts = prev.get('lastRoutingTouched')
        prev_count = prev.get('lastRoutingStepCount')
        prev_last_non_empty_comment = prev.get('lastNonEmptyRoutingComment')
        # Current computed
        latest_ts = latest.get('DateTouched') if latest else None
        step_count = len(routing_steps)
        current_non_empty_comment = latest_non_empty.get('UserComments') if latest_non_empty else None
        updated_routing = False
        updated_comment = False
        if latest_ts and latest_ts != prev_ts:
            updated_routing = True
        elif prev_count is not None and step_count != prev_count:
            updated_routing = True
        if current_non_empty_comment and current_non_empty_comment != prev_last_non_empty_comment:
            updated_comment = True
        updated = updated_routing or updated_comment
        # Record
        rec: Dict[str,Any] = {
            'DeviationNumber': dn,
            'CurrentRouting': dvals.get('CurrentRouting'),
            'DeviationState': dvals.get('DeviationState'),
            'IsClosed': dvals.get('IsClosed'),
            'Product': dvals.get('Product'),
            'SalesOrderNumber': dvals.get('SalesOrderNumber'),
            'CreationComments': dvals.get('CreationComments'),
            'LatestRoutingDepartment': latest.get('RoutingDepartment') if latest else None,
            'LatestRoutingUser': latest.get('UserName') if latest else None,
            'LatestRoutingState': latest.get('State') if latest else None,
            'LatestRoutingTouched': latest_ts,
            'LatestRoutingComment': latest.get('UserComments') if latest else None,
            'LatestNonEmptyRoutingComment': current_non_empty_comment,
            'RoutingStepCount': step_count,
            'Updated': updated,
            'UpdatedRouting': updated_routing,
            'UpdatedComment': updated_comment,
        }
        if notes_count is not None:
            rec['NotesCount'] = notes_count
        if args.include_history:
            rec['RoutingHistory'] = routing_steps
        output_records.append(rec)
        # Persist state per DR
        dr_state[str(dn)] = {
            'lastRoutingTouched': latest_ts,
            'lastRoutingStepCount': step_count,
            'lastNoteCount': notes_count,
            'lastSeenState': dvals.get('DeviationState'),
            'lastNonEmptyRoutingComment': current_non_empty_comment
        }

    # Persist state
    state['last_run'] = dt.datetime.now(dt.timezone.utc).isoformat()
    state['drs'] = dr_state
    if args.state_file:
        save_state(args.state_file, state)
        print(f"[INFO] State saved -> {args.state_file}")

    # JSON output
    if args.out_json:
        with open(args.out_json,'w',encoding='utf-8') as jf:
            json.dump(output_records, jf, indent=2, default=str)
        print(f"[INFO] Wrote JSON -> {args.out_json}")
    # CSV output
    if args.out_csv:
        cols = [
            'DeviationNumber','IsClosed','CurrentRouting','DeviationState','Product','SalesOrderNumber',
            'RoutingStepCount','LatestRoutingDepartment','LatestRoutingUser','LatestRoutingState','LatestRoutingTouched',
            'LatestRoutingComment','LatestNonEmptyRoutingComment','Updated','UpdatedRouting','UpdatedComment'
        ]
        if any('CreationComments' in r for r in output_records):
            cols.insert(6, 'CreationComments')
        if any('NotesCount' in r for r in output_records):
            cols.append('NotesCount')
        with open(args.out_csv,'w',newline='',encoding='utf-8') as cf:
            w = csv.DictWriter(cf, fieldnames=cols)
            w.writeheader()
            for r in output_records:
                w.writerow({c: r.get(c,'') for c in cols})
        print(f"[INFO] Wrote CSV -> {args.out_csv} (rows={len(output_records)})")

    # Only-updated outputs if requested
    updated_records = [r for r in output_records if r.get('Updated')]
    if getattr(args, 'only_updated_json', None):
        with open(args.only_updated_json, 'w', encoding='utf-8') as jf:
            json.dump(updated_records, jf, indent=2, default=str)
        print(f"[INFO] Wrote only-updated JSON -> {args.only_updated_json} (rows={len(updated_records)})")
    if getattr(args, 'only_updated_csv', None):
        cols2 = [
            'DeviationNumber','IsClosed','CurrentRouting','DeviationState','Product','SalesOrderNumber','CreationComments',
            'RoutingStepCount','LatestRoutingDepartment','LatestRoutingUser','LatestRoutingState','LatestRoutingTouched',
            'LatestRoutingComment','LatestNonEmptyRoutingComment','Updated','UpdatedRouting','UpdatedComment'
        ]
        if any('NotesCount' in r for r in updated_records):
            cols2.append('NotesCount')
        with open(args.only_updated_csv,'w',newline='',encoding='utf-8') as cf2:
            w2 = csv.DictWriter(cf2, fieldnames=cols2)
            w2.writeheader()
            for r in updated_records:
                w2.writerow({c: r.get(c,'') for c in cols2})
        print(f"[INFO] Wrote only-updated CSV -> {args.only_updated_csv} (rows={len(updated_records)})")

    total_elapsed = time.perf_counter() - poll_start_monotonic
    if getattr(args, 'timings', False):
        print('[INFO] Timing details:')
        for ev in timing_events:
            status = 'OK' if ev['successful'] else 'FAIL'
            print(f"  - {ev['description']} (attempt {ev['attempts']}, {status}) {ev['elapsed_seconds']:.4f}s")
        print(f"[INFO] Total wall time: {total_elapsed:.4f}s")
    if getattr(args, 'timings_json', None):
        try:
            timings_payload = {
                'generated_at_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
                'total_elapsed_seconds': round(total_elapsed, 6),
                'events': timing_events
            }
            with open(args.timings_json, 'w', encoding='utf-8') as tf:
                json.dump(timings_payload, tf, indent=2)
            print(f"[INFO] Wrote timings JSON -> {args.timings_json}")
        except Exception as e:
            print(f"[WARN] Failed writing timings JSON: {e}")
    print('[INFO] Poll complete.')

# --------------- CLI -------------------------------

def main():
    ap = argparse.ArgumentParser(description='Incremental DR poller (read-only).')
    ap.add_argument('--base-url', default=os.getenv('MOM_BASE_URL', DEFAULT_BASE_URL))
    ap.add_argument('--user', required=True)
    ap.add_argument('--password', required=True)
    ap.add_argument('--app-name', default=os.getenv('MOM_APP_NAME','ManufacturingDeviationSystem'))
    ap.add_argument('--window-days', type=int, default=7, help='Rolling window size in days (default 7).')
    ap.add_argument('--tz-offset-hours', type=int, default=0, help='UTC offset hours to approximate plant local time (e.g. -5 for CDT).')
    ap.add_argument('--include-closed', action='store_true', help='Use closed=true variants (appear to return all incl. open).')
    ap.add_argument('--include-notes', action='store_true', help='Fetch notes count via NotesForMaster.')
    ap.add_argument('--include-history', action='store_true', help='Embed full routing history in JSON output.')
    ap.add_argument('--state-file', default='dr_state.json', help='Path to JSON state file for incremental tracking.')
    ap.add_argument('--out-json', default=None, help='Write JSON summary.')
    ap.add_argument('--out-csv', default=None, help='Write CSV summary.')
    ap.add_argument('--only-updated-json', default=None, help='Write JSON with only Updated=true DRs.')
    ap.add_argument('--only-updated-csv', default=None, help='Write CSV with only Updated=true DRs.')
    ap.add_argument('--retries', type=int, default=3, help='Retry attempts for each SOAP call variant (default 3).')
    ap.add_argument('--retry-wait-seconds', type=float, default=1.0, help='Initial wait seconds before exponential backoff retries.')
    ap.add_argument('--verbose', action='store_true', help='Verbose debug output including variant call counts.')
    ap.add_argument('--jitter-seconds', type=float, default=0, help='Random 0..N seconds initial delay to de-synchronize schedules.')
    ap.add_argument('--timings', action='store_true', help='Print per-call timing diagnostics.')
    ap.add_argument('--timings-json', default=None, help='Write timing diagnostics JSON to path.')
    ap.add_argument('--single-closed-variant', action='store_true', help='When using --include-closed, call only the first closed variant (optimization if confirmed sufficient).')
    ap.add_argument('--expect-numbers', nargs='*', type=int, help='List of DR numbers expected to appear; warn if absent.')
    args = ap.parse_args()
    poll(args)

if __name__ == '__main__':
    main()
