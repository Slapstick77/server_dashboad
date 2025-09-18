"""Batch historical backfill for SCHSchedulingSummary.

Downloads overlapping multi‑month windows (primary 4 months, 1 month overlap by default),
cleans each CSV using existing clean.convert_file1_to_cleaned, and upserts rows so the
latest chunk overwrites earlier values for the same COMNumber1.

Usage examples (from repo root):
  python backfill_sched_summary_history.py --start 2021-05-14
  python backfill_sched_summary_history.py --start 2021-05-14 --primary 4 --overlap 1
  python backfill_sched_summary_history.py --start 2021-05-14 --end 2025-09-02

Environment / configuration:
  - Set SSRS_REPORTSERVER_ROOT (or create config.json) for the report server root.
  - Report path & parameter names are handled by PowerShell scripts (now centralized).

Progress phases emitted:
  chunk-start, download-done, clean-done, upsert-done, chunk-done, all-done, error, stopped

After completion prints summary counts and distinct COMNumber1 coverage.
"""
from __future__ import annotations
import argparse, os, sys, sqlite3
from datetime import date, datetime
from typing import Any, Dict

# Reuse existing functions
from report_update_service import (
    backfill_sched_summary_overlapping,
    ensure_change_log_tables,
    get_conn,
)

DEFAULT_START = date(2021,5,14)  # earliest historical start provided


def _parse_date(s: str) -> date:
    try:
        return datetime.strptime(s, '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError(f'Invalid date format (expected YYYY-MM-DD): {s}')


def progress_printer(phase: str, info: Dict[str, Any]):
    if phase == 'chunk-start':
        print(f"[START ] Chunk {info.get('index')} {info.get('start')}..{info.get('end')}")
    elif phase == 'download-done':
        print(f"[DL    ] {info.get('file')}")
    elif phase == 'clean-done':
        print(f"[CLEAN ] {os.path.basename(info.get('cleaned',''))}")
    elif phase == 'upsert-done':
        print(f"[UPSERT] rows={info.get('rows')} new={info.get('new')} upd={info.get('updates')} changes={info.get('changes')}")
    elif phase == 'chunk-done':
        print(f"[DONE  ] Chunk {info.get('index')} {info.get('start')}..{info.get('end')}")
    elif phase == 'all-done':
        print(f"[ALL   ] chunks={info.get('chunks')} new={info.get('total_new')} updated={info.get('total_updates')} changes={info.get('total_changes')}")
    elif phase == 'error':
        print(f"[ERROR ] {info}")
    elif phase == 'stopped':
        print(f"[STOP  ] {info}")
    sys.stdout.flush()


def distinct_comnumber1_count() -> int:
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='SCHSchedulingSummary'")
            if cur.fetchone()[0] == 0:
                return 0
            cur.execute("SELECT COUNT(DISTINCT comnumber1) FROM SCHSchedulingSummary")
            r = cur.fetchone()
            return r[0] if r and r[0] is not None else 0
    except Exception:
        return 0


def main():
    ap = argparse.ArgumentParser(description='Historical overlapping backfill for SCHSchedulingSummary.')
    ap.add_argument('--start', type=_parse_date, default=DEFAULT_START, help='Start date (YYYY-MM-DD) inclusive (default 2021-05-14).')
    ap.add_argument('--end', type=_parse_date, default=date.today(), help='End date (YYYY-MM-DD) inclusive (default today).')
    ap.add_argument('--primary', type=int, default=4, help='Primary window size in months (default 4).')
    ap.add_argument('--overlap', type=int, default=1, help='Overlap months between windows (default 1).')
    ap.add_argument('--dry-run', action='store_true', help='Parse args and exit without downloading.')
    args = ap.parse_args()

    if args.end < args.start:
        ap.error('End date must be >= start date')

    print(f"Historical backfill: start={args.start} end={args.end} primary_months={args.primary} overlap_months={args.overlap}")
    if args.dry_run:
        print('Dry run only. Exiting.')
        return 0

    ensure_change_log_tables()

    summary = backfill_sched_summary_overlapping(
        start=args.start,
        end=args.end,
        primary_months=args.primary,
        overlap_months=args.overlap,
        progress=progress_printer,
    )

    if not summary.get('ok'):
        print('FAILED:', summary)
        return 1

    distinct_cnt = distinct_comnumber1_count()
    print('\nSummary:')
    print(f"  Chunks completed : {summary.get('total_chunks')}")
    print(f"  New rows inserted : {summary.get('new_rows')}  (updates: {summary.get('updated_rows')}, total changes recorded: {summary.get('total_changes')})")
    print(f"  Distinct COMNumber1: {distinct_cnt}")
    print('Done.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
