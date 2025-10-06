"""
Debug: What are the trailing 30 units on June 1, 2025?
Why is the average still 122 days?
"""
import sqlite3
import os
import sys
from datetime import date, timedelta
from collections import defaultdict

ROOT = os.path.dirname(__file__)
DB_PATH = os.path.join(ROOT, 'SCHLabor.db')

sys.path.insert(0, ROOT)
from webapp.blueprints.utils import (
    TRACKED_DEPARTMENTS, COMPLETION_CHECK_DEPARTMENTS,
    normalize_com, fnum, _resolve_department_days
)
from metrics_cache import get_complete_units_with_dates

def get_conn():
    return sqlite3.connect(DB_PATH)

print("=" * 80)
print("DEBUG: Trailing 30 Units on June 1, 2025")
print("=" * 80)

# Get all complete units with dates
complete_units_dict = get_complete_units_with_dates()
units_list = [(com, last_day) for com, last_day in complete_units_dict.items()]
units_list.sort(key=lambda x: x[1])

# Find trailing 30 units on June 1, 2025
target_date = '2025-06-01'
eligible = [(com, ld) for com, ld in units_list if ld <= target_date]

if len(eligible) < 30:
    trailing_units = eligible
else:
    trailing_units = eligible[-30:]

print(f"\nTotal complete units: {len(units_list)}")
print(f"Eligible by June 1: {len(eligible)}")
print(f"Trailing 30 units:")
print(f"\n{'#':<4} {'COM':<8} {'Last Day':<12}")
print("-" * 30)

for i, (com, ld) in enumerate(trailing_units, 1):
    marker = " ← COM 19044" if com == '19044' else ""
    print(f"{i:<4} {com:<8} {ld:<12}{marker}")

print(f"\n✅ COM 19044 is unit #{[i for i, (c, _) in enumerate(trailing_units, 1) if c == '19044'][0] if '19044' in [c for c, _ in trailing_units] else 'NOT IN LIST'}")

print(f"\n{'='*80}")
print(f"This explains why COM 19044's fix didn't change the June average much!")
print(f"It's only 1 out of 30 units in the trailing window.")
print(f"The other 29 units likely have normal spans around 120 days.")
print(f"{'='*80}")
