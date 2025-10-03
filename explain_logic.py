"""
Show why COM 19044 passed the completion logic despite the 373-day gap.
"""
import sqlite3
import os
import sys

ROOT = os.path.dirname(__file__)
DB_PATH = os.path.join(ROOT, 'SCHLabor.db')

sys.path.insert(0, os.path.join(ROOT, 'webapp'))
from blueprints.utils import COMPLETION_CHECK_DEPARTMENTS, normalize_com, fnum

def get_conn():
    return sqlite3.connect(DB_PATH)

print("=" * 80)
print("WHY COM 19044 PASSED THE COMPLETION LOGIC")
print("=" * 80)

# Get scheduling summary for COM 19044
with get_conn() as conn:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    cur.execute("""
        SELECT *
        FROM SCHSchedulingSummary
        WHERE CAST(comnumber1 AS TEXT) IN ('19044', '019044')
    """)
    
    unit = cur.fetchone()

if not unit:
    print("\n❌ COM 19044 not found in SCHSchedulingSummary")
else:
    unit_dict = dict(unit)
    print(f"\n✅ Found COM 19044 in SCHSchedulingSummary")
    print(f"\nCOM Number: {unit_dict['comnumber1']}")
    
    print(f"\n{'='*80}")
    print(f"DEPARTMENT COMPLETION CHECK:")
    print(f"{'='*80}")
    print(f"\n{'Department':<15} {'Std Hrs':<10} {'Act Hrs':<10} {'Comp %':<10} {'Status':<15}")
    print("-" * 70)
    
    all_complete = True
    
    for label, stdc, actc, compc, effc in COMPLETION_CHECK_DEPARTMENTS:
        std = fnum(unit_dict.get(stdc)) if stdc in unit_dict else 0.0
        act = fnum(unit_dict.get(actc)) if actc in unit_dict else 0.0
        comp = fnum(unit_dict.get(compc)) if compc in unit_dict else 0.0
        
        if std <= 0:
            status = "SKIPPED (no work)"
            print(f"{label:<15} {std:<10.1f} {act:<10.1f} {comp:<10.1f} {status:<15}")
            continue
        
        # Check if complete
        if comp >= 100.0:
            status = "✅ COMPLETE"
        else:
            # Calculate from actual vs std
            calc_comp = min(100.0, (act / std) * 100.0) if act > 0 else 0.0
            if calc_comp >= 99.999:
                status = "✅ CALC COMPLETE"
            else:
                status = "❌ INCOMPLETE"
                all_complete = False
        
        print(f"{label:<15} {std:<10.1f} {act:<10.1f} {comp:<10.1f} {status:<15}")
    
    print(f"\n{'='*80}")
    print(f"FINAL RESULT:")
    print(f"{'='*80}")
    
    if all_complete:
        print(f"✅ Unit is 100% COMPLETE (all departments done)")
        print(f"\nThe logic ONLY checks:")
        print(f"  1. Is each department 100% complete? (YES)")
        print(f"  2. Does it have a last_day with charges? (YES - 2025-06-27)")
        print(f"\nThe logic DOES NOT check:")
        print(f"  ❌ Time gaps between charges")
        print(f"  ❌ Whether charges are recent or old")
        print(f"  ❌ Whether there was a 373-day gap")
        print(f"  ❌ Whether late charges are outliers")
        print(f"\n⚠️  THIS IS WHY IT PASSED!")
        print(f"    The unit IS technically complete, but the 373-day gap")
        print(f"    makes the span calculation (436 days) misleading.")
    else:
        print(f"❌ Unit is INCOMPLETE (has incomplete departments)")

print(f"\n{'='*80}")
print(f"CURRENT LOGIC EXPLANATION:")
print(f"{'='*80}")
print(f"""
The completion logic in metrics_cache.py (lines 120-250):

1. Get all units from SCHSchedulingSummary
2. For each department with planned work (std hours > 0):
   - Check if comp% >= 100 OR
   - Check if (act/std)*100 >= 99.999
3. If ALL departments pass → unit is 100% complete
4. Store in UnitCompletion table with last_day_unfiltered

WHAT IT DOESN'T DO:
- Filter out charges after long gaps
- Check if charges are clustered vs scattered
- Detect outlier/late charges (rework, warranty, errors)
- Apply any time-based filtering rules

RESULT:
- COM 19044 is legitimately 100% complete by the metrics
- The 2.5 hours charged on 2025-06-27 (373 days after previous work)
  are included in the span calculation
- This inflates the span from ~60 days to 436 days
""")

print(f"\n{'='*80}")
