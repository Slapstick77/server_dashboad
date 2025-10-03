"""
Quick analysis to investigate span drop around August.
Check what units completed and their actual span values.
"""
import sqlite3
import os
from datetime import date, timedelta
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), 'SCHLabor.db')

def get_conn():
    return sqlite3.connect(DB_PATH)

print("=" * 80)
print("SPAN DROP ANALYSIS - August 2025")
print("=" * 80)

# Get complete units from June through September
with get_conn() as conn:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Get units that completed between June 1 and Sept 30
    cur.execute("""
        SELECT com_number, last_day_unfiltered
        FROM UnitCompletion
        WHERE last_day_unfiltered >= '2025-06-01'
          AND last_day_unfiltered <= '2025-09-30'
        ORDER BY last_day_unfiltered
    """)
    
    summer_units = [dict(r) for r in cur.fetchall()]

print(f"\nFound {len(summer_units)} units completed between June-September 2025")
print("\nBreakdown by month:")

# Group by month
by_month = defaultdict(list)
for unit in summer_units:
    month = unit['last_day_unfiltered'][:7]  # YYYY-MM
    by_month[month].append(unit)

for month in sorted(by_month.keys()):
    print(f"  {month}: {len(by_month[month])} units")

# Now let's calculate actual span for units completing in August
print("\n" + "=" * 80)
print("CALCULATING ACTUAL SPANS FOR AUGUST UNITS")
print("=" * 80)

august_units = by_month.get('2025-08', [])
if not august_units:
    print("\nNo units completed in August 2025")
else:
    print(f"\nAnalyzing {len(august_units)} units that completed in August...")
    
    # Map department codes
    raw_code_to_label = {
        '0120':'Fab','0140':'Welding','0180':'BaseFormPaint','0200':'FanAssyTest','0220':'InsulWallFab',
        '0230':'Pipe','0260':'Assembly','0270':'DoorFab','0280':'Assembly','0300':'Electrical','0320':'Pipe',
        '0340':'Paint','0360':'Test','0380':'Crating',
    }
    tracked_codes = sorted(raw_code_to_label.keys())
    codes_sql = ','.join(f"'{c}'" for c in tracked_codes)
    
    # Get COMs for August units
    august_coms = [u['com_number'] for u in august_units]
    placeholders = ','.join('?' for _ in august_coms)
    
    # Load labor data
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        cur.execute(f"""
            SELECT CAST(COMNumber AS TEXT) com,
                   strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10))) day,
                   DepartmentNumber dept,
                   COALESCE(ActualHours,0) hrs
            FROM SCHLabor
            WHERE COALESCE(ActualHours,0) > 0
              AND DepartmentNumber IN ({codes_sql})
              AND CAST(COMNumber AS TEXT) IN ({placeholders})
        """, august_coms)
        
        labor_rows = cur.fetchall()
    
    print(f"Loaded {len(labor_rows)} labor rows for August units")
    
    # Calculate unfiltered span (first to last day with ANY charges)
    unit_days = defaultdict(set)
    for row in labor_rows:
        com = row['com'].zfill(5)
        day = row['day']
        if day:
            unit_days[com].add(day)
    
    # Calculate spans
    spans = []
    print("\nSample of August units (first 10):")
    print(f"{'COM':<8} {'First Day':<12} {'Last Day':<12} {'Span (days)':<12}")
    print("-" * 50)
    
    for i, com in enumerate(sorted(unit_days.keys())):
        days = sorted(unit_days[com])
        if days:
            first_day = days[0]
            last_day = days[-1]
            try:
                span = (date.fromisoformat(last_day) - date.fromisoformat(first_day)).days + 1
            except:
                span = 0
            spans.append(span)
            
            if i < 10:  # Show first 10
                print(f"{com:<8} {first_day:<12} {last_day:<12} {span:<12}")
    
    if spans:
        avg_span = sum(spans) / len(spans)
        min_span = min(spans)
        max_span = max(spans)
        
        print(f"\n{'Total Units Analyzed:':<25} {len(spans)}")
        print(f"{'Average Span:':<25} {avg_span:.1f} days")
        print(f"{'Min Span:':<25} {min_span} days")
        print(f"{'Max Span:':<25} {max_span} days")
        
        # Distribution
        buckets = {'0-10': 0, '11-20': 0, '21-30': 0, '31-60': 0, '61-90': 0, '90+': 0}
        for s in spans:
            if s <= 10: buckets['0-10'] += 1
            elif s <= 20: buckets['11-20'] += 1
            elif s <= 30: buckets['21-30'] += 1
            elif s <= 60: buckets['31-60'] += 1
            elif s <= 90: buckets['61-90'] += 1
            else: buckets['90+'] += 1
        
        print(f"\nSpan Distribution:")
        for bucket, count in buckets.items():
            pct = (count / len(spans)) * 100 if spans else 0
            bar = '█' * int(pct / 2)
            print(f"  {bucket:<10} {count:>3} units ({pct:>5.1f}%) {bar}")

# Analyze all summer months
print("\n" + "=" * 80)
print("MONTHLY COMPARISON: JUNE → JULY → AUGUST → SEPTEMBER")
print("=" * 80)

monthly_stats = {}

for month_code in ['2025-06', '2025-07', '2025-08', '2025-09']:
    month_units = by_month.get(month_code, [])
    if not month_units:
        continue
    
    month_coms = [u['com_number'] for u in month_units]
    placeholders = ','.join('?' for _ in month_coms)
    
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        cur.execute(f"""
            SELECT CAST(COMNumber AS TEXT) com,
                   strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10))) day
            FROM SCHLabor
            WHERE COALESCE(ActualHours,0) > 0
              AND DepartmentNumber IN ({codes_sql})
              AND CAST(COMNumber AS TEXT) IN ({placeholders})
        """, month_coms)
        
        labor_rows = cur.fetchall()
    
    unit_days = defaultdict(set)
    for row in labor_rows:
        com = row['com'].zfill(5)
        day = row['day']
        if day:
            unit_days[com].add(day)
    
    month_spans = []
    for com in unit_days:
        days = sorted(unit_days[com])
        if days:
            try:
                span = (date.fromisoformat(days[-1]) - date.fromisoformat(days[0])).days + 1
                month_spans.append(span)
            except:
                pass
    
    if month_spans:
        monthly_stats[month_code] = {
            'count': len(month_spans),
            'avg': sum(month_spans) / len(month_spans),
            'min': min(month_spans),
            'max': max(month_spans),
            'spans': month_spans
        }

# Print monthly comparison
print(f"\n{'Month':<12} {'Units':<8} {'Avg Span':<12} {'Min':<8} {'Max':<8}")
print("-" * 60)
for month in ['2025-06', '2025-07', '2025-08', '2025-09']:
    if month in monthly_stats:
        stats = monthly_stats[month]
        print(f"{month:<12} {stats['count']:<8} {stats['avg']:>7.1f} days  {stats['min']:>3}      {stats['max']:>3}")

# Trend analysis
if len(monthly_stats) >= 2:
    print("\n" + "=" * 80)
    print("TREND ANALYSIS")
    print("=" * 80)
    
    months = sorted(monthly_stats.keys())
    print(f"\nMonth-over-month changes:")
    for i in range(1, len(months)):
        prev_month = months[i-1]
        curr_month = months[i]
        prev_avg = monthly_stats[prev_month]['avg']
        curr_avg = monthly_stats[curr_month]['avg']
        change = curr_avg - prev_avg
        pct_change = (change / prev_avg) * 100 if prev_avg > 0 else 0
        
        arrow = "📈" if change > 0 else "📉" if change < 0 else "➡️"
        print(f"  {prev_month[-2:]} → {curr_month[-2:]}: {prev_avg:>5.1f} → {curr_avg:>5.1f} days  ({change:+.1f} days, {pct_change:+.1f}%) {arrow}")
    
    # Overall trend
    first_avg = monthly_stats[months[0]]['avg']
    last_avg = monthly_stats[months[-1]]['avg']
    total_change = last_avg - first_avg
    total_pct = (total_change / first_avg) * 100 if first_avg > 0 else 0
    
    print(f"\nOverall trend ({months[0][-2:]} → {months[-1][-2:]}):")
    print(f"  {first_avg:.1f} → {last_avg:.1f} days ({total_change:+.1f} days, {total_pct:+.1f}%)")
    
    if abs(total_pct) > 20:
        if total_pct < 0:
            print(f"\n✅ SIGNIFICANT IMPROVEMENT: {abs(total_pct):.0f}% faster completion")
        else:
            print(f"\n⚠️  SIGNIFICANT SLOWDOWN: {total_pct:.0f}% slower completion")

# Find the longest span unit
print("\n" + "=" * 80)
print("LONGEST SPAN UNIT DETAILS")
print("=" * 80)

all_units_with_spans = []
for month in monthly_stats:
    month_units = by_month.get(month, [])
    month_coms = [u['com_number'] for u in month_units]
    
    # Get span for each COM
    placeholders = ','.join('?' for _ in month_coms)
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        cur.execute(f"""
            SELECT CAST(COMNumber AS TEXT) com,
                   strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10))) day
            FROM SCHLabor
            WHERE COALESCE(ActualHours,0) > 0
              AND DepartmentNumber IN ({codes_sql})
              AND CAST(COMNumber AS TEXT) IN ({placeholders})
        """, month_coms)
        
        labor_rows = cur.fetchall()
    
    unit_days = defaultdict(set)
    for row in labor_rows:
        com = row['com'].zfill(5)
        day = row['day']
        if day:
            unit_days[com].add(day)
    
    for com in unit_days:
        days = sorted(unit_days[com])
        if days:
            try:
                first_day = days[0]
                last_day = days[-1]
                span = (date.fromisoformat(last_day) - date.fromisoformat(first_day)).days + 1
                all_units_with_spans.append({
                    'com': com,
                    'month': month,
                    'first_day': first_day,
                    'last_day': last_day,
                    'span': span
                })
            except:
                pass

# Find top 10 longest spans
all_units_with_spans.sort(key=lambda x: x['span'], reverse=True)
top_10 = all_units_with_spans[:10]

print(f"\nTop 10 Longest Span Units (June-September 2025):\n")
print(f"{'Rank':<6} {'COM':<8} {'Completed':<12} {'First Day':<12} {'Last Day':<12} {'Span':<12}")
print("-" * 75)

for i, unit in enumerate(top_10, 1):
    print(f"{i:<6} {unit['com']:<8} {unit['month']:<12} {unit['first_day']:<12} {unit['last_day']:<12} {unit['span']} days")

# Get details for the longest one
if top_10:
    longest = top_10[0]
    print(f"\n🔍 Details for COM {longest['com']} (436 day span):")
    print(f"   Started: {longest['first_day']}")
    print(f"   Completed: {longest['last_day']}")
    print(f"   Completion Month: {longest['month']}")
    print(f"   Total Span: {longest['span']} days")
    
    # Get department breakdown
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        cur.execute(f"""
            SELECT DepartmentNumber dept,
                   COUNT(*) charge_count,
                   SUM(COALESCE(ActualHours,0)) total_hours,
                   MIN(strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10)))) first_charge,
                   MAX(strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10)))) last_charge
            FROM SCHLabor
            WHERE CAST(COMNumber AS TEXT) = ?
              AND COALESCE(ActualHours,0) > 0
              AND DepartmentNumber IN ({codes_sql})
            GROUP BY DepartmentNumber
            ORDER BY first_charge
        """, [longest['com']])
        
        dept_data = cur.fetchall()
    
    if dept_data:
        print(f"\n   Department Timeline:")
        print(f"   {'Dept':<15} {'Hours':<10} {'Charges':<10} {'First Charge':<12} {'Last Charge':<12}")
        print(f"   {'-'*70}")
        for row in dept_data:
            dept_label = raw_code_to_label.get(row['dept'], row['dept'])
            print(f"   {dept_label:<15} {row['total_hours']:>7.1f}   {row['charge_count']:>6}     {row['first_charge']:<12} {row['last_charge']:<12}")

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)
