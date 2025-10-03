"""
Triple-check verification for COM 19044 span calculation.
Query raw data and manually calculate span.
"""
import sqlite3
import os
from datetime import date

DB_PATH = os.path.join(os.path.dirname(__file__), 'SCHLabor.db')

def get_conn():
    return sqlite3.connect(DB_PATH)

print("=" * 80)
print("VERIFICATION: COM 19044 SPAN CALCULATION")
print("=" * 80)

# Get ALL labor records for COM 19044
with get_conn() as conn:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Get raw records with all date fields
    cur.execute("""
        SELECT 
            CAST(COMNumber AS TEXT) as com,
            LoggedDate,
            iso_logged_date,
            DepartmentNumber,
            COALESCE(ActualHours, 0) as hours,
            strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10))) as parsed_date
        FROM SCHLabor
        WHERE CAST(COMNumber AS TEXT) IN ('19044', '019044')
        ORDER BY parsed_date, DepartmentNumber
    """)
    
    all_records = cur.fetchall()

print(f"\nTotal records found for COM 19044: {len(all_records)}")

if not all_records:
    print("❌ NO RECORDS FOUND! Checking variations...")
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT CAST(COMNumber AS TEXT) as com
            FROM SCHLabor
            WHERE CAST(COMNumber AS TEXT) LIKE '%19044%'
            OR CAST(COMNumber AS TEXT) LIKE '%1904%'
            LIMIT 10
        """)
        similar = cur.fetchall()
        print(f"Similar COM numbers: {[r[0] for r in similar]}")
else:
    # Filter to only records with hours > 0
    records_with_hours = [r for r in all_records if r['hours'] > 0]
    print(f"Records with hours > 0: {len(records_with_hours)}")
    
    # Map dept codes
    raw_code_to_label = {
        '0120':'Fab','0140':'Welding','0180':'BaseFormPaint','0200':'FanAssyTest','0220':'InsulWallFab',
        '0230':'Pipe','0260':'Assembly','0270':'DoorFab','0280':'Assembly','0300':'Electrical','0320':'Pipe',
        '0340':'Paint','0360':'Test','0380':'Crating',
    }
    
    # Filter to tracked departments only
    tracked_codes = set(raw_code_to_label.keys())
    tracked_records = [r for r in records_with_hours if r['DepartmentNumber'] in tracked_codes]
    print(f"Records in tracked departments: {len(tracked_records)}")
    
    if tracked_records:
        # Get all unique dates with charges
        all_dates = []
        for r in tracked_records:
            if r['parsed_date']:
                all_dates.append(r['parsed_date'])
        
        all_dates = sorted(set(all_dates))
        
        print(f"\nTotal days with charges: {len(all_dates)}")
        print(f"First day with charges: {all_dates[0]}")
        print(f"Last day with charges: {all_dates[-1]}")
        
        # Calculate span manually
        first_date = date.fromisoformat(all_dates[0])
        last_date = date.fromisoformat(all_dates[-1])
        span_days = (last_date - first_date).days + 1
        
        print(f"\n{'='*80}")
        print(f"MANUAL SPAN CALCULATION:")
        print(f"{'='*80}")
        print(f"First Date: {first_date} ({first_date.strftime('%A, %B %d, %Y')})")
        print(f"Last Date:  {last_date} ({last_date.strftime('%A, %B %d, %Y')})")
        print(f"Span:       {span_days} days")
        print(f"{'='*80}")
        
        # Verify this matches 436
        if span_days == 436:
            print(f"✅ VERIFIED: Span is exactly 436 days")
        else:
            print(f"⚠️  MISMATCH: Expected 436 days, calculated {span_days} days")
        
        # Show breakdown by department
        print(f"\n{'='*80}")
        print(f"DEPARTMENT BREAKDOWN:")
        print(f"{'='*80}")
        
        from collections import defaultdict
        dept_data = defaultdict(lambda: {'hours': 0, 'charges': 0, 'dates': []})
        
        for r in tracked_records:
            dept = r['DepartmentNumber']
            dept_label = raw_code_to_label.get(dept, dept)
            dept_data[dept_label]['hours'] += r['hours']
            dept_data[dept_label]['charges'] += 1
            if r['parsed_date']:
                dept_data[dept_label]['dates'].append(r['parsed_date'])
        
        print(f"\n{'Department':<15} {'Hours':>8} {'Charges':>8} {'First Day':<12} {'Last Day':<12} {'Days':<8}")
        print("-" * 80)
        
        for dept in sorted(dept_data.keys()):
            data = dept_data[dept]
            dates = sorted(set(data['dates']))
            if dates:
                first = dates[0]
                last = dates[-1]
                dept_span = (date.fromisoformat(last) - date.fromisoformat(first)).days + 1
                print(f"{dept:<15} {data['hours']:>8.1f} {data['charges']:>8} {first:<12} {last:<12} {dept_span:<8}")
        
        # Show sample of records from first week and last week
        print(f"\n{'='*80}")
        print(f"SAMPLE RECORDS - FIRST WEEK:")
        print(f"{'='*80}")
        first_week_end = date.fromisoformat(all_dates[0]) 
        from datetime import timedelta
        first_week_end = (first_week_end + timedelta(days=7)).isoformat()
        
        first_week = [r for r in tracked_records if r['parsed_date'] and r['parsed_date'] <= first_week_end]
        print(f"{'Date':<12} {'Dept':<15} {'Hours':>8}")
        print("-" * 40)
        for r in first_week[:10]:
            dept_label = raw_code_to_label.get(r['DepartmentNumber'], r['DepartmentNumber'])
            print(f"{r['parsed_date']:<12} {dept_label:<15} {r['hours']:>8.1f}")
        
        print(f"\n{'='*80}")
        print(f"SAMPLE RECORDS - LAST WEEK:")
        print(f"{'='*80}")
        last_week_start = date.fromisoformat(all_dates[-1])
        last_week_start = (last_week_start - timedelta(days=7)).isoformat()
        
        last_week = [r for r in tracked_records if r['parsed_date'] and r['parsed_date'] >= last_week_start]
        print(f"{'Date':<12} {'Dept':<15} {'Hours':>8}")
        print("-" * 40)
        for r in last_week[-10:]:
            dept_label = raw_code_to_label.get(r['DepartmentNumber'], r['DepartmentNumber'])
            print(f"{r['parsed_date']:<12} {dept_label:<15} {r['hours']:>8.1f}")
        
        # Check completion table
        print(f"\n{'='*80}")
        print(f"COMPLETION TABLE VERIFICATION:")
        print(f"{'='*80}")
        
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("""
                SELECT com_number, last_day_unfiltered
                FROM UnitCompletion
                WHERE com_number IN ('19044', '019044')
            """)
            completion = cur.fetchone()
        
        if completion:
            print(f"COM Number: {completion['com_number']}")
            print(f"Last Day (unfiltered): {completion['last_day_unfiltered']}")
            
            if completion['last_day_unfiltered'] == all_dates[-1]:
                print(f"✅ Completion table matches labor data last day")
            else:
                print(f"⚠️  Completion table shows {completion['last_day_unfiltered']}, labor shows {all_dates[-1]}")
        else:
            print(f"⚠️  COM 19044 not found in UnitCompletion table")

print(f"\n{'='*80}")
print(f"VERIFICATION COMPLETE")
print(f"{'='*80}")
