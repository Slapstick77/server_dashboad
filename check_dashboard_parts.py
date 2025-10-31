"""Check if current DRs on dashboard have parts in PartsTracker"""
import sqlite3
from datetime import datetime, timedelta
import re

conn = sqlite3.connect('SCHLabor.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# Get DRs from last 7 days (same as dashboard)
cutoff = datetime.now() - timedelta(days=7)

print("="*80)
print(f"Checking DRs created since {cutoff.strftime('%Y-%m-%d %H:%M')}")
print("="*80 + "\n")

# Get the DRs that would show on dashboard
cursor.execute('''
    WITH LatestDRs AS (
        SELECT deviation_number, MAX(run_id) as latest_run
        FROM DRItemSnapshot
        GROUP BY deviation_number
    )
    SELECT 
        d.deviation_number,
        d.current_routing,
        d.deviation_state,
        d.comnumber1,
        m.urgency,
        m.date_created
    FROM DRItemSnapshot d
    INNER JOIN LatestDRs l ON d.deviation_number = l.deviation_number AND d.run_id = l.latest_run
    LEFT JOIN DRStaticMetadata m ON d.deviation_number = m.deviation_number
    WHERE m.date_created IS NOT NULL
      AND datetime(m.date_created) >= datetime(?)
      AND LOWER(d.deviation_state) NOT IN ('complete', 'closed')
    ORDER BY m.date_created DESC
''', (cutoff.isoformat(),))

drs = cursor.fetchall()

print(f"Found {len(drs)} DRs that should be on dashboard:\n")

has_parts_count = 0
no_parts_count = 0

for dr in drs:
    dr_num = dr['deviation_number']
    
    # Use a separate cursor for nested queries
    cursor2 = conn.cursor()
    
    # Check for parts in PartsTracker
    cursor2.execute("""
        SELECT 
            COUNT(DISTINCT part) as unique_parts,
            COUNT(*) as total_scans,
            GROUP_CONCAT(DISTINCT rack) as racks
        FROM PartsTracker
        WHERE (com LIKE ? OR com LIKE ? OR com LIKE ?)
          AND part IS NOT NULL 
          AND part != ''
    """, (f'DR{dr_num}', f'DR {dr_num}', f'DR#{dr_num}'))
    
    parts = cursor2.fetchone()
    
    # Also check the actual COM entries to see what format they're in
    cursor2.execute("""
        SELECT DISTINCT com
        FROM PartsTracker
        WHERE (com LIKE ? OR com LIKE ? OR com LIKE ?)
        LIMIT 1
    """, (f'DR{dr_num}', f'DR {dr_num}', f'DR#{dr_num}'))
    
    com_found = cursor2.fetchone()
    cursor2.close()
    
    status = "✅ HAS PARTS" if parts['unique_parts'] else "❌ NO PARTS"
    
    if parts['unique_parts']:
        has_parts_count += 1
        print(f"{status} - DR#{dr_num}")
        print(f"  Created: {dr['date_created']}")
        print(f"  Routing: {dr['current_routing']}, State: {dr['deviation_state']}")
        print(f"  COM: {dr['comnumber1']}")
        if com_found:
            print(f"  PartsTracker COM format: {com_found['com']}")
        print(f"  Parts: {parts['unique_parts']} unique, {parts['total_scans']} scans")
        print(f"  Racks: {parts['racks']}")
        print()
    else:
        no_parts_count += 1
        # Just show DR number for ones without parts
        if no_parts_count <= 10:  # Only show first 10 to avoid clutter
            print(f"{status} - DR#{dr_num} (Created: {dr['date_created']}, Routing: {dr['current_routing']})")

if no_parts_count > 10:
    print(f"\n... and {no_parts_count - 10} more DRs without parts")

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"Total DRs on dashboard: {len(drs)}")
print(f"DRs with parts tracked: {has_parts_count} ({has_parts_count/len(drs)*100:.1f}%)")
print(f"DRs without parts: {no_parts_count} ({no_parts_count/len(drs)*100:.1f}%)")

print("\n" + "="*80)
print("Checking for parts with DR COMs that aren't in current dashboard...")
print("="*80 + "\n")

cursor.execute("""
    SELECT DISTINCT com
    FROM PartsTracker
    WHERE (com LIKE 'DR%' OR com LIKE 'dr%')
    ORDER BY _ingested_at DESC
    LIMIT 20
""")

recent_dr_coms = cursor.fetchall()
dr_numbers_on_dashboard = [dr['deviation_number'] for dr in drs]

for row in recent_dr_coms:
    com = row['com']
    # Extract number from COM
    match = re.search(r'\d+', com)
    if match:
        num = match.group()
        if num not in dr_numbers_on_dashboard:
            print(f"⚠️  {com} in PartsTracker but DR#{num} not on current dashboard")
            # Check if this DR exists but is older or closed
            cursor2 = conn.cursor()
            cursor2.execute("""
                SELECT m.date_created, d.deviation_state
                FROM DRStaticMetadata m
                LEFT JOIN DRItemSnapshot d ON m.deviation_number = d.deviation_number
                WHERE m.deviation_number = ?
                LIMIT 1
            """, (num,))
            dr_info = cursor2.fetchone()
            cursor2.close()
            if dr_info:
                print(f"    → DR#{num} exists: state={dr_info['deviation_state']}, created={dr_info['date_created']}")
            else:
                print(f"    → DR#{num} not found in database")

conn.close()
