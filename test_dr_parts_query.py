"""Create a query to get parts info for DRs from PartsTracker"""
import sqlite3
import re

conn = sqlite3.connect('SCHLabor.db')
cursor = conn.cursor()

# Test query to get parts for a specific DR
test_dr = "49225"  # From the latest data we saw

print("="*80)
print(f"Testing parts lookup for DR#{test_dr}")
print("="*80 + "\n")

# Query to find all parts for this DR
cursor.execute("""
    SELECT 
        com,
        part,
        rack,
        machine,
        operator,
        datetime,
        COUNT(*) as part_count
    FROM PartsTracker
    WHERE com LIKE ? OR com LIKE ? OR com LIKE ?
    GROUP BY part, rack
    ORDER BY datetime DESC
""", (f'DR{test_dr}', f'DR {test_dr}', f'DR#{test_dr}'))

parts = cursor.fetchall()

if parts:
    print(f"Found {len(parts)} unique part/rack combinations for DR#{test_dr}:\n")
    
    total_parts = sum(p[6] for p in parts)
    print(f"Total part scans: {total_parts}\n")
    
    for i, (com, part, rack, machine, operator, dt, count) in enumerate(parts, 1):
        if part:  # Only show if part number exists
            print(f"{i}. Part: {part}")
            print(f"   Rack/Cart: {rack}")
            print(f"   Count: {count} scans")
            print(f"   Last scan: {dt} by {operator}")
            print()
else:
    print(f"No parts found for DR#{test_dr}")

# Now create a general query that can work for any DR number
print("\n" + "="*80)
print("Sample query for DR parts summary:")
print("="*80 + "\n")

# Get a few DRs to demonstrate
cursor.execute("""
    SELECT DISTINCT com
    FROM PartsTracker
    WHERE com LIKE 'DR%' OR com LIKE 'dr%'
    ORDER BY _ingested_at DESC
    LIMIT 5
""")

recent_drs = [row[0] for row in cursor.fetchall()]

for dr_com in recent_drs:
    # Extract just the number
    match = re.search(r'\d+', dr_com)
    if match:
        dr_num = match.group()
    else:
        dr_num = dr_com
    
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT part) as unique_parts,
            COUNT(*) as total_scans,
            GROUP_CONCAT(DISTINCT rack) as racks
        FROM PartsTracker
        WHERE (com LIKE ? OR com LIKE ? OR com LIKE ?)
          AND part IS NOT NULL 
          AND part != ''
    """, (f'DR{dr_num}', f'DR {dr_num}', f'DR#{dr_num}'))
    
    result = cursor.fetchone()
    unique_parts, total_scans, racks = result
    
    if unique_parts and unique_parts > 0:
        print(f"DR#{dr_num}:")
        print(f"  {unique_parts} unique parts made, {total_scans} total scans")
        print(f"  Carts/Racks: {racks}")
        print()

conn.close()

print("\n" + "="*80)
print("Suggested display format for DR dashboard:")
print("="*80)
print("""
Example: "3 parts made on PALLET, UNIT"
Or: "5 unique parts, 12 scans on TR-37, PB-19"
""")
