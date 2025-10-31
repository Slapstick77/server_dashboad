"""Check PartsTracker for DR# patterns in COM field"""
import sqlite3

conn = sqlite3.connect('SCHLabor.db')
cursor = conn.cursor()

print("="*80)
print("Checking COM field for DR# patterns")
print("="*80)

# Check for DR patterns in COM field
cursor.execute("""
    SELECT com, rack, machine, operator, part, datetime, _ingested_at
    FROM PartsTracker 
    WHERE com LIKE '%DR%' OR com LIKE '%dr%'
    ORDER BY datetime DESC
    LIMIT 30
""")

dr_coms = cursor.fetchall()
print(f"\nFound {len(dr_coms)} entries with 'DR' in COM field (most recent first):\n")

for i, row in enumerate(dr_coms, 1):
    com, rack, machine, operator, part, dt, ingested = row
    print(f"{i}. COM: {com}")
    print(f"   Rack: {rack}, Machine: {machine}, Operator: {operator}")
    print(f"   Part: {part}, DateTime: {dt}")
    print(f"   Ingested: {ingested}")
    print()

# Check the latest entries overall to see current format
print("\n" + "="*80)
print("Latest 20 PartsTracker entries (to see current format):")
print("="*80 + "\n")

cursor.execute("""
    SELECT com, rack, machine, operator, part, datetime, _ingested_at
    FROM PartsTracker 
    ORDER BY _ingested_at DESC
    LIMIT 20
""")

latest = cursor.fetchall()
for i, row in enumerate(latest, 1):
    com, rack, machine, operator, part, dt, ingested = row
    print(f"{i}. COM: {com}")
    print(f"   Rack: {rack}, Part: {part}, DateTime: {dt}")
    print(f"   Ingested: {ingested}")
    print()

# Get distinct COM patterns that start with DR
print("\n" + "="*80)
print("Distinct COM patterns starting with 'DR':")
print("="*80 + "\n")

cursor.execute("""
    SELECT DISTINCT com, COUNT(*) as count
    FROM PartsTracker 
    WHERE com LIKE 'DR%' OR com LIKE 'dr%'
    GROUP BY com
    ORDER BY count DESC
    LIMIT 50
""")

patterns = cursor.fetchall()
for com, count in patterns:
    print(f"{com} ({count} entries)")

conn.close()
