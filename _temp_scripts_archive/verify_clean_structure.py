import sqlite3

conn = sqlite3.connect('SCHLabor.db')
cur = conn.cursor()

print("=" * 70)
print("TABLE SEPARATION - CLEAN ARCHITECTURE")
print("=" * 70)

# Show DRItemSnapshot structure (snapshot data that changes)
print("\n📊 DRItemSnapshot (21 columns) - Snapshot data that changes over time:")
cur.execute("SELECT COUNT(*) FROM DRItemSnapshot")
print(f"   Total snapshots: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(DISTINCT deviation_number) FROM DRItemSnapshot")
print(f"   Unique DRs: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(DISTINCT run_id) FROM DRItemSnapshot")
print(f"   Poll runs: {cur.fetchone()[0]}")

# Show DRStaticMetadata structure (static data, never changes)
print("\n📋 DRStaticMetadata (8 columns) - Static data captured once:")
cur.execute("SELECT COUNT(*) FROM DRStaticMetadata")
meta_count = cur.fetchone()[0]
print(f"   DRs with metadata: {meta_count}")

cur.execute("""
    SELECT 
        COUNT(CASE WHEN urgency IS NOT NULL THEN 1 END) as has_urgency,
        COUNT(CASE WHEN defect_description IS NOT NULL THEN 1 END) as has_defect,
        COUNT(CASE WHEN charged_to_dept IS NOT NULL THEN 1 END) as has_dept
    FROM DRStaticMetadata
""")
row = cur.fetchone()
print(f"   - With urgency: {row[0]}")
print(f"   - With defect: {row[1]}")
print(f"   - With charged_to: {row[2]}")

# Sample metadata
print("\n   Sample metadata (first 3 DRs):")
cur.execute("""
    SELECT deviation_number, urgency, defect_description, charged_to_dept
    FROM DRStaticMetadata
    LIMIT 3
""")
for row in cur.fetchall():
    urg = row[1][:25] + '...' if row[1] and len(row[1]) > 25 else row[1]
    def_desc = row[2][:25] + '...' if row[2] and len(row[2]) > 25 else row[2]
    print(f"   DR {row[0]}: {urg} | {def_desc} | {row[3]}")

print("\n" + "=" * 70)
print("✅ CLEAN SEPARATION: DRItemSnapshot = snapshots, DRStaticMetadata = static")
print("=" * 70)

conn.close()
