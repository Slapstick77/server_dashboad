"""
Diagnostic script to check DR comments issue
"""
import sqlite3
import os

# Database path
DB_PATH = os.path.join(os.path.dirname(__file__), 'SCHLabor.db')

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=" * 80)
print("DR COMMENTS DIAGNOSTIC")
print("=" * 80)

# Check a few recent DRs to see comment status
cur.execute("""
    SELECT deviation_number, UserComments, DateTouched, UserName
    FROM DRRoutingStep
    WHERE deviation_number IN (
        SELECT deviation_number 
        FROM DRItemSnapshot 
        ORDER BY run_id DESC 
        LIMIT 5
    )
    ORDER BY deviation_number, DateTouched DESC
""")

print("\n1. Sample of DRRoutingStep records:")
print("-" * 80)
for row in cur.fetchall():
    comment = row['UserComments']
    print(f"DR: {row['deviation_number']}")
    print(f"  Date: {row['DateTouched']}")
    print(f"  User: {row['UserName']}")
    print(f"  Comment IS NULL: {comment is None}")
    print(f"  Comment value: {repr(comment)}")
    print(f"  Comment length: {len(comment) if comment else 0}")
    print()

# Check how many have NULL vs empty string vs content
cur.execute("""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN UserComments IS NULL THEN 1 ELSE 0 END) as null_count,
        SUM(CASE WHEN UserComments IS NOT NULL AND UserComments = '' THEN 1 ELSE 0 END) as empty_string_count,
        SUM(CASE WHEN UserComments IS NOT NULL AND UserComments != '' THEN 1 ELSE 0 END) as has_content_count
    FROM DRRoutingStep
""")

row = cur.fetchone()
print("\n2. Comment statistics:")
print("-" * 80)
print(f"Total records: {row['total']}")
print(f"NULL comments: {row['null_count']}")
print(f"Empty string comments: {row['empty_string_count']}")
print(f"Comments with content: {row['has_content_count']}")

# Check if "sent to sheet shop" comments exist
cur.execute("""
    SELECT deviation_number, UserComments, DateTouched, UserName
    FROM DRRoutingStep
    WHERE UserComments LIKE '%sent to sheet shop%'
    ORDER BY DateTouched DESC
    LIMIT 5
""")

print("\n3. Recent 'sent to sheet shop' comments:")
print("-" * 80)
rows = cur.fetchall()
if rows:
    for row in rows:
        print(f"DR: {row['deviation_number']}, Date: {row['DateTouched']}")
        print(f"  User: {row['UserName']}")
        print(f"  Comment: {row['UserComments']}")
        print()
else:
    print("NO 'sent to sheet shop' comments found!")

# Test the actual query being used
print("\n4. Testing actual API query logic:")
print("-" * 80)
cur.execute("""
    WITH LatestRouting AS (
        SELECT deviation_number, MAX(DateTouched) as latest_date
        FROM DRRoutingStep
        WHERE UserComments IS NOT NULL
        GROUP BY deviation_number
    )
    SELECT COUNT(*) as count FROM LatestRouting
""")
count1 = cur.fetchone()['count']
print(f"DRs with comments (IS NOT NULL): {count1}")

cur.execute("""
    WITH LatestRouting AS (
        SELECT deviation_number, MAX(DateTouched) as latest_date
        FROM DRRoutingStep
        WHERE UserComments IS NOT NULL AND TRIM(UserComments) != ''
        GROUP BY deviation_number
    )
    SELECT COUNT(*) as count FROM LatestRouting
""")
count2 = cur.fetchone()['count']
print(f"DRs with comments (NOT NULL AND NOT EMPTY): {count2}")

conn.close()
print("\n" + "=" * 80)
print("DIAGNOSTIC COMPLETE")
print("=" * 80)
