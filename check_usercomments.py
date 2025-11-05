"""
Quick script to check UserComments in DRRoutingStep table
"""
import sqlite3

DB_PATH = 'SCHLabor.db'

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=" * 80)
print("Checking UserComments in DRRoutingStep table")
print("=" * 80)

# Check total routing steps
cur.execute("SELECT COUNT(*) as total FROM DRRoutingStep")
total = cur.fetchone()['total']
print(f"\nTotal routing steps: {total}")

# Check NULL comments
cur.execute("SELECT COUNT(*) as null_count FROM DRRoutingStep WHERE UserComments IS NULL")
null_count = cur.fetchone()['null_count']
print(f"NULL comments: {null_count}")

# Check empty string comments
cur.execute("SELECT COUNT(*) as empty_count FROM DRRoutingStep WHERE UserComments = ''")
empty_count = cur.fetchone()['empty_count']
print(f"Empty string ('') comments: {empty_count}")

# Check non-empty comments
cur.execute("SELECT COUNT(*) as nonempty FROM DRRoutingStep WHERE UserComments IS NOT NULL AND UserComments != ''")
nonempty = cur.fetchone()['nonempty']
print(f"Non-empty comments: {nonempty}")

# Show recent DRs with their latest comment info
print("\n" + "=" * 80)
print("Recent DRs (last 10) and their latest comments:")
print("=" * 80)

sql = '''
    WITH LatestRouting AS (
        SELECT deviation_number, MAX(DateTouched) as latest_date
        FROM DRRoutingStep
        WHERE UserComments IS NOT NULL
        GROUP BY deviation_number
    )
    SELECT 
        d.deviation_number,
        d.latest_date,
        r.UserComments,
        r.UserName,
        CASE 
            WHEN r.UserComments IS NULL THEN 'NULL'
            WHEN r.UserComments = '' THEN 'EMPTY STRING'
            ELSE 'HAS CONTENT'
        END as comment_status
    FROM LatestRouting d
    LEFT JOIN DRRoutingStep r ON d.deviation_number = r.deviation_number 
        AND d.latest_date = r.DateTouched
    ORDER BY d.latest_date DESC
    LIMIT 10
'''

cur.execute(sql)
rows = cur.fetchall()

for row in rows:
    dr_num = row['deviation_number']
    status = row['comment_status']
    comment = row['UserComments'] or '(none)'
    print(f"\nDR{dr_num} [{status}]")
    print(f"  Comment: {comment[:80]}")
    print(f"  By: {row['UserName']} at {row['latest_date']}")

# Check if there are routing steps with empty strings that are newer than non-empty
print("\n" + "=" * 80)
print("Checking for DRs where empty comments are NEWER than real comments:")
print("=" * 80)

sql2 = '''
    WITH EmptyComments AS (
        SELECT deviation_number, MAX(DateTouched) as latest_empty
        FROM DRRoutingStep
        WHERE UserComments = ''
        GROUP BY deviation_number
    ),
    RealComments AS (
        SELECT deviation_number, MAX(DateTouched) as latest_real
        FROM DRRoutingStep
        WHERE UserComments IS NOT NULL AND UserComments != ''
        GROUP BY deviation_number
    )
    SELECT 
        e.deviation_number,
        e.latest_empty,
        r.latest_real
    FROM EmptyComments e
    INNER JOIN RealComments r ON e.deviation_number = r.deviation_number
    WHERE datetime(e.latest_empty) > datetime(r.latest_real)
    ORDER BY e.latest_empty DESC
    LIMIT 10
'''

cur.execute(sql2)
problematic = cur.fetchall()

if problematic:
    print(f"\n⚠️  Found {len(problematic)} DRs where empty comments are newer than real comments!")
    print("These DRs will NOT show 'sent to sheet shop' even if they have the comment:\n")
    for row in problematic:
        print(f"  DR{row['deviation_number']}")
        print(f"    Latest empty comment: {row['latest_empty']}")
        print(f"    Latest real comment:  {row['latest_real']}")
else:
    print("\n✓ No DRs found with this issue")

conn.close()

print("\n" + "=" * 80)
