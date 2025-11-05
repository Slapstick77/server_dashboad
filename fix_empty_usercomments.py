"""
Fix empty string UserComments in DRRoutingStep table.

This script updates any UserComments that are empty strings ('') to NULL
so that the latest comment query works correctly.

Run this once on the server database to fix existing data.
"""

import sqlite3
import os

ROOT = os.path.dirname(__file__)
DB_PATH = os.path.join(ROOT, 'SCHLabor.db')

def fix_empty_comments():
    """Replace empty string UserComments with NULL"""
    
    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found: {DB_PATH}")
        return
    
    print(f"Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Count empty string comments
    cur.execute("SELECT COUNT(*) FROM DRRoutingStep WHERE UserComments = ''")
    count_before = cur.fetchone()[0]
    
    print(f"\nFound {count_before} rows with empty string UserComments")
    
    if count_before == 0:
        print("✓ No empty strings found - database is clean!")
        conn.close()
        return
    
    # Update empty strings to NULL
    print(f"\nUpdating {count_before} rows...")
    cur.execute("UPDATE DRRoutingStep SET UserComments = NULL WHERE UserComments = ''")
    conn.commit()
    
    # Verify
    cur.execute("SELECT COUNT(*) FROM DRRoutingStep WHERE UserComments = ''")
    count_after = cur.fetchone()[0]
    
    print(f"\n✓ Update complete!")
    print(f"  Empty strings remaining: {count_after}")
    print(f"  Rows fixed: {count_before - count_after}")
    
    # Show some examples of latest comments now
    print("\n" + "="*80)
    print("Sample of latest comments (should now show real comments):")
    print("="*80)
    
    cur.execute("""
        WITH LatestRouting AS (
            SELECT deviation_number, MAX(DateTouched) as latest_date
            FROM DRRoutingStep
            WHERE UserComments IS NOT NULL
            GROUP BY deviation_number
        )
        SELECT r.deviation_number, r.UserComments, r.UserName, r.DateTouched
        FROM DRRoutingStep r
        INNER JOIN LatestRouting l ON r.deviation_number = l.deviation_number 
            AND r.DateTouched = l.latest_date
        ORDER BY r.DateTouched DESC
        LIMIT 10
    """)
    
    for row in cur.fetchall():
        dr_num, comment, user, date = row
        comment_preview = (comment or '')[:60]
        print(f"DR{dr_num}: {comment_preview}...")
        print(f"  By: {user} at {date}")
    
    conn.close()
    print("\n✓ Database fix complete!")

if __name__ == "__main__":
    fix_empty_comments()
