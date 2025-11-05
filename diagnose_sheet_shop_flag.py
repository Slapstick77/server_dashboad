"""
Diagnostic script to check why 'sent to sheet shop' flag is not showing on server.
This checks the database for recent DRs and their latest comments.
"""

import sqlite3
from datetime import datetime, timedelta

def diagnose_sheet_shop():
    """Check database for DRs that should have the 'sent to sheet shop' flag"""
    
    # Try to connect to the database
    try:
        conn = sqlite3.connect('SCHLabor.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        return
    
    print("=" * 80)
    print("DIAGNOSTIC: 'Sent to Sheet Shop' Flag Issue")
    print("=" * 80)
    
    # Check if tables exist
    print("\n1. Checking database tables...")
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cur.fetchall()]
    
    required_tables = ['DRItemSnapshot', 'DRRoutingStep', 'DRStaticMetadata']
    for table in required_tables:
        if table in tables:
            print(f"   ✓ {table} exists")
        else:
            print(f"   ❌ {table} MISSING!")
            return
    
    # Check last poll run
    print("\n2. Checking last DR poll run...")
    try:
        cur.execute("SELECT generated_at_utc FROM DRPollRun ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        if row and row[0]:
            print(f"   Last poll: {row[0]}")
        else:
            print("   ⚠️  No poll runs found")
    except Exception as e:
        print(f"   ⚠️  DRPollRun table not found or error: {e}")
    
    # Check total DRs in database
    print("\n3. Checking DR counts...")
    cur.execute("SELECT COUNT(DISTINCT deviation_number) FROM DRItemSnapshot")
    total_drs = cur.fetchone()[0]
    print(f"   Total DRs in database: {total_drs}")
    
    # Check DRs with routing steps
    cur.execute("SELECT COUNT(DISTINCT deviation_number) FROM DRRoutingStep")
    drs_with_routing = cur.fetchone()[0]
    print(f"   DRs with routing steps: {drs_with_routing}")
    
    # Check for comments with "sheet shop" pattern
    print("\n4. Searching for 'sheet shop' comments...")
    cur.execute("""
        SELECT deviation_number, UserComments, UserName, DateTouched
        FROM DRRoutingStep
        WHERE UserComments LIKE '%sheet%shop%'
           OR UserComments LIKE '%sheetshop%'
           OR UserComments LIKE '%sheet metal%'
        ORDER BY DateTouched DESC
        LIMIT 10
    """)
    
    sheet_shop_comments = cur.fetchall()
    if sheet_shop_comments:
        print(f"   Found {len(sheet_shop_comments)} comments with 'sheet shop' references:")
        for row in sheet_shop_comments:
            print(f"      DR{row['deviation_number']}: {row['UserComments'][:60]}...")
            print(f"         By: {row['UserName']} at {row['DateTouched']}")
    else:
        print("   ⚠️  NO comments found with 'sheet shop' pattern!")
    
    # Check recent DRs (last 7 days) and their latest comments
    print("\n5. Checking recent DRs (last 7 days) and their latest comments...")
    cutoff = (datetime.now() - timedelta(days=7)).isoformat()
    
    sql = '''
        WITH LatestDRs AS (
            SELECT deviation_number, MAX(run_id) as latest_run
            FROM DRItemSnapshot
            GROUP BY deviation_number
        ),
        LatestRouting AS (
            SELECT deviation_number, MAX(DateTouched) as latest_date
            FROM DRRoutingStep
            WHERE UserComments IS NOT NULL
            GROUP BY deviation_number
        ),
        LatestCommentInfo AS (
            SELECT r.deviation_number, 
                   r.UserName as latest_user, 
                   r.UserComments as latest_comment,
                   r.DateTouched as latest_comment_date
            FROM DRRoutingStep r
            INNER JOIN LatestRouting l ON r.deviation_number = l.deviation_number 
                AND r.DateTouched = l.latest_date
        )
        SELECT 
            d.deviation_number,
            d.current_routing,
            m.date_created,
            lc.latest_comment,
            lc.latest_user,
            lc.latest_comment_date
        FROM DRItemSnapshot d
        INNER JOIN LatestDRs l ON d.deviation_number = l.deviation_number AND d.run_id = l.latest_run
        LEFT JOIN DRStaticMetadata m ON d.deviation_number = m.deviation_number
        LEFT JOIN LatestCommentInfo lc ON d.deviation_number = lc.deviation_number
        WHERE m.date_created IS NOT NULL
          AND datetime(m.date_created) >= datetime(?)
          AND LOWER(d.deviation_state) NOT IN ('complete', 'closed')
        ORDER BY datetime(lc.latest_comment_date) DESC
        LIMIT 10
    '''
    
    cur.execute(sql, (cutoff,))
    recent_drs = cur.fetchall()
    
    if recent_drs:
        print(f"   Found {len(recent_drs)} recent DRs:")
        for row in recent_drs:
            dr_num = row['deviation_number']
            latest_comment = row['latest_comment'] or "(no comment)"
            
            # Check if comment matches completion pattern
            matches_pattern = False
            if row['latest_comment']:
                comment_lower = row['latest_comment'].lower()
                if 'sheet' in comment_lower and ('shop' in comment_lower or 'metal' in comment_lower):
                    if 'complet' in comment_lower or 'sent' in comment_lower:
                        matches_pattern = True
            
            status = "✓ MATCHES" if matches_pattern else "  no match"
            print(f"      {status} DR{dr_num}: {latest_comment[:50]}...")
            if row['latest_comment_date']:
                print(f"              Last touched: {row['latest_comment_date']}")
    else:
        print("   ⚠️  No recent DRs found!")
    
    print("\n6. Summary:")
    print(f"   - Database has {total_drs} DRs with {drs_with_routing} having routing steps")
    print(f"   - {len(sheet_shop_comments)} total 'sheet shop' comments found")
    print(f"   - {len(recent_drs)} recent active DRs")
    
    # Recommendations
    print("\n7. Recommendations:")
    if len(sheet_shop_comments) == 0:
        print("   ⚠️  NO 'sheet shop' comments found - the data may not be syncing from the server")
        print("   → Check if the DR polling service is running")
        print("   → Verify database file location matches between dev and server")
    elif len(recent_drs) == 0:
        print("   ⚠️  No recent DRs found - check the date_created field in DRStaticMetadata")
    else:
        matching_recent = sum(1 for row in recent_drs if row['latest_comment'] and 
                             'sheet' in row['latest_comment'].lower() and 
                             ('shop' in row['latest_comment'].lower() or 'metal' in row['latest_comment'].lower()))
        if matching_recent > 0:
            print(f"   ✓ Found {matching_recent} recent DRs with sheet shop comments")
            print("   → The data appears correct. Check browser cache or JavaScript console for errors")
        else:
            print("   ⚠️  Recent DRs don't have 'sheet shop' comments")
            print("   → The comments may not be getting updated in the database")
    
    conn.close()
    print("\n" + "=" * 80)

if __name__ == "__main__":
    diagnose_sheet_shop()
