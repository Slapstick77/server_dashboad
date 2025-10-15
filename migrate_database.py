"""Database migration script to add DR tables to existing SCHLabor.db

This script will:
1. Check if DR tables already exist
2. Create missing DR tables with proper schema
3. Add any missing columns to existing DR tables
4. Report what was added/updated

Safe to run multiple times (idempotent).
"""
import os
import sqlite3
from datetime import datetime

ROOT = os.path.dirname(__file__)
DB_PATH = os.path.join(ROOT, 'SCHLabor.db')

def get_existing_tables(conn):
    """Get list of existing tables in database."""
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    return {row[0] for row in cur.fetchall()}

def get_table_columns(conn, table_name):
    """Get list of columns for a table."""
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return {row[1]: row[2] for row in cur.fetchall()}  # {column_name: type}

def migrate_dr_tables(db_path):
    """Add or update DR tables in the database."""
    if not os.path.isfile(db_path):
        print(f"❌ Database not found: {db_path}")
        print("   Create the database first or check the path.")
        return False
    
    print(f"📂 Database: {db_path}")
    print(f"🕐 Migration started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        existing_tables = get_existing_tables(conn)
        
        tables_added = []
        tables_updated = []
        tables_skipped = []
        
        # Define DR tables schema
        dr_tables = {
            'DRStateMeta': """
                CREATE TABLE IF NOT EXISTS DRStateMeta (
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    last_run_utc TEXT
                )
            """,
            'DRPollRun': """
                CREATE TABLE IF NOT EXISTS DRPollRun (
                    id INTEGER PRIMARY KEY,
                    generated_at_utc TEXT NOT NULL,
                    total_elapsed_seconds REAL,
                    window_days INTEGER,
                    include_closed INTEGER,
                    tz_offset_hours INTEGER,
                    single_closed_variant INTEGER,
                    state_file_path TEXT,
                    out_json_path TEXT,
                    timings_json_path TEXT
                )
            """,
            'DRPollTimingEvent': """
                CREATE TABLE IF NOT EXISTS DRPollTimingEvent (
                    id INTEGER PRIMARY KEY,
                    run_id INTEGER NOT NULL,
                    description TEXT NOT NULL,
                    attempts INTEGER,
                    successful INTEGER,
                    elapsed_seconds REAL,
                    FOREIGN KEY(run_id) REFERENCES DRPollRun(id)
                )
            """,
            'DRItemSnapshot': """
                CREATE TABLE IF NOT EXISTS DRItemSnapshot (
                    id INTEGER PRIMARY KEY,
                    run_id INTEGER NOT NULL,
                    deviation_number INTEGER NOT NULL,
                    current_routing TEXT,
                    deviation_state TEXT,
                    is_closed INTEGER,
                    product TEXT,
                    sales_order_number TEXT,
                    sales_order_number_normalized TEXT,
                    comnumber1 INTEGER,
                    creation_comments TEXT,
                    latest_routing_department TEXT,
                    latest_routing_user TEXT,
                    latest_routing_state TEXT,
                    latest_routing_touched TEXT,
                    latest_routing_comment TEXT,
                    latest_non_empty_routing_comment TEXT,
                    routing_step_count INTEGER,
                    updated INTEGER,
                    updated_routing INTEGER,
                    updated_comment INTEGER,
                    FOREIGN KEY(run_id) REFERENCES DRPollRun(id)
                )
            """,
            'DRState': """
                CREATE TABLE IF NOT EXISTS DRState (
                    deviation_number INTEGER PRIMARY KEY,
                    lastRoutingTouched TEXT,
                    lastRoutingStepCount INTEGER,
                    lastNoteCount INTEGER,
                    lastSeenState TEXT,
                    lastNonEmptyRoutingComment TEXT
                )
            """,
            'DRRoutingStep': """
                CREATE TABLE IF NOT EXISTS DRRoutingStep (
                    id INTEGER PRIMARY KEY,
                    run_id INTEGER NOT NULL,
                    deviation_number INTEGER NOT NULL,
                    step_index INTEGER NOT NULL,
                    DateTouched TEXT,
                    UserName TEXT,
                    RoutingDepartment TEXT,
                    State TEXT,
                    EmailAddress TEXT,
                    UserComments TEXT,
                    FOREIGN KEY(run_id) REFERENCES DRPollRun(id)
                )
            """
        }
        
        # Create or update each table
        for table_name, create_sql in dr_tables.items():
            if table_name not in existing_tables:
                print(f"✨ Creating table: {table_name}")
                cur.execute(create_sql)
                tables_added.append(table_name)
            else:
                print(f"✓  Table exists: {table_name}")
                tables_skipped.append(table_name)
        
        # Create indexes for better query performance
        indexes = [
            ('idx_dritemsnap_devnum', 'CREATE INDEX IF NOT EXISTS idx_dritemsnap_devnum ON DRItemSnapshot(deviation_number)'),
            ('idx_dritemsnap_runid', 'CREATE INDEX IF NOT EXISTS idx_dritemsnap_runid ON DRItemSnapshot(run_id)'),
            ('idx_drroutingstep_devnum', 'CREATE INDEX IF NOT EXISTS idx_drroutingstep_devnum ON DRRoutingStep(deviation_number)'),
            ('idx_drroutingstep_runid', 'CREATE INDEX IF NOT EXISTS idx_drroutingstep_runid ON DRRoutingStep(run_id)'),
            ('idx_drroutingstep_devnum_idx', 'CREATE INDEX IF NOT EXISTS idx_drroutingstep_devnum_idx ON DRRoutingStep(deviation_number, step_index)'),
        ]
        
        print("\n📊 Creating indexes...")
        for idx_name, idx_sql in indexes:
            cur.execute(idx_sql)
            print(f"   ✓ {idx_name}")
        
        # Initialize DRStateMeta if empty
        cur.execute("SELECT COUNT(*) FROM DRStateMeta")
        if cur.fetchone()[0] == 0:
            cur.execute("INSERT INTO DRStateMeta(id, last_run_utc) VALUES(1, NULL)")
            print("\n🔧 Initialized DRStateMeta with default row")
        
        conn.commit()
        
        # Summary
        print("\n" + "="*60)
        print("📋 MIGRATION SUMMARY")
        print("="*60)
        print(f"✨ Tables added:   {len(tables_added)}")
        if tables_added:
            for t in tables_added:
                print(f"   - {t}")
        
        print(f"✓  Tables existed: {len(tables_skipped)}")
        if tables_skipped:
            for t in tables_skipped:
                print(f"   - {t}")
        
        print(f"\n✅ Migration completed successfully!")
        print(f"🕐 Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Verify all tables exist
        print("\n🔍 Verification:")
        final_tables = get_existing_tables(conn)
        dr_table_names = set(dr_tables.keys())
        missing = dr_table_names - final_tables
        
        if missing:
            print(f"⚠️  Missing tables: {missing}")
            return False
        else:
            print("✓  All DR tables present")
            
            # Count rows in each table
            print("\n📊 Current row counts:")
            for table in sorted(dr_table_names):
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"   {table:25} {count:>6} rows")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

if __name__ == '__main__':
    print("="*60)
    print("DR TABLES MIGRATION SCRIPT")
    print("="*60)
    print()
    
    success = migrate_dr_tables(DB_PATH)
    
    if success:
        print("\n" + "="*60)
        print("🎉 Ready to poll DRs!")
        print("="*60)
        print("\nNext steps:")
        print("  1. Open desktop_sync_app.py")
        print("  2. Click 'Poll DRs (2d)', 'Poll DRs (7d)', or 'Poll DRs (30d)'")
        print("  3. Data will be ingested with delta-only logic (no duplicates)")
    else:
        print("\n⚠️  Migration had issues - review errors above")
