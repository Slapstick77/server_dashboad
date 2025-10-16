"""
Migration: Remove urgency, deviation_type, defect, part_number from DRItemSnapshot.
These fields belong only in DRStaticMetadata (static data that never changes).
DRItemSnapshot should only contain snapshot data that can change over time.
"""
import sqlite3

DB = 'SCHLabor.db'

def migrate():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    
    print('[MIGRATE] Removing unwanted columns from DRItemSnapshot...')
    
    # SQLite doesn't support DROP COLUMN easily, so we recreate the table
    # 1. Create new table with correct schema
    cur.execute("""
        CREATE TABLE DRItemSnapshot_new (
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
    """)
    
    # 2. Copy data (excluding the unwanted columns)
    cur.execute("""
        INSERT INTO DRItemSnapshot_new
        SELECT 
            id, run_id, deviation_number, current_routing, deviation_state,
            is_closed, product, sales_order_number, sales_order_number_normalized,
            comnumber1, creation_comments, latest_routing_department, 
            latest_routing_user, latest_routing_state, latest_routing_touched,
            latest_routing_comment, latest_non_empty_routing_comment, 
            routing_step_count, updated, updated_routing, updated_comment
        FROM DRItemSnapshot
    """)
    
    # 3. Drop view first (it references the table)
    cur.execute("DROP VIEW IF EXISTS V_DRSnapshot_WithSched")
    
    # 4. Drop old table
    cur.execute("DROP TABLE DRItemSnapshot")
    
    # 5. Rename new table
    cur.execute("ALTER TABLE DRItemSnapshot_new RENAME TO DRItemSnapshot")
    
    # 6. Recreate indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drsnap_run ON DRItemSnapshot(run_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drsnap_dev ON DRItemSnapshot(deviation_number)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drsnap_updated ON DRItemSnapshot(updated, updated_routing, updated_comment)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drsnap_salesnorm ON DRItemSnapshot(sales_order_number_normalized)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_drsnap_run_dev ON DRItemSnapshot(run_id, deviation_number)")
    
    # 7. Recreate view if SCHSchedulingSummary exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='SCHSchedulingSummary'")
    if cur.fetchone():
        cur.execute("DROP VIEW IF EXISTS V_DRSnapshot_WithSched")
        cur.execute("""
            CREATE VIEW V_DRSnapshot_WithSched AS
            SELECT d.*, s.comnumber1 AS sched_comnumber1, s.contractnumber AS sched_contractnumber
            FROM DRItemSnapshot d
            LEFT JOIN SCHSchedulingSummary s
              ON REPLACE(s.contractnumber, '-', '') = d.sales_order_number_normalized
        """)
    
    conn.commit()
    
    # Verify
    cur.execute("PRAGMA table_info(DRItemSnapshot)")
    cols = [r[1] for r in cur.fetchall()]
    
    unwanted = ['urgency', 'deviation_type', 'defect', 'part_number']
    remaining = [c for c in unwanted if c in cols]
    
    if remaining:
        print(f'❌ FAILED: Columns still present: {remaining}')
    else:
        print(f'✅ SUCCESS: Removed all unwanted columns')
        print(f'   DRItemSnapshot now has {len(cols)} columns (should be 21)')
    
    conn.close()

if __name__ == '__main__':
    migrate()
