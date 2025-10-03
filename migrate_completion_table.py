"""
Migrate UnitCompletion table to new schema.
Run this once to update the table structure.
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'SCHLabor.db')

def migrate():
    print("Migrating UnitCompletion table...")
    
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        
        # Drop old table
        cur.execute("DROP TABLE IF EXISTS UnitCompletion")
        print("✓ Dropped old UnitCompletion table")
        
        # Drop old indexes
        cur.execute("DROP INDEX IF EXISTS idx_unit_completion_status")
        cur.execute("DROP INDEX IF EXISTS idx_unit_completion_last_day")
        print("✓ Dropped old indexes")
        
        # Create new table (ONLY for 100% complete units)
        cur.execute("""
            CREATE TABLE UnitCompletion (
                com_number TEXT PRIMARY KEY,
                last_day_unfiltered TEXT NOT NULL,
                last_updated TEXT NOT NULL
            )
        """)
        print("✓ Created new UnitCompletion table")
        
        # Create new index
        cur.execute("""
            CREATE INDEX idx_unit_completion_last_day 
            ON UnitCompletion(last_day_unfiltered)
        """)
        print("✓ Created index on last_day_unfiltered")
        
        conn.commit()
    
    print("\n✅ Migration complete!")
    print("Now run: python -c \"from metrics_cache import refresh_unit_completion; refresh_unit_completion()\"")
    print("Or refresh from the Tasks page in the web UI")

if __name__ == '__main__':
    migrate()
