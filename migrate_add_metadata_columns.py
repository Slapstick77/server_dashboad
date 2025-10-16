"""Migration: Add deviation_type and component columns to DRStaticMetadata.

This migration adds two new columns that were added to the metadata capture:
- deviation_type: e.g. "Manufactured", "Purchased", "Customer"
- component: e.g. "Wall, Internal", "Damper", "Coil"

Run this on production database before deploying the updated poller/ingest code.
"""
import sqlite3
import os
from datetime import datetime

ROOT = os.path.dirname(__file__)
DB_PATH = os.path.join(ROOT, 'SCHLabor.db')

def migrate():
    """Add new metadata columns if they don't exist."""
    print('='*80)
    print('MIGRATION: Add deviation_type and component to DRStaticMetadata')
    print('='*80)
    print(f'Database: {DB_PATH}')
    print()
    
    if not os.path.exists(DB_PATH):
        print('[ERROR] Database not found')
        return
    
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        
        # Check current schema
        cursor.execute("PRAGMA table_info(DRStaticMetadata)")
        cols = {r[1] for r in cursor.fetchall()}
        
        print(f'Current columns: {len(cols)}')
        for col in sorted(cols):
            print(f'  - {col}')
        print()
        
        # Add missing columns
        changes_made = False
        
        if 'deviation_type' not in cols:
            print('[MIGRATE] Adding column: deviation_type')
            cursor.execute("ALTER TABLE DRStaticMetadata ADD COLUMN deviation_type TEXT")
            changes_made = True
        else:
            print('[SKIP] Column already exists: deviation_type')
        
        if 'component' not in cols:
            print('[MIGRATE] Adding column: component')
            cursor.execute("ALTER TABLE DRStaticMetadata ADD COLUMN component TEXT")
            changes_made = True
        else:
            print('[SKIP] Column already exists: component')
        
        if changes_made:
            conn.commit()
            print()
            print('[SUCCESS] Migration complete')
            
            # Verify final schema
            cursor.execute("PRAGMA table_info(DRStaticMetadata)")
            new_cols = {r[1] for r in cursor.fetchall()}
            print(f'Final columns: {len(new_cols)}')
            for col in sorted(new_cols):
                print(f'  - {col}')
        else:
            print()
            print('[SUCCESS] No changes needed - schema already up to date')
    
    except Exception as e:
        print(f'[ERROR] Migration failed: {e}')
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    migrate()
