"""
Database Migration: Remove duplicate lowercase efficiency/completion columns
Run this ONCE on the server after deploying the updated code.

This script removes the duplicate lowercase columns that have less complete data,
keeping only the uppercase versions (e.g., "Fab Efficiency" instead of "fab_efficiency").
"""
import sqlite3
import os
import sys

def get_db_path():
    """Get the database path, checking common locations."""
    # Try common locations
    possible_paths = [
        os.path.join(os.path.dirname(__file__), 'SCHLabor.db'),
        '/app/SCHLabor.db',  # Common Docker location
        'SCHLabor.db'
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            return path
    
    print("ERROR: Could not find SCHLabor.db")
    print("Please specify the database path as an argument:")
    print("  python migrate_remove_duplicate_columns.py /path/to/SCHLabor.db")
    return None

def migrate_database(db_path):
    """Remove duplicate columns from the database."""
    print(f"Migrating database: {db_path}")
    print("=" * 70)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check SQLite version
    cursor.execute("SELECT sqlite_version()")
    sqlite_version = cursor.fetchone()[0]
    print(f"SQLite version: {sqlite_version}")
    
    version_parts = [int(x) for x in sqlite_version.split('.')]
    if not (version_parts[0] > 3 or (version_parts[0] == 3 and version_parts[1] >= 35)):
        print("\nERROR: SQLite version 3.35.0 or higher required for DROP COLUMN")
        print("Current version does not support this migration.")
        conn.close()
        return False
    
    # List of duplicate lowercase columns to remove
    lowercase_cols_to_remove = [
        'fab_efficiency',
        'fab_completion',
        'welding_efficiency',
        'welding_completion',
        'baseformpaint_efficiency',
        'baseformpaint_completion',
        'fanassytest_efficiency',
        'fanassytest_completion',
        'insulwallfab_efficiency',
        'insulwallfab_completion',
        'doorfab_efficiency',
        'doorfab_completion',
        'electrical_efficiency',
        'electrical_completion',
        'pipe_efficiency',
        'pipe_completion',
        'paint_efficiency',
        'paint_completion',
        'crating_efficiency',
        'crating_completion',
        'assembly_efficiency',
        'assembly_completion'
    ]
    
    # Check current columns
    cursor.execute("PRAGMA table_info(SCHSchedulingSummary)")
    columns_before = cursor.fetchall()
    print(f"\nTotal columns before migration: {len(columns_before)}")
    
    # Check which columns exist
    existing_cols = {col[1] for col in columns_before}
    columns_to_drop = [col for col in lowercase_cols_to_remove if col in existing_cols]
    
    if not columns_to_drop:
        print("\n✓ No duplicate lowercase columns found. Migration not needed.")
        conn.close()
        return True
    
    print(f"\nFound {len(columns_to_drop)} duplicate lowercase columns to remove:")
    for col in columns_to_drop:
        print(f"  - {col}")
    
    # Verify uppercase columns exist before removing duplicates
    required_uppercase = [
        'Fab Efficiency', 'Fab Completion',
        'Welding Efficiency', 'Welding Completion',
        'BaseFormPaint Efficiency', 'BaseFormPaint Completion',
        'FanAssyTest Efficiency', 'FanAssyTest Completion',
        'InsulWallFab Efficiency', 'InsulWallFab Completion',
        'DoorFab Efficiency', 'DoorFab Completion',
        'Electrical Efficiency', 'Electrical Completion',
        'Pipe Efficiency', 'Pipe Completion',
        'Paint Efficiency', 'Paint Completion',
        'Crating Efficiency', 'Crating Completion',
        'Assembly Efficiency', 'Assembly Completion'
    ]
    
    missing_uppercase = [col for col in required_uppercase if col not in existing_cols]
    if missing_uppercase:
        print("\nERROR: Cannot remove lowercase columns - uppercase versions missing:")
        for col in missing_uppercase:
            print(f"  ✗ {col}")
        conn.close()
        return False
    
    print("\n✓ All uppercase columns exist and will be kept")
    
    try:
        # Drop any broken views that might prevent ALTER TABLE
        cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")
        views = cursor.fetchall()
        for (view_name,) in views:
            try:
                cursor.execute(f'DROP VIEW IF EXISTS "{view_name}"')
                print(f"Dropped view: {view_name}")
            except Exception as e:
                print(f"Warning: Could not drop view {view_name}: {e}")
        
        # Begin transaction
        cursor.execute("BEGIN TRANSACTION")
        
        # Drop each duplicate lowercase column
        for col_name in columns_to_drop:
            print(f"Dropping: {col_name}")
            cursor.execute(f'ALTER TABLE SCHSchedulingSummary DROP COLUMN "{col_name}"')
        
        # Commit the transaction
        conn.commit()
        
        # Verify columns were removed
        cursor.execute("PRAGMA table_info(SCHSchedulingSummary)")
        columns_after = cursor.fetchall()
        
        print(f"\n✓ Migration successful!")
        print(f"  Columns before: {len(columns_before)}")
        print(f"  Columns after: {len(columns_after)}")
        print(f"  Removed: {len(columns_before) - len(columns_after)} columns")
        
        # Show remaining efficiency/completion columns
        print("\nRemaining Efficiency and Completion columns:")
        for col in columns_after:
            col_name = col[1]
            if 'Efficiency' in col_name or 'Completion' in col_name:
                print(f"  ✓ {col_name}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"\n✗ Migration failed: {e}")
        print("Rolling back changes...")
        conn.rollback()
        conn.close()
        return False

if __name__ == "__main__":
    print("Database Migration: Remove Duplicate Columns")
    print("=" * 70)
    
    # Get database path from argument or auto-detect
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    else:
        db_path = get_db_path()
    
    if not db_path:
        sys.exit(1)
    
    if not os.path.exists(db_path):
        print(f"ERROR: Database not found at: {db_path}")
        sys.exit(1)
    
    # Run migration
    success = migrate_database(db_path)
    
    if success:
        print("\n" + "=" * 70)
        print("✓ Migration completed successfully!")
        print("\nNext steps:")
        print("  1. Restart your Flask application")
        print("  2. The next data update will use only the uppercase columns")
        sys.exit(0)
    else:
        print("\n" + "=" * 70)
        print("✗ Migration failed. Database unchanged.")
        sys.exit(1)
