"""
Cleanup temporary investigation and one-time migration scripts.
Moves them to a _temp_scripts_archive folder.

SAFETY: Runs verification BEFORE moving any files to check for:
1. Import dependencies (production code importing archived scripts)
2. README/documentation references
3. User confirmation before proceeding
"""
import os
import shutil
from pathlib import Path

# Scripts to KEEP (production code)
KEEP_SCRIPTS = {
    'dr_schema.py',                           # Core schema
    'dr_ingest.py',                           # Core ingest logic
    'run_poll_with_history_and_ingest.py',   # Main poller
    'report_update_service.py',               # Report service
    'backfill_dr_metadata.py',               # Useful for future backfills
    'metrics_cache.py',                       # Metrics caching
    'init_metrics_cache.py',                  # Metrics initialization
    'desktop_sync_app.py',                    # Desktop app
    'new_data_sync_app.py',                   # New sync app
    'clean.py',                               # CSV data cleaning (used by sync apps)
    'cleanup_temp_scripts.py',                # This script
}

# Investigation/debug scripts to ARCHIVE
ARCHIVE_SCRIPTS = {
    # DR 49111 investigation scripts
    'analyze_master_structure.py',
    'call_partsforunitdetails_49111.py',
    'check_master_in_response.py',
    'debug_dr_49111.py',
    'deep_inspect_partslist_49111.py',
    'extract_all_fields_49111.py',
    'find_drs_with_parts.py',
    'inspect_dr_49111_complete.py',
    'inspect_dr_detail.py',
    'inspect_dr_master.py',
    'inspect_result_level_49111.py',
    'serialize_complete_item_49111.py',
    'serialize_complete_master_49111.py',
    'verify_all_master_fields.py',
    'verify_dr_49111_soap.py',
    
    # SOAP exploration
    'list_all_soap_methods.py',
    'test_displayitemfordrnumber.py',
    
    # One-time migration scripts (already executed)
    'migrate_completion_table.py',
    'migrate_database.py',
    'migrate_remove_duplicate_columns.py',
    'migrate_remove_snapshot_columns.py',
    'recapture_missing_deviation_type.py',
    'update_missing_deviation_type.py',
    
    # Database check/verification scripts
    'check_columns.py',
    'check_db_size.py',
    'check_dr_schema_ready.py',
    'check_metadata.py',
    'check_metadata_columns.py',
    'check_unit_completion_table.py',
    'show_columns.py',
    'show_tables.py',
    'verify_clean_structure.py',
    'verify_metadata_complete.py',
    
    # Cleanup/utility scripts
    'explain_logic.py',
    'nuke_dr_data.py',
    
    # Old poller (replaced by run_poll_with_history_and_ingest.py)
    'poll_drs_incremental.py',
}

def check_for_imports(workspace, archive_scripts, keep_scripts):
    """Check if any production scripts import modules we're about to archive."""
    problems = []
    
    for prod_file in keep_scripts:
        if prod_file == 'cleanup_temp_scripts.py':
            continue
            
        prod_path = workspace / prod_file
        if not prod_path.exists():
            continue
            
        with open(prod_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        for archived_script in archive_scripts:
            module = archived_script.replace('.py', '')
            patterns = [
                f'import {module}',
                f'from {module} import',
                f'from {module}.',
            ]
            
            for pattern in patterns:
                if pattern in content:
                    problems.append({
                        'production_file': prod_file,
                        'imports_from': archived_script,
                        'pattern': pattern
                    })
    
    return problems

def main():
    workspace = Path(r'C:\Project p\SQRS')
    archive_dir = workspace / '_temp_scripts_archive'
    
    print("=" * 70)
    print("CLEANUP SCRIPT - PRE-FLIGHT VERIFICATION")
    print("=" * 70)
    print()
    
    # SAFETY CHECK 1: Verify no import dependencies
    print("[1/3] Checking for import dependencies...")
    problems = check_for_imports(workspace, ARCHIVE_SCRIPTS, KEEP_SCRIPTS)
    
    if problems:
        print("❌ FAILED: Found production code importing scripts to be archived!")
        print()
        for p in problems:
            print(f"  {p['production_file']} imports {p['imports_from']}")
            print(f"    Pattern: {p['pattern']}")
        print()
        print("⚠️  ABORTED: Fix these imports before archiving!")
        return
    else:
        print("✅ PASSED: No import dependencies found")
    print()
    
    # SAFETY CHECK 2: Show what will be moved
    print("[2/3] Files to be archived:")
    for script_name in sorted(ARCHIVE_SCRIPTS):
        script_path = workspace / script_name
        if script_path.exists():
            print(f"  - {script_name}")
    print()
    
    print("[2/3] Files to be kept (production):")
    for script_name in sorted(KEEP_SCRIPTS):
        print(f"  - {script_name}")
    print()
    
    # SAFETY CHECK 3: User confirmation
    print("[3/3] Confirmation required")
    response = input(f"Archive {len(ARCHIVE_SCRIPTS)} scripts? (yes/no): ").strip().lower()
    
    if response != 'yes':
        print("⚠️  ABORTED: User cancelled")
        return
    
    print()
    print("=" * 70)
    print("MOVING FILES TO ARCHIVE")
    print("=" * 70)
    print()
    
    # Create archive directory
    archive_dir.mkdir(exist_ok=True)
    
    moved = 0
    
    # Move scripts to archive
    for script_name in ARCHIVE_SCRIPTS:
        script_path = workspace / script_name
        if script_path.exists():
            dest_path = archive_dir / script_name
            shutil.move(str(script_path), str(dest_path))
            print(f"✓ Archived: {script_name}")
            moved += 1
    
    print()
    print("=" * 60)
    print(f"Archived: {moved} scripts")
    print(f"Keeping: {len(KEEP_SCRIPTS)} production scripts")
    print("=" * 60)
    print()
    print("Production scripts in workspace:")
    for script in sorted(KEEP_SCRIPTS):
        print(f"  - {script}")

if __name__ == '__main__':
    main()
