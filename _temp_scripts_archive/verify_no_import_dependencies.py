"""
Verify that archived scripts are not imported by production code.
"""
import os
from pathlib import Path

def main():
    workspace = Path(r'C:\Project p\SQRS')
    archive_dir = workspace / '_temp_scripts_archive'
    
    # Get all archived script names (without .py)
    archived_modules = set()
    if archive_dir.exists():
        for script in archive_dir.glob('*.py'):
            archived_modules.add(script.stem)
    
    print(f"Found {len(archived_modules)} archived scripts")
    print()
    
    # Get all production Python files
    production_files = [
        'dr_schema.py',
        'dr_ingest.py',
        'run_poll_with_history_and_ingest.py',
        'report_update_service.py',
        'backfill_dr_metadata.py',
        'metrics_cache.py',
        'init_metrics_cache.py',
        'desktop_sync_app.py',
        'new_data_sync_app.py',
        'clean.py',
    ]
    
    # Check for imports
    problems = []
    
    for prod_file in production_files:
        prod_path = workspace / prod_file
        if not prod_path.exists():
            continue
            
        with open(prod_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        for module in archived_modules:
            # Check for various import patterns
            patterns = [
                f'import {module}',
                f'from {module} import',
                f'from {module}.',
            ]
            
            for pattern in patterns:
                if pattern in content:
                    problems.append({
                        'file': prod_file,
                        'imports': module,
                        'pattern': pattern
                    })
    
    if problems:
        print("⚠️  WARNING: Found references to archived scripts in production code!")
        print()
        for p in problems:
            print(f"  {p['file']} -> {p['imports']}")
            print(f"    Pattern: {p['pattern']}")
        print()
        print("These scripts should NOT be archived!")
    else:
        print("✅ No production code imports archived scripts")
        print()
        print("Safe to archive:")
        for module in sorted(archived_modules):
            print(f"  - {module}.py")

if __name__ == '__main__':
    main()
