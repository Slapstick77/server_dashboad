"""
Smart cleanup script with built-in verification.
Analyzes imports, shows you what will be moved, and requires confirmation.
"""
import os
import shutil
from pathlib import Path
from typing import Set, Dict

def scan_imports(workspace: Path, filename: str) -> Set[str]:
    """Find all local module imports in a file."""
    filepath = workspace / filename
    if not filepath.exists():
        return set()
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except:
        return set()
    
    imports = set()
    for line in content.split('\n'):
        line = line.strip()
        if line.startswith('import '):
            module = line.replace('import ', '').split()[0]
            imports.add(f"{module}.py")
        elif line.startswith('from ') and ' import ' in line:
            module = line.split('from ')[1].split(' import')[0].split('.')[0]
            imports.add(f"{module}.py")
    return imports

def main():
    workspace = Path(r'C:\Project p\SQRS')
    archive_dir = workspace / '_temp_scripts_archive'
    
    # Known production files
    production_files = [
        'dr_schema.py',
        'dr_ingest.py',
        'run_poll_with_history_and_ingest.py',
        'report_update_service.py',
        'metrics_cache.py',
        'init_metrics_cache.py',
        'desktop_sync_app.py',
        'new_data_sync_app.py',
        'clean.py',
    ]
    
    print("=" * 70)
    print("SMART CLEANUP - WITH VERIFICATION")
    print("=" * 70)
    print()
    
    # Step 1: Find all Python files
    all_py_files = {f.name for f in workspace.glob('*.py')}
    
    # Step 2: Check what production files import
    print("[1/4] Analyzing production file dependencies...")
    protected_files = set(production_files)
    
    for prod_file in production_files:
        imports = scan_imports(workspace, prod_file)
        protected_files.update(imports)
    
    print(f"Protected files (production + dependencies): {len(protected_files)}")
    print()
    
    # Step 3: Determine what can be archived
    archivable = all_py_files - protected_files - {'cleanup.py'}  # Don't archive this script
    
    print("[2/4] Files to be ARCHIVED:")
    if archivable:
        for f in sorted(archivable):
            print(f"  - {f}")
    else:
        print("  (none)")
    print()
    
    print("[2/4] Files to be KEPT (production):")
    for f in sorted(protected_files):
        if f in all_py_files:
            print(f"  - {f}")
    print()
    
    # Step 4: Safety check - verify no imports
    print("[3/4] Safety check: verifying no production files import archived scripts...")
    problems = []
    for prod_file in protected_files:
        imports = scan_imports(workspace, prod_file)
        for imp in imports:
            if imp in archivable:
                problems.append((prod_file, imp))
    
    if problems:
        print("❌ FAILED: Found import dependencies!")
        for prod, arch in problems:
            print(f"  {prod} imports {arch}")
        print("\n⚠️  ABORTED: Cannot safely archive these files!")
        return
    else:
        print("✅ PASSED: No import conflicts found")
    print()
    
    # Step 5: User confirmation
    if not archivable:
        print("Nothing to archive. Exiting.")
        return
    
    print("[4/4] Confirmation required")
    print(f"Archive {len(archivable)} scripts to '_temp_scripts_archive/'?")
    response = input("Type 'yes' to proceed: ").strip().lower()
    
    if response != 'yes':
        print("⚠️  ABORTED: User cancelled")
        return
    
    print()
    print("=" * 70)
    print("ARCHIVING FILES")
    print("=" * 70)
    
    # Create archive directory
    archive_dir.mkdir(exist_ok=True)
    
    moved = 0
    for script_name in archivable:
        script_path = workspace / script_name
        if script_path.exists():
            dest_path = archive_dir / script_name
            shutil.move(str(script_path), str(dest_path))
            print(f"✓ Archived: {script_name}")
            moved += 1
    
    print()
    print("=" * 70)
    print(f"Complete: {moved} files archived")
    print("=" * 70)

if __name__ == '__main__':
    main()
