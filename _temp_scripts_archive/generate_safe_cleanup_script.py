"""
Intelligently generate a cleanup script by analyzing actual imports and usage.
This prevents accidentally archiving production scripts.
"""
import os
from pathlib import Path
from typing import Set, Dict, List

def scan_python_files(workspace: Path) -> Dict[str, str]:
    """Get all Python files and their content."""
    files = {}
    for py_file in workspace.glob('*.py'):
        if py_file.name.startswith('.'):
            continue
        try:
            with open(py_file, 'r', encoding='utf-8') as f:
                files[py_file.name] = f.read()
        except:
            pass
    return files

def find_imports(content: str) -> Set[str]:
    """Find all local module imports in a file."""
    imports = set()
    for line in content.split('\n'):
        line = line.strip()
        if line.startswith('import '):
            module = line.replace('import ', '').split()[0]
            imports.add(module)
        elif line.startswith('from ') and ' import ' in line:
            module = line.split('from ')[1].split(' import')[0]
            imports.add(module)
    return imports

def build_dependency_graph(files: Dict[str, str]) -> Dict[str, Set[str]]:
    """Build a graph of which files import which modules."""
    graph = {}
    for filename, content in files.items():
        imports = find_imports(content)
        # Filter to only local modules (files that exist in our workspace)
        local_imports = {imp for imp in imports if f"{imp}.py" in files}
        graph[filename] = local_imports
    return graph

def identify_production_files(files: Dict[str, str]) -> Set[str]:
    """Identify files that are clearly production code."""
    production = set()
    
    # Known production files
    known_production = {
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
    }
    
    production.update(known_production)
    
    # Files with "app" in name are likely production
    for filename in files:
        if 'app.py' in filename or '_app.py' in filename:
            production.add(filename)
    
    # Files in certain patterns
    for filename in files:
        if filename.startswith('run_') or filename.startswith('init_'):
            production.add(filename)
    
    return production

def find_all_dependencies(file: str, graph: Dict[str, Set[str]], visited: Set[str] = None) -> Set[str]:
    """Recursively find all dependencies of a file."""
    if visited is None:
        visited = set()
    
    if file in visited:
        return visited
    
    visited.add(file)
    
    if file in graph:
        for dep in graph[file]:
            dep_file = f"{dep}.py"
            find_all_dependencies(dep_file, graph, visited)
    
    return visited

def main():
    workspace = Path(r'C:\Project p\SQRS')
    
    print("=" * 70)
    print("INTELLIGENT CLEANUP SCRIPT GENERATOR")
    print("=" * 70)
    print()
    
    # Scan all Python files
    print("[1/5] Scanning Python files...")
    files = scan_python_files(workspace)
    print(f"Found {len(files)} Python files")
    print()
    
    # Build dependency graph
    print("[2/5] Building dependency graph...")
    graph = build_dependency_graph(files)
    print("Dependency graph built")
    print()
    
    # Identify production files
    print("[3/5] Identifying production files...")
    production = identify_production_files(files)
    print(f"Found {len(production)} known production files:")
    for f in sorted(production):
        print(f"  - {f}")
    print()
    
    # Find all transitive dependencies of production files
    print("[4/5] Finding all dependencies of production files...")
    all_production_deps = set()
    for prod_file in production:
        deps = find_all_dependencies(prod_file, graph)
        all_production_deps.update(deps)
    
    # Add the .py extension back
    all_production_with_ext = {f"{f}.py" if not f.endswith('.py') else f for f in all_production_deps}
    all_production_with_ext.update(production)
    
    print(f"Total files needed by production (including dependencies): {len(all_production_with_ext)}")
    for f in sorted(all_production_with_ext):
        if f in files and f not in production:
            print(f"  - {f} (dependency)")
    print()
    
    # Everything else can be archived
    print("[5/5] Identifying archivable files...")
    archivable = set(files.keys()) - all_production_with_ext
    
    # Categorize archivable files
    migrations = {f for f in archivable if 'migrate' in f}
    checks = {f for f in archivable if f.startswith('check_') or f.startswith('verify_') or f.startswith('show_')}
    investigations = {f for f in archivable if '49111' in f or 'inspect' in f or 'debug' in f or 'serialize' in f or 'analyze' in f or 'extract' in f or 'find_drs' in f or 'deep_' in f}
    obsolete = {f for f in archivable if 'poll_drs_incremental' in f or 'nuke' in f or 'explain' in f or 'test_' in f or 'list_all' in f}
    other = archivable - migrations - checks - investigations - obsolete
    
    print(f"\nArchivable files: {len(archivable)}")
    print(f"  - Migrations: {len(migrations)}")
    print(f"  - Checks/Verifications: {len(checks)}")
    print(f"  - Investigations: {len(investigations)}")
    print(f"  - Obsolete: {len(obsolete)}")
    print(f"  - Other: {len(other)}")
    print()
    
    if other:
        print("⚠️  REVIEW NEEDED - Uncategorized archivable files:")
        for f in sorted(other):
            print(f"  - {f}")
        print()
    
    print("=" * 70)
    print("SAFETY CHECK RESULTS")
    print("=" * 70)
    print()
    print(f"✅ All {len(all_production_with_ext)} production files and their dependencies are protected")
    print(f"✅ {len(archivable)} files identified as safe to archive")
    print()
    
    # Double-check: verify no production file imports an archivable file
    print("Final verification: Checking for any missed imports...")
    problems = []
    for prod_file in all_production_with_ext:
        if prod_file not in files:
            continue
        content = files[prod_file]
        imports = find_imports(content)
        for imp in imports:
            imp_file = f"{imp}.py"
            if imp_file in archivable:
                problems.append((prod_file, imp_file))
    
    if problems:
        print("❌ FOUND PROBLEMS:")
        for prod, arch in problems:
            print(f"  {prod} imports {arch}")
        print("\nDO NOT ARCHIVE - manual review required!")
    else:
        print("✅ No problems found - safe to archive")
        
        print()
        print("Recommended KEEP_SCRIPTS:")
        print("KEEP_SCRIPTS = {")
        for f in sorted(all_production_with_ext):
            # Get first line of docstring as comment
            comment = "Production script"
            if f in files:
                lines = files[f].split('\n')
                for line in lines[1:10]:  # Check first 10 lines
                    if '"""' in line or "'''" in line:
                        comment = line.strip('"\' ')
                        break
            print(f"    '{f}',  # {comment[:50]}")
        print("}")

if __name__ == '__main__':
    main()
