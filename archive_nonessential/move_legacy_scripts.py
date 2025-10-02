"""One-time maintenance script to move legacy / diagnostic scripts into archive_scripts/.

Keeps only the active runtime files:
  - desktop_sync_app.py
  - report_update_service.py
  - clean.py
  - webapp/app.py
  - config.json (not a .py)

Usage (from project root):
  python move_legacy_scripts.py  # dry run (shows what will move)
  python move_legacy_scripts.py --apply  # actually move

Safe: creates archive_scripts/ (if missing) and moves files there preserving content.
Won't overwrite existing copies (adds numeric suffix if needed).
"""
from __future__ import annotations
import os, shutil, sys

ROOT = os.path.dirname(__file__)
ARCHIVE = os.path.join(ROOT, 'archive_scripts')
KEEP = {
    'desktop_sync_app.py',
    'report_update_service.py',
    'clean.py',
    'move_legacy_scripts.py',  # itself
    'config.json',
}
WEBAPP_KEEP = {'app.py'}  # inside webapp/

def classify() -> list[str]:
    movers: list[str] = []
    for name in os.listdir(ROOT):
        if name.startswith('.'):
            continue
        path = os.path.join(ROOT, name)
        if os.path.isdir(path):
            # webapp special-case
            if name == 'webapp':
                for wn in os.listdir(path):
                    if wn.endswith('.py') and wn not in WEBAPP_KEEP:
                        movers.append(os.path.join('webapp', wn))
            continue
        if not name.endswith('.py'):
            continue
        if name in KEEP:
            continue
        movers.append(name)
    return sorted(movers)

def ensure_archive():
    os.makedirs(ARCHIVE, exist_ok=True)

def unique_dest(base_name: str) -> str:
    dest = os.path.join(ARCHIVE, base_name)
    if not os.path.exists(dest):
        return dest
    stem, dot, ext = base_name.partition('.')
    i = 1
    while True:
        candidate = os.path.join(ARCHIVE, f"{stem}_{i}{dot}{ext}" if dot else f"{stem}_{i}")
        if not os.path.exists(candidate):
            return candidate
        i += 1

def move_files(files: list[str], apply: bool):
    if not files:
        print('No legacy scripts found to move.')
        return
    print(f"Found {len(files)} legacy scripts:")
    for f in files:
        print('  ', f)
    if not apply:
        print('\nDry run. Re-run with --apply to move files.')
        return
    ensure_archive()
    moved = 0
    for rel in files:
        src = os.path.join(ROOT, rel)
        if not os.path.isfile(src):
            continue
        base = os.path.basename(rel)
        dest = unique_dest(base)
        shutil.move(src, dest)
        moved += 1
        print(f"Moved {rel} -> {os.path.relpath(dest, ROOT)}")
    print(f"Done. Moved {moved} file(s) into {os.path.relpath(ARCHIVE, ROOT)}")

def main():
    apply = '--apply' in sys.argv
    files = classify()
    move_files(files, apply)

if __name__ == '__main__':
    main()
