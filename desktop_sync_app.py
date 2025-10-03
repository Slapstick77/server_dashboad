"""Desktop Data Sync App (Windows GUI)

Simplified log‑centric UI.

Functions:
    - Labor Backfill (full reprocess with dedupe)
    - Scheduling Summary 120‑Day Upsert (past 60 / next 60 days) with change stats
    - Create Windows Scheduled Tasks (labor+schedule combined)

Depends on: SCHLabor.db, report_update_service.py, clean.py, PowerShell scripts.
Run:  python desktop_sync_app.py
"""
from __future__ import annotations
import os, sqlite3, threading, csv, subprocess, sys, glob, shutil, time, hashlib, re
from datetime import datetime
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

ROOT = os.path.dirname(__file__)
DB_PATH = os.path.join(ROOT, 'SCHLabor.db')

# Attempt to import existing service logic
try:
    import report_update_service as rus  # provides labor_backfill() & update_scheduling_summary()
except Exception as e:
    rus = None
    print('WARNING: report_update_service import failed:', e)

def db_conn():
    return sqlite3.connect(DB_PATH)

def ensure_change_tables():
    """Create RunLog and ChangeLog if absent (so UI can load before any run)."""
    if not os.path.isfile(DB_PATH):
        return
    try:
        with db_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS RunLog (
                  id INTEGER PRIMARY KEY,
                  run_started TEXT NOT NULL,
                  run_completed TEXT,
                  run_type TEXT NOT NULL,
                  success INTEGER DEFAULT 0,
                  message TEXT
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ChangeLog (
                  id INTEGER PRIMARY KEY,
                  run_id INTEGER NOT NULL,
                  comnumber1 INTEGER NOT NULL,
                  column_name TEXT NOT NULL,
                  old_value TEXT,
                  new_value TEXT,
                  FOREIGN KEY(run_id) REFERENCES RunLog(id)
                );
            """)
            conn.commit()
    except Exception:
        pass

# ---------- Parts Tracker Sync (CSV -> SQLite) ---------- #

PARTS_TABLE = 'PartsTracker'

def _sanitize_col(name: str) -> str:
    """Sanitize header to a safe SQLite column name (lower_snake), avoiding reserved issues."""
    if not name:
        return 'col'
    s = re.sub(r'\s+', '_', str(name).strip())
    s = re.sub(r'[^A-Za-z0-9_]', '', s)
    s = re.sub(r'_+', '_', s)
    s = s.strip('_').lower()
    if not s:
        s = 'col'
    if s[0].isdigit():
        s = '_' + s
    return s

def _infer_key_cols(headers: list[str]) -> list[str]:
    """Pick likely key columns by heuristics from headers (original header strings)."""
    hmap = {h.lower(): h for h in headers}
    def has(*alts):
        for a in alts:
            if a in hmap:
                return hmap[a]
        return None
    # Most probable unique IDs
    id_col = has('id','recordid','rowid','#','_id')
    if id_col:
        return [id_col]
    # Part identifiers
    part = has('partnumber','part number','pn','itemnumber','item number','item','sku','stock number','stocknumber')
    rev = has('revision','rev')
    serial = has('serialnumber','serial number','serial')
    if serial:
        return [serial]
    if part and rev:
        return [part, rev]
    if part:
        return [part]
    # Vendor+part combo
    vendor = has('vendor','supplier')
    vpart = has('vendor part','vendorpart','mfg part','mfgpart')
    if vendor and vpart:
        return [vendor, vpart]
    # Last resort: first non-empty column
    for h in headers:
        if h and h.strip():
            return [h]
    return []

def _row_hash(values: dict[str,str]) -> str:
    m = hashlib.sha256()
    for k in sorted(values.keys()):
        v = '' if values[k] is None else str(values[k])
        m.update(k.encode('utf-8')); m.update(b'\x00'); m.update(v.strip().encode('utf-8', errors='ignore')); m.update(b'\x00')
    return m.hexdigest()

def _read_csv_rows(csv_path: str):
    """Yield (headers, rows) reading with encoding fallbacks."""
    def open_try(enc):
        return open(csv_path, 'r', encoding=enc, newline='')
    last_err=None
    for enc in ('utf-8-sig','cp1252','utf-16','utf-8'):
        try:
            with open_try(enc) as f:
                reader = csv.DictReader(f)
                headers = list(reader.fieldnames or [])
                for row in reader:
                    yield headers, row
            return
        except Exception as e:
            last_err = e
            continue
    raise last_err or IOError('Failed to read CSV')

def ensure_parts_table(conn: sqlite3.Connection, headers: list[str]):
    """Create table if missing or add missing columns to match headers."""
    # Build sanitized mapping
    cols = []
    seen = set()
    name_map = {}
    for h in headers:
        base = _sanitize_col(h)
        name = base
        i=2
        while name in seen:
            name = f"{base}_{i}"
            i+=1
        seen.add(name)
        cols.append((h, name))
        name_map[h] = name
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (PARTS_TABLE,))
    exists = cur.fetchone() is not None
    if not exists:
        col_defs = ',\n  '.join([f'"{sql}" TEXT' for _, sql in cols])
        cur.execute(f"""
            CREATE TABLE {PARTS_TABLE} (
              id INTEGER PRIMARY KEY,
              _row_num INTEGER,
              _file_mtime TEXT,
              _file_size INTEGER,
              _key TEXT NOT NULL,
              _row_hash TEXT NOT NULL,
              _ingested_at TEXT NOT NULL,
              {col_defs}
            );
        """)
        # Ensure any legacy unique index on _key is dropped to allow duplicate parts rows
        try:
            cur.execute(f"DROP INDEX IF EXISTS idx_{PARTS_TABLE}_key")
        except Exception:
            pass
        conn.commit()
    else:
        # Add any missing columns
        cur.execute(f'PRAGMA table_info({PARTS_TABLE})')
        have = {r[1] for r in cur.fetchall()}
        to_add = [sql for _, sql in cols if sql not in have]
        # Add metadata columns if missing
        meta_adds = []
        if '_row_num' not in have:
            meta_adds.append('_row_num INTEGER')
        if '_file_mtime' not in have:
            meta_adds.append('_file_mtime TEXT')
        if '_file_size' not in have:
            meta_adds.append('_file_size INTEGER')
        for spec in meta_adds:
            colname = spec.split(' ',1)[0]
            cur.execute(f'ALTER TABLE {PARTS_TABLE} ADD COLUMN {spec}')
        for sql in to_add:
            cur.execute(f'ALTER TABLE {PARTS_TABLE} ADD COLUMN "{sql}" TEXT')
        # Drop legacy unique index if present
        try:
            cur.execute(f"DROP INDEX IF EXISTS idx_{PARTS_TABLE}_key")
        except Exception:
            pass
        if to_add or meta_adds:
            conn.commit()
    return {h: name_map[h] for h in headers}

def ensure_parts_meta(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS PartsTrackerMeta (
            file TEXT PRIMARY KEY,
            last_row INTEGER NOT NULL DEFAULT 0,
            file_size INTEGER,
            file_mtime TEXT
        )
        """
    )
    conn.commit()

def compute_key(row: dict[str,str], key_cols: list[str]) -> str:
    vals = []
    for h in key_cols:
        v = row.get(h)
        vals.append('' if v is None else str(v).strip().upper())
    key = '|'.join(vals).strip(' |')
    return key

def sync_parts_tracker(csv_path: str, progress=None) -> dict:
    """Append-only sync: insert new CSV rows since last run; allow duplicates.

    Returns stats dict: {rows, new, updated=0, skipped, key_cols}
    """
    if progress: progress('init', {'csv': csv_path})
    now = datetime.now().isoformat(timespec='seconds')
    fpath = os.path.abspath(csv_path)
    st = os.stat(fpath)
    with db_conn() as conn:
        # We need headers first; we'll read first row to get headers, then setup
        headers = None
        name_map = None
        cur = conn.cursor()
        ensure_parts_meta(conn)
        # Get last ingested row index for this file
        cur.execute("SELECT last_row, file_size, file_mtime FROM PartsTrackerMeta WHERE file=?", (fpath,))
        meta = cur.fetchone()
        last_row = int(meta[0]) if meta else 0
        rows_total = 0
        new_cnt = 0
        skipped = 0
        key_cols = []
        for hdrs, row in _read_csv_rows(fpath):
            if headers is None:
                headers = hdrs
                if not headers:
                    return {'ok': False, 'error': 'CSV appears empty'}
                name_map = ensure_parts_table(conn, headers)
                # Drop legacy unique index just in case
                try:
                    cur.execute(f"DROP INDEX IF EXISTS idx_{PARTS_TABLE}_key")
                except Exception:
                    pass
                # choose key columns only for reporting
                kc = _infer_key_cols(headers)
                key_cols = kc if kc else [headers[0]]
            rows_total += 1
            if rows_total <= last_row:
                continue
            # Build values dict using sanitized names
            vals = {}
            for h in headers:
                sqlc = name_map[h]
                vals[sqlc] = '' if row.get(h) is None else str(row.get(h))
            rhash = _row_hash(vals)
            # Invent a non-unique key per row for NOT NULL constraint, include row num
            key = f"{int(st.st_mtime)}:{rows_total}"
            cols_sql = ','.join([f'"{c}"' for c in vals.keys()])
            ph = ','.join(['?']*len(vals))
            cur.execute(
                f"INSERT INTO {PARTS_TABLE} (_row_num,_file_mtime,_file_size,_key,_row_hash,_ingested_at,{cols_sql}) VALUES (?,?,?,?,?,?,{ph})",
                [rows_total, str(int(st.st_mtime)), int(st.st_size), key, rhash, now] + list(vals.values())
            )
            new_cnt += 1
            if progress and new_cnt % 500 == 0:
                progress('upsert', {'processed': rows_total, 'new': new_cnt, 'updated': 0, 'skipped': skipped})
        # Update meta last_row
        cur.execute(
            "INSERT INTO PartsTrackerMeta(file,last_row,file_size,file_mtime) VALUES(?,?,?,?) "
            "ON CONFLICT(file) DO UPDATE SET last_row=excluded.last_row, file_size=excluded.file_size, file_mtime=excluded.file_mtime",
            (fpath, rows_total, int(st.st_size), str(int(st.st_mtime)))
        )
        conn.commit()
    skipped = max(0, rows_total - last_row - new_cnt)
    return {'ok': True, 'rows': rows_total, 'new': new_cnt, 'updated': 0, 'skipped': skipped, 'key_cols': key_cols}

def get_last_sched_run_changes(limit:int|None=None):
    if not os.path.isfile(DB_PATH):
        return None, []
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, run_started, run_completed, success, message FROM RunLog WHERE run_type='SchedulingSummary' ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        if not row:
            return None, []
        run_id = row[0]
        cur.execute("SELECT comnumber1, column_name, old_value, new_value FROM ChangeLog WHERE run_id=? ORDER BY id", (run_id,))
        changes = cur.fetchall()
        if limit is not None:
            changes = changes[:limit]
        return row, changes

class SyncApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('Scheduling & Labor Sync')
        self.geometry('820x520')
        self.running = False
        self._stop_event = None
        self._build_ui()
        ensure_change_tables()
        self._append_log('Application initialized. Ready.')

    def _build_ui(self):
        # Top controls
        top = ttk.Frame(self, padding=10)
        top.pack(fill='x')
        ttk.Label(top, text='Data Sync', font=('Segoe UI', 14, 'bold')).grid(row=0, column=0, columnspan=6, sticky='w', pady=(0,5))
        self.btn_labor = ttk.Button(top, text='Run Labor', command=self._run_labor)
        self.btn_sched = ttk.Button(top, text='Pull SCHSummary', command=self._run_sched)
        self.btn_parts = ttk.Button(top, text='Sync Parts Tracker', command=self._run_parts)
        self.btn_stop  = ttk.Button(top, text='Stop', command=self._request_stop, state='disabled')
        self.btn_task  = ttk.Button(top, text='Create Task...', command=self._open_scheduler_dialog)
        for idx, btn in enumerate((self.btn_labor, self.btn_sched, self.btn_parts, self.btn_stop, self.btn_task)):
            btn.grid(row=1, column=idx, padx=4, pady=4, sticky='ew')

        # Central log panel
        log_frame = ttk.Frame(self, padding=(10,4))
        log_frame.pack(fill='both', expand=True)
        self.status_var = tk.StringVar(value='Idle')
        ttk.Label(log_frame, textvariable=self.status_var, anchor='w').pack(fill='x')
        self.log = tk.Text(log_frame, height=18, wrap='word')
        self.log.pack(fill='both', expand=True, pady=(4,4))
        self.log.configure(state='disabled')

    # --------------- Actions --------------- #
    def _disable(self):
        for b in (self.btn_labor,self.btn_sched,self.btn_parts,self.btn_stop,self.btn_task):
            b.state(['disabled'])

    def _enable(self):
        for b in (self.btn_labor,self.btn_sched,self.btn_parts,self.btn_task):
            b.state(['!disabled'])
        self.btn_stop.state(['disabled'])

    def _run_labor(self):
        self._start_thread(self._labor_logic, 'Labor Backfill running...')

    def _run_sched(self):
        self._start_thread(self._sched_logic, 'Scheduling Summary update running...')

    def _run_parts(self):
        self._start_thread(self._parts_logic, 'Parts Tracker sync running...')

    def _start_thread(self, target, status_msg):
        if self.running:
            return
        self.running = True
        self.status_var.set(status_msg)
        self._disable()
        self.btn_stop.state(['!disabled'])
        self._stop_event = threading.Event()
        threading.Thread(target=self._wrapper, args=(target,), daemon=True).start()

    def _wrapper(self, func):
        try:
            func()
        except Exception as e:
            self._set_status(f'Error: {e}')
        finally:
            self.running = False
            self.after(100, self._enable)

    def _labor_logic(self):
        if rus is None:
            self._set_status('Logic module missing.')
            return
        def progress(phase, info):
            if phase == 'init':
                self._set_status(f"Labor init {info.get('start')} -> {info.get('end')}")
            elif phase == 'day':
                self._set_status(f"Labor {info.get('date')} inserted_today={info.get('inserted_today')} total={info.get('total_inserted')}")
            elif phase == 'stopped':
                self._set_status('Labor stopped by user')
            elif phase == 'error':
                self._set_status('Labor error: ' + info.get('message','?'))
            elif phase == 'done':
                self._set_status(f"Labor done inserted={info.get('inserted')}")
        rus.ensure_change_log_tables()
        res = rus.labor_backfill(stop_event=self._stop_event, progress=progress)
        self._set_status(f"Labor: {'OK' if res.get('ok') else 'FAIL'} inserted={res.get('inserted',0)}")
        self._archive_files(['SCHLabor_*.csv'])
        self._append_log(f"Labor run complete inserted={res.get('inserted',0)}")

    def _sched_logic(self):
        if rus is None:
            self._set_status('Logic module missing.')
            return
        rus.ensure_change_log_tables()
        res = rus.update_scheduling_summary()
        if res.get('ok'):
            rows = res.get('rows')
            new_rows = res.get('new_rows')
            updated = res.get('updated_rows')
            skipped = res.get('skipped_rows')
            changed_cols = res.get('changed_columns')
            self._set_status(f"Sched OK rows={rows} new={new_rows} upd={updated} skip={skipped} colchanges={changed_cols}")
            self._append_log(f"Scheduling Summary rows={rows} new={new_rows} updated={updated} skipped={skipped} changed_cols={changed_cols}")
            preview = res.get('changes', [])[:25]
            if preview:
                self._append_log('Changed columns preview:')
                for ch in preview:
                    self._append_log(f"COM {ch.get('comnumber1')} {ch.get('column')} {ch.get('old')} -> {ch.get('new')}")
        else:
            err = res.get('error','?')
            if 'ReportServerRoot' in err or 'ReportServerRoot not configured' in err:
                err += ' | Set env SSRS_REPORTSERVER_ROOT or create config.json with {"report_server_root":"http://server/ReportServer"}.'
            self._set_status(f"Scheduling Summary FAIL: {err[:300]}")
            self._append_log(f"Scheduling Summary FAIL: {err}")
        self._archive_files(['SCHSchedulingSummaryReport_*.csv','cleaned_file.csv'])
        self._append_log('SCHSummary files archived (older than 7 days purged).')

    def _parts_logic(self):
        # Network CSV path (read-only)
        csv_path = r"P:\\Database Parts Tracker\\Database Part Tracker II.csv"
        def progress(phase, info):
            if phase == 'init':
                self._set_status(f"Parts init: {info.get('csv')}")
            elif phase == 'read':
                self._set_status(f"Parts reading rows: {info.get('count')}")
            elif phase == 'upsert':
                self._set_status(f"Parts upsert processed={info.get('processed')} new={info.get('new')} upd={info.get('updated')} skip={info.get('skipped')}")
        try:
            res = sync_parts_tracker(csv_path, progress=progress)
            if not res.get('ok'):
                self._set_status('Parts FAIL: ' + res.get('error','?'))
                return
            cols = ', '.join(res.get('key_cols') or [])
            self._set_status(f"Parts OK rows={res['rows']} new={res['new']} updated={res['updated']} skipped={res['skipped']} (key: {cols})")
            self._append_log(f"Parts Tracker sync complete. Key cols: {cols}")
        except Exception as e:
            self._set_status(f"Parts error: {e}")

    def _archive_files(self, patterns, keep_days: int = 7):
        """Move matching files into an archive folder and purge anything older than keep_days.

        Matching is done before moving (working dir ROOT). After moving, we inspect all files in archive
        and delete those with modification time older than cutoff OR (for SCHLabor_YYYYMMDD.csv) whose
        embedded date is older than keep_days relative to today (whichever is stricter).
        """
        archive_dir = os.path.join(ROOT, 'download_archive')
        os.makedirs(archive_dir, exist_ok=True)
        moved = 0
        now = time.time()
        for pat in patterns:
            for path in glob.glob(os.path.join(ROOT, pat)):
                if os.path.isdir(path):
                    continue
                try:
                    dest = os.path.join(archive_dir, os.path.basename(path))
                    # Overwrite existing
                    if os.path.exists(dest):
                        os.remove(dest)
                    shutil.move(path, dest)
                    moved += 1
                except Exception:
                    pass
        # Purge old
        cutoff = now - keep_days*86400
        removed = 0
        for fname in os.listdir(archive_dir):
            fpath = os.path.join(archive_dir, fname)
            try:
                st = os.stat(fpath)
                too_old = st.st_mtime < cutoff
                # Additional heuristic: parse SCHLabor_YYYYMMDD.csv date
                if not too_old and fname.startswith('SCHLabor_') and fname.endswith('.csv'):
                    date_part = fname[len('SCHLabor_'):-4]
                    if len(date_part)==8 and date_part.isdigit():
                        # yyyymmdd
                        y=int(date_part[0:4]); m=int(date_part[4:6]); d=int(date_part[6:8])
                        import datetime
                        fdate = datetime.date(y,m,d)
                        if (datetime.date.today() - fdate).days > keep_days:
                            too_old = True
                if too_old:
                    os.remove(fpath); removed += 1
            except Exception:
                pass
        if moved or removed:
            self._set_status(f"Archived {moved} file(s); purged {removed} old; keeping last {keep_days} days")

    def _request_stop(self):
        if hasattr(self, '_stop_event'):
            self._stop_event.set()
            self._set_status('Stop requested...')

    def _set_status(self, msg: str):
        ts = datetime.now().strftime('%H:%M:%S')
        self.status_var.set(f"{ts} - {msg}")
        self._append_log(msg)

    def _append_log(self, line: str):
        try:
            self.log.configure(state='normal')
            ts = datetime.now().strftime('%H:%M:%S')
            self.log.insert('end', f"[{ts}] {line}\n")
            self.log.see('end')
            self.log.configure(state='disabled')
        except Exception:
            pass

    def _refresh_changes(self):
        return  # table removed

    def _export_changes(self):
        messagebox.showinfo('Export Disabled', 'Change export removed from simplified UI.')

    # -------- Scheduled Task Creation -------- #
    def _open_scheduler_dialog(self):
        dlg = tk.Toplevel(self)
        dlg.title('Create Scheduled Tasks')
        dlg.geometry('420x300')
        ttk.Label(dlg, text='Create Windows Scheduled Tasks', font=('Segoe UI', 12,'bold')).pack(pady=6)
        frm = ttk.Frame(dlg, padding=6)
        frm.pack(fill='both', expand=True)
        ttk.Label(frm, text='Run Time (HH:MM 24h):').grid(row=0,column=0, sticky='w')
        time_var = tk.StringVar(value='02:00')
        ttk.Entry(frm, textvariable=time_var, width=8).grid(row=0,column=1, sticky='w')
        ttk.Label(frm, text='Task Prefix:').grid(row=1,column=0, sticky='w', pady=(6,0))
        prefix_var = tk.StringVar(value='SCHSync')
        ttk.Entry(frm, textvariable=prefix_var, width=20).grid(row=1,column=1, sticky='w', pady=(6,0))
        both_var = tk.BooleanVar(value=True)
        labor_var = tk.BooleanVar(value=False)
        schedule_var = tk.BooleanVar(value=False)
        parts_var = tk.BooleanVar(value=False)
        
        ttk.Checkbutton(frm, text='Create Combined (Both) Task', variable=both_var).grid(row=2,column=0,columnspan=2, sticky='w', pady=(6,0))
        ttk.Checkbutton(frm, text='Create Labor Task', variable=labor_var).grid(row=3,column=0,columnspan=2, sticky='w')
        ttk.Checkbutton(frm, text='Create Scheduling Summary Task', variable=schedule_var).grid(row=4,column=0,columnspan=2, sticky='w')
        ttk.Checkbutton(frm, text='Create Parts Tracker Task', variable=parts_var).grid(row=5,column=0,columnspan=2, sticky='w')
        status_lbl = ttk.Label(frm, text='', foreground='blue')
        status_lbl.grid(row=6,column=0,columnspan=2, sticky='w', pady=(8,0))
        def create_tasks():
            tm = time_var.get().strip()
            if not _valid_time(tm):
                messagebox.showerror('Invalid','Time must be HH:MM')
                return
            
            messages = []
            if both_var.get():
                ok, msg = self._create_task(prefix_var.get()+'Both', tm, 'both')
                messages.append(msg)
            if labor_var.get():
                ok, msg = self._create_task(prefix_var.get()+'Labor', tm, 'labor')
                messages.append(msg)
            if schedule_var.get():
                ok, msg = self._create_task(prefix_var.get()+'Schedule', tm, 'schedule')
                messages.append(msg)
            if parts_var.get():
                ok, msg = self._create_task(prefix_var.get()+'Parts', tm, 'parts')
                messages.append(msg)
            
            if not messages:
                messagebox.showwarning('No Selection', 'Please select at least one task to create.')
                return
            
            status_lbl.config(text='\n'.join(messages), foreground='green')
        ttk.Button(frm, text='Create', command=create_tasks).grid(row=7,column=0, pady=10, sticky='w')
        ttk.Button(frm, text='Close', command=dlg.destroy).grid(row=7,column=1, pady=10, sticky='e')

    def _create_task(self, name:str, time_hhmm:str, mode:str):
        python_exe = sys.executable.replace('pythonw.exe','python.exe')
        target_script = os.path.join(ROOT, 'new_data_sync_app.py')
        if not os.path.isfile(target_script):
            return False, 'new_data_sync_app.py not found'
        cmd = [
            'schtasks','/Create','/SC','DAILY','/TN', name,
            '/TR', f'"{python_exe}" "{target_script}" {mode}',
            '/ST', time_hhmm,'/F'
        ]
        try:
            cp = subprocess.run(cmd, capture_output=True, text=True)
            if cp.returncode == 0:
                return True, f'Task {name} created.'
            return False, cp.stderr.strip() or 'Task creation failed.'
        except Exception as e:
            return False, str(e)

def _valid_time(s: str) -> bool:
    if len(s)!=5 or s[2] != ':':
        return False
    try:
        hh = int(s[:2]); mm = int(s[3:])
        return 0<=hh<24 and 0<=mm<60
    except ValueError:
        return False

def main():
    if not os.path.isfile(DB_PATH):
        messagebox.showerror('Missing DB', f'Database not found: {DB_PATH}')
        return
    app = SyncApp()
    app.mainloop()

if __name__ == '__main__':
    main()
