"""Desktop Data Sync App (Windows GUI)

Simplified log‑centric UI with auto-refresh capabilities.

Functions:
    - Labor Backfill (full reprocess with dedupe)
    - Scheduling Summary 120‑Day Upsert (past 60 / next 60 days) with change stats
    - Parts Tracker sync
    - DR Polling (2d, 7d, 30d windows)
    - Auto-refresh timers for all tasks (configurable intervals)
    - Headless mode for background execution

Run with GUI:  python desktop_sync_app.py
Run headless:  python desktop_sync_app.py --headless

Depends on: SCHLabor.db, report_update_service.py, clean.py, PowerShell scripts.
"""
from __future__ import annotations
import os, sqlite3, threading, csv, subprocess, sys, glob, shutil, time, hashlib, re, json, argparse
from datetime import datetime, timedelta
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog

ROOT = os.path.dirname(__file__)
DB_PATH = os.path.join(ROOT, 'SCHLabor.db')
CREDS_SERVICE = 'SQRS_DR'
CONFIG_FILE = os.path.join(ROOT, 'auto_sync_config.json')

# Optional secure storage via Windows Credential Manager (keyring)
try:
    import keyring  # type: ignore
    HAVE_KEYRING = True
except Exception:
    keyring = None
    HAVE_KEYRING = False

# Attempt to import existing service logic
try:
    import report_update_service as rus  # provides labor_backfill() & update_scheduling_summary()
except Exception as e:
    rus = None
    print('WARNING: report_update_service import failed:', e)

# Ensure DR schema is available
try:
    import dr_schema
except Exception:
    dr_schema = None

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

# ---------- Auto-Scheduler for Background Polling ---------- #

class AutoScheduler:
    """Manages auto-refresh timers for all sync operations.
    
    Can run in headless mode (no GUI) or be controlled by GUI.
    Saves/loads config from JSON file.
    """
    
    DEFAULT_CONFIG = {
        'labor': {'enabled': False, 'interval_minutes': 1440},  # Daily
        'scheduling': {'enabled': False, 'interval_minutes': 480},  # 8 hours
        'parts': {'enabled': False, 'interval_minutes': 720},  # 12 hours
        'dr_2d': {'enabled': False, 'interval_minutes': 60},  # Hourly
        'dr_7d': {'enabled': False, 'interval_minutes': 60},  # Hourly
        'dr_30d': {'enabled': False, 'interval_minutes': 180}  # 3 hours
    }
    
    def __init__(self, headless=False, gui_app=None):
        """Initialize scheduler.
        
        Args:
            headless: If True, runs without GUI (uses threading.Timer instead of tk.after)
            gui_app: Reference to SyncApp instance if running with GUI
        """
        self.headless = headless
        self.gui_app = gui_app
        self.config = self.DEFAULT_CONFIG.copy()
        self.timers = {}  # task_name -> timer_id or threading.Timer
        self.next_runs = {}  # task_name -> datetime
        
    def load_config(self):
        """Load config from JSON file."""
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r') as f:
                    loaded = json.load(f)
                    # Merge with defaults to handle missing keys
                    for task, defaults in self.DEFAULT_CONFIG.items():
                        if task in loaded:
                            self.config[task] = {**defaults, **loaded[task]}
                print(f'Loaded config from {CONFIG_FILE}')
            except Exception as e:
                print(f'Failed to load config: {e}')
                
    def save_config(self):
        """Save config to JSON file."""
        try:
            with open(CONFIG_FILE, 'w') as f:
                json.dump(self.config, f, indent=2)
            print(f'Saved config to {CONFIG_FILE}')
        except Exception as e:
            print(f'Failed to save config: {e}')
            
    def set_task_config(self, task_name, enabled, interval_minutes):
        """Update config for a specific task."""
        if task_name in self.config:
            self.config[task_name]['enabled'] = enabled
            self.config[task_name]['interval_minutes'] = interval_minutes
            self.save_config()
            
    def start_task(self, task_name):
        """Start auto-refresh timer for a task."""
        if task_name not in self.config:
            return
            
        cfg = self.config[task_name]
        if not cfg['enabled']:
            return
            
        interval_mins = cfg['interval_minutes']
        next_run = datetime.now() + timedelta(minutes=interval_mins)
        self.next_runs[task_name] = next_run
        
        if self.headless:
            # Use threading.Timer for headless mode
            import threading
            timer = threading.Timer(interval_mins * 60, lambda: self._task_callback(task_name))
            timer.daemon = True
            timer.start()
            self.timers[task_name] = timer
            print(f'[{task_name}] Scheduled for {next_run.strftime("%I:%M %p")} ({interval_mins}m)')
        else:
            # Use tk.after for GUI mode
            if self.gui_app:
                timer_id = self.gui_app.after(interval_mins * 60 * 1000, 
                                              lambda: self._task_callback(task_name))
                self.timers[task_name] = timer_id
                self.gui_app._append_log(f'[AUTO] {task_name}: scheduled for {next_run.strftime("%I:%M %p")}')
                
    def stop_task(self, task_name):
        """Stop auto-refresh timer for a task."""
        if task_name in self.timers:
            timer = self.timers[task_name]
            if self.headless:
                if hasattr(timer, 'cancel'):
                    timer.cancel()
            else:
                if self.gui_app:
                    self.gui_app.after_cancel(timer)
            del self.timers[task_name]
            
        if task_name in self.next_runs:
            del self.next_runs[task_name]
            
    def start_all_enabled(self):
        """Start timers for all enabled tasks."""
        for task_name, cfg in self.config.items():
            if cfg['enabled']:
                self.start_task(task_name)
                
    def stop_all(self):
        """Stop all running timers."""
        for task_name in list(self.timers.keys()):
            self.stop_task(task_name)
            
    def _task_callback(self, task_name):
        """Called when a task timer fires."""
        if self.headless:
            print(f'[AUTO-REFRESH] Running {task_name}...')
            self._execute_task_headless(task_name)
        else:
            if self.gui_app:
                self.gui_app._append_log(f'[AUTO-REFRESH] Running {task_name}...')
                self._execute_task_gui(task_name)
                
        # Reschedule if still enabled
        if self.config[task_name]['enabled']:
            self.start_task(task_name)
            
    def _execute_task_headless(self, task_name):
        """Execute a task in headless mode (direct function calls)."""
        try:
            if task_name == 'labor':
                if rus:
                    print(f'[{task_name}] Starting labor backfill...')
                    rus.labor_backfill(progress=None)
                    print(f'[{task_name}] Completed')
                else:
                    print(f'[{task_name}] ERROR: rus module not available')
                    
            elif task_name == 'scheduling':
                if rus:
                    print(f'[{task_name}] Starting scheduling summary...')
                    rus.update_scheduling_summary(progress=None)
                    print(f'[{task_name}] Completed')
                else:
                    print(f'[{task_name}] ERROR: rus module not available')
                    
            elif task_name == 'parts':
                csv_path = r"P:\\Database Parts Tracker\\Database Part Tracker II.csv"
                print(f'[{task_name}] Starting parts sync from {csv_path}...')
                result = sync_parts_tracker(csv_path, progress=None)
                if result.get('ok'):
                    print(f'[{task_name}] Completed: rows={result["rows"]} new={result["new"]}')
                else:
                    print(f'[{task_name}] ERROR: {result.get("error")}')
                    
            elif task_name.startswith('dr_'):
                days = int(task_name.split('_')[1].replace('d', ''))
                creds = _get_creds_headless()
                if not creds:
                    print(f'[{task_name}] ERROR: No credentials available')
                    return
                    
                print(f'[{task_name}] Starting DR poll ({days} days)...')
                
                # Run DR poller
                import run_poll_with_history_and_ingest as rph
                import dr_ingest
                
                archive_dir = os.path.join(ROOT, 'download_archive')
                os.makedirs(archive_dir, exist_ok=True)
                out_json = os.path.join(archive_dir, 'dr_incremental.json')
                timings_json = os.path.join(archive_dir, 'dr_timings.json')
                state_file = os.path.join(archive_dir, 'dr_state.json')
                
                # Safety limits
                if days <= 2:
                    max_drs, throttle = 100, 0.1
                elif days <= 7:
                    max_drs, throttle = 200, 0.2
                else:
                    max_drs, throttle = 500, 0.5
                
                print(f"[DEBUG] Days={days}, max_drs={max_drs}, throttle={throttle}")
                
                DEFAULT_BASE_URL = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc'
                base_url = os.getenv('MOM_BASE_URL', DEFAULT_BASE_URL)
                app_name = os.getenv('MOM_APP_NAME', 'ManufacturingDeviationSystem')
                
                urgency_map = rph.poll_and_write(
                    base_url=base_url,
                    user=creds[0].strip(),
                    pwd=creds[1].strip(),
                    app_name=app_name,
                    window_days=days,
                    include_closed=True,
                    include_history=True,
                    out_json=out_json,
                    timings_json=timings_json,
                    state_file=state_file,
                    max_drs=max_drs,
                    throttle_seconds=throttle,
                )
                
                # Ingest to DB
                dr_ingest.ingest_last_poll(
                    db_path=DB_PATH,
                    archive_dir=archive_dir,
                    urgency_map=urgency_map or {}
                )
                
                # Count results
                count = 0
                if os.path.isfile(out_json):
                    with open(out_json, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    if isinstance(data, list):
                        count = len(data)
                print(f'[{task_name}] Completed: {count} DRs processed')
                
        except Exception as e:
            import traceback
            print(f'[{task_name}] ERROR: {e}')
            traceback.print_exc()
            
    def _execute_task_gui(self, task_name):
        """Execute a task in GUI mode (use existing button methods)."""
        if not self.gui_app:
            return
            
        try:
            if task_name == 'labor':
                self.gui_app._run_labor()
            elif task_name == 'scheduling':
                self.gui_app._run_sched()
            elif task_name == 'parts':
                self.gui_app._run_parts()
            elif task_name == 'dr_2d':
                self.gui_app._run_drs(2)
            elif task_name == 'dr_7d':
                self.gui_app._run_drs(7)
            elif task_name == 'dr_30d':
                self.gui_app._run_drs(30)
        except Exception as e:
            self.gui_app._append_log(f'[AUTO] {task_name} error: {e}')

def _get_creds_headless():
    """Get DR credentials in headless mode (from keyring or env)."""
    if HAVE_KEYRING:
        try:
            usr = keyring.get_password(CREDS_SERVICE, 'username')
            pwd = keyring.get_password(CREDS_SERVICE, 'password')
            if usr and pwd:
                return (usr, pwd)
        except Exception:
            pass
    # Fallback to environment variables
    usr = os.environ.get('DR_USERNAME')
    pwd = os.environ.get('DR_PASSWORD')
    if usr and pwd:
        return (usr, pwd)
    return None

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
        
        # Initialize auto-scheduler
        self.scheduler = AutoScheduler(headless=False, gui_app=self)
        self.scheduler.load_config()
        
        self._build_ui()
        self._creds = None
        self._load_credentials()
        ensure_change_tables()
        # Create DR tables (no-op if already exist)
        try:
            if dr_schema is not None:
                with db_conn() as conn:
                    dr_schema.ensure_dr_tables(conn)
        except Exception as e:
            self._append_log(f'DR schema init failed: {e}')
        
        # Start enabled auto-refresh tasks
        self.scheduler.start_all_enabled()
        
        self._append_log('Application initialized. Ready.')
        self._update_scheduler_status()

    def _build_ui(self):
        # Top controls
        top = ttk.Frame(self, padding=10)
        top.pack(fill='x')
        ttk.Label(top, text='Data Sync', font=('Segoe UI', 14, 'bold')).grid(row=0, column=0, columnspan=9, sticky='w', pady=(0,5))
        self.btn_labor = ttk.Button(top, text='Run Labor', command=self._run_labor)
        self.btn_sched = ttk.Button(top, text='Pull SCHSummary', command=self._run_sched)
        self.btn_parts = ttk.Button(top, text='Sync Parts Tracker', command=self._run_parts)
        self.btn_dr2   = ttk.Button(top, text='Poll DRs (2d)', command=lambda: self._run_drs(2))
        self.btn_dr7   = ttk.Button(top, text='Poll DRs (7d)', command=lambda: self._run_drs(7))
        self.btn_dr30  = ttk.Button(top, text='Poll DRs (30d)', command=lambda: self._run_drs(30))
        self.btn_creds = ttk.Button(top, text='Credentials...', command=self._open_credentials_dialog)
        self.btn_stop  = ttk.Button(top, text='Stop', command=self._request_stop, state='disabled')
        self.btn_auto  = ttk.Button(top, text='Auto-Refresh...', command=self._open_auto_settings)
        for idx, btn in enumerate((self.btn_labor, self.btn_sched, self.btn_parts, self.btn_dr2, self.btn_dr7, self.btn_dr30, self.btn_creds, self.btn_stop, self.btn_auto)):
            btn.grid(row=1, column=idx, padx=4, pady=4, sticky='ew')

        # Auto-scheduler status display
        auto_frame = ttk.LabelFrame(top, text='Auto-Refresh Status', padding=8)
        auto_frame.grid(row=2, column=0, columnspan=9, sticky='ew', pady=(8,0))
        
        self.auto_status_var = tk.StringVar(value='Click "Auto-Refresh..." to configure')
        ttk.Label(auto_frame, textvariable=self.auto_status_var, foreground='gray', 
                 font=('Segoe UI', 9)).pack(anchor='w')

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
        for b in (self.btn_labor,self.btn_sched,self.btn_parts,self.btn_dr2,self.btn_dr7,self.btn_dr30,self.btn_creds,self.btn_stop,self.btn_auto):
            b.state(['disabled'])

    def _enable(self):
        for b in (self.btn_labor,self.btn_sched,self.btn_parts,self.btn_dr2,self.btn_dr7,self.btn_dr30,self.btn_creds,self.btn_auto):
            b.state(['!disabled'])
        self.btn_stop.state(['disabled'])

    def _run_labor(self):
        self._start_thread(self._labor_logic, 'Labor Backfill running...')

    def _run_sched(self):
        self._start_thread(self._sched_logic, 'Scheduling Summary update running...')

    def _run_parts(self):
        self._start_thread(self._parts_logic, 'Parts Tracker sync running...')

    def _run_drs(self, window_days=7):
        # Validate credentials on the UI thread to avoid background-thread dialogs
        creds = self._get_saved_or_env_creds()
        if not creds:
            messagebox.showwarning('Credentials required', 'Please set DR credentials (5 letters/numbers each) using the "Credentials..." button before running the poller.')
            self._append_log('DR Poller: missing credentials. Opened Credentials dialog advised.')
            return
        user, pwd = creds
        # Cache into instance so worker can use it
        self._creds = {'user': user, 'password': pwd}
        self._dr_window_days = window_days  # Store for worker thread
        self._start_thread(self._drs_logic, f'DR Poller ({window_days}d) running...')
    
    def _open_auto_settings(self):
        """Open auto-refresh settings dialog."""
        AutoSettingsDialog(self, self.scheduler)
    
    def _update_scheduler_status(self):
        """Update the status display with info about enabled auto-refresh tasks."""
        enabled = [name for name, cfg in self.scheduler.config.items() if cfg['enabled']]
        if not enabled:
            self.auto_status_var.set('No tasks enabled. Click "Auto-Refresh..." to configure')
        else:
            # Show next run times
            status_parts = []
            for task_name in enabled:
                if task_name in self.scheduler.next_runs:
                    next_time = self.scheduler.next_runs[task_name].strftime('%I:%M %p')
                    status_parts.append(f'{task_name}: {next_time}')
            if status_parts:
                self.auto_status_var.set(' | '.join(status_parts))
            else:
                self.auto_status_var.set(f'{len(enabled)} task(s) enabled')
        
        # Schedule next update in 30 seconds
        self.after(30000, self._update_scheduler_status)

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
        except SystemExit as e:
            self._set_status(f'Error: {e}')
        except BaseException as e:
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

    def _drs_logic(self):
        """Run DR poller with history and auto-ingest to DB."""
        try:
            # Credentials: must be present already (validated in _run_drs)
            user = (self._creds or {}).get('user')
            pwd = (self._creds or {}).get('password')
            window_days = getattr(self, '_dr_window_days', 7)
            
            if not user or not pwd:
                self._set_status('DR Poller error: credentials not set')
                return

            # Output paths
            archive_dir = os.path.join(ROOT, 'download_archive')
            os.makedirs(archive_dir, exist_ok=True)
            out_json = os.path.join(archive_dir, 'dr_incremental.json')
            timings_json = os.path.join(archive_dir, 'dr_timings.json')
            state_file = os.path.join(archive_dir, 'dr_state.json')

            self._set_status(f'DR Poller ({window_days}d) starting...')
            self._append_log(f"Window: {window_days} days | Output folder: {archive_dir}")
            
            # Safety limits based on window size
            if window_days <= 2:
                max_drs = 100
                throttle_sec = 0.1
            elif window_days <= 7:
                max_drs = 200
                throttle_sec = 0.2
            else:  # 30 days
                max_drs = 500
                throttle_sec = 0.5
            
            self._append_log(f"Safety: max {max_drs} DRs, {throttle_sec}s throttle (prevents host overload)")
            
            # Call inline poller
            import run_poll_with_history_and_ingest as rph
            
            DEFAULT_BASE_URL = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc'
            base_url = os.getenv('MOM_BASE_URL', DEFAULT_BASE_URL)
            app_name = os.getenv('MOM_APP_NAME', 'ManufacturingDeviationSystem')
            
            urgency_map = rph.poll_and_write(
                base_url=base_url,
                user=user.strip(),
                pwd=pwd.strip(),
                app_name=app_name,
                window_days=window_days,
                include_closed=True,
                include_history=True,
                out_json=out_json,
                timings_json=timings_json,
                state_file=state_file,  # Fixed: was state_json
                max_drs=max_drs,
                throttle_seconds=throttle_sec,
            )
            
            # Auto-ingest to DB
            self._append_log("Ingesting poll results to database...")
            import dr_ingest
            dr_ingest.ingest_last_poll(
                db_path=DB_PATH,
                archive_dir=archive_dir,
                urgency_map=urgency_map or {}
            )
            
            # Summarize
            count = None
            try:
                if os.path.isfile(out_json):
                    with open(out_json, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    if isinstance(data, list):
                        count = len(data)
            except Exception:
                pass
            
            size = os.path.getsize(out_json) if os.path.isfile(out_json) else 0
            self._set_status(f"DR Poller ({window_days}d) OK -> {count if count is not None else '?'} DRs ingested to DB")
            self._append_log(f"JSON: {size} bytes | Auto-ingested with delta logic (no duplicates)")
            
        except SystemExit as e:
            self._set_status(f"DR Poller failed: {e}")
        except BaseException as e:
            self._set_status(f"DR Poller error: {e}")

    def _get_saved_or_env_creds(self):
        """Return (user, pwd) if available and valid, else None."""
        # Prefer saved
        if self._creds:
            u = (self._creds.get('user') or '').strip()
            p = (self._creds.get('password') or '').strip()
            if re.fullmatch(r'[A-Za-z0-9]{5}', u) and re.fullmatch(r'[A-Za-z0-9]{5}', p):
                return (u, p)
        # Env fallback
        u = (os.getenv('DR_USER') or os.getenv('MOM_USER') or '').strip()
        p = (os.getenv('DR_PASSWORD') or os.getenv('MOM_PASSWORD') or '').strip()
        if re.fullmatch(r'[A-Za-z0-9]{5}', u) and re.fullmatch(r'[A-Za-z0-9]{5}', p):
            return (u, p)
        return None

    # ---------- Credentials persistence ---------- #
    def _creds_path(self) -> str:
        archive_dir = os.path.join(ROOT, 'download_archive')
        os.makedirs(archive_dir, exist_ok=True)
        return os.path.join(archive_dir, 'dr_credentials.json')

    def _load_credentials(self):
        try:
            p = self._creds_path()
            if os.path.isfile(p):
                with open(p, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    user = (data.get('user') or '').strip()
                    pwd = ''
                    # Prefer secure store; fall back to JSON if legacy password key exists
                    if user and HAVE_KEYRING:
                        try:
                            pwd = keyring.get_password(CREDS_SERVICE, user) or ''
                        except Exception:
                            pwd = ''
                    if not pwd:
                        pwd = (data.get('password') or '').strip()
                    if user and pwd and re.fullmatch(r'[A-Za-z0-9]{5}', user) and re.fullmatch(r'[A-Za-z0-9]{5}', pwd):
                        self._creds = {'user': user, 'password': pwd}
                        self._append_log('Loaded saved DR credentials.')
                        if 'password' in data and HAVE_KEYRING:
                            # Migrate legacy plaintext to keyring and rewrite file without password
                            try:
                                keyring.set_password(CREDS_SERVICE, user, pwd)
                                with open(p, 'w', encoding='utf-8') as fw:
                                    json.dump({'user': user, 'saved_at': datetime.now().isoformat(timespec='seconds')}, fw, indent=2)
                                self._append_log('Migrated DR password to Windows Credential Manager.')
                            except Exception:
                                pass
                        return
        except Exception:
            pass
        self._creds = None

    def _save_credentials(self, user: str, pwd: str):
        try:
            user_s = user.strip()
            pwd_s = pwd.strip()
            # Save password securely if possible
            if HAVE_KEYRING:
                try:
                    keyring.set_password(CREDS_SERVICE, user_s, pwd_s)
                    # Store only username in file
                    data = {'user': user_s, 'saved_at': datetime.now().isoformat(timespec='seconds')}
                    with open(self._creds_path(), 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2)
                    self._append_log('DR credentials saved securely (Windows Credential Manager).')
                except Exception as e:
                    # Fallback to plaintext JSON
                    data = {'user': user_s, 'password': pwd_s, 'saved_at': datetime.now().isoformat(timespec='seconds')}
                    with open(self._creds_path(), 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2)
                    self._append_log('DR credentials saved (plaintext fallback).')
            else:
                # No keyring available; save plaintext (warn user)
                data = {'user': user_s, 'password': pwd_s, 'saved_at': datetime.now().isoformat(timespec='seconds')}
                with open(self._creds_path(), 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
                self._append_log('DR credentials saved (plaintext file). Consider installing "keyring" for secure storage.')
            self._creds = {'user': user_s, 'password': pwd_s}
        except Exception as e:
            messagebox.showerror('Save Error', f'Failed to save credentials: {e}')

    def _open_credentials_dialog(self):
        dlg = tk.Toplevel(self)
        dlg.title('DR Credentials')
        dlg.geometry('320x180')
        dlg.transient(self)
        dlg.grab_set()
        frm = ttk.Frame(dlg, padding=10)
        frm.pack(fill='both', expand=True)

        ttk.Label(frm, text='Username (5 letters/numbers):').grid(row=0, column=0, sticky='w')
        user_var = tk.StringVar(value=(self._creds.get('user') if self._creds else ''))
        user_entry = ttk.Entry(frm, textvariable=user_var, width=20)
        user_entry.grid(row=1, column=0, sticky='w')

        ttk.Label(frm, text='Password (5 letters/numbers):').grid(row=2, column=0, sticky='w', pady=(8,0))
        pwd_var = tk.StringVar(value=(self._creds.get('password') if self._creds else ''))
        pwd_entry = ttk.Entry(frm, textvariable=pwd_var, width=20, show='*')
        pwd_entry.grid(row=3, column=0, sticky='w')

        note = 'Stored in Windows Credential Manager.' if HAVE_KEYRING else 'Stored in plaintext file (install "keyring" for secure storage).'
        status_lbl = ttk.Label(frm, text=note, foreground='blue')
        status_lbl.grid(row=4, column=0, sticky='w', pady=(8,0))

        def validate(u: str, p: str) -> tuple[bool, str]:
            if not re.fullmatch(r'[A-Za-z0-9]{5}', u or ''):
                return False, 'Username must be 5 letters/numbers.'
            if not re.fullmatch(r'[A-Za-z0-9]{5}', p or ''):
                return False, 'Password must be 5 letters/numbers.'
            return True, ''

        def on_save():
            u = user_var.get().strip()
            p = pwd_var.get().strip()
            ok, msg = validate(u, p)
            if not ok:
                status_lbl.config(text=msg, foreground='red')
                return
            self._save_credentials(u, p)
            status_lbl.config(text='Saved.', foreground='green')
            dlg.after(400, dlg.destroy)

        btns = ttk.Frame(frm)
        btns.grid(row=5, column=0, sticky='e', pady=(10,0))
        ttk.Button(btns, text='Save', command=on_save).grid(row=0, column=0, padx=4)
        ttk.Button(btns, text='Cancel', command=dlg.destroy).grid(row=0, column=1, padx=4)

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

# ---------- Auto-Refresh Settings Dialog ---------- #

class AutoSettingsDialog(tk.Toplevel):
    """Dialog to configure auto-refresh intervals for all tasks."""
    
    TASK_LABELS = {
        'labor': 'Labor Backfill',
        'scheduling': 'Scheduling Summary',
        'parts': 'Parts Tracker',
        'dr_2d': 'DR Polling (2 days)',
        'dr_7d': 'DR Polling (7 days)',
        'dr_30d': 'DR Polling (30 days)'
    }
    
    def __init__(self, parent, scheduler):
        super().__init__(parent)
        self.parent = parent
        self.scheduler = scheduler
        self.title('Auto-Refresh Settings')
        self.geometry('600x400')
        self.resizable(False, False)
        
        # Storage for widgets
        self.enabled_vars = {}
        self.interval_vars = {}
        
        self._build_ui()
        
    def _build_ui(self):
        # Header
        ttk.Label(self, text='Configure Auto-Refresh Intervals', 
                 font=('Segoe UI', 12, 'bold')).pack(pady=10)
        
        ttk.Label(self, text='Enable tasks to run automatically at specified intervals.\n'
                            'Changes are saved and persist across restarts.',
                 font=('Segoe UI', 9)).pack(pady=(0, 10))
        
        # Scrollable frame for task list
        canvas = tk.Canvas(self, highlightthickness=0)
        scrollbar = ttk.Scrollbar(self, orient='vertical', command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            '<Configure>',
            lambda e: canvas.configure(scrollregion=canvas.bbox('all'))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor='nw')
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Grid headers
        ttk.Label(scrollable_frame, text='Task', font=('Segoe UI', 9, 'bold')).grid(
            row=0, column=0, padx=10, pady=5, sticky='w')
        ttk.Label(scrollable_frame, text='Enabled', font=('Segoe UI', 9, 'bold')).grid(
            row=0, column=1, padx=10, pady=5)
        ttk.Label(scrollable_frame, text='Interval (minutes)', font=('Segoe UI', 9, 'bold')).grid(
            row=0, column=2, padx=10, pady=5)
        
        # Create row for each task
        row = 1
        for task_name in ['labor', 'scheduling', 'parts', 'dr_2d', 'dr_7d', 'dr_30d']:
            cfg = self.scheduler.config[task_name]
            
            # Task label
            ttk.Label(scrollable_frame, text=self.TASK_LABELS[task_name]).grid(
                row=row, column=0, padx=10, pady=8, sticky='w')
            
            # Enabled checkbox
            enabled_var = tk.BooleanVar(value=cfg['enabled'])
            self.enabled_vars[task_name] = enabled_var
            ttk.Checkbutton(scrollable_frame, variable=enabled_var).grid(
                row=row, column=1, padx=10, pady=8)
            
            # Interval spinbox
            interval_var = tk.StringVar(value=str(cfg['interval_minutes']))
            self.interval_vars[task_name] = interval_var
            
            # Different defaults based on task type
            if task_name == 'labor':
                from_val, to_val = 60, 10080  # 1 hour to 1 week
            elif task_name == 'scheduling':
                from_val, to_val = 60, 1440  # 1 hour to 1 day
            elif task_name == 'parts':
                from_val, to_val = 60, 1440  # 1 hour to 1 day
            else:  # DR tasks
                from_val, to_val = 5, 1440  # 5 min to 1 day
            
            spinbox = ttk.Spinbox(scrollable_frame, from_=from_val, to=to_val, 
                                 increment=5, textvariable=interval_var, width=10)
            spinbox.grid(row=row, column=2, padx=10, pady=8)
            
            row += 1
        
        canvas.pack(side='left', fill='both', expand=True, padx=10, pady=10)
        scrollbar.pack(side='right', fill='y')
        
        # Buttons
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=(0, 10))
        
        ttk.Button(btn_frame, text='Save & Apply', command=self._save).pack(side='left', padx=5)
        ttk.Button(btn_frame, text='Cancel', command=self.destroy).pack(side='left', padx=5)
        ttk.Button(btn_frame, text='Test (Run Now)', command=self._test).pack(side='right', padx=5)
        
    def _save(self):
        """Save settings and restart scheduler."""
        # Validate intervals
        for task_name, interval_var in self.interval_vars.items():
            try:
                interval = int(interval_var.get())
                if interval < 1:
                    raise ValueError()
            except:
                messagebox.showerror('Invalid Interval', 
                    f'Please enter a valid interval for {self.TASK_LABELS[task_name]}')
                return
        
        # Stop all tasks
        self.scheduler.stop_all()
        
        # Update config
        for task_name in self.enabled_vars.keys():
            enabled = self.enabled_vars[task_name].get()
            interval = int(self.interval_vars[task_name].get())
            self.scheduler.set_task_config(task_name, enabled, interval)
        
        # Start enabled tasks
        self.scheduler.start_all_enabled()
        
        # Update parent status
        self.parent._update_scheduler_status()
        self.parent._append_log('Auto-refresh settings saved and applied')
        
        messagebox.showinfo('Settings Saved', 
            'Auto-refresh settings have been saved and will persist across restarts.')
        self.destroy()
        
    def _test(self):
        """Run a test execution of selected tasks."""
        enabled_tasks = [name for name, var in self.enabled_vars.items() if var.get()]
        if not enabled_tasks:
            messagebox.showwarning('No Tasks Selected', 
                'Please enable at least one task to test.')
            return
        
        # Ask which one to test
        task = simpledialog.askstring('Test Task', 
            f'Which task to test?\n{", ".join(enabled_tasks)}')
        if task and task in enabled_tasks:
            self.parent._append_log(f'[TEST] Running {task}...')
            self.scheduler._execute_task_gui(task)

def _valid_time(s: str) -> bool:
    if len(s)!=5 or s[2] != ':':
        return False
    try:
        hh = int(s[:2]); mm = int(s[3:])
        return 0<=hh<24 and 0<=mm<60
    except ValueError:
        return False

def main():
    """Main entry point with headless mode support."""
    parser = argparse.ArgumentParser(description='Desktop Data Sync App')
    parser.add_argument('--headless', action='store_true', 
                       help='Run in headless mode (no GUI, auto-scheduler only)')
    parser.add_argument('--log-file', type=str, default=None,
                       help='Log file for headless mode output')
    args = parser.parse_args()
    
    if not os.path.isfile(DB_PATH):
        if args.headless:
            print(f'ERROR: Database not found: {DB_PATH}')
            sys.exit(1)
        else:
            messagebox.showerror('Missing DB', f'Database not found: {DB_PATH}')
            return
    
    if args.headless:
        # Run in headless mode - no GUI, just scheduler
        from threading import Event
        
        # Set up logging if requested
        if args.log_file:
            log_path = args.log_file
        else:
            log_path = os.path.join(ROOT, 'auto_sync_headless.log')
        
        # Log to file
        class Logger:
            def __init__(self, filename):
                self.terminal = sys.stdout
                self.log = open(filename, 'a', encoding='utf-8')
            
            def write(self, message):
                self.terminal.write(message)
                self.log.write(message)
                self.log.flush()
            
            def flush(self):
                self.terminal.flush()
                self.log.flush()
        
        sys.stdout = Logger(log_path)
        sys.stderr = sys.stdout
        
        print(f'\n{"="*60}')
        print(f'Desktop Sync App - HEADLESS MODE')
        print(f'Started: {datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")}')
        print(f'Config: {CONFIG_FILE}')
        print(f'Log: {log_path}')
        print(f'{"="*60}\n')
        
        # Check for credentials
        creds = _get_creds_headless()
        if creds:
            print(f'✓ Credentials loaded for user: {creds[0]}')
        else:
            print('⚠ WARNING: No DR credentials found (DR tasks will fail)')
            print('  Set via Windows Credential Manager or environment variables:')
            print('  - DR_USERNAME')
            print('  - DR_PASSWORD')
        
        # Load and start scheduler
        scheduler = AutoScheduler(headless=True)
        scheduler.load_config()
        
        enabled_tasks = [name for name, cfg in scheduler.config.items() if cfg['enabled']]
        if not enabled_tasks:
            print('\n⚠ WARNING: No tasks are enabled!')
            print(f'  Edit {CONFIG_FILE} to enable tasks, or run GUI mode to configure.')
            print('  Exiting...\n')
            sys.exit(0)
        
        print(f'\nEnabled tasks: {", ".join(enabled_tasks)}')
        scheduler.start_all_enabled()
        
        print('\n✓ Auto-scheduler started. Press Ctrl+C to stop.\n')
        print(f'{"="*60}\n')
        
        try:
            Event().wait()  # Wait forever
        except KeyboardInterrupt:
            print(f'\n\n{"="*60}')
            print('Received shutdown signal...')
            scheduler.stop_all()
            print('✓ All tasks stopped')
            print(f'Stopped: {datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")}')
            print(f'{"="*60}\n')
    else:
        # Run with GUI
        app = SyncApp()
        app.mainloop()

if __name__ == '__main__':
    main()
