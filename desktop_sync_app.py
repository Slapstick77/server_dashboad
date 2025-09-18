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
import os, sqlite3, threading, csv, subprocess, sys, glob, shutil, time
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
        ttk.Label(top, text='Data Sync', font=('Segoe UI', 14, 'bold')).grid(row=0, column=0, columnspan=5, sticky='w', pady=(0,5))
        self.btn_labor = ttk.Button(top, text='Run Labor', command=self._run_labor)
        self.btn_sched = ttk.Button(top, text='Pull SCHSummary', command=self._run_sched)
        self.btn_stop  = ttk.Button(top, text='Stop', command=self._request_stop, state='disabled')
        self.btn_task  = ttk.Button(top, text='Create Task...', command=self._open_scheduler_dialog)
        for idx, btn in enumerate((self.btn_labor, self.btn_sched, self.btn_stop, self.btn_task)):
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
        for b in (self.btn_labor,self.btn_sched,self.btn_stop,self.btn_task):
            b.state(['disabled'])

    def _enable(self):
        for b in (self.btn_labor,self.btn_sched,self.btn_task):
            b.state(['!disabled'])
        self.btn_stop.state(['disabled'])

    def _run_labor(self):
        self._start_thread(self._labor_logic, 'Labor Backfill running...')

    def _run_sched(self):
        self._start_thread(self._sched_logic, 'Scheduling Summary update running...')

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
        dlg.geometry('420x260')
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
        ttk.Checkbutton(frm, text='Create Combined (Both) Task', variable=both_var).grid(row=2,column=0,columnspan=2, sticky='w', pady=(6,0))
        ttk.Checkbutton(frm, text='Create Labor Task', variable=tk.BooleanVar(value=False), state='disabled').grid(row=3,column=0,columnspan=2, sticky='w')
        ttk.Checkbutton(frm, text='Create Scheduling Summary Task', variable=tk.BooleanVar(value=False), state='disabled').grid(row=4,column=0,columnspan=2, sticky='w')
        status_lbl = ttk.Label(frm, text='', foreground='blue')
        status_lbl.grid(row=5,column=0,columnspan=2, sticky='w', pady=(8,0))
        def create_tasks():
            tm = time_var.get().strip()
            if not _valid_time(tm):
                messagebox.showerror('Invalid','Time must be HH:MM')
                return
            if both_var.get():
                ok, msg = self._create_task(prefix_var.get()+'Both', tm, 'both')
                status_lbl.config(text=msg, foreground=('green' if ok else 'red'))
        ttk.Button(frm, text='Create', command=create_tasks).grid(row=6,column=0, pady=10, sticky='w')
        ttk.Button(frm, text='Close', command=dlg.destroy).grid(row=6,column=1, pady=10, sticky='e')

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
