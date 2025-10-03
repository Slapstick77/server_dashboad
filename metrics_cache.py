"""
Metrics Cache System

Pre-calculates and stores dashboard metrics to avoid heavy computation on every page load.
Metrics are automatically refreshed after data syncs.

Tables:
    - MetricsCache: Stores computed metrics with timestamps
    - MetricsRefreshLog: Tracks when and why metrics were refreshed

Usage:
    from metrics_cache import refresh_metrics_cache
    
    # After any data update:
    refresh_metrics_cache(trigger='labor_sync')
"""
import sqlite3
import os
from datetime import datetime, date, timedelta
from typing import Dict, Any

ROOT = os.path.dirname(__file__)
DB_PATH = os.path.join(ROOT, 'SCHLabor.db')

def get_conn():
    """Get database connection."""
    return sqlite3.connect(DB_PATH)

def ensure_cache_tables():
    """Create cache tables if they don't exist."""
    with get_conn() as conn:
        cur = conn.cursor()
        
        # Main cache table - stores the computed metrics
        cur.execute("""
            CREATE TABLE IF NOT EXISTS MetricsCache (
                id INTEGER PRIMARY KEY,
                metric_type TEXT NOT NULL UNIQUE,
                metric_data TEXT NOT NULL,
                computed_at TEXT NOT NULL,
                trigger_source TEXT
            )
        """)
        
        # Log table - tracks refresh history
        cur.execute("""
            CREATE TABLE IF NOT EXISTS MetricsRefreshLog (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                refreshed_at TEXT NOT NULL,
                trigger_source TEXT NOT NULL,
                duration_seconds REAL,
                success INTEGER DEFAULT 1,
                error_message TEXT
            )
        """)
        
        conn.commit()

def _compute_unit_time_trends() -> Dict[str, Any]:
    """
    Compute the expensive unit time trends metrics.
    This is the same logic as api_unit_time_trends() but returns raw data.
    """
    # Import here to avoid circular dependencies
    import sys
    sys.path.insert(0, os.path.join(ROOT, 'webapp'))
    from blueprints.utils import (
        TRACKED_DEPARTMENTS, COMPLETION_CHECK_DEPARTMENTS,
        normalize_com, fnum
    )
    
    today = date.today()
    day_90 = (today - timedelta(days=90)).isoformat()

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Check for DepartmentNumber column
        cur.execute('PRAGMA table_info(SCHLabor)')
        labor_cols = {r[1] for r in cur.fetchall()}
        if 'DepartmentNumber' not in labor_cols:
            return {'error': 'Labor table missing DepartmentNumber'}

        # Map department codes
        raw_code_to_label = {
            '0120':'Fab','0140':'Welding','0180':'BaseFormPaint','0200':'FanAssyTest','0220':'InsulWallFab',
            '0230':'Pipe','0260':'Assembly','0270':'DoorFab','0280':'Assembly','0300':'Electrical','0320':'Pipe',
            '0340':'Paint','0360':'Test','0380':'Crating',
        }
        tracked_codes = sorted(raw_code_to_label.keys())
        codes_sql = ','.join(f"'{c}'" for c in tracked_codes)
        
        cur.execute(
            f"""
            SELECT CAST(COMNumber AS TEXT) com,
                   DepartmentNumber dept,
                   strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10))) day,
                   EmployeeNumber1 emp,
                   COALESCE(ActualHours,0) hrs
            FROM SCHLabor
            WHERE COALESCE(ActualHours,0) > 0
              AND DepartmentNumber IN ({codes_sql})
              AND CAST(COMNumber AS TEXT) GLOB '[0-9][0-9][0-9][0-9][0-9]'
        """
        )
        rows = cur.fetchall()

    # Import the helper function
    from webapp.blueprints.api import _resolve_department_days
    
    # Aggregate department-day stats
    day_emp = {}
    for com, dept, day, emp, hrs in rows:
        label = raw_code_to_label.get(dept)
        if not label or day is None:
            continue
        key = (normalize_com(com), label)
        dm = day_emp.setdefault(key, {})
        rec = dm.setdefault(day, {'emps': set(), 'hours': 0.0})
        if emp:
            rec['emps'].add(str(emp).strip())
        try:
            rec['hours'] += float(hrs) if hrs is not None else 0.0
        except Exception:
            pass

    from collections import defaultdict
    
    # Get completion status from scheduling summary
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('PRAGMA table_info(SCHSchedulingSummary)')
        colset = {r[1] for r in cur.fetchall()}
        base_needed = ['comnumber1']
        for _, stdc, actc, compc, effc in TRACKED_DEPARTMENTS:
            base_needed.extend([stdc, actc, compc, effc])
        present = [c for c in base_needed if c in colset]
        cols_sql = ','.join(f'"{c}"' for c in present) if present else 'comnumber1'
        cur.execute(f'SELECT {cols_sql} FROM SCHSchedulingSummary WHERE CAST(comnumber1 AS TEXT) GLOB "[0-9][0-9][0-9][0-9][0-9]"')
        completion_rows = [dict(r) for r in cur.fetchall()]
    
    # Build completion maps
    completion_map = {}
    dept_completion_map = {}
    
    for r in completion_rows:
        com_raw = r.get('comnumber1')
        if not com_raw:
            continue
        com = normalize_com(str(com_raw))
        all_complete = True
        
        for label, stdc, actc, compc, effc in COMPLETION_CHECK_DEPARTMENTS:
            std = fnum(r.get(stdc)) if stdc in r else 0.0
            if std <= 0:
                dept_completion_map[(com, label)] = False
                continue
            
            comp = fnum(r.get(compc)) if compc in r else 0.0
            dept_complete = True
            if comp < 100.0:
                act = fnum(r.get(actc)) if actc in r else 0.0
                calc_comp = min(100.0, (act / std) * 100.0) if act > 0 else 0.0
                if calc_comp < 100.0:
                    dept_complete = False
                    all_complete = False
            
            dept_completion_map[(com, label)] = dept_complete
            
            if not dept_complete:
                all_complete = False
        
        completion_map[com] = all_complete
    
    # Merge per COM across departments
    tmp_by_com = defaultdict(lambda: {'days': set(), 'first': None, 'last': None})
    for (com, label), daymap in day_emp.items():
        is_dept_complete = dept_completion_map.get((com, label), False)
        use_days, meta = _resolve_department_days(label, daymap, is_complete=is_dept_complete)
        if not use_days:
            continue
        com_rec = tmp_by_com[com]
        com_rec['days'].update(use_days)
        first_day = min(use_days)
        last_day = max(use_days)
        if com_rec['first'] is None or first_day < com_rec['first']:
            com_rec['first'] = first_day
        if com_rec['last'] is None or last_day > com_rec['last']:
            com_rec['last'] = last_day
    
    # Prepare list of per-COM metrics - only fully complete units
    items = []
    for com, rec in tmp_by_com.items():
        if not completion_map.get(com, False):
            continue
        
        ds = sorted(rec['days'])
        if not ds:
            continue
        first_day = rec['first']
        last_day = rec['last']
        active_days = len(ds)
        try:
            span_days = (date.fromisoformat(last_day) - date.fromisoformat(first_day)).days + 1
        except Exception:
            span_days = None
        items.append({
            'com': com, 
            'first_day': first_day, 
            'last_day': last_day, 
            'active_days': active_days, 
            'span_days': span_days
        })

    # Sort by last_day
    items_sorted_by_last = sorted(
        [it for it in items if it.get('last_day')], 
        key=lambda x: x['last_day'], 
        reverse=True
    )

    # Last 10 completed units
    last10 = items_sorted_by_last[:10]

    def avg(vals):
        vals = [v for v in vals if v is not None]
        return (sum(vals)/len(vals)) if vals else 0.0

    a10_active = avg([it['active_days'] for it in last10])
    a10_span = avg([it['span_days'] for it in last10])

    # Last 90 days window
    last90_items = [it for it in items if it.get('last_day') and it['last_day'] >= day_90]
    a90_active = avg([it['active_days'] for it in last90_items])
    a90_span = avg([it['span_days'] for it in last90_items])

    # Compute efficiency
    def avg_overall_eff_for_coms(com_list):
        if not com_list:
            return 0.0
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('PRAGMA table_info(SCHSchedulingSummary)')
            colset = {r[1] for r in cur.fetchall()}
            base_needed = ['comnumber1']
            for _, stdc, actc, compc, effc in TRACKED_DEPARTMENTS:
                base_needed.extend([stdc, actc, compc, effc])
            present = [c for c in base_needed if c in colset]
            if not present:
                return 0.0
            cols_sql = ','.join(f'"{c}"' for c in present)
            placeholders = ','.join('?' for _ in com_list)
            cur.execute(
                f"SELECT {cols_sql} FROM SCHSchedulingSummary WHERE CAST(comnumber1 AS TEXT) IN ({placeholders})",
                list(com_list)
            )
            rows = [dict(r) for r in cur.fetchall()]
        if not rows:
            return 0.0
        effs = []
        for r in rows:
            total_std = total_act = total_eh = 0.0
            for label, stdc, actc, compc, effc in TRACKED_DEPARTMENTS:
                std = fnum(r.get(stdc)) if stdc in r else 0.0
                act = fnum(r.get(actc)) if actc in r else 0.0
                comp = fnum(r.get(compc)) if compc in r else 0.0
                if (comp <= 0 or comp > 100.0) and std > 0:
                    comp = min(100.0, (act / std) * 100.0) if act > 0 else 0.0
                comp = max(0.0, comp)
                eh = (comp / 100.0) * std
                total_std += std
                total_act += act
                total_eh += eh
            if total_act > 0:
                effs.append((total_eh / total_act) * 100.0)
        return (sum(effs)/len(effs)) if effs else 0.0

    last10_coms = {it['com'] for it in last10}
    last90_coms = {it['com'] for it in last90_items}
    a10_eff = avg_overall_eff_for_coms(last10_coms)
    a90_eff = avg_overall_eff_for_coms(last90_coms)

    return {
        'last10': {
            'n_units': len(last10), 
            'avg_active_days': a10_active, 
            'avg_span_days': a10_span, 
            'avg_efficiency': a10_eff
        },
        'last90d': {
            'n_units': len(last90_items), 
            'avg_active_days': a90_active, 
            'avg_span_days': a90_span, 
            'avg_efficiency': a90_eff
        },
        'trend': {
            'active_days_delta': a10_active - a90_active,
            'span_days_delta': a10_span - a90_span,
            'efficiency_delta': a10_eff - a90_eff,
        }
    }

def refresh_metrics_cache(trigger='manual') -> Dict[str, Any]:
    """
    Refresh all cached metrics.
    
    Args:
        trigger: What triggered the refresh (e.g., 'labor_sync', 'sched_sync', 'manual')
    
    Returns:
        Dictionary with success status and timing info
    """
    ensure_cache_tables()
    
    start_time = datetime.now()
    success = True
    error_msg = None
    
    try:
        # Compute the expensive metrics
        unit_trends = _compute_unit_time_trends()
        
        # Store in cache table
        import json
        computed_at = datetime.now().isoformat()
        
        with get_conn() as conn:
            cur = conn.cursor()
            
            # Update or insert the cached metrics
            cur.execute("""
                INSERT INTO MetricsCache (metric_type, metric_data, computed_at, trigger_source)
                VALUES ('unit_time_trends', ?, ?, ?)
                ON CONFLICT(metric_type) DO UPDATE SET
                    metric_data = excluded.metric_data,
                    computed_at = excluded.computed_at,
                    trigger_source = excluded.trigger_source
            """, (json.dumps(unit_trends), computed_at, trigger))
            
            conn.commit()
        
    except Exception as e:
        success = False
        error_msg = str(e)
    
    # Log the refresh
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO MetricsRefreshLog (refreshed_at, trigger_source, duration_seconds, success, error_message)
            VALUES (?, ?, ?, ?, ?)
        """, (end_time.isoformat(), trigger, duration, 1 if success else 0, error_msg))
        conn.commit()
    
    return {
        'success': success,
        'duration_seconds': duration,
        'error': error_msg,
        'trigger': trigger
    }

def get_cached_metrics(metric_type='unit_time_trends') -> Dict[str, Any]:
    """
    Get cached metrics from database.
    
    Returns:
        Dictionary with the cached data and metadata, or None if not cached
    """
    ensure_cache_tables()
    
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT metric_data, computed_at, trigger_source
            FROM MetricsCache
            WHERE metric_type = ?
        """, (metric_type,))
        
        row = cur.fetchone()
        if not row:
            return None
        
        import json
        data = json.loads(row[0])
        
        return {
            'data': data,
            'computed_at': row[1],
            'trigger_source': row[2]
        }
