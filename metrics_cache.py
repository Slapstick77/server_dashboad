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
        
        # Configuration table - stores logic rules and settings
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Configuration (
                id INTEGER PRIMARY KEY,
                config_key TEXT NOT NULL UNIQUE,
                config_value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        
        # Unit completion table - stores ONLY 100% complete units with their last day
        cur.execute("""
            CREATE TABLE IF NOT EXISTS UnitCompletion (
                com_number TEXT PRIMARY KEY,
                last_day_unfiltered TEXT NOT NULL,
                last_updated TEXT NOT NULL
            )
        """)
        
        # Create index for fast sorting by last day
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_unit_completion_last_day 
            ON UnitCompletion(last_day_unfiltered)
        """)
        
        conn.commit()

def save_configuration(config_key: str, config_value: Any):
    """Save configuration to database."""
    import json
    ensure_cache_tables()
    
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO Configuration (config_key, config_value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(config_key) DO UPDATE SET
                config_value = excluded.config_value,
                updated_at = excluded.updated_at
        """, (config_key, json.dumps(config_value), datetime.now().isoformat()))
        conn.commit()

def load_configuration(config_key: str) -> Any:
    """Load configuration from database."""
    import json
    ensure_cache_tables()
    
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT config_value FROM Configuration WHERE config_key = ?", (config_key,))
        row = cur.fetchone()
        if row:
            return json.loads(row[0])
        return None

def refresh_unit_completion() -> Dict[str, Any]:
    """
    Refresh the UnitCompletion table with ONLY 100% complete units.
    Includes last_day_unfiltered for each complete unit.
    Returns summary of updates.
    """
    import sys
    sys.path.insert(0, ROOT)
    from webapp.blueprints.utils import COMPLETION_CHECK_DEPARTMENTS, normalize_com, fnum
    
    ensure_cache_tables()
    
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Get all units from scheduling summary
        cur.execute('PRAGMA table_info(SCHSchedulingSummary)')
        colset = {r[1] for r in cur.fetchall()}

        base_needed = ['comnumber1']
        for _, stdc, actc, compc, effc in COMPLETION_CHECK_DEPARTMENTS:
            base_needed.extend([stdc, actc, compc, effc])
        present = [c for c in base_needed if c in colset]
        cols_sql = ','.join(f'"{c}"' for c in present) if present else 'comnumber1'

        cur.execute(f'SELECT {cols_sql} FROM SCHSchedulingSummary WHERE CAST(comnumber1 AS TEXT) GLOB "[0-9][0-9][0-9][0-9][0-9]"')
        units = [dict(r) for r in cur.fetchall()]

        # Get labor data to find last day for each unit
        cur.execute("""
            SELECT CAST(COMNumber AS TEXT) as com,
                   MAX(strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10)))) as last_day
            FROM SCHLabor
            WHERE COALESCE(ActualHours,0) > 0
              AND CAST(COMNumber AS TEXT) GLOB '[0-9][0-9][0-9][0-9][0-9]'
            GROUP BY CAST(COMNumber AS TEXT)
        """)
        last_days = {normalize_com(str(r['com'])): r['last_day'] for r in cur.fetchall() if r['last_day']}

        # Existing completion entries (append-only behaviour)
        cur.execute("SELECT com_number, last_day_unfiltered FROM UnitCompletion")
        existing_entries = {normalize_com(str(r['com_number'])): r['last_day_unfiltered'] for r in cur.fetchall()}
    
    # Calculate completion for each unit - ONLY keep complete ones
    new_completion_rows = []
    complete_count = 0
    incomplete_count = 0
    
    for r in units:
        com_raw = r.get('comnumber1')
        if not com_raw:
            continue
        
        com = normalize_com(str(com_raw))
        all_complete = True
        
        for label, stdc, actc, compc, effc in COMPLETION_CHECK_DEPARTMENTS:
            std = fnum(r.get(stdc)) if stdc in r else 0.0
            if std <= 0:
                # No work planned in this department - skip
                continue
            
            comp = fnum(r.get(compc)) if compc in r else 0.0
            if comp < 100.0:
                # Department not complete
                all_complete = False
                break
        
        if all_complete:
            complete_count += 1
            if com not in existing_entries:
                last_day = last_days.get(com)
                if last_day:
                    new_completion_rows.append((com, last_day, datetime.now().isoformat()))
                    existing_entries[com] = last_day
        else:
            incomplete_count += 1
    
    # Insert only newly completed units (append-only behavior)
    if new_completion_rows:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.executemany("""
                INSERT INTO UnitCompletion (com_number, last_day_unfiltered, last_updated)
                VALUES (?, ?, ?)
            """, new_completion_rows)
            conn.commit()
    
    return {
        'success': True,
        'total_units': len(units),
        'complete': complete_count,
        'incomplete': incomplete_count,
        'newly_added': len(new_completion_rows),
        'updated_at': datetime.now().isoformat()
    }

def get_complete_units() -> list:
    """Get list of all 100% complete unit COM numbers."""
    ensure_cache_tables()
    
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT com_number FROM UnitCompletion ORDER BY com_number")
        return [row[0] for row in cur.fetchall()]

def get_complete_units_with_dates() -> Dict[str, str]:
    """Get dict of complete units with their last_day_unfiltered. {com: last_day}"""
    ensure_cache_tables()
    
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT com_number, last_day_unfiltered FROM UnitCompletion")
        return {row[0]: row[1] for row in cur.fetchall()}

def is_unit_complete(com_number: str) -> bool:
    """Check if a specific unit is 100% complete (exists in table)."""
    ensure_cache_tables()
    
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM UnitCompletion WHERE com_number = ?", (com_number,))
        return cur.fetchone() is not None

def _compute_incomplete_units() -> Dict[str, Any]:
    """
    Compute the incomplete units data for the Recent Units page.
    This is the same expensive logic as api_incomplete() but cached.
    """
    import sys
    import re
    sys.path.insert(0, ROOT)
    from webapp.blueprints.utils import (
        TRACKED_DEPARTMENTS, COMPLETION_CHECK_DEPARTMENTS,
        normalize_com, fnum, build_unit, _recalculate_dept_stats_with_completion,
        PROJECT_DAY_RULES
    )
    
    today = date.today()
    day_60 = (today - timedelta(days=60)).isoformat()
    day_7 = (today - timedelta(days=7)).isoformat()
    
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Discover existing columns
        cur.execute('PRAGMA table_info(SCHSchedulingSummary)')
        colset = {r[1] for r in cur.fetchall()}
        
        base_needed = ['comnumber1', 'jobname']
        for _, stdc, actc, compc, effc in TRACKED_DEPARTMENTS:
            base_needed.extend([stdc, actc, compc, effc])
        present = [c for c in base_needed if c in colset]
        if not present:
            return {'count': 0, 'units': [], 'in_progress_count': 0}

        cols_sql = ','.join(f'"{c}"' for c in present)
        cur.execute(
            f'SELECT {cols_sql} FROM SCHSchedulingSummary '
            'WHERE CAST(comnumber1 AS TEXT) GLOB "[0-9][0-9][0-9][0-9][0-9]"'
        )
        sched_rows = [dict(r) for r in cur.fetchall()]

        # Labor activity windows
        cur.execute(
            """
            SELECT CAST(COMNumber AS TEXT) com,
                   MAX(strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10)))) last_day
            FROM SCHLabor
            WHERE COALESCE(ActualHours,0) > 0
              AND CAST(COMNumber AS TEXT) GLOB '[0-9][0-9][0-9][0-9][0-9]'
              AND strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10))) >= ?
            GROUP BY COMNumber
            """,
            (day_60,)
        )
        last_map_raw = {row[0]: row[1] for row in cur.fetchall() if row[1]}

        # Build day_emp structure
        cur.execute('PRAGMA table_info(SCHLabor)')
        labor_cols = {r[1] for r in cur.fetchall()}
        day_emp = {}
        if 'DepartmentNumber' in labor_cols:
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
            for com, dept, day, emp, hrs in rows:
                label = raw_code_to_label.get(dept)
                if not label or day is None:
                    continue
                key = (normalize_com(com), label)
                daymap = day_emp.setdefault(key, {})
                rec = daymap.setdefault(day, {'emps': set(), 'hours': 0.0})
                if emp:
                    rec['emps'].add(str(emp).strip())
                try:
                    rec['hours'] += float(hrs) if hrs is not None else 0.0
                except Exception:
                    pass

    # Filter by recency (last 7 days)
    fresh_coms = {normalize_com(c) for c, last in last_map_raw.items() if last >= day_7}
    norm_last_map = {normalize_com(c): last for c, last in last_map_raw.items() if last}
    
    # Build units
    units = [build_unit(r, colset) for r in sched_rows]
    units = [u for u in units if u['com'] in fresh_coms]

    # Recalculate with completion info
    stats_map, timeline_map = _recalculate_dept_stats_with_completion(units, day_emp, {}, {})

    # Attach department stats (simplified version for caching)
    def norm_dept_name(s: str):
        if s is None:
            return ''
        spaced = re.sub(r'([a-z])([A-Z])', r'\1 \2', str(s))
        return re.sub(r'[^A-Z0-9]', '', spaced.upper())

    for u in units:
        com = u['com']
        u['unit_days_worked'] = 0
        u['unit_span'] = None
        u['unit_span_status'] = 'No Data'
        
        all_dept_days = set()
        gsrc = timeline_map.get(com)
        if gsrc:
            for dept_name in u.get('departments', []):
                label = dept_name.get('name', '')
                dept_days = gsrc.get('rows', {}).get(label, [])
                if dept_days:
                    all_dept_days.update(dept_days)
        
        if all_dept_days:
            sorted_days = sorted(all_dept_days)
            u['unit_days_worked'] = len(sorted_days)
            u['unit_span_status'] = 'In Progress'
            u['first_day'] = sorted_days[0]  # Store first day for sorting
            u['last_day'] = sorted_days[-1]  # Store last day
            
            # Check if unit is complete
            is_complete = True
            for dept in u.get('departments', []):
                if dept.get('std', 0) > 0 and dept.get('completion', 0) < 99.999:
                    is_complete = False
                    break
            
            if is_complete:
                try:
                    first = date.fromisoformat(sorted_days[0])
                    last = date.fromisoformat(sorted_days[-1])
                    u['unit_span'] = (last - first).days + 1
                    u['unit_span_status'] = 'Complete'
                except Exception:
                    pass
        else:
            u['first_day'] = None
            u['last_day'] = None

    # Sort by first day (oldest first day at the top)
    units_with_days = [u for u in units if u.get('first_day')]
    units_without_days = [u for u in units if not u.get('first_day')]
    units_with_days.sort(key=lambda x: x['first_day'], reverse=False)
    units = units_with_days + units_without_days

    # Count in-progress units
    in_progress_count = sum(1 for u in units if u.get('overall_completion', 0) < 99.999)

    return {
        'count': len(units),
        'units': units,
        'in_progress_count': in_progress_count
    }

def _compute_unit_time_trends() -> Dict[str, Any]:
    """
    Compute the expensive unit time trends metrics.
    This is the same logic as api_unit_time_trends() but returns raw data.
    
    OPTIMIZED: Uses UnitCompletion table to pre-filter which units to load labor data for.
    Only loads labor data for 100% complete units (typically ~160 instead of 2000+).
    """
    # Import here to avoid circular dependencies
    import sys
    sys.path.insert(0, ROOT)
    from webapp.blueprints.utils import (
        TRACKED_DEPARTMENTS, COMPLETION_CHECK_DEPARTMENTS,
        normalize_com, fnum
    )
    
    today = date.today()
    day_90 = (today - timedelta(days=90)).isoformat()
    
    # OPTIMIZATION: Get complete units from UnitCompletion table
    complete_units_dict = get_complete_units_with_dates()  # {com: last_day}
    if not complete_units_dict:
        # No complete units - return zeros
        return {
            'last10': {'n_units': 0, 'avg_active_days': 0.0, 'avg_span_days': 0.0, 'avg_efficiency': 0.0},
            'last90d': {'n_units': 0, 'avg_active_days': 0.0, 'avg_span_days': 0.0, 'avg_efficiency': 0.0},
            'trend': {'active_days_delta': 0.0, 'span_days_delta': 0.0, 'efficiency_delta': 0.0}
        }
    
    complete_coms = list(complete_units_dict.keys())

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
        
        # OPTIMIZATION: Only load labor for complete units (filter by COM list)
        placeholders = ','.join('?' for _ in complete_coms)
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
              AND CAST(COMNumber AS TEXT) IN ({placeholders})
        """,
            complete_coms
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
    
    # OPTIMIZATION: We already know these units are 100% complete (from UnitCompletion table)
    # Still need dept_completion_map for _resolve_department_days, but skip full completion check
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
        
        # Only query for complete units
        placeholders = ','.join('?' for _ in complete_coms)
        cur.execute(
            f'SELECT {cols_sql} FROM SCHSchedulingSummary WHERE CAST(comnumber1 AS TEXT) IN ({placeholders})',
            complete_coms
        )
        completion_rows = [dict(r) for r in cur.fetchall()]
    
    # Build dept_completion_map for complete units only
    dept_completion_map = {}
    
    for r in completion_rows:
        com_raw = r.get('comnumber1')
        if not com_raw:
            continue
        com = normalize_com(str(com_raw))
        
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
            
            dept_completion_map[(com, label)] = dept_complete
    
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
    
    # Prepare list of per-COM metrics
    # OPTIMIZATION: All units are already confirmed 100% complete (from UnitCompletion table)
    items = []
    for com, rec in tmp_by_com.items():
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

def _compute_trailing_trend_charts() -> Dict[str, Any]:
    """
    Pre-compute all 6 trailing metrics trend charts (3 time ranges × 2 trailing counts).
    This is expensive but only runs during cache refresh.
    
    Returns:
        Dictionary with all 6 chart datasets:
        {
            '90_10': {labels: [...], avg_efficiency: [...], avg_act_days: [...], avg_span: [...]},
            '90_30': {...},
            '120_10': {...},
            '120_30': {...},
            '365_10': {...},
            '365_30': {...}
        }
    """
    import sys
    sys.path.insert(0, ROOT)
    from webapp.blueprints.utils import (
        TRACKED_DEPARTMENTS, COMPLETION_CHECK_DEPARTMENTS,
        normalize_com, fnum, _resolve_department_days
    )
    from collections import defaultdict
    
    print("🔄 Pre-calculating trailing trend charts (6 combinations)...")
    
    # Get all complete units with dates
    complete_units_dict = get_complete_units_with_dates()
    if not complete_units_dict:
        print("   ⚠️ No complete units found - skipping trend charts")
        return {}
    
    # Convert to sorted list
    units_list = [(com, last_day) for com, last_day in complete_units_dict.items()]
    units_list.sort(key=lambda x: x[1])
    
    # Get all COMs to load labor data ONCE for all charts
    all_coms = list(complete_units_dict.keys())
    
    # Map department codes
    raw_code_to_label = {
        '0120':'Fab','0140':'Welding','0180':'BaseFormPaint','0200':'FanAssyTest','0220':'InsulWallFab',
        '0230':'Pipe','0260':'Assembly','0270':'DoorFab','0280':'Assembly','0300':'Electrical','0320':'Pipe',
        '0340':'Paint','0360':'Test','0380':'Crating',
    }
    tracked_codes = sorted(raw_code_to_label.keys())
    codes_sql = ','.join(f"'{c}'" for c in tracked_codes)
    
    # Load ALL labor data for complete units ONCE (reuse for all 6 charts)
    print(f"   Loading labor data for {len(all_coms)} complete units...")
    placeholders = ','.join('?' for _ in all_coms)
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
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
              AND CAST(COMNumber AS TEXT) IN ({placeholders})
        """,
            all_coms
        )
        labor_rows = cur.fetchall()
        
        # Get scheduling summary for efficiency
        cur.execute('PRAGMA table_info(SCHSchedulingSummary)')
        colset = {r[1] for r in cur.fetchall()}
        base_needed = ['comnumber1']
        for _, stdc, actc, compc, effc in TRACKED_DEPARTMENTS:
            base_needed.extend([stdc, actc, compc, effc])
        present = [c for c in base_needed if c in colset]
        cols_sql = ','.join(f'"{c}"' for c in present) if present else 'comnumber1'
        
        cur.execute(
            f'SELECT {cols_sql} FROM SCHSchedulingSummary WHERE CAST(comnumber1 AS TEXT) IN ({placeholders})',
            all_coms
        )
        sched_rows = [dict(r) for r in cur.fetchall()]
    
    print(f"   Loaded {len(labor_rows)} labor rows")
    
    # Build day_emp structure (department-day aggregates)
    day_emp = {}
    for com, dept, day, emp, hrs in labor_rows:
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
        except:
            pass
    
    # Build dept completion map
    dept_completion_map = {}
    for r in sched_rows:
        com_raw = r.get('comnumber1')
        if not com_raw:
            continue
        com = normalize_com(str(com_raw))
        
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
            
            dept_completion_map[(com, label)] = dept_complete
    
    # Build per-unit metrics (act days, span) using filtering logic
    unit_metrics = {}  # {com: {'act_days': X, 'span': Y}}
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
    
    for com, rec in tmp_by_com.items():
        ds = sorted(rec['days'])
        if not ds:
            continue
        active_days = len(ds)
        try:
            span_days = (date.fromisoformat(rec['last']) - date.fromisoformat(rec['first'])).days + 1
        except:
            span_days = 0
        unit_metrics[com] = {'act_days': active_days, 'span': span_days}
    
    # Build efficiency map from scheduling summary
    eff_map = {}  # {com: efficiency}
    for r in sched_rows:
        com_raw = r.get('comnumber1')
        if not com_raw:
            continue
        com = normalize_com(str(com_raw))
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
            eff_map[com] = (total_eh / total_act) * 100.0
        else:
            eff_map[com] = 0.0
    
    # Helper to calculate metrics for a set of COMs
    def calc_avg(coms):
        if not coms:
            return {'avg_efficiency': 0, 'avg_act_days': 0, 'avg_span': 0}
        effs = [eff_map.get(c, 0) for c in coms if c in eff_map]
        acts = [unit_metrics[c]['act_days'] for c in coms if c in unit_metrics]
        spans = [unit_metrics[c]['span'] for c in coms if c in unit_metrics]
        
        def avg(vals):
            return (sum(vals) / len(vals)) if vals else 0.0
        
        return {
            'avg_efficiency': avg(effs),
            'avg_act_days': avg(acts),
            'avg_span': avg(spans)
        }
    
    # Generate all 6 chart combinations
    results = {}
    configs = [
        (90, 10), (90, 30),
        (120, 10), (120, 30),
        (365, 10), (365, 30)
    ]
    
    for days, trailing in configs:
        key = f"{days}_{trailing}"
        print(f"   Calculating {days} days × Trailing {trailing}...")
        
        end_date = date.today()
        start_date = end_date - timedelta(days=days-1)
        
        labels = []
        avg_efficiency = []
        avg_act_days = []
        avg_span = []
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.isoformat()
            labels.append(date_str)
            
            # Find trailing N units
            eligible = [(com, ld) for com, ld in units_list if ld <= date_str]
            if len(eligible) < trailing:
                trailing_units = eligible
            else:
                trailing_units = eligible[-trailing:]
            
            if not trailing_units:
                avg_efficiency.append(0)
                avg_act_days.append(0)
                avg_span.append(0)
            else:
                coms = [com for com, _ in trailing_units]
                metrics = calc_avg(coms)
                avg_efficiency.append(round(metrics['avg_efficiency'], 2))
                avg_act_days.append(round(metrics['avg_act_days'], 2))
                avg_span.append(round(metrics['avg_span'], 2))
            
            current_date += timedelta(days=1)
        
        results[key] = {
            'labels': labels,
            'avg_efficiency': avg_efficiency,
            'avg_act_days': avg_act_days,
            'avg_span': avg_span,
            'days': days,
            'trailing': trailing
        }
        print(f"   ✓ {key}: {len(labels)} data points")
    
    print(f"✅ All 6 charts pre-calculated")
    return results


def _compute_daily_hours_series(max_days: int = 90) -> Dict[str, Any]:
    """Compute total charged hours per day for the last `max_days` days."""
    max_days = max(1, min(max_days, 365))
    end_date = date.today()
    start_date = end_date - timedelta(days=max_days - 1)
    start_iso = start_date.isoformat()

    date_expr = "strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10)))"

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT {date_expr} AS day,
                   SUM(COALESCE(ActualHours,0)) AS hrs
            FROM SCHLabor
            WHERE COALESCE(ActualHours,0) > 0
              AND {date_expr} >= ?
            GROUP BY {date_expr}
            ORDER BY {date_expr}
            """,
            (start_iso,)
        )
        rows = cur.fetchall()

    day_map = {row['day']: float(row['hrs'] or 0) for row in rows if row['day']}
    labels = [(start_date + timedelta(days=i)).isoformat() for i in range(max_days)]
    totals = [day_map.get(label, 0.0) for label in labels]

    # 7-day moving average for smoother trend
    ma7 = []
    window = []
    window_sum = 0.0
    for value in totals:
        window.append(value)
        window_sum += value
        if len(window) > 7:
            window_sum -= window.pop(0)
        ma7.append(window_sum / len(window))

    return {
        'labels': labels,
        'totals': totals,
        'ma7': ma7,
        'start_date': start_iso,
        'end_date': end_date.isoformat(),
    }


def _compute_department_totals(day_windows=(30, 60, 90)) -> Dict[str, Any]:
    """Pre-compute department hour totals for the configured rolling windows."""
    if not day_windows:
        return {'windows': {}, 'max_window_days': 0}

    windows = sorted({max(1, min(365, int(w))) for w in day_windows if isinstance(w, (int, float))})
    if not windows:
        return {'windows': {}, 'max_window_days': 0}

    end_date = date.today()
    max_window = max(windows)
    start_date = end_date - timedelta(days=max_window - 1)
    start_iso = start_date.isoformat()

    raw_code_to_label = {
        '0120': 'Fab',
        '0140': 'Welding',
        '0180': 'BaseFormPaint',
        '0200': 'FanAssyTest',
        '0220': 'InsulWallFab',
        '0230': 'Pipe',
        '0260': 'Assembly',
        '0270': 'DoorFab',
        '0280': 'Assembly',
        '0300': 'Electrical',
        '0320': 'Pipe',
        '0340': 'Paint',
        '0360': 'Test',
        '0380': 'Crating',
    }
    tracked_codes = sorted(raw_code_to_label.keys())
    codes_sql = ','.join(f"'{c}'" for c in tracked_codes)
    date_expr = "strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10)))"

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT {date_expr} AS day,
                   DepartmentNumber AS dept,
                   SUM(COALESCE(ActualHours,0)) AS hrs
            FROM SCHLabor
            WHERE COALESCE(ActualHours,0) > 0
              AND DepartmentNumber IN ({codes_sql})
              AND {date_expr} >= ?
            GROUP BY {date_expr}, DepartmentNumber
            ORDER BY {date_expr}
            """,
            (start_iso,)
        )
        rows = cur.fetchall()

    window_starts = {w: end_date - timedelta(days=w - 1) for w in windows}
    accum = {w: {} for w in windows}
    totals = {w: 0.0 for w in windows}

    for row in rows:
        day_val = row['day']
        dept_code = str(row['dept'] or '').strip()
        if not day_val or not dept_code:
            continue
        label = raw_code_to_label.get(dept_code)
        if not label:
            continue
        try:
            day_date = date.fromisoformat(day_val)
        except ValueError:
            continue
        hrs = float(row['hrs'] or 0.0)
        if hrs <= 0:
            continue
        for window_days, window_start in window_starts.items():
            if day_date >= window_start:
                dept_totals = accum[window_days]
                dept_totals[label] = dept_totals.get(label, 0.0) + hrs
                totals[window_days] += hrs

    windows_payload: Dict[str, Any] = {}
    for window_days in windows:
        window_start = window_starts[window_days]
        dept_totals = accum[window_days]
        departments = sorted(
            [{'name': name, 'hours': round(value, 2)} for name, value in dept_totals.items()],
            key=lambda item: item['hours'],
            reverse=True
        )
        windows_payload[str(window_days)] = {
            'start_date': window_start.isoformat(),
            'end_date': end_date.isoformat(),
            'total_hours': round(totals[window_days], 2),
            'departments': departments,
        }

    return {
        'windows': windows_payload,
        'max_window_days': max_window,
        'tracked_departments': raw_code_to_label,
    }


def _build_daily_metric_charts(trailing_charts: Dict[str, Any], hours_series: Dict[str, Any]) -> Dict[str, Any]:
    """Derive 30/60/90 day windows for average metrics and total hours trends."""
    windows = (30, 60, 90)
    base_key = '90_10'
    base_chart = trailing_charts.get(base_key)
    if not base_chart:
        return {}

    base_labels = base_chart.get('labels', [])
    base_eff = base_chart.get('avg_efficiency', [])
    base_act = base_chart.get('avg_act_days', [])
    base_span = base_chart.get('avg_span', [])

    hours_labels = hours_series.get('labels', []) if hours_series else []
    hours_totals = hours_series.get('totals', []) if hours_series else []
    hours_ma7 = hours_series.get('ma7', []) if hours_series else []

    output = {
        'windows': {},
        'trailing_units': base_chart.get('trailing', 10),
        'hours_source_range': {
            'start_date': hours_series.get('start_date') if hours_series else None,
            'end_date': hours_series.get('end_date') if hours_series else None,
            'moving_average': 7
        },
        'max_avg_daily_hours': 0.0
    }

    for win in windows:
        window_len = min(
            win,
            len(base_labels),
            len(hours_labels) if hours_labels else win
        )
        if window_len <= 0:
            subset_labels = []
            eff = []
            act = []
            span = []
        else:
            subset_labels = base_labels[-window_len:]
            eff = base_eff[-window_len:]
            act = base_act[-window_len:]
            span = base_span[-window_len:]

        if hours_labels and window_len > 0 and len(hours_labels) >= window_len:
            hours_subset = hours_totals[-window_len:]
            ma_subset = hours_ma7[-window_len:] if hours_ma7 else []
        else:
            hours_subset = []
            ma_subset = []
        if hours_subset:
            avg_hours = sum(hours_subset) / len(hours_subset)
        else:
            avg_hours = 0.0
        if avg_hours > output['max_avg_daily_hours']:
            output['max_avg_daily_hours'] = avg_hours
        output['windows'][str(win)] = {
            'labels': subset_labels,
            'avg_efficiency': eff,
            'avg_act_days': act,
            'avg_span': span,
            'total_hours': hours_subset,
            'total_hours_ma7': ma_subset,
            'avg_daily_hours': avg_hours,
        }

    return output

def refresh_metrics_cache(trigger='manual') -> Dict[str, Any]:
    """
    Refresh all cached metrics.
    
    Args:
        trigger: What triggered the refresh (e.g., 'labor_sync', 'sched_sync', 'manual', 'logic_update')
    
    Returns:
        Dictionary with success status and timing info
    """
    ensure_cache_tables()
    
    start_time = datetime.now()
    success = True
    error_msg = None
    
    try:
        # Always refresh UnitCompletion first so trailing charts have up-to-date completions
        try:
            refresh_summary = refresh_unit_completion()
        except Exception as e:
            # Don't fail the whole refresh if completion refresh has an issue; just log
            refresh_summary = {'error': str(e)}

        # Compute the expensive metrics
        unit_trends = _compute_unit_time_trends()
        incomplete_units = _compute_incomplete_units()
        trailing_charts = _compute_trailing_trend_charts()
        daily_hours = _compute_daily_hours_series(max_days=90)
        daily_metric_charts = _build_daily_metric_charts(trailing_charts, daily_hours)
        department_totals = _compute_department_totals(day_windows=(30, 60, 90))
        
        # Store in cache table
        import json
        computed_at = datetime.now().isoformat()
        department_totals['generated_at'] = computed_at
        
        with get_conn() as conn:
            cur = conn.cursor()
            
            # Update or insert the unit time trends
            cur.execute("""
                INSERT INTO MetricsCache (metric_type, metric_data, computed_at, trigger_source)
                VALUES ('unit_time_trends', ?, ?, ?)
                ON CONFLICT(metric_type) DO UPDATE SET
                    metric_data = excluded.metric_data,
                    computed_at = excluded.computed_at,
                    trigger_source = excluded.trigger_source
            """, (json.dumps(unit_trends), computed_at, trigger))
            
            # Update or insert the incomplete units
            cur.execute("""
                INSERT INTO MetricsCache (metric_type, metric_data, computed_at, trigger_source)
                VALUES ('incomplete_units', ?, ?, ?)
                ON CONFLICT(metric_type) DO UPDATE SET
                    metric_data = excluded.metric_data,
                    computed_at = excluded.computed_at,
                    trigger_source = excluded.trigger_source
            """, (json.dumps(incomplete_units), computed_at, trigger))
            
            # Update or insert trailing trend charts
            cur.execute("""
                INSERT INTO MetricsCache (metric_type, metric_data, computed_at, trigger_source)
                VALUES ('trailing_trend_charts', ?, ?, ?)
                ON CONFLICT(metric_type) DO UPDATE SET
                    metric_data = excluded.metric_data,
                    computed_at = excluded.computed_at,
                    trigger_source = excluded.trigger_source
            """, (json.dumps(trailing_charts), computed_at, trigger))

            # Update or insert daily metric charts
            cur.execute("""
                INSERT INTO MetricsCache (metric_type, metric_data, computed_at, trigger_source)
                VALUES ('daily_metric_charts', ?, ?, ?)
                ON CONFLICT(metric_type) DO UPDATE SET
                    metric_data = excluded.metric_data,
                    computed_at = excluded.computed_at,
                    trigger_source = excluded.trigger_source
            """, (json.dumps({
                'charts': daily_metric_charts,
                'hours_source': daily_hours
            }), computed_at, trigger))

            # Update or insert department totals
            cur.execute("""
                INSERT INTO MetricsCache (metric_type, metric_data, computed_at, trigger_source)
                VALUES ('department_totals', ?, ?, ?)
                ON CONFLICT(metric_type) DO UPDATE SET
                    metric_data = excluded.metric_data,
                    computed_at = excluded.computed_at,
                    trigger_source = excluded.trigger_source
            """, (json.dumps(department_totals), computed_at, trigger))
            
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
