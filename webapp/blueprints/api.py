"""
API Blueprint - JSON endpoints

All JSON API routes are defined here.
"""
from flask import Blueprint, jsonify, request
import sqlite3
from datetime import datetime, timedelta, timezone, date
import re
from collections import defaultdict
from .utils import (
    get_conn, fnum, normalize_com, build_unit,
    _filter_days_by_gap, _resolve_department_days, _get_all_department_days,
    _recalculate_dept_stats_with_completion,
    TRACKED_DEPARTMENTS, COMPLETION_CHECK_DEPARTMENTS, GANTT_DEPT_ORDER, GANTT_INDEX,
    MIN_DAY_HOURS, OUTLIER_CAPS, DEPARTMENT_RULES, PROJECT_DAY_RULES
)

# Create blueprint
api = Blueprint('api', __name__)

# Routes will be moved here one at a time
@api.route('/api/incomplete')
def api_incomplete():
    """Return incomplete units (not 100% weighted complete) with recent labor.
    
    Now uses cached metrics for instant loading. Cache is auto-refreshed after:
    - Labor sync
    - Schedule sync
    - Logic rule updates
    - Manual refresh
    """
    import sys
    import os
    # Add parent directory to path for metrics_cache import
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
    from metrics_cache import get_cached_metrics
    
    # Try to get cached data first
    cached = get_cached_metrics('incomplete_units')
    
    if cached and 'data' in cached:
        # Return just the data portion (units, count, in_progress_count)
        return jsonify(cached['data'])
    
    # Fallback: If cache miss, compute on the fly (shouldn't happen after initial setup)
    # This is the original expensive logic kept as fallback
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
            return jsonify({'count': 0, 'units': [], 'error': 'Expected columns missing'}), 200

        cols_sql = ','.join(f'"{c}"' for c in present)
        cur.execute(f'SELECT {cols_sql} FROM SCHSchedulingSummary WHERE CAST(comnumber1 AS TEXT) GLOB "[0-9][0-9][0-9][0-9][0-9]"')
        sched_rows = [dict(r) for r in cur.fetchall()]
        sched_candidates = len(sched_rows)

        # Labor activity windows (strftime normalizes dates)
        today = date.today()
        day_60 = (today - timedelta(days=60)).isoformat()
        day_7 = (today - timedelta(days=7)).isoformat()
        # Separate threshold for including fully-complete units if they had very recent labor
        day_complete_recent = (today - timedelta(days=3)).isoformat()
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

        # Accurate day stats with Electrical exclusion of employee 1205797 (only if that is the sole worker on early days)
        cur.execute('PRAGMA table_info(SCHLabor)')
        labor_cols = {r[1] for r in cur.fetchall()}
        stats_map = {}
        timeline_map = {}
        if 'DepartmentNumber' in labor_cols:
            # Raw department code -> canonical label mapping
            # Maps labor codes from SCHLabor (via DepartmentCode table) to canonical tracked departments
            # 
            # Data Flow: SCHLabor.DepartmentNumber → DepartmentCode.DeptName → Canonical Department
            # 
            # Special mappings per specification:
            #   - 0200 FAN → FanAssyTest (canonical name in SCHSchedulingSummary)
            #   - 0260 ASSY + 0280 FLOW → Both map to Assembly (single department for filtering/metrics)
            #   - 0380 FINISH → Crating
            #   - 0300 ELEC → Electrical
            #   - 0360 TEST → Test (Gantt display only; NOT used for completion/metrics)
            raw_code_to_label = {
                '0120':'Fab',            # DepartmentCode: FAB
                '0140':'Welding',        # DepartmentCode: WELD
                '0180':'BaseFormPaint',  # DepartmentCode: FOAM/PAINT
                '0200':'FanAssyTest',    # DepartmentCode: FAN → canonical: FanAssyTest
                '0220':'InsulWallFab',   # DepartmentCode: WALL FAB
                '0230':'Pipe',           # DepartmentCode: PIPE
                '0260':'Assembly',       # DepartmentCode: ASSY → canonical: Assembly
                '0270':'DoorFab',        # DepartmentCode: DOOR
                '0280':'Assembly',       # DepartmentCode: FLOW → canonical: Assembly (merged with ASSY)
                '0300':'Electrical',     # DepartmentCode: ELEC
                '0320':'Pipe',           # DepartmentCode: PIPE (alternate code)
                '0340':'Paint',          # DepartmentCode: PAINT
                '0360':'Test',           # DepartmentCode: TEST (Gantt only, no completion tracking)
                '0380':'Crating',        # DepartmentCode: FINISH → canonical: Crating
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
            # Build nested structure by merged canonical labels
            day_emp = {}
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
            # Store day_emp data for later recalculation with completion info
            
    relax = request.args.get('relax') == '1'
    recency_cut = (today - timedelta(days=14)).isoformat() if relax else day_7
    fresh_coms = {normalize_com(c) for c, last in last_map_raw.items() if last >= recency_cut}
    # Normalized COM -> last labor day map for later inclusion of recent fully-complete units
    norm_last_map = {normalize_com(c): last for c, last in last_map_raw.items() if last}
    units = [build_unit(r, colset) for r in sched_rows]
    labor_60 = len(last_map_raw)
    labor_recent_kept = len(fresh_coms)

    # Apply recency
    before_recency = len(units)
    units = [u for u in units if u['com'] in fresh_coms]
    after_recency = len(units)

    # NOW recalculate department stats with completion information
    stats_map, timeline_map = _recalculate_dept_stats_with_completion(units, day_emp, {}, {})

    # Attach department day stats
    def norm_dept_name(s: str):
        if s is None:
            return ''
        spaced = re.sub(r'([a-z])([A-Z])', r'\1 \2', str(s))
        return re.sub(r'[^A-Z0-9]', '', spaced.upper())
    for u in units:
        for d in u['departments']:
            key = (u['com'], d['name'].upper())
            stat = stats_map.get(key)
            if not stat:
                continue
            distinct_days, first_day, last_day, meta, is_complete = stat
            if first_day:
                if is_complete and last_day:
                    end_day = last_day
                    span_status = "Complete"
                else:
                    end_day = today.isoformat()
                    span_status = "In Progress"
                try:
                    span = (date.fromisoformat(end_day) - date.fromisoformat(first_day)).days + 1
                    if span < 1:
                        span = 1
                except Exception:
                    span = None
                    span_status = "Error"
            else:
                span = None
                span_status = "No Data"
            d['days_active'] = distinct_days
            if span is not None:
                d['days_span'] = span
                d['span_status'] = span_status
            else:
                d['span_status'] = span_status
            # Preserve metadata so other layers (exports, tooltips, etc.) can use it.
            if meta:
                d.setdefault('day_rules_meta', meta)
        
        # Calculate unit-level aggregated metrics across all departments
        unit_first_day = None
        unit_last_day = None
        unit_all_days = set()
        unit_any_incomplete = False
        
        # Step 1: Check completion status using SCHSchedulingSummary departments
        completion_dept_names = {label for label, _, _, _, _ in COMPLETION_CHECK_DEPARTMENTS}
        
        for d in u['departments']:
            # Only check departments in COMPLETION_CHECK_DEPARTMENTS for completion status
            if d['name'] not in completion_dept_names:
                continue  # Skip Test and FlowLine for completion checking
            
            # Only check departments that have standard hours (meaning they're part of this unit)
            if d['std'] <= 0:
                continue  # Skip departments not applicable to this unit
            
            # Check completion percentage from scheduling summary
            if d['completion'] < 100.0:
                unit_any_incomplete = True
        
        # Step 2: Collect days and first/last from SCHLabor data (ALL departments with actual labor)
        gantt_timeline = timeline_map.get(u['com'])
        if gantt_timeline and gantt_timeline.get('rows'):
            # Collect ALL days from ALL departments that have labor data
            for dept_name, dept_days in gantt_timeline['rows'].items():
                unit_all_days.update(dept_days)
                
                # Also collect first/last days for span calculation
                key = (u['com'], dept_name.upper())
                stat = stats_map.get(key)
                if stat:
                    distinct_days, first_day, last_day, meta, is_complete = stat
                    if first_day:
                        if unit_first_day is None or first_day < unit_first_day:
                            unit_first_day = first_day
                    if last_day and is_complete:
                        if unit_last_day is None or last_day > unit_last_day:
                            unit_last_day = last_day
        
        # Per specification: Unit metrics (First/Last/Days/Span) are computed ONLY when Complete (100%)
        # If Not Started or In Progress, metrics are NULL
        if unit_any_incomplete or unit_last_day is None:
            # Incomplete unit - all metrics NULL per specification
            u['unit_days_worked'] = None
            u['unit_span'] = None
            u['unit_span_status'] = "In Progress"
            u['unit_first_day'] = None
            u['unit_last_day'] = None
        elif unit_first_day:
            # All departments complete - calculate all metrics
            u['unit_days_worked'] = len(unit_all_days)
            u['unit_first_day'] = unit_first_day
            u['unit_last_day'] = unit_last_day
            
            # Calculate actual span
            unit_end_day = unit_last_day
            try:
                unit_span = (date.fromisoformat(unit_end_day) - date.fromisoformat(unit_first_day)).days + 1
                if unit_span < 1:
                    unit_span = 1
            except Exception:
                unit_span = None
            
            u['unit_span'] = unit_span
            u['unit_span_status'] = "Complete"
        else:
            # No data at all
            u['unit_days_worked'] = None
            u['unit_span'] = None
            u['unit_span_status'] = "Not Started"
            u['unit_first_day'] = None
            u['unit_last_day'] = None
        
        # Attach Gantt timeline per-department squares
        gsrc = timeline_map.get(u['com'])
        if gsrc and gsrc.get('earliest') and gsrc.get('latest'):
            try:
                t0 = date.fromisoformat(gsrc['earliest'])
                tN = date.fromisoformat(gsrc['latest'])
                cols = (tN - t0).days + 1
                rows = []
                for d in u['departments']:
                    lbl = d['name']
                    days = gsrc['rows'].get(lbl)
                    if not days:
                        continue
                    offs = []
                    for ds in days:
                        try:
                            offs.append((date.fromisoformat(ds) - t0).days)
                        except Exception:
                            continue
                    rows.append({'label': lbl, 'offsets': sorted(set(offs))})
                if rows:
                    # Apply explicit ordering if configured
                    rows.sort(key=lambda r: GANTT_INDEX.get(r['label'], 999))
                    u['gantt'] = {'start': gsrc['earliest'], 'cols': cols, 'rows': rows}
            except Exception:
                pass

    # Final completion / hours filters
    # Attach last labor day to each unit for filtering
    try:
        norm_last_map  # ensure exists
    except NameError:
        norm_last_map = {}
    for u in units:
        if 'last_labor_day' not in u:
            u['last_labor_day'] = norm_last_map.get(u['com'])

    units_pre_hours = len(units)
    # Keep incomplete OR (recent fully complete with last charge within 3 days regardless of relax window)
    units = [u for u in units if u['overall_std'] > 0 and u['overall_act'] > 0 and (
        u['overall_completion'] < 99.999 or (u['overall_completion'] >= 99.999 and u.get('last_labor_day') and u['last_labor_day'] >= day_complete_recent)
    )]
    # Sort with completed units on top: primary by descending completion, then by remaining hours desc
    units.sort(key=lambda u: (-u['overall_completion'], -(u['overall_std'] - min(u['overall_act'], u['overall_std']))))
    final_count = len(units)
    in_progress_count = sum(1 for u in units if u['overall_completion'] < 99.999)

    if request.args.get('debug') == '1':
        return jsonify({
            'count': final_count,
            'in_progress_count': in_progress_count,
            'sched_candidates': sched_candidates,
            'before_recency': before_recency,
            'after_recency': after_recency,
            'units_pre_hours': units_pre_hours,
            'labor_60': labor_60,
            'labor_recent_kept': labor_recent_kept,
            'recency_cut_used': recency_cut,
            'relax_mode': relax,
            'sample_sched_com': [u['com'] for u in units[:5]],
            'sample_fresh_coms': list(sorted(fresh_coms))[:5],
            'units_sample': units[:15],
            'complete_recent_included': sum(1 for u in units if u['overall_completion'] >= 99.999)
        })
    return jsonify({'count': final_count, 'in_progress_count': in_progress_count, 'units': units})


@api.route('/api/metrics/unit_time_trends')
def api_unit_time_trends():
    """Get cached unit time trends metrics.
    
    If cache is missing or stale (>24 hours), falls back to live computation.
    Returns cached data with timestamp so UI can show freshness.
    """
    try:
        import sys
        import os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        from metrics_cache import get_cached_metrics
        
        cached = get_cached_metrics('unit_time_trends')
        
        if cached:
            # Check if cache is fresh (less than 24 hours old)
            from datetime import datetime as dt, timedelta
            computed_at = dt.fromisoformat(cached['computed_at'])
            age_hours = (dt.now() - computed_at).total_seconds() / 3600
            
            result = cached['data'].copy()
            result['_cache'] = {
                'computed_at': cached['computed_at'],
                'trigger_source': cached['trigger_source'],
                'age_hours': round(age_hours, 1),
                'is_stale': age_hours > 24
            }
            
            # If fresh, return cached data
            if age_hours <= 24:
                return jsonify(result)
        
        # Cache miss or stale - fall back to live computation
        # (Keep original expensive logic as fallback)
    except Exception as e:
        # If cache system fails, fall back to live computation
        print(f"Cache error: {e}")
    
    # ORIGINAL LIVE COMPUTATION (fallback when cache unavailable)
    """Compute averages for active days and span for:
    - last 10 completed units (by completion date = last labor day when overall completion >= ~100)
    - last 90 calendar days (all units with last labor day within window)

    Uses same day stats logic (MIN_DAY_HOURS, Electrical special rule) as elsewhere.
    """
    today = date.today()
    day_90 = (today - timedelta(days=90)).isoformat()

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Build department-day stats per COM (similar to api_incomplete)
        cur.execute('PRAGMA table_info(SCHLabor)')
        labor_cols = {r[1] for r in cur.fetchall()}
        if 'DepartmentNumber' not in labor_cols:
            return jsonify({'error': 'Labor table missing DepartmentNumber'}), 200

        # Map SCHLabor department codes to canonical tracked departments
        # Special: 0260 ASSY + 0280 FLOW both map to Assembly
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

    # Aggregate department-day stats with Electrical and min-hours rules
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

    com_stats = {}
    from collections import defaultdict
    
    # FIRST: Get completion status from scheduling summary for ALL COMs
    # We need this BEFORE filtering days so we can apply correct completion-aware filtering
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
    
    # Build completion maps:
    # 1. completion_map: com -> all_complete (True if ALL applicable departments are 100%)
    # 2. dept_completion_map: (com, label) -> is_dept_complete (True if THIS department is 100%)
    completion_map = {}
    dept_completion_map = {}
    
    for r in completion_rows:
        com_raw = r.get('comnumber1')
        if not com_raw:
            continue
        com = normalize_com(str(com_raw))
        all_complete = True
        
        # Check each department individually
        for label, stdc, actc, compc, effc in COMPLETION_CHECK_DEPARTMENTS:
            std = fnum(r.get(stdc)) if stdc in r else 0.0
            # Only check departments that have standard hours (are part of this unit)
            if std <= 0:
                dept_completion_map[(com, label)] = False  # Not applicable = not complete
                continue
            
            comp = fnum(r.get(compc)) if compc in r else 0.0
            # If completion is not explicitly 100%, calculate it
            dept_complete = True
            if comp < 100.0:
                act = fnum(r.get(actc)) if actc in r else 0.0
                calc_comp = min(100.0, (act / std) * 100.0) if act > 0 else 0.0
                if calc_comp < 100.0:
                    dept_complete = False
                    all_complete = False
            
            # Store individual department completion status
            dept_completion_map[(com, label)] = dept_complete
            
            if not dept_complete:
                all_complete = False
        
        completion_map[com] = all_complete
    
    # NOW: Merge per COM across departments to compute overall distinct active days and span
    # Use DEPARTMENT-SPECIFIC completion status to apply correct filtering
    tmp_by_com = defaultdict(lambda: {'days': set(), 'first': None, 'last': None})
    for (com, label), daymap in day_emp.items():
        # Apply completion-aware filtering based on THIS DEPARTMENT'S completion status
        # This allows each department to filter its last day independently when that dept is complete
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
    
    # Prepare list of per-COM metrics - ONLY include fully complete units
    items = []
    for com, rec in tmp_by_com.items():
        # Check if ALL departments are 100% complete
        if not completion_map.get(com, False):
            continue  # Skip incomplete units
        
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
        items.append({'com': com, 'first_day': first_day, 'last_day': last_day, 'active_days': active_days, 'span_days': span_days})

    # Sort completed units by last_day
    items_sorted_by_last = sorted([it for it in items if it.get('last_day')], key=lambda x: x['last_day'], reverse=True)

    # Last 10 completed units: simply take last 10 by last_day
    last10 = items_sorted_by_last[:10]

    def avg(vals):
        vals = [v for v in vals if v is not None]
        return (sum(vals)/len(vals)) if vals else 0.0

    a10_active = avg([it['active_days'] for it in last10])
    a10_span = avg([it['span_days'] for it in last10])

    # Last 90 days window: only units with last_day >= day_90
    last90_items = [it for it in items if it.get('last_day') and it['last_day'] >= day_90]
    a90_active = avg([it['active_days'] for it in last90_items])
    a90_span = avg([it['span_days'] for it in last90_items])

    # Compute average overall efficiency for these sets using scheduling summary
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

    return jsonify({
        'last10': {'n_units': len(last10), 'avg_active_days': a10_active, 'avg_span_days': a10_span, 'avg_efficiency': a10_eff},
        'last90d': {'n_units': len(last90_items), 'avg_active_days': a90_active, 'avg_span_days': a90_span, 'avg_efficiency': a90_eff},
        'trend': {
            'active_days_delta': a10_active - a90_active,
            'span_days_delta': a10_span - a90_span,
            'efficiency_delta': a10_eff - a90_eff,
        }
    })


@api.route('/api/metrics/trailing_trend')
def api_trailing_trend():
    """
    Return trailing metrics trend data for charting.
    
    NOW USES PRE-CACHED DATA - calculated during metrics refresh.
    
    Query params:
        days: 90, 120, or 365 (time window)
        trailing: 10 or 30 (number of trailing units to use per day)
    
    Returns cached chart data for instant loading.
    """
    import sys
    import os
    
    # Add parent directory for imports
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
    from metrics_cache import get_cached_metrics
    
    # Get parameters
    try:
        days = int(request.args.get('days', 120))
        if days not in [90, 120, 365]:
            days = 120
    except:
        days = 120
    
    try:
        trailing = int(request.args.get('trailing', 10))
        if trailing not in [10, 30]:
            trailing = 10
    except:
        trailing = 10
    
    # Get cached trailing trend charts
    cached = get_cached_metrics('trailing_trend_charts')
    
    if not cached or 'data' not in cached:
        return jsonify({
            'labels': [],
            'avg_efficiency': [],
            'avg_act_days': [],
            'avg_span': [],
            'error': 'Chart data not cached yet - refresh metrics cache'
        })
    
    # Get the specific chart requested
    chart_key = f"{days}_{trailing}"
    charts_data = cached['data']
    
    if chart_key not in charts_data:
        return jsonify({
            'labels': [],
            'avg_efficiency': [],
            'avg_act_days': [],
            'avg_span': [],
            'error': f'Chart {chart_key} not found in cache'
        })
    
    chart = charts_data[chart_key]
    
    return jsonify({
        'labels': chart['labels'],
        'avg_efficiency': chart['avg_efficiency'],
        'avg_act_days': chart['avg_act_days'],
        'avg_span': chart['avg_span'],
        'days': chart['days'],
        'trailing': chart['trailing'],
        '_cache': {
            'computed_at': cached.get('computed_at'),
            'trigger_source': cached.get('trigger_source')
        }
    })


@api.route('/api/metrics/daily_metric_trends')
def api_daily_metric_trends():
    """Return cached daily metric chart series (30/60/90 day windows)."""
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
    from metrics_cache import get_cached_metrics

    cached = get_cached_metrics('daily_metric_charts')

    if not cached or 'data' not in cached:
        return jsonify({
            'windows': {},
            'error': 'Daily metric charts not cached yet - refresh metrics cache'
        })

    data = cached['data']
    charts = data.get('charts', {}) if isinstance(data, dict) else {}
    hours_source = data.get('hours_source') if isinstance(data, dict) else None

    response = {
        'windows': charts.get('windows', {}),
        'trailing_units': charts.get('trailing_units'),
        'hours_source': charts.get('hours_source_range'),
        'max_avg_daily_hours': charts.get('max_avg_daily_hours'),
        'hours_series_source': hours_source,
        '_cache': {
            'computed_at': cached.get('computed_at'),
            'trigger_source': cached.get('trigger_source')
        }
    }

    return jsonify(response)


def _calculate_metrics_for_units(com_list):
    """
    Calculate avg efficiency, act days, and span for a list of COM numbers.
    Uses the same logic as the main metrics calculation (applies logic page settings).
    
    Returns: {avg_efficiency, avg_act_days, avg_span}
    """
    if not com_list:
        return {'avg_efficiency': 0, 'avg_act_days': 0, 'avg_span': 0}
    
    from datetime import date as dt_date
    from collections import defaultdict
    
    # Map department codes
    raw_code_to_label = {
        '0120':'Fab','0140':'Welding','0180':'BaseFormPaint','0200':'FanAssyTest','0220':'InsulWallFab',
        '0230':'Pipe','0260':'Assembly','0270':'DoorFab','0280':'Assembly','0300':'Electrical','0320':'Pipe',
        '0340':'Paint','0360':'Test','0380':'Crating',
    }
    tracked_codes = sorted(raw_code_to_label.keys())
    codes_sql = ','.join(f"'{c}'" for c in tracked_codes)
    
    # Load labor data for these units
    placeholders = ','.join('?' for _ in com_list)
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
            com_list
        )
        rows = cur.fetchall()
        
        # Also get scheduling summary for efficiency calculation
        cur.execute('PRAGMA table_info(SCHSchedulingSummary)')
        colset = {r[1] for r in cur.fetchall()}
        base_needed = ['comnumber1']
        for _, stdc, actc, compc, effc in TRACKED_DEPARTMENTS:
            base_needed.extend([stdc, actc, compc, effc])
        present = [c for c in base_needed if c in colset]
        cols_sql = ','.join(f'"{c}"' for c in present) if present else 'comnumber1'
        
        cur.execute(
            f'SELECT {cols_sql} FROM SCHSchedulingSummary WHERE CAST(comnumber1 AS TEXT) IN ({placeholders})',
            com_list
        )
        sched_rows = [dict(r) for r in cur.fetchall()]
    
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
        except:
            pass
    
    # Build dept completion map (all units are complete, but need per-dept status)
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
    
    # Calculate per-unit metrics using filtering logic
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
    
    # Calculate act days and span for each unit
    act_days_list = []
    span_list = []
    for com, rec in tmp_by_com.items():
        ds = sorted(rec['days'])
        if not ds:
            continue
        active_days = len(ds)
        try:
            span_days = (dt_date.fromisoformat(rec['last']) - dt_date.fromisoformat(rec['first'])).days + 1
        except:
            span_days = None
        
        act_days_list.append(active_days)
        if span_days is not None:
            span_list.append(span_days)
    
    # Calculate efficiency from scheduling summary
    effs = []
    for r in sched_rows:
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
    
    # Calculate averages
    def avg(vals):
        vals = [v for v in vals if v is not None]
        return (sum(vals) / len(vals)) if vals else 0.0
    
    return {
        'avg_efficiency': avg(effs),
        'avg_act_days': avg(act_days_list),
        'avg_span': avg(span_list)
    }



RECENT_PAGE = """<!doctype html><html><head><meta charset='utf-8'><title>Recent Unit Completion</title><style>
body{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}
header{padding:1rem 1.5rem;display:flex;justify-content:space-between;align-items:center;background:#161b22;border-bottom:1px solid #30363d}
h1{margin:0;font-size:1.05rem}
nav a{color:#8fb9ff;text-decoration:none;margin-right:.8rem;font-size:.72rem}
button{background:#238636;border:1px solid #2ea043;color:#fff;padding:.55rem .9rem;border-radius:6px;font-size:.7rem;font-weight:600;cursor:pointer}button:hover{background:#2ea043}
main{padding:1rem 1.1rem}
.pill{display:inline-block;background:#1f6feb33;border:1px solid #1f6feb55;border-radius:20px;padding:.45rem .75rem;font-size:.6rem;letter-spacing:.5px;margin:.25rem .4rem .6rem 0}
.unit{display:grid;border:1px solid #3f4751;border-radius:14px;margin:1.25rem 0;overflow:hidden;background:#141a21;font-size:.6rem;grid-template-columns:260px 1fr;transition:background .25s,border-color .25s,box-shadow .25s,transform .2s;position:relative;box-shadow:0 2px 5px -2px #000,0 0 0 1px #212a33;cursor:pointer}
.unit:hover{transform:translateY(-2px);box-shadow:0 4px 12px -2px rgba(0,0,0,.4),0 0 0 1px #3a4a5f,0 0 8px -2px rgba(31,111,235,.3);border-color:#4a5a6f}
.unit.alt{background:#10161c}
.unit:before{content:'';position:absolute;left:0;top:0;bottom:0;width:4px;background:#30363d}
.unit.eff-band-low:before{background:linear-gradient(#8b1111,#d53030)}
.unit.eff-band-mid:before{background:linear-gradient(#9a7300,#d6a400)}
.unit.eff-band-high:before{background:linear-gradient(#1d7f36,#28c14f)}
.unit.complete{background:#10291a;border-color:#2e8045;box-shadow:0 0 0 1px #2e8045,0 0 4px -1px #184d2b}
.unit-col1{grid-row:1 / span 4;padding:.75rem .95rem;border-right:1px solid #30363d;display:flex;flex-direction:column;gap:.55rem;background:linear-gradient(145deg,#12181f,#151e27 55%,#10161c)}
.com-card{background:linear-gradient(160deg,#0b141b,#0e1d28);border:1px solid #3a4a59;border-radius:12px;padding:.6rem .7rem .7rem;display:flex;flex-direction:column;gap:.6rem;box-shadow:0 2px 4px -2px #000,0 0 0 1px #18232c,0 0 10px -4px #0d3044}
.title{font-family:ui-monospace,Consolas,'Courier New',monospace;font-size:.83rem;font-weight:700;letter-spacing:.12rem;background:#0f161d;border:1px solid #2d3842;padding:.3rem .55rem .32rem;border-radius:8px;display:inline-block;box-shadow:0 0 0 1px #121a21,0 0 4px #0b0f13 inset}
.job{opacity:.7;font-size:.55rem;line-height:1.2}
.daysbox{display:flex;gap:.4rem;font-size:.55rem}
.daysbox span{background:#1d272f;padding:2px 6px;border:1px solid #2d3842;border-radius:6px}
.dept-row{display:flex;flex-wrap:wrap;gap:.4rem;padding:.5rem .7rem .55rem;border-bottom:1px solid #222b33}
.dept{flex:0 0 auto;background:#1d232a;border:1px solid #2d333b;padding:.45rem .55rem;border-radius:6px;min-width:120px;position:relative;transition:background .25s,border-color .25s}
.dept.complete{background:#142f1d;border-color:#2e8045}
.dept-name{font-size:.55rem;font-weight:600;margin-bottom:.25rem}
.bars{display:flex;flex-direction:column;gap:2px}
.bar{height:10px;background:#262c33;border-radius:5px;position:relative;overflow:hidden}
.bar span{position:absolute;left:0;top:0;bottom:0;background:linear-gradient(90deg,#ff914d,#ffcd3c)}
.bar.eff span{background:linear-gradient(90deg,#2f9e44,#52d96d)}
.bar.eff.low span{background:linear-gradient(90deg,#b32020,#ff5959)}
.bar.eff.mid span{background:linear-gradient(90deg,#c28a00,#ffd43b)}
.bar.comp span{background:linear-gradient(90deg,#4373d9,#6da8ff)}
.bar.eff.over span{background:linear-gradient(90deg,#52d96d,#2f9e44)}
.bar.comp.over span{background:linear-gradient(90deg,#6da8ff,#4373d9)}
/* Only recolor completion bars on complete items; keep efficiency threshold colors */
.dept.complete .bar.comp span,.unit.complete .bar.comp span{background:linear-gradient(90deg,#2f9e44,#52d96d)}
.ovr-rows{display:flex;flex-direction:column;gap:4px;padding:.6rem .7rem .7rem}
.unit-sep{height:16px;margin:-.4rem 0 .2rem;position:relative}
.unit-sep:after{content:"";position:absolute;left:0;right:0;top:6px;height:4px;background:linear-gradient(90deg,#141b22,#3d4a57,#141b22);opacity:.85;border-radius:2px}
.metrics{font-size:.52rem;opacity:.8;display:flex;flex-wrap:wrap;gap:.6rem}
.pct-label{font-size:.48rem;position:absolute;right:4px;top:0;bottom:0;display:flex;align-items:center;font-weight:600;text-shadow:0 0 2px #000}
</style></head><body><header><h1>Recent Unit Completion</h1><div><nav><a href='/dash'>&larr; Dashboard</a></nav><button onclick='loadData()'>Refresh</button></div></header><main>
<div id='loading' style='font-size:.7rem;opacity:.7;'>Loading...</div><div id='summary'></div><div id='units'></div>
</main><script>
function pctText(v){if(v>100){return '100%+'}return v.toFixed(1)+'%'}
function makeBar(pct,cls){
    let extra='';
    if(cls==='eff'){
        if(pct<45) extra=' low'; else if(pct<65) extra=' mid';
    }
    const div=document.createElement('div');div.className='bar '+cls+extra+(pct>100?' over':'');
    const span=document.createElement('span');span.style.width=Math.min(pct,100)+'%';div.appendChild(span);
    const lab=document.createElement('div');lab.className='pct-label';lab.textContent=pctText(pct);div.appendChild(lab);return div}
async function loadData(){
    const l=document.getElementById('loading');l.style.display='block';
    const r=await fetch('/api/incomplete');const data=await r.json();
    const unitsDiv=document.getElementById('units');const sDiv=document.getElementById('summary');
    unitsDiv.innerHTML='';sDiv.innerHTML='';
    let tEff=0,tComp=0;data.units.forEach(u=>{tEff+=u.overall_efficiency;tComp+=u.overall_completion});
    const inProg = (typeof data.in_progress_count==='number') ? data.in_progress_count : data.units.filter(u=>u.overall_completion<99.999).length;
    const denom = data.count||data.units.length||1;
    const avgEff=denom?(tEff/denom).toFixed(1):'0.0';
    const avgComp=denom?(tComp/denom).toFixed(1):'0.0';
    sDiv.innerHTML=`<span class='pill'>${inProg} Units in Progress</span><span class='pill'>Avg Eff ${avgEff}%</span><span class='pill'>Avg Comp ${avgComp}%</span>`;
    data.units.forEach((u,idx)=>{
        if(idx>0){const sep=document.createElement('div');sep.className='unit-sep';unitsDiv.appendChild(sep);}        
        const unit=document.createElement('div');unit.className='unit'+(idx%2===1?' alt':'')+(u.overall_completion>=100?' complete':'');
        // Efficiency band accent
        if(u.overall_efficiency<45) unit.classList.add('eff-band-low'); else if(u.overall_efficiency<65) unit.classList.add('eff-band-mid'); else unit.classList.add('eff-band-high');
        // Make unit clickable - navigate to COM lookup page
        unit.addEventListener('click', function(){ window.location.href='/com?com='+encodeURIComponent(u.com); });
        // Left merged column
    const c1=document.createElement('div');c1.className='unit-col1';
    const card=document.createElement('div');card.className='com-card';
    const title=document.createElement('div');title.className='title';title.textContent=u.com; card.appendChild(title);
    const job=document.createElement('div');job.className='job';job.textContent=u.jobname||'';card.appendChild(job);
    // Unit-level days/span metrics (aggregated across all departments)
    const daysBox=document.createElement('div');daysBox.className='daysbox';
    // Only show Days if unit has data
    if(u.unit_days_worked!==undefined && u.unit_days_worked > 0) {
        daysBox.innerHTML+=`<span>Days ${u.unit_days_worked}</span>`;
    }
    // Only show Span if unit is complete (has actual span calculated)
    if(u.unit_span!==undefined && u.unit_span !== null) {
        daysBox.innerHTML+=`<span>Span ${u.unit_span}</span>`;
    } else if(u.unit_span_status && u.unit_span_status !== 'Complete') {
        // Show status only (In Progress, No Data, Error) without span number
        daysBox.innerHTML+=`<span>${u.unit_span_status}</span>`;
    }
    card.appendChild(daysBox);
    const metrics=document.createElement('div');metrics.className='metrics';
    metrics.textContent=`Std ${u.overall_std}h  Act ${u.overall_act}h  EH ${u.overall_eff_actual}h`;
    card.appendChild(metrics);
    c1.appendChild(card);
    unit.appendChild(c1);
        // Row 1: departments (dual bars per dept)
        const deptRow=document.createElement('div');deptRow.className='dept-row';
    u.departments.forEach(d=>{if(d.std<=0) return; const isDeptComplete=d.completion>=100; const box=document.createElement('div');box.className='dept'+(isDeptComplete?' complete':'');
                const name=document.createElement('div');name.className='dept-name';name.textContent=d.name;box.appendChild(name);
                const bars=document.createElement('div');bars.className='bars';
    // Efficiency bar top (always threshold-based coloring)
    bars.appendChild(makeBar(d.efficiency,'eff'));
    // Completion bar bottom
    bars.appendChild(makeBar(d.completion,'comp'));
                box.appendChild(bars);
                deptRow.appendChild(box);
        });
        unit.appendChild(deptRow);
        // Overall bars rows (2 rows)
        const overallWrap=document.createElement('div');overallWrap.className='ovr-rows';
        const effBar=makeBar(u.overall_efficiency,'eff');
        const compBar=makeBar(u.overall_completion,'comp');
        const effLabel=document.createElement('div');effLabel.style.cssText='font-size:.5rem;margin-top:2px;';effLabel.textContent='Overall Efficiency';
        const compLabel=document.createElement('div');compLabel.style.cssText='font-size:.5rem;margin-top:6px;';compLabel.textContent='Overall Completion';
        overallWrap.appendChild(effLabel);overallWrap.appendChild(effBar);overallWrap.appendChild(compLabel);overallWrap.appendChild(compBar);
        unit.appendChild(overallWrap);
        unitsDiv.appendChild(unit);
    });
    l.style.display='none';
}
loadData();
</script></body></html>"""

@api.route('/api/employee/suggest')
def api_employee_suggest():
    """Suggest employee names based on last/first prefixes.

    Query params: last, first; returns up to 15 distinct names.
    """
    last = (request.args.get('last') or '').strip()
    first = (request.args.get('first') or '').strip()
    last_up = last.upper()
    first_up = first.upper()
    conds = ["EmployeeName IS NOT NULL AND TRIM(EmployeeName) <> ''"]
    params = []
    # Build robust conditions using SQLite string funcs
    if last:
        conds.append("UPPER(TRIM(CASE WHEN INSTR(EmployeeName, ',')>0 THEN SUBSTR(EmployeeName,1,INSTR(EmployeeName, ',')-1) ELSE EmployeeName END)) LIKE ?")
        params.append(last_up + '%')
    if first:
        conds.append("UPPER(TRIM(CASE WHEN INSTR(EmployeeName, ',')>0 THEN SUBSTR(EmployeeName,INSTR(EmployeeName, ',')+1) ELSE '' END)) LIKE ?")
        params.append(first_up + '%')
    where_sql = ' AND '.join(conds)
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(f"""
            SELECT DISTINCT TRIM(EmployeeName) AS EmployeeName
            FROM SCHLabor
            WHERE {where_sql}
            ORDER BY EmployeeName
            LIMIT 15
        """, params)
        names = [r['EmployeeName'] for r in cur.fetchall() if r['EmployeeName']]
    out = []
    for nm in names:
        parts = nm.split(',', 1)
        last_name = parts[0].strip()
        first_name = parts[1].strip() if len(parts) > 1 else ''
        out.append({'name': nm, 'last': last_name, 'first': first_name})
    return jsonify({'suggestions': out})


@api.route('/api/employee/lookup')
def api_employee_lookup():
    """Look up employee names with IDs for exclusion management.

    Query param: q (search term); returns name and EmployeeNumber1.
    """
    query = (request.args.get('q') or '').strip()
    if not query or len(query) < 2:
        return jsonify({'employees': []})
    
    query_up = query.upper()
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT TRIM(EmployeeName) AS EmployeeName, EmployeeNumber1
            FROM SCHLabor
            WHERE EmployeeName IS NOT NULL 
            AND TRIM(EmployeeName) <> ''
            AND EmployeeNumber1 IS NOT NULL
            AND UPPER(EmployeeName) LIKE ?
            ORDER BY EmployeeName
            LIMIT 20
        """, [f'%{query_up}%'])
        
        employees = []
        for row in cur.fetchall():
            if row['EmployeeName'] and row['EmployeeNumber1']:
                employees.append({
                    'name': row['EmployeeName'],
                    'id': str(row['EmployeeNumber1'])
                })
    
    return jsonify({'employees': employees})


@api.route('/api/employee/stats')
def api_employee_stats():
    """Return employee charge stats for an optional date range.

    Query: last, first, start (YYYY-MM-DD), end (YYYY-MM-DD).
    Returns: overall first/last charge dates, total hours in period, and breakdown by department with percent.
    """
    last = (request.args.get('last') or '').strip()
    first = (request.args.get('first') or '').strip()
    start = (request.args.get('start') or '').strip() or None
    end = (request.args.get('end') or '').strip() or None
    if not last and not first:
        return jsonify({'error': 'Provide at least last or first name'}), 400

    last_up = last.upper()
    first_up = first.upper()

    # Name match conditions
    name_conds = [
        "EmployeeName IS NOT NULL AND TRIM(EmployeeName) <> ''",
    ]
    params_all = []
    if last:
        name_conds.append("UPPER(TRIM(CASE WHEN INSTR(EmployeeName, ',')>0 THEN SUBSTR(EmployeeName,1,INSTR(EmployeeName, ',')-1) ELSE EmployeeName END)) = ?")
        params_all.append(last_up)
    if first:
        name_conds.append("UPPER(TRIM(CASE WHEN INSTR(EmployeeName, ',')>0 THEN SUBSTR(EmployeeName,INSTR(EmployeeName, ',')+1) ELSE '' END)) LIKE ?")
        params_all.append(first_up + '%')
    name_where = ' AND '.join(name_conds)

    date_expr = "strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10)))"

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Overall first/last dates for employee (no date filter)
        cur.execute(f"""
            SELECT MIN({date_expr}) as first_day, MAX({date_expr}) as last_day
            FROM SCHLabor
            WHERE {name_where}
              AND COALESCE(ActualHours,0) > 0
        """, params_all)
        row = cur.fetchone()
        overall_first = row['first_day'] if row and row['first_day'] else None
        overall_last = row['last_day'] if row and row['last_day'] else None

        # Period filter
        period_conds = [name_where, "COALESCE(ActualHours,0) > 0"]
        params_period = list(params_all)
        if start:
            period_conds.append(f"{date_expr} >= ?")
            params_period.append(start)
        if end:
            period_conds.append(f"{date_expr} <= ?")
            params_period.append(end)
        period_where = ' AND '.join(period_conds)

        # By department hours (date-range)
        cur.execute(f"""
            SELECT COALESCE(DepartmentNumber,'') AS DeptCode,
                   SUM(COALESCE(ActualHours,0)) AS Hours
            FROM SCHLabor
            WHERE {period_where}
            GROUP BY DepartmentNumber
            ORDER BY DeptCode
        """, params_period)
        dept_rows = [dict(r) for r in cur.fetchall()]

        total_hours = sum(float(r.get('Hours') or 0) for r in dept_rows)
        # Map department code to friendly label
        raw_code_to_label = {
            '0120':'Fab', '0140':'Welding', '0180':'BaseFormPaint', '0200':'FanAssyTest', '0220':'InsulWallFab',
            '0230':'Pipe', '0260':'Assembly', '0270':'DoorFab', '0280':'Assembly', '0300':'Electrical', '0320':'Pipe',
            '0340':'Paint', '0360':'Test', '0380':'Crating',
        }
        by_dept = []
        for r in dept_rows:
            code = (r.get('DeptCode') or '').strip()
            label = raw_code_to_label.get(code) or code or 'UNKNOWN'
            hrs = float(r.get('Hours') or 0)
            pct = (hrs / total_hours * 100.0) if total_hours > 0 else 0.0
            by_dept.append({'code': code, 'label': label, 'hours': round(hrs,2), 'percent': pct})

        # Lifetime totals by department (all-time)
        cur.execute(f"""
            SELECT COALESCE(DepartmentNumber,'') AS DeptCode,
                   SUM(COALESCE(ActualHours,0)) AS Hours
            FROM SCHLabor
            WHERE {name_where}
              AND COALESCE(ActualHours,0) > 0
            GROUP BY DepartmentNumber
            ORDER BY DeptCode
        """, params_all)
        dept_all_rows = [dict(r) for r in cur.fetchall()]
        total_all_hours = sum(float(r.get('Hours') or 0) for r in dept_all_rows)
        by_dept_all = []
        for r in dept_all_rows:
            code = (r.get('DeptCode') or '').strip()
            label = raw_code_to_label.get(code) or code or 'UNKNOWN'
            hrs = float(r.get('Hours') or 0)
            pct = (hrs / total_all_hours * 100.0) if total_all_hours > 0 else 0.0
            by_dept_all.append({'code': code, 'label': label, 'hours': round(hrs,2), 'percent': pct})

    # Compose display name
    display_name = (last + ', ' + first).strip(', ')
    return jsonify({
        'name': display_name,
        'last': last,
        'first': first,
        'overall_first': overall_first,
        'overall_last': overall_last,
        'start': start,
        'end': end,
        'total_hours': round(total_hours,2),
        'by_dept': by_dept,
        'by_dept_all': by_dept_all,
    })


@api.route('/api/metrics/department_totals')
def api_department_totals():
    """Return total charged hours per canonical department for the last N days (default 30)."""
    try:
        days = int(request.args.get('days', '30'))
    except Exception:
        days = 30
    if days < 1:
        days = 30
    days = min(days, 365)

    window_key = str(days)

    # Attempt to use cached department totals for supported windows
    cached = None
    try:
        import sys
        import os
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
        from metrics_cache import get_cached_metrics
        cached = get_cached_metrics('department_totals')
    except Exception:
        cached = None

    if cached and cached.get('data'):
        data = cached['data']
        windows = data.get('windows', {}) if isinstance(data, dict) else {}
        if window_key in windows:
            window = windows[window_key]
            generated = data.get('generated_at') or cached.get('computed_at')
            response = {
                'days': days,
                'start_date': window.get('start_date'),
                'end_date': window.get('end_date'),
                'total_hours': round((window.get('total_hours') or 0), 2),
                'departments': window.get('departments', []),
                'generated_at': generated,
            }
            cache_info = {
                'computed_at': cached.get('computed_at'),
                'trigger_source': cached.get('trigger_source')
            }
            if cache_info['computed_at'] or cache_info['trigger_source']:
                response['_cache'] = cache_info
            return jsonify(response)

    # Fallback: compute on demand (supports arbitrary day windows)
    today = date.today()
    start = (today - timedelta(days=days-1)).isoformat()

    raw_code_to_label = {
        '0120':'Fab','0140':'Welding','0180':'BaseFormPaint','0200':'FanAssyTest','0220':'InsulWallFab',
        '0230':'Pipe','0260':'Assembly','0270':'DoorFab','0280':'Assembly','0300':'Electrical','0320':'Pipe',
        '0340':'Paint','0360':'Test','0380':'Crating',
    }
    codes_sql = ','.join(f"'{c}'" for c in raw_code_to_label.keys())
    date_expr = "strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10)))"

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT DepartmentNumber AS dept,
                   SUM(COALESCE(ActualHours,0)) AS hrs
            FROM SCHLabor
            WHERE COALESCE(ActualHours,0) > 0
              AND DepartmentNumber IN ({codes_sql})
              AND {date_expr} >= ?
            GROUP BY DepartmentNumber
            """,
            (start,)
        )
        rows = cur.fetchall()

    totals = {}
    grand_total = 0.0
    for r in rows:
        label = raw_code_to_label.get(str(r['dept']))
        if not label:
            continue
        hrs = float(r['hrs'] or 0)
        totals[label] = totals.get(label, 0.0) + hrs
        grand_total += hrs

    departments = [
        {'name': name, 'hours': round(value, 2)}
        for name, value in sorted(totals.items(), key=lambda item: item[1], reverse=True)
    ]

    return jsonify({
        'days': days,
        'start_date': start,
        'end_date': today.isoformat(),
        'total_hours': round(grand_total, 2),
        'generated_at': datetime.datetime.utcnow().isoformat() + 'Z',
        'departments': departments,
    })


@api.route('/api/metrics/daily_hours')
def api_daily_hours():
    """Return daily hours per department and total for the last N days (default 60),
    including a 7-day moving average series.
    """
    try:
        days = int(request.args.get('days', '60'))
    except Exception:
        days = 60
    today = date.today()
    start = (today - timedelta(days=days-1)).isoformat()

    # Map raw department codes to labels
    raw_code_to_label = {
        '0120':'Fab','0140':'Welding','0180':'BaseFormPaint','0200':'FanAssyTest','0220':'InsulWallFab',
        '0230':'Pipe','0260':'Assembly','0270':'DoorFab','0280':'Assembly','0300':'Electrical','0320':'Pipe',
        '0340':'Paint','0360':'Test','0380':'Crating',
    }

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10))) AS day,
                   DepartmentNumber AS dept,
                   SUM(COALESCE(ActualHours,0)) AS hrs
            FROM SCHLabor
            WHERE COALESCE(ActualHours,0) > 0
              AND strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10))) >= ?
            GROUP BY day, DepartmentNumber
            ORDER BY day
            """, (start,)
        )
        rows = cur.fetchall()

    # Build series per department label
    from collections import defaultdict
    dates = [ (today - timedelta(days=i)).isoformat() for i in range(days-1, -1, -1) ]
    per_label = {lbl: {d:0.0 for d in dates} for lbl in set(raw_code_to_label.values())}
    total = {d:0.0 for d in dates}
    for r in rows:
        day = r['day']
        dept = r['dept']
        hrs = float(r['hrs'] or 0)
        if not day or day not in total:
            continue
        label = raw_code_to_label.get(str(dept))
        if not label:
            continue
        per_label[label][day] = per_label[label].get(day, 0.0) + hrs
        total[day] += hrs

    def moving_avg(series_dict, window=7):
        ds = dates
        vals = [series_dict[d] for d in ds]
        out = []
        s = 0.0
        from collections import deque
        q = deque()
        for v in vals:
            q.append(v)
            s += v
            if len(q) > window:
                s -= q.popleft()
            out.append(s/len(q))
        return out

    result = {
        'dates': dates,
        'per_dept': {},
        'total': {
            'hours': [total[d] for d in dates],
            'ma7': moving_avg(total, 7),
        }
    }
    for lbl, m in per_label.items():
        result['per_dept'][lbl] = {
            'hours': [m[d] for d in dates],
            'ma7': moving_avg(m, 7),
        }
    return jsonify(result)
@api.route('/api/com/charges')
def api_com_charges():
    """List charges for a COM#: who, how much, and date they charged it.

    Query params:
      - com (string): COM# — can be any format; we'll use the last 5 digits
      - exclude_emp (string): '1' to exclude exclusion employees
      - apply_logic (string): '1' to apply gap filtering and min hours rules
    Returns: rows of {day, employee, dept, hours}
    """
    raw = (request.args.get('com') or '').strip()
    com = normalize_com(raw)
    digits = ''.join(ch for ch in com if ch.isdigit())
    if len(digits) < 5:
        return jsonify({'rows': [], 'error': 'Provide a valid COM# (at least 5 digits)'}), 200
    target = digits[-5:]
    
    # Always compute filtering metadata (no longer controlled by user toggles)
    exclude_emp = False  # Not used for filtering anymore, only for metadata
    apply_logic = True   # Always compute what would be filtered
    
    date_expr = "strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10)))"
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        # Verify table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='SCHLabor'")
        if cur.fetchone() is None:
            return jsonify({'rows': [], 'error': 'SCHLabor table not found'}), 200
        sql = f"""
            SELECT {date_expr} AS day,
                   TRIM(COALESCE(EmployeeName,'')) AS employee,
                   EmployeeNumber1 AS emp_num,
                   COALESCE(DepartmentNumber,'') AS dept,
                   COALESCE(ActualHours,0) AS hours
            FROM SCHLabor
            WHERE COALESCE(ActualHours,0) > 0
              AND substr('00000'||CAST(COMNumber AS TEXT), -5) = ?
            ORDER BY day, employee
        """
        cur.execute(sql, (target,))
        raw_rows = [dict(r) for r in cur.fetchall()]
    
    # Always compute filtering metadata (removed the early return for unfiltered data)
    
    # Apply filtering
    raw_code_to_label = {
        '0120':'Fab','0140':'Welding','0180':'BaseFormPaint','0200':'FanAssyTest','0220':'InsulWallFab',
        '0230':'Pipe','0260':'Assembly','0270':'DoorFab','0280':'Assembly','0300':'Electrical','0320':'Pipe',
        '0340':'Paint','0360':'Test','0380':'Crating',
    }
    
    # Aggregate by department-day
    day_emp = {}
    for r in raw_rows:
        dept = r['dept']
        day = r['day']
        emp = r['emp_num']
        hrs = r['hours']
        label = raw_code_to_label.get(dept)
        if not label or day is None:
            continue
        dm = day_emp.setdefault(label, {})
        rec = dm.setdefault(day, {'emps': set(), 'hours': 0.0})
        if emp:
            rec['emps'].add(str(emp).strip())
        try:
            rec['hours'] += float(hrs) if hrs is not None else 0.0
        except Exception:
            pass
    
    # Get completion status if applying logic filtering
    dept_completion_map = {}
    if apply_logic:
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
            cur.execute(f'SELECT {cols_sql} FROM SCHSchedulingSummary WHERE substr("00000"||CAST(comnumber1 AS TEXT), -5) = ?', (target,))
            completion_row = cur.fetchone()
        
        if completion_row:
            r = dict(completion_row)
            for label, stdc, actc, compc, effc in COMPLETION_CHECK_DEPARTMENTS:
                std = fnum(r.get(stdc)) if stdc in r else 0.0
                # Skip departments with no standard hours (not applicable to this unit)
                if std <= 0:
                    continue
                comp = fnum(r.get(compc)) if compc in r else 0.0
                dept_complete = True
                if comp < 100.0:
                    act = fnum(r.get(actc)) if actc in r else 0.0
                    calc_comp = min(100.0, (act / std) * 100.0) if act > 0 else 0.0
                    if calc_comp < 100.0:
                        dept_complete = False
                dept_completion_map[label] = dept_complete
    
    # Apply filtering for each department and track reasons
    filtered_days_by_dept = {}
    filter_reasons_by_dept = {}
    dept_results = {}
    for label, daymap in day_emp.items():
        if apply_logic:
            is_dept_complete = dept_completion_map.get(label, False)
            use_days, meta = _resolve_department_days(label, daymap, is_complete=is_dept_complete)
            meta.setdefault('filter_reasons', {})
            meta.setdefault('dropped_before_fab_gap', [])
            meta.setdefault('dropped_after_assembly_gap', [])
            dept_results[label] = {
                'use_days': set(use_days),
                'meta': meta,
            }
        else:
            # Only apply exclusion employee filter
            use_days = sorted(daymap.keys())
            if exclude_emp:
                excl_employees = PROJECT_DAY_RULES.get('exclusion_employees', [])
                if excl_employees:
                    use_days = [d for d in use_days if not daymap[d]['emps'].issubset(set(excl_employees))]
            dept_results[label] = {
                'use_days': set(use_days),
                'meta': {
                    'filter_reasons': {},
                    'dropped_before_fab_gap': [],
                    'dropped_after_assembly_gap': [],
                }
            }
    
    if apply_logic and dept_results:
        try:
            pre_gap_days = max(0, int(PROJECT_DAY_RULES.get('pre_fab_gap_days', 10)))
        except Exception:
            pre_gap_days = 0
        try:
            post_gap_days = max(0, int(PROJECT_DAY_RULES.get('post_assembly_gap_days', 10)))
        except Exception:
            post_gap_days = 0

        fab_days_sorted = sorted(dept_results.get('Fab', {}).get('use_days', []))
        assembly_days_sorted = sorted(dept_results.get('Assembly', {}).get('use_days', []))

        fab_window_start = None
        if fab_days_sorted:
            try:
                fab_anchor = date.fromisoformat(fab_days_sorted[0])
                fab_window_start = fab_anchor - timedelta(days=pre_gap_days)
            except Exception:
                fab_window_start = None

        assembly_window_end = None
        if assembly_days_sorted:
            try:
                assembly_anchor = date.fromisoformat(assembly_days_sorted[-1])
                assembly_window_end = assembly_anchor + timedelta(days=post_gap_days)
            except Exception:
                assembly_window_end = None

        for label, payload in dept_results.items():
            use_days_set = payload['use_days']
            meta = payload['meta']
            trimmed_days = set()
            for day in sorted(use_days_set):
                try:
                    day_obj = date.fromisoformat(day)
                except Exception:
                    day_obj = None

                dropped = False
                if day_obj and fab_window_start and day_obj < fab_window_start:
                    meta['dropped_before_fab_gap'].append(day)
                    meta['filter_reasons'][day] = (
                        f'Dropped by Fab lead-in window (> {pre_gap_days} day gap before Fab)'
                    )
                    dropped = True
                if day_obj and assembly_window_end and day_obj > assembly_window_end:
                    meta['dropped_after_assembly_gap'].append(day)
                    meta['filter_reasons'][day] = (
                        f'Dropped by Assembly tail window (> {post_gap_days} day gap after Assembly)'
                    )
                    dropped = True

                if not dropped:
                    trimmed_days.add(day)

            payload['use_days'] = trimmed_days
            filter_reasons_by_dept[label] = meta['filter_reasons']
            filtered_days_by_dept[label] = trimmed_days
    else:
        for label, payload in dept_results.items():
            filter_reasons_by_dept[label] = payload['meta'].get('filter_reasons', {})
            filtered_days_by_dept[label] = payload['use_days']
    for label, payload in dept_results.items():
        filtered_days_by_dept[label] = payload['use_days']
        filter_reasons_by_dept[label] = payload['meta'].get('filter_reasons', {})
    # Check unit completion status (100% complete across all applicable departments)
    # Only check departments that have standard hours (are in dept_completion_map)
    unit_is_complete = False
    if dept_completion_map:
        unit_is_complete = all(dept_completion_map[label] for label in dept_completion_map)
    
    # Build output rows with filtering metadata - ALWAYS return all rows
    output_rows = []
    excl_employees_set = set(PROJECT_DAY_RULES.get('exclusion_employees', []))
    
    for r in raw_rows:
        dept = r['dept']
        day = r['day']
        emp_num = str(r['emp_num']).strip() if r['emp_num'] else ''
        label = raw_code_to_label.get(dept)
        
        is_excluded = emp_num in excl_employees_set
        is_filtered = False
        filter_reason = None
        
        # Check if this charge would be filtered out by logic filtering
        # A charge is filtered if:
        # 1. The employee is NOT excluded (excluded charges are marked separately)
        # 2. AND the day is not in the filtered set (meaning it was filtered by gap/hours rules)
        if label and not is_excluded:
            day_in_filtered_set = day in filtered_days_by_dept.get(label, set())
            if not day_in_filtered_set:
                is_filtered = True
                # Get the specific reason why this day was filtered
                filter_reason = filter_reasons_by_dept.get(label, {}).get(day, 'Filtered by logic rules')
        
        row_data = {
            'day': day,
            'employee': r['employee'],
            'dept': dept,
            'hours': r['hours'],
            'is_excluded_employee': is_excluded,
            'is_filtered_out': is_filtered,
            'filter_reason': filter_reason
        }
        
        output_rows.append(row_data)
    
    return jsonify({'rows': output_rows, 'count': len(output_rows), 'com': target, 'unit_complete': unit_is_complete})
@api.route('/api/com/employee_totals')
def api_com_employee_totals():
    raw = (request.args.get('com') or '').strip()
    com = normalize_com(raw)
    digits = ''.join(ch for ch in com if ch.isdigit())
    if len(digits) < 5:
        return jsonify({'rows': [], 'error': 'Provide a valid COM# (at least 5 digits)'}), 200
    target = digits[-5:]
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='SCHLabor'")
        if cur.fetchone() is None:
            return jsonify({'rows': [], 'error': 'SCHLabor table not found'}), 200
        # Detect available employee identifier
        cur.execute("PRAGMA table_info(SCHLabor)")
        cols = {r[1] for r in cur.fetchall()}
        emp_name = 'EmployeeName' if 'EmployeeName' in cols else None
        emp_num = 'EmployeeNumber1' if 'EmployeeNumber1' in cols else None
        if not emp_name and not emp_num:
            return jsonify({'rows': [], 'error': 'No employee fields found'}), 200
        if emp_name and emp_num:
            sel_emp = f"TRIM(COALESCE({emp_name},'')) AS employee_raw, CAST({emp_num} AS TEXT) AS emp_id"
        elif emp_name and not emp_num:
            sel_emp = f"TRIM(COALESCE({emp_name},'')) AS employee_raw, '' AS emp_id"
        else:  # no name, only number
            sel_emp = f"'' AS employee_raw, CAST({emp_num} AS TEXT) AS emp_id"
        sql = (
            "SELECT " + sel_emp + ", SUM(COALESCE(ActualHours,0)) AS hours, COUNT(*) AS entries "
            + "FROM SCHLabor WHERE COALESCE(ActualHours,0) > 0 AND substr('00000'||CAST(COMNumber AS TEXT), -5) = ? "
            + "GROUP BY employee_raw, emp_id ORDER BY hours DESC, employee_raw"
        )
        cur.execute(sql, (target,))
        raw = [dict(r) for r in cur.fetchall()]
    # Normalize employee display
    rows = []
    for r in raw:
        name = (r.get('employee_raw') or '').strip()
        emp_id = (r.get('emp_id') or '').strip()
        disp = name if name else (('Unknown ' + emp_id) if emp_id else 'Unknown')
        rows.append({'employee': disp, 'hours': r.get('hours'), 'entries': r.get('entries')})
    return jsonify({'rows': rows, 'count': len(rows), 'com': target})
@api.route('/api/parts/search')
def api_parts_search():
    """Search PartsTracker by part number or COM number.

    Query params:
      - part: substring match across likely part number columns
      - com: exact or substring match across any column containing 'com'
      - limit: optional, default 200
    """
    part = (request.args.get('part') or '').strip()
    com = (request.args.get('com') or '').strip()
    try:
        limit = int(request.args.get('limit','200'))
    except Exception:
        limit = 200
    if not part and not com:
        return jsonify({'rows': []})
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        # Discover columns on PartsTracker
        try:
            cur.execute('PRAGMA table_info(PartsTracker)')
            cols = [r[1] for r in cur.fetchall()]
        except Exception:
            return jsonify({'error': 'PartsTracker table not found'}), 200
        if not cols:
            return jsonify({'rows': []})
        # Build candidate columns
        lowcols = [c.lower() for c in cols]
        part_like_cols = [c for c in cols if any(k in c.lower() for k in ('part','item','sku','stock'))]
        com_like_cols = [c for c in cols if 'com' in c.lower()]
        where = []
        params = []
        if part:
            if not part_like_cols:
                part_like_cols = cols  # fallback
            ors = []
            for c in part_like_cols:
                ors.append(f'CAST("{c}" AS TEXT) LIKE ?')
                params.append('%'+part+'%')
            where.append('(' + ' OR '.join(ors) + ')')
        if com:
            search_cols = com_like_cols or cols
            ors = []
            for c in search_cols:
                ors.append(f'CAST("{c}" AS TEXT) LIKE ?')
                params.append('%'+com+'%')
            where.append('(' + ' OR '.join(ors) + ')')
        wsql = ' AND '.join(where) if where else '1=1'
        cur.execute(f'SELECT * FROM PartsTracker WHERE {wsql} ORDER BY id DESC LIMIT ?', (*params, limit))
        raw = [dict(r) for r in cur.fetchall()]
    # Filter out metadata columns the user doesn't want to see
    hide = {'id','_row_num','_file_mtime','_file_size','_key','_row_hash','_ingested_at'}
    rows = [ {k:v for k,v in rec.items() if k not in hide} for rec in raw ]
    # Return trimmed/proportional subset of columns: include metadata and common fields
    # Let frontend render dynamically
    return jsonify({'rows': rows, 'count': len(rows)})


@api.route('/api/dr-live')
def api_dr_live():
    """Get live DR data for dashboard - DRs created in last N days, sorted by touch"""
    from datetime import datetime, timedelta
    
    days = int(request.args.get('days', 7))  # Default to 7 days as requested
    cutoff = datetime.now() - timedelta(days=days)
    
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        # Get the most recent snapshot of each DR (latest run_id per deviation_number)
        # Filter by DRs CREATED in the last N days
        # Sort by most recently TOUCHED
        sql = '''
            WITH LatestDRs AS (
                SELECT deviation_number, MAX(run_id) as latest_run
                FROM DRItemSnapshot
                GROUP BY deviation_number
            ),
            -- Get the EARLIEST routing step (by DateTouched) = creator/original
            FirstRouting AS (
                SELECT deviation_number, MIN(DateTouched) as earliest_date
                FROM DRRoutingStep
                GROUP BY deviation_number
            ),
            CreatorInfo AS (
                SELECT r.deviation_number, 
                       r.UserName as creator_name, 
                       r.UserComments as creator_comment,
                       r.step_index
                FROM DRRoutingStep r
                INNER JOIN FirstRouting f ON r.deviation_number = f.deviation_number 
                    AND r.DateTouched = f.earliest_date
            ),
            CreatorInfoDeduped AS (
                SELECT deviation_number,
                       MAX(creator_name) as creator_name,
                       MAX(creator_comment) as creator_comment
                FROM CreatorInfo
                GROUP BY deviation_number
            ),
            -- Get the LATEST routing step with a non-NULL comment
            LatestRouting AS (
                SELECT deviation_number, MAX(DateTouched) as latest_date
                FROM DRRoutingStep
                WHERE UserComments IS NOT NULL
                GROUP BY deviation_number
            ),
            LatestCommentInfo AS (
                SELECT r.deviation_number, 
                       r.UserName as latest_user, 
                       r.UserComments as latest_comment,
                       r.DateTouched as latest_comment_date
                FROM DRRoutingStep r
                INNER JOIN LatestRouting l ON r.deviation_number = l.deviation_number 
                    AND r.DateTouched = l.latest_date
            ),
            LatestCommentDeduped AS (
                SELECT deviation_number,
                       MAX(latest_user) as latest_user,
                       MAX(latest_comment) as latest_comment,
                       MAX(latest_comment_date) as latest_comment_date
                FROM LatestCommentInfo
                GROUP BY deviation_number
            )
            SELECT 
                d.deviation_number,
                d.current_routing,
                d.deviation_state,
                d.creation_comments,
                d.latest_routing_touched,
                d.latest_routing_comment,
                d.latest_routing_user,
                d.comnumber1,
                m.urgency,
                m.date_created,
                m.user_created,
                c.creator_name,
                c.creator_comment,
                lc.latest_user as routing_latest_user,
                lc.latest_comment as routing_latest_comment,
                lc.latest_comment_date as routing_latest_comment_date
            FROM DRItemSnapshot d
            INNER JOIN LatestDRs l ON d.deviation_number = l.deviation_number AND d.run_id = l.latest_run
            LEFT JOIN DRStaticMetadata m ON d.deviation_number = m.deviation_number
            LEFT JOIN CreatorInfoDeduped c ON d.deviation_number = c.deviation_number
            LEFT JOIN LatestCommentDeduped lc ON d.deviation_number = lc.deviation_number
            WHERE m.date_created IS NOT NULL
              AND datetime(m.date_created) >= datetime(?)
              AND LOWER(d.deviation_state) != 'complete'
            ORDER BY datetime(d.latest_routing_touched) DESC
        '''
        
        cur.execute(sql, (cutoff.isoformat(),))
        rows = cur.fetchall()
        
        results = []
        for r in rows:
            # Parse dates for client-side timer calculation
            # Timestamps in DB are stored as UTC but without timezone indicator
            created_ms = None
            if r['date_created']:
                try:
                    # Parse as naive datetime then treat as UTC
                    dt = datetime.fromisoformat(r['date_created'].split('.')[0])  # Remove microseconds
                    # Add UTC timezone
                    dt = dt.replace(tzinfo=timezone.utc)
                    # Convert to epoch milliseconds
                    created_ms = int(dt.timestamp() * 1000)
                except:
                    pass
            
            # Use latest_routing_touched for time in current route
            touched_ms = None
            if r['latest_routing_touched']:
                try:
                    # Parse as naive datetime then treat as UTC
                    dt = datetime.fromisoformat(r['latest_routing_touched'].split('.')[0])  # Remove microseconds
                    # Add UTC timezone
                    dt = dt.replace(tzinfo=timezone.utc)
                    # Convert to epoch milliseconds
                    touched_ms = int(dt.timestamp() * 1000)
                except:
                    pass
            
            # Get timestamp of latest comment (for completion time calculation)
            latest_comment_ms = None
            if r['routing_latest_comment_date']:
                try:
                    # Parse as naive datetime then treat as UTC
                    dt = datetime.fromisoformat(r['routing_latest_comment_date'].split('.')[0])  # Remove microseconds
                    # Add UTC timezone
                    dt = dt.replace(tzinfo=timezone.utc)
                    # Convert to epoch milliseconds
                    latest_comment_ms = int(dt.timestamp() * 1000)
                except:
                    pass
            
            results.append({
                'deviation_number': r['deviation_number'],
                'current_routing': r['current_routing'],
                'state': r['deviation_state'],
                'creation_comments': r['creation_comments'],
                'latest_comment': r['routing_latest_comment'],  # From DRRoutingStep latest with comment
                'latest_user': r['routing_latest_user'],  # From DRRoutingStep latest with comment
                'creator_user': r['creator_name'],  # From DRRoutingStep earliest by DateTouched
                'creator_comment': r['creator_comment'],  # Original comment from creator
                'com': r['comnumber1'],
                'urgency': r['urgency'],
                'created_ms': created_ms,
                'touched_ms': touched_ms,  # This is the latest_routing_touched
                'latest_comment_ms': latest_comment_ms  # Timestamp of latest comment
            })
        
        # Get the last poll run time
        cur.execute('''
            SELECT generated_at_utc 
            FROM DRPollRun 
            ORDER BY id DESC 
            LIMIT 1
        ''')
        last_poll_row = cur.fetchone()
        last_poll_ms = None
        if last_poll_row and last_poll_row[0]:
            try:
                dt = datetime.fromisoformat(last_poll_row[0].split('.')[0])
                dt = dt.replace(tzinfo=timezone.utc)
                last_poll_ms = int(dt.timestamp() * 1000)
            except:
                pass
        
        return jsonify({
            'drs': results,
            'last_poll_ms': last_poll_ms
        })




