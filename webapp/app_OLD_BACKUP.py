"""
SQRS Dashboard - Production Application (Blueprint Architecture)

Refactored application using Flask blueprints for clean separation of concerns:
- dashboard.py: HTML page routes
- api.py: JSON API endpoints  
- admin.py: Administrative functions
- utils.py: Shared helpers and constants

All routes now in blueprints. This file just registers them.
"""
from flask import Flask
import os
import sys

# Add webapp to path
sys.path.insert(0, os.path.dirname(__file__))

# Import blueprints
from blueprints.dashboard import dashboard
from blueprints.api import api
from blueprints.admin import admin

# Create Flask app
app = Flask(__name__)

# Register blueprints
app.register_blueprint(dashboard)
app.register_blueprint(api)
app.register_blueprint(admin)


if __name__ == '__main__':  # pragma: no cover
        emps_last = len(valid_emps_last)
        if gap_last > cap and emps_last < min_employees:
            days = days[:-1]
    return days


def _resolve_department_days(label: str, daymap: dict, is_complete: bool = True) -> tuple[list, dict]:
    """Apply project-wide day filtering rules for a department.

    Returns a tuple of (kept_days, metadata) so callers can reuse the same
    logic the Gantt view depends on. Metadata currently exposes the original
    `total_days_seen` and any `dropped_for_hours` list for debugging/exports.
    """
    days = sorted(daymap.keys())
    meta = {
        'total_days_seen': len(days),
        'dropped_for_hours': [],
        'dropped_for_exclusion': [],
        'dropped_for_gap': [],
        'filter_reasons': {}  # day -> reason string
    }

    # Apply exclusion employees for first/last day calculations
    excl_employees = PROJECT_DAY_RULES.get('exclusion_employees', [])
    excl_set = set(excl_employees)
    if excl_employees:
        excluded_days = [d for d in days if daymap[d]['emps'].issubset(excl_set)]
        meta['dropped_for_exclusion'] = excluded_days
        for d in excluded_days:
            meta['filter_reasons'][d] = 'All employees on exclusion list'
        days = [d for d in days if d not in excluded_days]

    # Get department-specific rules or fall back to global defaults
    dept_rules = PROJECT_DAY_RULES.get('department_rules', {}).get(label, {})
    min_hours = dept_rules.get('min_hours', PROJECT_DAY_RULES.get('min_total_hours', MIN_DAY_HOURS))
    min_employees_override = dept_rules.get('min_employees', PROJECT_DAY_RULES.get('min_employees_override', 2))
    
    use_days = []
    for dday in days:
        # Count valid (non-excluded) employees
        all_emps = daymap[dday]['emps']
        valid_emps = all_emps - excl_set
        num_valid_emps = len(valid_emps)
        total_hours = daymap[dday]['hours']
        
        # Check if day meets hours threshold
        below_hours = total_hours + 1e-9 < min_hours
        
        # Employee override: if >= min_employees_override valid employees, KEEP the day even if below hours
        if num_valid_emps >= min_employees_override:
            use_days.append(dday)
            continue
        
        # Not enough employees to override - filter if below hours threshold
        if below_hours:
            meta['dropped_for_hours'].append(dday)
            meta['filter_reasons'][dday] = f'Below minimum hours threshold ({total_hours:.2f}h < {min_hours}h) and <{min_employees_override} valid employees ({num_valid_emps})'
            continue
        
        # Has enough hours and not filtered - KEEP
        use_days.append(dday)

    if use_days:
        days_before_gap = set(use_days)
        use_days = _filter_days_by_gap(label, sorted(use_days), daymap, is_complete, excl_employees, min_employees_override)
        days_after_gap = set(use_days)
        gap_filtered = days_before_gap - days_after_gap
        if gap_filtered:
            meta['dropped_for_gap'] = list(gap_filtered)
            cap = OUTLIER_CAPS.get(label, 0)
            excl_set = set(excl_employees)
            for d in gap_filtered:
                all_emps = (daymap.get(d) or {}).get('emps', set())
                valid_emps = len(all_emps - excl_set)
                total_emps = len(all_emps)
                meta['filter_reasons'][d] = f'Gap filtering: >{cap} day gap, <{min_employees_override} valid employees ({valid_emps} valid, {total_emps} total)'

    return use_days, meta


def _get_all_department_days(label: str, daymap: dict) -> list:
    """Get ALL department days for Gantt display (no gap filtering, only hours threshold).
    
    This is used for Gantt charts to show the complete picture and help identify issues.
    Only applies minimum hours threshold and exclusion employees.
    """
    days = sorted(daymap.keys())
    
    # Apply exclusion employees for first/last day calculations
    excl_employees = PROJECT_DAY_RULES.get('exclusion_employees', [])
    if excl_employees:
        days = [d for d in days if not daymap[d]['emps'].issubset(set(excl_employees))]

    min_hours = PROJECT_DAY_RULES.get('min_total_hours', MIN_DAY_HOURS)
    use_days = []
    for dday in days:
        total_hours = daymap[dday]['hours']
        if total_hours + 1e-9 >= min_hours:
            use_days.append(dday)
    
    return use_days


def _recalculate_dept_stats_with_completion(units: list, day_emp: dict, stats_map: dict, timeline_map: dict):
    """Recalculate department stats considering completion status for each unit/department."""
    
    # Create a mapping of COM -> department -> completion
    completion_map = {}
    for unit in units:
        com = unit['com']
        completion_map[com] = {}
        for dept in unit['departments']:
            completion_map[com][dept['name']] = dept['completion']
    
    # Recalculate stats with completion-aware filtering
    new_stats_map = {}
    new_timeline_map = {}
    
    for (com, label), daymap in day_emp.items():
        # Get completion status for this COM/department combination
        is_complete = completion_map.get(com, {}).get(label, 0.0) >= 100.0 - 1e-6
        
        # Use completion-aware filtering for metrics
        use_days, meta = _resolve_department_days(label, daymap, is_complete)
        
        # Use unfiltered days for Gantt display
        all_days = _get_all_department_days(label, daymap)
        
        if use_days:
            first_day = use_days[0]
            last_day = use_days[-1]
            new_stats_map[(com, label.upper())] = (
                len(use_days),
                first_day,
                last_day,
                meta,
                is_complete,  # Add completion status
            )
        
        # Always use all days for Gantt timeline (to show issues)
        if all_days:
            tm = new_timeline_map.setdefault(com, {'rows': {}, 'earliest': None, 'latest': None})
            tm['rows'][label] = list(all_days)
            first_all = all_days[0]
            last_all = all_days[-1]
            if first_all and (tm['earliest'] is None or first_all < tm['earliest']):
                tm['earliest'] = first_all
            if last_all and (tm['latest'] is None or last_all > tm['latest']):
                tm['latest'] = last_all
    
    return new_stats_map, new_timeline_map


def build_unit(row: dict, colset: set) -> dict:
    """Construct a unit dict with per-department metrics and overall rollups.

    - Dept completion: prefer completion column; fallback to min(100, act/std*100).
    - Dept earned hours (EH): completion% * std / 100.
    - Dept efficiency: if not provided, EH/act*100 (0 if act==0). Can exceed 100.
    - Overall completion: sum(EH)/sum(std) * 100.
    - Overall efficiency: sum(EH)/sum(act) * 100.
    """
    com = normalize_com(row.get('comnumber1'))
    jobname = (row.get('jobname') or '').strip()

    departments = []
    total_std = 0.0
    total_act = 0.0
    total_eh = 0.0

    for label, stdc, actc, compc, effc in TRACKED_DEPARTMENTS:
        if stdc not in colset and actc not in colset and compc not in colset and effc not in colset:
            continue
        std = fnum(row.get(stdc)) if stdc in colset else 0.0
        act = fnum(row.get(actc)) if actc in colset else 0.0
        comp = fnum(row.get(compc)) if compc in colset else 0.0
        eff = fnum(row.get(effc)) if effc in colset else 0.0

        # Fallback completion when blank or zero but hours exist
        if (comp <= 0 or comp > 100.0) and std > 0:
            comp = min(100.0, (act / std) * 100.0) if act > 0 else 0.0
        comp = max(0.0, comp)

        # Earned hours based on completion
        eh = (comp / 100.0) * std

        # Fallback efficiency
        if eff <= 0.0:
            eff = (eh / act) * 100.0 if act > 0 else 0.0

        departments.append({
            'name': label,
            'std': round(std, 2),
            'act': round(act, 2),
            'completion': comp,
            'efficiency': eff,
        })

        total_std += std
        total_act += act
        total_eh += eh

    overall_completion = (total_eh / total_std * 100.0) if total_std > 0 else 0.0
    overall_efficiency = (total_eh / total_act * 100.0) if total_act > 0 else 0.0

    return {
        'com': com,
        'jobname': jobname,
        'departments': departments,
        'overall_std': round(total_std, 2),
        'overall_act': round(total_act, 2),
        'overall_eff_actual': round(total_eh, 2),
        'overall_completion': overall_completion,
        'overall_efficiency': overall_efficiency,
    }


@app.route('/api/incomplete')
def api_incomplete():
    """Return incomplete units (not 100% weighted complete) with recent labor.

    Rules:
      1. Start from scheduling summary rows whose COM is a 5‑digit number.
      2. Weighted completion from department completion columns.
      3. Labor recency: labor within 60 days AND last labor within 7 days (14 if relax=1 in debug).
      4. Keep std>0, act>0, completion < ~100.
    """
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
        today = datetime.date.today()
        day_60 = (today - datetime.timedelta(days=60)).isoformat()
        day_7 = (today - datetime.timedelta(days=7)).isoformat()
        # Separate threshold for including fully-complete units if they had very recent labor
        day_complete_recent = (today - datetime.timedelta(days=3)).isoformat()
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
    recency_cut = (today - datetime.timedelta(days=14)).isoformat() if relax else day_7
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
                    span = (datetime.date.fromisoformat(end_day) - datetime.date.fromisoformat(first_day)).days + 1
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
                unit_span = (datetime.date.fromisoformat(unit_end_day) - datetime.date.fromisoformat(unit_first_day)).days + 1
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
                t0 = datetime.date.fromisoformat(gsrc['earliest'])
                tN = datetime.date.fromisoformat(gsrc['latest'])
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
                            offs.append((datetime.date.fromisoformat(ds) - t0).days)
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


@app.route('/api/metrics/unit_time_trends')
def api_unit_time_trends():
    """Compute averages for active days and span for:
    - last 10 completed units (by completion date = last labor day when overall completion >= ~100)
    - last 90 calendar days (all units with last labor day within window)

    Uses same day stats logic (MIN_DAY_HOURS, Electrical special rule) as elsewhere.
    """
    today = datetime.date.today()
    day_90 = (today - datetime.timedelta(days=90)).isoformat()

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
            span_days = (datetime.date.fromisoformat(last_day) - datetime.date.fromisoformat(first_day)).days + 1
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


@app.route('/')
def root():
        # Redirect to the new dashboard landing
        return redirect(url_for('dash'))


@app.route('/dash')
def dash():
        # Minimal landing page with buttons to reports
        page = """
        <!doctype html>
        <html><head><meta charset='utf-8'><title>Dashboard</title>
        <style>
            body{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}
            header{padding:1rem 1.5rem;display:flex;justify-content:space-between;align-items:center;background:#161b22;border-bottom:1px solid #30363d}
            h1{margin:0;font-size:1.15rem}
            main{padding:1.5rem}
            .grid{display:grid;gap:1rem;grid-template-columns:repeat(auto-fill,minmax(260px,1fr))}
            .card{background:#11161d;border:1px solid #2a323c;border-radius:12px;padding:1rem 1.1rem;box-shadow:0 6px 18px -8px #000}
            .card h2{margin:.1rem 0 .6rem;font-size:.95rem}
            .card p{margin:0 0 1rem;opacity:.8;font-size:.75rem}
            .actions{display:flex;gap:.6rem;flex-wrap:wrap}
            a.btn,button{background:#238636;border:1px solid #2ea043;color:#fff;padding:.55rem .9rem;border-radius:8px;font-size:.8rem;font-weight:700;cursor:pointer;text-decoration:none;display:inline-block}
            a.btn.secondary{background:#1f6feb;border-color:#3a78e0}
            a.btn:hover{filter:brightness(1.05)}
            .metrics{display:flex;gap:1rem;flex-wrap:wrap;margin:1.2rem 0 1.6rem}
            .pill{display:inline-block;background:#0f1a2a;border:1px solid #2a3b55;border-radius:16px;padding:.8rem 1rem;box-shadow:0 4px 10px -6px #000;min-width:240px}
            .kpi-title{font-size:.85rem;opacity:.85;margin-bottom:.2rem}
            .kpi-value{font-size:1.8rem;font-weight:800;letter-spacing:.5px}
            .kpi-trend{margin-left:.5rem;font-size:.9rem;font-weight:700}
            .pill .small{opacity:.8;font-size:.72rem}
            /* Increase in days is bad (red), decrease is good (green) */
            .up{color:#f85149}
            .down{color:#3fb950}
            header{box-shadow:0 3px 10px -6px #000}
            .card{transition:transform .12s ease, box-shadow .12s ease}
            .card:hover{transform:translateY(-2px); box-shadow:0 8px 22px -10px #000}
            /* Legend styling for Daily Hours chart */
            .legendWrap{display:flex;flex-wrap:wrap;gap:.5rem;margin:.4rem 0 .6rem}
            .legend-item{display:inline-flex;align-items:center;gap:.45rem;background:#0f1a2a;border:1px solid #2a3b55;border-radius:16px;padding:.28rem .6rem;font-size:.85rem;cursor:pointer;user-select:none}
            .legend-item.off{opacity:.5;border-color:#3a3f47}
            .legend-dot{width:12px;height:12px;border-radius:50%;box-shadow:0 0 0 1px #0007 inset}
            .legend-item:hover{filter:brightness(1.08)}
        </style></head>
        <body>
            <header><h1>Production Dashboard</h1></header>
            <main>
                <div class='grid'>
                    <div class='card'>
                        <h2>Recent Unit Completion</h2>
                        <p>List active units in progress and recently completed within 3 days.</p>
                        <div class='actions'><a class='btn' href='/recent'>Open</a></div>
                    </div>
                    <div class='card'>
                        <h2>DR Labor Lookup</h2>
                        <p>Look up labor by DR number across departments and employees.</p>
                        <div class='actions'><a class='btn secondary' href='/dr'>Open</a></div>
                    </div>
                    <div class='card'>
                        <h2>COM# Charges</h2>
                        <p>See who charged time, how much, and when for a COM#.</p>
                        <div class='actions'><a class='btn' href='/com'>Open</a></div>
                    </div>
                    <div class='card'>
                        <h2>Employee Lookup</h2>
                        <p>Find an employee and view hours by department for a date range.</p>
                        <div class='actions'><a class='btn' href='/emp'>Open</a></div>
                    </div>
                    <div class='card'>
                        <h2>Parts Tracker</h2>
                        <p>Lookup parts by Part Number or COM# from the tracked CSV.</p>
                        <div class='actions'><a class='btn secondary' href='/parts'>Open</a></div>
                    </div>
                    <div class='card'>
                        <h2>Total Daily Hours Charged Chart</h2>
                        <p>View daily hours trends across all departments with 7-day trailing average.</p>
                        <div class='actions'><a class='btn' href='/hours-chart'>View Chart</a></div>
                    </div>
                </div>
                <div id='unitMetrics' class='metrics'>
                    <span class='pill'>Loading unit metrics…</span>
                </div>
            </main>
            <script>
            function pct(n){return (Math.round(n*10)/10).toFixed(1)}
            function arrow(delta){
                if(Math.abs(delta) < 0.05) return '<span class="small">(flat vs last 90 days)</span>';
                // Increase in days is bad -> red; decrease good -> green
                return delta>0 ? '<span class="up">(+'+pct(delta)+' vs last 90 days)</span>' : '<span class="down">(-'+pct(-delta)+' vs last 90 days)</span>';
            }
            fetch('/api/metrics/unit_time_trends').then(r=>r.json()).then(m=>{
                const c=document.getElementById('unitMetrics');
                if(m.error){ c.innerHTML = '<span class="pill">'+m.error+'</span>'; return; }
                const a10=m.last10||{}; const a90=m.last90d||{}; const tr=m.trend||{};
                const dEff = (a10.avg_efficiency||0) - (a90.avg_efficiency||0);
                const dActive = (a10.avg_active_days||0) - (a90.avg_active_days||0);
                const dSpan = (a10.avg_span_days||0) - (a90.avg_span_days||0);
                const effArrow = dEff>0 ? '<span class="down">(+'+pct(dEff)+' vs 90d)</span>' : (dEff<0 ? '<span class="up">(-'+pct(-dEff)+' vs 90d)</span>' : '<span class="small">(flat vs 90d)</span>');
                c.innerHTML = `
                    <span class='pill'>
                        <div class='kpi-title'>Average Efficiency</div>
                        <div class='kpi-value'>${pct(a10.avg_efficiency||0)}% <span class='kpi-trend'>${effArrow}</span></div>
                    </span>
                    <span class='pill'>
                        <div class='kpi-title'>Avg Actual Days Worked</div>
                        <div class='kpi-value'>${pct(a10.avg_active_days||0)} <span class='kpi-trend'>${arrow(dActive)}</span></div>
                    </span>
                    <span class='pill'>
                        <div class='kpi-title'>Avg Total Time Span</div>
                        <div class='kpi-value'>${pct(a10.avg_span_days||0)} <span class='kpi-trend'>${arrow(dSpan)}</span></div>
                    </span>
                `;
            }).catch(()=>{ document.getElementById('unitMetrics').innerHTML = '<span class="pill">Metrics unavailable</span>'; });

            // Minimal inline charting without external libs
            function lineColor(idx){
                const colors=['#6ea8fe','#a8ff60','#ffd166','#ff6b6b','#64d2ff','#c0a7ff','#9be9a8','#f8a5ff','#ffcd3c','#52d96d','#ffa06a'];
                return colors[idx % colors.length];
            }
        function drawLineChart(ctx, labels, series, opts={}){
            const W=ctx.canvas.width, H=ctx.canvas.height,
                padL=90, padR=22, padT=18, padB=46;
                ctx.clearRect(0,0,W,H);
                // compute y range among visible series
                let ymin=0, ymax=0;
                const vis = series.filter(s=>s.visible!==false);
                vis.forEach(s=>{ s.data.forEach(v=>{ if(v>ymax) ymax=v; }); });
                if(ymax<=0) ymax=1;
                // nice y ticks
                const tickCount=10; // finer Y-axis resolution
                const rawStep=ymax/tickCount;
                const mag=Math.pow(10, Math.floor(Math.log10(rawStep)));
                const res=rawStep/mag;
                const niceFactor = res>=5 ? 5 : (res>=2 ? 2 : 1);
                const yStep = niceFactor*mag;
                const yMaxNice = Math.ceil(ymax / yStep) * yStep;
                const sx=(W-padL-padR)/Math.max(1,labels.length-1);
                const sy=(H-padT-padB)/yMaxNice;
                // axes
                ctx.strokeStyle='#2a323c';ctx.lineWidth=1.4;ctx.beginPath();
                ctx.moveTo(padL, padT);ctx.lineTo(padL, H-padB);ctx.lineTo(W-padR, H-padB);ctx.stroke();
                // y ticks and grid
                ctx.fillStyle='#b9c9da'; ctx.font='13.5px system-ui, sans-serif'; ctx.textAlign='right'; ctx.textBaseline='middle';
                for(let yv=0; yv<=yMaxNice+1e-6; yv+=yStep){
                    const y = H-padB - yv*sy;
                    // grid
                    ctx.strokeStyle='#1c2430'; ctx.lineWidth=1; ctx.beginPath();
                    ctx.moveTo(padL, y); ctx.lineTo(W-padR, y); ctx.stroke();
                    // tick
                    ctx.strokeStyle='#2a323c'; ctx.beginPath(); ctx.moveTo(padL-4, y); ctx.lineTo(padL, y); ctx.stroke();
                    ctx.fillText(String(Math.round(yv)), padL-10, y);
                }
                // lines
                let idx=0;
                vis.forEach((s)=>{
                    const color = s.color || lineColor(idx++);
                    ctx.strokeStyle=color;ctx.lineWidth=2.0;ctx.beginPath();
                    s.data.forEach((v,idx)=>{
                        const x=padL+idx*sx, y=H-padB - v*sy;
                        if(idx===0) ctx.moveTo(x,y); else ctx.lineTo(x,y);
                    });
                    ctx.stroke();
                });
                // x ticks (dates)
                ctx.fillStyle='#9fb3c8'; ctx.font='12.5px system-ui, sans-serif'; ctx.textAlign='center'; ctx.textBaseline='top';
                const xTickCount = 8;
                const stepIdx = Math.max(1, Math.floor((labels.length-1)/(xTickCount-1)));
                for(let i=0;i<labels.length;i+=stepIdx){
                    const x = padL + i*sx;
                    const lab = labels[i] || '';
                    ctx.strokeStyle='#2a323c'; ctx.beginPath(); ctx.moveTo(x, H-padB); ctx.lineTo(x, H-padB+4); ctx.stroke();
                    ctx.fillText(lab, x, H-padB+6);
                }
            }
            function buildLegend(containerId, series, onToggle){
                const el = document.getElementById(containerId);
                let html = '';
                series.forEach((s, i)=>{
                    const color = s.color || lineColor(i);
                    const off = s.visible===false ? ' off' : '';
                    html += `<div class="legend-item${off}" data-idx="${i}"><span class="legend-dot" style="background:${color}"></span><span>${s.name}</span></div>`;
                });
                el.innerHTML = html;
                el.querySelectorAll('.legend-item').forEach(item=>{
                    item.addEventListener('click', ()=>{
                        const i = Number(item.getAttribute('data-idx'));
                        series[i].visible = series[i].visible===false ? true : false;
                        item.classList.toggle('off', series[i].visible===false);
                        onToggle();
                    });
                });
            }
            </script>
        </body></html>
        """
        return render_template_string(page)


@app.route('/hours-chart')
def hours_chart():
    """Dedicated page for the Total Daily Hours Charged Chart."""
    page = """<!DOCTYPE html>
    <html>
    <head>
        <title>Total Daily Hours Charged Chart</title>
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background: #0d1117; color: #c9d1d9; }
            .container { max-width: 1400px; margin: 0 auto; }
            header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
            h1 { margin: 0; font-size: 1.5rem; }
            .back-link { padding: 8px 16px; background: #21262d; border-radius: 6px; text-decoration: none; color: #58a6ff; }
            .back-link:hover { background: #30363d; }
            .card { background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 20px; margin-bottom: 20px; }
            .controls { display: flex; gap: 15px; margin-bottom: 20px; flex-wrap: wrap; align-items: center; }
            .btn-group { display: flex; gap: 5px; }
            .btn { padding: 8px 16px; background: #21262d; border: 1px solid #30363d; border-radius: 6px; color: #c9d1d9; cursor: pointer; transition: all 0.2s; font-size: 0.9rem; }
            .btn:hover { background: #30363d; }
            .btn.active { background: #58a6ff; color: #0d1117; border-color: #58a6ff; }
            .btn-group-label { color: #8b949e; font-size: 0.85rem; display: flex; align-items: center; margin-right: 5px; }
            .legendWrap { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 15px; }
            .legend-item { display: flex; align-items: center; gap: 6px; padding: 4px 8px; background: #21262d; border-radius: 4px; cursor: pointer; transition: opacity 0.2s; }
            .legend-item:hover { background: #30363d; }
            .legend-item.off { opacity: 0.3; }
            .legend-dot { width: 12px; height: 12px; border-radius: 50%; }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>Total Daily Hours Charged Chart</h1>
                <a href="/dash" class="back-link">&larr; Back to Dashboard</a>
            </header>
            
            <div class="card">
                <h2 style="margin: 0 0 15px; font-size: 1rem;" id="chartTitle">Daily Hours (Trailing 7-day average)</h2>
                
                <div class="controls">
                    <div style="display: flex; align-items: center; gap: 10px;">
                        <span class="btn-group-label">Period:</span>
                        <div class="btn-group">
                            <button class="btn" data-period="30">30 Days</button>
                            <button class="btn active" data-period="90">90 Days</button>
                            <button class="btn" data-period="365">1 Year</button>
                        </div>
                    </div>
                    
                    <div style="display: flex; align-items: center; gap: 10px;">
                        <span class="btn-group-label">View:</span>
                        <div class="btn-group">
                            <button class="btn" data-view="ma30">Trailing 30</button>
                            <button class="btn active" data-view="ma7">Trailing 7</button>
                            <button class="btn" data-view="ma3">Trailing 3</button>
                            <button class="btn" data-view="actual">Actual</button>
                        </div>
                    </div>
                </div>
                
                <div id="deptLegend" class="legendWrap"></div>
                <canvas id="deptChart" width="1600" height="720" style="width:100%;height:520px"></canvas>
            </div>
        </div>
        
        <script>
        function lineColor(idx){
            const colors=['#6ea8fe','#a8ff60','#ffd166','#ff6b6b','#64d2ff','#c0a7ff','#9be9a8','#f8a5ff','#ffcd3c','#52d96d','#ffa06a'];
            return colors[idx % colors.length];
        }
        
        function drawLineChart(ctx, labels, series, opts={}){
            const W=ctx.canvas.width, H=ctx.canvas.height;
            const padL=60, padR=30, padT=30, padB=50;
            const cw=W-padL-padR, ch=H-padT-padB;
            ctx.clearRect(0,0,W,H);
            // Gather visible series
            const vis=series.filter(s=>s.visible!==false);
            if(vis.length===0) return;
            let allData=[];
            vis.forEach(s=>{ if(s.data && s.data.length) allData=allData.concat(s.data.filter(v=>v!=null)); });
            if(allData.length===0) return;
            const minY=Math.min(...allData), maxY=Math.max(...allData);
            const rangeY = maxY - minY || 1;
            function scaleY(v){ return padT + ch*(1 - (v-minY)/rangeY); }
            const n=labels.length;
            if(n<2) return;
            // Y-axis grid
            ctx.strokeStyle='#30363d';
            const ticks=5;
            for(let i=0;i<=ticks;i++){
                const yy=padT+(i/ticks)*ch;
                ctx.beginPath();ctx.moveTo(padL,yy);ctx.lineTo(W-padR,yy);ctx.stroke();
            }
            // Lines
            vis.forEach((s,i)=>{
                const arr=s.data||[];
                if(arr.length===0) return;
                ctx.strokeStyle=s.color||lineColor(i);
                ctx.lineWidth=2.5;
                ctx.beginPath();
                let first=true;
                for(let j=0;j<n;j++){
                    const v=arr[j];
                    if(v==null) continue;
                    const xx=padL+(cw/(n-1))*j;
                    const yy=scaleY(v);
                    if(first){ ctx.moveTo(xx,yy); first=false; }
                    else ctx.lineTo(xx,yy);
                }
                ctx.stroke();
            });
            // Y-axis labels
            ctx.fillStyle='#8b949e';
            ctx.font='14px sans-serif';
            ctx.textAlign='right';
            ctx.textBaseline='middle';
            for(let i=0;i<=ticks;i++){
                const v=maxY-(i/ticks)*rangeY;
                const yy=padT+(i/ticks)*ch;
                ctx.fillText(Math.round(v), padL-8, yy);
            }
            // X-axis labels (sample every ~7 labels for readability)
            ctx.textAlign='center';
            ctx.textBaseline='top';
            const step=Math.ceil(n/12);
            for(let i=0;i<n;i+=step){
                const xx=padL+(cw/(n-1))*i;
                const lab=labels[i]||'';
                ctx.strokeStyle='#30363d';
                ctx.beginPath();
                ctx.moveTo(xx,H-padB);
                ctx.lineTo(xx,H-padB+4);
                ctx.stroke();
                ctx.fillText(lab,xx,H-padB+6);
            }
        }
        
        function buildLegend(containerId, series, onToggle){
            const el=document.getElementById(containerId);
            let html='';
            series.forEach((s,i)=>{
                const color=s.color||lineColor(i);
                const off=s.visible===false?' off':'';
                html+=`<div class="legend-item${off}" data-idx="${i}"><span class="legend-dot" style="background:${color}"></span><span>${s.name}</span></div>`;
            });
            el.innerHTML=html;
            el.querySelectorAll('.legend-item').forEach(item=>{
                item.addEventListener('click',()=>{
                    const i=Number(item.getAttribute('data-idx'));
                    series[i].visible=series[i].visible===false?true:false;
                    item.classList.toggle('off',series[i].visible===false);
                    onToggle();
                });
            });
        }
        
        // Calculate moving average
        function movingAverage(data, window) {
            if (window === 1) return data; // Actual data
            const result = [];
            for (let i = 0; i < data.length; i++) {
                let sum = 0, count = 0;
                // Look back up to 'window' days
                for (let j = Math.max(0, i - window + 1); j <= i; j++) {
                    if (data[j] != null) {
                        sum += data[j];
                        count++;
                    }
                }
                result.push(count > 0 ? sum / count : null);
            }
            return result;
        }
        
        // Global state
        let chartData = null;
        let currentPeriod = 90;
        let currentView = 'ma7';
        let seriesState = [];
        
        function updateChart() {
            if (!chartData) return;
            
            // Exclude today (last day)
            let allDates = chartData.dates.slice(0, -1);
            let allDeptData = {};
            Object.keys(chartData.per_dept).forEach(dept => {
                allDeptData[dept] = chartData.per_dept[dept].slice(0, -1);
            });
            
            // Determine how many extra days we need for moving average calculation
            const maxWindow = 30; // Maximum trailing window
            const extraDays = maxWindow - 1; // Need 29 extra days before for trailing 30
            
            // Calculate where to start for the moving average calculation
            // We need to include extra days BEFORE the display period
            const displayStartIdx = Math.max(0, allDates.length - currentPeriod);
            const calcStartIdx = Math.max(0, displayStartIdx - extraDays);
            
            // Get data for calculation (includes extra days before display period)
            let calcDates = allDates.slice(calcStartIdx);
            let calcDeptData = {};
            Object.keys(allDeptData).forEach(dept => {
                calcDeptData[dept] = allDeptData[dept].slice(calcStartIdx);
            });
            
            // Apply view transformation on the calculation data (which includes extra days)
            const keys = Object.keys(calcDeptData).sort();
            let series;
            
            // How many days to skip from the calculated data to show only the display period
            const skipDays = Math.max(0, calcDates.length - currentPeriod);
            
            if (currentView === 'ma30') {
                series = keys.map((k, i) => {
                    const ma = movingAverage(calcDeptData[k], 30);
                    // Slice to show only the display period (skip the extra leading days)
                    const displayData = ma.slice(skipDays);
                    return {
                        name: k,
                        data: displayData,
                        color: lineColor(i),
                        visible: seriesState[i]?.visible !== false
                    };
                });
                document.getElementById('chartTitle').textContent = 'Daily Hours (Trailing 30-day average)';
            } else if (currentView === 'ma7') {
                series = keys.map((k, i) => {
                    const ma = movingAverage(calcDeptData[k], 7);
                    // Slice to show only the display period (skip the extra leading days)
                    const displayData = ma.slice(skipDays);
                    return {
                        name: k,
                        data: displayData,
                        color: lineColor(i),
                        visible: seriesState[i]?.visible !== false
                    };
                });
                document.getElementById('chartTitle').textContent = 'Daily Hours (Trailing 7-day average)';
            } else if (currentView === 'ma3') {
                series = keys.map((k, i) => {
                    const ma = movingAverage(calcDeptData[k], 3);
                    // Slice to show only the display period
                    const displayData = ma.slice(skipDays);
                    return {
                        name: k,
                        data: displayData,
                        color: lineColor(i),
                        visible: seriesState[i]?.visible !== false
                    };
                });
                document.getElementById('chartTitle').textContent = 'Daily Hours (Trailing 3-day average)';
            } else { // actual
                series = keys.map((k, i) => {
                    // For actual, just slice the display period
                    const displayData = calcDeptData[k].slice(skipDays);
                    return {
                        name: k,
                        data: displayData,
                        color: lineColor(i),
                        visible: seriesState[i]?.visible !== false
                    };
                });
                document.getElementById('chartTitle').textContent = 'Daily Hours (Actual)';
            }
            
            // Get display dates (only the requested period, not the extra calculation days)
            const displayDates = calcDates.slice(skipDays);
            
            // Store series state
            seriesState = series.map(s => ({ visible: s.visible }));
            
            const dctx = document.getElementById('deptChart').getContext('2d');
            const redraw = () => drawLineChart(dctx, displayDates, series, {});
            buildLegend('deptLegend', series, redraw);
            redraw();
        }
        
        // Period button handlers
        document.querySelectorAll('[data-period]').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('[data-period]').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentPeriod = parseInt(btn.getAttribute('data-period'));
                updateChart();
            });
        });
        
        // View button handlers
        document.querySelectorAll('[data-view]').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('[data-view]').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentView = btn.getAttribute('data-view');
                updateChart();
            });
        });
        
        // Load chart data (get full year of data)
        fetch('/api/metrics/daily_hours?days=365').then(r => r.json()).then(m => {
            // Extract raw hours data (not pre-calculated moving averages)
            chartData = {
                dates: m.dates,
                per_dept: {}
            };
            Object.keys(m.per_dept).forEach(dept => {
                chartData.per_dept[dept] = m.per_dept[dept].hours; // Use raw hours, not ma7
            });
            updateChart();
        }).catch(err => {
            console.error('Chart error:', err);
            document.getElementById('deptChart').parentElement.innerHTML = '<p style="color:#f85149;">Error loading chart data</p>';
        });
        </script>
    </body>
    </html>
    """
    return render_template_string(page)


@app.route('/recent')
def recent_units():
        return render_template_string(RECENT_PAGE)


@app.route('/dr', methods=['GET', 'POST'])
def dr_lookup():
        # DR lookup page. Detect probable columns and search broadly across patterns.
        import sqlite3, html
        query_dr = request.values.get('dr', '').strip()
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('PRAGMA table_info(SCHLabor)')
            lcols = {r[1] for r in cur.fetchall()}

            # Preferred explicit column if present
            dr_col = None
            for cand in ('DRNumber', 'DR', 'DispatchRequest', 'DR_No', 'MKNumber', 'MK_No'):
                if cand in lcols:
                    dr_col = cand
                    break

            # Build list of likely text columns to scan when explicit col absent
            likely_text_cols = [
                'Description','Comments','Comment','Notes','Note','Reference','Ref',
                'WorkOrder','WorkOrderNumber','WO','WONumber','Ticket','TicketNumber',
                'Dispatch','DispatchRequest','Job','JobNumber','JobID',
                'MK','MKNumber','MK_No'
            ]
            # Add any column containing keywords
            for c in list(lcols):
                uc = c.upper()
                if any(k in uc for k in ('DR','DISPATCH','TICKET','WORKORDER','MK')) and c not in likely_text_cols:
                    likely_text_cols.append(c)
            # Keep only existing
            likely_text_cols = [c for c in likely_text_cols if c in lcols]

            rows = []
            totals = {}
            searched_cols_info = []
            if query_dr:
                digits = ''.join(ch for ch in query_dr if ch.isdigit())
                patterns = []
                if digits:
                    patterns.extend([f'%{digits}%', f'%DR {digits}%', f'%DR#{digits}%', f'%DR-{digits}%'])
                patterns.append(f'%{query_dr}%')

                if dr_col:
                    wh = ' OR '.join([f'CAST({dr_col} AS TEXT) LIKE ?' for _ in patterns])
                    sql = f'''
                        SELECT COALESCE(EmployeeName,'') AS EmployeeName,
                               COALESCE(DepartmentNumber,'') AS DeptCode,
                               SUM(COALESCE(ActualHours,0)) AS Hours
                        FROM SCHLabor
                        WHERE {wh}
                        GROUP BY EmployeeName, DepartmentNumber
                        ORDER BY DeptCode, EmployeeName
                    '''
                    cur.execute(sql, patterns)
                    rows = [dict(r) for r in cur.fetchall()]
                    searched_cols_info.append(dr_col)
                else:
                    conds = []
                    params = []
                    for col in likely_text_cols:
                        for p in patterns:
                            conds.append(f'CAST({col} AS TEXT) LIKE ?')
                            params.append(p)
                    if conds:
                        sql = '''
                            SELECT COALESCE(EmployeeName,'') AS EmployeeName,
                                   COALESCE(DepartmentNumber,'') AS DeptCode,
                                   SUM(COALESCE(ActualHours,0)) AS Hours
                            FROM SCHLabor
                            WHERE ''' + ' OR '.join(conds) + '''
                            GROUP BY EmployeeName, DepartmentNumber
                            ORDER BY DeptCode, EmployeeName
                        '''
                        cur.execute(sql, params)
                        rows = [dict(r) for r in cur.fetchall()]
                        searched_cols_info.extend(likely_text_cols)
                for r in rows:
                    d = r.get('DeptCode') or 'UNKNOWN'
                    totals[d] = totals.get(d, 0.0) + float(r.get('Hours') or 0)

            # Render inline template
            page = f"""
            <!doctype html><html><head><meta charset='utf-8'><title>DR Labor Lookup</title>
            <style>
                body{{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}}
                header{{padding:1rem 1.5rem;display:flex;justify-content:space-between;align-items:center;background:#161b22;border-bottom:1px solid #30363d}}
                h1{{margin:0;font-size:1.05rem}}
                main{{padding:1rem 1.2rem}}
                form input,form button{{border-radius:6px;border:1px solid #30363d;background:#11161d;color:#e6edf3;padding:.5rem .65rem}}
                form button{{background:#1f6feb;border-color:#3a78e0;font-weight:700;cursor:pointer}}
                table{{border-collapse:collapse;width:100%;margin-top:1rem;font-size:.8rem}}
                th,td{{border:1px solid #2a323c;padding:.35rem .5rem;text-align:left}}
                th{{background:#1a2330}}
            </style></head>
            <body>
                <header><h1>DR Labor Lookup</h1><div><a href='/dash' style='color:#8fb9ff;text-decoration:none'>&larr; Dashboard</a></div></header>
                <main>
                    <form method='GET'>
                        <label for='dr'>DR#:</label>
                        <input id='dr' name='dr' type='text' value='{html.escape(query_dr)}' placeholder='e.g. 12345' required />
                        <button type='submit'>Lookup</button>
                    </form>
                    {('<p style=\"opacity:.8;margin-top:.5rem;\">Searching in column: <strong>'+html.escape(dr_col)+'</strong></p>') if (query_dr and dr_col) else (('<p style=\"opacity:.7;margin-top:.5rem;\">' + ('Scanned columns: '+html.escape(', '.join(searched_cols_info)) if query_dr and searched_cols_info else '') + '</p>') if query_dr else '')}
                    {('<p style=\"margin-top:1rem;opacity:.7;\">No matches found.</p>') if (query_dr and not rows) else ''}
                    {('''
                        <table>
                            <thead><tr><th>Employee</th><th>Dept</th><th>Hours</th></tr></thead>
                            <tbody>
                                ''' + '\n'.join(f"<tr><td>{html.escape(r.get('EmployeeName',''))}</td><td>{html.escape((r.get('DeptCode') or 'UNKNOWN'))}</td><td>{float(r.get('Hours') or 0):.2f}</td></tr>" for r in rows) + '''
                            </tbody>
                        </table>
                        <h3 style='margin-top:1rem;'>Totals by Dept</h3>
                        <table style='width:auto'>
                            <thead><tr><th>Department</th><th>Total Hours</th></tr></thead>
                            <tbody>
                                ''' + '\n'.join(f"<tr><td>{html.escape(k)}</td><td>{v:.2f}</td></tr>" for k,v in totals.items()) + '''
                            </tbody>
                        </table>
                    ''') if rows else ''}
                </main>
            </body></html>
            """
            return page


@app.route('/api/employee/suggest')
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


@app.route('/api/employee/lookup')
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


@app.route('/api/employee/stats')
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


@app.route('/api/metrics/daily_hours')
def api_daily_hours():
    """Return daily hours per department and total for the last N days (default 60),
    including a 7-day moving average series.
    """
    try:
        days = int(request.args.get('days', '60'))
    except Exception:
        days = 60
    today = datetime.date.today()
    start = (today - datetime.timedelta(days=days-1)).isoformat()

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
    dates = [ (today - datetime.timedelta(days=i)).isoformat() for i in range(days-1, -1, -1) ]
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


@app.route('/emp')
def employee_lookup():
    # Inline page for employee lookup with suggestions and stats
    page = """
    <!doctype html><html><head><meta charset='utf-8'><title>Employee Lookup</title>
    <style>
        body{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}
        header{padding:1rem 1.5rem;display:flex;justify-content:space-between;align-items:center;background:#161b22;border-bottom:1px solid #30363d}
        h1{margin:0;font-size:1.05rem}
        main{padding:1rem 1.2rem}
        .row{display:flex;gap:.6rem;flex-wrap:wrap;align-items:end}
        label{font-size:.72rem;opacity:.9}
        input,button{border-radius:6px;border:1px solid #30363d;background:#11161d;color:#e6edf3;padding:.5rem .65rem}
        input[type=date]{padding:.4rem .5rem}
        button{background:#238636;border-color:#2ea043;font-weight:700;cursor:pointer}
        .sugg{position:relative}
        .slist{position:absolute;z-index:10;background:#0f151c;border:1px solid #2a323c;border-radius:6px;min-width:240px;max-height:220px;overflow:auto;box-shadow:0 6px 18px -8px #000}
        .sopt{padding:.4rem .6rem;cursor:pointer}
        .sopt:hover{background:#1a2230}
    table{border-collapse:collapse;width:100%;margin-top:1rem;font-size:.85rem}
    th,td{border:1px solid #2a323c;padding:.45rem .6rem;text-align:left}
        th{background:#1a2330}
    .pill{display:inline-block;background:#1f6feb33;border:1px solid #1f6feb55;border-radius:20px;padding:.35rem .6rem;font-size:.65rem;margin:.3rem .4rem 0 0}
    .miniBar{height:8px;background:#263040;border-radius:4px;position:relative;overflow:hidden}
    .miniBar > span{position:absolute;left:0;top:0;bottom:0;background:linear-gradient(90deg,#2f9e44,#52d96d)}
    </style></head>
    <body>
        <header><h1>Employee Lookup</h1><div><a href='/dash' style='color:#8fb9ff;text-decoration:none'>&larr; Dashboard</a></div></header>
        <main>
            <div class='row'>
                <div class='sugg'>
                    <label>Last</label><br/>
                    <input id='last' placeholder='Last name' autocomplete='off' />
                    <div id='slist' class='slist' style='display:none'></div>
                </div>
                <div>
                    <label>First</label><br/>
                    <input id='first' placeholder='First name' autocomplete='off' />
                </div>
                <div>
                    <label>Start</label><br/>
                    <input id='start' type='date' />
                </div>
                <div>
                    <label>End</label><br/>
                    <input id='end' type='date' />
                </div>
                <div>
                    <button id='runBtn'>Run</button>
                </div>
            </div>
            <div id='summary' style='margin-top:1rem'></div>
            <div id='results'></div>
        </main>
        <script>
        function todayISO(){const d=new Date();return d.toISOString().slice(0,10)}
        function addDaysISO(dstr, days){const d=new Date(dstr); d.setDate(d.getDate()+days); return d.toISOString().slice(0,10)}
        const lastInp=document.getElementById('last');
        const firstInp=document.getElementById('first');
        const startInp=document.getElementById('start');
        const endInp=document.getElementById('end');
        const slist=document.getElementById('slist');
        // defaults
        endInp.value=todayISO();
        startInp.value=addDaysISO(endInp.value,-60);

        let suggTimer=null;
        function fetchSuggest(){
            const last=lastInp.value.trim();
            const first=firstInp.value.trim();
            if(!last && !first){slist.style.display='none'; return}
            const url=new URL(window.location.origin + '/api/employee/suggest');
            if(last) url.searchParams.set('last',last);
            if(first) url.searchParams.set('first',first);
            fetch(url).then(r=>r.json()).then(data=>{
                const arr=data.suggestions||[];
                if(arr.length===0){slist.style.display='none'; return}
                slist.innerHTML=arr.map(s=>`<div class='sopt' data-name="${s.name}">${s.name}</div>`).join('');
                slist.style.display='block';
                document.querySelectorAll('.sopt').forEach(el=>{
                    el.onclick=()=>{
                        const nm=el.getAttribute('data-name');
                        const parts=nm.split(',');
                        lastInp.value=(parts[0]||'').trim();
                        firstInp.value=(parts[1]||'').trim();
                        slist.style.display='none';
                    };
                });
            }).catch(()=>{slist.style.display='none'});
        }
        function scheduleSuggest(){ if(suggTimer) clearTimeout(suggTimer); suggTimer=setTimeout(fetchSuggest, 180); }
        lastInp.addEventListener('input', scheduleSuggest);
        firstInp.addEventListener('input', scheduleSuggest);
        document.addEventListener('click', (e)=>{ if(!slist.contains(e.target) && e.target!==lastInp) slist.style.display='none'; });

        function runQuery(){
            const last=lastInp.value.trim();
            const first=firstInp.value.trim();
            const start=startInp.value; const end=endInp.value;
            const url=new URL(window.location.origin + '/api/employee/stats');
            if(last) url.searchParams.set('last', last);
            if(first) url.searchParams.set('first', first);
            if(start) url.searchParams.set('start', start);
            if(end) url.searchParams.set('end', end);
            fetch(url).then(r=>r.json()).then(data=>{
                const sDiv=document.getElementById('summary');
                const rDiv=document.getElementById('results');
                if(data.error){ sDiv.textContent=data.error; rDiv.innerHTML=''; return; }
                sDiv.innerHTML=`
                    <span class='pill'>${(data.name||'').replaceAll('<','&lt;')}</span>
                    <span class='pill'>Overall First: ${data.overall_first||'-'}</span>
                    <span class='pill'>Overall Last: ${data.overall_last||'-'}</span>
                    <span class='pill'>Range: ${(data.start||'-')} to ${(data.end||'-')}</span>
                    <span class='pill'>Total Hours: ${Number(data.total_hours||0).toFixed(2)}</span>
                `;
                const allMap = new Map((data.by_dept_all||[]).map(x=>[x.code,{hours:x.hours,percent:x.percent,label:x.label}]));
                const merged = (data.by_dept||[]).map(d=>{
                    const all = allMap.get(d.code) || {hours:0,percent:0,label:d.label};
                    return {...d, all_percent: all.percent};
                });
                // Add any lifetime-only dept not in range
                (data.by_dept_all||[]).forEach(x=>{ if(!merged.find(m=>m.code===x.code)) merged.push({code:x.code,label:x.label,hours:0,percent:0,all_percent:x.percent}); });
                merged.sort((a,b)=> (b.percent - a.percent) || (b.all_percent - a.all_percent));
                const rows=merged.map(d=>`<tr>
                    <td>${d.code||''}</td>
                    <td>${d.label||''}</td>
                    <td>${Number(d.hours||0).toFixed(2)}</td>
                    <td>
                        <div style='display:flex;align-items:center;gap:.4rem;'>
                            <span>${Number(d.percent||0).toFixed(1)}%</span>
                            <div class='miniBar' style='width:120px'><span style='width:${Math.min(100,Math.max(0,d.percent||0))}%' ></span></div>
                        </div>
                    </td>
                    <td style='opacity:.85'>${Number(d.all_percent||0).toFixed(1)}% (all-time)</td>
                </tr>`).join('');
                rDiv.innerHTML = (rows? `<table><thead><tr><th>Dept Code</th><th>Department</th><th>Hours (range)</th><th>% of Time (range)</th><th>All-time % of Time</th></tr></thead><tbody>${rows}</tbody></table>` : '<p style=\"opacity:.7;\">No data for selection.</p>');
            });
        }
        document.getElementById('runBtn').addEventListener('click', runQuery);
        // Auto-run when names change and both fields have some text
        [lastInp, firstInp, startInp, endInp].forEach(inp=> inp.addEventListener('change', ()=>{ if(lastInp.value||firstInp.value) runQuery(); }));
        </script>
    </body></html>
    """
    return render_template_string(page)


@app.route('/api/com/charges')
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
    for label, daymap in day_emp.items():
        if apply_logic:
            is_dept_complete = dept_completion_map.get(label, False)
            use_days, meta = _resolve_department_days(label, daymap, is_complete=is_dept_complete)
            filter_reasons_by_dept[label] = meta.get('filter_reasons', {})
        else:
            # Only apply exclusion employee filter
            use_days = sorted(daymap.keys())
            if exclude_emp:
                excl_employees = PROJECT_DAY_RULES.get('exclusion_employees', [])
                if excl_employees:
                    use_days = [d for d in use_days if not daymap[d]['emps'].issubset(set(excl_employees))]
        filtered_days_by_dept[label] = set(use_days)
    
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


@app.route('/com')
def com_lookup():
    # Simple COM lookup UI
    page = """
    <!doctype html><html><head><meta charset='utf-8'><title>COM# Charges</title>
    <style>
        body{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}
        header{padding:1rem 1.5rem;display:flex;justify-content:space-between;align-items:center;background:#161b22;border-bottom:1px solid #30363d}
        h1{margin:0;font-size:1.05rem}
        main{padding:1rem 1.2rem}
        input,button{border-radius:6px;border:1px solid #30363d;background:#11161d;color:#e6edf3;padding:.5rem .65rem}
        button{background:#238636;border-color:#2ea043;font-weight:700;cursor:pointer}
        table{border-collapse:collapse;width:100%;margin-top:1rem;font-size:.85rem}
        th,td{border:1px solid #2a323c;padding:.45rem .6rem;text-align:left}
        th{background:#1a2330}
        .pill{display:inline-block;background:#1f6feb33;border:1px solid #1f6feb55;border-radius:20px;padding:.35rem .6rem;font-size:.65rem;margin:.3rem .4rem 0 0}
    </style></head>
    <body>
        <header><h1>COM# Charges</h1><div><a href='/dash' style='color:#8fb9ff;text-decoration:none'>&larr; Dashboard</a></div></header>
        <main>
            <div>
                <label>COM#</label>
                <input id='com' placeholder='e.g. 12345' />
                <button id='runBtn'>Lookup</button>
                <button id='totBtn' style='margin-left:.4rem;background:#1f6feb;border-color:#3a78e0'>Totals by Employee</button>
            </div>
            <div id='summary' style='margin-top:.6rem'></div>
            <div id='ganttWrap' style='margin-top:1rem'>
                <h3 style='margin:.6rem 0 .4rem;font-size:.95rem'>Timeline (by Department)</h3>
                <div id='gantt'></div>
            </div>
            <div id='results' style='margin-top:1rem'></div>
        </main>
        <script>
    const CODE2LABEL = {"0120":"Fab","0140":"Welding","0180":"Base/Form/Paint","0200":"Fan Assembly","0220":"Insul Wall Fab","0230":"Pipe","0260":"Assembly","0270":"Door Fab","0280":"Flow Line","0300":"Electrical","0320":"Pipe (Alt)","0340":"Paint","0360":"Test","0380":"Crating"};
        const G_STYLE = document.createElement('style');
        G_STYLE.textContent = `
            .gantt{border:1px solid #2a323c;border-radius:8px;overflow:auto;background:#10161c}
            .grow{display:flex;align-items:center;border-top:1px solid #1a2230}
            .grow:first-child{border-top:none}
            .glabel{flex:0 0 180px;padding:.35rem .5rem;border-right:1px solid #1a2230;background:#121922;font-size:.85rem}
            .ggrid{display:flex;gap:3px;padding:.3rem .5rem}
            .gcell{width:16px;height:16px;border:1px solid #263040;background:#141b22;border-radius:2px;position:relative}
            .gcell.on{border-color:#3fb950}
            .gcell.col{box-shadow:0 0 0 1px #3a78e0 inset}
            .grow.hl .glabel{background:#172233}
            .ghead{display:flex;align-items:center}
            .ghead .glabel{background:#0f161d;font-weight:700}
            .gtick{width:16px;height:16px;display:flex;align-items:center;justify-content:center;color:#7b8a99;font-size:.6rem}
            .gtick.hl{color:#bcd0ff;font-weight:700}
            .gtt{position:fixed;z-index:1000;pointer-events:none;background:#111820;border:1px solid #2a3440;color:#e6edf3;padding:.35rem .5rem;border-radius:6px;font-size:.7rem;box-shadow:0 6px 18px rgba(0,0,0,.45);display:none}
            .glegend{display:flex;align-items:center;gap:.5rem;padding:.4rem .5rem;border-bottom:1px solid #1a2230;background:#0f161d}
            .glegend .lab{font-size:.65rem;color:#9bb0c8}
            .glegend .bar{width:180px;height:10px;border-radius:5px;background:linear-gradient(90deg,hsl(140,65%,22%),hsl(140,65%,52%));border:1px solid #2a3544}
        `;
        document.head.appendChild(G_STYLE);
        const TIP = document.createElement('div');TIP.className='gtt';document.body.appendChild(TIP);

        function ymdToDate(s){ const [y,m,d]=s.split('-').map(Number); return new Date(y, m-1, d); }
        function addDays(d, n){ const x=new Date(d); x.setDate(x.getDate()+n); return x; }
        function ymd(d){ return d.toISOString().slice(0,10); }
        function renderGantt(rows, unitComplete){
            const wrap = document.getElementById('gantt');
            if(!rows || rows.length===0){ wrap.innerHTML=''; return; }
            // Aggregate hours, entries, and employees per dept/day; collect min/max dates
            // Also track filtering status per dept/day
            let minD=null, maxD=null;
            const byDeptHours = new Map(); // code -> Map(day -> sumHours)
            const byDeptEntries = new Map(); // code -> Map(day -> n entries)
            const byDeptEmps = new Map(); // code -> Map(day -> Set(emp))
            const byDeptStatus = new Map(); // code -> Map(day -> {hasValid, hasExcluded, hasFiltered, filterReason})
            let maxHours = 0;
            rows.forEach(r=>{
                const day = r.day; const dept = (r.dept||'').toString();
                if(!day) return;
                const dd = ymdToDate(day);
                if(!minD || dd<minD) minD=dd;
                if(!maxD || dd>maxD) maxD=dd;
                if(!byDeptHours.has(dept)) byDeptHours.set(dept, new Map());
                if(!byDeptEntries.has(dept)) byDeptEntries.set(dept, new Map());
                if(!byDeptEmps.has(dept)) byDeptEmps.set(dept, new Map());
                if(!byDeptStatus.has(dept)) byDeptStatus.set(dept, new Map());
                const mH = byDeptHours.get(dept);
                const mE = byDeptEntries.get(dept);
                const mP = byDeptEmps.get(dept);
                const mS = byDeptStatus.get(dept);
                const prev = Number(mH.get(day)||0);
                const add = Number(r.hours||0) || 0;
                const total = prev + add;
                mH.set(day, total);
                mE.set(day, (mE.get(day)||0) + 1);
                const emp = (r.employee||'').toString().trim();
                if(!mP.has(day)) mP.set(day, new Set());
                if(emp) mP.get(day).add(emp);
                // Track status: prioritize valid > excluded > filtered
                if(!mS.has(day)) mS.set(day, {hasValid: false, hasExcluded: false, hasFiltered: false, filterReason: null});
                const status = mS.get(day);
                if(r.is_excluded_employee) {
                    status.hasExcluded = true;
                } else if(r.is_filtered_out) {
                    status.hasFiltered = true;
                    // Store the filter reason (only need one, they should all be the same for the same day)
                    if(r.filter_reason && !status.filterReason) {
                        status.filterReason = r.filter_reason;
                    }
                } else {
                    status.hasValid = true;
                }
                if(total > maxHours) maxHours = total;
            });
            if(!minD || !maxD){ wrap.innerHTML=''; return; }
            // Build continuous day list
            const days=[]; for(let d=minD; d<=maxD; d=addDays(d,1)) days.push(ymd(d));
            // Header row (ticks every 5 days)
            let html = '<div class="grow ghead"><div class="glabel">Date</div><div class="ggrid">';
            for(let i=0;i<days.length;i++){
                if(i%5===0){ html += `<div class='gtick' data-day='${days[i]}'>${days[i].slice(8)}</div>`; } else { html += `<div class='gtick' data-day='${days[i]}'></div>`; }
            }
            html += '</div></div>';
            // Rows per dept code (keep codes distinct; labels are display-only)
            const entries = Array.from(byDeptHours.keys()).map(code=>({
                code,
                label: CODE2LABEL[code]||code,
                hours: byDeptHours.get(code),
                entries: byDeptEntries.get(code),
                emps: byDeptEmps.get(code),
                status: byDeptStatus.get(code)
            }));
            entries.sort((a,b)=> (a.label||'').localeCompare(b.label||'') || a.code.localeCompare(b.code));
            entries.forEach(ent=>{
                html += `<div class='grow'><div class='glabel'>${ent.label}</div><div class='ggrid'>`;
                days.forEach(d=>{
                    const hrs = Number(ent.hours.get(d)||0);
                    const n = Number(ent.entries.get(d)||0);
                    const emps = ent.emps.get(d) ? ent.emps.get(d).size : 0;
                    const dayStatus = ent.status.get(d) || {hasValid: false, hasExcluded: false, hasFiltered: false};
                    const on = hrs>0 ? ' on' : '';
                    let style = '';
                    let statusNote = '';
                    
                    // Determine cell color based on filtering status (only if unit is 100% complete)
                    // Priority: Valid (green) > Excluded (yellow) > Filtered (red)
                    if(hrs>0){
                        if(unitComplete && dayStatus.hasValid){
                            // Has valid charges - show green (normal)
                            if(maxHours>0){
                                const t = Math.sqrt(hrs / maxHours); // perceptual scale
                                const light = 22 + Math.round(t*30); // 22% -> 52%
                                style = `style="background-color:hsl(140,65%,${light}%);"`;
                            }
                            // Note if also has excluded/filtered
                            if(dayStatus.hasExcluded && dayStatus.hasFiltered) statusNote = ' [MIXED: Valid + Excluded + Filtered]';
                            else if(dayStatus.hasExcluded) statusNote = ' [MIXED: Valid + Excluded]';
                            else if(dayStatus.hasFiltered) statusNote = ' [MIXED: Valid + Filtered]';
                        } else if(unitComplete && dayStatus.hasExcluded){
                            // Only excluded employees - show yellow
                            style = `style="background-color:#f0e68c;"`;
                            if(dayStatus.hasFiltered) statusNote = ' [EXCLUDED + FILTERED]';
                            else statusNote = ' [EXCLUDED EMPLOYEE]';
                        } else if(unitComplete && dayStatus.hasFiltered){
                            // Only filtered entries - show red
                            style = `style="background-color:#ff6b6b;"`;
                            statusNote = ' [FILTERED OUT]';
                        } else if(maxHours>0){
                            // Not unit complete or no status - normal green gradient
                            const t = Math.sqrt(hrs / maxHours); // perceptual scale
                            const light = 22 + Math.round(t*30); // 22% -> 52%
                            style = `style="background-color:hsl(140,65%,${light}%);"`;
                        }
                    }
                    const title = `${ent.code} ${ent.label} — ${d}\n${hrs.toFixed(2)} hours • ${n} charges • ${emps} employees${statusNote}`;
                    html += `<div class='gcell${on}' data-day='${d}' data-dept='${ent.label}' data-hours='${hrs.toFixed(2)}' data-entries='${n}' data-emps='${emps}' data-status='${JSON.stringify(dayStatus)}' title='${title.replaceAll("'","&apos;")}' ${style}></div>`;
                });
                html += '</div></div>';
            });
            let legend = `<div class='glegend'><div class='lab'>Hours</div><div class='bar'></div><div class='lab'>${maxHours.toFixed(1)}h</div></div>`;
            if(unitComplete){
                legend += `<div class='glegend' style='margin-left:2rem;'><div style='width:20px;height:20px;background:#f0e68c;border-radius:3px;'></div><div class='lab'>Excluded Employee</div><div style='width:20px;height:20px;background:#ff6b6b;border-radius:3px;margin-left:1rem;'></div><div class='lab'>Filtered Out</div></div>`;
            }
            wrap.innerHTML = `<div class='gantt'>${legend}${html}</div>`;

            // Wire up hover interactions
            const cells = wrap.querySelectorAll('.gcell');
            function highlight(day, on){
                wrap.querySelectorAll(`.gcell[data-day="${day}"]`).forEach(el=> el.classList.toggle('col', on));
                const tick = wrap.querySelector(`.gtick[data-day="${day}"]`);
                if(tick) tick.classList.toggle('hl', on);
            }
            cells.forEach(cell=>{
                const row = cell.closest('.grow');
                cell.addEventListener('mouseenter', (e)=>{
                    const day = cell.getAttribute('data-day');
                    const dept = cell.getAttribute('data-dept');
                    const hrs = cell.getAttribute('data-hours');
                    const n = cell.getAttribute('data-entries');
                    const emps = cell.getAttribute('data-emps');
                    const statusStr = cell.getAttribute('data-status');
                    let dayStatus = {hasValid: false, hasExcluded: false, hasFiltered: false, filterReason: null};
                    try { dayStatus = JSON.parse(statusStr); } catch(e) {}
                    row.classList.add('hl');
                    highlight(day, true);
                    let statusBadge = '';
                    if(unitComplete){
                        const reason = dayStatus.filterReason ? `<div style='font-size:0.9em;margin-top:2px;opacity:0.9'>Reason: ${dayStatus.filterReason}</div>` : '';
                        if(dayStatus.hasValid && dayStatus.hasExcluded && dayStatus.hasFiltered){
                            statusBadge = `<div style='color:#8fb9ff;font-weight:600;margin-top:4px'>ℹ️ MIXED: Valid + Excluded + Filtered charges${reason}</div>`;
                        } else if(dayStatus.hasValid && dayStatus.hasExcluded){
                            statusBadge = `<div style='color:#8fb9ff;font-weight:600;margin-top:4px'>ℹ️ MIXED: Valid + Excluded charges</div>`;
                        } else if(dayStatus.hasValid && dayStatus.hasFiltered){
                            statusBadge = `<div style='color:#8fb9ff;font-weight:600;margin-top:4px'>ℹ️ MIXED: Valid + Filtered charges${reason}</div>`;
                        } else if(dayStatus.hasExcluded && dayStatus.hasFiltered){
                            statusBadge = `<div style='color:#f0ad4e;font-weight:600;margin-top:4px'>⚠ EXCLUDED + FILTERED${reason}</div>`;
                        } else if(dayStatus.hasExcluded){
                            statusBadge = `<div style='color:#f0e68c;font-weight:600;margin-top:4px'>⚠ EXCLUDED EMPLOYEE</div>`;
                        } else if(dayStatus.hasFiltered){
                            statusBadge = `<div style='color:#ff6b6b;font-weight:600;margin-top:4px'>⚠ FILTERED OUT${reason}</div>`;
                        }
                    }
                    TIP.innerHTML = `<div style='font-weight:600'>${dept}</div><div>${day}</div><div>${hrs} hours • ${n} charges • ${emps} employees</div>${statusBadge}`;
                    TIP.style.display='block';
                    TIP.style.left = (e.clientX + 14) + 'px';
                    TIP.style.top = (e.clientY + 14) + 'px';
                });
                cell.addEventListener('mousemove', (e)=>{
                    TIP.style.left = (e.clientX + 14) + 'px';
                    TIP.style.top = (e.clientY + 14) + 'px';
                });
                cell.addEventListener('mouseleave', ()=>{
                    TIP.style.display='none';
                    const day = cell.getAttribute('data-day');
                    highlight(day, false);
                    row.classList.remove('hl');
                });
            });
        }
        
        function run(){
            const com = document.getElementById('com').value.trim();
            const url = new URL(window.location.origin + '/api/com/charges');
            if(com) url.searchParams.set('com', com);
            fetch(url).then(r=>r.json()).then(data=>{
                const s=document.getElementById('summary');
                const div=document.getElementById('results');
                if(data.error){ s.textContent=data.error; div.innerHTML=''; return; }
                const rows = data.rows||[];
                const unitComplete = data.unit_complete || false;
                let pills = `<span class='pill'>COM ${data.com||''}</span> <span class='pill'>Rows: ${rows.length}</span>`;
                if(unitComplete){
                    pills += `<span class='pill' style='background:#28a74522;border-color:#28a74566'>Unit 100% Complete</span>`;
                }
                s.innerHTML = pills;
                if(rows.length===0){ div.innerHTML='<p style=\"opacity:.7\">No results.</p>'; return; }
                const head = '<tr><th>Date</th><th>Employee</th><th>Dept</th><th>Hours</th><th>Status</th></tr>';
                const body = rows.map(r=> {
                    let status = 'Valid';
                    let style = '';
                    let title = '';
                    if(unitComplete){
                        if(r.is_excluded_employee){
                            status = 'Excluded Employee';
                            style = 'background:#f0e68c22';
                        } else if(r.is_filtered_out){
                            status = 'Filtered Out';
                            style = 'background:#ff000022';
                            title = r.filter_reason ? `title="${r.filter_reason.replaceAll('"','&quot;')}"` : '';
                        }
                    }
                    return `<tr style='${style}' ${title}><td>${r.day||''}</td><td>${(r.employee||'').toString().replaceAll('<','&lt;')}</td><td>${r.dept||''}</td><td>${Number(r.hours||0).toFixed(2)}</td><td>${status}</td></tr>`;
                }).join('');
                div.innerHTML = `<table><thead>${head}</thead><tbody>${body}</tbody></table>`;
                renderGantt(rows, unitComplete);
            }).catch(()=>{ document.getElementById('summary').textContent='Error'; });
        }
        document.getElementById('runBtn').addEventListener('click', ()=>{ run();
            const com = document.getElementById('com').value.trim();
            const url = new URL(window.location.href);
            if(com) url.searchParams.set('com', com); else url.searchParams.delete('com');
            window.history.replaceState({}, '', url);
        });
        document.getElementById('totBtn').addEventListener('click', ()=>{
            const com = document.getElementById('com').value.trim();
            const url = new URL(window.location.origin + '/com_totals');
            if(com) url.searchParams.set('com', com);
            window.open(url.toString(), '_blank');
        });
        
        // Auto-run if ?com= is present
        (function(){
            const params = new URLSearchParams(window.location.search);
            const q = (params.get('com')||'').trim();
            if(q){ document.getElementById('com').value = q; run(); }
        })();
        </script>
    </body></html>
    """
    return render_template_string(page)


@app.route('/api/com/employee_totals')
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


@app.route('/com_totals')
def com_totals_page():
    com = (request.args.get('com') or '').strip()
    page = """
    <!doctype html><html><head><meta charset='utf-8'><title>COM# Totals by Employee</title>
    <style>
        body{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}
        header{padding:1rem 1.5rem;display:flex;justify-content:space-between;align-items:center;background:#161b22;border-bottom:1px solid #30363d}
        h1{margin:0;font-size:1.05rem}
        main{padding:1rem 1.2rem}
        table{border-collapse:collapse;width:100%;margin-top:1rem;font-size:.9rem}
        th,td{border:1px solid #2a323c;padding:.5rem .7rem;text-align:left}
        th{background:#1a2330}
        .pill{display:inline-block;background:#1f6feb33;border:1px solid #1f6feb55;border-radius:20px;padding:.35rem .6rem;font-size:.65rem;margin:.3rem .4rem 0 0}
    </style></head>
    <body>
    <header><h1>COM# Totals by Employee</h1><div><a id='backLink' href='/com' style='color:#8fb9ff;text-decoration:none'>&larr; Back to Charges</a></div></header>
        <main>
            <div id='summary'></div>
            <div id='results'></div>
        </main>
        <script>
        function loadTotals(){
            const url = new URL(window.location.origin + '/api/com/employee_totals');
            const params = new URLSearchParams(window.location.search);
            const com = params.get('com')||'';
            if(com) url.searchParams.set('com', com);
            fetch(url).then(r=>r.json()).then(data=>{
                const s=document.getElementById('summary');
                const div=document.getElementById('results');
                if(data.error){ s.textContent=data.error; div.innerHTML=''; return; }
                s.innerHTML = `<span class='pill'>COM ${data.com||''}</span> <span class='pill'>Employees: ${data.count||0}</span>`;
                const rows = data.rows||[];
                if(rows.length===0){ div.innerHTML='<p style=\"opacity:.7\">No results.</p>'; return; }
                const head = '<tr><th>Employee</th><th>Total Hours</th><th>Entries</th></tr>';
                const body = rows.map(r=> `<tr><td>${(r.employee||'').toString().replaceAll('<','&lt;')}</td><td>${Number(r.hours||0).toFixed(2)}</td><td>${r.entries||0}</td></tr>`).join('');
                div.innerHTML = `<table><thead>${head}</thead><tbody>${body}</tbody></table>`;
            });
        }
        // preserve back link with current com
        (function(){
            const params = new URLSearchParams(window.location.search);
            const com = params.get('com')||'';
            if(com){ const a=document.getElementById('backLink'); a.href = '/com?com='+encodeURIComponent(com); }
        })();
        loadTotals();
        </script>
    </body></html>
    """
    return render_template_string(page)


@app.route('/api/parts/search')
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


@app.route('/parts')
def parts_page():
    page = """
    <!doctype html><html><head><meta charset='utf-8'><title>Parts Tracker</title>
    <style>
        body{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}
        header{padding:1rem 1.5rem;display:flex;justify-content:space-between;align-items:center;background:#161b22;border-bottom:1px solid #30363d}
        h1{margin:0;font-size:1.05rem}
        main{padding:1rem 1.2rem}
        .row{display:flex;gap:.6rem;flex-wrap:wrap;align-items:end}
        label{font-size:.72rem;opacity:.9}
        input,button{border-radius:6px;border:1px solid #30363d;background:#11161d;color:#e6edf3;padding:.5rem .65rem}
        button{background:#1f6feb;border-color:#3a78e0;font-weight:700;cursor:pointer}
        table{border-collapse:collapse;width:100%;margin-top:1rem;font-size:.8rem}
        th,td{border:1px solid #2a323c;padding:.35rem .5rem;text-align:left;vertical-align:top;max-width:420px;overflow:hidden;text-overflow:ellipsis}
        th{background:#1a2330}
        .pill{display:inline-block;background:#1f6feb33;border:1px solid #1f6feb55;border-radius:20px;padding:.35rem .6rem;font-size:.65rem;margin:.3rem .4rem 0 0}
    </style></head>
    <body>
        <header><h1>Parts Tracker</h1><div><a href='/dash' style='color:#8fb9ff;text-decoration:none'>&larr; Dashboard</a></div></header>
        <main>
            <div class='row'>
                <div>
                    <label>Part Number</label><br/>
                    <input id='part' placeholder='e.g. 123-ABC' />
                </div>
                <div>
                    <label>COM#</label><br/>
                    <input id='com' placeholder='e.g. 12345' />
                </div>
                <div>
                    <button id='runBtn'>Search</button>
                </div>
            </div>
            <div id='summary' style='margin-top:.6rem'></div>
            <div id='results'></div>
        </main>
        <script>
        function run(){
            const part = document.getElementById('part').value.trim();
            const com = document.getElementById('com').value.trim();
            const url = new URL(window.location.origin + '/api/parts/search');
            if(part) url.searchParams.set('part', part);
            if(com) url.searchParams.set('com', com);
            fetch(url).then(r=>r.json()).then(data=>{
                const s=document.getElementById('summary');
                const div=document.getElementById('results');
                if(data.error){ s.textContent = data.error; div.innerHTML=''; return; }
                const rows = data.rows||[];
                s.innerHTML = `<span class='pill'>Matches: ${rows.length}</span>`;
                if(rows.length===0){ div.innerHTML='<p style="opacity:.7">No results.</p>'; return; }
                // dynamic columns from first row
                const cols = Object.keys(rows[0]);
                const head = '<tr>' + cols.map(c=>`<th>${c}</th>`).join('') + '</tr>';
                const body = rows.map(r=> '<tr>' + cols.map(c=>`<td>${(r[c]??'').toString().replaceAll('<','&lt;')}</td>`).join('') + '</tr>').join('');
                div.innerHTML = `<table><thead>${head}</thead><tbody>${body}</tbody></table>`;
            }).catch(()=>{ document.getElementById('summary').textContent='Error'; });
        }
        document.getElementById('runBtn').addEventListener('click', run);
        </script>
    </body></html>
    """
    return render_template_string(page)


@app.route('/logic', methods=['GET', 'POST'])
def logic_config():
    """Configuration page for project day filtering logic."""
    global PROJECT_DAY_RULES, MIN_DAY_HOURS, OUTLIER_CAPS, DEPARTMENT_RULES
    
    if request.method == 'POST':
        # Handle form submission - update the PROJECT_DAY_RULES
        try:
            # Handle different POST actions
            action = request.form.get('action', 'update')
            
            if action == 'add_exclusion':
                # Add new exclusion employee
                employee_id = request.form.get('new_exclusion_id', '').strip()
                if employee_id:
                    current_exclusions = PROJECT_DAY_RULES.get('exclusion_employees', [])
                    if employee_id not in current_exclusions:
                        current_exclusions.append(employee_id)
                        PROJECT_DAY_RULES['exclusion_employees'] = current_exclusions
                        message = f"✅ Added exclusion employee: {employee_id}"
                    else:
                        message = f"⚠️ Employee {employee_id} already excluded"
                else:
                    message = "❌ Employee ID required"
                    
            elif action == 'remove_exclusion':
                # Remove exclusion employee
                employee_id = request.form.get('remove_exclusion_id', '').strip()
                current_exclusions = PROJECT_DAY_RULES.get('exclusion_employees', [])
                if employee_id in current_exclusions:
                    current_exclusions.remove(employee_id)
                    PROJECT_DAY_RULES['exclusion_employees'] = current_exclusions
                    message = f"✅ Removed exclusion employee: {employee_id}"
                else:
                    message = f"❌ Employee {employee_id} not found in exclusions"
                    
            else:
                # Update all logic rules
                new_min_hours = float(request.form.get('min_hours', MIN_DAY_HOURS))
                new_min_employees = int(request.form.get('min_employees_override', PROJECT_DAY_RULES.get('min_employees_override', 2)))
                
                # Update outlier caps for each department
                new_outlier_caps = {}
                for dept in OUTLIER_CAPS.keys():
                    cap_value = int(request.form.get(f'outlier_cap_{dept}', OUTLIER_CAPS[dept]))
                    new_outlier_caps[dept] = cap_value
                
                # Update per-department rules (min_hours and min_employees)
                new_dept_rules = {}
                for dept in DEPARTMENT_RULES.keys():
                    dept_min_hours = float(request.form.get(f'dept_min_hours_{dept}', DEPARTMENT_RULES[dept]['min_hours']))
                    dept_min_employees = int(request.form.get(f'dept_min_employees_{dept}', DEPARTMENT_RULES[dept]['min_employees']))
                    new_dept_rules[dept] = {
                        'min_hours': dept_min_hours,
                        'min_employees': dept_min_employees
                    }
                
                # Update the global variables
                MIN_DAY_HOURS = new_min_hours
                OUTLIER_CAPS = new_outlier_caps
                DEPARTMENT_RULES.update(new_dept_rules)
                PROJECT_DAY_RULES.update({
                    'min_total_hours': MIN_DAY_HOURS,
                    'outlier_caps': OUTLIER_CAPS,
                    'min_employees_override': new_min_employees,
                    'department_rules': DEPARTMENT_RULES,
                })
                
                message = "✅ Logic rules updated successfully!"
            
        except Exception as e:
            message = f"❌ Error updating rules: {e}"
    else:
        message = ""
    
    # Get current exclusion employee names for display
    exclusion_employees = PROJECT_DAY_RULES.get('exclusion_employees', [])
    exclusion_names = {}
    if exclusion_employees:
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            placeholders = ','.join(['?' for _ in exclusion_employees])
            cur.execute(f"""
                SELECT DISTINCT EmployeeNumber1, EmployeeName
                FROM SCHLabor
                WHERE EmployeeNumber1 IN ({placeholders})
                AND EmployeeName IS NOT NULL
                AND TRIM(EmployeeName) <> ''
            """, exclusion_employees)
            
            for row in cur.fetchall():
                exclusion_names[str(row['EmployeeNumber1'])] = row['EmployeeName']
    
    page = f"""<!DOCTYPE html>
    <html>
    <head>
        <title>Logic Configuration</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
            .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            h1 {{ color: #2c3e50; margin-bottom: 10px; }}
            .back-link {{ margin-bottom: 20px; }}
            .back-link a {{ color: #3498db; text-decoration: none; }}
            .back-link a:hover {{ text-decoration: underline; }}
            .section {{ margin-bottom: 30px; padding: 20px; border: 1px solid #ddd; border-radius: 5px; background: #fafafa; }}
            .section h2 {{ margin-top: 0; color: #34495e; }}
            .definition {{ margin-bottom: 15px; padding: 10px; background: #e8f4fd; border-left: 4px solid #3498db; }}
            .form-group {{ margin-bottom: 15px; }}
            .form-group label {{ display: block; margin-bottom: 5px; font-weight: bold; }}
            .form-group input {{ width: 100px; padding: 5px; border: 1px solid #ddd; border-radius: 3px; }}
            .dept-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }}
            .dept-item {{ background: white; padding: 15px; border-radius: 5px; border: 1px solid #ddd; }}
            .submit-btn {{ background: #27ae60; color: white; padding: 12px 30px; border: none; border-radius: 5px; cursor: pointer; font-size: 16px; }}
            .submit-btn:hover {{ background: #229954; }}
            .btn-small {{ background: #3498db; color: white; padding: 8px 15px; border: none; border-radius: 3px; cursor: pointer; font-size: 14px; margin-left: 10px; }}
            .btn-small.danger {{ background: #e74c3c; }}
            .btn-small:hover {{ opacity: 0.8; }}
            .message {{ padding: 10px; margin-bottom: 20px; border-radius: 5px; font-weight: bold; }}
            .message.success {{ background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }}
            .message.error {{ background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }}
            .autocomplete {{ position: relative; display: inline-block; }}
            .autocomplete-input {{ width: 300px; padding: 8px; border: 1px solid #ddd; border-radius: 3px; }}
            .autocomplete-list {{ position: absolute; top: 100%; left: 0; right: 0; background: white; border: 1px solid #ddd; border-top: none; border-radius: 0 0 3px 3px; max-height: 200px; overflow-y: auto; display: none; z-index: 1000; }}
            .autocomplete-item {{ padding: 10px; cursor: pointer; border-bottom: 1px solid #eee; }}
            .autocomplete-item:hover {{ background: #f5f5f5; }}
            .autocomplete-item:last-child {{ border-bottom: none; }}
            .exclusion-list {{ background: white; padding: 15px; border-radius: 5px; border: 1px solid #ddd; }}
            .exclusion-item {{ display: flex; justify-content: space-between; align-items: center; padding: 8px; margin-bottom: 5px; background: #f8f9fa; border-radius: 3px; }}
            .exclusion-item:last-child {{ margin-bottom: 0; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="back-link">
                <a href="/dash">&larr; Back to Dashboard</a>
            </div>
            
            <h1>Project Day Filtering Logic Configuration</h1>
            
            {f'<div class="message {"success" if "✅" in message else "error" if "❌" in message else ""}">{message}</div>' if message else ''}
            
            <!-- Data Flow Information -->
            <div class="section" style="background: #fff9e6; border-left: 4px solid #f39c12;">
                <h2>📊 Data Flow & Department Mapping</h2>
                <div class="definition">
                    <strong>Data Sources:</strong> Labor charges come from <code>SCHLabor</code> with department codes resolved via <code>DepartmentCode</code> table, then mapped to canonical tracked departments.
                </div>
                <div style="margin: 15px 0; padding: 15px; background: white; border-radius: 5px;">
                    <strong>Labor Code → Department Mapping:</strong>
                    <ul style="margin: 10px 0 0 20px;">
                        <li><strong>0120 FAB</strong> → Fab</li>
                        <li><strong>0140 WELD</strong> → Welding</li>
                        <li><strong>0180 FOAM/PAINT</strong> → BaseFormPaint</li>
                        <li><strong>0200 FAN</strong> → FanAssyTest</li>
                        <li><strong>0220 WALL FAB</strong> → InsulWallFab</li>
                        <li><strong>0230 PIPE</strong> → Pipe</li>
                        <li><strong>0260 ASSY + 0280 FLOW</strong> → <strong>Assembly</strong> (both codes map to single Assembly department)</li>
                        <li><strong>0270 DOOR</strong> → DoorFab</li>
                        <li><strong>0300 ELEC</strong> → Electrical</li>
                        <li><strong>0340 PAINT</strong> → Paint</li>
                        <li><strong>0360 TEST</strong> → Test (Gantt display only; no completion tracking)</li>
                        <li><strong>0380 FINISH</strong> → Crating</li>
                    </ul>
                </div>
                <div class="definition" style="background: #e8f8f5; border-left-color: #27ae60;">
                    <strong>Unit Completion:</strong> A unit is "Complete" only when ALL 11 completion-tracked departments in SCHSchedulingSummary 
                    (Fab, Welding, BaseFormPaint, FanAssyTest, InsulWallFab, DoorFab, Electrical, Pipe, Paint, Crating, Assembly) 
                    with std > 0 are 100% complete. Test is excluded from completion tracking. Days and Span calculations use filtered SCHLabor data with the rules below.
                </div>
                <div class="definition" style="background: #f0f7ff; border-left-color: #1f6feb;">
                    <strong>Gantt Chart:</strong> Shows <strong>all</strong> raw charged days from SCHLabor for <strong>all departments</strong> including Test. 
                    The Gantt applies <strong>no filtering</strong> and is not affected by completion status—it's purely for visibility and troubleshooting.
                </div>
            </div>
            
            <form method="POST">
                <input type="hidden" name="action" value="update">
                
                <!-- Minimum Hours Section -->
                <div class="section">
                    <h2>Minimum Day Hours Threshold</h2>
                    <div class="definition">
                        <strong>Definition:</strong> A department day is only counted if the total hours charged are at least this amount. 
                        Days below this threshold are ignored for ALL departments in days_active and span calculations.
                    </div>
                    <div class="form-group">
                        <label>Minimum Total Hours:</label>
                        <input type="number" step="0.1" name="min_hours" value="{MIN_DAY_HOURS}" />
                    </div>
                </div>
                
                <!-- Employee Override Section -->
                <div class="section">
                    <h2>Employee Override Count</h2>
                    <div class="definition">
                        <strong>Definition:</strong> If a department day has at least this many employees, it will never be dropped as an outlier, 
                        regardless of the gap to adjacent days. This overrides the gap filtering rules below.
                    </div>
                    <div class="form-group">
                        <label>Minimum Employees to Override (Global Default):</label>
                        <input type="number" min="1" name="min_employees_override" value="{PROJECT_DAY_RULES.get('min_employees_override', 2)}" />
                    </div>
                    <div class="definition" style="background: #fff3cd; border-left-color: #ffc107;">
                        <strong>Note:</strong> The values above are global defaults. You can override them per-department in the section below.
                    </div>
                </div>
                
                <!-- Per-Department Rules Section -->
                <div class="section">
                    <h2>⚙️ Per-Department Filtering Rules</h2>
                    <div class="definition">
                        <strong>Department-Specific Overrides:</strong> Each department can have its own minimum hours and minimum employee thresholds. 
                        These override the global defaults above for that specific department. The employee override rule applies to BOTH hours threshold and gap filtering.
                    </div>
                    <div class="definition" style="background: #e8f8f5; border-left-color: #27ae60;">
                        <strong>Employee Override Rule:</strong> If a day has ≥ min_employees <strong>valid (non-excluded)</strong> employees, 
                        it is kept regardless of hours or gap filtering. This prevents losing important work days with multiple contributors.
                    </div>
                    <div class="dept-grid">
                        {chr(10).join([f'''
                        <div class="dept-item">
                            <strong>{dept}</strong>
                            <div style="margin-top: 10px;">
                                <label style="font-size: 0.9em;">Min Hours:</label>
                                <input type="number" step="0.1" name="dept_min_hours_{dept}" value="{DEPARTMENT_RULES[dept]['min_hours']}" style="width: 80px;" />h
                            </div>
                            <div style="margin-top: 8px;">
                                <label style="font-size: 0.9em;">Min Employees:</label>
                                <input type="number" min="1" name="dept_min_employees_{dept}" value="{DEPARTMENT_RULES[dept]['min_employees']}" style="width: 80px;" />
                            </div>
                        </div>''' for dept in sorted(DEPARTMENT_RULES.keys())])}
                    </div>
                </div>
                
                <!-- First/Last Day Gap Filtering Section -->
                <div class="section">
                    <h2>First/Last Day Gap Filtering (Outlier Caps)</h2>
                    <div class="definition">
                        <strong>First Day:</strong> ALWAYS apply gap filtering. Keep if gap to next charge ≤ cap below OR ≥ override count of employees.<br/>
                        <strong>Last Day:</strong> Apply gap filtering ONLY when department is 100% complete. Incomplete departments keep all days.<br/>
                        <strong>Gantt Chart:</strong> Shows ALL raw charged days with NO filtering for visibility and troubleshooting.
                    </div>
                    <div style="background: #f0f7ff; padding: 10px; border-radius: 5px; margin: 10px 0; border-left: 3px solid #1f6feb;">
                        <strong>Note:</strong> Assembly cap applies to both ASSY (0260) and FLOW (0280) labor codes since they map to the same Assembly department.
                    </div>
                    <div class="dept-grid">
                        {chr(10).join([f'''
                        <div class="dept-item">
                            <label>{dept}:</label>
                            <input type="number" name="outlier_cap_{dept}" value="{OUTLIER_CAPS[dept]}" /> days
                        </div>''' for dept in sorted(OUTLIER_CAPS.keys())])}
                    </div>
                </div>
                
                <button type="submit" class="submit-btn">Update Logic Rules</button>
                
            </form>
            
            <!-- Exclusion Employees Section -->
            <div class="section">
                <h2>Exclusion Employees</h2>
                <div class="definition">
                    <strong>Exclusion Logic:</strong> These employees are excluded from first/last day calculations for ALL departments. 
                    Their charges don't count toward hours or the employee override rule for gap filtering, but still count for other metrics.
                </div>
                
                <!-- Add New Exclusion -->
                <div style="margin-bottom: 20px;">
                    <h3>Add Exclusion Employee</h3>
                    <div class="autocomplete">
                        <input type="text" id="employeeSearch" class="autocomplete-input" placeholder="Start typing employee name..." />
                        <div id="autocompleteList" class="autocomplete-list"></div>
                    </div>
                    <button type="button" id="addExclusionBtn" class="btn-small" disabled>Add Exclusion</button>
                </div>
                
                <!-- Current Exclusions -->
                <div>
                    <h3>Current Exclusions ({len(exclusion_employees)})</h3>
                    <div class="exclusion-list">
                        {chr(10).join([f'''
                        <div class="exclusion-item">
                            <span><strong>{exclusion_names.get(emp_id, f"ID: {emp_id}")}</strong> ({emp_id})</span>
                            <form method="POST" style="display: inline;">
                                <input type="hidden" name="action" value="remove_exclusion">
                                <input type="hidden" name="remove_exclusion_id" value="{emp_id}">
                                <button type="submit" class="btn-small danger" onclick="return confirm('Remove this exclusion?')">Remove</button>
                            </form>
                        </div>''' for emp_id in exclusion_employees]) if exclusion_employees else '<p style="opacity: 0.7; margin: 0;">No exclusion employees configured.</p>'}
                    </div>
                </div>
            </div>
            
            <!-- Current Rules Display -->
            <div class="section">
                <h2>Current Active Rules Summary</h2>
                <pre style="background: #f8f9fa; padding: 15px; border-radius: 5px; overflow-x: auto;">
Min Hours: {MIN_DAY_HOURS}
Min Employees Override: {PROJECT_DAY_RULES.get('min_employees_override', 2)}
Outlier Caps: {dict(sorted(OUTLIER_CAPS.items()))}
Exclusion Employees: {len(exclusion_employees)} configured
                </pre>
            </div>
            
        </div>
        
        <script>
        let selectedEmployee = null;
        
        // Employee autocomplete functionality
        document.getElementById('employeeSearch').addEventListener('input', function() {{
            const query = this.value.trim();
            const list = document.getElementById('autocompleteList');
            
            if (query.length < 2) {{
                list.style.display = 'none';
                document.getElementById('addExclusionBtn').disabled = true;
                selectedEmployee = null;
                return;
            }}
            
            fetch(`/api/employee/lookup?q=${{encodeURIComponent(query)}}`)
                .then(r => r.json())
                .then(data => {{
                    const employees = data.employees || [];
                    
                    if (employees.length === 0) {{
                        list.innerHTML = '<div class="autocomplete-item">No employees found</div>';
                        list.style.display = 'block';
                        document.getElementById('addExclusionBtn').disabled = true;
                        selectedEmployee = null;
                        return;
                    }}
                    
                    list.innerHTML = employees.map(emp => 
                        `<div class="autocomplete-item" onclick="selectEmployee('${{emp.id}}', '${{emp.name.replace(/'/g, "&apos;")}}')">${{emp.name}} (ID: ${{emp.id}})</div>`
                    ).join('');
                    list.style.display = 'block';
                }})
                .catch(() => {{
                    list.innerHTML = '<div class="autocomplete-item">Error loading employees</div>';
                    list.style.display = 'block';
                }});
        }});
        
        function selectEmployee(id, name) {{
            selectedEmployee = {{ id: id, name: name }};
            document.getElementById('employeeSearch').value = `${{name}} (ID: ${{id}})`;
            document.getElementById('autocompleteList').style.display = 'none';
            document.getElementById('addExclusionBtn').disabled = false;
        }}
        
        // Add exclusion employee
        document.getElementById('addExclusionBtn').addEventListener('click', function() {{
            if (!selectedEmployee) return;
            
            const form = document.createElement('form');
            form.method = 'POST';
            form.innerHTML = `
                <input type="hidden" name="action" value="add_exclusion">
                <input type="hidden" name="new_exclusion_id" value="${{selectedEmployee.id}}">
            `;
            document.body.appendChild(form);
            form.submit();
        }});
        
        // Hide autocomplete when clicking outside
        document.addEventListener('click', function(e) {{
            if (!e.target.closest('.autocomplete')) {{
                document.getElementById('autocompleteList').style.display = 'none';
            }}
        }});
        </script>
    </body>
    </html>"""
    
    return render_template_string(page)


if __name__ == '__main__':  # pragma: no cover
    app.run(host='0.0.0.0', port=5000, debug=True)
