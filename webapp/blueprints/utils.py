"""Shared utilities and helper functions for the SQRS application.

This module contains database connection helpers, data normalization functions,
and day filtering logic used across multiple blueprints.
"""

import sqlite3
import os
import re
import datetime

# Database path
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'SCHLabor.db'))

# (Label, std col, act col, completion %, efficiency %)
# Tracked Departments based on canonical naming from specification
# Note: Test is included for display but NOT used for completion checking
TRACKED_DEPARTMENTS = [
    ("Fab", "fabstdhrs", "fabacthrs", "Fab Completion", "Fab Efficiency"),
    ("Welding", "weldingstdhrs", "weldingacthrs", "Welding Completion", "Welding Efficiency"),
    ("BaseFormPaint", "baseformpaintstdhrs", "baseformpaintacthrs", "BaseFormPaint Completion", "BaseFormPaint Efficiency"),
    ("FanAssyTest", "fanassyteststdhrs", "fanassytestacthrs", "FanAssyTest Completion", "FanAssyTest Efficiency"),
    ("InsulWallFab", "insulwallfabstdhrs", "insulwallfabacthrs", "InsulWallFab Completion", "InsulWallFab Efficiency"),
    ("DoorFab", "doorfabstdhrs", "doorfabacthrs", "DoorFab Completion", "DoorFab Efficiency"),
    ("Pipe", "pipestdhrs", "pipeacthrs", "Pipe Completion", "Pipe Efficiency"),
    ("Paint", "paintstdhrs", "paintacthrs", "Paint Completion", "Paint Efficiency"),
    ("Electrical", "electricalstdhrs", "electricalacthrs", "Electrical Completion", "Electrical Efficiency"),
    ("Assembly", "assystdhrs", "assyacthrs", "Assembly Completion", "Assembly Efficiency"),
    ("Crating", "cratingstdhrs", "cratingacthrs", "Crating Completion", "Crating Efficiency"),
    ("Test", "teststdhrs", "testacthrs", "Test Completion", "testefficiency"),  # Display only, not for completion
]

# Departments to check for unit completion (11 departments - excludes Test)
# A unit is complete when ALL of these departments with std > 0 are at 100% completion
COMPLETION_CHECK_DEPARTMENTS = [
    ("Fab", "fabstdhrs", "fabacthrs", "Fab Completion", "Fab Efficiency"),
    ("Welding", "weldingstdhrs", "weldingacthrs", "Welding Completion", "Welding Efficiency"),
    ("BaseFormPaint", "baseformpaintstdhrs", "baseformpaintacthrs", "BaseFormPaint Completion", "BaseFormPaint Efficiency"),
    ("FanAssyTest", "fanassyteststdhrs", "fanassytestacthrs", "FanAssyTest Completion", "FanAssyTest Efficiency"),
    ("InsulWallFab", "insulwallfabstdhrs", "insulwallfabacthrs", "InsulWallFab Completion", "InsulWallFab Efficiency"),
    ("DoorFab", "doorfabstdhrs", "doorfabacthrs", "DoorFab Completion", "DoorFab Efficiency"),
    ("Pipe", "pipestdhrs", "pipeacthrs", "Pipe Completion", "Pipe Efficiency"),
    ("Paint", "paintstdhrs", "paintacthrs", "Paint Completion", "Paint Efficiency"),
    ("Electrical", "electricalstdhrs", "electricalacthrs", "Electrical Completion", "Electrical Efficiency"),
    ("Crating", "cratingstdhrs", "cratingacthrs", "Crating Completion", "Crating Efficiency"),
    ("Assembly", "assystdhrs", "assyacthrs", "Assembly Completion", "Assembly Efficiency"),
]

# Desired visual order for Gantt (edit this list to tweak ordering quickly)
# Note: Gantt shows all departments including Test (raw data, no filtering)
GANTT_DEPT_ORDER = [
    'Fab','Welding','InsulWallFab','FanAssyTest','Test',
    'BaseFormPaint','DoorFab','Pipe','Paint','Electrical','Assembly','Crating'
]
GANTT_INDEX = {name:i for i,name in enumerate(GANTT_DEPT_ORDER)}

# Minimum total hours that must exist on a department day for that day to count toward
# days_active or span calculations. Days below this threshold are ignored for ALL departments.
MIN_DAY_HOURS = 2.0

# Per-department gap caps (days) - these are applied to filter outlier first/last days
# First day: ALWAYS apply gap filtering
# Last day: Apply ONLY when department is 100% complete
# Override: A day is NEVER dropped if ≥2 distinct employees charged on that day
OUTLIER_CAPS = {
    'Assembly': 12,          # Covers both ASSY (0260) and FLOW (0280) labor codes
    'BaseFormPaint': 7,
    'Crating': 7,
    'DoorFab': 7,
    'Electrical': 7,
    'Fab': 13,
    'FanAssyTest': 22,       # Labor code 0200 (FAN)
    'InsulWallFab': 11,
    'Paint': 7,
    'Pipe': 7,
    'Welding': 8,
    'Test': 22,              # Not used for metrics; Gantt only (shows all days, no filtering)
}

# Per-department filtering rules (min hours and min employees)
# These override the global defaults for specific departments
DEPARTMENT_RULES = {
    'Assembly': {'min_hours': 2.0, 'min_employees': 2},
    'BaseFormPaint': {'min_hours': 2.0, 'min_employees': 2},
    'Crating': {'min_hours': 2.0, 'min_employees': 2},
    'DoorFab': {'min_hours': 2.0, 'min_employees': 2},
    'Electrical': {'min_hours': 2.0, 'min_employees': 2},
    'Fab': {'min_hours': 2.0, 'min_employees': 2},
    'FanAssyTest': {'min_hours': 2.0, 'min_employees': 2},
    'InsulWallFab': {'min_hours': 2.0, 'min_employees': 2},
    'Paint': {'min_hours': 2.0, 'min_employees': 2},
    'Pipe': {'min_hours': 2.0, 'min_employees': 2},
    'Welding': {'min_hours': 2.0, 'min_employees': 2},
    'Test': {'min_hours': 2.0, 'min_employees': 2},
}

# Central definition of per-project rules governing department day filtering.
# These rules are configurable via the /logic page.
PROJECT_DAY_RULES = {
    'outlier_caps': OUTLIER_CAPS,
    'exclusion_employees': ['1205797'],  # Employee IDs to exclude from day selection logic
    'department_rules': DEPARTMENT_RULES,  # Per-department min_hours and min_employees
    'max_gap_override': 30,  # Maximum gap in days - drops charges beyond this even with 2+ employees
    'pre_fab_gap_days': 10,  # Drop days occurring more than this many days before first Fab cluster
    'post_assembly_gap_days': 10,  # Drop days occurring more than this many days after last Assembly cluster
}

# Load configuration from database on module import
def _load_configuration():
    """Load configuration from database and override defaults."""
    import sys
    import os
    # Add parent directory to path for metrics_cache import
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
    try:
        from metrics_cache import load_configuration
        
        # Load OUTLIER_CAPS
        saved_outlier_caps = load_configuration('OUTLIER_CAPS')
        if saved_outlier_caps:
            OUTLIER_CAPS.update(saved_outlier_caps)
        
        # Load DEPARTMENT_RULES
        saved_dept_rules = load_configuration('DEPARTMENT_RULES')
        if saved_dept_rules:
            DEPARTMENT_RULES.update(saved_dept_rules)
        
        # Load EXCLUSION_EMPLOYEES
        saved_exclusions = load_configuration('EXCLUSION_EMPLOYEES')
        if saved_exclusions:
            PROJECT_DAY_RULES['exclusion_employees'] = saved_exclusions
        
        # Load MAX_GAP_OVERRIDE
        saved_max_gap = load_configuration('MAX_GAP_OVERRIDE')
        if saved_max_gap is not None:
            PROJECT_DAY_RULES['max_gap_override'] = saved_max_gap

        # Load PRE_FAB_GAP_DAYS
        saved_pre_fab_gap = load_configuration('PRE_FAB_GAP_DAYS')
        if saved_pre_fab_gap is not None:
            PROJECT_DAY_RULES['pre_fab_gap_days'] = saved_pre_fab_gap

        # Load POST_ASSEMBLY_GAP_DAYS
        saved_post_assembly_gap = load_configuration('POST_ASSEMBLY_GAP_DAYS')
        if saved_post_assembly_gap is not None:
            PROJECT_DAY_RULES['post_assembly_gap_days'] = saved_post_assembly_gap
        
        # Update PROJECT_DAY_RULES references
        PROJECT_DAY_RULES['outlier_caps'] = OUTLIER_CAPS
        PROJECT_DAY_RULES['department_rules'] = DEPARTMENT_RULES
        
        print("✅ Configuration loaded from database")
    except Exception as e:
        print(f"⚠️ Could not load configuration from database: {e}")
        print("   Using default configuration values")

# Load configuration on module import
_load_configuration()


def get_conn():
    """Get a database connection to SCHLabor.db"""
    return sqlite3.connect(DB_PATH)


def fnum(v):
    """Robust parse to float - handles None, empty strings, and 'nan' gracefully"""
    try:
        if v in (None, '', 'nan', 'NaN'):
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def normalize_com(v) -> str:
    """Normalize COM to a 5-digit string of digits when possible."""
    if v is None:
        return ''
    s = str(v).strip()
    # keep digits only
    digits = ''.join(ch for ch in s if ch.isdigit())
    if len(digits) >= 5:
        return digits[-5:]
    return digits or s


def _filter_days_by_gap(label: str, ordered_days: list, daymap: dict, is_complete: bool = True, excl_employees: list = None, min_employees: int = 2) -> list:
    """Remove outlier days based on large gaps.

    Approach: Find "clusters" of days separated by large gaps, then remove small/weak clusters.
    
    Rules:
    - Split days into clusters based on gaps > max_gap_override or (gaps > cap AND weak cluster).
    - Keep the largest/strongest cluster, remove the rest.
    - Cluster strength = total hours + employee diversity.
    - Employee count excludes employees on the exclusion list.
    """
    cap = OUTLIER_CAPS.get(label)
    max_gap_override = PROJECT_DAY_RULES.get('max_gap_override', 30)
    if not cap or len(ordered_days) < 2 or not is_complete:
        return ordered_days
    
    days = list(ordered_days)
    excl_set = set(excl_employees or [])
    
    # Build clusters of consecutive days (separated by large gaps)
    clusters = []
    current_cluster = [days[0]]
    
    for i in range(1, len(days)):
        prev_day = days[i-1]
        curr_day = days[i]
        
        try:
            gap = (datetime.date.fromisoformat(curr_day) - datetime.date.fromisoformat(prev_day)).days
        except Exception:
            gap = 0
        
        # Check if this starts a new cluster
        # New cluster if: gap > max_gap_override (hard limit)
        #            OR: gap > cap AND both sides of gap are weak
        prev_emps = len((daymap.get(prev_day) or {}).get('emps', set()) - excl_set)
        curr_emps = len((daymap.get(curr_day) or {}).get('emps', set()) - excl_set)
        
        new_cluster = False
        if gap > max_gap_override:
            new_cluster = True
        elif gap > cap and prev_emps < min_employees and curr_emps < min_employees:
            new_cluster = True
        
        if new_cluster:
            clusters.append(current_cluster)
            current_cluster = [curr_day]
        else:
            current_cluster.append(curr_day)
    
    # Add the last cluster
    if current_cluster:
        clusters.append(current_cluster)
    
    # If only one cluster, return all days
    if len(clusters) == 1:
        return days
    
    # Score each cluster by total hours and number of days
    cluster_scores = []
    for cluster in clusters:
        total_hours = sum((daymap.get(d) or {}).get('hours', 0) for d in cluster)
        num_days = len(cluster)
        all_emps = set()
        for d in cluster:
            all_emps.update((daymap.get(d) or {}).get('emps', set()) - excl_set)
        num_emps = len(all_emps)
        
        # Score = hours + (days * 2) + (employees * 5)
        score = total_hours + (num_days * 2) + (num_emps * 5)
        cluster_scores.append((score, cluster, num_days, num_emps, total_hours))
    
    # Keep the highest-scoring cluster
    cluster_scores.sort(reverse=True, key=lambda x: x[0])
    best_score, best_cluster, best_days, best_emps, best_hours = cluster_scores[0]
    
    # If the best cluster is too weak, drop everything
    # Weak = only 1 day OR (< min_employees AND < 10 hours)
    if best_days == 1 or (best_emps < min_employees and best_hours < 10):
        return []
    
    return best_cluster


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
    
    # Create a mapping of COM -> department -> completion and overall unit completion status
    completion_map = {}
    unit_completion_status = {}
    for unit in units:
        com = unit['com']
        completion_map[com] = {}
        dept_lookup = {}
        for dept in unit['departments']:
            completion_map[com][dept['name']] = dept['completion']
            dept_lookup[dept['name']] = dept
        unit_complete = True
        for label, *_ in COMPLETION_CHECK_DEPARTMENTS:
            dept_info = dept_lookup.get(label)
            if not dept_info:
                continue
            std_hours = dept_info.get('std', 0)
            completion_pct = dept_info.get('completion', 0.0)
            if std_hours and completion_pct + 1e-6 < 100.0:
                unit_complete = False
                break
        unit_completion_status[com] = unit_complete
    
    # Recalculate stats with completion-aware filtering
    new_stats_map = {}
    new_timeline_map = {}
    unit_day_results = {}

    for (com, label), daymap in day_emp.items():
        # Get completion status for this COM/department combination
        is_complete = completion_map.get(com, {}).get(label, 0.0) >= 100.0 - 1e-6

        # Use completion-aware filtering for metrics
        use_days, meta = _resolve_department_days(label, daymap, is_complete)

        # Use unfiltered days for Gantt display
        all_days = _get_all_department_days(label, daymap)

        unit_entry = unit_day_results.setdefault(com, {})
        unit_entry[label] = {
            'use_days': list(use_days),
            'meta': meta,
            'is_complete': is_complete,
            'all_days': list(all_days),
        }

    pre_fab_gap = PROJECT_DAY_RULES.get('pre_fab_gap_days', 10)
    post_assembly_gap = PROJECT_DAY_RULES.get('post_assembly_gap_days', 10)
    try:
        pre_fab_gap_days = max(0, int(pre_fab_gap))
    except Exception:
        pre_fab_gap_days = 0
    try:
        post_assembly_gap_days = max(0, int(post_assembly_gap))
    except Exception:
        post_assembly_gap_days = 0

    for com, dept_map in unit_day_results.items():
        fab_days = dept_map.get('Fab', {}).get('use_days', [])
        assembly_days = dept_map.get('Assembly', {}).get('use_days', [])

        fab_window_start = None
        if fab_days:
            try:
                fab_anchor = datetime.date.fromisoformat(fab_days[0])
                fab_window_start = fab_anchor - datetime.timedelta(days=pre_fab_gap_days)
            except Exception:
                fab_window_start = None

        assembly_window_end = None
        if assembly_days:
            try:
                assembly_anchor = datetime.date.fromisoformat(assembly_days[-1])
                assembly_window_end = assembly_anchor + datetime.timedelta(days=post_assembly_gap_days)
            except Exception:
                assembly_window_end = None

        tm = new_timeline_map.setdefault(com, {'rows': {}, 'earliest': None, 'latest': None})
        unit_complete = unit_completion_status.get(com, False)

        initial_last_days = {}
        for label_key, info in dept_map.items():
            use_days_initial = info.get('use_days', [])
            if use_days_initial:
                try:
                    initial_last_days[label_key] = datetime.date.fromisoformat(use_days_initial[-1])
                except Exception:
                    continue

        fallback_anchor_by_label = {}
        if assembly_window_end is None and unit_complete and initial_last_days:
            for label_key in dept_map.keys():
                candidates = [dt for other_label, dt in initial_last_days.items() if other_label != label_key]
                if candidates:
                    fallback_anchor_by_label[label_key] = max(candidates)

        for label, info in dept_map.items():
            use_days = list(info.get('use_days', []))
            meta = info.get('meta', {})
            meta.setdefault('dropped_before_fab_gap', [])
            meta.setdefault('dropped_after_assembly_gap', [])
            meta.setdefault('filter_reasons', {})

            anchor_window_end = assembly_window_end
            fallback_anchor = fallback_anchor_by_label.get(label)
            fallback_reason = None
            if anchor_window_end is None and fallback_anchor:
                anchor_window_end = fallback_anchor + datetime.timedelta(days=post_assembly_gap_days)
                fallback_reason = 'latest completed department'

            trimmed_days = []
            for day in use_days:
                try:
                    day_obj = datetime.date.fromisoformat(day)
                except Exception:
                    day_obj = None

                dropped = False
                if day_obj and fab_window_start and day_obj < fab_window_start:
                    meta['dropped_before_fab_gap'].append(day)
                    meta['filter_reasons'][day] = (
                        f'Dropped by Fab lead-in window (> {pre_fab_gap_days} day gap before Fab)'
                    )
                    dropped = True
                if day_obj and anchor_window_end and day_obj > anchor_window_end:
                    meta['dropped_after_assembly_gap'].append(day)
                    reason_context = 'Assembly'
                    if fallback_reason:
                        reason_context = fallback_reason
                    meta['filter_reasons'][day] = (
                        f'Dropped by Assembly tail window (> {post_assembly_gap_days} day gap after {reason_context})'
                    )
                    dropped = True

                if not dropped:
                    trimmed_days.append(day)

            trimmed_days.sort()
            info['use_days'] = trimmed_days
            info['meta'] = meta

            all_days = info.get('all_days', [])
            if all_days:
                tm['rows'][label] = list(all_days)
                first_all = all_days[0]
                last_all = all_days[-1]
                if first_all and (tm['earliest'] is None or first_all < tm['earliest']):
                    tm['earliest'] = first_all
                if last_all and (tm['latest'] is None or last_all > tm['latest']):
                    tm['latest'] = last_all

            use_days_final = info['use_days']
            if use_days_final:
                first_day = use_days_final[0]
                last_day = use_days_final[-1]
                new_stats_map[(com, label.upper())] = (
                    len(use_days_final),
                    first_day,
                    last_day,
                    info['meta'],
                    info['is_complete'],
                )

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
