"""
Reports Blueprint - CSV Export with Department Day Analysis
"""
from flask import Blueprint, render_template_string, request, Response, jsonify
import sqlite3
from datetime import datetime, date
from collections import defaultdict
import csv
from io import StringIO

reports = Blueprint('reports', __name__)


@reports.route('/reports')
def reports_page():
    """Reports page with date range picker and logic override panel."""
    from . import utils
    
    # Get current logic settings to populate the form
    outlier_caps = utils.OUTLIER_CAPS
    dept_rules = utils.DEPARTMENT_RULES
    exclusion_employees = utils.PROJECT_DAY_RULES.get('exclusion_employees', [])
    
    # Get exclusion employee names
    exclusion_names = {}
    if exclusion_employees:
        with utils.get_conn() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            placeholders = ','.join(['?' for _ in exclusion_employees])
            cur.execute(f"""
                SELECT DISTINCT EmployeeNumber1, EmployeeName
                FROM SCHLabor
                WHERE EmployeeNumber1 IN ({placeholders})
            """, exclusion_employees)
            exclusion_names = {str(r['EmployeeNumber1']): r['EmployeeName'] for r in cur.fetchall()}
    
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Unit Reports - Export</title>
        <style>
            * { margin:0; padding:0; box-sizing:border-box; }
            body { 
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                background: linear-gradient(135deg, #0d1117 0%, #161b22 100%);
                color: #e6edf3;
                padding: 2rem;
                min-height: 100vh;
            }
            .container { max-width: 1400px; margin: 0 auto; }
            h1 { 
                font-size: 2rem; 
                margin-bottom: 0.5rem;
                background: linear-gradient(135deg, #58a6ff 0%, #79c0ff 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                background-clip: text;
            }
            .subtitle { 
                color: #8b949e; 
                font-size: 0.9rem; 
                margin-bottom: 2rem;
            }
            .panel {
                background: #161b22;
                border: 1px solid #30363d;
                border-radius: 8px;
                padding: 1.5rem;
                margin-bottom: 1.5rem;
            }
            .panel-title {
                font-size: 1.1rem;
                font-weight: 600;
                margin-bottom: 1rem;
                color: #58a6ff;
            }
            .form-group {
                margin-bottom: 1rem;
            }
            label {
                display: block;
                font-size: 0.85rem;
                color: #8b949e;
                margin-bottom: 0.4rem;
                font-weight: 500;
            }
            input[type="date"], input[type="number"], input[type="text"] {
                width: 100%;
                padding: 0.6rem;
                background: #0d1117;
                border: 1px solid #30363d;
                border-radius: 6px;
                color: #e6edf3;
                font-size: 0.9rem;
            }
            input[type="date"]:focus, input[type="number"]:focus, input[type="text"]:focus {
                outline: none;
                border-color: #58a6ff;
                box-shadow: 0 0 0 3px rgba(88, 166, 255, 0.1);
            }
            .date-range {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 1rem;
            }
            .dept-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                gap: 1rem;
                margin-top: 1rem;
            }
            .dept-box {
                background: #0d1117;
                border: 1px solid #21262d;
                border-radius: 6px;
                padding: 1rem;
            }
            .dept-box h3 {
                font-size: 0.9rem;
                color: #79c0ff;
                margin-bottom: 0.8rem;
                border-bottom: 1px solid #21262d;
                padding-bottom: 0.5rem;
            }
            .input-row {
                display: grid;
                grid-template-columns: 1fr 1fr 1fr;
                gap: 0.5rem;
                margin-bottom: 0.5rem;
            }
            .input-row label {
                font-size: 0.7rem;
            }
            .input-row input {
                padding: 0.4rem;
                font-size: 0.85rem;
            }
            .btn {
                padding: 0.7rem 1.5rem;
                border: none;
                border-radius: 6px;
                font-size: 0.9rem;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.2s;
            }
            .btn-primary {
                background: linear-gradient(135deg, #238636 0%, #2ea043 100%);
                color: white;
            }
            .btn-primary:hover {
                background: linear-gradient(135deg, #2ea043 0%, #3fb950 100%);
                transform: translateY(-1px);
                box-shadow: 0 4px 12px rgba(46, 160, 67, 0.3);
            }
            .btn-secondary {
                background: #21262d;
                color: #e6edf3;
                border: 1px solid #30363d;
            }
            .btn-secondary:hover {
                background: #30363d;
            }
            .actions {
                display: flex;
                gap: 1rem;
                margin-top: 2rem;
            }
            .exclusion-list {
                background: #0d1117;
                border: 1px solid #21262d;
                border-radius: 6px;
                padding: 0.8rem;
                margin-top: 0.5rem;
            }
            .exclusion-item {
                padding: 0.4rem;
                margin-bottom: 0.3rem;
                background: #161b22;
                border-radius: 4px;
                font-size: 0.85rem;
            }
            .note {
                background: #1c2128;
                border-left: 3px solid #58a6ff;
                padding: 0.8rem;
                border-radius: 4px;
                margin-top: 1rem;
                font-size: 0.85rem;
                color: #8b949e;
            }
            .loading {
                display: none;
                text-align: center;
                padding: 2rem;
                color: #58a6ff;
            }
            .loading.active {
                display: block;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 Unit Reports - Export</h1>
            <p class="subtitle">Generate detailed CSV reports with filtered and unfiltered department day analysis</p>
            
            <div class="panel">
                <div class="panel-title">📅 Date Range (Based on Unit Last Day)</div>
                <div class="date-range">
                    <div class="form-group">
                        <label>Start Date</label>
                        <input type="date" id="start_date" value="">
                    </div>
                    <div class="form-group">
                        <label>End Date</label>
                        <input type="date" id="end_date" value="">
                    </div>
                </div>
                <div class="note">
                    💡 <strong>Note:</strong> Date range filters units based on their <strong>last day</strong> (latest charge date). 
                    Leave blank to include all units.
                </div>
            </div>
            
            <div class="panel">
                <div class="panel-title">⚙️ Logic Override (Report Only)</div>
                <p style="font-size: 0.85rem; color: #8b949e; margin-bottom: 1rem;">
                    These settings are loaded from the Logic page but only affect this report. 
                    Changes here do NOT update the main logic page. Settings revert on page reload.
                </p>
                
                <div class="dept-grid" id="dept_rules_grid">
                    <!-- Department rules will be populated by JavaScript -->
                </div>
                
                <div style="margin-top: 1.5rem;">
                    <label>Exclusion Employees (comma-separated IDs)</label>
                    <input type="text" id="exclusion_employees" value="{{ exclusion_ids }}" 
                           placeholder="e.g., 1205797, 9999999">
                    {% if exclusion_names %}
                    <div class="exclusion-list">
                        <div style="font-size: 0.75rem; color: #8b949e; margin-bottom: 0.5rem;">Current Exclusions:</div>
                        {% for emp_id, emp_name in exclusion_names.items() %}
                        <div class="exclusion-item">{{ emp_id }} - {{ emp_name }}</div>
                        {% endfor %}
                    </div>
                    {% endif %}
                </div>
            </div>
            
            <div class="loading" id="loading">
                <div style="font-size: 2rem;">⏳</div>
                <div style="margin-top: 0.5rem;">Generating report...</div>
            </div>
            
            <div class="actions">
                <button class="btn btn-primary" onclick="exportCSV()">📥 Export CSV</button>
                <button class="btn btn-secondary" onclick="resetLogic()">🔄 Reset to Logic Page Defaults</button>
            </div>
        </div>
        
        <script>
            // Department configuration from server
            const DEPT_RULES = {{ dept_rules_json|safe }};
            const OUTLIER_CAPS = {{ outlier_caps_json|safe }};
            const ORIGINAL_EXCLUSIONS = "{{ exclusion_ids }}";
            
            // Populate department rules grid
            function populateDeptGrid() {
                const grid = document.getElementById('dept_rules_grid');
                const depts = Object.keys(DEPT_RULES).sort();
                
                grid.innerHTML = depts.map(dept => `
                    <div class="dept-box">
                        <h3>${dept}</h3>
                        <div class="input-row">
                            <div>
                                <label>Min Hours</label>
                                <input type="number" step="0.1" 
                                       id="dept_min_hours_${dept}" 
                                       value="${DEPT_RULES[dept].min_hours}">
                            </div>
                            <div>
                                <label>Min Emps</label>
                                <input type="number" 
                                       id="dept_min_employees_${dept}" 
                                       value="${DEPT_RULES[dept].min_employees}">
                            </div>
                            <div>
                                <label>Gap Cap</label>
                                <input type="number" 
                                       id="dept_outlier_cap_${dept}" 
                                       value="${OUTLIER_CAPS[dept] || 7}">
                            </div>
                        </div>
                    </div>
                `).join('');
            }
            
            // Reset to original logic page values
            function resetLogic() {
                populateDeptGrid();
                document.getElementById('exclusion_employees').value = ORIGINAL_EXCLUSIONS;
            }
            
            // Export CSV
            async function exportCSV() {
                const startDate = document.getElementById('start_date').value;
                const endDate = document.getElementById('end_date').value;
                const exclusions = document.getElementById('exclusion_employees').value;
                
                // Collect department rules
                const deptRules = {};
                const outlierCaps = {};
                Object.keys(DEPT_RULES).forEach(dept => {
                    deptRules[dept] = {
                        min_hours: parseFloat(document.getElementById(`dept_min_hours_${dept}`).value),
                        min_employees: parseInt(document.getElementById(`dept_min_employees_${dept}`).value)
                    };
                    outlierCaps[dept] = parseInt(document.getElementById(`dept_outlier_cap_${dept}`).value);
                });
                
                // Show loading
                document.getElementById('loading').classList.add('active');
                
                try {
                    const response = await fetch('/api/reports/export', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({
                            start_date: startDate,
                            end_date: endDate,
                            exclusion_employees: exclusions,
                            dept_rules: deptRules,
                            outlier_caps: outlierCaps
                        })
                    });
                    
                    if (!response.ok) {
                        throw new Error('Export failed');
                    }
                    
                    // Download the CSV
                    const blob = await response.blob();
                    const url = window.URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = `unit_report_${new Date().toISOString().split('T')[0]}.csv`;
                    document.body.appendChild(a);
                    a.click();
                    window.URL.revokeObjectURL(url);
                    document.body.removeChild(a);
                } catch (error) {
                    alert('Error generating report: ' + error.message);
                } finally {
                    document.getElementById('loading').classList.remove('active');
                }
            }
            
            // Initialize
            populateDeptGrid();
            
            // Set default date range (last 90 days)
            const today = new Date();
            const ninetyDaysAgo = new Date(today);
            ninetyDaysAgo.setDate(today.getDate() - 90);
            document.getElementById('end_date').value = today.toISOString().split('T')[0];
            document.getElementById('start_date').value = ninetyDaysAgo.toISOString().split('T')[0];
        </script>
    </body>
    </html>
    """
    
    import json
    exclusion_ids = ','.join(exclusion_employees)
    dept_rules_json = json.dumps(dept_rules)
    outlier_caps_json = json.dumps(outlier_caps)
    
    return render_template_string(html, 
                                 exclusion_ids=exclusion_ids,
                                 exclusion_names=exclusion_names,
                                 dept_rules_json=dept_rules_json,
                                 outlier_caps_json=outlier_caps_json)


@reports.route('/api/reports/export', methods=['POST'])
def export_report():
    """Generate CSV export with all department day analysis."""
    from . import utils
    from .utils import normalize_com, fnum
    import sys
    import os
    
    # Add parent directory to path for metrics_cache import
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
    from metrics_cache import get_complete_units_with_dates
    
    data = request.json
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    exclusion_employees = [e.strip() for e in data.get('exclusion_employees', '').split(',') if e.strip()]
    dept_rules = data.get('dept_rules', {})
    outlier_caps = data.get('outlier_caps', {})
    
    # Get dict of 100% complete units with their last days from database
    # {com: last_day_unfiltered}
    complete_units_dict = get_complete_units_with_dates()
    if not complete_units_dict:
        # If table is empty, return error message
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['Error', 'UnitCompletion table is empty. Please refresh unit completion data from Tasks page.'])
        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename=error_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'}
        )
    
    # Filter by date range FIRST (using pre-computed last days)
    if start_date or end_date:
        filtered_complete_units = {}
        for com, last_day in complete_units_dict.items():
            if start_date and last_day < start_date:
                continue
            if end_date and last_day > end_date:
                continue
            filtered_complete_units[com] = last_day
        complete_units_dict = filtered_complete_units
    
    complete_units = set(complete_units_dict.keys())
    
    data = request.json
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    exclusion_employees = [e.strip() for e in data.get('exclusion_employees', '').split(',') if e.strip()]
    dept_rules = data.get('dept_rules', {})
    outlier_caps = data.get('outlier_caps', {})
    
    # Create a temporary copy of logic rules for this report
    temp_project_rules = {
        'outlier_caps': outlier_caps,
        'exclusion_employees': exclusion_employees,
        'department_rules': dept_rules,
    }
    
    # Get all labor data
    with utils.get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        # Department code mapping
        raw_code_to_label = {
            '0120':'Fab','0140':'Welding','0180':'BaseFormPaint','0200':'FanAssyTest','0220':'InsulWallFab',
            '0230':'Pipe','0260':'Assembly','0270':'DoorFab','0280':'Assembly','0300':'Electrical','0320':'Pipe',
            '0340':'Paint','0360':'Test','0380':'Crating',
        }
        
        tracked_codes = sorted(raw_code_to_label.keys())
        codes_sql = ','.join(f"'{c}'" for c in tracked_codes)
        
        cur.execute(f"""
            SELECT CAST(COMNumber AS TEXT) com,
                   DepartmentNumber dept,
                   strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10))) day,
                   EmployeeNumber1 emp,
                   COALESCE(ActualHours,0) hrs
            FROM SCHLabor
            WHERE COALESCE(ActualHours,0) > 0
              AND DepartmentNumber IN ({codes_sql})
              AND CAST(COMNumber AS TEXT) GLOB '[0-9][0-9][0-9][0-9][0-9]'
        """)
        rows = cur.fetchall()
        
        # Get completion status from scheduling summary - ONLY include 100% complete units
        from .utils import TRACKED_DEPARTMENTS, COMPLETION_CHECK_DEPARTMENTS
        
        cur.execute('PRAGMA table_info(SCHSchedulingSummary)')
        colset = {r[1] for r in cur.fetchall()}
        base_needed = ['comnumber1']
        for _, stdc, actc, compc, effc in TRACKED_DEPARTMENTS:
            base_needed.extend([stdc, actc, compc, effc])
        present = [c for c in base_needed if c in colset]
        cols_sql = ','.join(f'"{c}"' for c in present) if present else 'comnumber1'
        cur.execute(f'SELECT {cols_sql} FROM SCHSchedulingSummary WHERE CAST(comnumber1 AS TEXT) GLOB "[0-9][0-9][0-9][0-9][0-9]"')
        completion_rows = [dict(r) for r in cur.fetchall()]
    
    # Build completion map - only fully complete units
    completion_map = {}
    for r in completion_rows:
        com_raw = r.get('comnumber1')
        if not com_raw:
            continue
        com = normalize_com(str(com_raw))
        all_complete = True
        
        for label, stdc, actc, compc, effc in COMPLETION_CHECK_DEPARTMENTS:
            std = fnum(r.get(stdc)) if stdc in r else 0.0
            if std <= 0:
                continue
            
            comp = fnum(r.get(compc)) if compc in r else 0.0
            if comp < 100.0:
                act = fnum(r.get(actc)) if actc in r else 0.0
                calc_comp = min(100.0, (act / std) * 100.0) if act > 0 else 0.0
                if calc_comp < 99.999:  # Allow tiny rounding tolerance
                    all_complete = False
                    break
        
        completion_map[com] = all_complete
    
    # Aggregate by COM, dept, day
    day_emp = defaultdict(lambda: defaultdict(lambda: {'emps': set(), 'hours': 0.0}))
    for r in rows:
        com = normalize_com(str(r['com']))
        label = raw_code_to_label.get(r['dept'])
        if not label or not r['day']:
            continue
        key = (com, label)
        dm = day_emp[key]
        rec = dm[r['day']]
        if r['emp']:
            rec['emps'].add(str(r['emp']).strip())
        rec['hours'] += float(r['hrs']) if r['hrs'] else 0.0
    
    # Calculate filtered and unfiltered days for each COM+dept
    from .utils import _filter_days_by_gap
    
    com_dept_data = {}
    for (com, label), daymap in day_emp.items():
        # Unfiltered: all days with >0 hours
        all_days = sorted([d for d in daymap.keys() if daymap[d]['hours'] > 0])
        
        # Apply filtering logic using report's custom rules
        excl_set = set(exclusion_employees)
        dept_rule = dept_rules.get(label, {})
        min_hours = dept_rule.get('min_hours', 2.0)
        min_employees = dept_rule.get('min_employees', 2)
        
        # Filter by hours and employees
        filtered_days = []
        for d in all_days:
            all_emps = daymap[d]['emps']
            valid_emps = all_emps - excl_set
            total_hours = daymap[d]['hours']
            
            # Skip if only excluded employees
            if not valid_emps:
                continue
            
            # Keep if >= min_employees OR >= min_hours
            if len(valid_emps) >= min_employees or total_hours >= min_hours:
                filtered_days.append(d)
        
        # Apply gap filtering
        if filtered_days:
            cap = outlier_caps.get(label, 7)
            # For simplicity, assume department is complete for filtering
            filtered_days = _filter_days_by_gap(label, sorted(filtered_days), daymap, 
                                               is_complete=True, 
                                               excl_employees=exclusion_employees,
                                               min_employees=min_employees)
        
        com_dept_data[(com, label)] = {
            'unfiltered_days': all_days,
            'filtered_days': filtered_days
        }
    
    # Aggregate by COM to get unit-level data (ONLY for 100% complete units)
    com_data = defaultdict(lambda: {
        'departments': {},
        'unit_unfiltered_days': set(),
        'unit_filtered_days': set()
    })
    
    for (com, label), data in com_dept_data.items():
        # Skip if unit is not 100% complete (already filtered by date range above)
        if com not in complete_units:
            continue
        
        com_data[com]['departments'][label] = data
        com_data[com]['unit_unfiltered_days'].update(data['unfiltered_days'])
        com_data[com]['unit_filtered_days'].update(data['filtered_days'])
    
    # com_data now contains only complete units (already filtered by date range)
    filtered_coms = com_data
    
    # Generate CSV
    output = StringIO()
    writer = csv.writer(output)
    
    # Department list for headers
    all_depts = sorted(set(label for com_data in filtered_coms.values() 
                          for label in com_data['departments'].keys()))
    
    # Build header row
    header = ['COM#']
    
    # Per-department columns
    for dept in all_depts:
        header.extend([
            f'{dept}_First_Unfiltered',
            f'{dept}_First_Filtered',
            f'{dept}_Last_Unfiltered',
            f'{dept}_Last_Filtered',
            f'{dept}_Act_Days',
            f'{dept}_Span'
        ])
    
    # Unit-level columns
    header.extend([
        'Unit_First_Unfiltered',
        'Unit_First_Filtered',
        'Unit_Last_Unfiltered',
        'Unit_Last_Filtered',
        'Unit_Act_Days',
        'Unit_Span'
    ])
    
    writer.writerow(header)
    
    # Sort units by last day (newest first) using pre-computed values from database
    com_with_last_day = [(com, complete_units_dict.get(com, '')) for com in filtered_coms.keys()]
    com_with_last_day.sort(key=lambda x: (x[1] if x[1] else '', x[0]), reverse=True)
    
    # Write data rows
    for com, _ in com_with_last_day:
        data = filtered_coms[com]
        row = [com]
        
        # Per-department data
        for dept in all_depts:
            dept_data = data['departments'].get(dept)
            if dept_data:
                unfilt = dept_data['unfiltered_days']
                filt = dept_data['filtered_days']
                
                first_unfilt = unfilt[0] if unfilt else ''
                first_filt = filt[0] if filt else ''
                last_unfilt = unfilt[-1] if unfilt else ''
                last_filt = filt[-1] if filt else ''
                
                act_days = len(filt)
                span = 0
                if filt:
                    try:
                        span = (date.fromisoformat(filt[-1]) - date.fromisoformat(filt[0])).days + 1
                    except:
                        span = 0
                
                row.extend([first_unfilt, first_filt, last_unfilt, last_filt, act_days, span])
            else:
                row.extend(['', '', '', '', '', ''])
        
        # Unit-level data
        unit_unfilt = sorted(data['unit_unfiltered_days'])
        unit_filt = sorted(data['unit_filtered_days'])
        
        unit_first_unfilt = unit_unfilt[0] if unit_unfilt else ''
        unit_first_filt = unit_filt[0] if unit_filt else ''
        unit_last_unfilt = unit_unfilt[-1] if unit_unfilt else ''
        unit_last_filt = unit_filt[-1] if unit_filt else ''
        
        unit_act_days = len(unit_filt)
        unit_span = 0
        if unit_filt:
            try:
                unit_span = (date.fromisoformat(unit_filt[-1]) - date.fromisoformat(unit_filt[0])).days + 1
            except:
                unit_span = 0
        
        row.extend([unit_first_unfilt, unit_first_filt, unit_last_unfilt, unit_last_filt, 
                   unit_act_days, unit_span])
        
        writer.writerow(row)
    
    # Return CSV as downloadable file
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=unit_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'}
    )
