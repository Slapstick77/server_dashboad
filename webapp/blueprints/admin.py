from flask import Blueprint, request, render_template_string, jsonify, redirect
import sqlite3
import subprocess
import re
import sys
import os
from . import utils

admin = Blueprint('admin', __name__)

@admin.route('/metrics/refresh', methods=['POST'])
def refresh_metrics():
    """Manually refresh the metrics cache."""
    try:
        # Add parent directory to path to import metrics_cache
        parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        sys.path.insert(0, parent_dir)
        from metrics_cache import refresh_metrics_cache
        
        result = refresh_metrics_cache(trigger='manual_refresh')
        
        if result['success']:
            return f"""<!DOCTYPE html>
            <html>
            <head>
                <title>Metrics Refreshed</title>
                <meta http-equiv="refresh" content="2;url=/tasks">
                <style>
                    body {{ font-family: Arial; text-align: center; padding: 50px; background: #0d1117; color: #e6edf3; }}
                    .success {{ color: #3fb950; font-size: 24px; }}
                </style>
            </head>
            <body>
                <div class="success">✓ Metrics cache refreshed in {result['duration_seconds']:.2f}s!</div>
                <p>Redirecting back to tasks page...</p>
            </body>
            </html>"""
        else:
            return f"""<!DOCTYPE html>
            <html>
            <head>
                <title>Error</title>
                <style>
                    body {{ font-family: Arial; text-align: center; padding: 50px; background: #0d1117; color: #e6edf3; }}
                    .error {{ color: #f85149; font-size: 18px; }}
                </style>
            </head>
            <body>
                <div class="error">Failed to refresh metrics: {result.get('error', 'Unknown error')}</div>
                <p><a href="/tasks" style="color: #8fb9ff;">Back to tasks page</a></p>
            </body>
            </html>"""
    except Exception as e:
        return f"""<!DOCTYPE html>
        <html>
        <head>
            <title>Error</title>
            <style>
                body {{ font-family: Arial; text-align: center; padding: 50px; background: #0d1117; color: #e6edf3; }}
                .error {{ color: #f85149; font-size: 18px; }}
            </style>
        </head>
        <body>
            <div class="error">Error: {str(e)}</div>
            <p><a href="/tasks" style="color: #8fb9ff;">Back to tasks page</a></p>
        </body>
        </html>"""

@admin.route('/completion/refresh', methods=['POST'])
def refresh_completion():
    """Manually refresh the unit completion table."""
    try:
        parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        sys.path.insert(0, parent_dir)
        from metrics_cache import refresh_unit_completion
        
        result = refresh_unit_completion()
        
        if result['success']:
            return f"""<!DOCTYPE html>
            <html>
            <head>
                <title>Completion Refreshed</title>
                <meta http-equiv="refresh" content="2;url=/tasks">
                <style>
                    body {{ font-family: Arial; text-align: center; padding: 50px; background: #0d1117; color: #e6edf3; }}
                    .success {{ color: #3fb950; font-size: 24px; }}
                </style>
            </head>
            <body>
                <div class="success">✓ Unit completion refreshed!</div>
                <p>Total: {result['total_units']} units | Complete: {result['complete']} | Incomplete: {result['incomplete']}</p>
                <p>Redirecting back to tasks page...</p>
            </body>
            </html>"""
        else:
            return f"""<!DOCTYPE html>
            <html>
            <head>
                <title>Error</title>
                <style>
                    body {{ font-family: Arial; text-align: center; padding: 50px; background: #0d1117; color: #e6edf3; }}
                    .error {{ color: #f85149; font-size: 18px; }}
                </style>
            </head>
            <body>
                <div class="error">Failed to refresh completion data</div>
                <p><a href="/tasks" style="color: #8fb9ff;">Back to tasks page</a></p>
            </body>
            </html>"""
    except Exception as e:
        return f"""<!DOCTYPE html>
        <html>
        <head>
            <title>Error</title>
            <style>
                body {{ font-family: Arial; text-align: center; padding: 50px; background: #0d1117; color: #e6edf3; }}
                .error {{ color: #f85149; font-size: 18px; }}
            </style>
        </head>
        <body>
            <div class="error">Error: {str(e)}</div>
            <p><a href="/tasks" style="color: #8fb9ff;">Back to tasks page</a></p>
        </body>
        </html>"""

@admin.route('/tasks')
def list_tasks():
    """View and manage scheduled tasks."""
    # Get all tasks that match SQRS pattern
    try:
        result = subprocess.run(
            ['schtasks', '/query', '/fo', 'LIST', '/v'],
            capture_output=True,
            text=True
        )
        
        tasks = []
        if result.returncode == 0:
            # Parse the output to find SQRS-related tasks
            current_task = {}
            for line in result.stdout.split('\n'):
                line = line.strip()
                if line.startswith('TaskName:'):
                    if current_task and ('SCH' in current_task.get('name', '') or 'SQRS' in current_task.get('name', '')):
                        tasks.append(current_task)
                    current_task = {'name': line.split(':', 1)[1].strip().replace('\\', '')}
                elif line.startswith('Next Run Time:'):
                    current_task['next_run'] = line.split(':', 1)[1].strip()
                elif line.startswith('Status:'):
                    current_task['status'] = line.split(':', 1)[1].strip()
                elif line.startswith('Task To Run:'):
                    current_task['command'] = line.split(':', 1)[1].strip()
                elif line.startswith('Schedule Type:'):
                    current_task['schedule'] = line.split(':', 1)[1].strip()
            
            # Don't forget the last task
            if current_task and ('SCH' in current_task.get('name', '') or 'SQRS' in current_task.get('name', '')):
                tasks.append(current_task)
    except Exception as e:
        tasks = []
        error = str(e)
    
    # Get latest RunLog entries
    import sqlite3
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'SCHLabor.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT id, run_started, run_completed, run_type, success, message FROM RunLog ORDER BY id DESC LIMIT 20')
    log_entries = cursor.fetchall()
    conn.close()
    
    log_html = ''
    for entry in log_entries:
        log_id, started, completed, run_type, success, message = entry
        status_class = 'success' if success else 'error'
        log_html += f'''
        <div class="log-entry {status_class}">
            <div class="log-header">
                <span class="log-id">#{log_id}</span>
                <span class="log-type">{run_type}</span>
                <span class="log-status">{'✓' if success else '✗'}</span>
            </div>
            <div class="log-time">Started: {started} | Completed: {completed}</div>
            <div class="log-message">{message}</div>
        </div>'''
    
    page = f"""<!DOCTYPE html>
    <html>
    <head>
        <title>Scheduled Tasks</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
            .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            h1 {{ color: #2c3e50; }}
            h2 {{ color: #34495e; margin-top: 30px; }}
            .back-link {{ margin-bottom: 20px; }}
            .back-link a {{ color: #3498db; text-decoration: none; }}
            .back-link a:hover {{ text-decoration: underline; }}
            .task-list {{ margin-top: 20px; }}
            .task-card {{ background: #f8f9fa; padding: 15px; margin-bottom: 15px; border-radius: 5px; border-left: 4px solid #3498db; }}
            .task-card.disabled {{ border-left-color: #95a5a6; }}
            .task-name {{ font-size: 18px; font-weight: bold; color: #2c3e50; }}
            .task-info {{ margin: 8px 0; color: #555; }}
            .task-command {{ font-family: monospace; background: #fff; padding: 8px; border-radius: 3px; font-size: 12px; margin: 8px 0; }}
            .btn-delete {{ background: #e74c3c; color: white; padding: 8px 15px; border: none; border-radius: 3px; cursor: pointer; }}
            .btn-delete:hover {{ background: #c0392b; }}
            .btn-refresh {{ background: #3498db; color: white; padding: 10px 20px; border: none; border-radius: 3px; cursor: pointer; font-size: 14px; }}
            .btn-refresh:hover {{ background: #2980b9; }}
            .btn-clear {{ background: #e67e22; color: white; padding: 8px 15px; border: none; border-radius: 3px; cursor: pointer; font-size: 14px; }}
            .btn-clear:hover {{ background: #d35400; }}
            .no-tasks {{ color: #7f8c8d; font-style: italic; padding: 20px; text-align: center; }}
            .header-actions {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }}
            .log-section {{ margin-top: 30px; background: #f8f9fa; padding: 20px; border-radius: 5px; }}
            .log-header-section {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; }}
            .log-entry {{ background: white; padding: 12px; margin-bottom: 10px; border-radius: 4px; border-left: 4px solid #27ae60; font-size: 13px; }}
            .log-entry.error {{ border-left-color: #e74c3c; }}
            .log-header {{ display: flex; gap: 15px; align-items: center; margin-bottom: 5px; }}
            .log-id {{ color: #7f8c8d; font-weight: bold; }}
            .log-type {{ background: #3498db; color: white; padding: 2px 8px; border-radius: 3px; font-size: 11px; font-weight: bold; }}
            .log-status {{ font-size: 16px; }}
            .log-time {{ color: #7f8c8d; font-size: 11px; margin: 5px 0; }}
            .log-message {{ color: #2c3e50; margin-top: 5px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="back-link">
                <a href="/dash">&larr; Back to Dashboard</a>
            </div>
            
            <div class="header-actions">
                <h1>Scheduled Tasks & Metrics</h1>
                <form method="POST" action="/metrics/refresh" style="display:inline;margin-right:10px;">
                    <button type="submit" class="btn-refresh">🔄 Refresh Metrics Cache</button>
                </form>
                <form method="POST" action="/completion/refresh" style="display:inline;">
                    <button type="submit" class="btn-refresh">✅ Refresh Unit Completion</button>
                </form>
            </div>
            
            <div class="task-list">
                {chr(10).join([f'''
                <div class="task-card {'' if task.get('status') == 'Ready' else 'disabled'}">
                    <div class="task-name">{task.get('name', 'Unknown')}</div>
                    <div class="task-info"><strong>Status:</strong> {task.get('status', 'Unknown')}</div>
                    <div class="task-info"><strong>Next Run:</strong> {task.get('next_run', 'N/A')}</div>
                    <div class="task-info"><strong>Schedule:</strong> {task.get('schedule', 'N/A')}</div>
                    <div class="task-command">{task.get('command', 'N/A')}</div>
                    <form method="POST" action="/tasks/delete" style="margin-top: 10px;" onsubmit="return confirm('Delete task {task.get('name', '')}?');">
                        <input type="hidden" name="task_name" value="{task.get('name', '')}">
                        <button type="submit" class="btn-delete">Delete Task</button>
                    </form>
                </div>''' for task in tasks]) if tasks else '<div class="no-tasks">No SQRS-related scheduled tasks found.</div>'}
            </div>
            
            <div class="log-section">
                <div class="log-header-section">
                    <h2>Database Update Log (Latest 20)</h2>
                    <form method="POST" action="/admin/tasks/clear-log" style="display:inline;" onsubmit="return confirm('Clear all log entries?');">
                        <button type="submit" class="btn-clear">🗑️ Clear Log</button>
                    </form>
                </div>
                <div class="log-entries">
                    {log_html if log_entries else '<div class="no-tasks">No log entries found.</div>'}
                </div>
            </div>
        </div>
    </body>
    </html>"""
    
    return render_template_string(page)


@admin.route('/tasks/delete', methods=['POST'])
def delete_task():
    """Delete a scheduled task."""
    task_name = request.form.get('task_name', '').strip()
    
    if not task_name:
        return jsonify({'success': False, 'error': 'Task name required'}), 400
    
    try:
        result = subprocess.run(
            ['schtasks', '/delete', '/tn', task_name, '/f'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            return f"""<!DOCTYPE html>
            <html>
            <head>
                <title>Task Deleted</title>
                <meta http-equiv="refresh" content="2;url=/tasks">
                <style>
                    body {{ font-family: Arial; text-align: center; padding: 50px; }}
                    .success {{ color: #27ae60; font-size: 24px; }}
                </style>
            </head>
            <body>
                <div class="success">✓ Task "{task_name}" deleted successfully!</div>
                <p>Redirecting back to task list...</p>
            </body>
            </html>"""
        else:
            return f"""<!DOCTYPE html>
            <html>
            <head>
                <title>Error</title>
                <style>
                    body {{ font-family: Arial; text-align: center; padding: 50px; }}
                    .error {{ color: #e74c3c; font-size: 18px; }}
                </style>
            </head>
            <body>
                <div class="error">Failed to delete task: {result.stderr}</div>
                <p><a href="/tasks">Back to task list</a></p>
            </body>
            </html>"""
    except Exception as e:
        return f"""<!DOCTYPE html>
        <html>
        <head>
            <title>Error</title>
            <style>
                body {{ font-family: Arial; text-align: center; padding: 50px; }}
                .error {{ color: #e74c3c; font-size: 18px; }}
            </style>
        </head>
        <body>
            <div class="error">Error: {str(e)}</div>
            <p><a href="/tasks">Back to task list</a></p>
        </body>
        </html>"""


@admin.route('/tasks/clear-log', methods=['POST'])
def clear_log():
    """Clear all RunLog entries."""
    try:
        import sqlite3
        db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'SCHLabor.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM RunLog')
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        
        return f"""<!DOCTYPE html>
        <html>
        <head>
            <title>Log Cleared</title>
            <meta http-equiv="refresh" content="2;url=/tasks">
            <style>
                body {{ font-family: Arial; text-align: center; padding: 50px; }}
                .success {{ color: #27ae60; font-size: 24px; }}
            </style>
        </head>
        <body>
            <div class="success">✓ Cleared {deleted_count} log entries!</div>
            <p>Redirecting back to task list...</p>
        </body>
        </html>"""
    except Exception as e:
        return f"""<!DOCTYPE html>
        <html>
        <head>
            <title>Error</title>
            <style>
                body {{ font-family: Arial; text-align: center; padding: 50px; }}
                .error {{ color: #e74c3c; font-size: 18px; }}
            </style>
        </head>
        <body>
            <div class="error">Error clearing log: {str(e)}</div>
            <p><a href="/tasks">Back to task list</a></p>
        </body>
        </html>"""


@admin.route('/logic', methods=['GET', 'POST'])
def logic_config():
    """Configuration page for project day filtering logic."""
    # Import utils module to modify its globals
    from . import utils
    import sys
    import os
    # Add parent directory to path for metrics_cache import
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
    from metrics_cache import refresh_metrics_cache
    
    if request.method == 'POST':
        # Handle form submission - update the PROJECT_DAY_RULES
        try:
            # Handle different POST actions
            action = request.form.get('action', 'update')
            
            if action == 'add_exclusion':
                # Add new exclusion employee
                employee_id = request.form.get('new_exclusion_id', '').strip()
                if employee_id:
                    current_exclusions = utils.PROJECT_DAY_RULES.get('exclusion_employees', [])
                    if employee_id not in current_exclusions:
                        current_exclusions.append(employee_id)
                        utils.PROJECT_DAY_RULES['exclusion_employees'] = current_exclusions
                        # Save to database
                        from metrics_cache import save_configuration
                        save_configuration('EXCLUSION_EMPLOYEES', current_exclusions)
                        # Refresh metrics cache since exclusions affect filtering
                        refresh_metrics_cache(trigger='exclusion_update')
                        message = f"✅ Added exclusion employee: {employee_id} (metrics refreshed)"
                    else:
                        message = f"⚠️ Employee {employee_id} already excluded"
                else:
                    message = "❌ Employee ID required"
                    
            elif action == 'remove_exclusion':
                # Remove exclusion employee
                employee_id = request.form.get('remove_exclusion_id', '').strip()
                current_exclusions = utils.PROJECT_DAY_RULES.get('exclusion_employees', [])
                if employee_id in current_exclusions:
                    current_exclusions.remove(employee_id)
                    utils.PROJECT_DAY_RULES['exclusion_employees'] = current_exclusions
                    # Save to database
                    from metrics_cache import save_configuration
                    save_configuration('EXCLUSION_EMPLOYEES', current_exclusions)
                    # Refresh metrics cache since exclusions affect filtering
                    refresh_metrics_cache(trigger='exclusion_update')
                    message = f"✅ Removed exclusion employee: {employee_id} (metrics refreshed)"
                else:
                    message = f"❌ Employee {employee_id} not found in exclusions"
                    
            else:
                # Update all logic rules
                new_min_hours = float(request.form.get('min_hours', utils.MIN_DAY_HOURS))
                new_min_employees = int(request.form.get('min_employees_override', utils.PROJECT_DAY_RULES.get('min_employees_override', 2)))
                new_max_gap_override = int(request.form.get('max_gap_override', utils.PROJECT_DAY_RULES.get('max_gap_override', 30)))
                new_pre_fab_gap = int(request.form.get('pre_fab_gap_days', utils.PROJECT_DAY_RULES.get('pre_fab_gap_days', 10)))
                new_post_assembly_gap = int(request.form.get('post_assembly_gap_days', utils.PROJECT_DAY_RULES.get('post_assembly_gap_days', 10)))
                new_pre_fab_gap = max(0, new_pre_fab_gap)
                new_post_assembly_gap = max(0, new_post_assembly_gap)
                
                # Update per-department rules (min_hours, min_employees, and outlier_cap)
                new_dept_rules = {}
                new_outlier_caps = {}
                for dept in utils.DEPARTMENT_RULES.keys():
                    dept_min_hours = float(request.form.get(f'dept_min_hours_{dept}', utils.DEPARTMENT_RULES[dept]['min_hours']))
                    dept_min_employees = int(request.form.get(f'dept_min_employees_{dept}', utils.DEPARTMENT_RULES[dept]['min_employees']))
                    dept_outlier_cap = int(request.form.get(f'dept_outlier_cap_{dept}', utils.OUTLIER_CAPS.get(dept, 7)))
                    new_dept_rules[dept] = {
                        'min_hours': dept_min_hours,
                        'min_employees': dept_min_employees
                    }
                    new_outlier_caps[dept] = dept_outlier_cap
                
                # Update the global variables in utils module
                utils.OUTLIER_CAPS = new_outlier_caps
                utils.DEPARTMENT_RULES.update(new_dept_rules)
                utils.PROJECT_DAY_RULES.update({
                    'outlier_caps': utils.OUTLIER_CAPS,
                    'department_rules': utils.DEPARTMENT_RULES,
                    'max_gap_override': new_max_gap_override,
                    'pre_fab_gap_days': new_pre_fab_gap,
                    'post_assembly_gap_days': new_post_assembly_gap,
                })
                
                # Save configuration to database
                from metrics_cache import save_configuration
                save_configuration('OUTLIER_CAPS', new_outlier_caps)
                save_configuration('DEPARTMENT_RULES', new_dept_rules)
                save_configuration('EXCLUSION_EMPLOYEES', utils.PROJECT_DAY_RULES.get('exclusion_employees', []))
                save_configuration('MAX_GAP_OVERRIDE', new_max_gap_override)
                save_configuration('PRE_FAB_GAP_DAYS', new_pre_fab_gap)
                save_configuration('POST_ASSEMBLY_GAP_DAYS', new_post_assembly_gap)
                
                # Refresh metrics cache since filtering rules changed
                refresh_metrics_cache(trigger='logic_update')
                message = "✅ Logic rules updated successfully! (metrics refreshed)"
            
        except Exception as e:
            message = f"❌ Error updating rules: {e}"
    else:
        message = ""
    
    # Get current exclusion employee names for display
    exclusion_employees = utils.PROJECT_DAY_RULES.get('exclusion_employees', [])
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
                
                <!-- Per-Department Rules Section -->
                <div class="section">
                    <h2>⚙️ Per-Department Filtering Rules</h2>
                    <div class="definition">
                        <strong>Department Configuration:</strong> Each department has three filtering settings that control how days are counted.
                    </div>
                    <div class="definition" style="background: #e8f8f5; border-left-color: #27ae60;">
                        <strong>Min Hours:</strong> A day must have at least this many total hours to be counted (unless overridden by employee count).<br/>
                        <strong>Min Employees:</strong> If a day has ≥ this many <strong>valid (non-excluded)</strong> employees, it's ALWAYS kept regardless of hours or gap.<br/>
                        <strong>Outlier Cap (days):</strong> Max gap allowed for first/last day filtering. First day always filtered; last day only when 100% complete.
                    </div>
                    <div class="definition" style="background: #fff3cd; border-left-color: #ffc107;">
                        <strong>⚠️ Hard Maximum Gap Override:</strong> Days with gaps EXCEEDING this threshold are ALWAYS dropped, even if they have ≥ Min Employees. 
                        This prevents extreme outliers (like charges after 300+ day gaps) from inflating span calculations.
                    </div>
                    <div class="definition" style="background: #f0f7ff; border-left-color: #1f6feb;">
                        <strong>Gantt Chart:</strong> Shows ALL charged days with color coding (green=kept, yellow=excluded, red=filtered). No filtering applied to Gantt display.
                    </div>
                    
                    <!-- Global Hard Maximum Gap Setting -->
                    <div style="margin-bottom: 20px; padding: 15px; background: #ffe6e6; border: 2px solid #dc3545; border-radius: 5px;">
                        <h3 style="margin-top: 0; color: #dc3545;">🚫 Hard Maximum Gap Override</h3>
                        <div style="margin-bottom: 10px;">
                            <label style="font-weight: bold;">Maximum Gap (days):</label>
                            <input type="number" min="1" name="max_gap_override" value="{utils.PROJECT_DAY_RULES.get('max_gap_override', 30)}" style="width: 100px; padding: 8px; font-size: 16px; font-weight: bold;" />
                        </div>
                        <p style="margin: 0; font-size: 0.9em; color: #666;">
                            Charges with gaps exceeding this value will be dropped regardless of employee count. 
                            Default: 30 days. Use this to prevent extreme outliers from skewing metrics.
                        </p>
                    </div>

                    <div style="margin-bottom: 20px; padding: 15px; background: #e8f4fd; border: 2px solid #1f6feb; border-radius: 5px;">
                        <h3 style="margin-top: 0; color: #1f6feb;">🧭 Fab &amp; Assembly Span Windows</h3>
                        <div style="display: flex; flex-wrap: wrap; gap: 20px;">
                            <div>
                                <label style="font-weight: bold;">Fab Lead-In Window (days):</label>
                                <input type="number" min="0" name="pre_fab_gap_days" value="{utils.PROJECT_DAY_RULES.get('pre_fab_gap_days', 10)}" style="width: 100px; padding: 8px; font-size: 16px; font-weight: bold;" />
                            </div>
                            <div>
                                <label style="font-weight: bold;">Assembly Tail Window (days):</label>
                                <input type="number" min="0" name="post_assembly_gap_days" value="{utils.PROJECT_DAY_RULES.get('post_assembly_gap_days', 10)}" style="width: 100px; padding: 8px; font-size: 16px; font-weight: bold;" />
                            </div>
                        </div>
                        <p style="margin: 10px 0 0; font-size: 0.9em; color: #666;">
                            Days that fall more than these buffers before the first Fab cluster or after the last Assembly cluster are filtered from span metrics to eliminate stray single-day charges.
                            Default: 10 days for both windows.
                        </p>
                    </div>
                    
                    <div class="dept-grid">
                        {chr(10).join([f'''
                        <div class="dept-item">
                            <strong>{dept}</strong>
                            <div style="margin-top: 10px;">
                                <label style="font-size: 0.9em;">Min Hours:</label>
                                <input type="number" step="0.1" name="dept_min_hours_{dept}" value="{utils.DEPARTMENT_RULES[dept]['min_hours']}" style="width: 70px;" />h
                            </div>
                            <div style="margin-top: 8px;">
                                <label style="font-size: 0.9em;">Min Employees:</label>
                                <input type="number" min="1" name="dept_min_employees_{dept}" value="{utils.DEPARTMENT_RULES[dept]['min_employees']}" style="width: 70px;" />
                            </div>
                            <div style="margin-top: 8px;">
                                <label style="font-size: 0.9em;">Outlier Cap:</label>
                                <input type="number" min="1" name="dept_outlier_cap_{dept}" value="{utils.OUTLIER_CAPS.get(dept, 7)}" style="width: 70px;" />d
                            </div>
                        </div>''' for dept in sorted(utils.DEPARTMENT_RULES.keys())])}
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
Department Rules: {len(utils.DEPARTMENT_RULES)} departments configured
Outlier Caps: {len(utils.OUTLIER_CAPS)} departments configured
Exclusion Employees: {len(exclusion_employees)} configured
Fab Lead-In Window: {utils.PROJECT_DAY_RULES.get('pre_fab_gap_days', 10)} days
Assembly Tail Window: {utils.PROJECT_DAY_RULES.get('post_assembly_gap_days', 10)} days
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
