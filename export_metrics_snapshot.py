"""
Export current metrics cache data to JSON files for analysis.
This creates snapshot files that can be shared/compared between dev and server.
"""
import sqlite3
import json
from datetime import datetime

def export_metrics_cache():
    """Export all cached metrics to JSON files with timestamps."""
    
    conn = sqlite3.connect('SCHLabor.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Get all cached metrics
    cur.execute("SELECT metric_type, metric_data, computed_at, trigger_source FROM MetricsCache")
    cache_entries = cur.fetchall()
    
    if not cache_entries:
        print("❌ No metrics cache found!")
        return
    
    print("\n" + "="*80)
    print("EXPORTING METRICS CACHE DATA")
    print("="*80)
    
    for entry in cache_entries:
        metric_type = entry['metric_type']
        metric_data = json.loads(entry['metric_data'])
        computed_at = entry['computed_at']
        trigger = entry['trigger_source']
        
        # Create filename
        filename = f"metrics_export_{metric_type}_{timestamp}.json"
        
        # Build export data with metadata
        export_data = {
            'metric_type': metric_type,
            'computed_at': computed_at,
            'trigger_source': trigger,
            'exported_at': datetime.now().isoformat(),
            'data': metric_data
        }
        
        # Write to file
        with open(filename, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        print(f"\n✅ Exported: {filename}")
        print(f"   Computed: {computed_at}")
        print(f"   Trigger: {trigger}")
        
        # Show summary based on type
        if metric_type == 'trailing_trend_charts':
            print(f"   Charts: {list(metric_data.keys())}")
            if '120_10' in metric_data:
                chart = metric_data['120_10']
                eff = chart.get('avg_efficiency', [])
                if eff:
                    print(f"   120_10 Efficiency: {min(eff):.1f}% to {max(eff):.1f}% (range: {max(eff)-min(eff):.1f}%)")
        
        elif metric_type == 'unit_time_trends':
            last10 = metric_data.get('last10', {})
            print(f"   Last 10 units: {last10.get('n_units', 0)} units, Avg Eff: {last10.get('avg_efficiency', 0):.1f}%")
        
        elif metric_type == 'incomplete_units':
            print(f"   Incomplete units: {metric_data.get('in_progress_count', 0)}")
        
        elif metric_type == 'daily_metric_charts':
            periods = list(metric_data.keys()) if isinstance(metric_data, dict) else []
            print(f"   Periods: {periods}")
        
        elif metric_type == 'department_totals':
            windows = list(metric_data.keys()) if isinstance(metric_data, dict) else []
            print(f"   Windows: {windows}")
    
    # Also export database info
    print("\n" + "="*80)
    print("DATABASE INFO")
    print("="*80)
    
    cur.execute("SELECT MAX(LoggedDate) as latest, MIN(LoggedDate) as earliest, COUNT(*) as total FROM SCHLabor")
    labor_info = cur.fetchone()
    
    cur.execute("SELECT COUNT(*) as total FROM UnitCompletion")
    completion_info = cur.fetchone()
    
    cur.execute("SELECT refreshed_at, trigger_source FROM MetricsRefreshLog ORDER BY refreshed_at DESC LIMIT 1")
    last_refresh = cur.fetchone()
    
    db_info = {
        'exported_at': datetime.now().isoformat(),
        'labor_data': {
            'earliest_date': labor_info['earliest'],
            'latest_date': labor_info['latest'],
            'total_rows': labor_info['total']
        },
        'complete_units': completion_info['total'],
        'last_refresh': {
            'timestamp': last_refresh['refreshed_at'] if last_refresh else None,
            'trigger': last_refresh['trigger_source'] if last_refresh else None
        }
    }
    
    db_filename = f"metrics_export_database_info_{timestamp}.json"
    with open(db_filename, 'w') as f:
        json.dump(db_info, f, indent=2)
    
    print(f"\n✅ Exported: {db_filename}")
    print(f"   Labor data: {labor_info['earliest']} to {labor_info['latest']}")
    print(f"   Complete units: {completion_info['total']:,}")
    
    conn.close()
    
    print("\n" + "="*80)
    print("EXPORT COMPLETE")
    print("="*80)
    print("\nFiles created:")
    print("  - metrics_export_trailing_trend_charts_*.json")
    print("  - metrics_export_unit_time_trends_*.json")
    print("  - metrics_export_incomplete_units_*.json")
    print("  - metrics_export_daily_metric_charts_*.json")
    print("  - metrics_export_department_totals_*.json")
    print("  - metrics_export_database_info_*.json")
    print("\nCopy these files to compare dev vs server!")

if __name__ == '__main__':
    export_metrics_cache()
