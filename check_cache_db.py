"""
Check what's actually in the MetricsCache database for trailing trend charts.
"""
import sqlite3
import os
import json

DB_PATH = os.path.join(os.path.dirname(__file__), 'SCHLabor.db')

def get_conn():
    return sqlite3.connect(DB_PATH)

print("=" * 80)
print("CHECKING METRICS CACHE DATABASE")
print("=" * 80)

with get_conn() as conn:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Get trailing trend charts cache entry
    cur.execute("""
        SELECT metric_type, computed_at, trigger_source, 
               LENGTH(metric_data) as data_size
        FROM MetricsCache
        WHERE metric_type = 'trailing_trend_charts'
    """)
    
    cache_entry = cur.fetchone()
    
    if not cache_entry:
        print("\n❌ NO CACHE ENTRY FOUND for 'trailing_trend_charts'")
        print("   Cache was not created!")
    else:
        print(f"\n✅ Cache entry exists:")
        print(f"   Type: {cache_entry['metric_type']}")
        print(f"   Last computed: {cache_entry['computed_at']}")
        print(f"   Trigger: {cache_entry['trigger_source']}")
        print(f"   Data size: {cache_entry['data_size']:,} bytes")
        
        # Get the actual data
        cur.execute("""
            SELECT metric_data
            FROM MetricsCache
            WHERE metric_type = 'trailing_trend_charts'
        """)
        
        row = cur.fetchone()
        data = json.loads(row['metric_data'])
        
        print(f"\n   Available chart keys: {list(data.keys())}")
        
        # Check 365-day chart to see June data (90-day won't have June)
        if '365_30' in data:
            chart = data['365_30']
            labels = chart.get('labels', [])
            avg_span = chart.get('avg_span', [])
            
            # Find June data points
            june_points = [(i, lbl, span) for i, (lbl, span) in enumerate(zip(labels, avg_span)) if '2025-06' in lbl]
            
            print(f"\n{'='*80}")
            print(f"SAMPLE: June 2025 Data Points (365-day, trailing 30)")
            print(f"{'='*80}")
            print(f"{'Date':<12} {'Avg Span':<12}")
            print("-" * 30)
            
            # Show ALL June points to see if there's a drop at end of month
            for i, lbl, span in june_points:
                marker = " ← COM 19044 enters here" if lbl >= '2025-06-27' else ""
                print(f"{lbl:<12} {span:>8.1f} days{marker}")
            
            # Check if we see the corrected values
            if june_points:
                early_june = [span for i, lbl, span in june_points[:5]]
                mid_june = [span for i, lbl, span in june_points[10:15] if i < len(june_points)]
                late_june = [span for i, lbl, span in june_points[-5:]]
                
                avg_early = sum(early_june) / len(early_june) if early_june else 0
                avg_mid = sum(mid_june) / len(mid_june) if mid_june else 0
                avg_late = sum(late_june) / len(late_june) if late_june else 0
                
                print(f"\n{'='*80}")
                print(f"JUNE TREND ANALYSIS:")
                print(f"{'='*80}")
                print(f"Early June (1-5) avg span: {avg_early:.1f} days")
                print(f"Mid June (10-15) avg span: {avg_mid:.1f} days")
                print(f"Late June (25-30) avg span: {avg_late:.1f} days")
                
                # With the filter, COM 19044 went from 436 to 63 days
                # June avg should drop from ~162 to something much lower
                if avg_early > 100:
                    print(f"\n⚠️  CACHE MAY BE OLD OR FILTER NOT APPLIED!")
                    print(f"   Early June still shows high span (~{avg_early:.0f} days)")
                    print(f"   Expected: ~40-50 days with 30-day gap filter applied")
                else:
                    print(f"\n✅ CACHE APPEARS UPDATED WITH FILTER!")
                    print(f"   June shows corrected spans (all under 100 days)")
            else:
                print(f"\n⚠️  No June 2025 data found in 365-day chart")
        
        # Check the last data point to see when cache was computed
        print(f"\n{'='*80}")
        print(f"CACHE TIMESTAMP CHECK:")
        print(f"{'='*80}")
        
        for key in ['90_30', '120_30', '365_30']:
            if key in data:
                chart = data[key]
                labels = chart.get('labels', [])
                if labels:
                    print(f"{key}: Last date = {labels[-1]}")
    
    # Check MetricsRefreshLog
    print(f"\n{'='*80}")
    print(f"RECENT REFRESH LOG:")
    print(f"{'='*80}")
    
    cur.execute("""
        SELECT refreshed_at, trigger_source, duration_seconds, success
        FROM MetricsRefreshLog
        ORDER BY refreshed_at DESC
        LIMIT 5
    """)
    
    logs = cur.fetchall()
    
    print(f"{'Timestamp':<25} {'Trigger':<25} {'Duration':<12} {'Success':<10}")
    print("-" * 80)
    for log in logs:
        success_icon = "✅" if log['success'] else "❌"
        print(f"{log['refreshed_at']:<25} {log['trigger_source']:<25} {log['duration_seconds']:>8.2f}s    {success_icon}")

print(f"\n{'='*80}")
