import sqlite3
from datetime import datetime

conn = sqlite3.connect('SCHLabor.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("\n" + "="*80)
print("RECENT METRICS REFRESH LOG:")
print("="*80)

cur.execute("""
    SELECT refreshed_at, trigger_source, duration_seconds, success, error_message
    FROM MetricsRefreshLog
    ORDER BY refreshed_at DESC
    LIMIT 10
""")

logs = cur.fetchall()

if not logs:
    print("❌ No refresh logs found!")
else:
    print(f"{'Timestamp':<25} {'Trigger':<20} {'Duration':<12} {'Status':<10}")
    print("-" * 80)
    for log in logs:
        success_icon = "✅" if log['success'] else "❌"
        timestamp = log['refreshed_at']
        
        # Calculate how long ago
        try:
            dt = datetime.fromisoformat(timestamp)
            age = datetime.now() - dt
            if age.days > 0:
                age_str = f"{age.days}d ago"
            elif age.seconds > 3600:
                age_str = f"{age.seconds // 3600}h ago"
            else:
                age_str = f"{age.seconds // 60}m ago"
        except:
            age_str = ""
        
        print(f"{timestamp:<25} {log['trigger_source']:<20} {log['duration_seconds']:>8.2f}s    {success_icon} {age_str}")
        if log['error_message']:
            print(f"  ⚠️  Error: {log['error_message']}")

print("\n" + "="*80)
print("CURRENT CACHE STATUS:")
print("="*80)

cur.execute("""
    SELECT metric_type, computed_at
    FROM MetricsCache
    ORDER BY metric_type
""")

cache_entries = cur.fetchall()

for entry in cache_entries:
    timestamp = entry['computed_at']
    try:
        dt = datetime.fromisoformat(timestamp)
        age = datetime.now() - dt
        if age.days > 0:
            age_str = f"{age.days} days old"
            stale = "⚠️  STALE" if age.days > 1 else ""
        elif age.seconds > 3600:
            age_str = f"{age.seconds // 3600} hours old"
            stale = ""
        else:
            age_str = f"{age.seconds // 60} minutes old"
            stale = ""
    except:
        age_str = "unknown"
        stale = ""
    
    print(f"{entry['metric_type']:<30} {timestamp:<25} ({age_str}) {stale}")

conn.close()
