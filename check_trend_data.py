import sqlite3
import json

conn = sqlite3.connect('SCHLabor.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("\n" + "="*80)
print("TREND GRAPH DATA INSPECTION:")
print("="*80)

# Check what's in the trailing_trend_charts cache
cur.execute("""
    SELECT metric_data, computed_at
    FROM MetricsCache
    WHERE metric_type = 'trailing_trend_charts'
""")

row = cur.fetchone()

if not row:
    print("\n❌ No trailing_trend_charts in cache!")
else:
    print(f"\nLast computed: {row['computed_at']}")
    
    data = json.loads(row['metric_data'])
    
    # Check the 120_10 chart (default view)
    if '120_10' in data:
        chart = data['120_10']
        
        print(f"\n📊 120-day / Trailing-10 Chart:")
        print(f"   Data points: {len(chart.get('labels', []))}")
        
        # Show first 5 and last 5 dates
        labels = chart.get('labels', [])
        efficiency = chart.get('avg_efficiency', [])
        act_days = chart.get('avg_act_days', [])
        span = chart.get('avg_span', [])
        
        if labels:
            print(f"\n   First 5 days:")
            for i in range(min(5, len(labels))):
                print(f"   {labels[i]}: Eff={efficiency[i]:.1f}%, ActDays={act_days[i]:.1f}, Span={span[i]:.1f}")
            
            print(f"\n   Last 5 days:")
            for i in range(max(0, len(labels)-5), len(labels)):
                print(f"   {labels[i]}: Eff={efficiency[i]:.1f}%, ActDays={act_days[i]:.1f}, Span={span[i]:.1f}")
            
            # Check if values are all the same (flat line)
            eff_range = max(efficiency) - min(efficiency)
            act_range = max(act_days) - min(act_days)
            span_range = max(span) - min(span)
            
            print(f"\n   📈 Value Ranges:")
            print(f"   Efficiency: {min(efficiency):.1f}% to {max(efficiency):.1f}% (range: {eff_range:.1f}%)")
            print(f"   Act Days: {min(act_days):.1f} to {max(act_days):.1f} (range: {act_range:.1f})")
            print(f"   Span: {min(span):.1f} to {max(span):.1f} (range: {span_range:.1f})")
            
            if eff_range < 0.1 and act_range < 0.1 and span_range < 0.1:
                print("\n   ⚠️  FLAT LINE DETECTED - all values are the same!")
                print("   This means there's not enough variation in the data.")
            else:
                print("\n   ✅ Graph should show trends (values are changing)")
        else:
            print("\n   ❌ No labels/data in chart!")
    else:
        print("\n❌ 120_10 chart not found in cache!")
        print(f"   Available charts: {list(data.keys())}")

# Check how many complete units we have per day
print(f"\n" + "="*80)
print("COMPLETE UNITS PER DAY (Last 10 days):")
print("="*80)

cur.execute("""
    SELECT last_day_unfiltered, COUNT(*) as unit_count
    FROM UnitCompletion
    WHERE last_day_unfiltered >= date('now', '-10 days')
    GROUP BY last_day_unfiltered
    ORDER BY last_day_unfiltered DESC
""")

days = cur.fetchall()
if days:
    for d in days:
        print(f"   {d['last_day_unfiltered']}: {d['unit_count']} units completed")
else:
    print("   ⚠️  No units completed in last 10 days")
    print("   This could cause flat trends!")

conn.close()
