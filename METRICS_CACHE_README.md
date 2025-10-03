# Metrics Cache System

## Overview
The dashboard metrics are now **cached** instead of being calculated on every page load. This makes the dashboard load instantly and reduces database load.

## How It Works

### 1. Cache Storage
- **MetricsCache** table stores pre-calculated metrics
- **MetricsRefreshLog** table tracks refresh history (when, why, how long)

### 2. Automatic Updates
Metrics are automatically refreshed **after every data sync**:
- ✅ After Labor sync (`python new_data_sync_app.py labor`)
- ✅ After Scheduling Summary sync (`python new_data_sync_app.py sched`)
- ✅ After Combined sync (`python new_data_sync_app.py both`)
- ❌ Parts tracker sync does NOT trigger refresh (doesn't affect metrics)

### 3. Cache Freshness
- Dashboard shows **when cache was last updated** ("Updated 2 hours ago via labor_sync")
- **Stale warning** appears if cache is >24 hours old
- Falls back to live calculation if cache is missing

## Setup (One-Time)

Run this once to initialize the cache:
```powershell
python init_metrics_cache.py
```

## Manual Refresh

You can manually refresh metrics anytime:

1. **Via Web UI**: Go to `/tasks` and click "🔄 Refresh Metrics Cache"
2. **Via Command Line**:
```python
from metrics_cache import refresh_metrics_cache
refresh_metrics_cache(trigger='manual')
```

## Files Changed

### New Files
- `metrics_cache.py` - Cache system core logic
- `init_metrics_cache.py` - One-time initialization script

### Modified Files
- `new_data_sync_app.py` - Auto-refresh after syncs
- `webapp/blueprints/api.py` - Use cache instead of live calculation
- `webapp/blueprints/admin.py` - Added manual refresh endpoint
- `webapp/blueprints/dashboard.py` - Show cache timestamp

## Performance Impact

### Before (Live Calculation)
- Dashboard load: **~3-5 seconds** (heavy SQL queries every time)
- Database load: High on every page view

### After (Cached)
- Dashboard load: **~100ms** (just reads from cache table)
- Database load: Only during scheduled syncs
- Metrics refresh: Happens in background during syncs

## Cache Lifecycle

```
User visits /dash
    ↓
API checks MetricsCache table
    ↓
Cache exists & fresh? → Return cached data instantly ✓
    ↓
Cache missing/stale? → Calculate live (fallback) → Store in cache
    ↓
Dashboard shows data with timestamp
```

```
Scheduled task runs (e.g., 2 AM daily)
    ↓
new_data_sync_app.py labor/sched/both
    ↓
Data synced to database
    ↓
Auto-detect: rows inserted/updated?
    ↓
YES → refresh_metrics_cache() runs automatically
    ↓
New metrics cached, ready for next dashboard view
```

## Troubleshooting

**Cache not updating?**
- Check MetricsRefreshLog table for errors
- Run `python init_metrics_cache.py` to force refresh

**Dashboard shows stale warning?**
- Click "Refresh Metrics Cache" button on /tasks page
- Or run a manual sync: `python new_data_sync_app.py both`

**Errors during refresh?**
- Check that webapp/blueprints/utils.py exists
- Verify database has both SCHLabor and SCHSchedulingSummary tables
- Check MetricsRefreshLog for error messages

## Future Enhancements

Could add caching for:
- Daily hours chart data
- Recent units completion stats
- Employee/department metrics
- Any other expensive queries
