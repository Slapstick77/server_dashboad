# Server Flatline Troubleshooting - Updated Instructions

## Problem Summary
- **Dev computer**: Trend graphs show variation (Efficiency 55.7% to 71.9%, range 16.1%)
- **Server computer**: Trend graphs are flatline even after refreshing metrics
- Both computers refreshed metrics via "Update Logic" button

## Dev Computer Status (for comparison)
```
Last refresh: 2025-10-30T20:56:01
Trigger: logic_update
Complete units: 2,009
Labor data: 1/10/2022 to 9/9/2024
Efficiency range: 55.7% to 71.9% (16.1% variation)
```

---

## For Claude on Server Computer

### Step 1: Run Full Diagnostic
```powershell
cd "C:\Project p\SQRS"
git pull origin trying-something-newe
python export_metrics_snapshot.py
```

### Step 2: Check These Key Values

**In the exported JSON files, look for:**

1. **`metrics_export_trailing_trend_charts_*.json`**:
   - Open the file and find the `120_10` chart
   - Check `avg_efficiency` array - are all values the same?
   - Example of GOOD data: `[55.7, 58.2, 61.3, 64.8, ...]` (variation)
   - Example of BAD data: `[60.0, 60.0, 60.0, 60.0, ...]` (flatline)

2. **`metrics_export_database_info_*.json`**:
   - Check `latest_date` - should be recent (Oct 2025)
   - Check `complete_units` - should be 1500+ units

### Step 3: Possible Root Causes

#### Cause 1: Not Enough Recent Unit Completions
**Symptom**: Last few days of trend all show identical values

**Diagnosis**:
```powershell
python -c "import sqlite3; conn = sqlite3.connect('SCHLabor.db'); cur = conn.cursor(); cur.execute('SELECT last_day_unfiltered, COUNT(*) FROM UnitCompletion WHERE last_day_unfiltered >= date(\"now\", \"-30 days\") GROUP BY last_day_unfiltered ORDER BY last_day_unfiltered DESC'); print('\n'.join([f\"{r[0]}: {r[1]} units\" for r in cur.fetchall()]))"
```

**Expected**: Should show units completing on multiple recent dates
**If empty**: No units completing recently = trends go flat

**Fix**: This is normal - if no units are completing, trends will plateau. Not a bug.

---

#### Cause 2: Browser Rendering Issue
**Symptom**: Server's exported JSON shows variation, but graph looks flat in browser

**Diagnosis**: Compare the JSON data vs what you see visually

**Fix**:
1. Hard refresh browser: `Ctrl+Shift+R`
2. Clear browser cache
3. Try different browser
4. Check browser console for JavaScript errors (F12)

---

#### Cause 3: Old Labor Data on Server
**Symptom**: `latest_date` in database_info shows old date (before Oct 2025)

**Diagnosis**: Check the `metrics_export_database_info_*.json` file

**Fix**:
```powershell
python desktop_sync_app.py
```
Then click the sync buttons to pull fresh labor data, then refresh metrics again.

---

#### Cause 4: Y-Axis Scaling Issue
**Symptom**: Data has variation but Y-axis is zoomed way out making it look flat

**Diagnosis**: Check the actual values in the JSON:
- If efficiency ranges from 60.0% to 65.0% (5% range), graph should show this
- If graph Y-axis goes from 0% to 100%, a 5% variation will look almost flat

**Fix**: This is a chart rendering issue, not a data issue. The metrics are working correctly.

---

#### Cause 5: Wrong Chart Being Viewed
**Symptom**: Some charts are flat, others show variation

**Diagnosis**: 
- Try clicking different time period buttons (90 Days, 120 Days, 1 Year)
- Try clicking different trailing count buttons (Trailing 10, Trailing 30)
- Some combinations might have less variation than others

**Fix**: Not a bug - different time windows show different patterns

---

### Step 4: Direct API Test

Test the API directly to see what data it's returning:

```powershell
# Test the trending endpoint
Invoke-WebRequest -Uri "http://localhost:5000/api/metrics/trailing_trend?days=120&trailing=10" | Select-Object -ExpandProperty Content | ConvertFrom-Json | Select-Object -ExpandProperty avg_efficiency
```

**Expected**: Should see array of numbers with variation
**If all same**: Metrics cache has flat data
**If varied but graph flat**: Browser/rendering issue

---

### Step 5: Compare with Dev

**Send me (Claude on server) these files:**
- `metrics_export_trailing_trend_charts_*.json`
- `metrics_export_database_info_*.json`

I'll compare with dev computer's export to find the exact difference.

---

## Common Misconceptions

❌ **"Clicking Update Logic should make graphs change"**
- ✅ Update Logic refreshes the CALCULATION, not the data
- If underlying data hasn't changed, trends won't change

❌ **"Server should match dev computer exactly"**
- ✅ They have different databases, could have different data dates
- Compare the `latest_date` in database_info files

❌ **"Flatline means broken"**
- ✅ Could mean stable performance (no variation in metrics)
- Check if actual values make sense for your operation

---

## Quick Decision Tree

```
Is efficiency range > 5%?
├─ YES → Browser rendering issue (hard refresh)
└─ NO → Check recent completions
    ├─ Units completing daily → Normal stable performance
    └─ No recent completions → Trends plateau (expected)
```

---

## Files to Share for Diagnosis

Upload these to the chat:
1. `metrics_export_trailing_trend_charts_*.json` (most important)
2. `metrics_export_database_info_*.json`
3. Screenshot of the flat graph

I'll analyze and tell you exactly what's wrong.
