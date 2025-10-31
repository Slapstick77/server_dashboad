# Server Graph Troubleshooting Guide

**Issue**: Trend graphs appear flat on server computer even after clicking "Update Logic"

**Location**: Server computer at `http://10.43.22.135:5000/dash`

---

## Step 1: Pull Latest Diagnostic Scripts

On the **server computer**, navigate to the project folder and pull the latest code:

```powershell
cd "C:\Project p\SQRS"
git pull origin trying-something-newe
```

This will download the diagnostic scripts created on the dev computer.

---

## Step 2: Run Diagnostics

Run these scripts **in order** and save the output:

### A. Check Data Freshness
```powershell
python diagnose_labor_data.py
```

**Look for:**
- ✅ Sample LoggedDate values showing recent dates (Oct 2025)
- ✅ "Last 90 days: XXX,XXX rows" - should be hundreds of thousands
- ❌ If you see old dates (Sept 2024 or earlier), data sync failed

---

### B. Check Metrics Cache Status
```powershell
python check_metrics_refresh.py
```

**Look for:**
- ✅ Recent refresh timestamps (within last few hours)
- ✅ Trigger sources: `logic_update`, `labor_sync`, or `both_sync`
- ❌ If last refresh is days old, cache isn't updating

---

### C. Check Trend Data Content
```powershell
python check_trend_data.py
```

**Look for:**
- ✅ "Value Ranges" showing variation (e.g., "Efficiency: 55.7% to 71.9% (range: 16.1%)")
- ❌ "FLAT LINE DETECTED" - all values are identical
- ❌ "range: 0.0%" - no variation means graphs will be flat

---

### D. Check Complete Units
```powershell
python -c "from metrics_cache import get_complete_units; units = get_complete_units(); print(f'{len(units)} complete units found')"
```

**Look for:**
- ✅ 1,500+ complete units (healthy dataset)
- ⚠️ <500 complete units (limited trend data)
- ❌ 0 complete units (metrics won't calculate)

---

## Step 3: Compare Dev vs Server

### Dev Computer Results (for comparison):

**Data Freshness:**
- Latest LoggedDate: `2025-10-28`
- Last 90 days: `466,686 rows`
- Total rows: `734,239`

**Metrics Cache:**
- Last refreshed: `2025-10-30T19:44:36` (recent)
- Trigger: `manual_refresh`

**Trend Data (120-day / Trailing-10):**
- Efficiency range: `55.7% to 71.9%` **(range: 16.1%)**
- Act Days range: `17.8 to 24.8` **(range: 7.0)**
- Span range: `27.9 to 44.7` **(range: 16.8)**
- ✅ Graph should show clear trends

**Complete Units:**
- `2,009 complete units`

---

## Step 4: Fix Based on Results

### If server shows old data (no recent dates):
```powershell
python new_data_sync_app.py both
```
This pulls fresh labor/scheduling data and auto-refreshes metrics.

---

### If server has recent data but flat trends (range: 0.0%):
```powershell
python refresh_metrics_now.py
```
This recalculates all trend charts from scratch.

---

### If server has good data AND good ranges but graphs still look flat:
**Browser cache issue** - Hard refresh the dashboard:
- Chrome/Edge: `Ctrl+Shift+R`
- Or clear browser cache and reload

---

## Step 5: Verify Fix

After running fixes, check the dashboard:
1. Open `http://10.43.22.135:5000/dash`
2. Look at the "📈 Metrics Trend" chart
3. Click different time ranges (90 Days, 120 Days, 1 Year)
4. Trend lines should show variation, not flat horizontal lines

---

## Common Issues & Solutions

| Symptom | Cause | Solution |
|---------|-------|----------|
| Old dates in labor data | Data sync not running | Run `python new_data_sync_app.py both` |
| Flat trend ranges (0.0%) | Not enough unit completions | Check if SCHSchedulingSummary has completion % data |
| Recent refresh but old data | Clicked "Update Logic" before syncing data | Sync data FIRST, then refresh metrics |
| Good data but graphs still flat | Browser cached old chart data | Hard refresh browser (Ctrl+Shift+R) |

---

## Quick One-Liner Diagnostic

Run this to get a quick overview:
```powershell
python -c "import sqlite3, json; conn = sqlite3.connect('SCHLabor.db'); cur = conn.cursor(); cur.execute('SELECT MAX(LoggedDate) FROM SCHLabor'); print(f'Latest data: {cur.fetchone()[0]}'); cur.execute('SELECT COUNT(*) FROM UnitCompletion'); print(f'Complete units: {cur.fetchone()[0]}'); cur.execute('SELECT metric_data FROM MetricsCache WHERE metric_type=\"trailing_trend_charts\"'); data = json.loads(cur.fetchone()[0]); chart = data['120_10']; eff = chart['avg_efficiency']; print(f'Efficiency range: {min(eff):.1f}% to {max(eff):.1f}%')"
```

**Expected output:**
```
Latest data: 2025-10-28
Complete units: 2009
Efficiency range: 55.7% to 71.9%
```

If any of these numbers are very different from the dev computer, you've found the problem!

---

## Need Help?

Copy all diagnostic outputs and compare with dev computer results above.
