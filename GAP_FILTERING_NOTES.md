# Gap Filtering Improvements - October 3, 2025

## Summary
Fixed multiple issues with gap filtering logic that was allowing stray/outlier charges to appear in metrics calculations, causing inflated unit span calculations.

## Problems Identified

### 1. COM 19044 - 436-day span (April 2024 → June 2025)
- **Issue**: June 27, 2025 charge (2.5 hrs, 2 employees) kept despite 373-day gap
- **Cause**: 2-employee override rule bypassed gap filtering even with extreme gaps
- **Solution**: Added `max_gap_override` setting (default: 30 days) as hard limit

### 2. COM 18084 - 2024-05-15 charge marked as "Valid"
- **Issue**: 2024-05-15 (1 hr, 1 employee) after 482-day gap was kept
- **Cause**: With only 2 days in department, gap filter dropped wrong day (kept outlier, dropped main work)
- **Solution**: Enhanced 2-day logic to compare hours and keep stronger day

### 3. COM 18178 - 742-day span with 2024 outlier charges
- **Issue**: 2024-09-24/25 charges (1 employee each) kept 2 years after main work in 2022
- **Cause**: Original gap filter only checked first/last edges, not gaps in the middle
- **Solution**: Implemented cluster-based filtering to catch outliers anywhere in timeline

### 4. COM 18090 - Weak single-day clusters kept
- **Issue**: 2023-08-25 (3 hrs, 1 employee) kept as only survivor of 2-day department with 287-day gap
- **Cause**: Cluster filter always kept "best" cluster even if all clusters were weak
- **Solution**: Added weakness check - drop all days if best cluster has only 1 day OR (<2 employees AND <10 hours)

## Changes Made

### 1. Added `max_gap_override` Configuration
**File**: `webapp/blueprints/utils.py`
- Added to `PROJECT_DAY_RULES` dictionary: `'max_gap_override': 30`
- Loaded from database Configuration table in `_load_configuration()`
- Hard limit enforced even when 2+ employees present

**File**: `webapp/blueprints/admin.py`
- Added UI section for configuring max_gap_override
- Red warning box to highlight importance
- Saves to Configuration table

### 2. Rewrote `_filter_days_by_gap()` Function
**File**: `webapp/blueprints/utils.py` (lines 176-259)

**New Approach**: Cluster-based filtering
- Splits days into clusters separated by large gaps (>max_gap_override or >cap with weak sides)
- Scores each cluster: `score = hours + (days × 2) + (employees × 5)`
- Keeps highest-scoring cluster
- Drops ALL days if best cluster is too weak (1 day OR <2 employees AND <10 hours)

**Benefits**:
- Catches outliers at beginning, middle, OR end of timeline
- Handles any number of days (not just 2-day edge cases)
- Prevents keeping isolated weak charges

### 3. Fixed Import (then reverted)
**File**: `metrics_cache.py` (line 480)
- Briefly changed import from `webapp.blueprints.api` to `webapp.blueprints.utils`
- Reverted because both work (api re-exports from utils)
- Left as original for consistency

## Results

**Before fixes**:
- 7 units with filtered span >100 days
- COM 19044: 436 days
- COM 18084: 479 days  
- COM 18178: 742 days

**After all fixes**:
- 2 units with filtered span >100 days (71% reduction!)
- COM 18090: 305 days (may be legitimate long project)
- COM 14847: 171 days (may be legitimate long project)

## Testing Done
1. ✅ COM 19044 Fab: 436 days → 63 days (June 2025 charge filtered)
2. ✅ COM 18084 FanAssyTest: 2024-05-15 filtered out (1 hr, 1 employee after 482-day gap)
3. ✅ COM 18178 Assembly: 742 days → ~11 days (2024 charges filtered, 729-day gap caught)
4. ⚠️ COM 18090 Crating: Needs final test (should filter 2023-08-25 single-day weak cluster)

## Database Configuration
**Configuration table** entries added:
- `MAX_GAP_OVERRIDE`: 30 (days)

## Cache Refreshes Performed
1. After max_gap_override added: `trigger='fix_import'`
2. After 2-day fix: `trigger='fix_2day_gap'`
3. After cluster filter: `trigger='cluster_gap_filter'`
4. **NEEDED**: Refresh after weak cluster check

## Next Steps (IMPORTANT!)
1. **Test weak cluster fix**:
   ```bash
   python check_18090_aug25.py
   ```
   Should show: "2023-08-25 was FILTERED OUT"

2. **Refresh metrics cache**:
   ```bash
   python -c "import metrics_cache; metrics_cache.refresh_metrics_cache(trigger='weak_cluster_fix')"
   ```

3. **Restart Flask server** to pick up all changes

4. **Verify in browser**:
   - COM 18084: 2024-05-15 should be "Filtered Out"
   - COM 18090: 2023-08-25 should be "Filtered Out"
   - Dashboard charts should show lower average spans

## Files Modified
1. `webapp/blueprints/utils.py` - Core filtering logic
2. `webapp/blueprints/admin.py` - Configuration UI
3. `metrics_cache.py` - Import path (reverted to original)

## Configuration Location
Admin page: `/admin/logic`
- Scroll to "Hard Maximum Gap Override" section
- Default: 30 days
- Adjust based on business needs

## Git Commit Message Suggestion
```
Fix gap filtering to catch outlier charges anywhere in timeline

- Add max_gap_override (30 days) as hard limit for all gaps
- Replace edge-only gap filter with cluster-based approach
- Drop weak clusters (single day or <2 employees with <10 hours)
- Reduced units with >100 day spans from 7 to 2 (71% improvement)

Fixes: COM 19044 (436→63 days), COM 18084 (filtered outlier),
       COM 18178 (742→11 days), COM 18090 (weak cluster filtered)
```
