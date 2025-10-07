# Database Migration - October 2025

## Issue
The database had duplicate efficiency and completion columns for all departments:
- Lowercase with underscores: `fab_efficiency`, `fab_completion`, etc.
- Uppercase with spaces: `Fab Efficiency`, `Fab Completion`, etc.

The uppercase columns consistently had **more complete data** (1-25 more records per department).

## Solution
Removed all duplicate lowercase columns, keeping only the uppercase versions.

## Deployment Steps

### On the Server:

1. **Pull the latest code:**
   ```bash
   git pull origin trying-something-newe
   ```

2. **Run the migration script:**
   ```bash
   python migrate_remove_duplicate_columns.py
   ```
   
   Or if the database is in a different location:
   ```bash
   python migrate_remove_duplicate_columns.py /path/to/SCHLabor.db
   ```

3. **Verify the migration:**
   - The script will show before/after column counts
   - Should remove 20-22 duplicate columns
   - Check that uppercase columns like "Fab Efficiency" still exist

4. **Restart the application:**
   ```bash
   # Stop the Flask app
   # Restart the Flask app
   ```

5. **(Optional) Test a data update:**
   - Trigger a scheduling summary update
   - Verify InsulWallFab and other departments show completion values
   - Check the database to ensure no duplicate columns were recreated

## What Changed

### Database Schema
- **Before:** 95-97 columns in SCHSchedulingSummary table
- **After:** 75 columns in SCHSchedulingSummary table
- **Removed:** 20 duplicate lowercase columns

### Code Changes
- `webapp/blueprints/utils.py`: Removed completion fallback calculation
- `clean.py`: Enhanced InsulWallFab column parsing with 3 variants
- No changes needed to `report_update_service.py` (already uses uppercase columns)

### Data Quality
- Uppercase columns have 1-6% more data coverage than lowercase versions
- No data loss - all values from lowercase columns already existed in uppercase

## Rollback (if needed)

If the migration causes issues:

1. **Restore from backup:**
   ```bash
   cp SCHLabor.db.backup SCHLabor.db
   ```

2. **Or revert the git changes:**
   ```bash
   git checkout main
   git pull
   ```

The old code will work with the old database schema.

## Notes

- Migration script requires SQLite 3.35.0 or higher
- Script is idempotent - safe to run multiple times
- Broken views (e.g., vSCHLaborWithDept) are automatically dropped
- Future data updates will NOT recreate the duplicate columns

## Verification Queries

Check the cleanup worked:
```sql
-- Should return 0 rows
SELECT name FROM pragma_table_info('SCHSchedulingSummary') 
WHERE name LIKE '%_efficiency' OR name LIKE '%_completion';

-- Should return all department columns
SELECT name FROM pragma_table_info('SCHSchedulingSummary') 
WHERE name LIKE '% Efficiency' OR name LIKE '% Completion';

-- Check data integrity
SELECT COUNT(*) as total,
       COUNT(CASE WHEN "InsulWallFab Completion" > 0 THEN 1 END) as with_completion
FROM SCHSchedulingSummary
WHERE insulwallfabstdhrs > 0;
```
