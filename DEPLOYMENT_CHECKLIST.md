# Server Deployment Checklist

## Before Deployment
- [x] Local changes committed
- [x] Migration script created
- [ ] Code pushed to GitHub

## Deployment Steps

### 1. Backup the database (IMPORTANT!)
```bash
cd /path/to/server/app
cp SCHLabor.db SCHLabor.db.backup.$(date +%Y%m%d_%H%M%S)
```

### 2. Pull latest code
```bash
git pull origin trying-something-newe
```

### 3. Stop the Flask application
```bash
# Use your process manager command, e.g.:
sudo systemctl stop flask-app
# OR
supervisorctl stop flask-app
# OR kill the process manually
```

### 4. Run the migration
```bash
python migrate_remove_duplicate_columns.py
```

**Expected output:**
- "Total columns before migration: 95" (or similar)
- "Found 20 duplicate lowercase columns to remove"
- "✓ Migration successful!"
- "Columns after: 75"

### 5. Restart the Flask application
```bash
# Use your process manager command, e.g.:
sudo systemctl start flask-app
# OR
supervisorctl start flask-app
```

### 6. Verify the application
- [ ] Check Flask app logs for errors
- [ ] Load the dashboard in browser
- [ ] Check InsulWallFab department shows completion values
- [ ] Verify no errors in browser console

### 7. Test a data update (optional)
```bash
# Trigger a scheduling summary update manually if possible
# Or wait for the next scheduled update
```

## Troubleshooting

### If migration fails:
1. Check SQLite version: `sqlite3 --version` (need 3.35.0+)
2. Check error message in migration output
3. Restore backup: `cp SCHLabor.db.backup.XXXXXXXX SCHLabor.db`
4. Contact support with error details

### If app won't start:
1. Check logs: `journalctl -u flask-app -n 50` or supervisor logs
2. Verify database file exists and is readable
3. Restore backup if needed

### If completion values still show 0:
1. Wait for next scheduled data update
2. Or manually trigger an update
3. Check database: `sqlite3 SCHLabor.db "SELECT * FROM SCHSchedulingSummary WHERE comnumber1='19044'"`

## Success Criteria
- ✅ Flask app starts without errors
- ✅ Dashboard loads and displays data
- ✅ Department completion values visible (not all 0)
- ✅ No duplicate columns in database
- ✅ Next data update completes successfully

## Rollback Plan
If issues persist:
```bash
# Stop app
sudo systemctl stop flask-app

# Restore backup
cp SCHLabor.db.backup.XXXXXXXX SCHLabor.db

# Revert code
git checkout main
git pull

# Restart app
sudo systemctl start flask-app
```
