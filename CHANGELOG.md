# Changelog

All notable changes to the SCH Labor Dashboard project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.1] - 2025-10-30

### Added
- **DR Dashboard TV Display Mode**: Optimized viewing for 4K TVs
  - Access via URL parameter: `/dr-dashboard?tv=1`
  - Larger fonts (4rem title, 2.5rem metrics) for distance viewing
  - Increased card sizes (450px min-height) and spacing (2rem gaps)
  - 4-column grid layout (vs 6-column default) for better readability
  - Enhanced button sizes and timer displays
  - Optimized for factory floor/conference room displays

### Fixed
- **API DateTime Import Errors**: Resolved AttributeError in multiple endpoints
  - Fixed `/api/metrics/unit_time_trends` endpoint
  - Fixed `/api/metrics/daily_hours` endpoint
  - Added `date` to datetime imports at module level
  - Replaced all `datetime.date` and `datetime.timedelta` references with direct imports
  - Prevents "datetime object has no attribute 'date'" errors

## [1.3.0] - 2025-10-16

### Added
- **Desktop Sync App Auto-Scheduler**: New auto-refresh feature for all sync operations
  - Configure independent intervals for Labor, SCHSummary, Parts, and DR polls (2d/7d/30d)
  - GUI mode with settings dialog for easy configuration
  - Headless mode (`--headless` flag) for background execution without GUI
  - Config persistence in `auto_sync_config.json`
  - Startup scripts: `start_auto_sync_gui.bat` and `start_auto_sync_headless.bat/ps1`
  - Complete documentation in `AUTO_REFRESH_README.md`, `STARTUP_OPTIONS.md`, `IMPLEMENTATION_SUMMARY.md`
- **DR Dashboard Enhancements**: Major improvements to deviation request tracking
  - 6-column grid layout for compact display (previously 4 columns)
  - Completion detection: Green cards for DRs with "Complete sent to sheet metal/shop" comments
  - Accurate completion times using comment timestamps (not routing activity timestamps)
  - Single "DR Completed In" timer for completed DRs vs three timers for in-progress
  - Average completion time metric with prominent disclaimer about "handling" vs "full completion"
  - "Last MOM Pull" timestamp showing when DR data was last synced
  - Auto-refresh every 30 seconds
- **DR Metadata Capture**: Added `deviation_type` and `component` fields to DRStaticMetadata table
  - Migration script: `migrate_add_metadata_columns.py`

### Changed
- **DR API Endpoint**: `/api/dr-live` now returns structured response with metadata
  - Response format: `{ "drs": [...], "last_poll_ms": 1234567890 }`
  - Added `latest_comment_ms` field for accurate completion time calculations
  - Filters out DRs with `state='complete'` to show only active/handling status
- **DR Dashboard Styling**: Updated card layout and completion indicators
  - Completed DR cards show green background
  - Yellow warning banner for average completion metric disclaimer
  - Improved header layout with poll timestamp

### Fixed
- **DR Completion Detection**: Strict regex patterns prevent false positives
  - Only matches exact phrases: "complete [and] sent to sheet metal/shop"
  - Prevents "Complete?" questions from triggering completion status
- **DR Completion Times**: Now uses comment timestamp instead of routing activity
  - Previously used `touched_ms` which reflected any routing activity
  - Now uses `latest_comment_ms` for accurate time-to-completion calculation

## [1.2.0] - 2025-10-07

### Fixed
- **Data Quality**: Removed duplicate efficiency/completion columns from database
  - Database had both lowercase (`fab_completion`) and uppercase (`Fab Completion`) versions
  - Uppercase columns had 1-6% more complete data across all departments
  - Migration script added to clean up server databases
- **InsulWallFab Parsing**: Enhanced column name matching to handle 3 variants
  - Now checks: "InsulWallFab", "InsuWallFab", "Insul_Wall_Fab"
  - Fixes issue where InsulWallFab completion was always showing 0
- **Completion Calculation**: Removed automatic fallback calculation
  - Previously calculated completion as min(100, actual/standard*100) when missing
  - Now only shows completion when explicitly set to 100% in source data
  - Prevents misleading completion percentages

### Changed
- Database schema reduced from 95-97 columns to 75 columns
- Only uppercase column names with spaces are now used (e.g., "Fab Efficiency")

### Added
- Migration script `migrate_remove_duplicate_columns.py` for server deployment
- Deployment documentation in `MIGRATION_NOTES.md` and `DEPLOYMENT_CHECKLIST.md`
- Version number display in dashboard footer
- Changelog with link from dashboard

## [1.1.0] - 2025-10-06

### Added
- **Database Update Log Viewer**: Added RunLog viewer to tasks page
  - Shows latest 20 database update entries
  - Color-coded by success/failure status
  - Clear log functionality at `/tasks/clear-log`
- **Fullscreen Mode Enhancement**: Improved display page fullscreen experience
  - Charts now blend seamlessly into background
  - Card borders and headers hidden in fullscreen
  - Transparent backgrounds for cleaner presentation

### Changed
- **UI Color Updates**: Changed alpha testing banner from red to amber
  - Updated gradient colors across all dashboard pages
  - More visible and less alarming appearance
- **Display Page**: Removed alpha testing banner from display route
  - Cleaner presentation for production use

### Fixed
- **Chart Z-Index Issues**: Fixed charts disappearing under toolbar
  - Changed body layout to flex with fixed height (100vh)
  - Toolbar set to flex-shrink:0 to prevent overlap
  - Charts now properly visible at all times

## [1.0.0] - 2025-09-XX

### Initial Release
- Production dashboard with department tracking
- Display page for real-time metrics
- Charts page with data visualization
- Task management interface
- Database integration with SCHLabor.db
- Scheduled data updates from SSRS reports
- Metrics caching system
- Multi-department completion tracking

---

## Version History

- **1.2.0** - Data quality fixes and completion logic updates
- **1.1.0** - UI enhancements and logging features
- **1.0.0** - Initial production release
