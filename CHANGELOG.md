# Changelog

All notable changes to the SCH Labor Dashboard project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.4.2] - 2025-11-05

### Fixed
- **"Latest Activity" Timer**: Fixed to use actual routing step timestamps instead of incorrect snapshot data
  - Changed from using `latest_routing_touched` (which was identical to creation time) to `routing_latest_comment_date`
  - Now correctly shows time since last routing activity/comment, not just creation time
  - Applied UTC-to-local timezone conversion (subtract 6 hours) to fix negative time values
  - "Age" and "Latest Activity" now show different, accurate values

### Changed
- **Milestone Dashboard Layout**: Optimized for better visibility on 60" TV
  - Table width increased from 58% to 70%
  - Card panel reduced from 40% to 28%
  - Reduced cards from 4 to 3 for larger display
  - Reduced table rows from 20 to 15
  - Increased font sizes across board:
    - Table DR numbers: 1.35rem → 1.45rem
    - Table headers: 1.0rem → 1.15rem
    - Table body text: 1.25rem → 1.35rem
    - Card DR numbers: 2.2rem (kept larger)
    - Card info text: 1.3rem (kept larger)
  - Total visible DRs: 18 (3 cards + 15 table rows)

- **Table Columns**: Reorganized for better information display
  - Removed "Status" column (was mostly empty dashes)
  - Added "Comment" column showing creator's initial DR comment
  - Comment truncated to 60 characters with ellipsis if longer
  - Milestone badges (sent to sheet shop, parts made) now shown above comments
  - Column order: DR# | Routing | Comment | COM# | Age | Latest Activity

## [1.4.1] - 2025-11-05

### Fixed
- **DR Age and "In Route" Timer Calculation**: Fixed incorrect timestamp handling for routing activity
  - Database stores `latest_routing_touched` timestamps inconsistently (mix of UTC and local time)
  - API now treats all routing timestamps as UTC and converts to local time
  - Resolves negative "In Route" times that appeared 6 hours in the future
  - Fixes DR age calculations that were showing 6+ hours older than actual
  - Affects DRs with UTC-stored timestamps (e.g., DR #49323, #49322, #49321, #49319)
  - Minimal impact on DRs already stored in local time (e.g., DR #49318, #49320)

## [1.4.0] - 2025-10-30

### Added
- **Simple URL Access**: New easy-to-type URLs for dashboard
  - `/tv` - Direct access to 4K milestone dashboard (auto-enables milestone mode)
  - `/dr` - Short alias for DR dashboard
  - `/dr-dashboard` - Original URL (still works)
- **Parts Tracking Integration**: Real-time parts manufacturing data from PartsTracker
  - Shows unique parts count and cart locations
  - Displays parts scan activity and timestamps
  - Tracks active DRs with parts in production (8 of 33 DRs currently)
  - Handles multiple DR number formats (DR#49225, DR 49225, DR49225)
- **Split-Screen Milestone Dashboard**: Optimized layout for 60" 4K TV at 20ft viewing distance
  - Left panel (58%): Table view with 20 DRs
  - Right panel (40%): 4 large cards showing most recent activity
  - Total 24 DRs visible simultaneously
  - Access via `/tv` or `/dr-dashboard?milestone=1`
- **Fullscreen Mode**: One-click fullscreen button for immersive TV display
- **Status Badges**: Visual indicators for milestone progress
  - Green "Sent to sheet shop" badge
  - Blue "X parts made" badge with count
- **Refresh Timestamps**: Three-part timestamp display in header
  - Page refresh time
  - DR MOM data pull time
  - Parts tracker data pull time
  - All update every 30 seconds

### Changed
- **Terminology Updates**:
  - "Milestones" → "Status"
  - "Last Touch" → "Latest Activity"
  - "DR Milestone Dashboard" → "DR Dashboard"
- **Color Scheme**: Simplified from urgency-based to status-based
  - Blue for active DRs
  - Green for completed DRs
  - Removed urgency color coding for cleaner display
- **Font Sizes**: Increased all text by 40-150% for 20ft viewing distance
  - Header: 2.8rem title, 1.2rem controls
  - Cards: 1.8rem DR numbers, 1.1rem info, 1rem comments, 1.3rem timers
  - Table: 1.2rem DR numbers, 1.1rem text, 1rem headers, 0.9rem badges
  - Icons: 32px milestone icons, 8px status dots
- **Table Capacity**: Expanded from 14 to 20 DRs in table view
- **Badge Order**: "Sent to sheet shop" now appears before "parts made"

### Improved
- **4K Rendering Quality**: Optimized for high-resolution displays
  - Added font antialiasing (`-webkit-font-smoothing: antialiased`)
  - Firefox font smoothing (`-moz-osx-font-smoothing: grayscale`)
  - Enabled subpixel rendering for high-DPI displays
  - Applied `image-rendering: crisp-edges` for sharp graphics
  - Optimized text rendering with `optimizeLegibility`
  - GPU acceleration with `transform: translateZ(0)` and `will-change: transform`
  - Enhanced font-family stack for better cross-platform rendering
  - Letter spacing adjustments (`-0.02em`) for improved readability
- **Performance**:
  - Hardware acceleration for smoother animations
  - Optimized backdrop filters with blur
  - Will-change hints for transform properties
  - Efficient DOM updates with real-time timers

### Fixed
- **Closed DRs Filter**: Excluded closed DRs from dashboard
  - SQL filter: `state NOT IN ('complete', 'closed')`
- **Parts Tracking Query**: Robust DR number extraction
  - Handles multiple formats with regex pattern matching
  - Uses separate cursor for nested queries to avoid conflicts
- **API Response Structure**: Enhanced `/api/dr-live` endpoint
  - Added `parts_info` object with `unique_parts`, `total_scans`, `racks`
  - Added `last_parts_pull_ms` timestamp from PartsTracker ingestion
  - Proper JSON structure for frontend consumption

### Technical Details
- **Database**: Integrated PartsTracker table (28,045 records, 470 distinct COMs)
- **API Endpoint**: Enhanced `/api/dr-live` with parts tracking data
- **Dashboard Routes**: Multiple route decorators for easy access
- **Rendering Engine**: CSS optimizations for 4K displays with GPU acceleration
- **Auto-refresh**: 30-second intervals for all timers and timestamps

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
