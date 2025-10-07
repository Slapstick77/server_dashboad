# Changelog

All notable changes to the SCH Labor Dashboard project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
