# DR Poller Continuity Reference

This file is an extended context / knowledge handoff so a new workspace (or a future AI assistant) retains the architectural and operational knowledge accumulated during development of `poll_drs_incremental.py`.

---
## High-Level Purpose
Pull recent Manufacturing Deviation Reports (DRs) directly from the MOM WCF service (read-only) to:
- List current DRs within a rolling creation-date window (default 7 days)
- Capture latest routing step & latest non-empty routing comment
- Detect and flag routing/comment changes across runs
- Output full and delta (only-updated) JSON/CSV datasets for downstream consumption

## Endpoint & Operations
Base service: `ServiceManufacturingDeviationSystem.svc`
Primary calls:
1. `RegisterSession` (best effort; may no-op)
2. `GetApplicationPK`
3. `MOM_Login`
4. `GetUser` (obtains devUser context)
5. `DisplayItemForDateRange` (core list)
6. `RoutingsForMaster` (routing history per DR)
7. `NotesForMaster` (optional count)

All mutating verbs excluded (Save/Delete/Approve/Void/Import/Lock/Unlock/Upload etc.).

## Key Behavioral Discoveries
- `DisplayItemForDateRange closed=True` returns BOTH open and closed DRs (superset); open variants returned zero in practice here.
- The date range filter uses DR creation date, NOT last routing update; thus a small ("today") window misses updates to previously-created DRs.
- Negative timezone offset previously hid newest DRs because both start and end times were shifted earlier; use UTC (offset 0) or implement a corrected local-midnight-only adjustment if needed.
- Parameter ordering matters (we retained two variants originally; now using only the first closed variant with `--single-closed-variant`).

## Script Capabilities (`poll_drs_incremental.py`)
- Rolling window: `--window-days N` (creation-date based)
- Closed variant superset: `--include-closed` plus optimization `--single-closed-variant`
- State tracking in `dr_state.json` to compute change flags:
  - `lastRoutingTouched`, `lastRoutingStepCount`, `lastNonEmptyRoutingComment`, etc.
- Change flags: `UpdatedRouting`, `UpdatedComment`, combined `Updated`
- Outputs: full JSON/CSV, only-updated JSON/CSV
- Optional routing history embedding: `--include-history`
- Optional notes counting: `--include-notes`
- Retry with exponential backoff: `--retries`, `--retry-wait-seconds`
- Timing diagnostics: `--timings`, `--timings-json`
- Jitter to desynchronize schedules: `--jitter-seconds`
- Expectation alerting: `--expect-numbers <list>` warns if specific DR IDs are absent

## Rationale for 7-Day Window
Because filtering is by creation date, a multi-day span is required to continue seeing older DRs that accumulate new routing steps. A "today-only" window test returned only 13 of 86 DRs (73 missed) even though older DRs still changed. Seven days comfortably covers the active lifecycle; could tune after empirical analysis.

## Performance Characteristics
Typical: ~12–18s runtime for ~70–80 DRs
Breakdown:
- DisplayItemForDateRange closed variant: 2–8s (network variability)
- Each `RoutingsForMaster`: ~0.06–0.10s (occasional 0.2–0.5s outlier)
Optimization levers (not yet implemented): skip routing fetch for long-stable DRs, periodic full sweep, or parallelization (latter increases server load risk).

## Known Gotchas
- Using `--tz-offset-hours -5` previously truncated the end of window; remove unless corrected logic is added.
- Creation-day filter means you *cannot* detect routing updates for DRs outside your window.
- If the service ever introduces Daylight Saving or local time shifts, naive offset arithmetic could misalign midnight boundaries.
- Latest routing comment may be `null` even if earlier steps have comments; script separately tracks latest non-empty.

## State File Schema (Excerpt)
```
{
  "last_run": "2025-10-14T19:31:47.707000+00:00",
  "drs": {
    "49090": {
      "lastRoutingTouched": "2025-10-14T19:31:47.707000",
      "lastRoutingStepCount": 2,
      "lastNoteCount": null,
      "lastSeenState": "NEW",
      "lastNonEmptyRoutingComment": "Complete and sent to Sheet Metal"
    },
    ...
  }
}
```

## Typical Scheduled Command (Hourly)
```
powershell -NoProfile -ExecutionPolicy Bypass -Command "python 'C:/JobFolder/poll_drs_incremental.py' --user USER --password PASS --include-closed --single-closed-variant --window-days 7 --state-file 'C:/JobFolder/dr_state.json' --out-json 'C:/JobFolder/dr_incremental.json' --out-csv 'C:/JobFolder/dr_incremental.csv' --only-updated-json 'C:/JobFolder/dr_incremental_updates.json' --only-updated-csv 'C:/JobFolder/dr_incremental_updates.csv' --expect-numbers 49090 49089 49088 --jitter-seconds 60"
```
(Use offset 0; remove credentials from plaintext for production.)

## Quick Interpretation Guide
Field | Meaning
----- | -------
Updated | Any routing/comment change since last run
UpdatedRouting | New routing step or DateTouched changed
UpdatedComment | Latest non-empty comment changed
LatestRoutingTouched | Timestamp of most recent routing step
LatestRoutingComment | Comment on the latest step (may be empty)
LatestNonEmptyRoutingComment | Most recent nonblank routing comment (could be older step)
RoutingStepCount | Total steps fetched (history length)

## Validation Heuristics
- Monotonic (or increasing) max DeviationNumber across runs indicates new DR creation ingestion.
- Expectation list warnings mean either ingestion lag or window boundary issue.
- Compare counts (e.g., 7-day vs today-only) to confirm filter semantics.

## Future Enhancements (Backlog)
1. Local-midnight mode that shifts only fromDate.
2. Periodic extended backfill (weekly or daily) automatically.
3. Skip-stable routing fetch heuristic with fallback full sweep every N runs.
4. Transition log file capturing each state change event.
5. Auto-expect: seed highest N DR IDs from prior run.
6. Sorting or filtering for output sets (by department, product).
7. Structured logging (JSON lines) + log rotation.

## Why Not Today-Only
Creation-date filtering means routing updates to older DRs vanish from the sample if you restrict to today; you lose change detection for multi-day DR lifecycles.

## Minimal Setup Steps
```
python -m venv .venv
.\.venv\Scripts\activate
pip install zeep
python poll_drs_incremental.py --user USER --password PASS --include-closed --single-closed-variant --window-days 7 --state-file dr_state.json --out-json dr_incremental.json --out-csv dr_incremental.csv
```

## Security Considerations
- Keep credentials out of version control.
- Consider environment variables or Windows Credential Manager for passwords.
- Log files avoid printing passwords; only operation names and counts.

## Support / Handoff Notes
If new DRs begin missing again, first check:
1. tz offset (should be 0) 
2. window-days value
3. Closed variant still returning expected counts
4. Network latency spikes (inspect timings JSON)

End of continuity reference.
