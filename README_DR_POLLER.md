## DR Poller Unified README (Operational + Continuity)

This single document merges the concise project overview and the extended continuity context so it can be relocated to a new workspace without losing institutional knowledge.

---
### 1. Purpose
Automated, read‑only polling of the MOM WCF service to obtain recent Deviation Reports (DRs) with current routing status, latest routing step, and latest non‑empty routing comment, while flagging incremental changes.

### 2. Core Script
`poll_drs_incremental.py` – production poller (Python 3 + `zeep`).

### 2.1 Prerequisites / Requirements
Runtime:
- Python 3.10+ (tested with current environment; 3.11 recommended)
- `zeep>=4.3.1,<5.0` (see `requirements.txt`)

Setup quickstart:
```
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```
Optional (development / diagnostics):
- Powershell (for scheduled task and timing commands)
- Network access to MOM WCF endpoint
No database or message broker required; state persisted as JSON file.

### 3. Endpoint & Operations (Read‑Only)
Service: `ServiceManufacturingDeviationSystem.svc`
Used ops: `RegisterSession`, `GetApplicationPK`, `MOM_Login`, `GetUser`, `DisplayItemForDateRange`, `RoutingsForMaster`, `NotesForMaster` (optional). Mutating verbs (Save/Delete/Approve/Void/Import/Lock/Unlock/Upload) intentionally excluded.

### 4. Key Behavioral Findings
1. `DisplayItemForDateRange closed=True` returns BOTH open & closed DRs (superset). Open variants yielded zero here.
2. Date filter is by DR creation date, NOT last routing update – so a today-only window misses older DRs with new activity.
3. Negative TZ offset previously truncated newest DRs (both start AND end shifted). Use offset 0 unless a corrected local-midnight-only mode is implemented.
4. Parameter order matters; original dual variants kept for resilience; now `--single-closed-variant` uses only the first.
5. Per-DR routing fetch dominates runtime (0.06–0.10s each; occasional spikes to 0.25–0.5s).

### 5. Change Detection Logic
Persisted state (in `dr_state.json`) stores for each DR:
`lastRoutingTouched`, `lastRoutingStepCount`, `lastNonEmptyRoutingComment`, `lastNoteCount`, `lastSeenState`.
Flags:
- `UpdatedRouting`: DateTouched changed OR step count changed.
- `UpdatedComment`: latest non-empty comment text changed.
- `Updated`: logical OR of the above.

### 6. Outputs
Full: JSON + CSV (summary). Optional: routing history embedded in JSON (`--include-history`).
Delta: only-updated JSON/CSV (`--only-updated-*`).
Diagnostics: timings (`--timings`, `--timings-json`), expectation warnings (`--expect-numbers`).

### 7. Important Flags
| Flag | Purpose |
|------|---------|
| `--include-closed` | Use closed variant (superset) |
| `--single-closed-variant` | Skip second variant for speed |
| `--window-days N` | Rolling creation-date window (default 7) |
| `--include-history` | Embed full routing steps (JSON only) |
| `--include-notes` | Add notes count per DR |
| `--only-updated-json/csv` | Delta outputs |
| `--retries` / `--retry-wait-seconds` | Exponential backoff |
| `--timings` / `--timings-json` | Performance diagnostics |
| `--jitter-seconds` | Random startup delay (0..N) |
| `--expect-numbers` | Warn if listed DR IDs absent |
| `--tz-offset-hours` | (Legacy) Avoid non-zero in current logic |

### 8. Window Strategy
7-day window retained to keep older DRs that continue to update. A today-only test returned only 13 of 86 DRs (73 missed). Optional future pattern: frequent 3–5 day window + daily 7-day sweep.

### 9. Performance Snapshot
Typical total: 12–18 s for ~70–80 DRs.
List call (closed variant 1): 2–8 s variable.
Routing calls: majority of runtime (N * ~0.07 s).

### 10. Example Command (Manual)
```
python poll_drs_incremental.py \
  --user YOURUSER --password YOURPASS \
  --include-closed --single-closed-variant \
  --window-days 7 \
  --state-file dr_state.json \
  --out-json dr_incremental.json \
  --out-csv dr_incremental.csv \
  --only-updated-json dr_incremental_updates.json \
  --only-updated-csv dr_incremental_updates.csv \
  --expect-numbers 49090 49089 49088 \
  --timings --jitter-seconds 60
```

### 11. Scheduling (Windows Task Scheduler)
```
powershell -NoProfile -ExecutionPolicy Bypass -Command "python 'C:/Path/poll_drs_incremental.py' --user USER --password PASS --include-closed --single-closed-variant --window-days 7 --state-file 'C:/Path/dr_state.json' --out-json 'C:/Path/dr_incremental.json' --out-csv 'C:/Path/dr_incremental.csv' --only-updated-json 'C:/Path/dr_incremental_updates.json' --only-updated-csv 'C:/Path/dr_incremental_updates.csv' --expect-numbers 49090 49089 49088 --jitter-seconds 60"
```

### 12. CSV Columns
DeviationNumber, IsClosed, CurrentRouting, DeviationState, Product, SalesOrderNumber, CreationComments (if present), RoutingStepCount, LatestRoutingDepartment, LatestRoutingUser, LatestRoutingState, LatestRoutingTouched, LatestRoutingComment, LatestNonEmptyRoutingComment, Updated, UpdatedRouting, UpdatedComment, NotesCount (optional).

### 13. Monitoring
Use `--expect-numbers` for newest IDs; inspect timings JSON for latency spikes; verify highest DeviationNumber rises over time.

### 14. Limitations
Creation-date filter hides updates beyond window. No routing skip optimization yet. Negative tz offset suppresses newest DRs.

### 15. Backlog
Local-midnight start; routing skip heuristic; transition log; auto expectation seeding; daily extended backfill; structured logging; filtered outputs.

### 16. Setup
```
python -m venv .venv
.\.venv\Scripts\activate
pip install zeep
python poll_drs_incremental.py --user USER --password PASS --include-closed --single-closed-variant --window-days 7 --state-file dr_state.json --out-json dr_incremental.json --out-csv dr_incremental.csv
```

### 17. Troubleshooting Quick Table
Symptom | Action
------- | ------
Missing newest DRs | Ensure tz offset 0; closed variant used
Few/zero DRs | Check window-days & credentials
Slow run | Review timings JSON
No updates flagged | Confirm new routing step exists or comment changed

### 18. Security Notes
Store credentials securely (env vars or secret store). Script only invokes read-only ops.

### 19. Ultra-Short Summary
Closed variant + 7-day creation window + state tracking => incremental DR routing/comment feed. Avoid negative tz offset. Use delta outputs & expectation checks for monitoring.

---
End of DR Poller README.
