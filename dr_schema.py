"""DR schema creation for SQLite (SCHLabor.db).

Creates tables to store all information found in:
- dr_incremental.json (snapshot of DR items per run)
- dr_timings.json (timing events per run)
- dr_state.json (persisted last-seen state by DR)

No relationships to existing project tables are created here; we can add them later when specified.
"""
from __future__ import annotations
import sqlite3


def ensure_dr_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    # Poll run table (one row per run)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS DRPollRun (
            id INTEGER PRIMARY KEY,
            generated_at_utc TEXT,
            total_elapsed_seconds REAL,
            window_days INTEGER,
            include_closed INTEGER,
            tz_offset_hours INTEGER,
            single_closed_variant INTEGER,
            state_file_path TEXT,
            out_json_path TEXT,
            timings_json_path TEXT
        )
        """
    )
    # Timing events for a run
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS DRPollTimingEvent (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            description TEXT,
            attempts INTEGER,
            successful INTEGER,
            elapsed_seconds REAL,
            FOREIGN KEY(run_id) REFERENCES DRPollRun(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drtime_run ON DRPollTimingEvent(run_id)")

    # Snapshot of DR items for a given run (composite by run + deviation)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS DRItemSnapshot (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            deviation_number INTEGER NOT NULL,
            current_routing TEXT,
            deviation_state TEXT,
            is_closed INTEGER,
            product TEXT,
            sales_order_number TEXT,
            -- normalized (remove dashes) to join with SCHSchedulingSummary.contractnumber
            sales_order_number_normalized TEXT,
            -- optional COM linkage (to be populated via join/view later)
            comnumber1 INTEGER,
            creation_comments TEXT,
            latest_routing_department TEXT,
            latest_routing_user TEXT,
            latest_routing_state TEXT,
            latest_routing_touched TEXT,
            latest_routing_comment TEXT,
            latest_non_empty_routing_comment TEXT,
            routing_step_count INTEGER,
            updated INTEGER,
            updated_routing INTEGER,
            updated_comment INTEGER,
            FOREIGN KEY(run_id) REFERENCES DRPollRun(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drsnap_run ON DRItemSnapshot(run_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drsnap_dev ON DRItemSnapshot(deviation_number)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drsnap_updated ON DRItemSnapshot(updated, updated_routing, updated_comment)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drsnap_salesnorm ON DRItemSnapshot(sales_order_number_normalized)")
    # Enforce uniqueness per run per deviation
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_drsnap_run_dev ON DRItemSnapshot(run_id, deviation_number)")

    # State persistence: last run time and last-seen fields per DR
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS DRStateMeta (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_run_utc TEXT
        )
        """
    )
    # Seed a single row if empty
    cur.execute("INSERT OR IGNORE INTO DRStateMeta(id, last_run_utc) VALUES (1, NULL)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS DRState (
            deviation_number INTEGER PRIMARY KEY,
            lastRoutingTouched TEXT,
            lastRoutingStepCount INTEGER,
            lastNoteCount INTEGER,
            lastSeenState TEXT,
            lastNonEmptyRoutingComment TEXT
        )
        """
    )
    # Backfill new columns if table existed before
    try:
        cur.execute("PRAGMA table_info(DRItemSnapshot)")
        cols = {r[1] for r in cur.fetchall()}
        if 'sales_order_number_normalized' not in cols:
            cur.execute("ALTER TABLE DRItemSnapshot ADD COLUMN sales_order_number_normalized TEXT")
        if 'comnumber1' not in cols:
            cur.execute("ALTER TABLE DRItemSnapshot ADD COLUMN comnumber1 INTEGER")
    except Exception:
        pass

    # Routing history steps per run/DR (optional when include_history is enabled)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS DRRoutingStep (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            deviation_number INTEGER NOT NULL,
            step_index INTEGER NOT NULL,
            DateTouched TEXT,
            UserName TEXT,
            RoutingDepartment TEXT,
            State TEXT,
            EmailAddress TEXT,
            UserComments TEXT,
            FOREIGN KEY(run_id) REFERENCES DRPollRun(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drrouting_run ON DRRoutingStep(run_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drrouting_dev ON DRRoutingStep(deviation_number)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_drrouting_run_dev_idx ON DRRoutingStep(run_id, deviation_number, step_index)")

    # Create a convenience view joining to SCHSchedulingSummary if present
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='SCHSchedulingSummary'")
        if cur.fetchone():
            # Drop and recreate view for idempotency
            cur.execute("DROP VIEW IF EXISTS V_DRSnapshot_WithSched")
            cur.execute(
                """
                CREATE VIEW V_DRSnapshot_WithSched AS
                SELECT d.*, s.comnumber1 AS sched_comnumber1, s.contractnumber AS sched_contractnumber
                FROM DRItemSnapshot d
                LEFT JOIN SCHSchedulingSummary s
                  ON REPLACE(s.contractnumber, '-', '') = d.sales_order_number_normalized
                """
            )
    except Exception:
        pass

    conn.commit()
