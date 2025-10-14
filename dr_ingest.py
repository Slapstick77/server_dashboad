from __future__ import annotations
import os, json, sqlite3, re
from typing import Dict, Any


def _norm_so(so: str|None) -> str|None:
    if not so:
        return None
    return re.sub(r"-", "", so.strip())


def ingest_last_poll(db_path: str, archive_dir: str) -> Dict[str, Any]:
    """Ingest dr_incremental.json, dr_timings.json, dr_state.json from archive_dir into DB.

    Returns counts: {run_id, snapshots, timing_events, state_rows}
    """
    inc_path = os.path.join(archive_dir, 'dr_incremental.json')
    tim_path = os.path.join(archive_dir, 'dr_timings.json')
    st_path  = os.path.join(archive_dir, 'dr_state.json')
    if not (os.path.isfile(inc_path) and os.path.isfile(tim_path) and os.path.isfile(st_path)):
        raise RuntimeError('Expected dr_incremental.json, dr_timings.json, dr_state.json in ' + archive_dir)

    drs = json.load(open(inc_path, 'r', encoding='utf-8'))
    timings = json.load(open(tim_path, 'r', encoding='utf-8'))
    state = json.load(open(st_path, 'r', encoding='utf-8'))

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        # Ensure schema (idempotent)
        try:
            import dr_schema
            dr_schema.ensure_dr_tables(conn)
        except Exception:
            pass

        # Insert run row
        gen = timings.get('generated_at_utc')
        total = timings.get('total_elapsed_seconds')
        cur.execute(
            """
            INSERT INTO DRPollRun(generated_at_utc,total_elapsed_seconds,window_days,include_closed,tz_offset_hours,single_closed_variant,state_file_path,out_json_path,timings_json_path)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                gen,
                float(total) if total is not None else None,
                7,
                1,
                0,
                1,
                st_path,
                inc_path,
                tim_path,
            ),
        )
        run_id = cur.lastrowid

        # Insert timing events
        tev_cnt = 0
        for ev in timings.get('events', []) or []:
            cur.execute(
                """
                INSERT INTO DRPollTimingEvent(run_id,description,attempts,successful,elapsed_seconds)
                VALUES(?,?,?,?,?)
                """,
                (
                    run_id,
                    ev.get('description'),
                    int(ev.get('attempts') or 0),
                    1 if ev.get('successful') else 0,
                    float(ev.get('elapsed_seconds') or 0.0),
                ),
            )
            tev_cnt += 1

        # Insert snapshots (and optional routing history)
        snap_cnt = 0
        for row in drs:
            so = row.get('SalesOrderNumber')
            so_norm = _norm_so(so)
            # Only use ComNumber from JSON; do not backfill from scheduling here
            comnum = None
            if row.get('ComNumber') is not None:
                try:
                    comnum = int(str(row.get('ComNumber')).strip())
                except Exception:
                    comnum = None
            cur.execute(
                """
                INSERT OR REPLACE INTO DRItemSnapshot (
                    run_id, deviation_number, current_routing, deviation_state, is_closed,
                    product, sales_order_number, sales_order_number_normalized, comnumber1,
                    creation_comments, latest_routing_department, latest_routing_user, latest_routing_state,
                    latest_routing_touched, latest_routing_comment, latest_non_empty_routing_comment,
                    routing_step_count, updated, updated_routing, updated_comment
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    int(row.get('DeviationNumber')),
                    row.get('CurrentRouting'),
                    row.get('DeviationState'),
                    1 if row.get('IsClosed') else 0,
                    row.get('Product'),
                    so,
                    so_norm,
                    comnum,
                    row.get('CreationComments'),
                    row.get('LatestRoutingDepartment'),
                    row.get('LatestRoutingUser'),
                    row.get('LatestRoutingState'),
                    row.get('LatestRoutingTouched'),
                    row.get('LatestRoutingComment'),
                    row.get('LatestNonEmptyRoutingComment'),
                    int(row.get('RoutingStepCount') or 0),
                    1 if row.get('Updated') else 0,
                    1 if row.get('UpdatedRouting') else 0,
                    1 if row.get('UpdatedComment') else 0,
                ),
            )
            snap_cnt += 1

            # Optional routing history list
            rh = row.get('RoutingHistory')
            if isinstance(rh, list) and rh:
                # Insert with deterministic order
                for idx, step in enumerate(rh):
                    cur.execute(
                        """
                        INSERT OR REPLACE INTO DRRoutingStep(
                            run_id, deviation_number, step_index, DateTouched, UserName, RoutingDepartment, State, EmailAddress, UserComments
                        ) VALUES (?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            run_id,
                            int(row.get('DeviationNumber')),
                            idx,
                            step.get('DateTouched'),
                            step.get('UserName'),
                            step.get('RoutingDepartment'),
                            step.get('State'),
                            step.get('EmailAddress'),
                            step.get('UserComments'),
                        ),
                    )

        # Upsert state
        st_cnt = 0
        last_run = state.get('last_run')
        cur.execute("UPDATE DRStateMeta SET last_run_utc=? WHERE id=1", (last_run,))
        drs_state = (state.get('drs') or {})
        for k, v in drs_state.items():
            try:
                dn = int(k)
            except Exception:
                continue
            cur.execute(
                """
                INSERT INTO DRState(deviation_number,lastRoutingTouched,lastRoutingStepCount,lastNoteCount,lastSeenState,lastNonEmptyRoutingComment)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(deviation_number) DO UPDATE SET
                    lastRoutingTouched=excluded.lastRoutingTouched,
                    lastRoutingStepCount=excluded.lastRoutingStepCount,
                    lastNoteCount=excluded.lastNoteCount,
                    lastSeenState=excluded.lastSeenState,
                    lastNonEmptyRoutingComment=excluded.lastNonEmptyRoutingComment
                """,
                (
                    dn,
                    v.get('lastRoutingTouched'),
                    int(v.get('lastRoutingStepCount') or 0),
                    (None if v.get('lastNoteCount') is None else int(v.get('lastNoteCount'))),
                    v.get('lastSeenState'),
                    v.get('lastNonEmptyRoutingComment'),
                ),
            )
            st_cnt += 1

        conn.commit()
        return {"run_id": run_id, "snapshots": snap_cnt, "timing_events": tev_cnt, "state_rows": st_cnt}
    finally:
        conn.close()
