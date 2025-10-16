from __future__ import annotations
import os, json, sqlite3, re
from typing import Dict, Any
from datetime import datetime, timezone


def _norm_so(so: str|None) -> str|None:
    if not so:
        return None
    return re.sub(r"-", "", so.strip())


def _extract_metadata_from_json(row: Dict, urgency_map: Dict[str, str]) -> Dict[str, Any]:
    """Extract static metadata from DR JSON row (if Master is present)."""
    master = row.get('_Master')  # Will be added by poller
    if not master:
        return {}
    
    # Urgency
    urgency_pk = master.get('UrgencyPK')
    urgency = urgency_map.get(urgency_pk) if urgency_pk else None
    
    # Defect description
    defect_description = None
    defect_type = master.get('DefectType')
    if defect_type and isinstance(defect_type, dict):
        defect_description = defect_type.get('Name')
    
    # ChargedTo department
    charged_to_dept = master.get('ChargedToDeptName')
    
    # Deviation Type and Component from ReasonLink
    deviation_type = None
    component = None
    reason_link = master.get('ReasonLink')
    if reason_link and isinstance(reason_link, dict):
        deviation_type = reason_link.get('DeviationTypeName')
        component = reason_link.get('ComponentTypeName')
    
    # Creation info
    date_created = master.get('DateCreated')
    user_created = None  # Would need UserCreatedPK lookup
    
    return {
        'urgency': urgency,
        'defect_description': defect_description,
        'charged_to_dept': charged_to_dept,
        'deviation_type': deviation_type,
        'component': component,
        'date_created': date_created,
        'user_created': user_created
    }


def ingest_last_poll(db_path: str, archive_dir: str, urgency_map: Dict[str, str] | None = None) -> Dict[str, Any]:
    """Ingest dr_incremental.json, dr_timings.json, dr_state.json from archive_dir into DB.

    Args:
        urgency_map: Optional urgency PK -> name lookup (for metadata capture)

    Returns counts: {run_id, snapshots, timing_events, state_rows, metadata_captured}
    """
    if urgency_map is None:
        urgency_map = {}
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
            
            dn = int(row.get('DeviationNumber'))

            # Delta-only: check if this DR changed since last snapshot
            cur.execute(
                """
                SELECT deviation_state, latest_routing_user, latest_routing_comment, 
                       latest_non_empty_routing_comment, routing_step_count
                FROM DRItemSnapshot
                WHERE deviation_number = ?
                ORDER BY run_id DESC
                LIMIT 1
                """,
                (dn,)
            )
            last_snap = cur.fetchone()
            
            # Compare key fields to detect changes
            current_state = row.get('DeviationState')
            current_user = row.get('LatestRoutingUser')
            current_comment = row.get('LatestRoutingComment')
            current_nonempty = row.get('LatestNonEmptyRoutingComment')
            current_step_count = int(row.get('RoutingStepCount') or 0)
            
            changed = (
                last_snap is None or  # First time seeing this DR
                last_snap[0] != current_state or
                last_snap[1] != current_user or
                last_snap[2] != current_comment or
                last_snap[3] != current_nonempty or
                last_snap[4] != current_step_count
            )
            
            if changed:
                cur.execute(
                    """
                    INSERT INTO DRItemSnapshot (
                        run_id, deviation_number, current_routing, deviation_state, is_closed,
                        product, sales_order_number, sales_order_number_normalized, comnumber1,
                        creation_comments, latest_routing_department, latest_routing_user, latest_routing_state,
                        latest_routing_touched, latest_routing_comment, latest_non_empty_routing_comment,
                        routing_step_count, updated, updated_routing, updated_comment
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        run_id,
                        dn,
                        row.get('CurrentRouting'),
                        current_state,
                        1 if row.get('IsClosed') else 0,
                        row.get('Product'),
                        so,
                        so_norm,
                        comnum,
                        row.get('CreationComments'),
                        row.get('LatestRoutingDepartment'),
                        current_user,
                        row.get('LatestRoutingState'),
                        row.get('LatestRoutingTouched'),
                        current_comment,
                        current_nonempty,
                        current_step_count,
                        1 if row.get('Updated') else 0,
                        1 if row.get('UpdatedRouting') else 0,
                        1 if row.get('UpdatedComment') else 0,
                    ),
                )
                snap_cnt += 1
            
            # Capture static metadata for NEW DRs only (first time we see them)
            if last_snap is None and urgency_map:
                # Check if metadata already exists
                cur.execute("SELECT 1 FROM DRStaticMetadata WHERE deviation_number = ?", (dn,))
                if not cur.fetchone():
                    # Extract metadata from JSON
                    metadata = _extract_metadata_from_json(row, urgency_map)
                    if metadata:
                        cur.execute("""
                            INSERT OR IGNORE INTO DRStaticMetadata (
                                deviation_number, urgency, defect_description, charged_to_dept,
                                deviation_type, component,
                                date_created, user_created, first_captured_run_id, first_captured_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            dn,
                            metadata.get('urgency'),
                            metadata.get('defect_description'),
                            metadata.get('charged_to_dept'),
                            metadata.get('deviation_type'),
                            metadata.get('component'),
                            metadata.get('date_created'),
                            metadata.get('user_created'),
                            run_id,
                            datetime.now(timezone.utc).isoformat()
                        ))

            # Optional routing history list
            rh = row.get('RoutingHistory')
            if isinstance(rh, list) and rh:
                # Only insert new steps beyond what we already have (append-only comment model)
                dn = int(row.get('DeviationNumber'))
                cur.execute(
                    "SELECT COALESCE(MAX(step_index), -1) FROM DRRoutingStep WHERE deviation_number=?",
                    (dn,)
                )
                last_step_idx = cur.fetchone()[0]
                
                # Insert steps beyond last known index
                for idx, step in enumerate(rh):
                    if idx > last_step_idx:
                        cur.execute(
                            """
                            INSERT INTO DRRoutingStep(
                                run_id, deviation_number, step_index, DateTouched, UserName, RoutingDepartment, State, EmailAddress, UserComments
                            ) VALUES (?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                run_id,
                                dn,
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
