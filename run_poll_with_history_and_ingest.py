from __future__ import annotations
import os, json, sqlite3, types

ROOT = os.path.dirname(__file__)
ARCH = os.path.join(ROOT, 'download_archive')
DB = os.path.join(ROOT, 'SCHLabor.db')
CRED_PATH = os.path.join(ARCH, 'dr_credentials.json')
SERVICE = 'SQRS_DR'

# Load credentials from keyring or plaintext fallback
user = None
pwd = None
if os.path.isfile(CRED_PATH):
    data = json.load(open(CRED_PATH,'r',encoding='utf-8'))
    user = (data.get('user') or '').strip()
    if user:
        try:
            import keyring  # type: ignore
            pwd = keyring.get_password(SERVICE, user)
        except Exception:
            pwd = None
    if not pwd:
        pwd = (data.get('password') or '').strip()

if not (user and pwd and len(user)==5 and len(pwd)==5):
    raise SystemExit('Saved credentials not found or invalid. Open the app and save 5-char creds first.')

# Inline poller using zeep (self-contained, no external poll_drs_incremental dependency)
from datetime import datetime, timedelta, timezone
import time
from zeep import Client
from zeep.transports import Transport
from zeep.cache import InMemoryCache

def poll_and_write(base_url, user, pwd, app_name, window_days, include_closed, include_history,
                    state_file, out_json, timings_json, tz_offset_hours=0, max_drs=None, throttle_seconds=0):
    """Poll MOM WCF service and write JSON outputs with safety limits.
    
    Args:
        max_drs: Maximum number of DRs to process (safety limit)
        throttle_seconds: Sleep time between each DR routing fetch (prevents hammering host)
    """
    t0 = time.perf_counter()
    wsdl = base_url.rstrip('?') + '?wsdl'
    
    # Set strict timeout to prevent runaway connections
    client = Client(wsdl=wsdl, transport=Transport(cache=InMemoryCache(), timeout=30))
    
    # Login (multi-method like original working code)
    dev_user = None
    try:
        client.service.RegisterSession(username=user, password=pwd)
    except Exception:
        pass
    try:
        client.service.GetApplicationPK(applicationKey=app_name, username=user, password=pwd)
    except Exception:
        pass
    try:
        client.service.MOM_Login(username=user, password=pwd)
    except Exception:
        pass
    try:
        dev_user = client.service.GetUser(username=user, password=pwd)
    except Exception:
        pass
    
    # Date window - use local time, not UTC (SOAP service expects local time)
    now = datetime.now()
    # Add 1 day buffer to end_dt to catch DRs created "today"
    end_dt = now + timedelta(days=1)
    start_dt = (now - timedelta(days=window_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Build urgency lookup map (one call per poll)
    urgency_map = {}
    try:
        urgency_list = client.service.GetUrgencyList(devUser=dev_user)
        if urgency_list:
            for u in urgency_list:
                u_vals = getattr(u, '__values__', {})
                pk = u_vals.get('PK')
                long_name = u_vals.get('LongName')
                if pk:
                    urgency_map[pk] = long_name
        print(f'[INFO] Loaded {len(urgency_map)} urgency options')
    except Exception as e:
        print(f'[WARN] GetUrgencyList failed: {e}')
    
    # Fetch DRs (single attempt, handle null response gracefully)
    try:
        items = client.service.DisplayItemForDateRange(
            devUser=dev_user,
            fromDate=start_dt, 
            toDate=end_dt, 
            closed=include_closed
        )
        if items is None:
            items = []
    except Exception as e:
        print(f'[ERROR] DisplayItemForDateRange failed: {e}')
        items = []
    
    # Apply safety limit
    original_count = len(items)
    if max_drs and len(items) > max_drs:
        items = items[:max_drs]
        print(f'[SAFETY] Limited to {max_drs} DRs (found {original_count})')
    
    def iso(dt):
        return dt.isoformat() if isinstance(dt, datetime) else (str(dt) if dt else None)
    
    def extract_master_metadata(master):
        """Extract metadata fields from Master object for static storage."""
        if not master:
            return {}
        m_vals = getattr(master, '__values__', {})
        
        # Urgency PK
        urgency_pk = m_vals.get('UrgencyPK')
        
        # Defect
        defect_description = None
        defect_type_obj = m_vals.get('DefectType')
        if defect_type_obj:
            dt_vals = getattr(defect_type_obj, '__values__', {})
            defect_description = dt_vals.get('Name')
        
        # ChargedTo
        charged_to_dept = m_vals.get('ChargedToDeptName')
        
        # Deviation Type and Component from ReasonLink
        deviation_type = None
        component = None
        reason_link_obj = m_vals.get('ReasonLink')
        if reason_link_obj:
            rl_vals = getattr(reason_link_obj, '__values__', {})
            deviation_type = rl_vals.get('DeviationTypeName')
            component = rl_vals.get('ComponentTypeName')
        
        # Creation info
        date_created = m_vals.get('DateCreated')
        if date_created and hasattr(date_created, 'isoformat'):
            date_created = date_created.isoformat()
        
        return {
            'UrgencyPK': urgency_pk,
            'DefectType': {'Name': defect_description} if defect_description else None,
            'ChargedToDeptName': charged_to_dept,
            'ReasonLink': {
                'DeviationTypeName': deviation_type,
                'ComponentTypeName': component
            } if (deviation_type or component) else None,
            'DateCreated': str(date_created) if date_created else None
        }
    
    output = []
    for idx, item in enumerate(items):
        try:
            vals = getattr(item, '__values__', {})
            dn = vals.get('DeviationNumber')
            master = vals.get('Master')
            
            # Throttle between DRs to prevent hammering
            if throttle_seconds > 0 and idx > 0:
                time.sleep(throttle_seconds)
            
            # Fetch routing history if requested (single attempt, no retries)
            history = []
            if include_history and master:
                try:
                    routes = client.service.RoutingsForMaster(devUser=dev_user, master=master) or []
                    for idx_r, r in enumerate(routes):
                        rvals = getattr(r, '__values__', {})
                        history.append({
                            'DateTouched': iso(rvals.get('DateTouched')),
                            'UserName': rvals.get('UserName'),
                            'RoutingDepartment': rvals.get('RoutingDepartment'),
                            'State': rvals.get('State'),
                            'EmailAddress': rvals.get('EmailAddress'),
                            'UserComments': rvals.get('UserComments'),
                        })
                except Exception:
                    # Skip routing history on failure, but continue with DR
                    pass
            
            latest = history[-1] if history else {}
            nonempty_comment = next((h['UserComments'] for h in reversed(history) if h.get('UserComments')), None)
            
            # Extract Master metadata for static capture
            master_metadata = extract_master_metadata(master) if master else {}
            
            output.append({
                'DeviationNumber': dn,
                'CurrentRouting': vals.get('CurrentRouting'),
                'DeviationState': vals.get('DeviationState'),
                'IsClosed': vals.get('IsClosed'),
                'Product': vals.get('Product'),
                'ComNumber': vals.get('ComNumber'),
                'SalesOrderNumber': vals.get('SalesOrderNumber'),
                'CreationComments': vals.get('CreationComments'),
                'LatestRoutingDepartment': latest.get('RoutingDepartment'),
                'LatestRoutingUser': latest.get('UserName'),
                'LatestRoutingState': latest.get('State'),
                'LatestRoutingTouched': latest.get('DateTouched'),
                'LatestRoutingComment': latest.get('UserComments'),
                'LatestNonEmptyRoutingComment': nonempty_comment,
                'RoutingStepCount': len(history),
                'RoutingHistory': history,
                'Updated': False,
                'UpdatedRouting': False,
                'UpdatedComment': False,
                # Include Master metadata for ingest to capture
                '_Master': master_metadata if master_metadata else None,
            })
        except Exception:
            # Skip individual DR on failure, continue with next
            continue
    
    # Write outputs
    os.makedirs(os.path.dirname(out_json), exist_ok=True)
    with open(out_json, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)
    
    elapsed = time.perf_counter() - t0
    if timings_json:
        with open(timings_json, 'w', encoding='utf-8') as f:
            json.dump({
                'generated_at_utc': datetime.now(timezone.utc).isoformat(),
                'total_elapsed_seconds': round(elapsed, 3),
                'events': []
            }, f, indent=2)
    
    # Minimal state file
    if state_file:
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump({'last_run': datetime.now(timezone.utc).isoformat(), 'drs': {}}, f, indent=2)
    
    print(f'[INFO] Retrieved {len(output)} DRs in {elapsed:.2f}s')
    
    # Return urgency_map for use in ingestion
    return urgency_map

# Standalone execution (only when run directly, not when imported)
if __name__ == '__main__':
    # Build poll args with safety limits (7 day = 150 DRs max)
    DEFAULT_BASE_URL = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc'
    window_days = int(os.getenv('WINDOW_DAYS', '7'))

    # Safety limits based on window
    if window_days <= 2:
        max_drs = 50
        throttle_sec = 0.1
    elif window_days <= 7:
        max_drs = 150
        throttle_sec = 0.2
    else:
        max_drs = 500
        throttle_sec = 0.5

    print(f'[RUN] Polling with history (window={window_days}d, max={max_drs} DRs, throttle={throttle_sec}s)...')
    urgency_map = poll_and_write(
        base_url=os.getenv('MOM_BASE_URL', DEFAULT_BASE_URL),
        user=user,
        pwd=pwd,
        app_name=os.getenv('MOM_APP_NAME','ManufacturingDeviationSystem'),
        window_days=window_days,
        include_closed=True,
        include_history=True,
        state_file=os.path.join(ARCH, 'dr_state.json'),
        out_json=os.path.join(ARCH, 'dr_incremental.json'),
        timings_json=os.path.join(ARCH, 'dr_timings.json'),
        tz_offset_hours=0,
        max_drs=max_drs,
        throttle_seconds=throttle_sec,
    )

    # Ingest into DB
    import dr_ingest
    print('[RUN] Ingesting into DB...')
    res = dr_ingest.ingest_last_poll(DB, ARCH, urgency_map)
    print('[RESULT] Ingest:', res)

    # Quick verification for a few present DRs (sample known from recent outputs)
    samples = [49087, 49088, 49052]
    print('[VERIFY] Routing step counts:')
    conn = sqlite3.connect(DB)
    try:
        cur = conn.cursor()
        cur.execute('SELECT MAX(id) FROM DRPollRun')
        last_run = cur.fetchone()[0]
        for dn in samples:
            cur.execute('SELECT COUNT(*) FROM DRRoutingStep WHERE run_id=? AND deviation_number=?', (last_run, dn))
            cnt = cur.fetchone()[0]
            cur.execute('SELECT COALESCE(UserComments, "") FROM DRRoutingStep WHERE run_id=? AND deviation_number=? ORDER BY step_index', (last_run, dn))
            comments = [r[0] for r in cur.fetchall()]
            nonempty = [c for c in comments if c and c.strip()]
            print(f'  DR {dn}: steps={cnt}, nonempty_comments={len(nonempty)}')
            if nonempty:
                print('   - sample:', nonempty[:5])
    finally:
        conn.close()
