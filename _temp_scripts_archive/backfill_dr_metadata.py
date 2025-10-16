"""One-time backfill script to populate DRStaticMetadata for existing DRs.

This script:
1. Finds all DRs in DRItemSnapshot that don't have metadata yet
2. Calls DisplayItemForDRNumber for each to get Master object
3. Extracts Urgency, Defect, ChargedTo from Master
4. Inserts into DRStaticMetadata

Run once after creating DRStaticMetadata table, then never again.
"""
from __future__ import annotations
import os
import sqlite3
import time
from datetime import datetime, timezone
from zeep import Client
from zeep.transports import Transport
from zeep.cache import InMemoryCache

ROOT = os.path.dirname(__file__)
DB_PATH = os.path.join(ROOT, 'SCHLabor.db')
CRED_PATH = os.path.join(ROOT, 'download_archive', 'dr_credentials.json')
SERVICE = 'SQRS_DR'

# Load credentials
import json
user = None
pwd = None
if os.path.isfile(CRED_PATH):
    data = json.load(open(CRED_PATH, 'r', encoding='utf-8'))
    user = (data.get('user') or '').strip()
    if user:
        try:
            import keyring
            pwd = keyring.get_password(SERVICE, user)
        except Exception:
            pwd = None
    if not pwd:
        pwd = (data.get('password') or '').strip()

if not (user and pwd and len(user) == 5 and len(pwd) == 5):
    raise SystemExit('Saved credentials not found or invalid. Open the app and save 5-char creds first.')

DEFAULT_BASE_URL = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc'
THROTTLE_SECONDS = 0.5
MAX_DRS_PER_RUN = 500


def build_urgency_map(client, dev_user):
    """Build lookup table for urgency PK -> human-readable name."""
    try:
        urgency_list = client.service.GetUrgencyList(devUser=dev_user)
        urgency_map = {}
        if urgency_list:
            for u in urgency_list:
                u_vals = getattr(u, '__values__', {})
                pk = u_vals.get('PK')
                long_name = u_vals.get('LongName')
                if pk:
                    urgency_map[pk] = long_name
        return urgency_map
    except Exception as e:
        print(f'[WARN] GetUrgencyList failed: {e}')
        return {}


def extract_metadata_from_master(master, urgency_map):
    """Extract static metadata from Master object."""
    if not master:
        return None
    
    m_vals = getattr(master, '__values__', {})
    
    # Urgency
    urgency_pk = m_vals.get('UrgencyPK')
    urgency = urgency_map.get(urgency_pk) if urgency_pk else None
    
    # Defect description
    defect_description = None
    defect_type_obj = m_vals.get('DefectType')
    if defect_type_obj:
        dt_vals = getattr(defect_type_obj, '__values__', {})
        defect_description = dt_vals.get('Name')
    
    # ChargedTo department
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
    
    # User created - need to look up from PK
    user_created = None
    user_created_pk = m_vals.get('UserCreatedPK')
    # For now, skip user lookup (would need another service call)
    
    return {
        'urgency': urgency,
        'defect_description': defect_description,
        'charged_to_dept': charged_to_dept,
        'deviation_type': deviation_type,
        'component': component,
        'date_created': str(date_created) if date_created else None,
        'user_created': user_created
    }


def backfill_metadata():
    """Main backfill logic."""
    print('='*80)
    print('DR STATIC METADATA BACKFILL')
    print('='*80)
    print(f'Database: {DB_PATH}')
    print(f'Credentials: {user} (5-char)')
    print(f'Throttle: {THROTTLE_SECONDS}s between DRs')
    print(f'Safety limit: {MAX_DRS_PER_RUN} DRs per run')
    print()
    
    # Ensure schema
    conn = sqlite3.connect(DB_PATH)
    try:
        import dr_schema
        dr_schema.ensure_dr_tables(conn)
        print('[OK] Schema ensured (DRStaticMetadata table exists)')
    except Exception as e:
        print(f'[ERROR] Schema ensure failed: {e}')
        return
    finally:
        conn.close()
    
    # Get list of DRs needing backfill
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT deviation_number
            FROM DRItemSnapshot
            WHERE deviation_number NOT IN (SELECT deviation_number FROM DRStaticMetadata)
            ORDER BY deviation_number
            LIMIT ?
        """, (MAX_DRS_PER_RUN,))
        drs_to_backfill = [row[0] for row in cur.fetchall()]
    finally:
        conn.close()
    
    if not drs_to_backfill:
        print('[OK] No DRs need backfill. All DRs already have metadata.')
        return
    
    print(f'[INFO] Found {len(drs_to_backfill)} DRs needing metadata')
    print()
    
    # Setup SOAP client
    wsdl = DEFAULT_BASE_URL + '?wsdl'
    client = Client(wsdl=wsdl, transport=Transport(cache=InMemoryCache(), timeout=30))
    
    # Login
    try:
        client.service.RegisterSession(username=user, password=pwd)
    except Exception:
        pass
    try:
        dev_user = client.service.GetUser(username=user, password=pwd)
    except Exception as e:
        print(f'[ERROR] Login failed: {e}')
        return
    
    # Build urgency lookup
    print('[INFO] Building urgency lookup table...')
    urgency_map = build_urgency_map(client, dev_user)
    print(f'[OK] Loaded {len(urgency_map)} urgency options')
    print()
    
    # Process each DR
    success_count = 0
    skip_count = 0
    error_count = 0
    
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        
        for idx, dr_num in enumerate(drs_to_backfill, 1):
            # Throttle
            if idx > 1:
                time.sleep(THROTTLE_SECONDS)
            
            try:
                # Call DisplayItemForDRNumber
                result = client.service.DisplayItemForDRNumber(devUser=dev_user, drNumber=str(dr_num))
                
                if not result:
                    print(f'[{idx}/{len(drs_to_backfill)}] DR {dr_num}: No result from server')
                    skip_count += 1
                    continue
                
                # Extract Master
                vals = getattr(result, '__values__', {})
                master = vals.get('Master')
                
                if not master:
                    print(f'[{idx}/{len(drs_to_backfill)}] DR {dr_num}: No Master object')
                    skip_count += 1
                    continue
                
                # Extract metadata
                metadata = extract_metadata_from_master(master, urgency_map)
                
                if not metadata:
                    print(f'[{idx}/{len(drs_to_backfill)}] DR {dr_num}: Failed to extract metadata')
                    skip_count += 1
                    continue
                
                # Insert into DB
                cur.execute("""
                    INSERT OR IGNORE INTO DRStaticMetadata (
                        deviation_number, urgency, defect_description, charged_to_dept,
                        deviation_type, component,
                        date_created, user_created, first_captured_run_id, first_captured_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
                """, (
                    dr_num,
                    metadata['urgency'],
                    metadata['defect_description'],
                    metadata['charged_to_dept'],
                    metadata['deviation_type'],
                    metadata['component'],
                    metadata['date_created'],
                    metadata['user_created'],
                    datetime.now(timezone.utc).isoformat()
                ))
                
                success_count += 1
                
                if idx % 10 == 0:
                    print(f'[{idx}/{len(drs_to_backfill)}] Progress: {success_count} captured, {skip_count} skipped, {error_count} errors')
                
            except Exception as e:
                print(f'[{idx}/{len(drs_to_backfill)}] DR {dr_num}: ERROR - {e}')
                error_count += 1
                continue
        
        conn.commit()
        
    finally:
        conn.close()
    
    print()
    print('='*80)
    print('BACKFILL COMPLETE')
    print('='*80)
    print(f'Success: {success_count}')
    print(f'Skipped: {skip_count}')
    print(f'Errors:  {error_count}')
    print()
    
    if len(drs_to_backfill) >= MAX_DRS_PER_RUN:
        print(f'[INFO] Hit safety limit of {MAX_DRS_PER_RUN} DRs.')
        print(f'[INFO] Run this script again to process more DRs.')


if __name__ == '__main__':
    backfill_metadata()
