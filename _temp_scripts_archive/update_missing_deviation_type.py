"""
Update the DRs that are missing deviation_type and component.
Fetches their Master data and updates in place.
"""
import sqlite3
import time
from datetime import datetime, timedelta
from zeep import Client
from zeep.wsse.username import UsernameToken
import json

DB_PATH = r'C:\Project p\SQRS\SCHLabor.db'
CREDS_PATH = r'C:\Project p\SQRS\download_archive\dr_credentials.json'
WSDL_URL = 'http://service1.ahu.jci.com/AHUService.svc?singleWsdl'

def load_credentials():
    with open(CREDS_PATH, 'r') as f:
        creds = json.load(f)
    return creds['user'], creds['password']

def get_urgency_map(client):
    """Get urgency lookup table."""
    result = client.service.GetUrgencyList()
    urgency_map = {}
    if result and hasattr(result, 'CUrgency'):
        for urg in result.CUrgency:
            vals = getattr(urg, '__values__', {})
            urg_id = vals.get('UrgencyID')
            urg_desc = vals.get('UrgencyDescription')
            if urg_id is not None and urg_desc:
                urgency_map[str(urg_id)] = urg_desc
    return urgency_map

def extract_metadata_from_master(master, urgency_map):
    """Extract metadata from Master object."""
    m_vals = getattr(master, '__values__', {})
    
    urgency_id = m_vals.get('UrgencyID')
    urgency = urgency_map.get(str(urgency_id)) if urgency_id is not None else None
    
    defect_obj = m_vals.get('DefectType')
    defect = None
    if defect_obj:
        d_vals = getattr(defect_obj, '__values__', {})
        defect = d_vals.get('DefectTypeName')
    
    charged_obj = m_vals.get('ChargedTo')
    charged_to = None
    if charged_obj:
        c_vals = getattr(charged_obj, '__values__', {})
        charged_to = c_vals.get('Name')
    
    # Extract deviation_type and component from ReasonLink
    reason_link_obj = m_vals.get('ReasonLink')
    deviation_type = None
    component = None
    if reason_link_obj:
        rl_vals = getattr(reason_link_obj, '__values__', {})
        deviation_type = rl_vals.get('DeviationTypeName')
        component = rl_vals.get('ComponentTypeName')
    
    return {
        'urgency': urgency,
        'defect': defect,
        'charged_to': charged_to,
        'deviation_type': deviation_type,
        'component': component
    }

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all DRs that we need to process (currently in snapshot but missing from metadata)
    cursor.execute("""
        SELECT DISTINCT deviation_number 
        FROM DRItemSnapshot 
        WHERE deviation_number NOT IN (SELECT deviation_number FROM DRStaticMetadata)
        ORDER BY deviation_number
    """)
    missing_drs = [row[0] for row in cursor.fetchall()]
    
    if not missing_drs:
        print("No DRs missing from DRStaticMetadata. All done!")
        conn.close()
        return
    
    print(f"Found {len(missing_drs)} DRs missing from DRStaticMetadata")
    print(f"DRs to capture: {missing_drs[:20]}")  # Show first 20
    if len(missing_drs) > 20:
        print(f"... and {len(missing_drs) - 20} more")
    print()
    
    # Set up SOAP client
    username, password = load_credentials()
    client = Client(WSDL_URL, wsse=UsernameToken(username, password))
    
    # Get urgency map
    print("Loading urgency map...")
    urgency_map = get_urgency_map(client)
    print(f"Loaded {len(urgency_map)} urgency options")
    print()
    
    # Use DisplayItemForDateRange to get all DRs at once
    print("Fetching all DRs from SOAP...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365*2)  # 2 years back
    
    result = client.service.DisplayItemForDateRange(
        beginDate=start_date,
        endDate=end_date
    )
    
    # Build a map of DR number -> Master object
    dr_master_map = {}
    if result and hasattr(result, 'CDisplayItem'):
        for item in result.CDisplayItem:
            vals = getattr(item, '__values__', {})
            dr_num = vals.get('DeviationNumber')
            master = vals.get('Master')
            if dr_num in missing_drs:
                dr_master_map[dr_num] = master
    
    print(f"Found {len(dr_master_map)} DRs with Master objects")
    print()
    
    # Process each missing DR
    success = 0
    skipped = 0
    errors = 0
    
    for i, dr_num in enumerate(missing_drs, 1):
        print(f"[{i}/{len(missing_drs)}] DR {dr_num}...", end=' ')
        
        try:
            master = dr_master_map.get(dr_num)
            if master:
                metadata = extract_metadata_from_master(master, urgency_map)
                
                cursor.execute("""
                    INSERT INTO DRStaticMetadata 
                    (deviation_number, urgency, defect_description, charged_to_dept,
                     deviation_type, component, date_created, user_created,
                     first_captured_run_id, first_captured_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    dr_num,
                    metadata['urgency'],
                    metadata['defect'],
                    metadata['charged_to'],
                    metadata['deviation_type'],
                    metadata['component'],
                    datetime.now().isoformat(),
                    'update_script',
                    -1,
                    datetime.now().isoformat()
                ))
                conn.commit()
                success += 1
                print(f"✓ Captured (deviation_type={metadata['deviation_type']}, component={metadata['component']})")
            else:
                skipped += 1
                print("✗ No Master object")
            
        except Exception as e:
            errors += 1
            print(f"✗ Error: {e}")
            conn.rollback()
    
    conn.close()
    
    print()
    print("=" * 60)
    print("UPDATE COMPLETE")
    print(f"Success: {success}")
    print(f"Skipped: {skipped}")
    print(f"Errors: {errors}")
    print("=" * 60)

if __name__ == '__main__':
    main()
