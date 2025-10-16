"""
Re-capture the 17 DRs that are missing deviation_type and component.
Deletes them from DRStaticMetadata and re-runs metadata capture.
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
    
    # Find DRs missing deviation_type
    cursor.execute("""
        SELECT deviation_number 
        FROM DRStaticMetadata 
        WHERE deviation_type IS NULL
        ORDER BY deviation_number
    """)
    missing_drs = [row[0] for row in cursor.fetchall()]
    
    if not missing_drs:
        print("No DRs missing deviation_type. All done!")
        conn.close()
        return
    
    print(f"Found {len(missing_drs)} DRs missing deviation_type")
    print(f"DRs to re-capture: {missing_drs}")
    print()
    
    # Delete these DRs from DRStaticMetadata
    placeholders = ','.join('?' * len(missing_drs))
    cursor.execute(f"DELETE FROM DRStaticMetadata WHERE deviation_number IN ({placeholders})", missing_drs)
    conn.commit()
    print(f"Deleted {len(missing_drs)} DRs from DRStaticMetadata")
    print()
    
    # Set up SOAP client
    username, password = load_credentials()
    client = Client(WSDL_URL, wsse=UsernameToken(username, password))
    
    # Get urgency map
    print("Loading urgency map...")
    urgency_map = get_urgency_map(client)
    print(f"Loaded {len(urgency_map)} urgency options")
    print()
    
    # Re-capture each DR
    success = 0
    skipped = 0
    errors = 0
    
    for i, dr_num in enumerate(missing_drs, 1):
        print(f"[{i}/{len(missing_drs)}] DR {dr_num}...", end=' ')
        
        try:
            # Call DisplayItemForDateRange with a wide range to get this DR
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            
            result = client.service.DisplayItemForDateRange(
                beginDate=start_date,
                endDate=end_date
            )
            
            # Find this specific DR in the results
            dr_found = False
            if result and hasattr(result, 'CDisplayItem'):
                for item in result.CDisplayItem:
                    vals = getattr(item, '__values__', {})
                    if vals.get('DeviationNumber') == dr_num:
                        master = vals.get('Master')
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
                                'recapture_script',
                                -1,
                                datetime.now().isoformat()
                            ))
                            conn.commit()
                            success += 1
                            print(f"✓ Captured (deviation_type={metadata['deviation_type']}, component={metadata['component']})")
                            dr_found = True
                        else:
                            skipped += 1
                            print("✗ No Master object")
                            dr_found = True
                        break
            
            if not dr_found:
                skipped += 1
                print("✗ DR not found in date range")
            
            time.sleep(0.5)  # Throttle
            
        except Exception as e:
            errors += 1
            print(f"✗ Error: {e}")
            conn.rollback()
    
    conn.close()
    
    print()
    print("=" * 60)
    print("RE-CAPTURE COMPLETE")
    print(f"Success: {success}")
    print(f"Skipped: {skipped}")
    print(f"Errors: {errors}")
    print("=" * 60)

if __name__ == '__main__':
    main()
