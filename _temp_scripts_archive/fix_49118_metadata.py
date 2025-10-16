"""Fix missing metadata for DRs 49118, 49119, 49120"""
import sqlite3
import json
import os
from datetime import datetime, timezone
from zeep import Client
from zeep.transports import Transport
from zeep.cache import InMemoryCache

conn = sqlite3.connect('SCHLabor.db')
cursor = conn.cursor()

# Load the poll data
data = json.load(open('download_archive/dr_incremental.json'))

# Get credentials
CRED_PATH = os.path.join('download_archive', 'dr_credentials.json')
cred_data = json.load(open(CRED_PATH, 'r', encoding='utf-8'))
user = cred_data.get('user', '').strip()
pwd = cred_data.get('password', '').strip()

# Get urgency map from SOAP (same way poller does it)
base_url = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc'
wsdl = base_url + '?wsdl'
client = Client(wsdl=wsdl, transport=Transport(cache=InMemoryCache(), timeout=30))

# Login
dev_user = None
try:
    dev_user = client.service.GetUser(username=user, password=pwd)
except Exception as e:
    print(f"Login failed: {e}")

# Build urgency map from SOAP
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
    print(f'Loaded {len(urgency_map)} urgency options from SOAP')
except Exception as e:
    print(f'GetUrgencyList failed: {e}')
    raise

# Process DRs 49118, 49119, 49120
target_drs = [49118, 49119, 49120]
fixed_count = 0

for dr_num in target_drs:
    # Find DR in data
    dr_list = [d for d in data if d['DeviationNumber'] == dr_num]
    if not dr_list:
        print(f"⚠️  DR {dr_num} not found in poll data")
        continue
    
    dr = dr_list[0]
    
    # Check if already has metadata
    cursor.execute("SELECT 1 FROM DRStaticMetadata WHERE deviation_number = ?", (dr_num,))
    if cursor.fetchone():
        print(f"⚠️  DR {dr_num} already has metadata, skipping")
        continue
    
    # Extract metadata
    master = dr.get('_Master')
    if not master:
        print(f"⚠️  DR {dr_num} has no Master object")
        continue
    
    urg_pk = master.get('UrgencyPK')
    urgency = urgency_map.get(urg_pk)
    defect_obj = master.get('DefectType', {})
    defect = defect_obj.get('Name') if defect_obj else None
    charged_to = master.get('ChargedToDeptName')
    reason_link = master.get('ReasonLink', {})
    deviation_type = reason_link.get('DeviationTypeName') if reason_link else None
    component = reason_link.get('ComponentTypeName') if reason_link else None
    date_created = master.get('DateCreated')
    
    # Insert metadata
    cursor.execute("""
        INSERT OR IGNORE INTO DRStaticMetadata (
            deviation_number, urgency, defect_description, charged_to_dept,
            deviation_type, component, date_created, user_created,
            first_captured_run_id, first_captured_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        dr_num,
        urgency,
        defect,
        charged_to,
        deviation_type,
        component,
        date_created,
        None,
        10,
        datetime.now(timezone.utc).isoformat()
    ))
    
    print(f"✓ DR {dr_num}")
    print(f"  Urgency: {urgency}")
    print(f"  Defect: {defect}")
    print(f"  Deviation Type: {deviation_type}")
    print(f"  Component: {component}")
    fixed_count += 1

conn.commit()
print()
print(f"Fixed {fixed_count} DRs")
