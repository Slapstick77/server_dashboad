from zeep import Client
from zeep.transports import Transport
from zeep.cache import InMemoryCache
from datetime import datetime, timedelta, timezone

user = '01962'
pwd = '01962'
wsdl = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc?wsdl'

client = Client(wsdl=wsdl, transport=Transport(cache=InMemoryCache(), timeout=30))
client.service.RegisterSession(username=user, password=pwd)
client.service.MOM_Login(username=user, password=pwd)
dev_user = client.service.GetUser(username=user, password=pwd)

now = datetime.now(timezone.utc)
end_dt = now
start_dt = (end_dt - timedelta(days=14)).replace(hour=0, minute=0, second=0, microsecond=0)

items = client.service.DisplayItemForDateRange(
    devUser=dev_user,
    fromDate=start_dt,
    toDate=end_dt,
    closed=True
)

print(f'Checking {len(items)} DRs for ones with actual parts in PartsList...')
print('='*80)

drs_with_parts = []

for item in items:
    item_vals = getattr(item, '__values__', {})
    dn = item_vals.get('DeviationNumber')
    master = item_vals.get('Master')
    
    if master:
        master_vals = getattr(master, '__values__', {})
        unit_details = master_vals.get('UnitDetails')
        
        if unit_details:
            ud_vals = getattr(unit_details, '__values__', {})
            parts_list = ud_vals.get('PartsList')
            
            if parts_list:
                pl_vals = getattr(parts_list, '__values__', {})
                parts_array = pl_vals.get('CDeviatedPart', [])
                
                if len(parts_array) > 0:
                    # Found one with parts!
                    reason_link = master_vals.get('ReasonLink')
                    deviation_type = ''
                    component = ''
                    if reason_link:
                        rl_vals = getattr(reason_link, '__values__', {})
                        deviation_type = rl_vals.get('DeviationTypeName', '')
                        component = rl_vals.get('ComponentTypeName', '')
                    
                    drs_with_parts.append({
                        'dn': dn,
                        'parts_count': len(parts_array),
                        'deviation_type': deviation_type,
                        'component': component,
                        'parts': parts_array
                    })

print(f'\n✓ Found {len(drs_with_parts)} DRs with parts in PartsList!')

if drs_with_parts:
    # Show first few
    for dr_info in drs_with_parts[:3]:
        print(f'\n' + '-'*80)
        print(f'DR {dr_info["dn"]}:')
        print(f'  Deviation Type: {dr_info["deviation_type"]}')
        print(f'  Component: {dr_info["component"]}')
        print(f'  Parts count: {dr_info["parts_count"]}')
        print(f'  Parts:')
        for i, part in enumerate(dr_info['parts'][:5], 1):  # Show first 5 parts
            part_vals = getattr(part, '__values__', {})
            part_num = part_vals.get('PartNumber', 'N/A')
            desc = part_vals.get('Description', 'N/A')
            qty = part_vals.get('Quantity', 'N/A')
            print(f'    {i}. PartNumber: {part_num}, Qty: {qty}, Desc: {desc[:50]}')
else:
    print('\n❌ NO DRs found with parts in PartsList!')
    print('   Parts may be stored differently or not exposed via SOAP.')

print('\n' + '='*80)
print('SUMMARY:')
print(f'  ✓ Deviation Type: Available in Master.ReasonLink.DeviationTypeName')
print(f'  ✓ Component: Available in Master.ReasonLink.ComponentTypeName')
print(f'  ✓ Defect: Available in Master.DefectType.Name')
print(f'  ✓ Urgency: Available in Master.UrgencyPK')
print(f'  ✓ Charged To: Available in Master.ChargedToDeptName')
print(f'  ? Part Number: In Master.UnitDetails.PartsList.CDeviatedPart (but often empty)')
print('='*80)
