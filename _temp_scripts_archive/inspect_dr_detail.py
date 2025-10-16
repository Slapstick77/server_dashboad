from zeep import Client
from zeep.transports import Transport
from zeep.cache import InMemoryCache

user = '01962'
pwd = '01962'
wsdl = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc?wsdl'

client = Client(wsdl=wsdl, transport=Transport(cache=InMemoryCache(), timeout=30))
client.service.RegisterSession(username=user, password=pwd)
dev_user = client.service.GetUser(username=user, password=pwd)

# Get detail for DR 49051
result = client.service.DisplayItemForDRNumber(devUser=dev_user, drNumber='49051')
master = result.Master
master_vals = getattr(master, '__values__', {})

print('='*80)
print('Master object ALL keys:')
print('='*80)
for k in sorted(master_vals.keys()):
    print(f'  {k}')

print('\n' + '='*80)
print('Urgency and Defect detail:')
print('='*80)

# Get urgency lookup table
urgency_list = client.service.GetUrgencyList(devUser=dev_user)
urgency_map = {}
if urgency_list:
    print('Urgency list first item fields:')
    if len(urgency_list) > 0:
        first_u = urgency_list[0]
        u_vals = getattr(first_u, '__values__', {})
        for k, v in u_vals.items():
            print(f'  {k}: {v}')
    
    for u in urgency_list:
        u_vals = getattr(u, '__values__', {})
        pk = u_vals.get('PK')
        # Try different field names
        name = u_vals.get('Name') or u_vals.get('Description') or u_vals.get('Text') or u_vals.get('Value')
        urgency_map[pk] = name

urgency_pk = master_vals.get('UrgencyPK')
print(f'\nThis DR Urgency PK: {urgency_pk}')
print(f'This DR Urgency Name: {urgency_map.get(urgency_pk, "NOT FOUND")}')

defect_type_obj = master_vals.get('DefectType')
if defect_type_obj:
    dt_vals = getattr(defect_type_obj, '__values__', {})
    print(f'\nDefect Name: {dt_vals.get("Name")}')

# Check UnitDetails for part number
unit_details = master_vals.get('UnitDetails')
if unit_details:
    ud_vals = getattr(unit_details, '__values__', {})
    parts_list = ud_vals.get('PartsList')
    if parts_list:
        parts_vals = getattr(parts_list, '__values__', {})
        parts_array = parts_vals.get('CDeviatedPart', [])
        print(f'\nParts count: {len(parts_array)}')
        if parts_array:
            print('First part fields:')
            first_part = parts_array[0]
            part_vals = getattr(first_part, '__values__', {})
            for k, v in part_vals.items():
                print(f'  {k}: {v}')
        else:
            print('No parts in PartsList for this DR')
    else:
        print('\nNo PartsList in UnitDetails')
