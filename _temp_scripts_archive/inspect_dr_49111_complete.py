from zeep import Client
from zeep.transports import Transport
from zeep.cache import InMemoryCache
import json

user = '01962'
pwd = '01962'
wsdl = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc?wsdl'

client = Client(wsdl=wsdl, transport=Transport(cache=InMemoryCache(), timeout=30))
client.service.RegisterSession(username=user, password=pwd)
dev_user = client.service.GetUser(username=user, password=pwd)

# Get DR 49111 which we KNOW has Deviation Type, Component, and Part Number
dr_num = 49111

print('='*80)
print(f'COMPLETE INSPECTION OF DR {dr_num}')
print('='*80)

result = client.service.DisplayItemForDRNumber(devUser=dev_user, drNumber=str(dr_num))
master = result.Master
master_vals = getattr(master, '__values__', {})

print('\nALL Master fields and values:')
print('-'*80)
for k in sorted(master_vals.keys()):
    v = master_vals[k]
    if v is None:
        print(f'  {k}: None')
    elif isinstance(v, (str, int, float, bool)):
        print(f'  {k}: {v}')
    elif hasattr(v, '__values__'):
        obj_vals = getattr(v, '__values__', {})
        print(f'  {k}: (object) {obj_vals}')
    elif isinstance(v, list):
        print(f'  {k}: (list with {len(v)} items)')
    else:
        print(f'  {k}: {type(v).__name__}')

# Deep dive into UnitDetails
print('\n' + '='*80)
print('UnitDetails DEEP DIVE:')
print('='*80)
unit_details = master_vals.get('UnitDetails')
if unit_details:
    ud_vals = getattr(unit_details, '__values__', {})
    print('\nAll UnitDetails fields:')
    for k in sorted(ud_vals.keys()):
        v = ud_vals[k]
        if v is None:
            print(f'  {k}: None')
        elif isinstance(v, (str, int, float, bool)):
            print(f'  {k}: {v}')
        elif hasattr(v, '__values__'):
            obj_vals = getattr(v, '__values__', {})
            print(f'  {k}: (object)')
            for sub_k, sub_v in obj_vals.items():
                print(f'    .{sub_k}: {sub_v}')
        elif isinstance(v, list):
            print(f'  {k}: (list with {len(v)} items)')
            if len(v) > 0:
                print(f'    First item: {v[0]}')
        else:
            print(f'  {k}: {type(v).__name__}')
    
    # Check PartsList specifically
    parts_list = ud_vals.get('PartsList')
    if parts_list:
        parts_vals = getattr(parts_list, '__values__', {})
        print(f'\n  PartsList structure: {parts_vals.keys()}')
        
        parts_array = parts_vals.get('CDeviatedPart', [])
        print(f'  PartsList has {len(parts_array)} parts')
        
        if parts_array:
            print('\n  FIRST PART DETAILS:')
            first_part = parts_array[0]
            part_vals = getattr(first_part, '__values__', {})
            for pk, pv in sorted(part_vals.items()):
                print(f'    {pk}: {pv}')

# Check if ReasonLink might contain deviation type
print('\n' + '='*80)
print('ReasonLink (might be deviation type):')
print('='*80)
reason_link = master_vals.get('ReasonLink')
if reason_link:
    rl_vals = getattr(reason_link, '__values__', {})
    print(f'ReasonLink fields: {rl_vals}')
else:
    print('ReasonLink is None/empty')

# Check RootCause
root_cause = master_vals.get('RootCause')
if root_cause:
    rc_vals = getattr(root_cause, '__values__', {})
    print(f'\nRootCause fields: {rc_vals}')

print('\n' + '='*80)
print('LOOKING FOR: Deviation Type="Manufactured", Component="Reconnect"')
print('='*80)
