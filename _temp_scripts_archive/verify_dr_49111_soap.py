from zeep import Client
from zeep.transports import Transport
from zeep.cache import InMemoryCache
from datetime import datetime, timedelta, timezone
import json

user = '01962'
pwd = '01962'
wsdl = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc?wsdl'

client = Client(wsdl=wsdl, transport=Transport(cache=InMemoryCache(), timeout=30))

# Login (multiple methods like the working poller)
try:
    client.service.RegisterSession(username=user, password=pwd)
except Exception:
    pass
try:
    client.service.MOM_Login(username=user, password=pwd)
except Exception:
    pass
try:
    dev_user = client.service.GetUser(username=user, password=pwd)
except Exception as e:
    print(f'Failed to login: {e}')
    exit(1)

print(f'✓ Logged in successfully')

# Get DRs from the last 7 days to catch DR 49111
now = datetime.now(timezone.utc)
end_dt = now
start_dt = (end_dt - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)

print(f'Fetching DRs from {start_dt.date()} to {end_dt.date()}...')

items = client.service.DisplayItemForDateRange(
    devUser=dev_user,
    fromDate=start_dt,
    toDate=end_dt,
    closed=True  # Include closed DRs
)

print(f'✓ Retrieved {len(items)} DRs')

# Find DR 49111
target_dr = None
for item in items:
    item_vals = getattr(item, '__values__', {})
    dn = item_vals.get('DeviationNumber')
    if dn == 49111:
        target_dr = item
        break

if not target_dr:
    print(f'\n❌ DR 49111 NOT FOUND in the response!')
    print(f'   DRs found: {[getattr(item, "__values__", {}).get("DeviationNumber") for item in items[:10]]}...')
    exit(1)

print(f'\n✓ FOUND DR 49111!')
print('='*80)

# Show ALL fields at the item level
item_vals = getattr(target_dr, '__values__', {})
print('\nALL ITEM-LEVEL FIELDS for DR 49111:')
print('-'*80)
for k in sorted(item_vals.keys()):
    v = item_vals[k]
    if v is None:
        print(f'  {k}: None')
    elif isinstance(v, (str, int, float, bool)):
        val_str = str(v)[:80]
        print(f'  {k}: {val_str}')
    elif hasattr(v, '__values__'):
        obj_vals = getattr(v, '__values__', {})
        print(f'  {k}: (object with {len(obj_vals)} fields)')
    elif isinstance(v, list):
        print(f'  {k}: (list with {len(v)} items)')
    else:
        print(f'  {k}: {type(v).__name__}')

# Deep dive into Master
print('\n' + '='*80)
print('MASTER OBJECT COMPLETE INSPECTION:')
print('='*80)
master = item_vals.get('Master')
if master:
    master_vals = getattr(master, '__values__', {})
    print(f'\nMaster has {len(master_vals)} fields:')
    for k in sorted(master_vals.keys()):
        v = master_vals[k]
        if v is None:
            print(f'  {k}: None')
        elif isinstance(v, (str, int, float, bool)):
            print(f'  {k}: {v}')
        elif hasattr(v, '__values__'):
            obj_vals = getattr(v, '__values__', {})
            print(f'  {k}: (object)')
            for sub_k, sub_v in sorted(obj_vals.items()):
                if isinstance(sub_v, (str, int, float, bool)):
                    print(f'    .{sub_k}: {sub_v}')
                elif sub_v is None:
                    print(f'    .{sub_k}: None')
                elif isinstance(sub_v, list):
                    print(f'    .{sub_k}: (list with {len(sub_v)} items)')
                else:
                    print(f'    .{sub_k}: {type(sub_v).__name__}')
        elif isinstance(v, list):
            print(f'  {k}: (list with {len(v)} items)')
            if v and len(v) > 0:
                first = v[0]
                if hasattr(first, '__values__'):
                    first_vals = getattr(first, '__values__', {})
                    print(f'    First item fields: {list(first_vals.keys())}')
        else:
            print(f'  {k}: {type(v).__name__}')
    
    # Check UnitDetails specifically
    print('\n' + '-'*80)
    print('UnitDetails DEEP DIVE:')
    print('-'*80)
    unit_details = master_vals.get('UnitDetails')
    if unit_details:
        ud_vals = getattr(unit_details, '__values__', {})
        print(f'UnitDetails has {len(ud_vals)} fields:')
        for k, v in sorted(ud_vals.items()):
            if v is None:
                print(f'  {k}: None')
            elif isinstance(v, (str, int, float, bool)):
                print(f'  {k}: {v}')
            elif hasattr(v, '__values__'):
                obj_vals = getattr(v, '__values__', {})
                print(f'  {k}: (object with {len(obj_vals)} fields)')
                for sub_k in sorted(obj_vals.keys()):
                    print(f'    .{sub_k}')
            elif isinstance(v, list):
                print(f'  {k}: (list with {len(v)} items)')
            else:
                print(f'  {k}: {type(v).__name__}')
    else:
        print('  UnitDetails: None or missing')
else:
    print('Master is None!')

print('\n' + '='*80)
print('LOOKING FOR: Deviation Type, Component, PartsList')
print('='*80)
