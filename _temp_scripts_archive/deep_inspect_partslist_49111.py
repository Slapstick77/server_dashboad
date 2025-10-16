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

# Get DR 49111
now = datetime.now(timezone.utc)
end_dt = now
start_dt = (end_dt - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)

items = client.service.DisplayItemForDateRange(
    devUser=dev_user,
    fromDate=start_dt,
    toDate=end_dt,
    closed=True
)

# Find DR 49111
target_dr = None
for item in items:
    item_vals = getattr(item, '__values__', {})
    if item_vals.get('DeviationNumber') == 49111:
        target_dr = item
        break

item_vals = getattr(target_dr, '__values__', {})
master = item_vals.get('Master')
master_vals = getattr(master, '__values__', {})

print('='*80)
print('DR 49111 - DETAILED PartsList INSPECTION')
print('='*80)

unit_details = master_vals.get('UnitDetails')
if unit_details:
    ud_vals = getattr(unit_details, '__values__', {})
    parts_list = ud_vals.get('PartsList')
    
    print(f'\nPartsList type: {type(parts_list)}')
    print(f'PartsList value: {parts_list}')
    print(f'PartsList dir: {[x for x in dir(parts_list) if not x.startswith("_")]}')
    
    # Try to iterate directly
    if parts_list:
        print(f'\nTrying to iterate PartsList directly...')
        try:
            for i, part in enumerate(parts_list):
                print(f'\n  Part {i+1}:')
                print(f'    Type: {type(part)}')
                part_vals = getattr(part, '__values__', {})
                for k, v in part_vals.items():
                    print(f'    {k}: {v}')
        except Exception as e:
            print(f'  Error iterating: {e}')
        
        # Try accessing as attribute
        print(f'\nTrying __values__ on PartsList...')
        try:
            pl_vals = getattr(parts_list, '__values__', {})
            print(f'  PartsList __values__: {pl_vals}')
            
            # Try different array field names
            for possible_name in ['CDeviatedPart', 'DeviatedPart', 'Part', 'Parts', 'Item', 'Items']:
                if possible_name in pl_vals:
                    array = pl_vals[possible_name]
                    print(f'\n  Found array under "{possible_name}": {len(array)} items')
                    if array:
                        first = array[0]
                        first_vals = getattr(first, '__values__', {})
                        print(f'  First part: {first_vals}')
        except Exception as e:
            print(f'  Error: {e}')
        
        # Try to convert to dict
        print(f'\nTrying to serialize PartsList...')
        try:
            from zeep.helpers import serialize_object
            serialized = serialize_object(parts_list)
            print(f'  Serialized: {serialized}')
        except Exception as e:
            print(f'  Error: {e}')

print('\n' + '='*80)
