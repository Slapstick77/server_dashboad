from zeep import Client
from zeep.transports import Transport
from zeep.cache import InMemoryCache
from zeep.helpers import serialize_object
from datetime import datetime, timedelta, timezone
import json

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
unit_details = master_vals.get('UnitDetails')
ud_vals = getattr(unit_details, '__values__', {})
unit_details_pk = ud_vals.get('PK')

print('='*80)
print(f'DR 49111 - Calling PartsForUnitDetails')
print('='*80)
print(f'\nUnitDetails PK: {unit_details_pk}')

# Call PartsForUnitDetails with the UnitDetails object itself
try:
    parts_result = client.service.PartsForUnitDetails(
        unitDetails=unit_details,
        devUser=dev_user
    )
    
    print(f'\nPartsForUnitDetails returned: {type(parts_result)}')
    
    # Serialize it
    serialized = serialize_object(parts_result)
    print(f'\nSerialized result:')
    print(json.dumps(serialized, indent=2, default=str))
    
    # Look for part number
    if '87674' in str(serialized):
        print('\n*** FOUND PART NUMBER 87674! ***')
    
except Exception as e:
    print(f'\nError calling PartsForUnitDetails: {e}')

print('\n' + '='*80)
