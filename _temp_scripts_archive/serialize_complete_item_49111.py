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

print('='*80)
print('DR 49111 - COMPLETE ITEM SERIALIZATION (NOT JUST MASTER)')
print('='*80)

# Serialize the ENTIRE ITEM (not just Master)
serialized = serialize_object(target_dr)

# Save to file
with open('dr_49111_complete_item.json', 'w', encoding='utf-8') as f:
    json.dump(serialized, f, indent=2, default=str)

print('\nSaved complete Item to: dr_49111_complete_item.json')

# Search for part number
def search_for_text(d, search_text, path=''):
    results = []
    if isinstance(d, dict):
        for k, v in d.items():
            current_path = f'{path}.{k}' if path else k
            if isinstance(v, str) and search_text in v:
                results.append((current_path, v))
            results.extend(search_for_text(v, search_text, current_path))
    elif isinstance(d, list):
        for i, item in enumerate(d):
            results.extend(search_for_text(item, search_text, f'{path}[{i}]'))
    return results

print('\nSearching for "87674":')
matches = search_for_text(serialized, '87674')
if matches:
    for path, value in matches:
        print(f'  {path}:')
        print(f'    {value[:200]}...' if len(value) > 200 else f'    {value}')
else:
    print('  NOT FOUND in Item object!')

print('\n' + '='*80)
