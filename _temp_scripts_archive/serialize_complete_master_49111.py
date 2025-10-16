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
print('DR 49111 - COMPLETE SERIALIZATION OF MASTER OBJECT')
print('='*80)

item_vals = getattr(target_dr, '__values__', {})
master = item_vals.get('Master')

# Serialize the entire Master object to see EVERYTHING
serialized = serialize_object(master)

# Save to file for inspection
with open('dr_49111_master_complete.json', 'w', encoding='utf-8') as f:
    json.dump(serialized, f, indent=2, default=str)

print('\nSaved complete Master object to: dr_49111_master_complete.json')

# Also print to screen
print('\nCOMPLETE MASTER OBJECT:')
print(json.dumps(serialized, indent=2, default=str))

# Look specifically for any field containing "87674" or "part"
print('\n' + '='*80)
print('SEARCHING FOR PART NUMBER 87674-21-248:')
print('='*80)

def search_dict(d, search_terms, path=''):
    """Recursively search for terms in dict"""
    results = []
    if isinstance(d, dict):
        for k, v in d.items():
            current_path = f'{path}.{k}' if path else k
            # Check if key contains search term
            if any(term.lower() in str(k).lower() for term in search_terms):
                results.append((current_path, v))
            # Check if value contains search term
            if isinstance(v, str) and any(term in v for term in search_terms):
                results.append((current_path, v))
            # Recurse
            results.extend(search_dict(v, search_terms, current_path))
    elif isinstance(d, list):
        for i, item in enumerate(d):
            results.extend(search_dict(item, search_terms, f'{path}[{i}]'))
    return results

search_terms = ['87674', 'part', 'Part']
matches = search_dict(serialized, search_terms)

if matches:
    print(f'\nFound {len(matches)} matches:')
    for path, value in matches:
        val_str = str(value)[:100] if not isinstance(value, (dict, list)) else f'<{type(value).__name__}>'
        print(f'  {path}: {val_str}')
else:
    print('\nNo matches found for part number!')

print('\n' + '='*80)
