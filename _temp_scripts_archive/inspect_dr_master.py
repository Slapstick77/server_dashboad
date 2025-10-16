from zeep import Client
from zeep.transports import Transport
from zeep.cache import InMemoryCache

user = '01962'
pwd = '01962'
wsdl = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc?wsdl'

client = Client(wsdl=wsdl, transport=Transport(cache=InMemoryCache(), timeout=30))
client.service.RegisterSession(username=user, password=pwd)
dev_user = client.service.GetUser(username=user, password=pwd)

# Get DR 49051 from your screenshot
result = client.service.DisplayItemForDRNumber(devUser=dev_user, drNumber='49051')
master = result.Master
master_vals = getattr(master, '__values__', {})

print('='*80)
print('MASTER OBJECT FIELDS FOR DR 49051')
print('='*80)
print(f'\nTotal fields in Master: {len(master_vals)}')
print('\nAll keys:')
for k in sorted(master_vals.keys()):
    print(f'  {k}')

print('\n' + '='*80)
print('SEARCHING FOR: urgency, component, deviation, type')
print('='*80)
for k, v in master_vals.items():
    kl = k.lower()
    if any(word in kl for word in ['urgency', 'component', 'deviation', 'type']):
        print(f'{k}: {v}')

print('\n' + '='*80)
print('FULL MASTER DUMP (first 100 chars per field):')
print('='*80)
for k, v in sorted(master_vals.items()):
    v_str = str(v)[:100] if not hasattr(v, '__values__') else f'<nested {type(v).__name__}>'
    print(f'{k:30} = {v_str}')
