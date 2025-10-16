from zeep import Client
from zeep.transports import Transport
from zeep.cache import InMemoryCache

user = '01962'
pwd = '01962'
wsdl = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc?wsdl'

client = Client(wsdl=wsdl, transport=Transport(cache=InMemoryCache(), timeout=30))
client.service.RegisterSession(username=user, password=pwd)
dev_user = client.service.GetUser(username=user, password=pwd)

# Get DR 49111 - we KNOW it has Deviation Type, Component, Part Number
dr_num = 49111
print('='*80)
print(f'DR {dr_num} - RESULT LEVEL FIELDS (NOT Master)')
print('='*80)

result = client.service.DisplayItemForDRNumber(devUser=dev_user, drNumber=str(dr_num))

# Show ALL result-level fields
result_vals = getattr(result, '__values__', {})
print('\nAll Result fields:')
for k in sorted(result_vals.keys()):
    v = result_vals[k]
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

# Check ChargedTo - might have deviation type or component info
print('\n' + '='*80)
print('ChargedTo detail:')
print('='*80)
charged_to = result_vals.get('ChargedTo')
if charged_to:
    ct_vals = getattr(charged_to, '__values__', {})
    for k, v in ct_vals.items():
        print(f'  {k}: {v}')

# Check DefectType
print('\n' + '='*80)
print('DefectType detail:')
print('='*80)
defect_type = result_vals.get('DefectType')
if defect_type:
    dt_vals = getattr(defect_type, '__values__', {})
    for k, v in dt_vals.items():
        print(f'  {k}: {v}')

# Check MfgLocation - might have component or deviation type
print('\n' + '='*80)
print('MfgLocation detail:')
print('='*80)
mfg_loc = result_vals.get('MfgLocation')
if mfg_loc:
    ml_vals = getattr(mfg_loc, '__values__', {})
    for k, v in ml_vals.items():
        print(f'  {k}: {v}')

# Check Product
print('\n' + '='*80)
print('Product detail:')
print('='*80)
product = result_vals.get('Product')
if product:
    p_vals = getattr(product, '__values__', {})
    for k, v in p_vals.items():
        print(f'  {k}: {v}')

print('\n' + '='*80)
print('Still looking for: Deviation Type, Component, PartsList...')
print('='*80)
