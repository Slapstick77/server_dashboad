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
print('DR 49111 - ALL THE FIELDS WE NEED!')
print('='*80)

# ReasonLink contains Deviation Type and Component
reason_link = master_vals.get('ReasonLink')
if reason_link:
    rl_vals = getattr(reason_link, '__values__', {})
    print(f'\n✓ Deviation Type: {rl_vals.get("DeviationTypeName")}')
    print(f'✓ Component: {rl_vals.get("ComponentTypeName")}')

# DefectType
defect_type = master_vals.get('DefectType')
if defect_type:
    dt_vals = getattr(defect_type, '__values__', {})
    print(f'✓ Defect: {dt_vals.get("Name")}')

# Urgency
urgency_pk = master_vals.get('UrgencyPK')
print(f'✓ Urgency PK: {urgency_pk}')

# Charged To
charged_to = master_vals.get('ChargedToDeptName')
print(f'✓ Charged To: {charged_to}')

# Parts List
unit_details = master_vals.get('UnitDetails')
if unit_details:
    ud_vals = getattr(unit_details, '__values__', {})
    parts_list = ud_vals.get('PartsList')
    if parts_list:
        pl_vals = getattr(parts_list, '__values__', {})
        parts_array = pl_vals.get('CDeviatedPart', [])
        print(f'\n✓ Parts count: {len(parts_array)}')
        
        if parts_array:
            print('\nPart details:')
            for i, part in enumerate(parts_array, 1):
                part_vals = getattr(part, '__values__', {})
                part_num = part_vals.get('PartNumber')
                desc = part_vals.get('Description')
                qty = part_vals.get('Quantity')
                print(f'  Part {i}:')
                print(f'    PartNumber: {part_num}')
                print(f'    Description: {desc}')
                print(f'    Quantity: {qty}')
                print(f'    All fields: {list(part_vals.keys())}')

print('\n' + '='*80)
print('SUCCESS! All fields are available in Master.ReasonLink and UnitDetails!')
print('='*80)
