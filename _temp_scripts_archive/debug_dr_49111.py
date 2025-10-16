from zeep import Client
from zeep.transports import Transport
from zeep.cache import InMemoryCache

user = '01962'
pwd = '01962'
wsdl = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc?wsdl'

client = Client(wsdl=wsdl, transport=Transport(cache=InMemoryCache(), timeout=30))

# Login
client.service.RegisterSession(username=user, password=pwd)
dev_user = client.service.GetUser(username=user, password=pwd)

print(f'Logged in as: {dev_user}')
print(f'dev_user type: {type(dev_user)}')

# Get DR 49111
dr_num = 49111
print(f'\nFetching DR {dr_num}...')

result = client.service.DisplayItemForDRNumber(devUser=dev_user, drNumber=str(dr_num))

print(f'Result type: {type(result)}')
print(f'Result dir: {[x for x in dir(result) if not x.startswith("_")]}')

if hasattr(result, 'Master'):
    master = result.Master
    print(f'\nMaster type: {type(master)}')
    print(f'Master is None: {master is None}')
    
    if master is not None:
        master_vals = getattr(master, '__values__', {})
        print(f'Master __values__ keys: {list(master_vals.keys())}')
        print(f'\nMaster fields from dir(): {[x for x in dir(master) if not x.startswith("_")]}')
        
        # Try accessing fields directly
        print('\nTrying direct field access:')
        try:
            print(f'  DeviationNumber: {getattr(master, "DeviationNumber", "NOT FOUND")}')
            print(f'  DefectType: {getattr(master, "DefectType", "NOT FOUND")}')
            print(f'  UnitDetails: {getattr(master, "UnitDetails", "NOT FOUND")}')
            print(f'  ReasonLink: {getattr(master, "ReasonLink", "NOT FOUND")}')
        except Exception as e:
            print(f'  Error: {e}')
else:
    print('No Master attribute in result!')
    print(f'Result attributes: {[x for x in dir(result) if not x.startswith("_")]}')
