from zeep import Client
from zeep.transports import Transport
from zeep.cache import InMemoryCache

user = '01962'
pwd = '01962'
wsdl = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc?wsdl'

client = Client(wsdl=wsdl, transport=Transport(cache=InMemoryCache(), timeout=30))

print('='*80)
print('ALL AVAILABLE SOAP METHODS:')
print('='*80)

# Get all service operations
for service in client.wsdl.services.values():
    print(f'\nService: {service.name}')
    for port in service.ports.values():
        operations = sorted(port.binding._operations.keys())
        for op_name in operations:
            print(f'  - {op_name}')

print('\n' + '='*80)
print('Looking for methods that might return Part information...')
print('='*80)

part_methods = [op for service in client.wsdl.services.values() 
                for port in service.ports.values() 
                for op in port.binding._operations.keys() 
                if 'part' in op.lower()]

if part_methods:
    print(f'\nMethods with "part" in name:')
    for method in part_methods:
        print(f'  - {method}')
else:
    print('\nNo methods with "part" in the name')

# Check for methods with "detail" or "complete" or "full"
detail_methods = [op for service in client.wsdl.services.values() 
                  for port in service.ports.values() 
                  for op in port.binding._operations.keys() 
                  if any(word in op.lower() for word in ['detail', 'complete', 'full', 'extended'])]

if detail_methods:
    print(f'\nMethods with detail/complete/full:')
    for method in detail_methods:
        print(f'  - {method}')

print('\n' + '='*80)
