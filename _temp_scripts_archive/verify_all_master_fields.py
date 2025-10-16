from zeep import Client
from zeep.transports import Transport
from zeep.cache import InMemoryCache

user = '01962'
pwd = '01962'
wsdl = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc?wsdl'

client = Client(wsdl=wsdl, transport=Transport(cache=InMemoryCache(), timeout=30))
client.service.RegisterSession(username=user, password=pwd)
dev_user = client.service.GetUser(username=user, password=pwd)

# Test multiple DRs to find one with parts
test_drs = [49101, 49102, 49103, 49104, 49105, 49087, 49088, 49052]

print('='*80)
print('Searching for DRs with PartsList and checking for Component/DeviationType')
print('='*80)

for dr_num in test_drs:
    try:
        result = client.service.DisplayItemForDRNumber(devUser=dev_user, drNumber=str(dr_num))
        master = result.Master
        master_vals = getattr(master, '__values__', {})
        
        # Check for Component and DeviationType fields
        has_component = 'Component' in master_vals
        has_deviation_type = 'DeviationType' in master_vals
        
        component_val = master_vals.get('Component')
        deviation_type_val = master_vals.get('DeviationType')
        
        # Check UnitDetails
        unit_details = master_vals.get('UnitDetails')
        parts_count = 0
        if unit_details:
            ud_vals = getattr(unit_details, '__values__', {})
            
            # Show ALL UnitDetails fields for first DR
            if dr_num == test_drs[0]:
                print(f'\nUnitDetails fields for DR {dr_num}:')
                for k in sorted(ud_vals.keys()):
                    print(f'  {k}')
            
            parts_list = ud_vals.get('PartsList')
            if parts_list:
                parts_vals = getattr(parts_list, '__values__', {})
                parts_array = parts_vals.get('CDeviatedPart', [])
                parts_count = len(parts_array)
                
                if parts_count > 0:
                    print(f'\n✓ DR {dr_num}: Has {parts_count} parts!')
                    # Show first part detail
                    first_part = parts_array[0]
                    part_vals = getattr(first_part, '__values__', {})
                    print(f'  Part fields: {sorted(part_vals.keys())}')
                    print(f'  Sample part: {part_vals}')
        
        print(f'\nDR {dr_num}:')
        print(f'  Component field exists: {has_component}, value: {component_val}')
        print(f'  DeviationType field exists: {has_deviation_type}, value: {deviation_type_val}')
        print(f'  Parts count: {parts_count}')
        
        if parts_count > 0:
            break  # Found one with parts
            
    except Exception as e:
        print(f'Error checking DR {dr_num}: {e}')

print('\n' + '='*80)
print('SUMMARY: What fields are available?')
print('='*80)
