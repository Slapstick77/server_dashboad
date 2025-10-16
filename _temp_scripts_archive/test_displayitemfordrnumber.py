from zeep import Client
import json
import os

# Load credentials
ARCH = os.path.join(os.path.dirname(__file__), 'download_archive')
with open(os.path.join(ARCH, 'dr_credentials.json')) as f:
    creds = json.load(f)

base_url = creds.get('base_url', 'http://192.168.111.55/MOMWebService')
user = creds['user']
pwd = creds['password']

wsdl = f'{base_url}/ManufacturingDeviationSystem.svc?singleWsdl'
client = Client(wsdl)

# Test DisplayItemForDRNumber for a specific DR to see ALL fields
test_dr = 49101

print(f"Testing DisplayItemForDRNumber for DR {test_dr}...")
print("=" * 70)

result = client.service.DisplayItemForDRNumber(
    applicationKey=user,
    password=pwd,
    devNumber=test_dr
)

# Convert to dict and show structure
result_dict = {
    key: getattr(result, key)
    for key in dir(result)
    if not key.startswith('_')
}

print(f"\nAll fields returned by DisplayItemForDRNumber:")
for key in sorted(result_dict.keys()):
    value = result_dict[key]
    if value is None:
        print(f"  {key}: None")
    elif isinstance(value, (str, int, float, bool)):
        val_str = str(value)[:60]
        print(f"  {key}: {val_str}")
    elif hasattr(value, '__dict__'):
        print(f"  {key}: (object)")
        obj_dict = {k: getattr(value, k) for k in dir(value) if not k.startswith('_')}
        for sub_key in sorted(obj_dict.keys()):
            sub_val = obj_dict[sub_key]
            if isinstance(sub_val, list):
                print(f"    .{sub_key}: (list with {len(sub_val)} items)")
            else:
                print(f"    .{sub_key}: {str(sub_val)[:60]}")
    elif isinstance(value, list):
        print(f"  {key}: (list with {len(value)} items)")
        if value and hasattr(value[0], '__dict__'):
            print(f"    First item fields: {[k for k in dir(value[0]) if not k.startswith('_')]}")
    else:
        print(f"  {key}: {type(value)}")
