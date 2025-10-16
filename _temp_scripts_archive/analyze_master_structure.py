import json

# Load the JSON and examine Master structure
with open('download_archive/dr_incremental.json', 'r') as f:
    data = json.load(f)

print("Examining Master object structure from recent poll...")
print("=" * 70)

# Get a few samples and show ALL fields in Master
for i, item in enumerate(data[:5]):
    if '_Master' in item:
        print(f"\nDR {item['DeviationNumber']} Master fields:")
        master = item['_Master']
        for key, value in master.items():
            if isinstance(value, dict):
                print(f"  {key}: {value}")
            elif isinstance(value, list):
                print(f"  {key}: (list with {len(value)} items)")
                if value:
                    print(f"    First item: {value[0]}")
            else:
                print(f"  {key}: {value}")

# Check for Component/DeviationType fields
print("\n" + "=" * 70)
print("Searching for deviation_type/component/part fields...")
print("=" * 70)

# Count how many have various fields
stats = {
    'total': len(data),
    'has_master': 0,
    'has_component': 0,
    'has_deviation_type': 0,
    'has_unit_details': 0,
    'has_parts_list': 0,
    'parts_list_not_empty': 0
}

for item in data:
    if '_Master' in item:
        stats['has_master'] += 1
        master = item['_Master']
        
        # Check for all possible field names
        if 'Component' in master:
            stats['has_component'] += 1
        if 'DeviationType' in master:
            stats['has_deviation_type'] += 1
        if 'UnitDetails' in master:
            stats['has_unit_details'] += 1
            unit_details = master['UnitDetails']
            if 'PartsList' in unit_details:
                stats['has_parts_list'] += 1
                if unit_details['PartsList']:
                    stats['parts_list_not_empty'] += 1

print(f"Total DRs: {stats['total']}")
print(f"DRs with _Master: {stats['has_master']}")
print(f"DRs with Component: {stats['has_component']}")
print(f"DRs with DeviationType: {stats['has_deviation_type']}")
print(f"DRs with UnitDetails: {stats['has_unit_details']}")
print(f"DRs with PartsList: {stats['has_parts_list']}")
print(f"DRs with non-empty PartsList: {stats['parts_list_not_empty']}")

# Show a sample with PartsList
print("\n" + "=" * 70)
print("Sample DRs with PartsList:")
print("=" * 70)
count = 0
for item in data:
    if '_Master' in item and 'UnitDetails' in item['_Master']:
        unit_details = item['_Master']['UnitDetails']
        if 'PartsList' in unit_details and unit_details['PartsList']:
            print(f"\nDR {item['DeviationNumber']}: {len(unit_details['PartsList'])} parts")
            for part in unit_details['PartsList'][:3]:  # Show first 3
                print(f"  Part: {part}")
            count += 1
            if count >= 3:
                break
