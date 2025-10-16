# Getting Part Numbers for DRs

## Overview
Part numbers ARE available via SOAP but require an additional call per DR. We chose to skip this for now to minimize API calls, but this document explains how to retrieve them if needed later.

## Why Parts Aren't in DRStaticMetadata
- Requires 1 extra SOAP call per DR (expensive for bulk operations)
- A DR can have multiple parts (doesn't fit single-column model well)
- Parts might be added/edited after DR creation (not truly "static")

## Current Metadata Coverage
✅ **Currently captured in DRStaticMetadata:**
- `urgency` - from Master.UrgencyPK → GetUrgencyList lookup
- `defect_description` - from Master.DefectType.Name
- `charged_to_dept` - from Master.ChargedToDeptName
- `deviation_type` - from Master.ReasonLink.DeviationTypeName
- `component` - from Master.ReasonLink.ComponentTypeName

❌ **NOT captured:**
- `part_number` - requires separate PartsForUnitDetails call

## How to Get Parts (For Future Implementation)

### Method: PartsForUnitDetails
SOAP service provides `PartsForUnitDetails(unitDetails, devUser)` method.

### Example Code
```python
from zeep import Client
from zeep.transports import Transport
from zeep.cache import InMemoryCache

# Setup client and login
wsdl = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc?wsdl'
client = Client(wsdl=wsdl, transport=Transport(cache=InMemoryCache(), timeout=30))
client.service.RegisterSession(username=user, password=pwd)
dev_user = client.service.GetUser(username=user, password=pwd)

# Get DR via DisplayItemForDateRange
items = client.service.DisplayItemForDateRange(
    devUser=dev_user,
    fromDate=start_dt,
    toDate=end_dt,
    closed=True
)

# For each DR, get parts
for item in items:
    item_vals = getattr(item, '__values__', {})
    master = item_vals.get('Master')
    master_vals = getattr(master, '__values__', {})
    unit_details = master_vals.get('UnitDetails')
    
    if unit_details:
        # Call PartsForUnitDetails
        parts_result = client.service.PartsForUnitDetails(
            unitDetails=unit_details,
            devUser=dev_user
        )
        
        # parts_result is a list of CDeviatedPart objects
        for part in parts_result:
            part_vals = getattr(part, '__values__', {})
            part_number = part_vals.get('PartNumber')
            quantity = part_vals.get('Quantity')
            description = part_vals.get('Description')
            action_taken = part_vals.get('ActionTaken')
            # ... etc
```

### Part Fields Available
Each part has these fields:
- `PartNumber` - The part number (e.g., "87674-21-248")
- `Quantity` - Number of parts (e.g., 4.0)
- `Description` - Part description
- `ActionTaken` - What was done (e.g., "Reworked", "Scrapped")
- `UnitOfMeasure` - e.g., "EA"
- `StandardCost` - Cost value
- `DepartmentNumber` - Manufacturing dept
- `IsRework` - Boolean flag
- `IsScrapped` - Boolean flag
- Many more fields (see example output below)

### Example Response for DR 49111
```json
[
  {
    "PartNumber": "87674-21-248",
    "Quantity": 4.0,
    "Description": "3.05W X 92.75L X SHEET,PRE-PAI",
    "ActionTaken": "Reworked",
    "UnitOfMeasure": "EA",
    "StandardCost": 13.7,
    "DepartmentNumber": "3800",
    "IsRework": true,
    "IsScrapped": false,
    "UnitDetailsPK": "aeea3658-1e06-4e04-aeaa-593ab679a307"
  }
]
```

## Implementation Options (If Needed Later)

### Option 1: Extend DRStaticMetadata
Add columns:
- `part_numbers` TEXT - Comma-separated list of part numbers
- `part_count` INTEGER - Number of parts

Pros: Simple, keeps everything in one table
Cons: Loses detailed part info, messy for multiple parts

### Option 2: Create DRParts Table
```sql
CREATE TABLE DRParts (
    id INTEGER PRIMARY KEY,
    deviation_number INTEGER NOT NULL,
    part_number TEXT,
    quantity REAL,
    description TEXT,
    action_taken TEXT,
    unit_of_measure TEXT,
    standard_cost REAL,
    is_rework INTEGER,
    is_scrapped INTEGER,
    FOREIGN KEY(deviation_number) REFERENCES DRStaticMetadata(deviation_number)
)
```

Pros: Proper normalization, keeps all part details
Cons: More complex queries, requires joins

### Option 3: On-Demand Retrieval
Don't store parts at all. When dashboard needs them:
- Query DR from database
- Make live SOAP call to get parts
- Display in UI

Pros: Always fresh data, no storage needed
Cons: Slower dashboard, requires SOAP access

## Performance Considerations

### Backfill Cost
For 278 existing DRs:
- 278 PartsForUnitDetails calls
- At 0.5s throttle = ~139 seconds (~2.3 minutes)

### Ongoing Cost
Typical poll with 5-10 new DRs:
- 5-10 additional PartsForUnitDetails calls
- At 0.5s throttle = ~2.5-5 seconds added per poll

## Testing Script
See: `call_partsforunitdetails_49111.py` for working example

## Decision
**Current decision:** Skip parts to minimize SOAP calls. Capture deviation_type and component from Master.ReasonLink instead (no extra calls needed).

**Reconsider if:** Dashboard requirements change to absolutely need part numbers for filtering/reporting.
