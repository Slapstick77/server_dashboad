from zeep import Client
from zeep.transports import Transport
from zeep.cache import InMemoryCache
from datetime import datetime, timedelta, timezone

user = '01962'
pwd = '01962'
wsdl = 'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc?wsdl'

client = Client(wsdl=wsdl, transport=Transport(cache=InMemoryCache(), timeout=30))
client.service.RegisterSession(username=user, password=pwd)
dev_user = client.service.GetUser(username=user, password=pwd)

# Get DRs from range
items = client.service.DisplayItemForDateRange(
    devUser=dev_user,
    fromDate=datetime.now(timezone.utc) - timedelta(days=1),
    toDate=datetime.now(timezone.utc),
    closed=True
)

if items:
    sample = items[0]
    vals = getattr(sample, '__values__', {})
    master = vals.get('Master')
    
    print('='*80)
    print('MASTER OBJECT IN DisplayItemForDateRange')
    print('='*80)
    print(f'Master present: {"YES" if master else "NO"}')
    
    if master:
        m_vals = getattr(master, '__values__', {})
        print(f'Master has UrgencyPK: {"YES" if m_vals.get("UrgencyPK") else "NO"}')
        print(f'Master has DefectType: {"YES" if m_vals.get("DefectType") else "NO"}')
        print(f'Master has ChargedToDeptName: {"YES" if m_vals.get("ChargedToDeptName") else "NO"}')
        print(f'Master has UnitDetails: {"YES" if m_vals.get("UnitDetails") else "NO"}')
        
        print('\nSo we CAN get Defect, Urgency, ChargedTo WITHOUT extra calls!')
        print('We just need to parse the Master object that is ALREADY in the response.')
else:
    print('No DRs found')
