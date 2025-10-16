"""Get complete urgency map from SOAP"""
from zeep import Client
from zeep.wsse.username import UsernameToken
import json

creds = json.load(open('download_archive/dr_credentials.json'))
client = Client(
    'http://service1.ahu.jci.com/MOM_WCF/ServiceManufacturingDeviationSystem/ServiceManufacturingDeviationSystem.svc?wsdl',
    wsse=UsernameToken(creds['user'], creds['password'])
)

app_pk = client.service.GetApplicationPK(applicationName='ManufacturingDeviationSystem')
session_pk = client.service.RegisterSession(applicationPK=app_pk)
dev_user = client.service.MOM_Login(
    applicationPK=app_pk,
    sessionPK=session_pk,
    userName=creds['user'],
    password=creds['password']
)

urg_list = client.service.GetUrgencyList(devUser=dev_user)

print('Complete Urgency Map:')
for u in urg_list:
    vals = getattr(u, '__values__', {})
    pk = vals.get('PK')
    name = vals.get('LongName')
    print(f"    '{pk}': '{name}',")
