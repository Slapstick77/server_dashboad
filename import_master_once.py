import report_update_service as rus
from report_update_service import import_master_missing
from datetime import datetime
MASTER='SCHLabor_master.csv'
print('Starting master import', datetime.utcnow())
added = import_master_missing(MASTER, progress=lambda p,i: (p=='done' and print('DONE', i)) or None)
print('Added rows from master:', added)
