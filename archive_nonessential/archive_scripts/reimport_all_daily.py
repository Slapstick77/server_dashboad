from report_update_service import reimport_all_labor_files
from datetime import datetime

print('Reimport ALL daily SCHLabor_*.csv files start', datetime.utcnow())
res = reimport_all_labor_files(progress=lambda phase, info: (phase in ('file','done') and print(phase, info)))
print('Summary:', res)
