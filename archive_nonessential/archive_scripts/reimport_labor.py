from datetime import date, timedelta
import report_update_service as rus
from report_update_service import reimport_existing_labor_files

# Re-import last 14 days (adjust as needed)
END = date.today()
START = END - timedelta(days=14)
print(f'Re-importing labor CSVs from {START} to {END}...')
added = reimport_existing_labor_files(START, END, progress=lambda phase, info: print(info))
print('Added rows:', added)
