import json
import report_update_service as rus
rus.ensure_change_log_tables()
try:
    d = rus.labor_diagnostics()
    print(json.dumps(d, indent=2))
except Exception as e:
    print('Error:', e)
