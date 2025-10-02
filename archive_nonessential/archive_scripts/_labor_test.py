import report_update_service as rus
print('ensure_change_log_tables...')
rus.ensure_change_log_tables()
print('labor_backfill run...')
print(rus.labor_backfill())
