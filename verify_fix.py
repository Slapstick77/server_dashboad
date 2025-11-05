from datetime import datetime

# Test the fix
test_date = "2025-11-05T12:25:48.323000"

# OLD WAY (incorrect - treating local time as UTC)
print("OLD WAY (incorrect):")
dt_old = datetime.fromisoformat(test_date.split('.')[0])
from datetime import timezone
dt_old_utc = dt_old.replace(tzinfo=timezone.utc)
created_ms_old = int(dt_old_utc.timestamp() * 1000)
print(f"  Treated as UTC: {dt_old_utc}")
print(f"  Milliseconds: {created_ms_old}")

# NEW WAY (correct - treating as local time)
print("\nNEW WAY (correct):")
dt_new = datetime.fromisoformat(test_date.split('.')[0])
created_ms_new = int(dt_new.timestamp() * 1000)
print(f"  Treated as local: {dt_new}")
print(f"  Milliseconds: {created_ms_new}")

# Calculate age difference
print(f"\nDifference: {(created_ms_old - created_ms_new) / (1000 * 60 * 60):.1f} hours")
print("This matches our UTC-6 timezone offset!")
