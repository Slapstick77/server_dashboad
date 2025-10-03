"""Test the optimized metrics cache refresh"""
from metrics_cache import refresh_metrics_cache

result = refresh_metrics_cache(trigger='optimization_test')
print(f"Success: {result['success']}")
print(f"Duration: {result['duration_seconds']:.2f}s")
print(f"Trigger: {result.get('trigger', 'N/A')}")
if not result['success']:
    print(f"Error: {result.get('error')}")
