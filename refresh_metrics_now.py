from metrics_cache import refresh_metrics_cache

print("Refreshing metrics cache...")
result = refresh_metrics_cache(trigger='manual_refresh')
print(f"\nSuccess: {result['success']}")
print(f"Duration: {result['duration_seconds']:.2f}s")
if result.get('error'):
    print(f"Error: {result['error']}")
else:
    print("✅ Metrics cache refreshed successfully!")
    print("\nNow check your dashboard - graphs should show recent trends.")
