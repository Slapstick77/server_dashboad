"""
Initialize Metrics Cache

Run this once to create the cache tables and do an initial metrics calculation.
After this, the cache will be automatically updated by data sync scripts.

Usage:
    python init_metrics_cache.py
"""
from metrics_cache import refresh_metrics_cache, ensure_cache_tables

if __name__ == '__main__':
    print("Initializing metrics cache system...")
    
    # Create tables
    ensure_cache_tables()
    print("✓ Cache tables created")
    
    # Initial refresh
    print("Running initial metrics calculation (this may take a minute)...")
    result = refresh_metrics_cache(trigger='initial_setup')
    
    if result['success']:
        print(f"✓ Metrics cache initialized successfully in {result['duration_seconds']:.2f} seconds")
        print("\nCache will now be automatically refreshed after each data sync.")
        print("You can also manually refresh via the web UI at /tasks")
    else:
        print(f"✗ Failed to initialize cache: {result.get('error')}")
