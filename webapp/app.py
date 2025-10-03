"""
SQRS Dashboard - Production Application (Blueprint Architecture)

Refactored application using Flask blueprints for clean separation of concerns:
- dashboard.py: HTML page routes
- api.py: JSON API endpoints  
- admin.py: Administrative functions
- utils.py: Shared helpers and constants

All routes now in blueprints. This file just registers them.
"""
from flask import Flask
import os
import sys

# Add webapp to path
sys.path.insert(0, os.path.dirname(__file__))

# Import blueprints
from blueprints.dashboard import dashboard
from blueprints.api import api
from blueprints.admin import admin
from blueprints.reports import reports

# Create Flask app
app = Flask(__name__)

# Register blueprints
app.register_blueprint(dashboard)
app.register_blueprint(api)
app.register_blueprint(admin)
app.register_blueprint(reports)

# Initialize metrics cache on startup (if empty)
def init_cache():
    """Initialize metrics cache and unit completion table if empty."""
    try:
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
        from metrics_cache import (get_cached_metrics, refresh_metrics_cache, 
                                   ensure_cache_tables, refresh_unit_completion, get_complete_units)
        
        ensure_cache_tables()
        
        # Check if cache is empty
        cached_trends = get_cached_metrics('unit_time_trends')
        cached_incomplete = get_cached_metrics('incomplete_units')
        
        if not cached_trends or not cached_incomplete:
            print("🔄 Initializing metrics cache (first run)...")
            refresh_metrics_cache(trigger='app_startup')
            print("✅ Metrics cache initialized!")
        else:
            print(f"✅ Metrics cache loaded (last refresh: {cached_trends.get('trigger_source', 'unknown')})")
        
        # Check if unit completion table is empty
        complete_units = get_complete_units()
        if not complete_units:
            print("🔄 Initializing unit completion table (first run)...")
            result = refresh_unit_completion()
            print(f"✅ Unit completion initialized! ({result['complete']} complete / {result['total_units']} total)")
        else:
            print(f"✅ Unit completion table loaded ({len(complete_units)} complete units)")
            
    except Exception as e:
        print(f"⚠️ Warning: Could not initialize cache: {e}")

if __name__ == '__main__':
    init_cache()
    app.run(host='0.0.0.0', port=5000, debug=True)
