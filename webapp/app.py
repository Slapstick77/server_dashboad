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

# Create Flask app
app = Flask(__name__)

# Register blueprints
app.register_blueprint(dashboard)
app.register_blueprint(api)
app.register_blueprint(admin)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
