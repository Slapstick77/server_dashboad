"""
SQRS Dashboard - Main Application (Blueprint Version)

This is the refactored entry point that uses blueprints.
DO NOT RUN THIS YET - Under construction, testing one route at a time.
"""
from flask import Flask
import os
import sys

# Add webapp to path so we can import from it
sys.path.insert(0, os.path.dirname(__file__))

# Import blueprints
from blueprints.dashboard import dashboard

# Create Flask app
app = Flask(__name__)

# Register blueprints
app.register_blueprint(dashboard)

if __name__ == '__main__':
    print("=" * 70)
    print("⚠️  TESTING MODE - Only / route implemented")
    print("=" * 70)
    app.run(host='0.0.0.0', port=5001, debug=True)  # Using port 5001 to not conflict
