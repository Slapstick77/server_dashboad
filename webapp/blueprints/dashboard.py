"""
Dashboard Blueprint - HTML page routes

All user-facing HTML pages are defined here.
"""
from flask import Blueprint, redirect, url_for

# Create blueprint
dashboard = Blueprint('dashboard', __name__)

# Routes will be moved here one at a time

@dashboard.route('/')
def root():
        # Redirect to the new dashboard landing
        return redirect(url_for('dashboard.dash'))


@dashboard.route('/dash')
def dash():
        # Temporary placeholder - will be replaced with real route next
        return "Dashboard placeholder - route exists but not yet fully implemented"
