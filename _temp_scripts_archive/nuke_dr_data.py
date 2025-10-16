"""
Nuke all DR data from the database to start fresh.
Run this before re-polling to eliminate duplicates from earlier test runs.
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'SCHLabor.db')

def nuke_dr_data():
    """Delete all DR poll data from the database."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    print("Nuking all DR data...")
    
    # Delete in reverse dependency order
    cur.execute("DELETE FROM DRRoutingStep")
    routing_deleted = cur.rowcount
    print(f"  Deleted {routing_deleted} routing step rows")
    
    cur.execute("DELETE FROM DRItemSnapshot")
    snapshot_deleted = cur.rowcount
    print(f"  Deleted {snapshot_deleted} snapshot rows")
    
    cur.execute("DELETE FROM DRState")
    state_deleted = cur.rowcount
    print(f"  Deleted {state_deleted} state rows")
    
    cur.execute("DELETE FROM DRStateMeta")
    meta_deleted = cur.rowcount
    print(f"  Deleted {meta_deleted} state meta rows")
    
    cur.execute("DELETE FROM DRPollTimingEvent")
    timing_deleted = cur.rowcount
    print(f"  Deleted {timing_deleted} timing event rows")
    
    cur.execute("DELETE FROM DRPollRun")
    run_deleted = cur.rowcount
    print(f"  Deleted {run_deleted} poll run rows")
    
    conn.commit()
    conn.close()
    
    print("\n✅ All DR data nuked. Ready for fresh poll.")
    print("Run 'Poll DRs' from the desktop app to start clean.")

if __name__ == '__main__':
    nuke_dr_data()
