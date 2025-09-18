import sqlite3, os
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'SCHLabor.db'))
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
COM = '19810'
DEPTS = [('0100','Fab'),('0280','Pipe')]
for code,label in DEPTS:
    cur.execute(
        """
        SELECT DepartmentNumber,
               strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10))) day,
               EmployeeNumber1,
               SUM(COALESCE(ActualHours,0)) hrs
        FROM SCHLabor
        WHERE CAST(COMNumber AS TEXT)=? AND DepartmentNumber=? AND COALESCE(ActualHours,0) > 0
        GROUP BY day, EmployeeNumber1
        ORDER BY day
        """, (COM, code))
    rows = cur.fetchall()
    print(f"--- COM {COM} Dept {code} ({label}) rows: {len(rows)}")
    day_totals = {}
    for dept, day, emp, hrs in rows:
        day_totals.setdefault(day, 0.0)
        day_totals[day] += hrs
        print(f"  {day} emp {emp} hrs {hrs:.2f}")
    if not rows:
        print("  (no labor rows)")
    print("Per-day totals >=0:")
    for day,total in sorted(day_totals.items()):
        print(f"  {day}: {total:.2f} hrs")
    qualifying = [day for day,total in day_totals.items() if total >= 2.0]
    print('Qualifying days (>=2.0h):', qualifying if qualifying else '(none)')
    print()
conn.close()
