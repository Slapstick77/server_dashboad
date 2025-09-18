import sqlite3, datetime, json

con = sqlite3.connect('SCHLabor.db')
cur = con.cursor()

# Text MIN/MAX (lexicographic)
cur.execute("SELECT MIN(LoggedDate), MAX(LoggedDate) FROM SCHLabor")
text_min, text_max = cur.fetchone()

# Parsed chronological MIN/MAX
cur.execute("SELECT LoggedDate FROM SCHLabor")
parsed_min = None
parsed_max = None
for (d,) in cur.fetchall():
    try:
        m, day, y = d.split('/')
        dt = datetime.date(int(y), int(m), int(day))
        if parsed_min is None or dt < parsed_min:
            parsed_min = dt
        if parsed_max is None or dt > parsed_max:
            parsed_max = dt
    except Exception:
        pass

# Counts of years present
cur.execute("SELECT substr(LoggedDate, instr(LoggedDate,'/')+1) FROM SCHLabor LIMIT 1")
# Quick year extraction per row
cur.execute("SELECT substr(LoggedDate, length(LoggedDate)-3, 4) AS Y, COUNT(*) FROM SCHLabor GROUP BY Y ORDER BY Y")
year_counts = cur.fetchall()

# Row counts for first parsed day and last parsed day
first_day_str = f"{parsed_min.month}/{parsed_min.day}/{parsed_min.year}" if parsed_min else None
last_day_str = f"{parsed_max.month}/{parsed_max.day}/{parsed_max.year}" if parsed_max else None

samples = {}
if first_day_str:
    cur.execute("SELECT LoggedDate, COMNumber, EmployeeNumber1, DepartmentNumber, ActualHours FROM SCHLabor WHERE LoggedDate=? LIMIT 3", (first_day_str,))
    samples['earliest_rows'] = cur.fetchall()
if last_day_str:
    cur.execute("SELECT LoggedDate, COMNumber, EmployeeNumber1, DepartmentNumber, ActualHours FROM SCHLabor WHERE LoggedDate=? LIMIT 3", (last_day_str,))
    samples['latest_rows'] = cur.fetchall()

# 2025 presence
cur.execute("SELECT COUNT(*) FROM SCHLabor WHERE LoggedDate LIKE '%/2025'")
rows_2025 = cur.fetchone()[0]
cur.execute("SELECT DISTINCT LoggedDate FROM SCHLabor WHERE LoggedDate LIKE '%/2025' ORDER BY 1 LIMIT 5")
first_2025_samples = [r[0] for r in cur.fetchall()]
cur.execute("SELECT DISTINCT LoggedDate FROM SCHLabor WHERE LoggedDate LIKE '%/2025' ORDER BY 1 DESC LIMIT 5")
last_2025_samples = [r[0] for r in cur.fetchall()]

print(json.dumps({
    'text_min': text_min,
    'text_max': text_max,
    'parsed_min': str(parsed_min),
    'parsed_max': str(parsed_max),
    'year_counts': year_counts,
    'earliest_day': first_day_str,
    'latest_day': last_day_str,
    'samples': samples,
    'rows_with_2025_year': rows_2025,
    'first_2025_dates': first_2025_samples,
    'last_2025_dates': last_2025_samples
}, indent=2))
