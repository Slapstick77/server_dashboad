import sqlite3, statistics, datetime

DB='SCHLabor.db'

def parse_raw_date(s:str):
    # Expect M/D/YYYY or MM/DD/YYYY
    try:
        return datetime.datetime.strptime(s, '%m/%d/%Y').date()
    except ValueError:
        # Maybe single digit day with no leading zero still covered; fallback generic split
        m,d,y = s.split('/')
        return datetime.date(int(y), int(m), int(d))

def main():
    con=sqlite3.connect(DB)
    cur=con.cursor()
    # Variant groups (same key except differing ActualHours)
    cur.execute("""
        SELECT LoggedDate, COUNT(*) variant_groups, SUM(c) variant_rows
        FROM (
          SELECT LoggedDate, COMNumber, EmployeeName, EmployeeNumber1, DepartmentNumber, Area, IFNULL(Reference,'') ref,
                 COUNT(*) c, COUNT(DISTINCT ActualHours) dh
          FROM SCHLabor_rebuild
          GROUP BY 1,2,3,4,5,6,7
          HAVING dh>1
        ) t
        GROUP BY LoggedDate
    """)
    variant_days = {row[0]: {'variant_groups': row[1], 'variant_rows': row[2]} for row in cur.fetchall()}
    # All day counts
    cur.execute("SELECT LoggedDate, COUNT(*) rows FROM SCHLabor_rebuild GROUP BY LoggedDate")
    day_rows = {row[0]: row[1] for row in cur.fetchall()}
    all_counts = list(day_rows.values())
    median_rows = int(statistics.median(all_counts)) if all_counts else 0
    low_threshold = max(10, median_rows // 4)  # heuristic
    results = []
    for raw_date, info in variant_days.items():
        dt = parse_raw_date(raw_date)
        prev_dt = dt - datetime.timedelta(days=1)
        prev_raw = f"{prev_dt.month}/{prev_dt.day}/{prev_dt.year}"
        prev_rows = day_rows.get(prev_raw, 0)
        today_rows = day_rows.get(raw_date, 0)
        results.append({
            'date': raw_date,
            'rows_today': today_rows,
            'variant_groups': info['variant_groups'],
            'variant_rows': info['variant_rows'],
            'prev_date': prev_raw,
            'prev_rows': prev_rows,
            'prev_is_low': prev_rows < low_threshold
        })
    # Sort by prev_rows ascending to highlight low previous days
    results.sort(key=lambda r: r['prev_rows'])
    low_prev = [r for r in results if r['prev_is_low']]
    print(f"Daily median rows: {median_rows}  Low-threshold: < {low_threshold}\n")
    print(f"Total variant days: {len(results)}  (rows involved: {sum(r['variant_rows'] for r in results)})")
    print(f"Variant days with low previous-day volume: {len(low_prev)}")
    print("\nSample (first 15 with lowest prev-day rows):")
    for r in results[:15]:
        print(f" {r['date']} rows={r['rows_today']} var_groups={r['variant_groups']} var_rows={r['variant_rows']} prev={r['prev_date']} prev_rows={r['prev_rows']} low_prev={r['prev_is_low']}")
    # Also show top 10 largest variant day impacts
    top_variant = sorted(results, key=lambda r: r['variant_rows'], reverse=True)[:10]
    print("\nTop 10 variant-impact days (by variant_rows):")
    for r in top_variant:
        print(f" {r['date']} var_rows={r['variant_rows']} groups={r['variant_groups']} rows_today={r['rows_today']} prev_rows={r['prev_rows']}")

if __name__ == '__main__':
    main()
