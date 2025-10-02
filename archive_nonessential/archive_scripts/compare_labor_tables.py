import sqlite3, json

def main():
    con = sqlite3.connect('SCHLabor.db')
    cur = con.cursor()
    out = {}
    tables = ['SCHLabor', 'SCHLabor_rebuild']
    for t in tables:
        try:
            cur.execute(f'SELECT COUNT(*) FROM {t}')
            cnt = cur.fetchone()[0]
            cur.execute(f'SELECT MIN(LoggedDate), MAX(LoggedDate), COUNT(DISTINCT LoggedDate) FROM {t}')
            mn, mx, dc = cur.fetchone()
            out[t] = {'rows': cnt, 'min_date': mn, 'max_date': mx, 'distinct_dates': dc}
        except Exception as e:
            out[t] = {'error': str(e)}

    key_cols = "LoggedDate, COMNumber, EmployeeName, EmployeeNumber1, DepartmentNumber, Area, IFNULL(Reference,'')"
    for label, table in [('orig','SCHLabor'), ('rebuild','SCHLabor_rebuild')]:
        try:
            cur.execute(f'SELECT COUNT(*) FROM (SELECT {key_cols} FROM {table} GROUP BY 1,2,3,4,5,6,7)')
            out[f'{label}_unique_key_rows'] = cur.fetchone()[0]
        except Exception as e:
            out[f'{label}_unique_key_rows'] = str(e)

    try:
        cur.execute("""
            SELECT COUNT(*) FROM (
              SELECT LoggedDate, COMNumber, EmployeeName, EmployeeNumber1, DepartmentNumber, Area, ActualHours, IFNULL(Reference,'') ref FROM SCHLabor_rebuild
              EXCEPT
              SELECT LoggedDate, COMNumber, EmployeeName, EmployeeNumber1, DepartmentNumber, Area, ActualHours, IFNULL(Reference,'') FROM SCHLabor
            )
        """)
        out['rebuild_only_rows'] = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM (
              SELECT LoggedDate, COMNumber, EmployeeName, EmployeeNumber1, DepartmentNumber, Area, ActualHours, IFNULL(Reference,'') ref FROM SCHLabor
              EXCEPT
              SELECT LoggedDate, COMNumber, EmployeeName, EmployeeNumber1, DepartmentNumber, Area, ActualHours, IFNULL(Reference,'') FROM SCHLabor_rebuild
            )
        """)
        out['original_only_rows'] = cur.fetchone()[0]
    except Exception as e:
        out['diff_error'] = str(e)

    print(json.dumps(out, indent=2))

if __name__ == '__main__':
    main()
