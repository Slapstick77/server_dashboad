import sqlite3, json

def main():
    con=sqlite3.connect('SCHLabor.db')
    cur=con.cursor()
    like = '%/2025'
    # Key duplicate groups (5-field key)
    cur.execute("""SELECT COUNT(*) FROM (
        SELECT LoggedDate, EmployeeNumber1, COMNumber, DepartmentNumber, IFNULL(Reference,'') r, COUNT(*) c
        FROM SCHLabor_rebuild WHERE LoggedDate LIKE ?
        GROUP BY 1,2,3,4,5 HAVING c>1)""", (like,))
    key_groups = cur.fetchone()[0]
    cur.execute("""SELECT SUM(c) FROM (
        SELECT LoggedDate, EmployeeNumber1, COMNumber, DepartmentNumber, IFNULL(Reference,'') r, COUNT(*) c
        FROM SCHLabor_rebuild WHERE LoggedDate LIKE ?
        GROUP BY 1,2,3,4,5 HAVING c>1)""", (like,))
    key_rows = cur.fetchone()[0] or 0
    # Variant groups (ActualHours differs among otherwise identical 7 fields)
    cur.execute("""SELECT COUNT(*), SUM(c) FROM (
        SELECT LoggedDate, COMNumber, EmployeeName, EmployeeNumber1, DepartmentNumber, Area, IFNULL(Reference,'') r, COUNT(*) c, COUNT(DISTINCT ActualHours) dh
        FROM SCHLabor_rebuild WHERE LoggedDate LIKE ?
        GROUP BY 1,2,3,4,5,6,7 HAVING dh>1)""", (like,))
    variant_groups, variant_rows = cur.fetchone()
    # Exact full-row duplicate groups (all 8 columns identical)
    cur.execute("""SELECT COUNT(*), SUM(c) FROM (
        SELECT LoggedDate, COMNumber, EmployeeName, EmployeeNumber1, DepartmentNumber, Area, ActualHours, IFNULL(Reference,'') r, COUNT(*) c
        FROM SCHLabor_rebuild WHERE LoggedDate LIKE ?
        GROUP BY 1,2,3,4,5,6,7,8 HAVING c>1)""", (like,))
    exact_groups, exact_rows = cur.fetchone()
    cur.execute("SELECT COUNT(*) FROM SCHLabor_rebuild WHERE LoggedDate LIKE ?", (like,))
    total_2025 = cur.fetchone()[0]
    print(json.dumps({
        'total_2025_rows': total_2025,
        'key_duplicate_groups': key_groups,
        'key_duplicate_rows': key_rows,
        'variant_groups_hours_differ': variant_groups,
        'variant_rows': variant_rows,
        'exact_full_row_duplicate_groups': exact_groups,
        'exact_full_row_duplicate_rows': exact_rows
    }, indent=2))

if __name__ == '__main__':
    main()
