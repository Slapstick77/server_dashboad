import sqlite3, json

def main():
    con = sqlite3.connect('SCHLabor.db')
    cur = con.cursor()
    # Totals
    cur.execute('SELECT COUNT(*) FROM SCHLabor_rebuild')
    total_all = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM SCHLabor_rebuild WHERE LoggedDate LIKE '%/2025'")
    total_2025 = cur.fetchone()[0]

    pair_sql_all = (
        "WITH g AS ( "
        " SELECT LoggedDate, COMNumber, EmployeeName, EmployeeNumber1, DepartmentNumber, Area, IFNULL(Reference,'') ref, COUNT(*) cnt, COUNT(DISTINCT ActualHours) dh "
        " FROM SCHLabor_rebuild "
        " GROUP BY 1,2,3,4,5,6,7 "
        " HAVING dh>1 AND cnt=2 ) "
        " SELECT COUNT(*) groups, SUM(cnt) rows FROM g"
    )
    cur.execute(pair_sql_all)
    pairs_all_groups, pairs_all_rows = cur.fetchone()

    pair_sql_2025 = (
        "WITH g AS ( "
        " SELECT LoggedDate, COMNumber, EmployeeName, EmployeeNumber1, DepartmentNumber, Area, IFNULL(Reference,'') ref, COUNT(*) cnt, COUNT(DISTINCT ActualHours) dh "
        " FROM SCHLabor_rebuild WHERE LoggedDate LIKE '%/2025' "
        " GROUP BY 1,2,3,4,5,6,7 "
        " HAVING dh>1 AND cnt=2 ) "
        " SELECT COUNT(*) groups, SUM(cnt) rows FROM g"
    )
    cur.execute(pair_sql_2025)
    pairs_2025_groups, pairs_2025_rows = cur.fetchone()

    result = {
        'all': {
            'total_rows': total_all,
            'pair_groups': pairs_all_groups,
            'rows_in_pairs': pairs_all_rows,
            'rows_in_pairs_pct': round(100.0 * pairs_all_rows / total_all, 4) if total_all else 0.0,
            'pair_groups_vs_total_rows_pct': round(100.0 * pairs_all_groups / total_all, 4) if total_all else 0.0
        },
        '2025': {
            'total_rows_2025': total_2025,
            'pair_groups_2025': pairs_2025_groups,
            'rows_in_pairs_2025': pairs_2025_rows,
            'rows_in_pairs_2025_pct': round(100.0 * pairs_2025_rows / total_2025, 4) if total_2025 else 0.0,
            'pair_groups_vs_2025_rows_pct': round(100.0 * pairs_2025_groups / total_2025, 4) if total_2025 else 0.0
        }
    }
    print(json.dumps(result, indent=2))

if __name__ == '__main__':
    main()
