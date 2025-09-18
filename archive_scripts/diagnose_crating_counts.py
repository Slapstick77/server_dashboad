import sqlite3, json

def main():
    con = sqlite3.connect('SCHLabor.db')
    cur = con.cursor()
    # Total rows in summary
    cur.execute('SELECT COUNT(*) FROM SCHSchedulingSummary')
    total_rows = cur.fetchone()[0]

    # Rows where COM starts with 1
    cur.execute("SELECT COUNT(*) FROM SCHSchedulingSummary WHERE CAST(comnumber1 AS TEXT) LIKE '1%'")
    rows_start1 = cur.fetchone()[0]

    # Distinct COMs starting with 1
    cur.execute("SELECT COUNT(DISTINCT comnumber1) FROM SCHSchedulingSummary WHERE CAST(comnumber1 AS TEXT) LIKE '1%'")
    distinct_com_start1 = cur.fetchone()[0]

    # Any non-null crating std OR act
    cur.execute("""
        SELECT COUNT(*) FROM SCHSchedulingSummary
        WHERE CAST(comnumber1 AS TEXT) LIKE '1%'
          AND (cratingstdhrs IS NOT NULL OR cratingacthrs IS NOT NULL)
    """)
    any_non_null = cur.fetchone()[0]

    # Any positive (>0) crating std OR act
    cur.execute("""
        SELECT COUNT(*) FROM SCHSchedulingSummary
        WHERE CAST(comnumber1 AS TEXT) LIKE '1%'
          AND ((cratingstdhrs IS NOT NULL AND cratingstdhrs>0) OR (cratingacthrs IS NOT NULL AND cratingacthrs>0))
    """)
    any_positive = cur.fetchone()[0]

    # Both null
    cur.execute("""
        SELECT COUNT(*) FROM SCHSchedulingSummary
        WHERE CAST(comnumber1 AS TEXT) LIKE '1%'
          AND cratingstdhrs IS NULL AND cratingacthrs IS NULL
    """)
    both_null = cur.fetchone()[0]

    # Both zero or null (treat null as zero)
    cur.execute("""
        SELECT COUNT(*) FROM SCHSchedulingSummary
        WHERE CAST(comnumber1 AS TEXT) LIKE '1%'
          AND IFNULL(cratingstdhrs,0)=0 AND IFNULL(cratingacthrs,0)=0
    """)
    both_zero_or_null = cur.fetchone()[0]

    # Sample a few with zeros but non-null fields
    cur.execute("""
        SELECT comnumber1, jobname, cratingstdhrs, cratingacthrs FROM SCHSchedulingSummary
        WHERE CAST(comnumber1 AS TEXT) LIKE '1%'
          AND IFNULL(cratingstdhrs,0)=0 AND IFNULL(cratingacthrs,0)=0
        LIMIT 5
    """)
    sample_zero = cur.fetchall()

    # Ship month distribution for COM starting with 1 (if used for date approximations)
    cur.execute("SELECT MIN(shipmonth), MAX(shipmonth) FROM SCHSchedulingSummary WHERE CAST(comnumber1 AS TEXT) LIKE '1%'")
    ship_min, ship_max = cur.fetchone()

    out = {
        'table_total_rows': total_rows,
        'rows_starting_1': rows_start1,
        'distinct_com_starting_1': distinct_com_start1,
        'with_any_non_null_crating': any_non_null,
        'with_any_positive_crating': any_positive,
        'both_crating_null': both_null,
        'both_crating_zero_or_null': both_zero_or_null,
        'shipmonth_min': ship_min,
        'shipmonth_max': ship_max,
        'sample_zero_rows': sample_zero
    }
    print(json.dumps(out, indent=2))

if __name__ == '__main__':
    main()
