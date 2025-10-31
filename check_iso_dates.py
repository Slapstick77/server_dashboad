import sqlite3

conn = sqlite3.connect('SCHLabor.db')
cur = conn.cursor()

print("Checking iso_logged_date column:")
cur.execute("SELECT LoggedDate, iso_logged_date FROM SCHLabor WHERE iso_logged_date IS NOT NULL ORDER BY ROWID DESC LIMIT 10")
rows = cur.fetchall()

if rows:
    for r in rows:
        print(f"  LoggedDate: {r[0]:<15} -> iso_logged_date: {r[1]}")
else:
    print("  ❌ No rows have iso_logged_date populated!")

print("\nChecking NULL iso_logged_date:")
cur.execute("SELECT COUNT(*) FROM SCHLabor WHERE iso_logged_date IS NULL")
null_count = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM SCHLabor")
total_count = cur.fetchone()[0]

print(f"  NULL iso_logged_date: {null_count:,} / {total_count:,} rows ({100*null_count/total_count:.1f}%)")

conn.close()
