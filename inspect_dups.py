import sqlite3, os, json
DB='SCHLabor.db'
DB_PATH=os.path.abspath(DB)
conn=sqlite3.connect(DB_PATH)
cur=conn.cursor()
cur.execute("SELECT COUNT(*) FROM SCHSchedulingSummary")
rows=cur.fetchone()[0]
cur.execute("SELECT COUNT(DISTINCT comnumber1) FROM SCHSchedulingSummary WHERE comnumber1 IS NOT NULL AND comnumber1<>''")
distinct=cur.fetchone()[0]
cur.execute("SELECT comnumber1, COUNT(*) c FROM SCHSchedulingSummary WHERE comnumber1 IS NOT NULL AND comnumber1<>'' GROUP BY comnumber1 HAVING c>1 ORDER BY c DESC LIMIT 25")
dups=cur.fetchall()
print(json.dumps({'total_rows':rows,'distinct_coms':distinct,'dup_top':dups}, indent=2))
