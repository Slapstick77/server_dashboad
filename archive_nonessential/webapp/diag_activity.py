import sqlite3, os, datetime, json
DB=os.path.abspath(os.path.join(os.path.dirname(__file__),'..','SCHLabor.db'))
con=sqlite3.connect(DB)
cur=con.cursor()
today=datetime.date.today()
windows=[7,14,30,45,60]
res={}
for w in windows:
    cutoff=(today-datetime.timedelta(days=w)).isoformat()
    cur.execute("""
        SELECT COUNT(DISTINCT CAST(COMNumber AS TEXT))
        FROM SCHLabor
        WHERE COALESCE(ActualHours,0)>0
          AND CAST(COMNumber AS TEXT) GLOB '[0-9][0-9][0-9][0-9][0-9]'
          AND COALESCE(iso_logged_date, substr(LoggedDate,1,10)) >= ?
    """, (cutoff,))
    res[w]=cur.fetchone()[0]
print(json.dumps(res, indent=2))
