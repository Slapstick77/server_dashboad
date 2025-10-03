import sqlite3

db_path = r'c:\Project p\SQRS\SCHLabor.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Check the completion data for COM 18084
query = """
SELECT comnumber1,
       fanassytesthours as std_0200,
       actualfanassytesthours as act_0200,
       fanassytestcomplete as comp_0200
FROM SCHSchedulingSummary
WHERE substr('00000'||CAST(comnumber1 AS TEXT), -5) = '18084'
"""

cursor.execute(query)
row = cursor.fetchone()

if row:
    com, std, act, comp = row
    print(f"COM 18084 - Department 0200 (FanAssyTest) Completion Status:")
    print("=" * 60)
    print(f"COM Number: {com}")
    print(f"Standard Hours: {std}")
    print(f"Actual Hours: {act}")
    print(f"Completion %: {comp}")
    print()
    
    if std and std > 0:
        calc_comp = min(100.0, (act / std) * 100.0) if act and act > 0 else 0.0
        is_complete = comp >= 100.0 or calc_comp >= 100.0
        print(f"Calculated Completion: {calc_comp:.2f}%")
        print(f"Department is {'COMPLETE' if is_complete else 'INCOMPLETE'}")
        print()
        
        if is_complete:
            print("⚠️  Department marked as COMPLETE")
            print("   -> Gap filtering SHOULD be applied (is_complete=True)")
            print("   -> 2024-05-15 SHOULD be filtered out (482-day gap, 1 emp, 1 hr)")
        else:
            print("⚠️  Department marked as INCOMPLETE")
            print("   -> Gap filtering might NOT be applied (is_complete=False)")
            print("   -> 2024-05-15 might be KEPT if no gap filtering on incomplete")
    else:
        print("Department has no standard hours (not applicable to this unit)")
else:
    print("No scheduling summary found for COM 18084")

conn.close()
