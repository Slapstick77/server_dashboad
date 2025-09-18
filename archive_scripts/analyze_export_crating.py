import csv
from pathlib import Path
p=Path('export_COMs_Dept0380_CratingHours.csv')
if not p.exists():
    raise SystemExit('CSV not found')
blanks=0; total=0; nonzero=0
with p.open(newline='',encoding='utf-8') as f:
    r=csv.DictReader(f)
    for row in r:
        total+=1
        std=row['cratingstdhrs']
        act=row['cratingacthrs']
        if (std in (None,'','0','0.0')) and (act in (None,'','0','0.0')):
            blanks+=1
        if std not in ('', '0','0.0', None) or act not in ('','0','0.0',None):
            nonzero+=1
print('Total rows:', total)
print('Rows with both blank/zero:', blanks)
print('Rows with any non-zero:', nonzero)
