import sqlite3, os, math, statistics

DB_PATH = os.path.join(os.path.dirname(__file__), 'SCHLabor.db')

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
try:
    cur.execute("SELECT sqft FROM SCHSchedulingSummary WHERE sqft IS NOT NULL AND sqft > 0 ORDER BY sqft")
except Exception as e:
    print("ERROR: Could not query SCHSchedulingSummary.sqft:", e)
    raise SystemExit(1)
vals = []
for (v,) in cur.fetchall():
    try:
        fv = float(v)
        if fv > 0:
            vals.append(fv)
    except (TypeError, ValueError):
        continue
vals.sort()
conn.close()

n = len(vals)
print(f'row_count {n}')
if not vals:
    raise SystemExit

# Basic stats
print(f'min {vals[0]:.2f}')
print(f'max {vals[-1]:.2f}')
mean = statistics.mean(vals)
median = statistics.median(vals)
print(f'mean {mean:.2f}')
print(f'median {median:.2f}')

percentiles = [0.05,0.10,0.15,0.20,0.25,0.30,0.40,0.50,0.60,0.70,0.75,0.80,0.85,0.90,0.95,0.975,0.99]

get = lambda p: vals[min(int(p*n), n-1)]
for p in percentiles:
    print(f'p{int(p*1000)/10:>5} {get(p):.2f}')

# IQR and Freedman–Diaconis bin estimate
q25 = get(0.25); q75 = get(0.75)
iqr = q75 - q25
bw = 2*iqr*(n ** (-1/3)) if iqr > 0 else (vals[-1]-vals[0])/10
bins = math.ceil((vals[-1]-vals[0]) / bw) if bw > 0 else 10
print(f'iqr {iqr:.2f} fd_bin_width {bw:.2f} approx_bins {bins}')

# Suggest 5 size bands using quantiles; adjust rounding.
raw_thresholds = [get(q) for q in (0.25,0.50,0.75,0.90)]
# Round thresholds to a "clean" increment based on magnitude
rounded = []
for t in raw_thresholds:
    if t < 500: inc = 25
    elif t < 2000: inc = 50
    else: inc = 100
    rounded.append(int(round(t / inc) * inc))
print('raw_thresholds', ','.join(str(round(x,2)) for x in raw_thresholds))
print('rounded_thresholds', rounded)

# Count per proposed band
edges = [0] + rounded + [10**12]
labels = ['XS','SM','MD','LG','XL']
counts = []
for i in range(len(labels)):
    lo, hi = edges[i], edges[i+1]
    c = sum(1 for v in vals if lo < v <= hi)
    counts.append((labels[i], lo, hi if hi<10**12 else None, c))
print('band_counts')
for label, lo, hi, c in counts:
    rng = f'({lo},{"∞" if hi is None else hi}]'
    print(f'{label:>2} {rng:>12} count {c}')

# Alternative 4-band suggestion (collapsing XS+SM => SMALL, etc.)
alt_edges = [0, rounded[1], rounded[2], rounded[3], 10**12]
alt_labels = ['SMALL','MEDIUM','LARGE','XLARGE']
print('alt_band_counts')
for i in range(len(alt_labels)):
    lo, hi = alt_edges[i], alt_edges[i+1]
    c = sum(1 for v in vals if lo < v <= hi)
    rng = f'({lo},{"∞" if hi==10**12 else hi}]'
    print(f'{alt_labels[i]:>6} {rng:>14} count {c}')
