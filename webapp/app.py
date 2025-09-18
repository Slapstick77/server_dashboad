"""Minimal Flask app serving an 'Incomplete AHUs' dashboard.

This file intentionally replaces the legacy multi-graph dashboard. It exposes:
  - GET /              : Single-page HTML (rendered from an in-file template string)
  - GET /api/incomplete: JSON list of units that are not 100% complete across tracked departments

Overall efficiency = total actual / total standard (capped at 100%) across tracked depts.
Overall completion = total actual / total standard (capped at 100%) using only tracked departments (no weighting by provided completion columns).
Department completion fallback = actual/standard when explicit completion column blank (each dept capped at 100%).
"""

from flask import Flask, jsonify, render_template_string, request
import sqlite3, os, re, datetime

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'SCHLabor.db'))

# (Label, std col, act col, completion %, efficiency %)
TRACKED_DEPARTMENTS = [
    ("Fab", "fabstdhrs", "fabacthrs", "Fab Completion", "Fab Efficiency"),
    ("Welding", "weldingstdhrs", "weldingacthrs", "Welding Completion", "Welding Efficiency"),
    ("BaseFormPaint", "baseformpaintstdhrs", "baseformpaintacthrs", "BaseFormPaint Completion", "BaseFormPaint Efficiency"),
    ("FanAssyTest", "fanassyteststdhrs", "fanassytestacthrs", "FanAssyTest Completion", "FanAssyTest Efficiency"),
    ("InsulWallFab", "insulwallfabstdhrs", "insulwallfabacthrs", "InsulWallFab Completion", "InsulWallFab Efficiency"),
    ("DoorFab", "doorfabstdhrs", "doorfabacthrs", "DoorFab Completion", "DoorFab Efficiency"),
    ("Assembly", "assystdhrs", "assyacthrs", "Assembly Completion", "Assembly Efficiency"),  # moved right after DoorFab
    ("Electrical", "electricalstdhrs", "electricalacthrs", "Electrical Completion", "Electrical Efficiency"),
    ("Pipe", "pipestdhrs", "pipeacthrs", "Pipe Completion", "Pipe Efficiency"),
    ("Paint", "paintstdhrs", "paintacthrs", "Paint Completion", "Paint Efficiency"),
    ("Crating", "cratingstdhrs", "cratingacthrs", "Crating Completion", "Crating Efficiency"),  # placed last
]

# Minimum total hours that must exist on a department day for that day to count toward
# days_active or span calculations. Days below this threshold are ignored for ALL departments.
MIN_DAY_HOURS = 2.0

app = Flask(__name__)


def get_conn():
    return sqlite3.connect(DB_PATH)


def fnum(v):  # robust parse -> float
    try:
        if v in (None, '', 'nan', 'NaN'):
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def normalize_com(v):
    """Return 5-digit COM string if possible, stripping .0 and whitespace."""
    if v is None:
        return ''
    s = str(v).strip()
    if s.endswith('.0'):
        s = s[:-2]
    digits = ''.join(ch for ch in s if ch.isdigit())
    if len(digits) == 5:
        return digits
    return s


def build_unit(row: dict, colset: set):
    """Build unit metrics.

    Definitions:
      EH (Earned Hours) per department = min(actual_hours, standard_hours * completion%).
      Dept Efficiency (%) = (EH / actual_hours) * 100.  If you spent more hours than earned, efficiency drops below 100.
        - Example: Std 55.2, Act 77.5, Completion 100% -> EH = 55.2, Efficiency = 55.2 / 77.5 = 71.3%.
        - If actual < earned (rare / early complete), efficiency can exceed 100.
      Overall Efficiency (%) = sum(EH) / sum(actual_hours) * 100.
      Overall Completion (%) = total actual / total standard * 100 (capped at 100 for completion display only).
    """
    total_std = 0.0
    total_act = 0.0
    total_eh = 0.0  # sum of earned hours (EH)
    incomplete = False
    depts = []

    for name, stdc, actc, compc, effc in TRACKED_DEPARTMENTS:
        if stdc not in colset and actc not in colset:
            continue
        std = fnum(row.get(stdc)) if stdc in colset else 0.0
        act = fnum(row.get(actc)) if actc in colset else 0.0
        comp_raw = row.get(compc) if compc in colset else None
        eff_raw = row.get(effc) if effc in colset else None

        # Completion percent
        comp_pct = None
        if comp_raw not in (None, ''):
            try:
                comp_pct = float(comp_raw)
            except Exception:
                comp_pct = None
        if comp_pct is None or (comp_pct <= 0 and act > 0 and std > 0):
            comp_pct = (act / std * 100.0) if std > 0 else 0.0
        if comp_pct < 100 - 1e-6 and std > 0 and act >= std:
            comp_pct = 100.0
        if comp_pct < 0:
            comp_pct = 0.0
        if comp_pct > 100:
            comp_pct = 100.0

        # Earned Hours (EH) based on completion% capped at standard and at actual spent
        eh = min(act, std * (comp_pct / 100.0)) if std > 0 else 0.0
        # Efficiency = EH / Actual (drops below 100 when actual > EH)
        eff_pct = (eh / act * 100.0) if act > 0 else 0.0

        if comp_pct < 100 - 1e-6 and std > 0:
            incomplete = True

        total_std += std
        total_act += act
        total_eh += eh
        depts.append({
            'name': name,
            'std': round(std, 2),
            'act': round(act, 2),
            'eff_actual': round(eh, 2),  # keeping JSON key name for compatibility, represents EH
            'efficiency': round(eff_pct, 1),
            'completion': round(comp_pct, 1),
            'status': 'COMPLETE' if comp_pct >= 100 - 1e-6 or std == 0 else 'IN PROGRESS'
        })

    overall_eff_pct = (total_eh / total_act * 100.0) if total_act > 0 else 0.0
    overall_comp_pct = (total_act / total_std * 100.0) if total_std > 0 else 0.0
    if overall_comp_pct > 100:
        overall_comp_pct = 100.0
    return {
        'com': normalize_com(row.get('comnumber1')),
        'jobname': row.get('jobname'),
        'overall_std': round(total_std, 2),
        'overall_act': round(total_act, 2),
        'overall_eff_actual': round(total_eh, 2),  # still labeled eff_actual in payload (EH aggregate)
        'overall_efficiency': round(overall_eff_pct, 1),
        'overall_completion': round(overall_comp_pct, 1),
        'incomplete': incomplete,
        'departments': depts
    }


@app.route('/api/incomplete')
def api_incomplete():
    """Return incomplete units (not 100% weighted complete) with recent labor.

    Rules:
      1. Start from scheduling summary rows whose COM is a 5‑digit number.
      2. Weighted completion from department completion columns.
      3. Labor recency: labor within 60 days AND last labor within 7 days (14 if relax=1 in debug).
      4. Keep std>0, act>0, completion < ~100.
    """
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Discover existing columns
        cur.execute('PRAGMA table_info(SCHSchedulingSummary)')
        colset = {r[1] for r in cur.fetchall()}
        base_needed = ['comnumber1', 'jobname']
        for _, stdc, actc, compc, effc in TRACKED_DEPARTMENTS:
            base_needed.extend([stdc, actc, compc, effc])
        present = [c for c in base_needed if c in colset]
        if not present:
            return jsonify({'count': 0, 'units': [], 'error': 'Expected columns missing'}), 200

        cols_sql = ','.join(f'"{c}"' for c in present)
        cur.execute(f'SELECT {cols_sql} FROM SCHSchedulingSummary WHERE CAST(comnumber1 AS TEXT) GLOB "[0-9][0-9][0-9][0-9][0-9]"')
        sched_rows = [dict(r) for r in cur.fetchall()]
        sched_candidates = len(sched_rows)

        # Labor activity windows (strftime normalizes dates)
        today = datetime.date.today()
        day_60 = (today - datetime.timedelta(days=60)).isoformat()
        day_7 = (today - datetime.timedelta(days=7)).isoformat()
        cur.execute(
            """
            SELECT CAST(COMNumber AS TEXT) com,
                   MAX(strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10)))) last_day
            FROM SCHLabor
            WHERE COALESCE(ActualHours,0) > 0
              AND CAST(COMNumber AS TEXT) GLOB '[0-9][0-9][0-9][0-9][0-9]'
              AND strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10))) >= ?
            GROUP BY COMNumber
            """,
            (day_60,)
        )
        last_map_raw = {row[0]: row[1] for row in cur.fetchall() if row[1]}

        # Accurate day stats with Electrical exclusion of employee 1205797 (only if that is the sole worker on early days)
        cur.execute('PRAGMA table_info(SCHLabor)')
        labor_cols = {r[1] for r in cur.fetchall()}
        stats_map = {}
        if 'DepartmentNumber' in labor_cols:
            # Raw department code -> canonical label mapping (merged codes where appropriate)
            raw_code_to_label = {
                '0120':'Fab',            # FAB
                '0140':'Welding',        # WELD
                '0180':'BaseFormPaint',  # FOAM/PAINT
                '0200':'FanAssyTest',    # FAN
                '0220':'InsulWallFab',   # WALL FAB
                '0230':'Pipe',           # PIPE legacy
                '0260':'Assembly',       # ASSY
                '0270':'DoorFab',        # DOOR
                '0280':'Pipe',           # FLOW merged into Pipe
                '0300':'Electrical',     # ELEC
                '0320':'Pipe',           # PIPE alt
                '0340':'Paint',          # PAINT
                '0360':'FanAssyTest',    # TEST -> FanAssyTest
                '0380':'Crating',        # FINISH -> Crating
            }
            tracked_codes = sorted(raw_code_to_label.keys())
            codes_sql = ','.join(f"'{c}'" for c in tracked_codes)
            cur.execute(
                f"""
                SELECT CAST(COMNumber AS TEXT) com,
                       DepartmentNumber dept,
                       strftime('%Y-%m-%d', COALESCE(iso_logged_date, substr(LoggedDate,1,10))) day,
                       EmployeeNumber1 emp,
                       COALESCE(ActualHours,0) hrs
                FROM SCHLabor
                WHERE COALESCE(ActualHours,0) > 0
                  AND DepartmentNumber IN ({codes_sql})
                  AND CAST(COMNumber AS TEXT) GLOB '[0-9][0-9][0-9][0-9][0-9]'
                """
            )
            rows = cur.fetchall()
            # Build nested structure by merged canonical labels
            day_emp = {}
            for com, dept, day, emp, hrs in rows:
                label = raw_code_to_label.get(dept)
                if not label or day is None:
                    continue
                key = (normalize_com(com), label)
                daymap = day_emp.setdefault(key, {})
                rec = daymap.setdefault(day, {'emps': set(), 'hours': 0.0})
                if emp:
                    rec['emps'].add(str(emp).strip())
                try:
                    rec['hours'] += float(hrs) if hrs is not None else 0.0
                except Exception:
                    pass
            # Aggregate respecting Electrical exclusion after merge
            for (com, label), daymap in day_emp.items():
                days = sorted(daymap.keys())
                # Apply Electrical special exclusion (drop days comprised solely of employee 1205797)
                if label == 'Electrical':
                    tmp_days = []
                    for dday in days:
                        emps = daymap[dday]['emps']
                        if emps == {'1205797'}:
                            continue
                        tmp_days.append(dday)
                    days = tmp_days
                # Apply minimum hours threshold for ALL departments
                use_days = []
                for dday in days:
                    total_hours = daymap[dday]['hours']
                    if total_hours + 1e-9 < MIN_DAY_HOURS:  # ignore tiny floating noise
                        continue
                    use_days.append(dday)
                if not use_days:
                    continue
                first_day = use_days[0]
                last_day = use_days[-1]
                stats_map[(com, label.upper())] = (len(use_days), first_day, last_day)
    relax = request.args.get('relax') == '1'
    recency_cut = (today - datetime.timedelta(days=14)).isoformat() if relax else day_7
    fresh_coms = {normalize_com(c) for c, last in last_map_raw.items() if last >= recency_cut}
    # Normalized COM -> last labor day map for later inclusion of recent fully-complete units
    norm_last_map = {normalize_com(c): last for c, last in last_map_raw.items() if last}
    units = [build_unit(r, colset) for r in sched_rows]
    labor_60 = len(last_map_raw)
    labor_recent_kept = len(fresh_coms)

    # values from outside the with-block now available: units, fresh_coms, labor_60, labor_recent_kept, stats_map, today

    # Apply recency
    before_recency = len(units)
    units = [u for u in units if u['com'] in fresh_coms]
    after_recency = len(units)

    # Attach department day stats
    def norm_dept_name(s: str):
        if s is None:
            return ''
        spaced = re.sub(r'([a-z])([A-Z])', r'\1 \2', str(s))
        return re.sub(r'[^A-Z0-9]', '', spaced.upper())
    for u in units:
        for d in u['departments']:
            key = (u['com'], d['name'].upper())
            stat = stats_map.get(key)
            if not stat:
                continue
            distinct_days, first_day, last_day = stat
            if first_day:
                if d['completion'] >= 100 - 1e-6 and last_day:
                    end_day = last_day
                else:
                    end_day = today.isoformat()
                try:
                    span = (datetime.date.fromisoformat(end_day) - datetime.date.fromisoformat(first_day)).days + 1
                    if span < 1:
                        span = 1
                except Exception:
                    span = None
            else:
                span = None
            d['days_active'] = distinct_days
            if span is not None:
                d['days_span'] = span

    # Final completion / hours filters
    # Attach last labor day to each unit for filtering
    try:
        norm_last_map  # ensure exists
    except NameError:
        norm_last_map = {}
    for u in units:
        if 'last_labor_day' not in u:
            u['last_labor_day'] = norm_last_map.get(u['com'])

    units_pre_hours = len(units)
    # Keep incomplete OR (recent fully complete with last charge within 7 days regardless of relax window)
    units = [u for u in units if u['overall_std'] > 0 and u['overall_act'] > 0 and (
        u['overall_completion'] < 99.999 or (u['overall_completion'] >= 99.999 and u.get('last_labor_day') and u['last_labor_day'] >= day_7)
    )]
    units.sort(key=lambda u: (u['overall_completion'], -(u['overall_std'] - min(u['overall_act'], u['overall_std']))))
    final_count = len(units)

    if request.args.get('debug') == '1':
        return jsonify({
            'count': final_count,
            'sched_candidates': sched_candidates,
            'before_recency': before_recency,
            'after_recency': after_recency,
            'units_pre_hours': units_pre_hours,
            'labor_60': labor_60,
            'labor_recent_kept': labor_recent_kept,
            'recency_cut_used': recency_cut,
            'relax_mode': relax,
            'sample_sched_com': [u['com'] for u in units[:5]],
            'sample_fresh_coms': list(sorted(fresh_coms))[:5],
            'units_sample': units[:15],
            'complete_recent_included': sum(1 for u in units if u['overall_completion'] >= 99.999)
        })
    return jsonify({'count': final_count, 'units': units})


PAGE = """<!doctype html><html><head><meta charset='utf-8'><title>Incomplete AHUs</title><style>
body{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}
header{padding:1rem 1.5rem;display:flex;justify-content:space-between;align-items:center;background:#161b22;border-bottom:1px solid #30363d}
h1{margin:0;font-size:1.05rem}
button{background:#238636;border:1px solid #2ea043;color:#fff;padding:.55rem .9rem;border-radius:6px;font-size:.7rem;font-weight:600;cursor:pointer}button:hover{background:#2ea043}
main{padding:1rem 1.1rem}
.pill{display:inline-block;background:#1f6feb33;border:1px solid #1f6feb55;border-radius:20px;padding:.45rem .75rem;font-size:.6rem;letter-spacing:.5px;margin:.25rem .4rem .6rem 0}
.unit{display:grid;border:1px solid #3f4751;border-radius:14px;margin:1.25rem 0;overflow:hidden;background:#141a21;font-size:.6rem;grid-template-columns:260px 1fr;transition:background .25s,border-color .25s,box-shadow .25s;position:relative;box-shadow:0 2px 5px -2px #000,0 0 0 1px #212a33}
.unit.alt{background:#10161c}
.unit:before{content:'';position:absolute;left:0;top:0;bottom:0;width:4px;background:#30363d}
.unit.eff-band-low:before{background:linear-gradient(#8b1111,#d53030)}
.unit.eff-band-mid:before{background:linear-gradient(#9a7300,#d6a400)}
.unit.eff-band-high:before{background:linear-gradient(#1d7f36,#28c14f)}
.unit.complete{background:#10291a;border-color:#2e8045;box-shadow:0 0 0 1px #2e8045,0 0 4px -1px #184d2b}
.unit-col1{grid-row:1 / span 4;padding:.75rem .95rem;border-right:1px solid #30363d;display:flex;flex-direction:column;gap:.55rem;background:linear-gradient(145deg,#12181f,#151e27 55%,#10161c)}
.com-card{background:linear-gradient(160deg,#0b141b,#0e1d28);border:1px solid #3a4a59;border-radius:12px;padding:.6rem .7rem .7rem;display:flex;flex-direction:column;gap:.6rem;box-shadow:0 2px 4px -2px #000,0 0 0 1px #18232c,0 0 10px -4px #0d3044}
.title{font-family:ui-monospace,Consolas,'Courier New',monospace;font-size:.83rem;font-weight:700;letter-spacing:.12rem;background:#0f161d;border:1px solid #2d3842;padding:.3rem .55rem .32rem;border-radius:8px;display:inline-block;box-shadow:0 0 0 1px #121a21,0 0 4px #0b0f13 inset}
.job{opacity:.7;font-size:.55rem;line-height:1.2}
.daysbox{display:flex;gap:.4rem;font-size:.55rem}
.daysbox span{background:#1d272f;padding:2px 6px;border:1px solid #2d3842;border-radius:6px}
.dept-row{display:flex;flex-wrap:wrap;gap:.4rem;padding:.5rem .7rem .55rem;border-bottom:1px solid #222b33}
.dept{flex:0 0 auto;background:#1d232a;border:1px solid #2d333b;padding:.45rem .55rem;border-radius:6px;min-width:120px;position:relative;transition:background .25s,border-color .25s}
.dept.complete{background:#142f1d;border-color:#2e8045}
.dept-name{font-size:.55rem;font-weight:600;margin-bottom:.25rem}
.bars{display:flex;flex-direction:column;gap:2px}
.bar{height:10px;background:#262c33;border-radius:5px;position:relative;overflow:hidden}
.bar span{position:absolute;left:0;top:0;bottom:0;background:linear-gradient(90deg,#ff914d,#ffcd3c)}
.bar.eff span{background:linear-gradient(90deg,#2f9e44,#52d96d)}
.bar.eff.low span{background:linear-gradient(90deg,#b32020,#ff5959)}
.bar.eff.mid span{background:linear-gradient(90deg,#c28a00,#ffd43b)}
.bar.comp span{background:linear-gradient(90deg,#4373d9,#6da8ff)}
.bar.eff.over span{background:linear-gradient(90deg,#52d96d,#2f9e44)}
.bar.comp.over span{background:linear-gradient(90deg,#6da8ff,#4373d9)}
/* Only recolor completion bars on complete items; keep efficiency threshold colors */
.dept.complete .bar.comp span,.unit.complete .bar.comp span{background:linear-gradient(90deg,#2f9e44,#52d96d)}
.ovr-rows{display:flex;flex-direction:column;gap:4px;padding:.6rem .7rem .7rem}
.unit-sep{height:16px;margin:-.4rem 0 .2rem;position:relative}
.unit-sep:after{content:"";position:absolute;left:0;right:0;top:6px;height:4px;background:linear-gradient(90deg,#141b22,#3d4a57,#141b22);opacity:.85;border-radius:2px}
.metrics{font-size:.52rem;opacity:.8;display:flex;flex-wrap:wrap;gap:.6rem}
.pct-label{font-size:.48rem;position:absolute;right:4px;top:0;bottom:0;display:flex;align-items:center;font-weight:600;text-shadow:0 0 2px #000}
</style></head><body><header><h1>Incomplete AHUs</h1><div><button onclick='loadData()'>Refresh</button></div></header><main>
<div id='loading' style='font-size:.7rem;opacity:.7;'>Loading...</div><div id='summary'></div><div id='units'></div>
</main><script>
function pctText(v){if(v>100){return '100%+'}return v.toFixed(1)+'%'}
function makeBar(pct,cls){
    let extra='';
    if(cls==='eff'){
        if(pct<45) extra=' low'; else if(pct<65) extra=' mid';
    }
    const div=document.createElement('div');div.className='bar '+cls+extra+(pct>100?' over':'');
    const span=document.createElement('span');span.style.width=Math.min(pct,100)+'%';div.appendChild(span);
    const lab=document.createElement('div');lab.className='pct-label';lab.textContent=pctText(pct);div.appendChild(lab);return div}
async function loadData(){
    const l=document.getElementById('loading');l.style.display='block';
    const r=await fetch('/api/incomplete');const data=await r.json();
    const unitsDiv=document.getElementById('units');const sDiv=document.getElementById('summary');
    unitsDiv.innerHTML='';sDiv.innerHTML='';
    let tEff=0,tComp=0;data.units.forEach(u=>{tEff+=u.overall_efficiency;tComp+=u.overall_completion});
    const avgEff=data.count?(tEff/data.count).toFixed(1):'0.0';
    const avgComp=data.count?(tComp/data.count).toFixed(1):'0.0';
    sDiv.innerHTML=`<span class='pill'>${data.count} Units</span><span class='pill'>Avg Eff ${avgEff}%</span><span class='pill'>Avg Comp ${avgComp}%</span>`;
    data.units.forEach((u,idx)=>{
        if(idx>0){const sep=document.createElement('div');sep.className='unit-sep';unitsDiv.appendChild(sep);}        
        const unit=document.createElement('div');unit.className='unit'+(idx%2===1?' alt':'')+(u.overall_completion>=100?' complete':'');
        // Efficiency band accent
        if(u.overall_efficiency<45) unit.classList.add('eff-band-low'); else if(u.overall_efficiency<65) unit.classList.add('eff-band-mid'); else unit.classList.add('eff-band-high');
        // Left merged column
    const c1=document.createElement('div');c1.className='unit-col1';
    const card=document.createElement('div');card.className='com-card';
    const title=document.createElement('div');title.className='title';title.textContent=u.com; card.appendChild(title);
    const job=document.createElement('div');job.className='job';job.textContent=u.jobname||'';card.appendChild(job);
    // Days (aggregate unique across departments where available)
    let daysAct=null, span=null;
    u.departments.forEach(d=>{if(d.days_active!==undefined){if(daysAct===null||d.days_active>daysAct) daysAct=d.days_active;}if(d.days_span!==undefined){if(span===null||d.days_span>span) span=d.days_span;}});
    const daysBox=document.createElement('div');daysBox.className='daysbox';
    if(daysAct!==null) daysBox.innerHTML+=`<span>Days ${daysAct}</span>`;
    if(span!==null) daysBox.innerHTML+=`<span>Span ${span}</span>`;
    card.appendChild(daysBox);
    const metrics=document.createElement('div');metrics.className='metrics';
    metrics.textContent=`Std ${u.overall_std}h  Act ${u.overall_act}h  EH ${u.overall_eff_actual}h`;
    card.appendChild(metrics);
    c1.appendChild(card);
    unit.appendChild(c1);
        // Row 1: departments (dual bars per dept)
        const deptRow=document.createElement('div');deptRow.className='dept-row';
    u.departments.forEach(d=>{if(d.std<=0) return; const isDeptComplete=d.completion>=100; const box=document.createElement('div');box.className='dept'+(isDeptComplete?' complete':'');
                const name=document.createElement('div');name.className='dept-name';name.textContent=d.name;box.appendChild(name);
                const bars=document.createElement('div');bars.className='bars';
    // Efficiency bar top (always threshold-based coloring)
    bars.appendChild(makeBar(d.efficiency,'eff'));
    // Completion bar bottom
    bars.appendChild(makeBar(d.completion,'comp'));
                box.appendChild(bars);
                deptRow.appendChild(box);
        });
        unit.appendChild(deptRow);
        // Overall bars rows (2 rows)
        const overallWrap=document.createElement('div');overallWrap.className='ovr-rows';
        const effBar=makeBar(u.overall_efficiency,'eff');
        const compBar=makeBar(u.overall_completion,'comp');
        const effLabel=document.createElement('div');effLabel.style.cssText='font-size:.5rem;margin-top:2px;';effLabel.textContent='Overall Efficiency';
        const compLabel=document.createElement('div');compLabel.style.cssText='font-size:.5rem;margin-top:6px;';compLabel.textContent='Overall Completion';
        overallWrap.appendChild(effLabel);overallWrap.appendChild(effBar);overallWrap.appendChild(compLabel);overallWrap.appendChild(compBar);
        unit.appendChild(overallWrap);
        unitsDiv.appendChild(unit);
    });
    l.style.display='none';
}
loadData();
</script></body></html>"""


@app.route('/')
def index():
    return render_template_string(PAGE)


if __name__ == '__main__':  # pragma: no cover
    app.run(host='0.0.0.0', port=5000, debug=True)
