"""
Dashboard Blueprint - HTML page routes

All user-facing HTML pages are defined here.
"""
from flask import Blueprint, redirect, url_for, render_template_string, request
import html
import os
import sys
from .utils import get_conn

# Import version info
try:
    # Add parent directory to path to import version
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    from version import VERSION, VERSION_DATE
except ImportError:
    # Fallback if version.py doesn't exist
    VERSION = "1.2.0"
    VERSION_DATE = "2025-10-07"

# Create blueprint
dashboard = Blueprint('dashboard', __name__)

# Template constants
RECENT_PAGE = """<!doctype html><html><head><meta charset='utf-8'><title>Recent Unit Completion</title><style>
body{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}
header{padding:1rem 1.5rem;display:flex;flex-direction:column;gap:.75rem;background:#161b22;border-bottom:1px solid #30363d}
.header-row{display:flex;justify-content:space-between;align-items:center;gap:.75rem;width:100%}
h1{margin:0;font-size:1.05rem}
nav a{color:#8fb9ff;text-decoration:none;margin-right:.8rem;font-size:.72rem}
button{background:#238636;border:1px solid #2ea043;color:#fff;padding:.55rem .9rem;border-radius:6px;font-size:.7rem;font-weight:600;cursor:pointer}button:hover{background:#2ea043}
main{padding:1rem 1.1rem}
.alpha-banner{width:100%;background:linear-gradient(135deg,#d97706,#92400e);border:1px solid rgba(255,255,255,0.22);border-radius:10px;padding:.65rem .9rem;font-size:.68rem;line-height:1.4;font-weight:600;color:#fdf2f2;box-shadow:0 6px 18px -14px #000;letter-spacing:.02rem}
.alpha-banner strong{display:block;font-size:.7rem;margin-bottom:.25rem;color:#ffe2cc}
.pill{display:inline-block;background:#1f6feb33;border:1px solid #1f6feb55;border-radius:20px;padding:.45rem .75rem;font-size:.6rem;letter-spacing:.5px;margin:.25rem .4rem .6rem 0}
.unit{display:grid;border:1px solid #3f4751;border-radius:14px;margin:1.25rem 0;overflow:hidden;background:#141a21;font-size:.6rem;grid-template-columns:260px 1fr;transition:background .25s,border-color .25s,box-shadow .25s,transform .2s;position:relative;box-shadow:0 2px 5px -2px #000,0 0 0 1px #212a33;cursor:pointer}
.unit:hover{transform:translateY(-2px);box-shadow:0 4px 12px -2px rgba(0,0,0,.4),0 0 0 1px #3a4a5f,0 0 8px -2px rgba(31,111,235,.3);border-color:#4a5a6f}
.unit.alt{background:#10161c}
.unit:before{content:'';position:absolute;left:0;top:0;bottom:0;width:4px;background:#30363d}
.unit.eff-band-low:before{background:linear-gradient(#8b1111,#d53030)}
.unit.eff-band-mid:before{background:linear-gradient(#9a7300,#d6a400)}
.unit.eff-band-high:before{background:linear-gradient(#1d7f36,#28c14f)}
.unit.complete{background:linear-gradient(150deg,#18122a,#140e24 58%,#1f1338);border-color:#7b5cd6;box-shadow:0 0 0 1px #7b5cd6,0 0 10px -3px rgba(123,92,214,.55)}
.unit-col1{grid-row:1 / span 4;padding:.75rem .95rem;border-right:1px solid #30363d;display:flex;flex-direction:column;gap:.55rem;background:linear-gradient(145deg,#12181f,#151e27 55%,#10161c)}
.com-card{background:linear-gradient(160deg,#0b141b,#0e1d28);border:1px solid #3a4a59;border-radius:12px;padding:.6rem .7rem .7rem;display:flex;flex-direction:column;gap:.6rem;box-shadow:0 2px 4px -2px #000,0 0 0 1px #18232c,0 0 10px -4px #0d3044}
.title{font-family:ui-monospace,Consolas,'Courier New',monospace;font-size:.83rem;font-weight:700;letter-spacing:.12rem;background:#0f161d;border:1px solid #2d3842;padding:.3rem .55rem .32rem;border-radius:8px;display:inline-block;box-shadow:0 0 0 1px #121a21,0 0 4px #0b0f13 inset}
.job{opacity:.7;font-size:.55rem;line-height:1.2}
.daysbox{display:flex;gap:.4rem;font-size:.55rem}
.daysbox span{background:#1d272f;padding:2px 6px;border:1px solid #2d3842;border-radius:6px}
.dept-row{display:flex;flex-wrap:wrap;gap:.4rem;padding:.5rem .7rem .55rem;border-bottom:1px solid #222b33}
.dept{flex:0 0 auto;background:#1d232a;border:1px solid #2d333b;padding:.55rem .65rem;border-radius:6px;min-width:140px;position:relative;display:flex;flex-direction:column;gap:.35rem;transition:background .25s,border-color .25s,color .25s}
.dept.zero{background:#2b161a;border-color:#a23d4a;color:#f4d7dd}
.dept.zero .dept-values span{color:#f2c2cb}
.dept.partial{background:#242417;border-color:#6f6224}
.dept.partial .dept-values span{color:#d8cd83}
.dept.complete{background:linear-gradient(150deg,#173525,#1d3f2c);border-color:#2e8045;color:#d6f3dc}
.dept.complete .dept-values span{color:#9fd7aa}
.dept-name{font-size:.56rem;font-weight:600}
.dept-values{display:flex;gap:.6rem;font-size:.5rem;opacity:.8;flex-wrap:wrap}
.dept-values span{white-space:nowrap}
.overall-metrics{margin:.6rem .7rem .7rem auto;font-size:.54rem;display:flex;flex-direction:column;gap:.3rem;align-items:flex-end}
.overall-metrics .metric-line{display:flex;gap:.6rem;align-items:center;color:#c9d1d9}
.overall-metrics .metric-line span:first-child{opacity:.75}
.overall-metrics .metric-line span:last-child{font-weight:700;font-size:.6rem}
.unit-sep{height:16px;margin:-.4rem 0 .2rem;position:relative}
.unit-sep:after{content:"";position:absolute;left:0;right:0;top:6px;height:4px;background:linear-gradient(90deg,#141b22,#3d4a57,#141b22);opacity:.85;border-radius:2px}
.metrics{font-size:.52rem;opacity:.8;display:flex;flex-wrap:wrap;gap:.6rem}
.pct-label{font-size:.48rem;position:absolute;right:4px;top:0;bottom:0;display:flex;align-items:center;font-weight:600;text-shadow:0 0 2px #000}
.nav-links{padding:0.5rem 1.5rem;display:flex;gap:1rem;background:#0d1117}
.nav-links a{color:#8fb9ff;text-decoration:none;font-size:.85rem}
.nav-links a:hover{text-decoration:underline}
</style></head><body><header><div class='header-row'><h1>Recent Unit Completion</h1><div><button onclick='loadData()'>Refresh</button></div></div><p class='alpha-banner'><strong>Alpha testing preview</strong>These dashboards are experimental. Data may be incomplete or inaccurate—do not rely on them for operational or business decisions.</p></header>
<div class='nav-links'><a href='/dash'>&larr; Dashboard</a></div>
<main>
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
    const inProg = (typeof data.in_progress_count==='number') ? data.in_progress_count : data.units.filter(u=>u.overall_completion<99.999).length;
    sDiv.innerHTML=`<span class='pill'>${inProg} Units in Progress</span>`;
    data.units.forEach((u,idx)=>{
        if(idx>0){const sep=document.createElement('div');sep.className='unit-sep';unitsDiv.appendChild(sep);}        
        const unit=document.createElement('div');unit.className='unit'+(idx%2===1?' alt':'')+(u.overall_completion>=100?' complete':'');
        // Efficiency band accent
        if(u.overall_efficiency<45) unit.classList.add('eff-band-low'); else if(u.overall_efficiency<65) unit.classList.add('eff-band-mid'); else unit.classList.add('eff-band-high');
        // Make unit clickable - navigate to COM lookup page
        unit.addEventListener('click', function(){ window.location.href='/com?com='+encodeURIComponent(u.com); });
        // Left merged column
    const c1=document.createElement('div');c1.className='unit-col1';
    const card=document.createElement('div');card.className='com-card';
    const title=document.createElement('div');title.className='title';title.textContent=u.com; card.appendChild(title);
    const job=document.createElement('div');job.className='job';job.textContent=u.jobname||'';card.appendChild(job);
    // Unit-level days/span metrics (aggregated across all departments)
    const daysBox=document.createElement('div');daysBox.className='daysbox';
    // Only show Days if unit has data
    if(u.unit_days_worked!==undefined && u.unit_days_worked > 0) {
        daysBox.innerHTML+=`<span>Days ${u.unit_days_worked}</span>`;
    }
    // Only show Span if unit is complete (has actual span calculated)
    if(u.unit_span!==undefined && u.unit_span !== null) {
        daysBox.innerHTML+=`<span>Span ${u.unit_span}</span>`;
    } else if(u.unit_span_status && u.unit_span_status !== 'Complete') {
        // Show status only (In Progress, No Data, Error) without span number
        daysBox.innerHTML+=`<span>${u.unit_span_status}</span>`;
    }
    card.appendChild(daysBox);
    const metrics=document.createElement('div');metrics.className='metrics';
    metrics.textContent=`Std ${u.overall_std}h  Act ${u.overall_act}h  EH ${u.overall_eff_actual}h`;
    card.appendChild(metrics);
    c1.appendChild(card);
    unit.appendChild(c1);
        // Row 1: departments (dual bars per dept)
        const deptRow=document.createElement('div');deptRow.className='dept-row';
    const sortedDepts=[...u.departments].filter(d=>d.std>0).sort((a,b)=>{
        const aComplete=a.completion>=100?1:0;
        const bComplete=b.completion>=100?1:0;
        if(aComplete!==bComplete){
            return bComplete-aComplete; // completed to the left
        }
        return b.completion-a.completion;
    });
    sortedDepts.forEach(d=>{const compVal = Number(d.completion) || 0;
                const effVal = Number(d.efficiency) || 0;
                let statusClass=' partial';
                if(compVal<=0){statusClass=' zero';}
                else if(compVal>=100){statusClass=' complete';}
                const box=document.createElement('div');box.className='dept'+statusClass;
                const name=document.createElement('div');name.className='dept-name';name.textContent=d.name;box.appendChild(name);
                const values=document.createElement('div');values.className='dept-values';
                values.innerHTML=`<span>${compVal.toFixed(1)}% complete</span><span>${effVal.toFixed(1)}% eff</span>`;
                box.appendChild(values);
                deptRow.appendChild(box);
        });
        unit.appendChild(deptRow);
        // Overall metrics summary (numbers only)
        const overallWrap=document.createElement('div');overallWrap.className='overall-metrics';
        const effLine=document.createElement('div');effLine.className='metric-line';
    effLine.innerHTML=`<span>Overall Efficiency</span><span>${(Number(u.overall_efficiency)||0).toFixed(1)}%</span>`;
        const compLine=document.createElement('div');compLine.className='metric-line';
    compLine.innerHTML=`<span>Overall Completion</span><span>${(Number(u.overall_completion)||0).toFixed(1)}%</span>`;
        overallWrap.appendChild(effLine);overallWrap.appendChild(compLine);
        unit.appendChild(overallWrap);
        unitsDiv.appendChild(unit);
    });
    l.style.display='none';
}
loadData();
</script></body></html>"""

# Routes will be moved here one at a time

@dashboard.route('/')
def root():
        # Redirect to the new dashboard landing
        return redirect(url_for('dashboard.dash'))


@dashboard.route('/dash')
def dash():
        # Minimal landing page with buttons to reports
        page = """
        <!doctype html>
        <html><head><meta charset='utf-8'><title>Dashboard</title>
        <style>
            body{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}
            header{padding:1rem 1.5rem;background:#161b22;border-bottom:1px solid #30363d;display:flex;flex-direction:column;gap:.75rem}
            .header-row{display:flex;justify-content:space-between;align-items:center;gap:.75rem;width:100%}
            h1{margin:0;font-size:1.15rem}
            main{padding:1.5rem}
            .grid{display:grid;gap:1rem;grid-template-columns:repeat(auto-fill,minmax(260px,1fr))}
            .card{background:#11161d;border:1px solid #2a323c;border-radius:12px;padding:1rem 1.1rem;box-shadow:0 6px 18px -8px #000}
            .card h2{margin:.1rem 0 .6rem;font-size:.95rem}
            .card p{margin:0 0 1rem;opacity:.8;font-size:.75rem}
            .actions{display:flex;gap:.6rem;flex-wrap:wrap}
            a.btn,button{background:#238636;border:1px solid #2ea043;color:#fff;padding:.55rem .9rem;border-radius:8px;font-size:.8rem;font-weight:700;cursor:pointer;text-decoration:none;display:inline-block}
            a.btn.secondary{background:#1f6feb;border-color:#3a78e0}
            a.btn:hover{filter:brightness(1.05)}
            .metrics{display:flex;gap:1rem;flex-wrap:wrap;margin:1.2rem 0 1.6rem}
            .pill{display:inline-block;background:#0f1a2a;border:1px solid #2a3b55;border-radius:16px;padding:.8rem 1rem;box-shadow:0 4px 10px -6px #000;min-width:240px}
            .kpi-title{font-size:.85rem;opacity:.85;margin-bottom:.2rem}
            .kpi-value{font-size:1.8rem;font-weight:800;letter-spacing:.5px}
            .kpi-trend{margin-left:.5rem;font-size:.9rem;font-weight:700}
            .pill .small{opacity:.8;font-size:.72rem}
            /* Increase in days is bad (red), decrease is good (green) */
            .up{color:#f85149}
            .down{color:#3fb950}
            header{box-shadow:0 3px 10px -6px #000}
            .alpha-banner{width:100%;background:linear-gradient(135deg,#d97706,#92400e);border:1px solid rgba(255,255,255,0.22);border-radius:10px;padding:.7rem .95rem;font-size:.72rem;line-height:1.4;font-weight:600;color:#fdf2f2;box-shadow:0 6px 18px -14px #000;letter-spacing:.02rem}
            .alpha-banner strong{display:block;font-size:.78rem;margin-bottom:.25rem;color:#ffe2cc;text-transform:uppercase;letter-spacing:.05rem}
            .card{transition:transform .12s ease, box-shadow .12s ease}
            .card:hover{transform:translateY(-2px); box-shadow:0 8px 22px -10px #000}
            /* Legend styling for Daily Hours chart */
            .legendWrap{display:flex;flex-wrap:wrap;gap:.5rem;margin:.4rem 0 .6rem}
            .legend-item{display:inline-flex;align-items:center;gap:.45rem;background:#0f1a2a;border:1px solid #2a3b55;border-radius:16px;padding:.28rem .6rem;font-size:.85rem;cursor:pointer;user-select:none}
            .legend-item.off{opacity:.5;border-color:#3a3f47}
            .legend-dot{width:12px;height:12px;border-radius:50%;box-shadow:0 0 0 1px #0007 inset}
            .legend-item:hover{filter:brightness(1.08)}
            /* Chart button styling */
            .chart-btn{background:#21262d;border:1px solid #30363d;color:#8b949e;padding:.4rem .75rem;border-radius:6px;font-size:.75rem;font-weight:600;cursor:pointer;transition:all .2s}
            .chart-btn:hover{background:#30363d;border-color:#484f58;color:#c9d1d9}
            .chart-btn.active{background:#238636;border-color:#2ea043;color:#fff}
            .pie-card{margin-top:2rem;background:#161b22;border:1px solid #30363d;border-radius:12px;padding:1.5rem;box-shadow:0 6px 18px -12px #000}
            .pie-header{display:flex;justify-content:space-between;align-items:center;gap:1rem;margin-bottom:1.2rem}
            .pie-header h2{margin:0;font-size:1.05rem}
            .pie-controls{display:flex;gap:.6rem;flex-wrap:wrap}
            .pie-layout{display:flex;flex-wrap:wrap;gap:1.8rem;align-items:center;justify-content:center}
            #deptPieCanvas{max-width:380px;max-height:380px}
            .pie-legend{display:flex;flex-direction:column;gap:.55rem;min-width:220px}
            .pie-legend-item{display:flex;align-items:center;gap:.6rem;background:#0f1a2a;border:1px solid #2a3b55;border-radius:12px;padding:.45rem .7rem;font-size:.78rem;box-shadow:0 2px 6px -6px #000}
            .pie-legend-swatch{width:14px;height:14px;border-radius:50%;box-shadow:0 0 0 1px #0007 inset}
            .pie-legend-item span.label{font-weight:600}
            .pie-legend-item span.value{margin-left:auto;font-weight:600;font-size:.78rem;color:#d0d7de}
            .pie-summary{margin-top:1.2rem;text-align:center;font-size:.75rem;opacity:.75}
            .pie-empty{padding:1.2rem;text-align:center;font-size:.85rem;opacity:.7}
            .daily-metrics-card{margin-top:2rem;background:#161b22;border:1px solid#30363d;border-radius:12px;padding:1.5rem;box-shadow:0 6px 18px -12px #000}
            .daily-header{display:flex;justify-content:space-between;align-items:center;gap:1rem;margin-bottom:1.2rem}
            .daily-header h2{margin:0;font-size:1.05rem}
            .daily-controls{display:flex;gap:.6rem;flex-wrap:wrap}
            footer{margin-top:3rem;padding:1.5rem;text-align:center;border-top:1px solid #30363d;background:#0d1117}
            footer .version{font-size:.72rem;opacity:.7;margin-bottom:.4rem}
            footer .links{display:flex;gap:1rem;justify-content:center;font-size:.7rem}
            footer .links a{color:#8fb9ff;text-decoration:none}
            footer .links a:hover{text-decoration:underline}
            .daily-gauge-grid{display:grid;gap:1.4rem;grid-template-columns:repeat(auto-fit,minmax(230px,1fr))}
            .daily-status{margin-top:1rem;text-align:center;font-size:.75rem;opacity:.75}
            .daily-empty{padding:1rem;text-align:center;font-size:.85rem;opacity:.7}
            .gauge-card{background:#0f1a2a;border:1px solid #2a3b55;border-radius:12px;padding:1.4rem;display:flex;flex-direction:column;gap:1rem;align-items:center;box-shadow:0 2px 6px -8px #000}
            .gauge-card.metric-gauge{margin-top:0}
            .gauge-header{width:100%;display:flex;flex-direction:column;gap:.4rem;text-align:center}
            .gauge-header h3{margin:0;font-size:1rem}
            .gauge-header .gauge-meta{font-size:.74rem;opacity:.75;display:flex;justify-content:center;flex-wrap:wrap;gap:.6rem}
            .gauge-header .gauge-meta span{white-space:nowrap}
            .gauge-body{display:flex;justify-content:center;align-items:center;width:100%}
            .gauge{position:relative;width:220px;aspect-ratio:1;border-radius:50%;background:#131c27;display:flex;align-items:center;justify-content:center;box-shadow:0 0 0 1px #1f2a38,0 12px 30px -18px #000}
            .gauge-fill{position:absolute;inset:0;border-radius:50%;background:conic-gradient(#58a6ff 0deg,#1b2531 0deg);transition:background .4s ease}
            .gauge::after{content:"";position:absolute;inset:18%;border-radius:50%;background:#0d1117;box-shadow:inset 0 0 18px -12px #000}
            .gauge-center{position:relative;display:flex;flex-direction:column;align-items:center;gap:.35rem;z-index:1}
            .gauge-value{font-size:1.4rem;font-weight:700;color:#d1e3ff}
            .gauge-range{font-size:.72rem;opacity:.7}
        </style></head>
        <body>
            <header>
                <div class='header-row'>
                    <h1>Production Dashboard</h1>
                    <div style='font-size:.75rem;opacity:.7;'>Version {{ version }} ({{ version_date }}) • <a href='/changelog' target='_blank' rel='noopener' style='color:#8fb9ff;text-decoration:none;' title='View changelog'>Changelog</a></div>
                </div>
                <p class='alpha-banner'><strong>Alpha testing preview</strong>These dashboards are experimental. Data may be incomplete or inaccurate—do not rely on them for operational or business decisions.</p>
            </header>
            <main>
                <div class='grid'>
                    <div class='card'>
                        <h2>Recent Unit Completion</h2>
                        <p>List active units in progress and recently completed within 3 days.</p>
                        <div class='actions'><a class='btn' href='/recent'>Open</a></div>
                    </div>
                    <div class='card'>
                        <h2>DR Labor Lookup</h2>
                        <p>Look up labor by DR number across departments and employees.</p>
                        <div class='actions'><a class='btn secondary' href='/dr'>Open</a></div>
                    </div>
                    <div class='card'>
                        <h2>COM# Charges</h2>
                        <p>See who charged time, how much, and when for a COM#.</p>
                        <div class='actions'><a class='btn' href='/com'>Open</a></div>
                    </div>
                    <div class='card'>
                        <h2>Employee Lookup</h2>
                        <p>Find an employee and view hours by department for a date range.</p>
                        <div class='actions'><a class='btn' href='/emp'>Open</a></div>
                    </div>
                    <div class='card'>
                        <h2>Parts Tracker</h2>
                        <p>Lookup parts by Part Number or COM# from the tracked CSV.</p>
                        <div class='actions'><a class='btn secondary' href='/parts'>Open</a></div>
                    </div>
                    <div class='card'>
                        <h2>Total Daily Hours Charged Chart</h2>
                        <p>View daily hours trends across all departments with 7-day trailing average.</p>
                        <div class='actions'><a class='btn' href='/hours-chart'>View Chart</a></div>
                    </div>
                    <div class='card'>
                        <h2>📊 Unit Reports</h2>
                        <p>Export detailed CSV reports with filtered and unfiltered department day analysis.</p>
                        <div class='actions'><a class='btn' href='/reports' style='background: linear-gradient(135deg, #238636 0%, #2ea043 100%);'>Create Report</a></div>
                    </div>
                </div>
                <div id='unitMetrics' class='metrics'>
                    <span class='pill'>Loading unit metrics…</span>
                </div>
                
                <!-- Trailing Metrics Trend Chart -->
                <div style='margin-top:2.5rem;background:#161b22;border:1px solid #30363d;border-radius:12px;padding:1.5rem;'>
                    <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:1.5rem;'>
                        <h2 style='margin:0;font-size:1.1rem;'>📈 Metrics Trend (100% Complete Units)</h2>
                        <div style='display:flex;gap:1rem;'>
                            <div style='display:flex;gap:0.5rem;'>
                                <button class='chart-btn' data-type='days' data-value='90'>90 Days</button>
                                <button class='chart-btn active' data-type='days' data-value='120'>120 Days</button>
                                <button class='chart-btn' data-type='days' data-value='365'>1 Year</button>
                            </div>
                            <div style='border-left:1px solid #30363d;padding-left:1rem;display:flex;gap:0.5rem;'>
                                <button class='chart-btn active' data-type='trailing' data-value='10'>Trailing 10</button>
                                <button class='chart-btn' data-type='trailing' data-value='30'>Trailing 30</button>
                            </div>
                        </div>
                    </div>
                    <div style='margin-bottom:1rem;'>
                        <canvas id='trailingMetricsChart' width='1600' height='500'></canvas>
                    </div>
                    <div id='trailingLegend' class='legend' style='display:flex;gap:1.5rem;justify-content:center;flex-wrap:wrap;'></div>
                    <div id='trailingChartInfo' style='text-align:center;margin-top:1rem;font-size:0.75rem;opacity:0.6;'></div>
                    </div>
                    <div class='pie-card'>
                        <div class='pie-header'>
                            <h2>Department Hours (Last 30 Days)</h2>
                            <div class='pie-controls'>
                                <button class='chart-btn pie-btn active' data-pie-days='30'>30 Days</button>
                                <button class='chart-btn pie-btn' data-pie-days='60'>60 Days</button>
                                <button class='chart-btn pie-btn' data-pie-days='90'>90 Days</button>
                            </div>
                        </div>
                        <div id='deptPieContent' class='pie-layout'>
                            <canvas id='deptPieCanvas' width='380' height='380'></canvas>
                            <div id='deptPieLegend' class='pie-legend'></div>
                        </div>
                        <div id='deptPieSummary' class='pie-summary'></div>
                    </div>
                    <div class='daily-metrics-card'>
                        <div class='daily-header'>
                            <h2>Daily Metrics Pulse</h2>
                            <div class='daily-controls'>
                                <button class='chart-btn daily-btn active' data-daily-days='30'>30 Days</button>
                                <button class='chart-btn daily-btn' data-daily-days='60'>60 Days</button>
                                <button class='chart-btn daily-btn' data-daily-days='90'>90 Days</button>
                            </div>
                        </div>
                        <div id='dailyMetricsContent' class='daily-gauge-grid'>
                            <div class='gauge-card metric-gauge'>
                                <div class='gauge-header'>
                                    <h3>Average Efficiency</h3>
                                    <div class='gauge-meta' id='gaugeEffMeta'></div>
                                </div>
                                <div class='gauge-body'>
                                    <div class='gauge' id='gaugeEff'>
                                        <div class='gauge-fill'></div>
                                        <div class='gauge-center'>
                                            <div class='gauge-value' id='gaugeEffValue'>0%</div>
                                            <div class='gauge-range' id='gaugeEffRange'></div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div class='gauge-card metric-gauge'>
                                <div class='gauge-header'>
                                    <h3>Average Active Days</h3>
                                    <div class='gauge-meta' id='gaugeActMeta'></div>
                                </div>
                                <div class='gauge-body'>
                                    <div class='gauge' id='gaugeAct'>
                                        <div class='gauge-fill'></div>
                                        <div class='gauge-center'>
                                            <div class='gauge-value' id='gaugeActValue'>0 days</div>
                                            <div class='gauge-range' id='gaugeActRange'></div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div class='gauge-card metric-gauge'>
                                <div class='gauge-header'>
                                    <h3>Average Span Days</h3>
                                    <div class='gauge-meta' id='gaugeSpanMeta'></div>
                                </div>
                                <div class='gauge-body'>
                                    <div class='gauge' id='gaugeSpan'>
                                        <div class='gauge-fill'></div>
                                        <div class='gauge-center'>
                                            <div class='gauge-value' id='gaugeSpanValue'>0 days</div>
                                            <div class='gauge-range' id='gaugeSpanRange'></div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div id='dailyMetricsStatus' class='daily-status'></div>
                    </div>
                </main>
            <script>
            function pct(n){return (Math.round(n*10)/10).toFixed(1)}
            function arrow(delta){
                if(Math.abs(delta) < 0.05) return '<span class="small">(flat vs last 90 days)</span>';
                // Increase in days is bad -> red; decrease good -> green
                return delta>0 ? '<span class="up">(+'+pct(delta)+' vs last 90 days)</span>' : '<span class="down">(-'+pct(-delta)+' vs last 90 days)</span>';
            }
            function formatTimestamp(isoStr) {
                const d = new Date(isoStr);
                return d.toLocaleString('en-US', { 
                    month: 'short', 
                    day: 'numeric', 
                    hour: 'numeric', 
                    minute: '2-digit',
                    hour12: true 
                });
            }
            fetch('/api/metrics/unit_time_trends').then(r=>r.json()).then(m=>{
                const c=document.getElementById('unitMetrics');
                if(m.error){ c.innerHTML = '<span class="pill">'+m.error+'</span>'; return; }
                const a10=m.last10||{}; const a90=m.last90d||{}; const tr=m.trend||{};
                const dEff = (a10.avg_efficiency||0) - (a90.avg_efficiency||0);
                const dActive = (a10.avg_active_days||0) - (a90.avg_active_days||0);
                const dSpan = (a10.avg_span_days||0) - (a90.avg_span_days||0);
                const effArrow = dEff>0 ? '<span class="down">(+'+pct(dEff)+' vs 90d)</span>' : (dEff<0 ? '<span class="up">(-'+pct(-dEff)+' vs 90d)</span>' : '<span class="small">(flat vs 90d)</span>');
                
                // Cache info as plain text below the metrics
                const staleWarning = m._cache && m._cache.is_stale ? '<span style="color:#f85149;font-size:.7rem"> ⚠ Stale data</span>' : '';
                const cacheTimestamp = m._cache ? `<div style="text-align:center;margin-top:1rem;opacity:.6;font-size:.75rem">Metrics updated: ${formatTimestamp(m._cache.computed_at)} (${m._cache.trigger_source})${staleWarning}</div>` : '';
                
                c.innerHTML = `
                    <span class='pill'>
                        <div class='kpi-title'>Average Efficiency</div>
                        <div class='kpi-value'>${pct(a10.avg_efficiency||0)}% <span class='kpi-trend'>${effArrow}</span></div>
                    </span>
                    <span class='pill'>
                        <div class='kpi-title'>Avg Actual Days Worked</div>
                        <div class='kpi-value'>${pct(a10.avg_active_days||0)} <span class='kpi-trend'>${arrow(dActive)}</span></div>
                    </span>
                    <span class='pill'>
                        <div class='kpi-title'>Avg Total Time Span</div>
                        <div class='kpi-value'>${pct(a10.avg_span_days||0)} <span class='kpi-trend'>${arrow(dSpan)}</span></div>
                    </span>
                    ${cacheTimestamp}
                `;
            }).catch(()=>{ document.getElementById('unitMetrics').innerHTML = '<span class="pill">Metrics unavailable</span>'; });

            // Minimal inline charting without external libs
            function lineColor(idx){
                const colors=['#6ea8fe','#a8ff60','#ffd166','#ff6b6b','#64d2ff','#c0a7ff','#9be9a8','#f8a5ff','#ffcd3c','#52d96d','#ffa06a'];
                return colors[idx % colors.length];
            }
        function drawLineChart(ctx, labels, series, opts={}){
            const W=ctx.canvas.width, H=ctx.canvas.height,
                padL=90, padR=22, padT=18, padB=46;
                ctx.clearRect(0,0,W,H);
                // compute y range among visible series
                let ymin=0, ymax=0;
                const vis = series.filter(s=>s.visible!==false);
                vis.forEach(s=>{ s.data.forEach(v=>{ if(v>ymax) ymax=v; }); });
                if(ymax<=0) ymax=1;
                // nice y ticks
                const tickCount=10; // finer Y-axis resolution
                const rawStep=ymax/tickCount;
                const mag=Math.pow(10, Math.floor(Math.log10(rawStep)));
                const res=rawStep/mag;
                const niceFactor = res>=5 ? 5 : (res>=2 ? 2 : 1);
                const yStep = niceFactor*mag;
                const yMaxNice = Math.ceil(ymax / yStep) * yStep;
                const sx=(W-padL-padR)/Math.max(1,labels.length-1);
                const sy=(H-padT-padB)/yMaxNice;
                let idx=0;
                vis.forEach((s)=>{
                    const color = s.color || lineColor(idx++);
                    ctx.strokeStyle=color;ctx.lineWidth=2.0;ctx.beginPath();
                    s.data.forEach((v,idx)=>{
                        const x=padL+idx*sx, y=H-padB - v*sy;
                        if(idx===0) ctx.moveTo(x,y); else ctx.lineTo(x,y);
                    });
                    ctx.stroke();
                });
                // x ticks (dates)
                ctx.fillStyle='#9fb3c8'; ctx.font='12.5px system-ui, sans-serif'; ctx.textAlign='center'; ctx.textBaseline='top';
                const xTickCount = 8;
                const stepIdx = Math.max(1, Math.floor((labels.length-1)/(xTickCount-1)));
                for(let i=0;i<labels.length;i+=stepIdx){
                    const x = padL + i*sx;
                    const lab = labels[i] || '';
                    ctx.strokeStyle='#2a323c'; ctx.beginPath(); ctx.moveTo(x, H-padB); ctx.lineTo(x, H-padB+4); ctx.stroke();
                    ctx.fillText(lab, x, H-padB+6);
                }
            }
            function buildLegend(containerId, series, onToggle){
                const el = document.getElementById(containerId);
                let html = '';
                series.forEach((s, i)=>{
                    const color = s.color || lineColor(i);
                    const off = s.visible===false ? ' off' : '';
                    html += `<div class="legend-item${off}" data-idx="${i}"><span class="legend-dot" style="background:${color}"></span><span>${s.name}</span></div>`;
                });
                el.innerHTML = html;
                el.querySelectorAll('.legend-item').forEach(item=>{
                    item.addEventListener('click', ()=>{
                        const i = Number(item.getAttribute('data-idx'));
                        series[i].visible = series[i].visible===false ? true : false;
                        item.classList.toggle('off', series[i].visible===false);
                        onToggle();
                    });
                });
            }
            function formatHours(val){
                if(val === null || val === undefined) return '0';
                const num = Number(val) || 0;
                return new Intl.NumberFormat('en-US',{maximumFractionDigits:1, minimumFractionDigits:0}).format(num);
            }
            function formatDateRangeLabel(startIso, endIso){
                if(!startIso || !endIso) return '';
                try{
                    const start = new Date(startIso + 'T00:00:00');
                    const end = new Date(endIso + 'T00:00:00');
                    const fmt = new Intl.DateTimeFormat('en-US',{month:'short', day:'numeric'});
                    return `${fmt.format(start)} – ${fmt.format(end)}`;
                }catch(err){
                    return `${startIso} – ${endIso}`;
                }
            }
            const pieColors=['#58a6ff','#7ee787','#fbbc64','#f778ba','#9b9bff','#4fd2c2','#ffa657','#8cddff','#d2a8ff','#6fdd8b','#ff9b9b','#e3b341'];
            function drawDeptPie(ctx, segments, total){
                const {canvas}=ctx;
                const {width,height}=canvas;
                ctx.clearRect(0,0,width,height);
                if(!segments.length || total<=0){
                    return;
                }
                const cx=width/2;
                const cy=height/2;
                const radius=Math.min(width,height)/2 - 12;
                let startAngle=-Math.PI/2;
                segments.forEach(seg=>{
                    const portion = seg.hours/total;
                    const slice = portion*Math.PI*2;
                    if(slice<=0) return;
                    ctx.beginPath();
                    ctx.moveTo(cx,cy);
                    ctx.fillStyle=seg.color;
                    ctx.arc(cx,cy,radius,startAngle,startAngle+slice);
                    ctx.closePath();
                    ctx.fill();
                    startAngle+=slice;
                });
                const innerRadius=radius*0.55;
                ctx.beginPath();
                ctx.fillStyle='#0d1117';
                ctx.arc(cx,cy,innerRadius,0,Math.PI*2);
                ctx.fill();
                ctx.fillStyle='#c9d1d9';
                ctx.textAlign='center';
                ctx.textBaseline='middle';
                ctx.font='600 14px system-ui,-apple-system,sans-serif';
                ctx.fillText('Total Hours',cx,cy-12);
                ctx.font='700 20px system-ui,-apple-system,sans-serif';
                ctx.fillText(`${formatHours(total)}`,cx,cy+14);
            }
            const deptPieState={days:30};
            function loadDeptPie(){
                const canvas=document.getElementById('deptPieCanvas');
                const legend=document.getElementById('deptPieLegend');
                const summary=document.getElementById('deptPieSummary');
                if(!canvas || !legend || !summary){
                    return;
                }
                summary.innerHTML='<span style="opacity:0.7;">Loading…</span>';
                legend.innerHTML='';
                const ctx=canvas.getContext('2d');
                ctx.clearRect(0,0,canvas.width,canvas.height);
                fetch(`/api/metrics/department_totals?days=${deptPieState.days}`)
                    .then(r=>r.json())
                    .then(data=>{
                        let departments=(data.departments||[]).map(d=>({
                            name:d.name,
                            hours:Number(d.hours)||0
                        })).filter(d=>d.hours>0);
                        let total=Number(data.total_hours)||0;
                        if(!(total>0)){
                            total=departments.reduce((sum,d)=>sum+d.hours,0);
                        }
                        if(!departments.length || !(total>0)){
                            legend.innerHTML='';
                            summary.innerHTML=`<div class="pie-empty">No hours logged in the last ${deptPieState.days} days.</div>`;
                            ctx.clearRect(0,0,canvas.width,canvas.height);
                            return;
                        }
                        if(departments.length>8){
                            const primary=departments.slice(0,7);
                            const otherTotal=departments.slice(7).reduce((sum,d)=>sum+d.hours,0);
                            if(otherTotal>0){
                                primary.push({name:'Other', hours:otherTotal});
                            }
                            departments=primary;
                        }
                        const segments=departments.map((dept,idx)=>({
                            ...dept,
                            color:pieColors[idx % pieColors.length],
                            percent: total ? (dept.hours/total)*100 : 0
                        }));
                        drawDeptPie(ctx, segments, total);
                        legend.innerHTML='';
                        segments.forEach(seg=>{
                            const item=document.createElement('div');
                            item.className='pie-legend-item';
                            const swatch=document.createElement('span');
                            swatch.className='pie-legend-swatch';
                            swatch.style.background=seg.color;
                            const label=document.createElement('span');
                            label.className='label';
                            label.textContent=seg.name;
                            const value=document.createElement('span');
                            value.className='value';
                            value.textContent=`${formatHours(seg.hours)}h • ${seg.percent.toFixed(1)}%`;
                            item.appendChild(swatch);
                            item.appendChild(label);
                            item.appendChild(value);
                            legend.appendChild(item);
                        });
                        const rangeLabel=formatDateRangeLabel(data.start_date, data.end_date);
                        const updated = data.generated_at ? formatTimestamp(data.generated_at) : '';
                        const pieces=[];
                        pieces.push(`${formatHours(total)} total hours`);
                        pieces.push(`${data.days}-day window`);
                        if(rangeLabel){
                            pieces.push(rangeLabel);
                        }
                        if(updated){
                            pieces.push(`Updated ${updated}`);
                        }
                        summary.innerHTML=pieces.join(' • ');
                    })
                    .catch(err=>{
                        console.error('Failed to load department totals:', err);
                        summary.innerHTML='<span style="color:#f85149;">Unable to load department totals</span>';
                    });
            }
            const dailyChartState={days:30};
            let dailyMetricDataset=null;
            function mean(values){
                if(!values || !values.length) return 0;
                let total=0;
                values.forEach(v=>{total+=Number(v)||0;});
                return total/values.length;
            }
            function seriesMax(values){
                if(!values || !values.length) return 0;
                let maxVal=-Infinity;
                values.forEach(v=>{
                    const num=Number(v)||0;
                    if(num>maxVal) maxVal=num;
                });
                return maxVal===-Infinity?0:maxVal;
            }
            function computeDailyMaxAverages(data){
                const result={eff:0, act:0, span:0};
                const windows=data.windows||{};
                Object.values(windows).forEach(win=>{
                    if(!win) return;
                    result.eff=Math.max(result.eff, seriesMax(win.avg_efficiency||[]));
                    result.act=Math.max(result.act, seriesMax(win.avg_act_days||[]));
                    result.span=Math.max(result.span, seriesMax(win.avg_span||[]));
                });
                result.eff = Math.max(result.eff, 100); // keep efficiency gauge targeting 100%
                return result;
            }
            function renderDailyCharts(){
                const statusEl=document.getElementById('dailyMetricsStatus');
                if(!dailyMetricDataset || !dailyMetricDataset.windows){
                    statusEl.innerHTML='<span class="daily-empty">Daily metrics unavailable.</span>';
                    return;
                }
                const key=String(dailyChartState.days);
                const windowData=dailyMetricDataset.windows[key];
                if(!windowData || !(windowData.labels||[]).length){
                    statusEl.innerHTML=`<span class="daily-empty">No data for the last ${dailyChartState.days} days.</span>`;
                    return;
                }
                statusEl.innerHTML='';
                const labels=windowData.labels;
                const displayLabels=labels.map(l=>{
                    try{const d=new Date(l+'T00:00:00');return d.toLocaleDateString('en-US',{month:'short',day:'numeric'});}catch{return l;}
                });
                const rangeLabel=formatDateRangeLabel(labels[0], labels[labels.length-1]);
                const cacheInfo=dailyMetricDataset._cache||{};
                const maxAverages=dailyMetricDataset._maxAverages || {eff:100, act:0, span:0};
                const gaugeConfigs=[
                    {
                        key:'avg_efficiency',
                        gaugeId:'gaugeEff',
                        valueId:'gaugeEffValue',
                        rangeId:'gaugeEffRange',
                        metaId:'gaugeEffMeta',
                        formatter:(v)=>`${pct(v)}%`,
                        unitLabel:'% avg',
                        maxValue:100
                    },
                    {
                        key:'avg_act_days',
                        gaugeId:'gaugeAct',
                        valueId:'gaugeActValue',
                        rangeId:'gaugeActRange',
                        metaId:'gaugeActMeta',
                        formatter:(v)=>`${(Number(v)||0).toFixed(1)} days`,
                        maxValue:maxAverages.act || seriesMax(windowData.avg_act_days||[])
                    },
                    {
                        key:'avg_span',
                        gaugeId:'gaugeSpan',
                        valueId:'gaugeSpanValue',
                        rangeId:'gaugeSpanRange',
                        metaId:'gaugeSpanMeta',
                        formatter:(v)=>`${(Number(v)||0).toFixed(1)} days`,
                        maxValue:maxAverages.span || seriesMax(windowData.avg_span||[])
                    }
                ];

                gaugeConfigs.forEach(cfg=>{
                    const series = windowData[cfg.key] || [];
                    const avgValue = mean(series);
                    const maxValue = cfg.maxValue > 0 ? cfg.maxValue : avgValue || 1;
                    const gauge=document.getElementById(cfg.gaugeId);
                    if(gauge){
                        const fill=gauge.querySelector('.gauge-fill');
                        const ratio = maxValue>0 ? Math.max(0, Math.min(1, avgValue/maxValue)) : 0;
                        if(fill){
                            const sweep=Math.min(360, Math.max(0, ratio*360));
                            fill.style.background=`conic-gradient(#58a6ff ${sweep}deg,#1b2531 ${sweep}deg 360deg)`;
                        }
                    }
                    const valueEl=document.getElementById(cfg.valueId);
                    if(valueEl){
                        valueEl.textContent = cfg.formatter(avgValue);
                    }
                    const rangeEl=document.getElementById(cfg.rangeId);
                    if(rangeEl){
                        if(cfg.key==='avg_efficiency'){
                            rangeEl.textContent = '';
                        } else {
                            rangeEl.textContent = maxValue ? `Peak avg ${(Number(maxValue)||0).toFixed(1)} days` : '';
                        }
                    }
                    const metaPieces=[`Window avg ${cfg.formatter(avgValue)}`];
                    if(rangeLabel) metaPieces.push(rangeLabel);
                    metaPieces.push('All units included');
                    if(series && series.length) metaPieces.push(`${series.length} days`);
                    const metaEl=document.getElementById(cfg.metaId);
                    if(metaEl){
                        metaEl.innerHTML=metaPieces.map(v=>`<span>${v}</span>`).join('');
                    }
                });
                if(cacheInfo.computed_at){
                    const updated=formatTimestamp(cacheInfo.computed_at);
                    statusEl.innerHTML=`Charts cached ${updated}${cacheInfo.trigger_source?` • ${cacheInfo.trigger_source}`:''}`;
                }
            }
            function loadDailyMetrics(){
                const statusEl=document.getElementById('dailyMetricsStatus');
                statusEl.innerHTML='<span style="opacity:0.7;">Loading daily charts…</span>';
                fetch('/api/metrics/daily_metric_trends')
                    .then(r=>r.json())
                    .then(data=>{
                        if(data.error){
                            statusEl.innerHTML=`<span style="color:#f85149;">${data.error}</span>`;
                            dailyMetricDataset=null;
                            return;
                        }
                        data._maxAverages = computeDailyMaxAverages(data);
                        dailyMetricDataset=data;
                        renderDailyCharts();
                    })
                    .catch(err=>{
                        console.error('Failed to load daily metric charts:', err);
                        statusEl.innerHTML='<span style="color:#f85149;">Unable to load daily metric charts</span>';
                    });
            }
            
            // Trailing Metrics Chart
            let trailingChartState = { days: 120, trailing: 10 };
            let trailingMetricsSeries = [];
            
            function loadTrailingMetrics() {
                const params = new URLSearchParams({
                    days: trailingChartState.days,
                    trailing: trailingChartState.trailing
                });
                
                document.getElementById('trailingChartInfo').innerHTML = '<span style="opacity:0.8;">Loading...</span>';
                
                fetch('/api/metrics/trailing_trend?' + params)
                    .then(r => r.json())
                    .then(data => {
                        if (data.error) {
                            document.getElementById('trailingChartInfo').innerHTML = `<span style="color:#f85149;">${data.error}</span>`;
                            return;
                        }
                        
                        // Format labels for display (show every Nth date)
                        const displayLabels = data.labels.map(d => {
                            const parts = d.split('-');
                            return parts[1] + '/' + parts[2];  // MM/DD
                        });
                        
                        // Build series data
                        trailingMetricsSeries = [
                            { 
                                name: 'Avg Efficiency %', 
                                data: data.avg_efficiency, 
                                color: '#6ea8fe',
                                visible: true 
                            },
                            { 
                                name: 'Avg Act Days', 
                                data: data.avg_act_days, 
                                color: '#a8ff60',
                                visible: true 
                            },
                            { 
                                name: 'Avg Span Days', 
                                data: data.avg_span, 
                                color: '#ffd166',
                                visible: true 
                            }
                        ];
                        
                        // Draw chart
                        const canvas = document.getElementById('trailingMetricsChart');
                        const ctx = canvas.getContext('2d');
                        drawLineChart(ctx, displayLabels, trailingMetricsSeries);
                        
                        // Build legend
                        buildLegend('trailingLegend', trailingMetricsSeries, () => {
                            drawLineChart(ctx, displayLabels, trailingMetricsSeries);
                        });
                        
                        // Update info
                        document.getElementById('trailingChartInfo').innerHTML = 
                            `Showing ${data.days} days | Trailing ${data.trailing} complete units per day | Uses logic page settings`;
                    })
                    .catch(err => {
                        console.error('Failed to load trailing metrics:', err);
                        document.getElementById('trailingChartInfo').innerHTML = 
                            '<span style="color:#f85149;">Failed to load chart data</span>';
                    });
            }
            
            // Handle button clicks for department pie chart
            document.querySelectorAll('.pie-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const days = parseInt(btn.getAttribute('data-pie-days'), 10);
                    if(!days || days === deptPieState.days){
                        return;
                    }
                    deptPieState.days = days;
                    document.querySelectorAll('.pie-btn').forEach(b => b.classList.remove('active'));
                    btn.classList.add('active');
                    loadDeptPie();
                });
            });
            document.querySelectorAll('.daily-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const days = parseInt(btn.getAttribute('data-daily-days'), 10);
                    if(!days || days === dailyChartState.days){
                        return;
                    }
                    dailyChartState.days = days;
                    document.querySelectorAll('.daily-btn').forEach(b=>b.classList.remove('active'));
                    btn.classList.add('active');
                    renderDailyCharts();
                });
            });
            // Handle button clicks for trailing metrics chart
            document.querySelectorAll('.chart-btn[data-type]').forEach(btn => {
                btn.addEventListener('click', () => {
                    const type = btn.getAttribute('data-type');
                    const value = parseInt(btn.getAttribute('data-value'));
                    
                    // Update state
                    trailingChartState[type] = value;
                    
                    // Update button states
                    document.querySelectorAll(`.chart-btn[data-type="${type}"]`).forEach(b => {
                        b.classList.remove('active');
                    });
                    btn.classList.add('active');
                    
                    // Reload chart
                    loadTrailingMetrics();
                });
            });
            
            // Load initial chart
            loadTrailingMetrics();
            loadDeptPie();
            loadDailyMetrics();
            </script>
        </body></html>
        """
        return render_template_string(page, version=VERSION, version_date=VERSION_DATE)


@dashboard.route('/recent')
def recent_units():
        return render_template_string(RECENT_PAGE)


@dashboard.route('/changelog')
def changelog():
    """Display the changelog from CHANGELOG.md."""
    import os
    import markdown
    
    # Try to find CHANGELOG.md
    changelog_path = None
    possible_paths = [
        os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'CHANGELOG.md'),
        os.path.join(os.path.dirname(__file__), '..', '..', 'CHANGELOG.md'),
        'CHANGELOG.md'
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            changelog_path = path
            break
    
    if changelog_path and os.path.exists(changelog_path):
        with open(changelog_path, 'r', encoding='utf-8') as f:
            changelog_md = f.read()
        # Convert markdown to HTML
        try:
            import markdown
            changelog_html = markdown.markdown(changelog_md, extensions=['extra', 'codehilite'])
        except ImportError:
            # Fallback to basic HTML if markdown library not available
            changelog_html = '<pre>' + html.escape(changelog_md) + '</pre>'
    else:
        changelog_html = '<p>Changelog not found.</p>'
    
    page = f"""<!DOCTYPE html>
    <html>
    <head>
        <meta charset='utf-8'>
        <title>Changelog - SCH Labor Dashboard</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif; margin: 0; background: #0d1117; color: #c9d1d9; line-height: 1.6; }}
            header {{ padding: 1rem 1.5rem; background: #161b22; border-bottom: 1px solid #30363d; }}
            h1 {{ margin: 0; font-size: 1.2rem; }}
            .nav {{ padding: 0.5rem 1.5rem; background: #0d1117; }}
            .nav a {{ color: #8fb9ff; text-decoration: none; font-size: .85rem; }}
            .nav a:hover {{ text-decoration: underline; }}
            .content {{ max-width: 1000px; margin: 0 auto; padding: 2rem 1.5rem; }}
            .content h1 {{ font-size: 2rem; margin-bottom: 1.5rem; border-bottom: 2px solid #30363d; padding-bottom: 0.5rem; }}
            .content h2 {{ font-size: 1.5rem; margin-top: 2rem; margin-bottom: 1rem; color: #58a6ff; border-bottom: 1px solid #30363d; padding-bottom: 0.3rem; }}
            .content h3 {{ font-size: 1.2rem; margin-top: 1.5rem; margin-bottom: 0.75rem; color: #79c0ff; }}
            .content ul {{ margin-left: 1.5rem; }}
            .content li {{ margin: 0.5rem 0; }}
            .content code {{ background: #161b22; padding: 0.2rem 0.4rem; border-radius: 3px; font-family: ui-monospace, monospace; font-size: 0.9em; color: #79c0ff; }}
            .content pre {{ background: #161b22; padding: 1rem; border-radius: 6px; overflow-x: auto; border: 1px solid #30363d; }}
            .content pre code {{ background: none; padding: 0; }}
            .content strong {{ color: #c9d1d9; }}
            .content a {{ color: #58a6ff; }}
            .content a:hover {{ text-decoration: underline; }}
            .content hr {{ border: 0; border-top: 1px solid #30363d; margin: 2rem 0; }}
            .version-badge {{ display: inline-block; background: #238636; color: #fff; padding: 0.3rem 0.6rem; border-radius: 6px; font-size: 0.85rem; font-weight: 600; margin-left: 0.5rem; }}
            footer {{ margin-top: 3rem; padding: 1.5rem; text-align: center; border-top: 1px solid #30363d; background: #0d1117; }}
            footer .version {{ font-size: .72rem; opacity: .7; margin-bottom: .4rem; }}
        </style>
    </head>
    <body>
        <header>
            <h1>📋 Changelog</h1>
        </header>
        <div class='nav'>
            <a href='/dash'>← Back to Dashboard</a>
        </div>
        <div class='content'>
            {changelog_html}
        </div>
        <footer>
            <div class='version'>Version {VERSION} ({VERSION_DATE})</div>
        </footer>
    </body>
    </html>
    """
    return render_template_string(page)


@dashboard.route('/hours-chart')
def hours_chart():
    """Dedicated page for the Total Daily Hours Charged Chart."""
    page = """<!DOCTYPE html>
    <html>
    <head>
        <title>Total Daily Hours Charged Chart</title>
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 0; background: #0d1117; color: #c9d1d9; }
            .container { max-width: 1400px; margin: 0 auto; }
            header { padding: 1rem 1.5rem; background: #161b22; border-bottom: 1px solid #30363d; display:flex; flex-direction:column; gap:.75rem; }
            .header-row { display:flex; justify-content:space-between; align-items:center; gap:.75rem; width:100%; }
            h1 { margin: 0; font-size: 1.05rem; }
            .nav-links { padding: 0.5rem 1.5rem; display: flex; gap: 1rem; background: #0d1117; }
            .nav-links a { color: #8fb9ff; text-decoration: none; font-size: .85rem; }
            .nav-links a:hover { text-decoration: underline; }
            .content { padding: 20px; }
            .card { background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 20px; margin-bottom: 20px; }
            .controls { display: flex; gap: 15px; margin-bottom: 20px; flex-wrap: wrap; align-items: center; }
            .btn-group { display: flex; gap: 5px; }
            .btn { padding: 8px 16px; background: #21262d; border: 1px solid #30363d; border-radius: 6px; color: #c9d1d9; cursor: pointer; transition: all 0.2s; font-size: 0.9rem; }
            .btn:hover { background: #30363d; }
            .btn.active { background: #58a6ff; color: #0d1117; border-color: #58a6ff; }
            .btn-group-label { color: #8b949e; font-size: 0.85rem; display: flex; align-items: center; margin-right: 5px; }
            .legendWrap { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 15px; }
            .legend-item { display: flex; align-items: center; gap: 6px; padding: 4px 8px; background: #21262d; border-radius: 4px; cursor: pointer; transition: opacity 0.2s; }
            .legend-item:hover { background: #30363d; }
            .legend-item.off { opacity: 0.3; }
            .legend-dot { width: 12px; height: 12px; border-radius: 50%; }
            .alpha-banner { width:100%; background:linear-gradient(135deg,#d97706,#92400e); border:1px solid rgba(255,255,255,0.22); border-radius:10px; padding:.7rem .95rem; font-size:.72rem; line-height:1.4; font-weight:600; color:#fdf2f2; box-shadow:0 6px 18px -14px #000; letter-spacing:.02rem; }
            .alpha-banner strong { display:block; font-size:.78rem; margin-bottom:.25rem; color:#ffe2cc; text-transform:uppercase; letter-spacing:.05rem; }
        </style>
    </head>
    <body>
        <header><div class='header-row'><h1>Total Daily Hours Charged Chart</h1></div><p class='alpha-banner'><strong>Alpha testing preview</strong>These dashboards are experimental. Data may be incomplete or inaccurate—do not rely on them for operational or business decisions.</p></header>
        <div class='nav-links'><a href='/dash'>&larr; Dashboard</a></div>
        <div class="container">
            <div class="content">
            
            <div class="card">
                <h2 style="margin: 0 0 15px; font-size: 1rem;" id="chartTitle">Daily Hours (Trailing 7-day average)</h2>
                
                <div class="controls">
                    <div style="display: flex; align-items: center; gap: 10px;">
                        <span class="btn-group-label">Period:</span>
                        <div class="btn-group">
                            <button class="btn" data-period="30">30 Days</button>
                            <button class="btn active" data-period="90">90 Days</button>
                            <button class="btn" data-period="365">1 Year</button>
                        </div>
                    </div>
                    
                    <div style="display: flex; align-items: center; gap: 10px;">
                        <span class="btn-group-label">View:</span>
                        <div class="btn-group">
                            <button class="btn" data-view="ma30">Trailing 30</button>
                            <button class="btn active" data-view="ma7">Trailing 7</button>
                            <button class="btn" data-view="ma3">Trailing 3</button>
                            <button class="btn" data-view="actual">Actual</button>
                        </div>
                    </div>
                </div>
                
                <div id="deptLegend" class="legendWrap"></div>
                <canvas id="deptChart" width="1600" height="720" style="width:100%;height:520px"></canvas>
            </div>
        </div>
        
        <script>
        function lineColor(idx){
            const colors=['#6ea8fe','#a8ff60','#ffd166','#ff6b6b','#64d2ff','#c0a7ff','#9be9a8','#f8a5ff','#ffcd3c','#52d96d','#ffa06a'];
            return colors[idx % colors.length];
        }
        
        function drawLineChart(ctx, labels, series, opts={}){
            const W=ctx.canvas.width, H=ctx.canvas.height;
            const padL=60, padR=30, padT=30, padB=50;
            const cw=W-padL-padR, ch=H-padT-padB;
            ctx.clearRect(0,0,W,H);
            const vis=series.filter(s=>s.visible!==false);
            if(vis.length===0) return;
            let allData=[];
            vis.forEach(s=>{ if(s.data && s.data.length) allData=allData.concat(s.data.filter(v=>v!=null)); });
            if(allData.length===0) return;
            const minY=Math.min(...allData), maxY=Math.max(...allData);
            const rangeY = maxY - minY || 1;
            function scaleY(v){ return padT + ch*(1 - (v-minY)/rangeY); }
            const n=labels.length;
            if(n<2) return;
            ctx.strokeStyle='#30363d';
            const ticks=5;
            for(let i=0;i<=ticks;i++){
                const yy=padT+(i/ticks)*ch;
                ctx.beginPath();ctx.moveTo(padL,yy);ctx.lineTo(W-padR,yy);ctx.stroke();
            }
            vis.forEach((s,i)=>{
                const arr=s.data||[];
                if(arr.length===0) return;
                ctx.strokeStyle=s.color||lineColor(i);
                ctx.lineWidth=2.5;
                ctx.beginPath();
                let first=true;
                for(let j=0;j<n;j++){
                    const v=arr[j];
                    if(v==null) continue;
                    const xx=padL+(cw/(n-1))*j;
                    const yy=scaleY(v);
                    if(first){ ctx.moveTo(xx,yy); first=false; }
                    else ctx.lineTo(xx,yy);
                }
                ctx.stroke();
            });
            ctx.fillStyle='#8b949e';
            ctx.font='14px sans-serif';
            ctx.textAlign='right';
            ctx.textBaseline='middle';
            for(let i=0;i<=ticks;i++){
                const v=maxY-(i/ticks)*rangeY;
                const yy=padT+(i/ticks)*ch;
                ctx.fillText(Math.round(v), padL-8, yy);
            }
            ctx.textAlign='center';
            ctx.textBaseline='top';
            const step=Math.ceil(n/12);
            for(let i=0;i<n;i+=step){
                const xx=padL+(cw/(n-1))*i;
                const lab=labels[i]||'';
                ctx.strokeStyle='#30363d';
                ctx.beginPath();
                ctx.moveTo(xx,H-padB);
                ctx.lineTo(xx,H-padB+4);
                ctx.stroke();
                ctx.fillText(lab,xx,H-padB+6);
            }
        }
        
        function buildLegend(containerId, series, onToggle){
            const el=document.getElementById(containerId);
            let html='';
            series.forEach((s,i)=>{
                const color=s.color||lineColor(i);
                const off=s.visible===false?' off':'';
                html+=`<div class="legend-item${off}" data-idx="${i}"><span class="legend-dot" style="background:${color}"></span><span>${s.name}</span></div>`;
            });
            el.innerHTML=html;
            el.querySelectorAll('.legend-item').forEach(item=>{
                item.addEventListener('click',()=>{
                    const i=Number(item.getAttribute('data-idx'));
                    series[i].visible=series[i].visible===false?true:false;
                    item.classList.toggle('off',series[i].visible===false);
                    onToggle();
                });
            });
        }
        
        let chartData = null;
        let currentPeriod = 90;
        let currentView = 'ma7';
        let seriesState = [];
        let weekdayMask = []; // Track which indices are weekdays (Mon-Fri)
        
        function movingAverage(data, window, excludeWeekends = true) {
            if (window === 1) return data;
            const result = [];
            
            if (excludeWeekends && weekdayMask.length > 0) {
                // Use last N *weekday* values (exclude Sat/Sun)
                for (let i = 0; i < data.length; i++) {
                    let sum = 0, count = 0;
                    // Look backwards to find N weekdays
                    for (let j = i; j >= 0 && count < window; j--) {
                        if (weekdayMask[j] && data[j] != null) {
                            sum += data[j];
                            count++;
                        }
                    }
                    const avg = count > 0 ? sum / count : null;
                    result.push(avg);
                    // Debug for MA3
                    if (window === 3 && i >= data.length - 5 && i < data.length) {
                        console.log(`MA3 at i=${i}: weekday=${weekdayMask[i]}, value=${data[i]?.toFixed(1)}, avg=${avg?.toFixed(1)}`);
                    }
                }
            } else {
                // Original calendar-based moving average (all days)
                for (let i = 0; i < data.length; i++) {
                    let sum = 0, count = 0;
                    for (let j = Math.max(0, i - window + 1); j <= i; j++) {
                        if (data[j] != null) {
                            sum += data[j];
                            count++;
                        }
                    }
                    const avg = count > 0 ? sum / count : null;
                    result.push(avg);
                }
            }
            return result;
        }
        
        function updateChart() {
            if (!chartData) return;
            let allDates = chartData.dates.slice(0, -1);
            let allDeptData = {};
            Object.keys(chartData.per_dept).forEach(dept => {
                allDeptData[dept] = chartData.per_dept[dept].slice(0, -1);
            });
            // Determine the actual window size based on current view
            let actualWindow = 1;
            if (currentView === 'ma30') actualWindow = 30;
            else if (currentView === 'ma7') actualWindow = 7;
            else if (currentView === 'ma3') actualWindow = 3;
            
            const extraDays = actualWindow - 1;
            const displayStartIdx = Math.max(0, allDates.length - currentPeriod);
            const calcStartIdx = Math.max(0, displayStartIdx - extraDays);
            let calcDates = allDates.slice(calcStartIdx);
            let calcDeptData = {};
            Object.keys(allDeptData).forEach(dept => {
                calcDeptData[dept] = allDeptData[dept].slice(calcStartIdx);
            });
            const keys = Object.keys(calcDeptData).sort();
            let series;
            const skipDays = Math.max(0, calcDates.length - currentPeriod);
            
            if (currentView === 'ma30') {
                series = keys.map((k, i) => {
                    const ma = movingAverage(calcDeptData[k], 30);
                    const displayData = ma.slice(skipDays);
                    return {
                        name: k,
                        data: displayData,
                        color: lineColor(i),
                        visible: seriesState[i]?.visible !== false
                    };
                });
                document.getElementById('chartTitle').textContent = 'Daily Hours (Trailing 30-day average)';
            } else if (currentView === 'ma7') {
                series = keys.map((k, i) => {
                    const ma = movingAverage(calcDeptData[k], 7);
                    const displayData = ma.slice(skipDays);
                    return {
                        name: k,
                        data: displayData,
                        color: lineColor(i),
                        visible: seriesState[i]?.visible !== false
                    };
                });
                document.getElementById('chartTitle').textContent = 'Daily Hours (Trailing 7-day average)';
            } else if (currentView === 'ma3') {
                series = keys.map((k, i) => {
                    const ma = movingAverage(calcDeptData[k], 3);
                    const displayData = ma.slice(skipDays);
                    return {
                        name: k,
                        data: displayData,
                        color: lineColor(i),
                        visible: seriesState[i]?.visible !== false
                    };
                });
                document.getElementById('chartTitle').textContent = 'Daily Hours (Trailing 3-day average)';
            } else {
                series = keys.map((k, i) => {
                    const displayData = calcDeptData[k].slice(skipDays);
                    return {
                        name: k,
                        data: displayData,
                        color: lineColor(i),
                        visible: seriesState[i]?.visible !== false
                    };
                });
                document.getElementById('chartTitle').textContent = 'Daily Hours (Actual)';
            }
            const displayDates = calcDates.slice(skipDays);
            seriesState = series.map(s => ({ visible: s.visible }));
            const dctx = document.getElementById('deptChart').getContext('2d');
            const redraw = () => drawLineChart(dctx, displayDates, series, {});
            buildLegend('deptLegend', series, redraw);
            redraw();
        }
        
        document.querySelectorAll('[data-period]').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('[data-period]').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentPeriod = parseInt(btn.getAttribute('data-period'));
                updateChart();
            });
        });
        
        document.querySelectorAll('[data-view]').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('[data-view]').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentView = btn.getAttribute('data-view');
                updateChart();
            });
        });
        
        fetch('/api/metrics/daily_hours?days=425').then(r => r.json()).then(m => {
            chartData = {
                dates: m.dates,
                per_dept: {}
            };
            Object.keys(m.per_dept).forEach(dept => {
                chartData.per_dept[dept] = m.per_dept[dept].hours;
            });
            // Build weekday mask (true for Mon-Fri, false for Sat-Sun)
            weekdayMask = chartData.dates.map(dateStr => {
                const d = new Date(dateStr);
                const day = d.getDay(); // 0=Sun, 1=Mon, ..., 6=Sat
                return day >= 1 && day <= 5; // Mon-Fri
            });
            const weekdayCount = weekdayMask.filter(x => x).length;
            const weekendCount = weekdayMask.length - weekdayCount;
            console.log(`Total days: ${weekdayMask.length}, Weekdays: ${weekdayCount}, Weekends: ${weekendCount}`);
            updateChart();
        }).catch(err => {
            console.error('Chart error:', err);
            document.getElementById('deptChart').parentElement.innerHTML = '<p style="color:#f85149;">Error loading chart data</p>';
        });
        </script>
        </div>
        </div>
    </body>
    </html>
    """
    return render_template_string(page)


@dashboard.route('/display')
def display_board():
    page = """<!doctype html>
    <html>
    <head>
        <meta charset='utf-8'>
        <title>Display Dashboard</title>
        <style>
            :root{color-scheme:dark;}
            *{box-sizing:border-box;}
            body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:#0d1117;color:#ccd6f6;display:flex;flex-direction:column;height:100vh;}
            a{color:#8fb9ff;text-decoration:none;}
            a:hover{text-decoration:underline;}
            .hidden{display:none!important;}
            .toolbar{display:flex;flex-direction:column;align-items:stretch;padding:0.9rem 1.4rem;background:#161b22;border-bottom:1px solid #30363d;gap:0.9rem;z-index:100;flex-shrink:0;}
            .toolbar-row{display:flex;justify-content:space-between;align-items:center;gap:1rem;flex-wrap:wrap;}
            .toolbar-left{display:flex;gap:0.75rem;align-items:center;}
            .toolbar-right{display:flex;gap:0.75rem;align-items:center;}
            .toolbar-btn{background:#21262d;color:#c9d1d9;border:1px solid #30363d;border-radius:8px;padding:0.55rem 1.1rem;font-size:0.85rem;font-weight:600;cursor:pointer;transition:all .2s ease;}
            .toolbar-btn.primary{background:#238636;border-color:#2ea043;color:#fff;}
            .toolbar-btn:hover{filter:brightness(1.08);}
            .toolbar-btn:disabled{opacity:0.45;cursor:not-allowed;}
            .toolbar-link{font-size:0.85rem;opacity:0.85;}
            .alpha-banner{width:100%;background:linear-gradient(135deg,#d97706,#92400e);border:1px solid rgba(255,255,255,0.18);border-radius:10px;padding:0.65rem 0.9rem;font-size:0.68rem;line-height:1.35;font-weight:600;color:#fbe5e8;box-shadow:0 6px 18px -14px #000;letter-spacing:0.02rem;}
            .alpha-banner strong{display:block;font-size:0.7rem;margin-bottom:0.25rem;color:#ffe2cc;letter-spacing:0.05rem;text-transform:uppercase;}
            .board-wrapper{flex:1;width:100%;background:#0a0f16;position:relative;overflow:hidden;}
            .chart-board{position:relative;width:100%;height:100%;overflow:auto;padding:1.5rem;padding-top:0.5rem;}
            .display-card{position:absolute;min-width:320px;min-height:220px;width:520px;height:340px;background:#111821;border:1px solid #2a3240;border-radius:14px;box-shadow:0 8px 26px -18px rgba(0,0,0,0.8),0 0 0 1px rgba(48,54,61,0.8);resize:both;overflow:hidden;display:flex;flex-direction:column;}
            .display-card.locked{resize:none;}
            .card-header{cursor:grab;padding:0.65rem 0.95rem;border-bottom:1px solid #212b35;display:flex;align-items:center;justify-content:space-between;gap:0.75rem;background:linear-gradient(130deg,#151d27,#111620);font-weight:600;font-size:0.9rem;}
            .card-header:active{cursor:grabbing;}
            .card-title{flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
            .chart-close{background:transparent;border:0;color:#b6c3e5;font-size:1.1rem;cursor:pointer;line-height:1;width:28px;height:28px;border-radius:6px;display:flex;align-items:center;justify-content:center;}
            .chart-close:hover{background:rgba(255,255,255,0.08);color:#fff;}
            .card-body{flex:1;position:relative;background:#0d131a;padding:0.75rem;display:flex;align-items:center;justify-content:center;overflow:hidden;}
            .card-body canvas{width:100%;height:100%;display:block;}
            .card-meta{padding:0.6rem 0.95rem;border-top:1px solid #212b35;font-size:0.75rem;opacity:0.75;display:flex;gap:0.8rem;flex-wrap:wrap;}
            .loading{font-size:0.85rem;opacity:0.7;}
            .error{color:#ff7b72;font-size:0.85rem;text-align:center;padding:0.5rem;}
            .picker-backdrop{position:fixed;inset:0;background:rgba(2,10,20,0.78);backdrop-filter:blur(4px);opacity:0;pointer-events:none;transition:opacity 0.2s ease;}
            .picker-backdrop.open{opacity:1;pointer-events:auto;}
            .chart-picker{position:fixed;top:50%;left:50%;transform:translate(-50%,-45%) scale(0.96);width:min(720px,92vw);max-height:80vh;overflow:auto;background:#0f1724;border:1px solid #243044;border-radius:18px;box-shadow:0 30px 70px -40px rgba(0,0,0,0.9);padding:1.5rem;opacity:0;pointer-events:none;transition:opacity 0.2s ease,transform 0.2s ease;display:flex;flex-direction:column;gap:1.2rem;}
            .chart-picker.open{opacity:1;pointer-events:auto;transform:translate(-50%,-50%) scale(1);}
            .picker-header{display:flex;justify-content:space-between;align-items:center;gap:1rem;}
            .picker-header h2{margin:0;font-size:1.1rem;}
            .picker-close{background:transparent;border:0;color:#8fb9ff;font-size:1.5rem;cursor:pointer;width:34px;height:34px;border-radius:8px;display:flex;align-items:center;justify-content:center;}
            .picker-close:hover{background:rgba(143,185,255,0.15);}
            .picker-section{display:flex;flex-direction:column;gap:0.7rem;}
            .picker-section h3{margin:0;font-size:0.95rem;color:#9fb4da;text-transform:uppercase;letter-spacing:0.08rem;font-weight:700;}
            .picker-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:0.6rem;}
            .picker-option{background:#1a2331;border:1px solid #273448;border-radius:10px;padding:0.75rem 0.85rem;font-size:0.82rem;text-align:left;cursor:pointer;color:#d2dcf8;transition:all 0.2s ease;min-height:64px;display:flex;flex-direction:column;gap:0.35rem;}
            .picker-option strong{font-size:0.85rem;color:#f1f5ff;}
            .picker-option span{opacity:0.75;font-size:0.75rem;}
            .picker-option:hover{border-color:#3a8bff;background:#1f2d44;}
            .chart-gauge-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:1rem;width:100%;height:100%;align-content:center;}
            .gauge{position:relative;width:100%;aspect-ratio:1;border-radius:50%;background:#151e2b;box-shadow:0 15px 35px -20px rgba(0,0,0,0.9),0 0 0 1px rgba(43,56,71,0.7);display:flex;align-items:center;justify-content:center;}
            .gauge-fill{position:absolute;inset:0;border-radius:50%;background:conic-gradient(#58a6ff 0deg,#111821 0deg);transition:background 0.45s ease;}
            .gauge::after{content:"";position:absolute;inset:16%;border-radius:50%;background:#0d1117;box-shadow:inset 0 0 20px -12px rgba(0,0,0,0.8);}
            .gauge-center{position:relative;z-index:1;display:flex;flex-direction:column;align-items:center;gap:0.35rem;color:#d5e4ff;}
            .gauge-value{font-size:1.4rem;font-weight:700;}
            .gauge-label{font-size:0.8rem;opacity:0.75;}
            .gauge-meta{display:flex;flex-direction:column;gap:0.35rem;font-size:0.72rem;opacity:0.75;text-align:center;}
            .fullscreen-active body,body.fullscreen-active{overflow:hidden;}
            .fullscreen-active .toolbar-btn.primary{display:none;}
            .fullscreen-active .chart-close{display:none;}
            .fullscreen-active .display-card{border:none;box-shadow:none;cursor:default;background:transparent;}
            .fullscreen-active .card-header{background:transparent;border-bottom:none;display:none;}
            .fullscreen-active .card-body{background:transparent;}
            .fullscreen-active .display-card.locked .card-header{cursor:default;}
            .fullscreen-active .chart-picker{display:none!important;}
            .fullscreen-active #pickerBackdrop{display:none!important;}
            .fullscreen-active .display-card.locked{pointer-events:none;}
            .chart-badge{display:inline-flex;align-items:center;gap:0.4rem;font-size:0.72rem;background:#131c27;border:1px solid #283347;border-radius:999px;padding:0.25rem 0.65rem;}
            .chart-badge span{opacity:0.75;}
            .card-body .chart-legend{position:absolute;top:0.6rem;left:0.75rem;display:flex;gap:0.55rem;flex-wrap:wrap;background:rgba(13,17,23,0.78);padding:0.32rem 0.5rem;border-radius:8px;font-size:0.72rem;line-height:1.3;box-shadow:0 18px 28px -24px rgba(0,0,0,0.9);}
            .card-body .chart-legend-item{display:inline-flex;align-items:center;gap:0.4rem;color:#d8e4ff;opacity:0.85;white-space:nowrap;}
            .card-body .chart-legend-swatch{width:10px;height:10px;border-radius:50%;box-shadow:0 0 0 1px rgba(0,0,0,0.45);}
            .card-body .chart-meta-stack{position:absolute;bottom:0.55rem;left:0.75rem;background:rgba(13,17,23,0.75);padding:0.35rem 0.55rem;border-radius:8px;font-size:0.7rem;display:flex;gap:0.6rem;flex-wrap:wrap;}
            .card-body .chart-meta-stack span{opacity:0.7;}
            @media (max-width:900px){
                .toolbar{flex-wrap:wrap;}
                .toolbar-left{flex-wrap:wrap;}
                .display-card{width:420px;height:300px;}
            }
        </style>
    </head>
    <body>
        <header class='toolbar'>
            <div class='toolbar-row'>
                <div class='toolbar-left'>
                    <button id='addChartBtn' class='toolbar-btn primary'>Add Chart</button>
                    <button id='fullScreenBtn' class='toolbar-btn'>Full Screen</button>
                </div>
                <div class='toolbar-right'>
                    <a class='toolbar-link' href='/dash'>&larr; Dashboard</a>
                </div>
            </div>
        </header>
        <div class='board-wrapper'>
            <div id='chartBoard' class='chart-board'></div>
        </div>
        <div id='pickerBackdrop' class='picker-backdrop hidden'></div>
        <aside id='chartPicker' class='chart-picker hidden'>
            <div class='picker-header'>
                <h2>Select a chart</h2>
                <button id='pickerClose' class='picker-close' title='Close'>&times;</button>
            </div>
            <div class='picker-section'>
                <h3>Trailing Metrics (Line)</h3>
                <div class='picker-grid' data-chart-group='trailing'></div>
            </div>
            <div class='picker-section'>
                <h3>Department Pie Charts</h3>
                <div class='picker-grid' data-chart-group='pie'></div>
            </div>
            <div class='picker-section'>
                <h3>Average Metrics Gauges</h3>
                <div class='picker-grid' data-chart-group='avg'></div>
            </div>
            <div class='picker-section'>
                <h3>Daily Total Hours (Line)</h3>
                <div class='picker-grid' data-chart-group='dailyTotals'></div>
            </div>
        </aside>
        <script>
        (function(){
            const board = document.getElementById('chartBoard');
            const addBtn = document.getElementById('addChartBtn');
            const picker = document.getElementById('chartPicker');
            const pickerBackdrop = document.getElementById('pickerBackdrop');
            const pickerClose = document.getElementById('pickerClose');
            const fullScreenBtn = document.getElementById('fullScreenBtn');
            let isFullScreen = false;
            let cardCounter = 0;

            const chartDefinitions = [
                {id:'trend-90-10',group:'trailing',label:'Trailing Metrics • 90 days • trailing 10',type:'trailing',params:{days:90,trailing:10}},
                {id:'trend-90-30',group:'trailing',label:'Trailing Metrics • 90 days • trailing 30',type:'trailing',params:{days:90,trailing:30}},
                {id:'trend-120-10',group:'trailing',label:'Trailing Metrics • 120 days • trailing 10',type:'trailing',params:{days:120,trailing:10}},
                {id:'trend-120-30',group:'trailing',label:'Trailing Metrics • 120 days • trailing 30',type:'trailing',params:{days:120,trailing:30}},
                {id:'trend-365-10',group:'trailing',label:'Trailing Metrics • 1 year • trailing 10',type:'trailing',params:{days:365,trailing:10}},
                {id:'trend-365-30',group:'trailing',label:'Trailing Metrics • 1 year • trailing 30',type:'trailing',params:{days:365,trailing:30}},
                {id:'pie-30',group:'pie',label:'Department Hours • 30 days',type:'pie',params:{days:30}},
                {id:'pie-60',group:'pie',label:'Department Hours • 60 days',type:'pie',params:{days:60}},
                {id:'pie-90',group:'pie',label:'Department Hours • 90 days',type:'pie',params:{days:90}},
                {id:'avg-30',group:'avg',label:'Average Metrics Gauges • 30 days',type:'avg',params:{window:30}},
                {id:'avg-60',group:'avg',label:'Average Metrics Gauges • 60 days',type:'avg',params:{window:60}},
                {id:'avg-90',group:'avg',label:'Average Metrics Gauges • 90 days',type:'avg',params:{window:90}},
                {id:'daily-30-actual',group:'dailyTotals',label:'Daily Total Hours • 30 days Actual',type:'dailyTotals',params:{days:30,view:'actual'}},
                {id:'daily-30-ma7',group:'dailyTotals',label:'Daily Total Hours • 30 days • 7-day avg',type:'dailyTotals',params:{days:30,view:'ma7'}},
                {id:'daily-60-actual',group:'dailyTotals',label:'Daily Total Hours • 60 days Actual',type:'dailyTotals',params:{days:60,view:'actual'}},
                {id:'daily-60-ma7',group:'dailyTotals',label:'Daily Total Hours • 60 days • 7-day avg',type:'dailyTotals',params:{days:60,view:'ma7'}},
                {id:'daily-90-actual',group:'dailyTotals',label:'Daily Total Hours • 90 days Actual',type:'dailyTotals',params:{days:90,view:'actual'}},
                {id:'daily-90-ma7',group:'dailyTotals',label:'Daily Total Hours • 90 days • 7-day avg',type:'dailyTotals',params:{days:90,view:'ma7'}},
            ];

            const dataCache = {
                trailing:{},
                pie:{},
                avg:null,
                dailyTotals:{}
            };

            const cardHandlers = new Map();

            function populatePicker(){
                const groups = picker.querySelectorAll('[data-chart-group]');
                groups.forEach(groupEl=>{
                    const groupKey = groupEl.getAttribute('data-chart-group');
                    groupEl.innerHTML='';
                    chartDefinitions.filter(def=>def.group===groupKey).forEach(def=>{
                        const btn=document.createElement('button');
                        btn.type='button';
                        btn.className='picker-option';
                        btn.dataset.chartId=def.id;
                        const parts=def.label.split('•');
                        if(parts.length>1){
                            btn.innerHTML=`<strong>${parts[0].trim()}</strong><span>${parts.slice(1).join('•').trim()}</span>`;
                        }else{
                            btn.innerHTML=`<strong>${def.label}</strong>`;
                        }
                        btn.addEventListener('click',()=>{
                            addChart(def);
                        });
                        groupEl.appendChild(btn);
                    });
                });
            }

            function openPicker(){
                if(document.body.classList.contains('fullscreen-active')) return;
                picker.classList.remove('hidden');
                pickerBackdrop.classList.remove('hidden');
                setTimeout(()=>{
                    picker.classList.add('open');
                    pickerBackdrop.classList.add('open');
                },10);
            }

            function closePicker(){
                picker.classList.remove('open');
                pickerBackdrop.classList.remove('open');
                setTimeout(()=>{
                    picker.classList.add('hidden');
                    pickerBackdrop.classList.add('hidden');
                },180);
            }

            addBtn.addEventListener('click',openPicker);
            pickerClose.addEventListener('click',closePicker);
            pickerBackdrop.addEventListener('click',closePicker);

            function removeCard(card){
                const handler=cardHandlers.get(card);
                if(handler){
                    if(handler.observer){handler.observer.disconnect();}
                    if(typeof handler.cleanup==='function'){handler.cleanup();}
                    cardHandlers.delete(card);
                }
                card.remove();
            }

            function enableDrag(card){
                const header=card.querySelector('.card-header');
                header.addEventListener('pointerdown',event=>{
                    if(event.target.closest('.chart-close')) return;
                    if(document.body.classList.contains('fullscreen-active')) return;
                    const boardRect=board.getBoundingClientRect();
                    const cardRect=card.getBoundingClientRect();
                    const offsetX=event.clientX-cardRect.left;
                    const offsetY=event.clientY-cardRect.top;
                    function onMove(e){
                        const x=e.clientX-boardRect.left-offsetX;
                        const y=e.clientY-boardRect.top-offsetY;
                        card.style.left=Math.max(0,x)+'px';
                        card.style.top=Math.max(0,y)+'px';
                    }
                    function onUp(){
                        document.removeEventListener('pointermove',onMove);
                    }
                    document.addEventListener('pointermove',onMove);
                    document.addEventListener('pointerup',onUp,{once:true});
                    event.preventDefault();
                });
            }

            function prepareCanvas(container){
                const canvas=document.createElement('canvas');
                canvas.className='chart-canvas';
                container.innerHTML='';
                container.appendChild(canvas);
                const ctx=canvas.getContext('2d');
                return {canvas,ctx};
            }

            function resizeCanvas(canvas,ctx){
                const parent=canvas.parentElement;
                const width=Math.max(220,parent.clientWidth||220);
                const height=Math.max(180,parent.clientHeight||180);
                const ratio=window.devicePixelRatio||1;
                canvas.width=width*ratio;
                canvas.height=height*ratio;
                canvas.style.width=width+'px';
                canvas.style.height=height+'px';
                ctx.setTransform(ratio,0,0,ratio,0,0);
                ctx.clearRect(0,0,width,height);
                return {width,height};
            }

            function drawLineChart(ctx,width,height,labels,series,opts={}){
                const padL=60,padR=35,padT=30,padB=54;
                const plotW=width-padL-padR;
                const plotH=height-padT-padB;
                ctx.clearRect(0,0,width,height);
                const visibleSeries=series.filter(s=>s && s.data && s.data.length);
                if(!visibleSeries.length) return;
                let minY=Number.POSITIVE_INFINITY,maxY=Number.NEGATIVE_INFINITY;
                visibleSeries.forEach(s=>{
                    s.data.forEach(v=>{
                        if(v==null) return;
                        if(v<minY) minY=v;
                        if(v>maxY) maxY=v;
                    });
                });
                if(!isFinite(minY) || !isFinite(maxY)) return;
                if(Math.abs(maxY-minY)<1e-6){maxY=minY+1;}
                const count=labels.length;
                const stepX=count>1?plotW/(count-1):plotW;
                ctx.strokeStyle='#1f2933';
                ctx.lineWidth=1;
                const gridLines=5;
                for(let i=0;i<=gridLines;i++){
                    const y=padT+plotH*(i/gridLines);
                    ctx.beginPath();
                    ctx.moveTo(padL,y);
                    ctx.lineTo(width-padR,y);
                    ctx.stroke();
                }
                function scaleY(v){
                    return padT + plotH * (1 - (v - minY) / (maxY - minY));
                }
                visibleSeries.forEach((s,idx)=>{
                    ctx.strokeStyle=s.color||'#58a6ff';
                    ctx.lineWidth=2.5;
                    ctx.beginPath();
                    let started=false;
                    s.data.forEach((v,i)=>{
                        if(v==null) return;
                        const x=padL+stepX*i;
                        const y=scaleY(v);
                        if(!started){ctx.moveTo(x,y);started=true;}else{ctx.lineTo(x,y);} 
                    });
                    ctx.stroke();
                });
                ctx.fillStyle='#9fb4da';
                ctx.font='12px sans-serif';
                ctx.textAlign='right';
                ctx.textBaseline='middle';
                for(let i=0;i<=gridLines;i++){
                    const value=maxY - (maxY-minY)*(i/gridLines);
                    const y=padT+plotH*(i/gridLines);
                    ctx.fillText(value.toFixed(opts.yPrecision||1), padL-8, y);
                }
                ctx.textAlign='center';
                ctx.textBaseline='top';
                const labelStep=Math.max(1,Math.floor(count/12));
                for(let i=0;i<count;i+=labelStep){
                    const x=padL+stepX*i;
                    const label=opts.formatLabel?opts.formatLabel(labels[i],i):labels[i];
                    ctx.fillText(label,x,height-padB+12);
                    ctx.beginPath();
                    ctx.moveTo(x,height-padB);
                    ctx.lineTo(x,height-padB+6);
                    ctx.strokeStyle='#233144';
                    ctx.stroke();
                }
            }

            function drawPie(ctx,width,height,segments,total){
                ctx.clearRect(0,0,width,height);
                const radius=Math.min(width,height)/2 - 12;
                const cx=width/2,cy=height/2;
                let start=-Math.PI/2;
                segments.forEach(seg=>{
                    if(seg.hours<=0) return;
                    const slice=(seg.hours/total)*Math.PI*2;
                    ctx.beginPath();
                    ctx.moveTo(cx,cy);
                    ctx.fillStyle=seg.color;
                    ctx.arc(cx,cy,radius,start,start+slice);
                    ctx.closePath();
                    ctx.fill();
                    start+=slice;
                });
                const innerRadius=radius*0.55;
                ctx.beginPath();
                ctx.fillStyle='#0d1117';
                ctx.arc(cx,cy,innerRadius,0,Math.PI*2);
                ctx.fill();
                ctx.fillStyle='#d0dcff';
                ctx.font='600 15px system-ui';
                ctx.textAlign='center';
                ctx.fillText('Total Hours',cx,cy-12);
                ctx.font='700 20px system-ui';
                ctx.fillText((total||0).toFixed(1),cx,cy+14);
            }

            function mean(values){
                if(!values || !values.length) return 0;
                let sum=0,count=0;
                values.forEach(v=>{
                    const num=Number(v);
                    if(!isNaN(num)){sum+=num;count++;}
                });
                return count?sum/count:0;
            }

            function formatDateLabel(label){
                if(!label) return '';
                const parts=label.split('-');
                if(parts.length!==3) return label;
                return parts[1].replace(/^0/,'')+'/'+parts[2].replace(/^0/,'');
            }

            function setCardHandler(card, handler){
                const prev=cardHandlers.get(card);
                if(prev){
                    if(prev.observer){prev.observer.disconnect();}
                    if(typeof prev.cleanup==='function'){prev.cleanup();}
                }
                if(handler && handler.contentEl){
                    const observer=new ResizeObserver(()=>{
                        if(typeof handler.redraw==='function') handler.redraw();
                    });
                    observer.observe(handler.contentEl);
                    handler.observer=observer;
                }
                cardHandlers.set(card,handler);
            }

            function addChart(def){
                closePicker();
                const card=document.createElement('div');
                card.className='display-card';
                const cardId='card-'+(++cardCounter);
                card.dataset.cardId=cardId;
                card.style.left=24 + (cardCounter%5)*28 + 'px';
                card.style.top=24 + (cardCounter%3)*34 + 'px';
                if(def.type==='pie'){card.style.width='420px';card.style.height='360px';}
                if(def.type==='avg'){card.style.width='500px';card.style.height='360px';}
                card.innerHTML=`<div class="card-header"><span class="card-title">${def.label}</span><button class="chart-close" title="Remove">&times;</button></div><div class="card-body"><div class="loading">Loading…</div></div>`;
                board.appendChild(card);
                const closeBtn=card.querySelector('.chart-close');
                closeBtn.addEventListener('click',()=>removeCard(card));
                enableDrag(card);
                renderChart(card,def);
            }

            async function fetchTrailing(days,trailing){
                const key=`${days}_${trailing}`;
                if(dataCache.trailing[key]) return dataCache.trailing[key];
                const res=await fetch(`/api/metrics/trailing_trend?days=${days}&trailing=${trailing}`);
                const json=await res.json();
                dataCache.trailing[key]=json;
                return json;
            }

            async function fetchPie(days){
                const key=String(days);
                if(dataCache.pie[key]) return dataCache.pie[key];
                const res=await fetch(`/api/metrics/department_totals?days=${days}`);
                const json=await res.json();
                dataCache.pie[key]=json;
                return json;
            }

            async function fetchAvg(){
                if(dataCache.avg) return dataCache.avg;
                const res=await fetch('/api/metrics/daily_metric_trends');
                const json=await res.json();
                dataCache.avg=json;
                return json;
            }

            async function fetchDailyTotals(days){
                const key=String(Math.min(365,Math.max(days,7)));
                if(dataCache.dailyTotals[key]) return dataCache.dailyTotals[key];
                const requestDays=Math.min(365,Math.max(days,7));
                const res=await fetch(`/api/metrics/daily_hours?days=${requestDays}`);
                const json=await res.json();
                dataCache.dailyTotals[key]=json;
                return json;
            }

            function renderChart(card, def){
                const body=card.querySelector('.card-body');
                body.innerHTML='<div class="loading">Loading…</div>';
                if(def.type==='trailing'){
                    renderTrailing(card,body,def).catch(err=>showError(body,err));
                }else if(def.type==='pie'){
                    renderPie(card,body,def).catch(err=>showError(body,err));
                }else if(def.type==='avg'){
                    renderAvg(card,body,def).catch(err=>showError(body,err));
                }else if(def.type==='dailyTotals'){
                    renderDailyTotals(card,body,def).catch(err=>showError(body,err));
                }
            }

            function showError(body,err){
                console.error('Chart render error',err);
                body.innerHTML=`<div class="error">Unable to load chart data</div>`;
            }

            async function renderTrailing(card,body,def){
                const data=await fetchTrailing(def.params.days,def.params.trailing);
                if(!data || !data.labels){throw new Error('No trailing data');}
                const labels=data.labels;
                const series=[
                    {name:'Avg Efficiency',color:'#58a6ff',data:(data.avg_efficiency||[]).map(v=>Number(v)||0)},
                    {name:'Avg Active Days',color:'#fbbc64',data:(data.avg_act_days||[]).map(v=>Number(v)||0)},
                    {name:'Avg Span',color:'#7ee787',data:(data.avg_span||[]).map(v=>Number(v)||0)},
                ];
                const {canvas,ctx}=prepareCanvas(body);
                const legend=document.createElement('div');
                legend.className='chart-legend';
                series.forEach(s=>{
                    const item=document.createElement('div');
                    item.className='chart-legend-item';
                    item.innerHTML=`<span class="chart-legend-swatch" style="background:${s.color}"></span><span>${s.name}</span>`;
                    legend.appendChild(item);
                });
                body.appendChild(legend);
                const meta=document.createElement('div');
                meta.className='chart-meta-stack';
                const range=labels.length?rangeLabel(labels[0],labels[labels.length-1]):'';
                meta.innerHTML=`<span>Trailing ${def.params.trailing} units</span><span>${def.params.days} day window</span>${range?`<span>${range}</span>`:''}`;
                body.appendChild(meta);
                function redraw(){
                    const {width,height}=resizeCanvas(canvas,ctx);
                    drawLineChart(ctx,width,height,labels,series,{formatLabel:formatDateLabel,yPrecision:1});
                }
                redraw();
                setCardHandler(card,{redraw,contentEl:body});
            }

            async function renderPie(card,body,def){
                const data=await fetchPie(def.params.days);
                const total=Number(data.total_hours)||0;
                const departments=(data.departments||[]).map((d,i)=>({
                    name:d.name,
                    hours:Number(d.hours)||0,
                    color:pieColors[i % pieColors.length]
                })).filter(d=>d.hours>0);
                body.innerHTML='';
                const {canvas,ctx}=prepareCanvas(body);
                const info=document.createElement('div');
                info.className='chart-meta-stack';
                const range=rangeLabel(data.start_date,data.end_date);
                info.innerHTML=`<span>${(total||0).toFixed(1)}h total</span>${range?`<span>${range}</span>`:''}`;
                body.appendChild(info);
                function redraw(){
                    if(!departments.length){
                        ctx.clearRect(0,0,canvas.width,canvas.height);
                        body.innerHTML='<div class="error">No hours for this period</div>';
                        return;
                    }
                    const {width,height}=resizeCanvas(canvas,ctx);
                    const sum=departments.reduce((acc,d)=>acc+d.hours,0) || total || 1;
                    drawPie(ctx,width,height,departments,sum);
                }
                redraw();
                setCardHandler(card,{redraw,contentEl:body});
            }

            async function renderAvg(card,body,def){
                const data=await fetchAvg();
                const winKey=String(def.params.window);
                const windows=data.windows || (data.data && data.data.windows) || {};
                const windowData=windows[winKey];
                if(!windowData){throw new Error('Window not available');}
                body.innerHTML='';
                const grid=document.createElement('div');
                grid.className='chart-gauge-grid';
                body.appendChild(grid);
                const labels=windowData.labels||[];
                const windowRange=labels.length?rangeLabel(labels[0],labels[labels.length-1]):'';
                function buildGauge(label,value,unit,maxValue,metaText){
                    const wrap=document.createElement('div');
                    wrap.style.display='flex';
                    wrap.style.flexDirection='column';
                    wrap.style.alignItems='center';
                    wrap.style.gap='0.6rem';
                    const title=document.createElement('div');
                    title.className='gauge-label';
                    title.textContent=label;
                    const gauge=document.createElement('div');
                    gauge.className='gauge';
                    const fill=document.createElement('div');
                    fill.className='gauge-fill';
                    const center=document.createElement('div');
                    center.className='gauge-center';
                    const valueEl=document.createElement('div');
                    valueEl.className='gauge-value';
                    valueEl.textContent=unit(value);
                    const meta=document.createElement('div');
                    meta.className='gauge-meta';
                    gauge.appendChild(fill);
                    center.appendChild(valueEl);
                    center.appendChild(meta);
                    gauge.appendChild(center);
                    wrap.appendChild(title);
                    wrap.appendChild(gauge);
                    const limit=maxValue && maxValue>0?maxValue:Math.max(value,1);
                    const ratio=limit?Math.min(1,Math.max(0,value/limit)):0;
                    const sweep=Math.round(ratio*360);
                    fill.style.background=`conic-gradient(#58a6ff ${sweep}deg,#111821 ${sweep}deg)`;
                    const parts=[];
                    if(metaText) parts.push(metaText);
                    if(windowRange) parts.push(windowRange);
                    meta.innerHTML=parts.map(txt=>`<span>${txt}</span>`).join('');
                    return wrap;
                }
                const eff=mean(windowData.avg_efficiency);
                const act=mean(windowData.avg_act_days);
                const span=mean(windowData.avg_span);
                const maxAct=windowMax(windowData.avg_act_days);
                const maxSpan=windowMax(windowData.avg_span);
                grid.appendChild(buildGauge('Avg Efficiency',eff,v=>v.toFixed(1)+'%',100,''));
                grid.appendChild(buildGauge('Avg Active Days',act,v=>v.toFixed(1)+' days',maxAct||act||1,`Peak ${ (maxAct||act||0).toFixed(1) } days`));
                grid.appendChild(buildGauge('Avg Span',span,v=>v.toFixed(1)+' days',maxSpan||span||1,`Peak ${ (maxSpan||span||0).toFixed(1) } days`));
                setCardHandler(card,{redraw:null,contentEl:body});
            }

            function windowMax(values){
                if(!values || !values.length) return 0;
                let max=-Infinity;
                values.forEach(v=>{const num=Number(v);if(!isNaN(num) && num>max) max=num;});
                return max===-Infinity?0:max;
            }

            async function renderDailyTotals(card,body,def){
                const data=await fetchDailyTotals(def.params.days);
                const dates=data.dates||[];
                const total=data.total||{};
                const targetDays=def.params.days;
                const labels=dates.slice(-targetDays);
                const seriesData=def.params.view==='ma7'?(total.ma7||[]):(total.hours||[]);
                let dataSlice=seriesData.slice(-(labels.length));
                if(dataSlice.length<labels.length){
                    const padLength=labels.length-dataSlice.length;
                    dataSlice=[...Array(padLength).fill(null), ...dataSlice];
                }
                const {canvas,ctx}=prepareCanvas(body);
                const meta=document.createElement('div');
                meta.className='chart-meta-stack';
                const descriptor=def.params.view==='ma7'?'Trailing 7-day avg':'Actual totals';
                const range=labels.length?rangeLabel(labels[0],labels[labels.length-1]):'';
                meta.innerHTML=`<span>${descriptor}</span>${range?`<span>${range}</span>`:''}`;
                body.appendChild(meta);
                function redraw(){
                    const {width,height}=resizeCanvas(canvas,ctx);
                    drawLineChart(ctx,width,height,labels,[{name:'Hours',color:'#8fb9ff',data:dataSlice}],{formatLabel:formatDateLabel,yPrecision:0});
                }
                redraw();
                setCardHandler(card,{redraw,contentEl:body});
            }

            const pieColors=['#6ea8fe','#f6c177','#3fb950','#ff8c69','#9d79f2','#3dd68c','#ff6ec7','#6ad7ff','#f4a259','#8dc891'];

            function rangeLabel(start,end){
                if(!start || !end) return '';
                try{
                    const s=new Date(start+'T00:00:00');
                    const e=new Date(end+'T00:00:00');
                    const startTxt=s.toLocaleDateString('en-US',{month:'short',day:'numeric'});
                    const endTxt=e.toLocaleDateString('en-US',{month:'short',day:'numeric'});
                    return `${startTxt} - ${endTxt}`;
                }catch{return '';
                }
            }

            populatePicker();

            window.addEventListener('resize',()=>{
                cardHandlers.forEach(handler=>{
                    if(handler && typeof handler.redraw==='function') handler.redraw();
                });
            });

            function toggleFullScreen(){
                if(!isFullScreen){
                    const elem=document.documentElement;
                    if(elem.requestFullscreen){
                        const req=elem.requestFullscreen();
                        if(req && typeof req.catch==='function'){
                            req.catch(()=>enterDisplayMode());
                        }
                    }else{
                        enterDisplayMode();
                    }
                }else{
                    if(document.fullscreenElement && document.exitFullscreen){
                        document.exitFullscreen();
                    }
                    exitDisplayMode();
                }
            }

            function enterDisplayMode(){
                document.body.classList.add('fullscreen-active');
                document.querySelectorAll('.display-card').forEach(card=>card.classList.add('locked'));
                isFullScreen=true;
                fullScreenBtn.textContent='Exit Full Screen';
                closePicker();
            }

            function exitDisplayMode(){
                document.body.classList.remove('fullscreen-active');
                document.querySelectorAll('.display-card').forEach(card=>card.classList.remove('locked'));
                isFullScreen=false;
                fullScreenBtn.textContent='Full Screen';
            }

            fullScreenBtn.addEventListener('click',toggleFullScreen);

            document.addEventListener('fullscreenchange',()=>{
                if(document.fullscreenElement){
                    enterDisplayMode();
                }else if(isFullScreen){
                    exitDisplayMode();
                }
            });
        })();
        </script>
    </body>
    </html>
    """
    return render_template_string(page)


@dashboard.route('/dr', methods=['GET', 'POST'])
def dr_lookup():
        # DR lookup page. Detect probable columns and search broadly across patterns.
        import sqlite3
        query_dr = request.values.get('dr', '').strip()
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('PRAGMA table_info(SCHLabor)')
            lcols = {r[1] for r in cur.fetchall()}

            # Preferred explicit column if present
            dr_col = None
            for cand in ('DRNumber', 'DR', 'DispatchRequest', 'DR_No', 'MKNumber', 'MK_No'):
                if cand in lcols:
                    dr_col = cand
                    break

            # Build list of likely text columns to scan when explicit col absent
            likely_text_cols = [
                'Description','Comments','Comment','Notes','Note','Reference','Ref',
                'WorkOrder','WorkOrderNumber','WO','WONumber','Ticket','TicketNumber',
                'Dispatch','DispatchRequest','Job','JobNumber','JobID',
                'MK','MKNumber','MK_No'
            ]
            # Add any column containing keywords
            for c in list(lcols):
                uc = c.upper()
                if any(k in uc for k in ('DR','DISPATCH','TICKET','WORKORDER','MK')) and c not in likely_text_cols:
                    likely_text_cols.append(c)
            # Keep only existing
            likely_text_cols = [c for c in likely_text_cols if c in lcols]

            rows = []
            totals = {}
            searched_cols_info = []
            if query_dr:
                digits = ''.join(ch for ch in query_dr if ch.isdigit())
                patterns = []
                if digits:
                    patterns.extend([f'%{digits}%', f'%DR {digits}%', f'%DR#{digits}%', f'%DR-{digits}%'])
                patterns.append(f'%{query_dr}%')

                if dr_col:
                    wh = ' OR '.join([f'CAST({dr_col} AS TEXT) LIKE ?' for _ in patterns])
                    sql = f'''
                        SELECT COALESCE(EmployeeName,'') AS EmployeeName,
                               COALESCE(DepartmentNumber,'') AS DeptCode,
                               SUM(COALESCE(ActualHours,0)) AS Hours
                        FROM SCHLabor
                        WHERE {wh}
                        GROUP BY EmployeeName, DepartmentNumber
                        ORDER BY DeptCode, EmployeeName
                    '''
                    cur.execute(sql, patterns)
                    rows = [dict(r) for r in cur.fetchall()]
                    searched_cols_info.append(dr_col)
                else:
                    conds = []
                    params = []
                    for col in likely_text_cols:
                        for p in patterns:
                            conds.append(f'CAST({col} AS TEXT) LIKE ?')
                            params.append(p)
                    if conds:
                        sql = '''
                            SELECT COALESCE(EmployeeName,'') AS EmployeeName,
                                   COALESCE(DepartmentNumber,'') AS DeptCode,
                                   SUM(COALESCE(ActualHours,0)) AS Hours
                            FROM SCHLabor
                            WHERE ''' + ' OR '.join(conds) + '''
                            GROUP BY EmployeeName, DepartmentNumber
                            ORDER BY DeptCode, EmployeeName
                        '''
                        cur.execute(sql, params)
                        rows = [dict(r) for r in cur.fetchall()]
                        searched_cols_info.extend(likely_text_cols)
                for r in rows:
                    d = r.get('DeptCode') or 'UNKNOWN'
                    totals[d] = totals.get(d, 0.0) + float(r.get('Hours') or 0)

            # Render inline template
            if query_dr and dr_col:
                search_message = (
                    "<p style='opacity:.8;margin-top:.5rem;'>"
                    "Searching in column: <strong>"
                    f"{html.escape(dr_col)}</strong></p>"
                )
            elif query_dr:
                scanned = ''
                if searched_cols_info:
                    scanned = 'Scanned columns: ' + html.escape(', '.join(searched_cols_info))
                search_message = (
                    "<p style='opacity:.7;margin-top:.5rem;'>"
                    f"{scanned}</p>"
                )
            else:
                search_message = ''

            no_matches_html = (
                "<p style='margin-top:1rem;opacity:.7;'>No matches found.</p>"
                if query_dr and not rows
                else ''
            )

            rows_table = ''
            if rows:
                row_html = '\n'.join(
                    f"<tr><td>{html.escape(r.get('EmployeeName', ''))}</td>"
                    f"<td>{html.escape(r.get('DeptCode') or 'UNKNOWN')}</td>"
                    f"<td>{float(r.get('Hours') or 0):.2f}</td></tr>"
                    for r in rows
                )
                totals_html = '\n'.join(
                    f"<tr><td>{html.escape(k)}</td><td>{v:.2f}</td></tr>"
                    for k, v in totals.items()
                )
                rows_table = (
                    "<table>"
                    "<thead><tr><th>Employee</th><th>Dept</th><th>Hours</th></tr></thead>"
                    "<tbody>"
                    f"{row_html}"
                    "</tbody></table>"
                    "<h3 style='margin-top:1rem;'>Totals by Dept</h3>"
                    "<table style='width:auto'>"
                    "<thead><tr><th>Department</th><th>Total Hours</th></tr></thead>"
                    "<tbody>"
                    f"{totals_html}"
                    "</tbody></table>"
                )

            page = f"""
            <!doctype html><html><head><meta charset='utf-8'><title>DR Labor Lookup</title>
            <style>
                body{{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}}
                header{{padding:1rem 1.5rem;display:flex;flex-direction:column;gap:.75rem;background:#161b22;border-bottom:1px solid #30363d}}
                .header-row{{display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:.75rem;width:100%}}
                h1{{margin:0;font-size:1.05rem}}
                main{{padding:1rem 1.2rem}}
                form input,form button{{border-radius:6px;border:1px solid #30363d;background:#11161d;color:#e6edf3;padding:.5rem .65rem}}
                form button{{background:#1f6feb;border-color:#3a78e0;font-weight:700;cursor:pointer}}
                .alpha-banner{{flex-basis:100%;background:linear-gradient(135deg,#d97706,#92400e);border:1px solid rgba(255,255,255,0.18);border-radius:10px;padding:.65rem .9rem;font-size:.68rem;line-height:1.35;font-weight:600;color:#fbe5e8;box-shadow:0 6px 18px -14px #000;letter-spacing:.02rem}}
                .alpha-banner strong{{display:block;font-size:.7rem;margin-bottom:.25rem;color:#ffe2cc}}
                table{{border-collapse:collapse;width:100%;margin-top:1rem;font-size:.8rem}}
                th,td{{border:1px solid #2a323c;padding:.35rem .5rem;text-align:left}}
                th{{background:#1a2330}}
                .nav-links{{padding:0.5rem 1.5rem;display:flex;gap:1rem;background:#0d1117}}
                .nav-links a{{color:#8fb9ff;text-decoration:none;font-size:.85rem}}
                .nav-links a:hover{{text-decoration:underline}}
            </style></head>
            <body>
                <header><div class='header-row'><h1>DR Labor Lookup</h1></div><p class='alpha-banner'><strong>Alpha testing preview</strong>These dashboards are experimental. Data may be incomplete or inaccurate—do not rely on them for operational or business decisions.</p></header>
                <div class='nav-links'><a href='/dash'>&larr; Dashboard</a></div>
                <main>
                    <form method='GET'>
                        <label for='dr'>DR#:</label>
                        <input id='dr' name='dr' type='text' value='{html.escape(query_dr)}' placeholder='e.g. 12345' required />
                        <button type='submit'>Lookup</button>
                    </form>
                    {search_message}
                    {no_matches_html}
                    {rows_table}
                </main>
            </body></html>
            """
            return page


@dashboard.route('/emp')
def employee_lookup():
    # Inline page for employee lookup with suggestions and stats
    page = """
    <!doctype html><html><head><meta charset='utf-8'><title>Employee Lookup</title>
    <style>
        body{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}
        header{padding:1rem 1.5rem;display:flex;flex-direction:column;gap:.75rem;background:#161b22;border-bottom:1px solid #30363d}
        .header-row{display:flex;justify-content:space-between;align-items:center;gap:.75rem;width:100%}
        h1{margin:0;font-size:1.05rem}
        main{padding:1rem 1.2rem}
        .row{display:flex;gap:.6rem;flex-wrap:wrap;align-items:end}
        label{font-size:.72rem;opacity:.9}
        input,button{border-radius:6px;border:1px solid #30363d;background:#11161d;color:#e6edf3;padding:.5rem .65rem}
        input[type=date]{padding:.4rem .5rem}
        button{background:#238636;border-color:#2ea043;font-weight:700;cursor:pointer}
        .sugg{position:relative}
        .slist{position:absolute;z-index:10;background:#0f151c;border:1px solid #2a323c;border-radius:6px;min-width:240px;max-height:220px;overflow:auto;box-shadow:0 6px 18px -8px #000}
        .sopt{padding:.4rem .6rem;cursor:pointer}
        .sopt:hover{background:#1a2230}
        .alpha-banner{width:100%;background:linear-gradient(135deg,#d97706,#92400e);border:1px solid rgba(255,255,255,0.22);border-radius:10px;padding:.65rem .9rem;font-size:.68rem;line-height:1.4;font-weight:600;color:#fdf2f2;box-shadow:0 6px 18px -14px #000;letter-spacing:.02rem}
        .alpha-banner strong{display:block;font-size:.7rem;margin-bottom:.25rem;color:#ffe2cc}
    table{border-collapse:collapse;width:100%;margin-top:1rem;font-size:.85rem}
    th,td{border:1px solid #2a323c;padding:.45rem .6rem;text-align:left}
        th{background:#1a2330}
    .pill{display:inline-block;background:#1f6feb33;border:1px solid #1f6feb55;border-radius:20px;padding:.35rem .6rem;font-size:.65rem;margin:.3rem .4rem 0 0}
    .miniBar{height:8px;background:#263040;border-radius:4px;position:relative;overflow:hidden}
    .miniBar > span{position:absolute;left:0;top:0;bottom:0;background:linear-gradient(90deg,#2f9e44,#52d96d)}
    .nav-links{padding:0.5rem 1.5rem;display:flex;gap:1rem;background:#0d1117}
    .nav-links a{color:#8fb9ff;text-decoration:none;font-size:.85rem}
    .nav-links a:hover{text-decoration:underline}
    </style></head>
    <body>
        <header><div class='header-row'><h1>Employee Lookup</h1></div><p class='alpha-banner'><strong>Alpha testing preview</strong>These dashboards are experimental. Data may be incomplete or inaccurate—do not rely on them for operational or business decisions.</p></header>
        <div class='nav-links'><a href='/dash'>&larr; Dashboard</a></div>
        <main>
            <div class='row'>
                <div class='sugg'>
                    <label>Last</label><br/>
                    <input id='last' placeholder='Last name' autocomplete='off' />
                    <div id='slist' class='slist' style='display:none'></div>
                </div>
                <div>
                    <label>First</label><br/>
                    <input id='first' placeholder='First name' autocomplete='off' />
                </div>
                <div>
                    <label>Start</label><br/>
                    <input id='start' type='date' />
                </div>
                <div>
                    <label>End</label><br/>
                    <input id='end' type='date' />
                </div>
                <div>
                    <button id='runBtn'>Run</button>
                </div>
            </div>
            <div id='summary' style='margin-top:1rem'></div>
            <div id='results'></div>
        </main>
        <script>
        function todayISO(){const d=new Date();return d.toISOString().slice(0,10)}
        function addDaysISO(dstr, days){const d=new Date(dstr); d.setDate(d.getDate()+days); return d.toISOString().slice(0,10)}
        const lastInp=document.getElementById('last');
        const firstInp=document.getElementById('first');
        const startInp=document.getElementById('start');
        const endInp=document.getElementById('end');
        const slist=document.getElementById('slist');
        endInp.value=todayISO();
        startInp.value=addDaysISO(endInp.value,-60);

        let suggTimer=null;
        function fetchSuggest(){
            const last=lastInp.value.trim();
            const first=firstInp.value.trim();
            if(!last && !first){slist.style.display='none'; return}
            const url=new URL(window.location.origin + '/api/employee/suggest');
            if(last) url.searchParams.set('last',last);
            if(first) url.searchParams.set('first',first);
            fetch(url).then(r=>r.json()).then(data=>{
                const arr=data.suggestions||[];
                if(arr.length===0){slist.style.display='none'; return}
                slist.innerHTML=arr.map(s=>`<div class='sopt' data-name="${s.name}">${s.name}</div>`).join('');
                slist.style.display='block';
                document.querySelectorAll('.sopt').forEach(el=>{
                    el.onclick=()=>{
                        const nm=el.getAttribute('data-name');
                        const parts=nm.split(',');
                        lastInp.value=(parts[0]||'').trim();
                        firstInp.value=(parts[1]||'').trim();
                        slist.style.display='none';
                    };
                });
            }).catch(()=>{slist.style.display='none'});
        }
        function scheduleSuggest(){ if(suggTimer) clearTimeout(suggTimer); suggTimer=setTimeout(fetchSuggest, 180); }
        lastInp.addEventListener('input', scheduleSuggest);
        firstInp.addEventListener('input', scheduleSuggest);
        document.addEventListener('click', (e)=>{ if(!slist.contains(e.target) && e.target!==lastInp) slist.style.display='none'; });

        function runQuery(){
            const last=lastInp.value.trim();
            const first=firstInp.value.trim();
            const start=startInp.value; const end=endInp.value;
            const url=new URL(window.location.origin + '/api/employee/stats');
            if(last) url.searchParams.set('last', last);
            if(first) url.searchParams.set('first', first);
            if(start) url.searchParams.set('start', start);
            if(end) url.searchParams.set('end', end);
            fetch(url).then(r=>r.json()).then(data=>{
                const sDiv=document.getElementById('summary');
                const rDiv=document.getElementById('results');
                if(data.error){ sDiv.textContent=data.error; rDiv.innerHTML=''; return; }
                sDiv.innerHTML=`
                    <span class='pill'>${(data.name||'').replaceAll('<','&lt;')}</span>
                    <span class='pill'>Overall First: ${data.overall_first||'-'}</span>
                    <span class='pill'>Overall Last: ${data.overall_last||'-'}</span>
                    <span class='pill'>Range: ${(data.start||'-')} to ${(data.end||'-')}</span>
                    <span class='pill'>Total Hours: ${Number(data.total_hours||0).toFixed(2)}</span>
                `;
                const allMap = new Map((data.by_dept_all||[]).map(x=>[x.code,{hours:x.hours,percent:x.percent,label:x.label}]));
                const merged = (data.by_dept||[]).map(d=>{
                    const all = allMap.get(d.code) || {hours:0,percent:0,label:d.label};
                    return {...d, all_percent: all.percent};
                });
                (data.by_dept_all||[]).forEach(x=>{ if(!merged.find(m=>m.code===x.code)) merged.push({code:x.code,label:x.label,hours:0,percent:0,all_percent:x.percent}); });
                merged.sort((a,b)=> (b.percent - a.percent) || (b.all_percent - a.all_percent));
                const rows=merged.map(d=>`<tr>
                    <td>${d.code||''}</td>
                    <td>${d.label||''}</td>
                    <td>${Number(d.hours||0).toFixed(2)}</td>
                    <td>
                        <div style='display:flex;align-items:center;gap:.4rem;'>
                            <span>${Number(d.percent||0).toFixed(1)}%</span>
                            <div class='miniBar' style='width:120px'><span style='width:${Math.min(100,Math.max(0,d.percent||0))}%' ></span></div>
                        </div>
                    </td>
                    <td style='opacity:.85'>${Number(d.all_percent||0).toFixed(1)}% (all-time)</td>
                </tr>`).join('');
                rDiv.innerHTML = (rows? `<table><thead><tr><th>Dept Code</th><th>Department</th><th>Hours (range)</th><th>% of Time (range)</th><th>All-time % of Time</th></tr></thead><tbody>${rows}</tbody></table>` : '<p style=\"opacity:.7;\">No data for selection.</p>');
            });
        }
        document.getElementById('runBtn').addEventListener('click', runQuery);
        [lastInp, firstInp, startInp, endInp].forEach(inp=> inp.addEventListener('change', ()=>{ if(lastInp.value||firstInp.value) runQuery(); }));
        </script>
    </body></html>
    """
    return render_template_string(page)


@dashboard.route('/parts')
def parts_page():
    page = """
    <!doctype html><html><head><meta charset='utf-8'><title>Parts Tracker</title>
    <style>
    body{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}
    header{padding:1rem 1.5rem;display:flex;flex-direction:column;gap:.75rem;background:#161b22;border-bottom:1px solid #30363d}
    .header-row{display:flex;justify-content:space-between;align-items:center;gap:.75rem;width:100%}
        h1{margin:0;font-size:1.05rem}
        main{padding:1rem 1.2rem}
        .row{display:flex;gap:.6rem;flex-wrap:wrap;align-items:end}
        label{font-size:.72rem;opacity:.9}
        input,button{border-radius:6px;border:1px solid #30363d;background:#11161d;color:#e6edf3;padding:.5rem .65rem}
        button{background:#1f6feb;border-color:#3a78e0;font-weight:700;cursor:pointer}
    .alpha-banner{width:100%;background:linear-gradient(135deg,#d97706,#92400e);border:1px solid rgba(255,255,255,0.22);border-radius:10px;padding:.65rem .9rem;font-size:.68rem;line-height:1.4;font-weight:600;color:#fdf2f2;box-shadow:0 6px 18px -14px #000;letter-spacing:.02rem}
    .alpha-banner strong{display:block;font-size:.7rem;margin-bottom:.25rem;color:#ffe2cc}
        table{border-collapse:collapse;width:100%;margin-top:1rem;font-size:.8rem}
        th,td{border:1px solid #2a323c;padding:.35rem .5rem;text-align:left;vertical-align:top;max-width:420px;overflow:hidden;text-overflow:ellipsis}
        th{background:#1a2330}
        .pill{display:inline-block;background:#1f6feb33;border:1px solid #1f6feb55;border-radius:20px;padding:.35rem .6rem;font-size:.65rem;margin:.3rem .4rem 0 0}
        .nav-links{padding:0.5rem 1.5rem;display:flex;gap:1rem;background:#0d1117}
        .nav-links a{color:#8fb9ff;text-decoration:none;font-size:.85rem}
        .nav-links a:hover{text-decoration:underline}
    </style></head>
    <body>
        <header><div class='header-row'><h1>Parts Tracker</h1></div><p class='alpha-banner'><strong>Alpha testing preview</strong>These dashboards are experimental. Data may be incomplete or inaccurate—do not rely on them for operational or business decisions.</p></header>
        <div class='nav-links'><a href='/dash'>&larr; Dashboard</a></div>
        <main>
            <div class='row'>
                <div>
                    <label>Part Number</label><br/>
                    <input id='part' placeholder='e.g. 123-ABC' />
                </div>
                <div>
                    <label>COM#</label><br/>
                    <input id='com' placeholder='e.g. 12345' />
                </div>
                <div>
                    <button id='runBtn'>Search</button>
                </div>
            </div>
            <div id='summary' style='margin-top:.6rem'></div>
            <div id='results'></div>
        </main>
        <script>
        function run(){
            const part = document.getElementById('part').value.trim();
            const com = document.getElementById('com').value.trim();
            const url = new URL(window.location.origin + '/api/parts/search');
            if(part) url.searchParams.set('part', part);
            if(com) url.searchParams.set('com', com);
            fetch(url).then(r=>r.json()).then(data=>{
                const s=document.getElementById('summary');
                const div=document.getElementById('results');
                if(data.error){ s.textContent = data.error; div.innerHTML=''; return; }
                const rows = data.rows||[];
                s.innerHTML = `<span class='pill'>Matches: ${rows.length}</span>`;
                if(rows.length===0){ div.innerHTML='<p style="opacity:.7">No results.</p>'; return; }
                const cols = Object.keys(rows[0]);
                const head = '<tr>' + cols.map(c=>`<th>${c}</th>`).join('') + '</tr>';
                const body = rows.map(r=> '<tr>' + cols.map(c=>`<td>${(r[c]??'').toString().replaceAll('<','&lt;')}</td>`).join('') + '</tr>').join('');
                div.innerHTML = `<table><thead>${head}</thead><tbody>${body}</tbody></table>`;
            }).catch(()=>{ document.getElementById('summary').textContent='Error'; });
        }
        document.getElementById('runBtn').addEventListener('click', run);
        </script>
    </body></html>
    """
    return render_template_string(page)


@dashboard.route('/com_totals')
def com_totals_page():
    com = (request.args.get('com') or '').strip()
    page = """
    <!doctype html><html><head><meta charset='utf-8'><title>COM# Totals by Employee</title>
    <style>
        body{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}
        header{padding:1rem 1.5rem;display:flex;flex-direction:column;gap:.75rem;background:#161b22;border-bottom:1px solid #30363d}
        .header-row{display:flex;justify-content:space-between;align-items:center;gap:.75rem;width:100%}
        h1{margin:0;font-size:1.05rem}
        main{padding:1rem 1.2rem}
        table{border-collapse:collapse;width:100%;margin-top:1rem;font-size:.9rem}
        th,td{border:1px solid #2a323c;padding:.5rem .7rem;text-align:left}
        th{background:#1a2330}
        .pill{display:inline-block;background:#1f6feb33;border:1px solid #1f6feb55;border-radius:20px;padding:.35rem .6rem;font-size:.65rem;margin:.3rem .4rem 0 0}
        .nav-links{padding:0.5rem 1.5rem;display:flex;gap:1rem;background:#0d1117}
        .nav-links a{color:#8fb9ff;text-decoration:none;font-size:.85rem}
        .nav-links a:hover{text-decoration:underline}
        .alpha-banner{width:100%;background:linear-gradient(135deg,#d97706,#92400e);border:1px solid rgba(255,255,255,0.22);border-radius:10px;padding:.65rem .9rem;font-size:.68rem;line-height:1.4;font-weight:600;color:#fdf2f2;box-shadow:0 6px 18px -14px #000;letter-spacing:.02rem}
        .alpha-banner strong{display:block;font-size:.7rem;margin-bottom:.25rem;color:#ffe2cc}
    </style></head>
    <body>
    <header><div class='header-row'><h1>COM# Totals by Employee</h1></div><p class='alpha-banner'><strong>Alpha testing preview</strong>These dashboards are experimental. Data may be incomplete or inaccurate—do not rely on them for operational or business decisions.</p></header>
    <div class='nav-links'>
        <a href='/com'>&larr; Back to Charges</a>
        <a href='/dash'>&larr; Dashboard</a>
    </div>
        <main>
            <div id='summary'></div>
            <div id='results'></div>
        </main>
        <script>
        function loadTotals(){
            const url = new URL(window.location.origin + '/api/com/employee_totals');
            const params = new URLSearchParams(window.location.search);
            const com = params.get('com')||'';
            if(com) url.searchParams.set('com', com);
            fetch(url).then(r=>r.json()).then(data=>{
                const s=document.getElementById('summary');
                const div=document.getElementById('results');
                if(data.error){ s.textContent=data.error; div.innerHTML=''; return; }
                s.innerHTML = `<span class='pill'>COM ${data.com||''}</span> <span class='pill'>Employees: ${data.count||0}</span>`;
                const rows = data.rows||[];
                if(rows.length===0){ div.innerHTML='<p style=\"opacity:.7\">No results.</p>'; return; }
                const head = '<tr><th>Employee</th><th>Total Hours</th><th>Entries</th></tr>';
                const body = rows.map(r=> `<tr><td>${(r.employee||'').toString().replaceAll('<','&lt;')}</td><td>${Number(r.hours||0).toFixed(2)}</td><td>${r.entries||0}</td></tr>`).join('');
                div.innerHTML = `<table><thead>${head}</thead><tbody>${body}</tbody></table>`;
            });
        }
        (function(){
            const params = new URLSearchParams(window.location.search);
            const com = params.get('com')||'';
            if(com){ const a=document.getElementById('backLink'); a.href = '/com?com='+encodeURIComponent(com); }
        })();
        loadTotals();
        </script>
    </body></html>
    """
    return render_template_string(page)


@dashboard.route('/com')
def com_lookup():
    # Simple COM lookup UI
    page = """
    <!doctype html><html><head><meta charset='utf-8'><title>COM# Charges</title>
    <style>
        body{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}
        header{padding:1rem 1.5rem;display:flex;flex-direction:column;gap:.75rem;background:#161b22;border-bottom:1px solid #30363d}
        .header-row{display:flex;justify-content:space-between;align-items:center;gap:.75rem;width:100%}
        h1{margin:0;font-size:1.05rem}
        main{padding:1rem 1.2rem}
        input,button{border-radius:6px;border:1px solid #30363d;background:#11161d;color:#e6edf3;padding:.5rem .65rem}
        button{background:#238636;border-color:#2ea043;font-weight:700;cursor:pointer}
        table{border-collapse:collapse;width:100%;margin-top:1rem;font-size:.85rem}
        th,td{border:1px solid #2a323c;padding:.45rem .6rem;text-align:left}
        th{background:#1a2330}
        .pill{display:inline-block;background:#1f6feb33;border:1px solid #1f6feb55;border-radius:20px;padding:.35rem .6rem;font-size:.65rem;margin:.3rem .4rem 0 0}
        .nav-links{padding:0.5rem 1.5rem;display:flex;gap:1rem;background:#0d1117}
        .nav-links a{color:#8fb9ff;text-decoration:none;font-size:.85rem}
        .nav-links a:hover{text-decoration:underline}
        .alpha-banner{width:100%;background:linear-gradient(135deg,#d97706,#92400e);border:1px solid rgba(255,255,255,0.22);border-radius:10px;padding:.65rem .9rem;font-size:.68rem;line-height:1.4;font-weight:600;color:#fdf2f2;box-shadow:0 6px 18px -14px #000;letter-spacing:.02rem}
        .alpha-banner strong{display:block;font-size:.7rem;margin-bottom:.25rem;color:#ffe2cc}
    </style></head>
    <body>
        <header><div class='header-row'><h1>COM# Charges</h1></div><p class='alpha-banner'><strong>Alpha testing preview</strong>These dashboards are experimental. Data may be incomplete or inaccurate—do not rely on them for operational or business decisions.</p></header>
        <div class='nav-links'>
            <a href='/recent'>&larr; Recent Units</a>
            <a href='/dash'>&larr; Dashboard</a>
        </div>
        <main>
            <div>
                <label>COM#</label>
                <input id='com' placeholder='e.g. 12345' />
                <button id='runBtn'>Lookup</button>
                <button id='totBtn' style='margin-left:.4rem;background:#1f6feb;border-color:#3a78e0'>Totals by Employee</button>
            </div>
            <div id='summary' style='margin-top:.6rem'></div>
            <div id='ganttWrap' style='margin-top:1rem'>
                <h3 style='margin:.6rem 0 .4rem;font-size:.95rem'>Timeline (by Department)</h3>
                <div id='gantt'></div>
            </div>
            <div id='results' style='margin-top:1rem'></div>
        </main>
        <script>
    const CODE2LABEL = {"0120":"Fab","0140":"Welding","0180":"Base/Form/Paint","0200":"Fan Assembly","0220":"Insul Wall Fab","0230":"Pipe","0260":"Assembly","0270":"Door Fab","0280":"Flow Line","0300":"Electrical","0320":"Pipe (Alt)","0340":"Paint","0360":"Test","0380":"Crating"};
        const G_STYLE = document.createElement('style');
        G_STYLE.textContent = `
            .gantt{border:1px solid #2a323c;border-radius:8px;overflow:auto;background:#10161c}
            .grow{display:flex;align-items:center;border-top:1px solid #1a2230}
            .grow:first-child{border-top:none}
            .glabel{flex:0 0 180px;padding:.35rem .5rem;border-right:1px solid #1a2230;background:#121922;font-size:.85rem}
            .gstats{flex:0 0 120px;padding:.35rem .5rem;border-right:1px solid #1a2230;background:#0f161d;font-size:.75rem;display:flex;gap:.8rem;justify-content:center}
            .gstat{display:flex;flex-direction:column;align-items:center}
            .gstat-label{font-size:.6rem;color:#7b8a99;margin-bottom:2px}
            .gstat-value{font-size:.8rem;font-weight:600;color:#c9d1d9}
            .ggrid{display:flex;gap:3px;padding:.3rem .5rem}
            .gcell{width:16px;height:16px;border:1px solid #263040;background:#141b22;border-radius:2px;position:relative}
            .gcell.on{border-color:#3fb950}
            .gcell.col{box-shadow:0 0 0 1px #3a78e0 inset}
            .grow.hl .glabel{background:#172233}
            .grow.hl .gstats{background:#0d1419}
            .ghead{display:flex;align-items:center}
            .ghead .glabel{background:#0f161d;font-weight:700}
            .ghead .gstats{background:#0a0e12;font-weight:700;font-size:.7rem;color:#8b949e}
            .gtick{width:16px;height:16px;display:flex;align-items:center;justify-content:center;color:#7b8a99;font-size:.6rem}
            .gtick.hl{color:#bcd0ff;font-weight:700}
            .gtt{position:fixed;z-index:1000;pointer-events:none;background:#111820;border:1px solid #2a3440;color:#e6edf3;padding:.35rem .5rem;border-radius:6px;font-size:.7rem;box-shadow:0 6px 18px rgba(0,0,0,.45);display:none}
            .glegend{display:flex;align-items:center;gap:.5rem;padding:.4rem .5rem;border-bottom:1px solid #1a2230;background:#0f161d}
            .glegend .lab{font-size:.65rem;color:#9bb0c8}
            .glegend .bar{width:180px;height:10px;border-radius:5px;background:linear-gradient(90deg,hsl(140,65%,22%),hsl(140,65%,52%));border:1px solid #2a3544}
            .gunit{border-top:2px solid #2a4060}
            .gunit .glabel{background:#1a2433;font-weight:700;color:#8fb4ff}
            .gunit .gstats{background:#141d2a;border-top:1px solid #2a4060}
            .gunit .gstat-value{color:#8fb4ff;font-weight:700}
        `;
        document.head.appendChild(G_STYLE);
        const TIP = document.createElement('div');TIP.className='gtt';document.body.appendChild(TIP);

        function ymdToDate(s){ const [y,m,d]=s.split('-').map(Number); return new Date(y, m-1, d); }
        function addDays(d, n){ const x=new Date(d); x.setDate(x.getDate()+n); return x; }
        function ymd(d){ return d.toISOString().slice(0,10); }
        function renderGantt(rows, unitComplete){
            const wrap = document.getElementById('gantt');
            if(!rows || rows.length===0){ wrap.innerHTML=''; return; }
            // Aggregate hours, entries, and employees per dept/day; collect min/max dates
            // Also track filtering status per dept/day
            let minD=null, maxD=null;
            const byDeptHours = new Map(); // code -> Map(day -> sumHours)
            const byDeptEntries = new Map(); // code -> Map(day -> n entries)
            const byDeptEmps = new Map(); // code -> Map(day -> Set(emp))
            const byDeptStatus = new Map(); // code -> Map(day -> {hasValid, hasExcluded, hasFiltered, filterReason})
            let maxHours = 0;
            rows.forEach(r=>{
                const day = r.day; const dept = (r.dept||'').toString();
                if(!day) return;
                const dd = ymdToDate(day);
                if(!minD || dd<minD) minD=dd;
                if(!maxD || dd>maxD) maxD=dd;
                if(!byDeptHours.has(dept)) byDeptHours.set(dept, new Map());
                if(!byDeptEntries.has(dept)) byDeptEntries.set(dept, new Map());
                if(!byDeptEmps.has(dept)) byDeptEmps.set(dept, new Map());
                if(!byDeptStatus.has(dept)) byDeptStatus.set(dept, new Map());
                const mH = byDeptHours.get(dept);
                const mE = byDeptEntries.get(dept);
                const mP = byDeptEmps.get(dept);
                const mS = byDeptStatus.get(dept);
                const prev = Number(mH.get(day)||0);
                const add = Number(r.hours||0) || 0;
                const total = prev + add;
                mH.set(day, total);
                mE.set(day, (mE.get(day)||0) + 1);
                const emp = (r.employee||'').toString().trim();
                if(!mP.has(day)) mP.set(day, new Set());
                if(emp) mP.get(day).add(emp);
                // Track status: prioritize valid > excluded > filtered
                if(!mS.has(day)) mS.set(day, {hasValid: false, hasExcluded: false, hasFiltered: false, filterReason: null});
                const status = mS.get(day);
                if(r.is_excluded_employee) {
                    status.hasExcluded = true;
                } else if(r.is_filtered_out) {
                    status.hasFiltered = true;
                    // Store the filter reason (only need one, they should all be the same for the same day)
                    if(r.filter_reason && !status.filterReason) {
                        status.filterReason = r.filter_reason;
                    }
                } else {
                    status.hasValid = true;
                }
                if(total > maxHours) maxHours = total;
            });
            if(!minD || !maxD){ wrap.innerHTML=''; return; }
            // Build continuous day list
            const days=[]; for(let d=minD; d<=maxD; d=addDays(d,1)) days.push(ymd(d));
            // Header row (ticks every 5 days) - only show Act Days/Span if unit is complete
            let html = '<div class="grow ghead"><div class="glabel">Department</div>';
            if(unitComplete){
                html += '<div class="gstats">Act Days / Span</div>';
            }
            html += '<div class="ggrid">';
            for(let i=0;i<days.length;i++){
                if(i%5===0){ html += `<div class='gtick' data-day='${days[i]}'>${days[i].slice(8)}</div>`; } else { html += `<div class='gtick' data-day='${days[i]}'></div>`; }
            }
            html += '</div></div>';
            // Rows per dept code (keep codes distinct; labels are display-only)
            const entries = Array.from(byDeptHours.keys()).map(code=>({
                code,
                label: CODE2LABEL[code]||code,
                hours: byDeptHours.get(code),
                entries: byDeptEntries.get(code),
                emps: byDeptEmps.get(code),
                status: byDeptStatus.get(code)
            }));
            entries.sort((a,b)=> (a.label||'').localeCompare(b.label||'') || a.code.localeCompare(b.code));
            entries.forEach(ent=>{
                // Calculate Act Days and Span for this department - ONLY from valid (green) days
                let actDays = 0;
                let span = 0;
                if(unitComplete){
                    // Only count days with valid (non-excluded, non-filtered) charges
                    const validDays = Array.from(ent.status.keys()).filter(d => {
                        const st = ent.status.get(d);
                        return st.hasValid && ent.hours.get(d) > 0;
                    }).sort();
                    actDays = validDays.length;
                    if(validDays.length > 0){
                        const firstDay = ymdToDate(validDays[0]);
                        const lastDay = ymdToDate(validDays[validDays.length - 1]);
                        span = Math.floor((lastDay - firstDay) / (1000*60*60*24)) + 1;
                    }
                }
                html += `<div class='grow'><div class='glabel'>${ent.label}</div>`;
                if(unitComplete){
                    html += `<div class='gstats'><div class='gstat'><div class='gstat-label'>Act</div><div class='gstat-value'>${actDays}</div></div><div class='gstat'><div class='gstat-label'>Span</div><div class='gstat-value'>${span}</div></div></div>`;
                }
                html += `<div class='ggrid'>`;
                days.forEach(d=>{
                    const hrs = Number(ent.hours.get(d)||0);
                    const n = Number(ent.entries.get(d)||0);
                    const emps = ent.emps.get(d) ? ent.emps.get(d).size : 0;
                    const dayStatus = ent.status.get(d) || {hasValid: false, hasExcluded: false, hasFiltered: false};
                    const on = hrs>0 ? ' on' : '';
                    let style = '';
                    let statusNote = '';
                    
                    // Determine cell color based on filtering status (only if unit is 100% complete)
                    // Priority: Valid (green) > Excluded (yellow) > Filtered (red)
                    if(hrs>0){
                        if(unitComplete && dayStatus.hasValid){
                            // Has valid charges - show green (normal)
                            if(maxHours>0){
                                const t = Math.sqrt(hrs / maxHours); // perceptual scale
                                const light = 22 + Math.round(t*30); // 22% -> 52%
                                style = `style="background-color:hsl(140,65%,${light}%);"`;
                            }
                            // Note if also has excluded/filtered
                            if(dayStatus.hasExcluded && dayStatus.hasFiltered) statusNote = ' [MIXED: Valid + Excluded + Filtered]';
                            else if(dayStatus.hasExcluded) statusNote = ' [MIXED: Valid + Excluded]';
                            else if(dayStatus.hasFiltered) statusNote = ' [MIXED: Valid + Filtered]';
                        } else if(unitComplete && dayStatus.hasExcluded){
                            // Only excluded employees - show yellow
                            style = `style="background-color:#f0e68c;"`;
                            if(dayStatus.hasFiltered) statusNote = ' [EXCLUDED + FILTERED]';
                            else statusNote = ' [EXCLUDED EMPLOYEE]';
                        } else if(unitComplete && dayStatus.hasFiltered){
                            // Only filtered entries - show red
                            style = `style="background-color:#ff6b6b;"`;
                            statusNote = ' [FILTERED OUT]';
                        } else if(maxHours>0){
                            // Not unit complete or no status - normal green gradient
                            const t = Math.sqrt(hrs / maxHours); // perceptual scale
                            const light = 22 + Math.round(t*30); // 22% -> 52%
                            style = `style="background-color:hsl(140,65%,${light}%);"`;
                        }
                    }
                    const title = `${ent.code} ${ent.label} — ${d}\n${hrs.toFixed(2)} hours • ${n} charges • ${emps} employees${statusNote}`;
                    html += `<div class='gcell${on}' data-day='${d}' data-dept='${ent.label}' data-hours='${hrs.toFixed(2)}' data-entries='${n}' data-emps='${emps}' data-status='${JSON.stringify(dayStatus)}' title='${title.replaceAll("'","&apos;")}' ${style}></div>`;
                });
                html += '</div></div>';
            });
            
            // Add unit summary row (only if unit is complete)
            if(unitComplete){
                // Collect all valid days across all departments
                const unitValidDays = new Set();
                entries.forEach(ent => {
                    Array.from(ent.status.keys()).forEach(d => {
                        const st = ent.status.get(d);
                        if(st.hasValid && ent.hours.get(d) > 0){
                            unitValidDays.add(d);
                        }
                    });
                });
                
                // Calculate unit-level Act Days and Span
                const sortedValidDays = Array.from(unitValidDays).sort();
                const unitActDays = sortedValidDays.length;
                let unitSpan = 0;
                if(sortedValidDays.length > 0){
                    const firstDay = ymdToDate(sortedValidDays[0]);
                    const lastDay = ymdToDate(sortedValidDays[sortedValidDays.length - 1]);
                    unitSpan = Math.floor((lastDay - firstDay) / (1000*60*60*24)) + 1;
                }
                
                // Build unit summary row
                html += `<div class='grow gunit'><div class='glabel'>UNIT</div>`;
                html += `<div class='gstats'><div class='gstat'><div class='gstat-label'>Act</div><div class='gstat-value'>${unitActDays}</div></div><div class='gstat'><div class='gstat-label'>Span</div><div class='gstat-value'>${unitSpan}</div></div></div>`;
                html += `<div class='ggrid'>`;
                days.forEach(d => {
                    const hasValid = unitValidDays.has(d);
                    const on = hasValid ? ' on' : '';
                    const style = hasValid ? `style="background-color:hsl(200,70%,45%);"` : '';
                    const title = hasValid ? `Unit — ${d}\nValid charges exist` : `Unit — ${d}\nNo valid charges`;
                    html += `<div class='gcell${on}' data-day='${d}' data-dept='UNIT' title='${title}' ${style}></div>`;
                });
                html += '</div></div>';
            }
            
            let legend = '';
            if(unitComplete){
                legend += `<div class='glegend'><div style='width:20px;height:20px;background:#f0e68c;border-radius:3px;'></div><div class='lab'>Excluded Employee</div><div style='width:20px;height:20px;background:#ff6b6b;border-radius:3px;margin-left:1rem;'></div><div class='lab'>Filtered Out</div></div>`;
            }
            wrap.innerHTML = `<div class='gantt'>${legend}${html}</div>`;

            // Wire up hover interactions
            const cells = wrap.querySelectorAll('.gcell');
            function highlight(day, on){
                wrap.querySelectorAll(`.gcell[data-day="${day}"]`).forEach(el=> el.classList.toggle('col', on));
                const tick = wrap.querySelector(`.gtick[data-day="${day}"]`);
                if(tick) tick.classList.toggle('hl', on);
            }
            cells.forEach(cell=>{
                const row = cell.closest('.grow');
                cell.addEventListener('mouseenter', (e)=>{
                    const day = cell.getAttribute('data-day');
                    const dept = cell.getAttribute('data-dept');
                    const hrs = cell.getAttribute('data-hours');
                    const n = cell.getAttribute('data-entries');
                    const emps = cell.getAttribute('data-emps');
                    const statusStr = cell.getAttribute('data-status');
                    let dayStatus = {hasValid: false, hasExcluded: false, hasFiltered: false, filterReason: null};
                    try { dayStatus = JSON.parse(statusStr); } catch(e) {}
                    row.classList.add('hl');
                    highlight(day, true);
                    let statusBadge = '';
                    if(unitComplete){
                        const reason = dayStatus.filterReason ? `<div style='font-size:0.9em;margin-top:2px;opacity:0.9'>Reason: ${dayStatus.filterReason}</div>` : '';
                        if(dayStatus.hasValid && dayStatus.hasExcluded && dayStatus.hasFiltered){
                            statusBadge = `<div style='color:#8fb9ff;font-weight:600;margin-top:4px'>ℹ️ MIXED: Valid + Excluded + Filtered charges${reason}</div>`;
                        } else if(dayStatus.hasValid && dayStatus.hasExcluded){
                            statusBadge = `<div style='color:#8fb9ff;font-weight:600;margin-top:4px'>ℹ️ MIXED: Valid + Excluded charges</div>`;
                        } else if(dayStatus.hasValid && dayStatus.hasFiltered){
                            statusBadge = `<div style='color:#8fb9ff;font-weight:600;margin-top:4px'>ℹ️ MIXED: Valid + Filtered charges${reason}</div>`;
                        } else if(dayStatus.hasExcluded && dayStatus.hasFiltered){
                            statusBadge = `<div style='color:#f0ad4e;font-weight:600;margin-top:4px'>⚠ EXCLUDED + FILTERED${reason}</div>`;
                        } else if(dayStatus.hasExcluded){
                            statusBadge = `<div style='color:#f0e68c;font-weight:600;margin-top:4px'>⚠ EXCLUDED EMPLOYEE</div>`;
                        } else if(dayStatus.hasFiltered){
                            statusBadge = `<div style='color:#ff6b6b;font-weight:600;margin-top:4px'>⚠ FILTERED OUT${reason}</div>`;
                        }
                    }
                    TIP.innerHTML = `<div style='font-weight:600'>${dept}</div><div>${day}</div><div>${hrs} hours • ${n} charges • ${emps} employees</div>${statusBadge}`;
                    TIP.style.display='block';
                    TIP.style.left = (e.clientX + 14) + 'px';
                    TIP.style.top = (e.clientY + 14) + 'px';
                });
                cell.addEventListener('mousemove', (e)=>{
                    TIP.style.left = (e.clientX + 14) + 'px';
                    TIP.style.top = (e.clientY + 14) + 'px';
                });
                cell.addEventListener('mouseleave', ()=>{
                    TIP.style.display='none';
                    const day = cell.getAttribute('data-day');
                    highlight(day, false);
                    row.classList.remove('hl');
                });
            });
        }
        
        function run(){
            const com = document.getElementById('com').value.trim();
            const url = new URL(window.location.origin + '/api/com/charges');
            if(com) url.searchParams.set('com', com);
            fetch(url).then(r=>r.json()).then(data=>{
                const s=document.getElementById('summary');
                const div=document.getElementById('results');
                if(data.error){ s.textContent=data.error; div.innerHTML=''; return; }
                const rows = data.rows||[];
                const unitComplete = data.unit_complete || false;
                let pills = `<span class='pill'>COM ${data.com||''}</span> <span class='pill'>Rows: ${rows.length}</span>`;
                if(unitComplete){
                    pills += `<span class='pill' style='background:#28a74522;border-color:#28a74566'>Unit 100% Complete</span>`;
                }
                s.innerHTML = pills;
                if(rows.length===0){ div.innerHTML='<p style=\"opacity:.7\">No results.</p>'; return; }
                const head = '<tr><th>Date</th><th>Employee</th><th>Dept</th><th>Hours</th><th>Status</th></tr>';
                const body = rows.map(r=> {
                    let status = 'Valid';
                    let style = '';
                    let title = '';
                    if(unitComplete){
                        if(r.is_excluded_employee){
                            status = 'Excluded Employee';
                            style = 'background:#f0e68c22';
                        } else if(r.is_filtered_out){
                            status = 'Filtered Out';
                            style = 'background:#ff000022';
                            title = r.filter_reason ? `title="${r.filter_reason.replaceAll('"','&quot;')}"` : '';
                        }
                    }
                    return `<tr style='${style}' ${title}><td>${r.day||''}</td><td>${(r.employee||'').toString().replaceAll('<','&lt;')}</td><td>${r.dept||''}</td><td>${Number(r.hours||0).toFixed(2)}</td><td>${status}</td></tr>`;
                }).join('');
                div.innerHTML = `<table><thead>${head}</thead><tbody>${body}</tbody></table>`;
                renderGantt(rows, unitComplete);
            }).catch(()=>{ document.getElementById('summary').textContent='Error'; });
        }
        document.getElementById('runBtn').addEventListener('click', ()=>{ run();
            const com = document.getElementById('com').value.trim();
            const url = new URL(window.location.href);
            if(com) url.searchParams.set('com', com); else url.searchParams.delete('com');
            window.history.replaceState({}, '', url);
        });
        document.getElementById('totBtn').addEventListener('click', ()=>{
            const com = document.getElementById('com').value.trim();
            const url = new URL(window.location.origin + '/com_totals');
            if(com) url.searchParams.set('com', com);
            window.open(url.toString(), '_blank');
        });
        
        // Auto-run if ?com= is present
        (function(){
            const params = new URLSearchParams(window.location.search);
            const q = (params.get('com')||'').trim();
            if(q){ document.getElementById('com').value = q; run(); }
        })();
        </script>
    </body></html>
    """
    return render_template_string(page)




def render_dr_milestone_dashboard(tv_mode=False):
    """Render split-screen DR dashboard with milestone tracking"""
    from flask import render_template_string
    
    page = """<!doctype html>
<html>
<head>
    <meta charset='utf-8'>
    <title>DR Dashboard</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="color-scheme" content="dark">
    <style>
        * { 
            box-sizing: border-box; 
            margin: 0; 
            padding: 0;
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
            text-rendering: optimizeLegibility;
        }
        
        @media (-webkit-min-device-pixel-ratio: 2), (min-resolution: 192dpi) {
            * {
                -webkit-font-smoothing: subpixel-antialiased;
            }
        }
        
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
            color: #e2e8f0;
            min-height: 100vh;
            padding: 1rem;
            overflow: hidden;
            image-rendering: -webkit-optimize-contrast;
            image-rendering: crisp-edges;
        }
        
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 1.5rem 2rem;
            background: rgba(15, 23, 42, 0.9);
            backdrop-filter: blur(10px);
            border-radius: 12px;
            margin-bottom: 1rem;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
            will-change: transform;
            transform: translateZ(0);
        }
        
        h1 {
            font-size: 2.8rem;
            font-weight: 700;
            background: linear-gradient(135deg, #60a5fa 0%, #a78bfa 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            letter-spacing: -0.02em;
        }
        
        .header-controls {
            display: flex;
            gap: 2rem;
            align-items: center;
            font-size: 1.2rem;
        }
        
        .fullscreen-btn {
            background: linear-gradient(135deg, #3b82f6 0%, #8b5cf6 100%);
            border: none;
            color: white;
            padding: 0.75rem 1.5rem;
            border-radius: 10px;
            font-size: 1.1rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s;
            box-shadow: 0 4px 12px rgba(59, 130, 246, 0.4);
            white-space: nowrap;
        }
        
        .fullscreen-btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(59, 130, 246, 0.6);
        }
        
        .main-container {
            display: flex;
            gap: 1rem;
            height: calc(100vh - 150px);
        }
        
        /* LEFT SIDE - Table of all DRs */
        .dr-table-container {
            flex: 0 0 70%;
            background: rgba(30, 41, 59, 0.6);
            backdrop-filter: blur(10px);
            border-radius: 12px;
            padding: 1rem;
            overflow-y: auto;
            border: 1px solid rgba(148, 163, 184, 0.2);
            display: flex;
            flex-direction: column;
        }
        
        .table-header {
            font-size: 1.6rem;
            font-weight: 600;
            margin-bottom: 0.75rem;
            color: #60a5fa;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .dr-table {
            width: 100%;
            border-collapse: separate;
            border-spacing: 0 0.4rem;
        }
        
        .dr-table thead th {
            background: rgba(15, 23, 42, 0.8);
            padding: 0.9rem 0.7rem;
            text-align: left;
            font-size: 1.15rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: #94a3b8;
            position: sticky;
            top: 0;
            z-index: 10;
        }
        
        .dr-table tbody tr {
            background: rgba(30, 41, 59, 0.4);
            transition: all 0.2s;
        }
        
        .dr-table tbody tr:hover {
            background: rgba(59, 130, 246, 0.15);
            transform: translateX(4px);
        }
        
        .dr-table tbody tr.has-milestones {
            background: rgba(34, 197, 94, 0.08);
        }
        
        .dr-table tbody td {
            padding: 1.35rem 1rem;
            font-size: 1.7rem;
            border-top: 1px solid rgba(148, 163, 184, 0.1);
        }
        
        .dr-num {
            font-weight: 700;
            font-family: 'Courier New', monospace;
            color: #60a5fa;
            font-size: 1.6rem;
        }

        .com-num {
            font-weight: 700;
            font-family: 'Courier New', monospace;
            color: #e2e8f0;
            font-size: 1.85rem;
        }
        
        .comment-text {
            font-size: 1.6rem;
            color: #cbd5e1;
            font-style: italic;
            opacity: 0.9;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            overflow: hidden;
        }
        
        .milestone-badges {
            display: flex;
            gap: 0.3rem;
            flex-wrap: wrap;
        }
        
        .milestone-badge {
            display: inline-flex;
            align-items: center;
            gap: 0.3rem;
            padding: 0.25rem 0.6rem;
            border-radius: 4px;
            font-size: 0.9rem;
            font-weight: 600;
            white-space: nowrap;
        }
        
        .milestone-badge.parts-made {
            background: rgba(59, 130, 246, 0.2);
            color: #60a5fa;
            border: 1px solid rgba(59, 130, 246, 0.4);
        }
        
        .milestone-badge.sent-to-shop {
            background: rgba(34, 197, 94, 0.2);
            color: #4ade80;
            border: 1px solid rgba(34, 197, 94, 0.4);
        }
        
        .milestone-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            display: inline-block;
        }
        
        .milestone-dot.parts { background: #60a5fa; }
        .milestone-dot.complete { background: #22c55e; }
        
        .routing-badge {
            background: rgba(100, 116, 139, 0.3);
            padding: 0.25rem 0.6rem;
            border-radius: 4px;
            font-size: 0.95rem;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 280px;
        }
        
        .time-elapsed {
            font-family: 'Courier New', monospace;
            font-size: 1rem;
            color: #94a3b8;
        }
        
        /* RIGHT: Recent Activity Cards */
        .recent-activity {
            flex: 0 0 28%;
            display: flex;
            flex-direction: column;
            gap: 0.75rem;
            overflow-y: auto;
            max-height: 100%;
        }
        
        .activity-header {
            background: rgba(30, 41, 59, 0.6);
            backdrop-filter: blur(10px);
            border-radius: 12px;
            padding: 1rem 1.25rem;
            border: 1px solid rgba(148, 163, 184, 0.2);
            flex-shrink: 0;
        }
        
        .activity-header h2 {
            font-size: 1.6rem;
            color: #a78bfa;
            font-weight: 600;
        }
        
        .activity-card {
            background: rgba(30, 41, 59, 0.6);
            backdrop-filter: blur(10px);
            border-radius: 12px;
            padding: 1.25rem;
            border: 2px solid rgba(148, 163, 184, 0.2);
            position: relative;
            transition: all 0.3s;
        }
        
        .activity-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
            border-color: rgba(59, 130, 246, 0.5);
        }
        
        .activity-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 3px;
            background: linear-gradient(90deg, #3b82f6, #60a5fa);
            border-radius: 12px 12px 0 0;
        }
        
        .activity-card.has-completion::before {
            background: linear-gradient(90deg, #22c55e, #4ade80);
        }
        
        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: start;
            margin-bottom: 0.9rem;
            gap: 1rem;
        }
        
        .card-com-num {
            font-size: 3.2rem;
            font-weight: 700;
            font-family: 'Courier New', monospace;
            color: #e2e8f0;
        }
        
        .card-milestones {
            display: flex;
            gap: 0.5rem;
        }
        
        .card-milestone-icon {
            width: 38px;
            height: 38px;
            border-radius: 6px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.4rem;
        }
        
        .card-milestone-icon.parts {
            background: rgba(59, 130, 246, 0.2);
            border: 2px solid #60a5fa;
            color: #60a5fa;
        }
        
        .card-milestone-icon.complete {
            background: rgba(34, 197, 94, 0.2);
            border: 2px solid #22c55e;
            color: #22c55e;
        }
        
        .card-info {
            display: grid;
            grid-template-columns: auto 1fr;
            gap: 0.5rem 1.1rem;
            font-size: 1.4rem;
            margin-bottom: 0.85rem;
        }
        
        .card-label {
            opacity: 0.7;
            font-weight: 500;
        }
        
        .card-value {
            font-weight: 600;
        }
        
        .card-comment {
            background: rgba(15, 23, 42, 0.6);
            border-radius: 6px;
            padding: 1rem;
            font-size: 1.6rem;
            line-height: 1.7;
            border-left: 3px solid #3b82f6;
            margin-top: 0.5rem;
            max-height: 9.5rem;
            overflow: hidden;
        }
        
        .card-comment.complete-comment {
            border-left-color: #22c55e;
            background: rgba(5, 46, 22, 0.3);
        }
        
        .card-timers {
            display: flex;
            gap: 0.9rem;
            margin-top: 0.75rem;
            padding-top: 0.75rem;
            border-top: 1px solid rgba(148, 163, 184, 0.2);
        }

        .card-timers-top {
            display: grid;
            gap: 0.35rem;
            justify-items: end;
            text-align: right;
        }
        
        .card-timer {
            flex: 1;
            text-align: center;
        }
        
        .card-timer-label {
            font-size: 0.8rem;
            opacity: 0.6;
            text-transform: uppercase;
            letter-spacing: 0.3px;
        }
        
        .card-timer-value {
            font-size: 1.35rem;
            font-weight: 700;
            font-family: 'Courier New', monospace;
            color: #60a5fa;
            margin-top: 0.2rem;
        }
        
        .pulse { animation: pulse 2s ease-in-out infinite; }
        @keyframes pulse {
            0%, 100% { opacity: 0.5; }
            50% { opacity: 1; }
        }
        
        /* Scrollbar styling */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        
        ::-webkit-scrollbar-track {
            background: rgba(15, 23, 42, 0.4);
            border-radius: 4px;
        }
        
        ::-webkit-scrollbar-thumb {
            background: rgba(100, 116, 139, 0.6);
            border-radius: 4px;
        }
        
        ::-webkit-scrollbar-thumb:hover {
            background: rgba(100, 116, 139, 0.8);
        }
        
        /* Fullscreen mode adjustments */
        :fullscreen {
            padding: 1.5rem;
        }
        
        :fullscreen .header {
            margin-bottom: 1.5rem;
        }
        
        :fullscreen .main-container {
            height: calc(100vh - 160px);
        }
        
        :fullscreen .recent-activity {
            display: grid;
            grid-template-rows: auto 1fr;
            overflow: hidden;
        }
        
        :fullscreen #recent-cards {
            display: grid;
            grid-template-rows: repeat(3, 1fr);
            gap: 0.5rem;
            overflow-y: hidden;
        }
        
        :fullscreen .activity-card {
            min-height: 0;
            overflow: hidden;
            padding: 0.75rem;
            display: flex;
            flex-direction: column;
        }
        
        :fullscreen .card-header {
            margin-bottom: 0.5rem;
            flex-shrink: 0;
        }
        
        :fullscreen .card-com-num {
            font-size: 2.8rem;
        }
        
        :fullscreen .card-info {
            gap: 0.4rem 0.9rem;
            font-size: 1.3rem;
            margin-bottom: 0.6rem;
            flex-shrink: 0;
        }
        
        :fullscreen .card-comment {
            font-size: 1.4rem;
            line-height: 1.55;
            margin-top: 0.4rem !important;
            overflow: hidden;
            text-overflow: ellipsis;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
        }
        
        :fullscreen .card-timers {
            margin-top: auto;
            flex-shrink: 0;
            gap: 0.75rem;
        }
        
        :fullscreen .card-timer {
            gap: 0.2rem;
        }

        :fullscreen .card-timer-label {
            font-size: 0.9rem;
        }

        :fullscreen .card-timer-value {
            font-size: 1.5rem;
        }
        
        """ + ("""
        /* TV Mode Enhancements */
        body.tv-mode {
            padding: 1.5rem;
        }
        body.tv-mode .header {
            padding: 1.5rem 2rem;
        }
        body.tv-mode h1 {
            font-size: 3rem;
        }
        body.tv-mode .header-controls {
            font-size: 1.2rem;
        }
        body.tv-mode .table-header {
            font-size: 2rem;
        }
        body.tv-mode .dr-table thead th {
            font-size: 1.25rem;
            padding: 1.2rem 1rem;
        }
        body.tv-mode .dr-table tbody td {
            font-size: 1.55rem;
            padding: 1.25rem 1rem;
        }
        body.tv-mode .dr-num {
            font-size: 1.55rem;
        }
        body.tv-mode .com-num {
            font-size: 1.8rem;
        }
        body.tv-mode .milestone-badge {
            font-size: 0.9rem;
            padding: 0.3rem 0.6rem;
        }
        body.tv-mode .milestone-dot {
            width: 10px;
            height: 10px;
        }
        body.tv-mode .routing-badge {
            font-size: 1rem;
            max-width: 300px;
        }
        body.tv-mode .time-elapsed {
            font-size: 1rem;
        }
        body.tv-mode .activity-header h2 {
            font-size: 1.8rem;
        }
        body.tv-mode .card-com-num {
            font-size: 3.6rem;
        }
        body.tv-mode .card-milestone-icon {
            width: 36px;
            height: 36px;
            font-size: 1.4rem;
        }
        body.tv-mode .card-info {
            font-size: 1.3rem;
            gap: 0.7rem 1.2rem;
        }
        body.tv-mode .card-comment {
            font-size: 1.7rem;
            padding: 1.1rem;
        }
        body.tv-mode .card-timer-label {
            font-size: 0.95rem;
        }
        body.tv-mode .card-timer-value {
            font-size: 1.55rem;
        }
        """ if tv_mode else "") + """
    </style>
</head>
<body""" + (' class="tv-mode"' if tv_mode else '') + """>
    <div class="header">
        <h1>🎯 DR Dashboard</h1>
        <div class="header-controls">
            <div style="display: flex; gap: 1.5rem; align-items: center;">
                <div>
                    <div class="pulse">●</div>
                    <span>Auto-refresh: 30s</span>
                </div>
                <div id="last-refresh" style="opacity: 0.8;">Page: --</div>
                <div id="last-mom-pull" style="opacity: 0.8;">DR MOM: --</div>
                <div id="last-parts-pull" style="opacity: 0.8;">Parts: --</div>
                <div id="dr-count">Loading...</div>
            </div>
            <button class="fullscreen-btn" onclick="toggleFullscreen()">⛶ Fullscreen</button>
        </div>
    </div>
    
    <div class="main-container">
        <!-- LEFT: Recent Activity Cards -->
        <div class="recent-activity">
            <div class="activity-header">
                <h2>📌 Recent Activity</h2>
            </div>
            <div id="recent-cards"></div>
        </div>
        
        <!-- RIGHT: Table of all DRs -->
        <div class="dr-table-container">
            <div class="table-header">
                <span>All Active DRs</span>
                <span id="table-count" style="font-size: 0.9rem; opacity: 0.7;"></span>
            </div>
            <table class="dr-table">
                <thead>
                    <tr>
                        <th>COM#</th>
                        <th>Routing</th>
                        <th>Comment</th>
                        <th>DR#</th>
                        <th>Age</th>
                        <th>Latest Activity</th>
                    </tr>
                </thead>
                <tbody id="dr-table-body">
                </tbody>
            </table>
        </div>
    </div>
    
    <script>
        let allDRs = [];
        let lastPageRefresh = new Date();
        
        function formatDuration(ms) {
            const seconds = Math.floor(ms / 1000);
            const minutes = Math.floor(seconds / 60);
            const hours = Math.floor(minutes / 60);
            const days = Math.floor(hours / 24);
            
            if (days > 0) return `${days}d ${hours % 24}h`;
            if (hours > 0) return `${hours}h ${minutes % 60}m`;
            if (minutes > 0) return `${minutes}m`;
            return `${seconds}s`;
        }
        
        function formatTimestamp(date) {
            if (!date) return '--';
            const now = new Date();
            const diff = now - date;
            const minutes = Math.floor(diff / 60000);
            
            if (minutes < 1) return 'Just now';
            if (minutes < 60) return `${minutes}m ago`;
            
            const hours = new Date(date).getHours();
            const mins = new Date(date).getMinutes();
            return `${hours}:${mins.toString().padStart(2, '0')}`;
        }
        
        function toggleFullscreen() {
            if (!document.fullscreenElement) {
                document.documentElement.requestFullscreen();
            } else {
                document.exitFullscreen();
            }
        }
        
        function isCompletedDR(comment) {
            if (!comment) return false;
            const completionPatterns = [
                /complete\s+and\s+sent\s+to\s+sheet\s+(metal|shop)/i,
                /complete\s+sent\s+to\s+sheet\s+(metal|shop)/i,
                /completed\s+and\s+sent\s+to\s+sheet\s+(metal|shop)/i,
                /completed\s+sent\s+to\s+sheet\s+(metal|shop)/i,
                /complete,?\s+sent\s+to\s+sheet\s+(metal|shop)/i,
                /completed,?\s+sent\s+to\s+sheet\s+(metal|shop)/i,
                // Unit destination
                /complete\s+(and\s+)?sent\s+to\s+unit/i,
                /completed\s+(and\s+)?sent\s+to\s+unit/i,
                /complete\s+(and\s+)?taken\s+to\s+unit/i,
                /completed\s+(and\s+)?taken\s+to\s+unit/i,
                // Common misspellings
                /compelte\s+(and\s+)?sent\s+to\s+sheet\s+(metal|shop)/i,
                /complete\s+(and\s+)?send\s+to\s+sheet\s+(metal|shop)/i,
                /complete\s+(and\s+)?sent\s+to\s+sheetshop/i,
                /complete\s+(and\s+)?sent\s+to\s+sheetmetal/i
            ];
            return completionPatterns.some(pattern => pattern.test(comment));
        }
        
        function renderTable() {
            const tbody = document.getElementById('dr-table-body');
            const now = Date.now();
            
            // Get top 3 most recently touched for the right side
            const recentDRs = [...allDRs]
                .sort((a, b) => (b.latest_comment_ms || 0) - (a.latest_comment_ms || 0))
                .slice(0, 3);
            
            const recentDRNumbers = new Set(recentDRs.map(dr => dr.deviation_number));
            
            // Get next 15 DRs after the top 3 for the table
            const tableDRs = allDRs
                .filter(dr => !recentDRNumbers.has(dr.deviation_number))
                .slice(0, 10);
            
            tbody.innerHTML = tableDRs.map(dr => {
                const hasParts = dr.parts_info && dr.parts_info.unique_parts > 0;
                const isComplete = isCompletedDR(dr.latest_comment);
                const hasMilestones = hasParts || isComplete;
                
                const age = dr.created_ms ? formatDuration(now - dr.created_ms) : '--';
                const lastTouch = dr.latest_comment_ms ? formatDuration(now - dr.latest_comment_ms) + ' ago' : '--';
                
                // DEBUG for DR 49323 and 49318
                if (dr.deviation_number === 49323 || dr.deviation_number === 49318) {
                    console.log(`TABLE RENDER DR ${dr.deviation_number}:`, {
                        now: now,
                        touched_ms: dr.touched_ms,
                        diff: now - dr.touched_ms,
                        lastTouch: lastTouch
                    });
                }
                
                let milestones = '';
                if (isComplete) {
                    milestones += `<span class="milestone-badge sent-to-shop">
                        <span class="milestone-dot complete"></span>
                        Sent to sheet shop
                    </span>`;
                }
                if (hasParts) {
                    milestones += `<span class="milestone-badge parts-made">
                        <span class="milestone-dot parts"></span>
                        ${dr.parts_info.unique_parts} part${dr.parts_info.unique_parts > 1 ? 's' : ''} made
                    </span>`;
                }
                
                // Truncate comment if too long
                const comment = dr.creator_comment || '';
                const truncatedComment = comment.length > 90 ? comment.substring(0, 90) + '...' : comment;
                
                // Combine milestones and comment in one cell
                let commentCell = '';
                if (milestones) {
                    commentCell = `<div class="milestone-badges" style="margin-bottom: 0.3rem;">${milestones}</div>`;
                }
                if (truncatedComment) {
                    commentCell += `<span class="comment-text" title="${comment}">${truncatedComment}</span>`;
                }
                if (!commentCell) {
                    commentCell = '<span style="opacity:0.3">—</span>';
                }
                
                return `
                    <tr class="${hasMilestones ? 'has-milestones' : ''}">
                        <td><span class="com-num">${dr.com ? `${dr.com}` : '—'}</span></td>
                        <td><span class="routing-badge" title="${dr.current_routing || 'Unknown'}">${dr.current_routing || 'Unknown'}</span></td>
                        <td>${commentCell}</td>
                        <td><span class="dr-num">${dr.deviation_number}</span></td>
                        <td><span class="time-elapsed">${age}</span></td>
                        <td><span class="time-elapsed">${lastTouch}</span></td>
                    </tr>
                `;
            }).join('');
            
            document.getElementById('table-count').textContent = `${tableDRs.length} shown`;
        }
        
        function renderRecentCards() {
            const container = document.getElementById('recent-cards');
            const now = Date.now();
            
            // Get the 3 most recently touched DRs
            const recent = [...allDRs]
                .sort((a, b) => (b.latest_comment_ms || 0) - (a.latest_comment_ms || 0))
                .slice(0, 3);
            
            container.innerHTML = recent.map(dr => {
                const hasParts = dr.parts_info && dr.parts_info.unique_parts > 0;
                const isComplete = isCompletedDR(dr.latest_comment);
                
                let milestoneIcons = '';
                if (isComplete) {
                    milestoneIcons += `<div class="card-milestone-icon complete" title="Sent to sheet shop">✓</div>`;
                }
                if (hasParts) {
                    milestoneIcons += `<div class="card-milestone-icon parts" title="${dr.parts_info.unique_parts} part${dr.parts_info.unique_parts > 1 ? 's' : ''} made on ${dr.parts_info.racks}">📦</div>`;
                }
                
                const age = dr.created_ms ? formatDuration(now - dr.created_ms) : '--';
                const inRoute = dr.latest_comment_ms ? formatDuration(now - dr.latest_comment_ms) : '--';
                
                // DEBUG for DR 49323 and 49318
                if (dr.deviation_number === 49323 || dr.deviation_number === 49318) {
                    console.log(`CARD RENDER DR ${dr.deviation_number}:`, {
                        now: now,
                        touched_ms: dr.touched_ms,
                        diff: now - dr.touched_ms,
                        inRoute: inRoute
                    });
                }
                
                return `
                    <div class="activity-card ${isComplete ? 'has-completion' : ''}">
                        <div class="card-header">
                            <div>
                                <div class="card-com-num">${dr.com ? `${dr.com}` : '—'}</div>
                            </div>
                            <div style="display:flex;gap:0.75rem;align-items:flex-start;">
                                <div class="card-milestones">${milestoneIcons}</div>
                                <div class="card-timers-top">
                                    <div class="card-timer">
                                        <div class="card-timer-label">Age</div>
                                        <div class="card-timer-value">${age}</div>
                                    </div>
                                    <div class="card-timer">
                                        <div class="card-timer-label">In Route</div>
                                        <div class="card-timer-value">${inRoute}</div>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div class="card-info">
                            <span class="card-label">DR#:</span><span class="card-value">${dr.deviation_number}</span>
                            ${dr.urgency ? `<span class="card-label">Urgency:</span><span class="card-value">${dr.urgency}</span>` : ''}
                            <span class="card-label">Routing:</span><span class="card-value">${dr.current_routing || 'Unknown'}</span>
                            ${hasParts ? `<span class="card-label">Parts Made:</span><span class="card-value">${dr.parts_info.unique_parts} on ${dr.parts_info.racks}</span>` : ''}
                        </div>
                        ${dr.creator_comment ? `
                            <div class="card-comment">
                                ${dr.creator_user ? `<strong>${dr.creator_user}:</strong> ` : ''}
                                ${dr.creator_comment}
                            </div>
                        ` : ''}
                        ${(dr.latest_comment && dr.latest_comment !== dr.creator_comment) ? `
                            <div class="card-comment ${isComplete ? 'complete-comment' : ''}" style="margin-top: 0.4rem;">
                                ${dr.latest_user ? `<strong>${dr.latest_user}:</strong> ` : ''}
                                ${dr.latest_comment}
                            </div>
                        ` : ''}
                    </div>
                `;
            }).join('');
        }
        
        async function loadData() {
            try {
                const resp = await fetch('/api/dr-live?days=7');
                const data = await resp.json();
                allDRs = data.drs || [];
                
                // DEBUG: Log first DR timestamps
                if (allDRs.length > 0) {
                    console.log('=== API Data Check ===');
                    console.log('First 5 DRs:');
                    allDRs.slice(0, 5).forEach(dr => {
                        console.log(`DR ${dr.deviation_number}:`);
                        console.log(`  created_ms: ${dr.created_ms}`);
                        console.log(`  latest_comment_ms: ${dr.latest_comment_ms}`);
                        console.log(`  Same? ${dr.created_ms === dr.latest_comment_ms ? 'YES - no routing updates since creation' : 'No - different'}`);
                    });
                    console.log('Date.now():', Date.now());
                }
                
                // Sort by latest_comment_ms DESC for table display
                allDRs.sort((a, b) => (b.latest_comment_ms || 0) - (a.latest_comment_ms || 0));
                
                document.getElementById('dr-count').textContent = `${allDRs.length} DRs`;
                
                // Update last MOM pull time
                if (data.last_poll_ms) {
                    const momDate = new Date(data.last_poll_ms);
                    document.getElementById('last-mom-pull').textContent = `DR MOM: ${formatTimestamp(momDate)}`;
                } else {
                    document.getElementById('last-mom-pull').textContent = 'DR MOM: Never';
                }
                
                // Update page refresh time
                lastPageRefresh = new Date();
                document.getElementById('last-refresh').textContent = `Page: ${formatTimestamp(lastPageRefresh)}`;
                
                // Update PartsTracker pull time
                if (data.last_parts_pull_ms) {
                    const partsDate = new Date(data.last_parts_pull_ms);
                    document.getElementById('last-parts-pull').textContent = `Parts: ${formatTimestamp(partsDate)}`;
                } else {
                    document.getElementById('last-parts-pull').textContent = 'Parts: Unknown';
                }
                
                renderTable();
                renderRecentCards();
            } catch (e) {
                console.error('Failed to load DR data:', e);
            }
        }
        
        // Initial load
        loadData();
        
        // Reload data every 30 seconds
        setInterval(loadData, 30000);
        
        // Update timers every 30 seconds (aligned with data refresh)
        setInterval(() => {
            renderTable();
            renderRecentCards();
        }, 30000);
    </script>
</body>
</html>
    """
    return render_template_string(page)


@dashboard.route('/dr-dashboard')
@dashboard.route('/dr')
@dashboard.route('/tv')
def dr_dashboard():
    """Live DR Dashboard with milestone tracking"""
    from flask import request
    
    # Check for TV display mode
    tv_mode = request.args.get('tv', '0') == '1'
    
    # Always use milestone dashboard
    return render_dr_milestone_dashboard(tv_mode)
