"""
Dashboard Blueprint - HTML page routes

All user-facing HTML pages are defined here.
"""
from flask import Blueprint, redirect, url_for, render_template_string, request
import html
from .utils import get_conn

# Create blueprint
dashboard = Blueprint('dashboard', __name__)

# Template constants
RECENT_PAGE = """<!doctype html><html><head><meta charset='utf-8'><title>Recent Unit Completion</title><style>
body{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}
header{padding:1rem 1.5rem;display:flex;justify-content:space-between;align-items:center;background:#161b22;border-bottom:1px solid #30363d}
h1{margin:0;font-size:1.05rem}
nav a{color:#8fb9ff;text-decoration:none;margin-right:.8rem;font-size:.72rem}
button{background:#238636;border:1px solid #2ea043;color:#fff;padding:.55rem .9rem;border-radius:6px;font-size:.7rem;font-weight:600;cursor:pointer}button:hover{background:#2ea043}
main{padding:1rem 1.1rem}
.pill{display:inline-block;background:#1f6feb33;border:1px solid #1f6feb55;border-radius:20px;padding:.45rem .75rem;font-size:.6rem;letter-spacing:.5px;margin:.25rem .4rem .6rem 0}
.unit{display:grid;border:1px solid #3f4751;border-radius:14px;margin:1.25rem 0;overflow:hidden;background:#141a21;font-size:.6rem;grid-template-columns:260px 1fr;transition:background .25s,border-color .25s,box-shadow .25s,transform .2s;position:relative;box-shadow:0 2px 5px -2px #000,0 0 0 1px #212a33;cursor:pointer}
.unit:hover{transform:translateY(-2px);box-shadow:0 4px 12px -2px rgba(0,0,0,.4),0 0 0 1px #3a4a5f,0 0 8px -2px rgba(31,111,235,.3);border-color:#4a5a6f}
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
</style></head><body><header><h1>Recent Unit Completion</h1><div><nav><a href='/dash'>&larr; Dashboard</a></nav><button onclick='loadData()'>Refresh</button></div></header><main>
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
    const inProg = (typeof data.in_progress_count==='number') ? data.in_progress_count : data.units.filter(u=>u.overall_completion<99.999).length;
    const denom = data.count||data.units.length||1;
    const avgEff=denom?(tEff/denom).toFixed(1):'0.0';
    const avgComp=denom?(tComp/denom).toFixed(1):'0.0';
    sDiv.innerHTML=`<span class='pill'>${inProg} Units in Progress</span><span class='pill'>Avg Eff ${avgEff}%</span><span class='pill'>Avg Comp ${avgComp}%</span>`;
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
            header{padding:1rem 1.5rem;display:flex;justify-content:space-between;align-items:center;background:#161b22;border-bottom:1px solid #30363d}
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
            .card{transition:transform .12s ease, box-shadow .12s ease}
            .card:hover{transform:translateY(-2px); box-shadow:0 8px 22px -10px #000}
            /* Legend styling for Daily Hours chart */
            .legendWrap{display:flex;flex-wrap:wrap;gap:.5rem;margin:.4rem 0 .6rem}
            .legend-item{display:inline-flex;align-items:center;gap:.45rem;background:#0f1a2a;border:1px solid #2a3b55;border-radius:16px;padding:.28rem .6rem;font-size:.85rem;cursor:pointer;user-select:none}
            .legend-item.off{opacity:.5;border-color:#3a3f47}
            .legend-dot{width:12px;height:12px;border-radius:50%;box-shadow:0 0 0 1px #0007 inset}
            .legend-item:hover{filter:brightness(1.08)}
        </style></head>
        <body>
            <header><h1>Production Dashboard</h1></header>
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
                </div>
                <div id='unitMetrics' class='metrics'>
                    <span class='pill'>Loading unit metrics…</span>
                </div>
            </main>
            <script>
            function pct(n){return (Math.round(n*10)/10).toFixed(1)}
            function arrow(delta){
                if(Math.abs(delta) < 0.05) return '<span class="small">(flat vs last 90 days)</span>';
                // Increase in days is bad -> red; decrease good -> green
                return delta>0 ? '<span class="up">(+'+pct(delta)+' vs last 90 days)</span>' : '<span class="down">(-'+pct(-delta)+' vs last 90 days)</span>';
            }
            fetch('/api/metrics/unit_time_trends').then(r=>r.json()).then(m=>{
                const c=document.getElementById('unitMetrics');
                if(m.error){ c.innerHTML = '<span class="pill">'+m.error+'</span>'; return; }
                const a10=m.last10||{}; const a90=m.last90d||{}; const tr=m.trend||{};
                const dEff = (a10.avg_efficiency||0) - (a90.avg_efficiency||0);
                const dActive = (a10.avg_active_days||0) - (a90.avg_active_days||0);
                const dSpan = (a10.avg_span_days||0) - (a90.avg_span_days||0);
                const effArrow = dEff>0 ? '<span class="down">(+'+pct(dEff)+' vs 90d)</span>' : (dEff<0 ? '<span class="up">(-'+pct(-dEff)+' vs 90d)</span>' : '<span class="small">(flat vs 90d)</span>');
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
                // axes
                ctx.strokeStyle='#2a323c';ctx.lineWidth=1.4;ctx.beginPath();
                ctx.moveTo(padL, padT);ctx.lineTo(padL, H-padB);ctx.lineTo(W-padR, H-padB);ctx.stroke();
                // y ticks and grid
                ctx.fillStyle='#b9c9da'; ctx.font='13.5px system-ui, sans-serif'; ctx.textAlign='right'; ctx.textBaseline='middle';
                for(let yv=0; yv<=yMaxNice+1e-6; yv+=yStep){
                    const y = H-padB - yv*sy;
                    // grid
                    ctx.strokeStyle='#1c2430'; ctx.lineWidth=1; ctx.beginPath();
                    ctx.moveTo(padL, y); ctx.lineTo(W-padR, y); ctx.stroke();
                    // tick
                    ctx.strokeStyle='#2a323c'; ctx.beginPath(); ctx.moveTo(padL-4, y); ctx.lineTo(padL, y); ctx.stroke();
                    ctx.fillText(String(Math.round(yv)), padL-10, y);
                }
                // lines
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
            </script>
        </body></html>
        """
        return render_template_string(page)


@dashboard.route('/recent')
def recent_units():
        return render_template_string(RECENT_PAGE)


@dashboard.route('/hours-chart')
def hours_chart():
    """Dedicated page for the Total Daily Hours Charged Chart."""
    page = """<!DOCTYPE html>
    <html>
    <head>
        <title>Total Daily Hours Charged Chart</title>
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background: #0d1117; color: #c9d1d9; }
            .container { max-width: 1400px; margin: 0 auto; }
            header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
            h1 { margin: 0; font-size: 1.5rem; }
            .back-link { padding: 8px 16px; background: #21262d; border-radius: 6px; text-decoration: none; color: #58a6ff; }
            .back-link:hover { background: #30363d; }
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
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>Total Daily Hours Charged Chart</h1>
                <a href="/dash" class="back-link">&larr; Back to Dashboard</a>
            </header>
            
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
        
        function movingAverage(data, window) {
            if (window === 1) return data;
            const result = [];
            for (let i = 0; i < data.length; i++) {
                let sum = 0, count = 0;
                for (let j = Math.max(0, i - window + 1); j <= i; j++) {
                    if (data[j] != null) {
                        sum += data[j];
                        count++;
                    }
                }
                result.push(count > 0 ? sum / count : null);
            }
            return result;
        }
        
        let chartData = null;
        let currentPeriod = 90;
        let currentView = 'ma7';
        let seriesState = [];
        
        function updateChart() {
            if (!chartData) return;
            let allDates = chartData.dates.slice(0, -1);
            let allDeptData = {};
            Object.keys(chartData.per_dept).forEach(dept => {
                allDeptData[dept] = chartData.per_dept[dept].slice(0, -1);
            });
            const maxWindow = 30;
            const extraDays = maxWindow - 1;
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
        
        fetch('/api/metrics/daily_hours?days=365').then(r => r.json()).then(m => {
            chartData = {
                dates: m.dates,
                per_dept: {}
            };
            Object.keys(m.per_dept).forEach(dept => {
                chartData.per_dept[dept] = m.per_dept[dept].hours;
            });
            updateChart();
        }).catch(err => {
            console.error('Chart error:', err);
            document.getElementById('deptChart').parentElement.innerHTML = '<p style="color:#f85149;">Error loading chart data</p>';
        });
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
            page = f"""
            <!doctype html><html><head><meta charset='utf-8'><title>DR Labor Lookup</title>
            <style>
                body{{margin:0;font-family:system-ui,-apple-system,Roboto,Arial,sans-serif;background:#0d1117;color:#e6edf3}}
                header{{padding:1rem 1.5rem;display:flex;justify-content:space-between;align-items:center;background:#161b22;border-bottom:1px solid #30363d}}
                h1{{margin:0;font-size:1.05rem}}
                main{{padding:1rem 1.2rem}}
                form input,form button{{border-radius:6px;border:1px solid #30363d;background:#11161d;color:#e6edf3;padding:.5rem .65rem}}
                form button{{background:#1f6feb;border-color:#3a78e0;font-weight:700;cursor:pointer}}
                table{{border-collapse:collapse;width:100%;margin-top:1rem;font-size:.8rem}}
                th,td{{border:1px solid #2a323c;padding:.35rem .5rem;text-align:left}}
                th{{background:#1a2330}}
            </style></head>
            <body>
                <header><h1>DR Labor Lookup</h1><div><a href='/dash' style='color:#8fb9ff;text-decoration:none'>&larr; Dashboard</a></div></header>
                <main>
                    <form method='GET'>
                        <label for='dr'>DR#:</label>
                        <input id='dr' name='dr' type='text' value='{html.escape(query_dr)}' placeholder='e.g. 12345' required />
                        <button type='submit'>Lookup</button>
                    </form>
                    {('<p style=\"opacity:.8;margin-top:.5rem;\">Searching in column: <strong>'+html.escape(dr_col)+'</strong></p>') if (query_dr and dr_col) else (('<p style=\"opacity:.7;margin-top:.5rem;\">' + ('Scanned columns: '+html.escape(', '.join(searched_cols_info)) if query_dr and searched_cols_info else '') + '</p>') if query_dr else '')}
                    {('<p style=\"margin-top:1rem;opacity:.7;\">No matches found.</p>') if (query_dr and not rows) else ''}
                    {('''
                        <table>
                            <thead><tr><th>Employee</th><th>Dept</th><th>Hours</th></tr></thead>
                            <tbody>
                                ''' + '\n'.join(f"<tr><td>{html.escape(r.get('EmployeeName',''))}</td><td>{html.escape((r.get('DeptCode') or 'UNKNOWN'))}</td><td>{float(r.get('Hours') or 0):.2f}</td></tr>" for r in rows) + '''
                            </tbody>
                        </table>
                        <h3 style='margin-top:1rem;'>Totals by Dept</h3>
                        <table style='width:auto'>
                            <thead><tr><th>Department</th><th>Total Hours</th></tr></thead>
                            <tbody>
                                ''' + '\n'.join(f"<tr><td>{html.escape(k)}</td><td>{v:.2f}</td></tr>" for k,v in totals.items()) + '''
                            </tbody>
                        </table>
                    ''') if rows else ''}
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
        header{padding:1rem 1.5rem;display:flex;justify-content:space-between;align-items:center;background:#161b22;border-bottom:1px solid #30363d}
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
    table{border-collapse:collapse;width:100%;margin-top:1rem;font-size:.85rem}
    th,td{border:1px solid #2a323c;padding:.45rem .6rem;text-align:left}
        th{background:#1a2330}
    .pill{display:inline-block;background:#1f6feb33;border:1px solid #1f6feb55;border-radius:20px;padding:.35rem .6rem;font-size:.65rem;margin:.3rem .4rem 0 0}
    .miniBar{height:8px;background:#263040;border-radius:4px;position:relative;overflow:hidden}
    .miniBar > span{position:absolute;left:0;top:0;bottom:0;background:linear-gradient(90deg,#2f9e44,#52d96d)}
    </style></head>
    <body>
        <header><h1>Employee Lookup</h1><div><a href='/dash' style='color:#8fb9ff;text-decoration:none'>&larr; Dashboard</a></div></header>
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
        header{padding:1rem 1.5rem;display:flex;justify-content:space-between;align-items:center;background:#161b22;border-bottom:1px solid #30363d}
        h1{margin:0;font-size:1.05rem}
        main{padding:1rem 1.2rem}
        .row{display:flex;gap:.6rem;flex-wrap:wrap;align-items:end}
        label{font-size:.72rem;opacity:.9}
        input,button{border-radius:6px;border:1px solid #30363d;background:#11161d;color:#e6edf3;padding:.5rem .65rem}
        button{background:#1f6feb;border-color:#3a78e0;font-weight:700;cursor:pointer}
        table{border-collapse:collapse;width:100%;margin-top:1rem;font-size:.8rem}
        th,td{border:1px solid #2a323c;padding:.35rem .5rem;text-align:left;vertical-align:top;max-width:420px;overflow:hidden;text-overflow:ellipsis}
        th{background:#1a2330}
        .pill{display:inline-block;background:#1f6feb33;border:1px solid #1f6feb55;border-radius:20px;padding:.35rem .6rem;font-size:.65rem;margin:.3rem .4rem 0 0}
    </style></head>
    <body>
        <header><h1>Parts Tracker</h1><div><a href='/dash' style='color:#8fb9ff;text-decoration:none'>&larr; Dashboard</a></div></header>
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
        header{padding:1rem 1.5rem;display:flex;justify-content:space-between;align-items:center;background:#161b22;border-bottom:1px solid #30363d}
        h1{margin:0;font-size:1.05rem}
        main{padding:1rem 1.2rem}
        table{border-collapse:collapse;width:100%;margin-top:1rem;font-size:.9rem}
        th,td{border:1px solid #2a323c;padding:.5rem .7rem;text-align:left}
        th{background:#1a2330}
        .pill{display:inline-block;background:#1f6feb33;border:1px solid #1f6feb55;border-radius:20px;padding:.35rem .6rem;font-size:.65rem;margin:.3rem .4rem 0 0}
    </style></head>
    <body>
    <header><h1>COM# Totals by Employee</h1><div><a id='backLink' href='/com' style='color:#8fb9ff;text-decoration:none'>&larr; Back to Charges</a></div></header>
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
        header{padding:1rem 1.5rem;display:flex;justify-content:space-between;align-items:center;background:#161b22;border-bottom:1px solid #30363d}
        h1{margin:0;font-size:1.05rem}
        main{padding:1rem 1.2rem}
        input,button{border-radius:6px;border:1px solid #30363d;background:#11161d;color:#e6edf3;padding:.5rem .65rem}
        button{background:#238636;border-color:#2ea043;font-weight:700;cursor:pointer}
        table{border-collapse:collapse;width:100%;margin-top:1rem;font-size:.85rem}
        th,td{border:1px solid #2a323c;padding:.45rem .6rem;text-align:left}
        th{background:#1a2330}
        .pill{display:inline-block;background:#1f6feb33;border:1px solid #1f6feb55;border-radius:20px;padding:.35rem .6rem;font-size:.65rem;margin:.3rem .4rem 0 0}
    </style></head>
    <body>
        <header><h1>COM# Charges</h1><div><a href='/dash' style='color:#8fb9ff;text-decoration:none'>&larr; Dashboard</a></div></header>
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
            .ggrid{display:flex;gap:3px;padding:.3rem .5rem}
            .gcell{width:16px;height:16px;border:1px solid #263040;background:#141b22;border-radius:2px;position:relative}
            .gcell.on{border-color:#3fb950}
            .gcell.col{box-shadow:0 0 0 1px #3a78e0 inset}
            .grow.hl .glabel{background:#172233}
            .ghead{display:flex;align-items:center}
            .ghead .glabel{background:#0f161d;font-weight:700}
            .gtick{width:16px;height:16px;display:flex;align-items:center;justify-content:center;color:#7b8a99;font-size:.6rem}
            .gtick.hl{color:#bcd0ff;font-weight:700}
            .gtt{position:fixed;z-index:1000;pointer-events:none;background:#111820;border:1px solid #2a3440;color:#e6edf3;padding:.35rem .5rem;border-radius:6px;font-size:.7rem;box-shadow:0 6px 18px rgba(0,0,0,.45);display:none}
            .glegend{display:flex;align-items:center;gap:.5rem;padding:.4rem .5rem;border-bottom:1px solid #1a2230;background:#0f161d}
            .glegend .lab{font-size:.65rem;color:#9bb0c8}
            .glegend .bar{width:180px;height:10px;border-radius:5px;background:linear-gradient(90deg,hsl(140,65%,22%),hsl(140,65%,52%));border:1px solid #2a3544}
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
            // Header row (ticks every 5 days)
            let html = '<div class="grow ghead"><div class="glabel">Date</div><div class="ggrid">';
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
                html += `<div class='grow'><div class='glabel'>${ent.label}</div><div class='ggrid'>`;
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
            let legend = `<div class='glegend'><div class='lab'>Hours</div><div class='bar'></div><div class='lab'>${maxHours.toFixed(1)}h</div></div>`;
            if(unitComplete){
                legend += `<div class='glegend' style='margin-left:2rem;'><div style='width:20px;height:20px;background:#f0e68c;border-radius:3px;'></div><div class='lab'>Excluded Employee</div><div style='width:20px;height:20px;background:#ff6b6b;border-radius:3px;margin-left:1rem;'></div><div class='lab'>Filtered Out</div></div>`;
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

