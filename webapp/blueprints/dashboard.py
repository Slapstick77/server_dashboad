"""
Dashboard Blueprint - HTML page routes

All user-facing HTML pages are defined here.
"""
from flask import Blueprint, redirect, url_for, render_template_string

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
