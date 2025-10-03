"""
Dashboard Blueprint - HTML page routes

All user-facing HTML pages are defined here.
"""
from flask import Blueprint, redirect, url_for, render_template_string

# Create blueprint
dashboard = Blueprint('dashboard', __name__)

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
