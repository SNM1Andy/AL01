"""Offline, read-only snapshot viewer. No service, commands, or hidden-world values."""
import json, copy
from pathlib import Path

def observation_data(individual):
    state=individual.state
    visible=individual.world.visible(state['position'])
    objects={k:{'position':v['position'],'observed':True,'value':v['value']} for k,v in state['observed'].items()}
    for key,value in visible.items(): objects.setdefault(key,dict(position=value['position'],observed=False))
    events=[]
    for e in state['events'][-200:]:
        p=e['payload']
        events.append({'index':e['index'],'tick':e['tick'],'kind':e['kind'],'hash':e['hash'],
                       'payload':{k:v for k,v in p.items() if k!='candidates'},'candidate_count':len(p.get('candidates',[]))})
    return copy.deepcopy({'id':state['id'],'tick':state['tick'],'version':state['version'],'position':state['position'],
            'width':individual.world.width,'height':individual.world.height,'local':individual.world.local,
            'observed_count':len(state['observed']),'closed_loops':state['closed_loops'],'lifecycle':state['lifecycle'],
            'goal_state':state['goal_state'],'waiting':state['waiting'],'queue_counts':{k:len(state[k]) for k in ('inbox','outbox','tasks','requested_actions')},
            'objects':objects,'events':events})

def export_html(individual,path):
    data=json.dumps(observation_data(individual),ensure_ascii=False).replace('&','\\u0026').replace('<','\\u003c').replace('>','\\u003e')
    Path(path).write_text(TEMPLATE.replace('__DATA__',data),encoding='utf8')

TEMPLATE='''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; img-src data:; connect-src 'none'">
<title>AL-01 · 只读观察台</title><style>
*{box-sizing:border-box}body{margin:0;background:#10171b;color:#e8f0ee;font:16px system-ui,sans-serif}main{max-width:1100px;margin:0 auto;padding:36px 24px}h1{font-size:30px;margin:0 0 8px}h2{font-size:18px;margin:0 0 18px}p{color:#a6b8b3;line-height:1.6}.tag{color:#93dac0;font-size:13px;letter-spacing:.08em}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:26px 0}.card,.panel{background:#19242a;border:1px solid #304149;border-radius:14px;padding:20px}.card b{display:block;font-size:30px;margin-top:10px}.card span{color:#a6b8b3;font-size:13px}.grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}canvas{width:100%;height:320px;background:#111c20;border-radius:8px}pre{white-space:pre-wrap;overflow-wrap:anywhere;color:#cce0d7;font-size:12px;max-height:360px;overflow:auto}input{width:100%;accent-color:#82d8b4}button{background:#234639;color:#d8f8e7;border:1px solid #539779;border-radius:7px;padding:8px 14px;cursor:pointer}.controls{display:flex;gap:8px;align-items:center}.note{font-size:13px}.legend{font-size:13px;line-height:1.8;color:#aec2bb}footer{font-size:12px;color:#8ca29a;margin-top:24px}@media(max-width:700px){main{padding:24px 14px}.cards{grid-template-columns:1fr 1fr}.grid{grid-template-columns:1fr}h1{font-size:25px}}
</style><main><div class="tag">AL-01 / FOUNDATION 0.14</div><h1>只读观察台</h1><p>离线实验快照。看到的是记录，不是模拟独白；页面不能执行动作或改写个体。</p>
<div class="cards"><div class="card"><span>有效认知 tick</span><b id="tick"></b></div><div class="card"><span>已观察对象</span><b id="observed"></b></div><div class="card"><span>观测反馈闭环</span><b id="loops"></b></div><div class="card"><span>待处理队列</span><b id="queues"></b></div></div>
<div class="grid"><section class="panel"><h2>当前已知世界</h2><canvas id="world" width="900" height="600" aria-label="当前位置与已知对象示意图"></canvas><p class="legend">蓝色：当前位置 · 绿色：已观察 · 灰色：可见但未知<br>隐藏对象及未观察的反馈值不会导出到页面。</p><p id="state" class="note"></p></section>
<section class="panel"><h2>可追溯事件</h2><label for="event">最近最多 200 条事件，只读浏览</label><input id="event" type="range" min="0" value="0"><div class="controls"><button id="previous">上一条</button><button id="next">下一条</button><span id="event-label"></span></div><pre id="detail"></pre></section></div>
<footer>这不是实时监控，也不证明主观意识。世界图显示最终快照，事件滑块只切换记录详情。实验结束后重新导出以获得新快照。</footer></main>
<script type="application/json" id="data">__DATA__</script><script>
const d=JSON.parse(document.getElementById('data').textContent); const byId=id=>document.getElementById(id);
byId('tick').textContent=d.tick;byId('observed').textContent=d.observed_count;byId('loops').textContent=d.closed_loops;byId('queues').textContent=Object.values(d.queue_counts).reduce((a,b)=>a+b,0);
byId('state').textContent=`${d.id} · ${d.local?'局部感知':'全局基线'} · ${d.lifecycle} · 目标 ${d.goal_state.status} · 位置 (${d.position.join(', ')})${d.waiting?' · 等待新事件':''}`;
const c=byId('world'),ctx=c.getContext('2d'),pad=30,cell=Math.min((c.width-2*pad)/d.width,(c.height-2*pad)/d.height);
ctx.strokeStyle='#24373d';for(let y=0;y<d.height;y++)for(let x=0;x<d.width;x++)ctx.strokeRect(pad+x*cell,pad+y*cell,cell,cell);
for(const o of Object.values(d.objects)){ctx.fillStyle=o.observed?'#64c49a':'#667b81';ctx.beginPath();ctx.arc(pad+(o.position[0]+.5)*cell,pad+(o.position[1]+.5)*cell,Math.max(2,cell*.23),0,2*Math.PI);ctx.fill();}
ctx.strokeStyle='#77baff';ctx.lineWidth=4;ctx.strokeRect(pad+d.position[0]*cell+2,pad+d.position[1]*cell+2,Math.max(1,cell-4),Math.max(1,cell-4));
const slider=byId('event');slider.max=Math.max(0,d.events.length-1);slider.value=slider.max;slider.disabled=!d.events.length;
function render(){const e=d.events[Number(slider.value)];byId('event-label').textContent=e?`tick ${e.tick} · ${e.kind}`:'暂无事件';byId('detail').textContent=e?JSON.stringify(e,null,2):'开始一次离线实验后导出。';}
slider.addEventListener('input',render);byId('previous').onclick=()=>{slider.value=Math.max(0,Number(slider.value)-1);render()};byId('next').onclick=()=>{slider.value=Math.min(Number(slider.max),Number(slider.value)+1);render()};render();
</script></html>'''
