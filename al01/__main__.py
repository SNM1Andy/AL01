import argparse, json
from pathlib import Path
from .core import Individual, World, CLError
from .observatory import export_html
p=argparse.ArgumentParser(description='AL-01 offline P0/P1 foundation, not a consciousness claim')
p.add_argument('--steps',type=int,default=1000)
p.add_argument('--objects',type=int,default=600)
p.add_argument('--hazard',action='store_true')
p.add_argument('--local',action='store_true',help='Use radius-one local perception and movement')
p.add_argument('--resume',type=Path)
p.add_argument('--output',type=Path,default=Path('runs/latest'))
a=p.parse_args()
if not 0<=a.steps<=10000 or not 0<=a.objects<=10000: p.error('steps and objects must be 0..10000')
i=Individual.load(a.resume) if a.resume else Individual(World(a.objects,a.hazard,local=a.local))
start_tick=i.state['tick']; stop_error=None
try: executed=i.run(a.steps)
except CLError as exc:
    if exc.code!='BUDGET_EXCEEDED': raise
    executed=i.state['tick']-start_tick; stop_error=str(exc)
a.output.mkdir(parents=True,exist_ok=True); i.save(a.output/'checkpoint.json')
with (a.output/'events.jsonl').open('w',encoding='utf8') as f:
    for event in i.state['events']: f.write(json.dumps(event,ensure_ascii=False)+'\n')
report={'stop_error':stop_error,'effective_steps_this_run':executed,'logical_tick':i.state['tick'],'closed_loops':i.state['closed_loops'],
        'observed_objects':len(i.state['observed']),'waiting':i.state['waiting'],'teacher_enabled':False,
        'status':'P0/P1 foundation subset; full blueprint acceptance NOT claimed'}
(a.output/'report.json').write_text(json.dumps(report,indent=2),encoding='utf8')
print(json.dumps(report,indent=2))

export_html(i,a.output/"observatory.html")
