"""Bounded configurable deterministic fixtures. No learning/statistical claim."""
import csv,hashlib,json,platform,random
from pathlib import Path
from .core import Individual,World,canonical,digest,fail

def validate_config(c):
    if not isinstance(c,dict) or set(c)!={'schema','cases'} or c['schema']!='al01-acceptance-1':fail('CONFIG_INVALID','schema')
    if not isinstance(c['cases'],list) or not 1<=len(c['cases'])<=64:fail('CONFIG_INVALID','case count')
    names=set();total=0
    for case in c['cases']:
        if not isinstance(case,dict) or set(case)!={'name','seed','objects','steps','resume_at','local','hazard'}:fail('CONFIG_INVALID','case fields')
        name=case['name']
        if not isinstance(name,str) or not name or len(name)>64 or any(x not in 'abcdefghijklmnopqrstuvwxyz0123456789_-' for x in name) or name in names:fail('CONFIG_INVALID','case name')
        names.add(name)
        for key in ('seed','objects','steps','resume_at'):
            if type(case[key]) is not int:fail('CONFIG_INVALID',key)
        if not 0<=case['seed']<=2**32-1 or not 0<=case['objects']<=1000 or not 1<=case['steps']<=2000 or not 0<=case['resume_at']<=case['steps']:fail('CONFIG_INVALID','bounds')
        if type(case['local']) is not bool or type(case['hazard']) is not bool:fail('CONFIG_INVALID','flags')
        total+=case['steps']
    if total>40000:fail('BUDGET_EXCEEDED','acceptance total ticks')
    return c

def fixture(case):
    w=World(case['objects'],local=case['local']);rng=random.Random(case['seed'])
    # Seeds vary disclosed numeric feedback fixtures, not geometry or learned weights.
    for obj in w.objects.values():obj['value']=rng.randint(1,5)
    if w.objects:w.objects['object_0000']['value']=-1 if case['hazard'] else 1
    return w

def run_acceptance(config,output):
    validate_config(config);output=Path(output)
    if output.exists():fail('CONFIG_INVALID','output must be new directory')
    output.mkdir(parents=True,exist_ok=False)
    (output/'config.json').write_text(json.dumps(config,indent=2))
    code={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(Path(__file__).parent.glob('*.py'))}
    rows=[]
    for case in config['cases']:
        a=Individual(fixture(case));a.run(case['steps'])
        b=Individual(fixture(case));b.run(case['resume_at']);checkpoint=output/(case['name']+'.json');b.save(checkpoint)
        b=Individual.load(checkpoint);b.run(case['steps']-case['resume_at'])
        equal=a.snapshot()==b.snapshot()
        if not equal:fail('ACCEPTANCE_FAILED','restart mismatch '+case['name'])
        b.save(checkpoint)
        with (output/(case['name']+'.jsonl')).open('w') as f:
            for event in b.state['events']:f.write(canonical(event)+'\n')
        observations=[e['payload']['proposals'][0]['target'] for e in b.state['events'] if e['kind']=='commit' and e['payload']['proposals'] and e['payload']['proposals'][0]['kind']=='observe']
        rows.append({'name':case['name'],'seed':case['seed'],'local':case['local'],'hazard':case['hazard'],
          'ticks':b.state['tick'],'observed':len(b.state['observed']),'closed_loops':b.state['closed_loops'],
          'goal_status':b.state['goal_state']['status'],'second_observation':observations[1] if len(observations)>1 else '',
          'resume_equal':equal,'snapshot_sha256':digest(b.snapshot())})
    report={'schema':'al01-acceptance-report-1','config_sha256':digest(config),'source_sha256':digest(code),'source_files':code,
            'python':platform.python_version(),'platform':platform.platform(),'cases':rows,'all_resume_equal':True,
            'randomization':'numeric feedback only; fixed geometry','learning_improvement_claimed':False}
    (output/'report.json').write_text(json.dumps(report,indent=2))
    with (output/'metrics.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
    return report

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('config',type=Path);p.add_argument('output',type=Path);a=p.parse_args()
    if a.config.stat().st_size>65536:fail('CONFIG_INVALID','config size')
    report=run_acceptance(json.loads(a.config.read_text()),a.output)
    print(json.dumps({'cases':len(report['cases']),'all_resume_equal':True,'config_sha256':report['config_sha256']},indent=2))
