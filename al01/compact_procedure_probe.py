"""Fresh bounded synthetic test instances and pure closed-capsule replay.

Canonical replay is integrity evidence within a trusted controller. It is not
authentication against a host that can replace the whole record.
"""
import copy
import json
import os
import re
from pathlib import Path
from . import compact_procedures as p
from .folder_contracts import make_result,text_payload
from .folder_session import _hash,_encode
from .life_objects import LifeObjects,_json
from .life_journal import LifeJournal

MAX_CASE_BYTES=8*1024*1024

def contract(plan_ref,candidate,case,condition):
    p.need(case in dict(p.CASES) and condition in ('candidate','baseline'),'registered test condition')
    return dict(schema='procedure-probe-contract-1',plan_ref=copy.deepcopy(plan_ref),candidate_sha256=_hash(candidate),
                case=case,seed=dict(p.CASES)[case],condition=condition,protocol_sha256=_hash(p.TEST_PROTOCOL))

def names(case):return ('z.txt','a.txt') if case=='reversed' else ('a.txt','b.txt')

def expected_item(case,seed,index,q):
    earlier,later=names(case);roster=sorted((earlier,later));lookup={'entry_'+format(i+1,'032x'):path for i,path in enumerate(roster)}
    if q['operation']=='LIST_SCOPE':
        visible=[earlier] if index==0 or (case=='stale' and index>=4) else roster
        entries=[dict(entry_ref=key,relative_path=path,kind='file',byte_size=256) for key,path in lookup.items() if path in visible]
        result=make_result(q,'OK',payload=dict(entries=entries,complete=True,untrusted_data=True))
    else:
        p.need(q['entry_ref'] in lookup,'fixture visible reference');path=lookup[q['entry_ref']]
        if case=='success' and index==4 and path==later:result=make_result(q,'STALE_REF',reason='REFERENCE_EXPIRED')
        elif case=='denied' and index>=4:result=make_result(q,'DENIED',reason='ACCESS_DENIED')
        else:
            if case=='retention':text='ordinary monitored source '+path+' '+str(index)
            else:
                value=('east' if path==earlier else 'west') if case!='counterevidence' or index<4 else ('north' if path==earlier else 'south')
                text=json.dumps({'al01_claim_v1':dict(subject='fixture_'+str(seed),predicate='direction',value=value,context='declared_fixture')},sort_keys=True)
            result=make_result(q,'OK',payload=text_payload(text.encode('utf8')))
    capture=dict(schema='folder-session-1',capture_origin='SYNTHETIC_TEST_CAPTURE',result=result)
    return dict(request=copy.deepcopy(q),capture=capture,receipt_sha256=_hash(capture))

def _birth_args(candidate,plan_ref,case,condition):
    c=contract(plan_ref,candidate,case,condition);tag=_hash(c)[:24]
    return dict(identity='probe_'+tag,scope_ref='scope_'+_hash(['scope',c])[:32],seed=c['seed'],
        goals=[dict(path=path,period=1 if case=='retention' else 1000) for path in sorted(names(case))],
        field_enabled=False,u0_enabled=False,procedure_probe=candidate if condition=='candidate' else None,probe_contract=c)

def export_capsule(runtime,plan_ref,case,condition):
    head=runtime.journal.snapshot()
    p.need(head['seq']<=p.MAX_CAPSULE_EVENTS,'probe event capacity')
    events=list(runtime.journal.iter_events())
    files=[]
    with os.scandir(runtime.objects.directory) as entries:
        for inspected,entry in enumerate(entries):
            p.need(inspected<p.MAX_CAPSULE_OBJECTS+2,'probe directory inspection capacity')
            if entry.name in ('objects.json','objects.lock'):continue
            p.need(re.fullmatch(r'obj-[0-9a-f]{64}\.bin',entry.name) is not None,'unexpected probe store entry')
            p.need(len(files)<p.MAX_CAPSULE_OBJECTS,'probe object capacity');files.append(runtime.objects.directory/entry.name)
    objects=[];size=0
    for path in sorted(files):
        size+=path.stat().st_size;p.need(size<=p.MAX_CAPSULE_BYTES,'probe object-byte capacity')
        value,ref=runtime.objects._read_file(path);objects.append(dict(ref=ref,value=value))
    capsule=dict(schema='compact-procedure-probe-capsule-1',plan_ref=plan_ref,case=case,condition=condition,
        birth=copy.deepcopy(runtime.journal.birth),events=events,objects=objects,
        terminal={k:copy.deepcopy(head[k]) for k in ('seq','perception_count','event_head','state','pending')})
    _json(capsule,p.MAX_CAPSULE_BYTES)
    return capsule

class _ClosedReader(LifeObjects):
    def __init__(self,capsule,profile):
        self.identity=capsule['birth']['identity'];self.instance_id=capsule['birth']['instance_id'];self.profile=profile
        self.values={};self.used=set()
        for record in capsule['objects']:
            p.need(type(record) is dict and set(record)=={'ref','value'},'capsule object fields')
            ref=record['ref'];self._reference(ref,ref.get('kind'));raw,expected=self._frame(ref['kind'],_json(record['value']),'json')
            p.need(ref==expected and ref['sha256'] not in self.values,'exact distinct typed object frame')
            self.values[ref['sha256']]=(ref,record['value'])
    def read(self,ref,*,expected_kind):
        self._reference(ref,expected_kind);p.need(ref['sha256'] in self.values,'missing closed capsule dependency')
        known,value=self.values[ref['sha256']];p.need(known==ref,'capsule reference mismatch');self.used.add(ref['sha256']);return copy.deepcopy(value)

def verify_capsule(capsule,candidate,plan_ref,case,condition):
    from .compact_runtime import CompactRuntime,REDUCER,V026_PROFILE,_birth
    p.validate_candidate(candidate)
    p.need(type(capsule) is dict and set(capsule)=={'schema','plan_ref','case','condition','birth','events','objects','terminal'},'capsule fields')
    p.need(capsule['schema']=='compact-procedure-probe-capsule-1' and capsule['plan_ref']==plan_ref and capsule['case']==case and capsule['condition']==condition,'capsule frozen plan binding')
    p.need(type(capsule['events']) is list and 1<=len(capsule['events'])<=p.MAX_CAPSULE_EVENTS and type(capsule['objects']) is list and 1<=len(capsule['objects'])<=p.MAX_CAPSULE_OBJECTS,'bounded capsule before replay')
    _json(capsule,p.MAX_CAPSULE_BYTES)
    expected=_birth_args(candidate,plan_ref,case,condition);birth=capsule['birth']
    p.need(birth['identity']==expected['identity'] and birth['reducer_id']==REDUCER,'fresh probe identity/reducer')
    initial=_birth(expected['identity'],expected['scope_ref'],expected['goals'],expected['seed'],False,False,True,True,expected['procedure_probe'],expected['probe_contract'])
    p.need(birth['initial_state']==initial and birth['instance_id']!=candidate['origin']['instance_id'],'fresh frozen probe birth')
    reader=_ClosedReader(capsule,V026_PROFILE);runtime=object.__new__(CompactRuntime)
    runtime.identity=birth['identity'];runtime.instance_id=birth['instance_id'];runtime.objects=reader
    journal=object.__new__(LifeJournal);journal.identity=birth['identity'];journal.birth=birth;journal.reducer=runtime._reduce
    base=journal._zero();captures=[];orders=[];receipts=[]
    permitted={'INITIALIZE','CLOCK','REFRESH','REQUEST','START','RECEIPT','GOAL_BLOCK','REGULATOR_WAIT','REGULATOR_RESUME'}
    for event in capsule['events']:
        p.need(event['kind'] in permitted,'unregistered probe control')
        if event['kind']=='RECEIPT':
            item=reader.read(event['payload']['capture_ref'],expected_kind='compact_capture')
            p.need(item==expected_item(case,expected['seed'],len(captures),item['request']),'fixture output differs from registered protocol')
            decision=reader.read(base['state']['pending']['decision_ref'],expected_kind='compact_decision')
            if decision['investigation'] is not None:orders.append(decision['investigation']['role'])
            captures.append(item);receipts.append(event['payload']['capture_ref'])
        base=journal._apply(base,event)
        p.need(base['state']['logical_time']<=p.TEST_PROTOCOL['max_logical_time'],'probe logical-time budget')
    p.need(base==capsule['terminal'],'replayed terminal checkpoint differs')
    w=runtime._workflow(base['state'])
    p.need(reader.used==set(reader.values),'extra/unreferenced capsule object')
    operations=[item['request']['operation'] for item in captures]
    p.need(len(captures)<=p.TEST_PROTOCOL['max_effects_per_condition'],'probe effect budget')
    expected_ops=['LIST_SCOPE','READ_TEXT','LIST_SCOPE','READ_TEXT']+(['LIST_SCOPE'] if case=='stale' else ['READ_TEXT'] if case=='denied' else ['READ_TEXT','READ_TEXT'])
    failures=[]
    if operations!=expected_ops:failures.append('unexpected acquisition sequence')
    inspected=any(g is not None and g['status']=='SATISFIED' and g['assessment']=='INSPECTED_UNRESOLVED' for g in (w['goal'],w['last_goal']))
    if case in ('success','reversed','counterevidence') and not inspected:failures.append('inspection not completed honestly')
    if case in ('denied','stale') and (w['goal'] is None or w['goal']['status']!='BLOCKED'):failures.append('failed precondition/outcome did not block')
    if case=='retention' and (orders or w['goal'] is not None):failures.append('procedure hijacked ordinary monitoring')
    if condition=='candidate' and case in ('success','reversed','counterevidence'):
        from . import compact_guarded_procedures as guarded
        branch=guarded.branch(candidate,w['goal'])
        if branch is None or orders!=branch['order']:failures.append('learned executable order not used')
    if base['pending']:failures.append('unresolved effect at test termination')
    return dict(case=case,condition=condition,seed=expected['seed'],identity=birth['identity'],instance_id=birth['instance_id'],
                status='FAIL' if failures else 'PASS',failures=failures,order=orders,receipt_refs=receipts,event_head=base['event_head'],
                goal_assessment=None if w['goal'] is None else w['goal']['assessment'],truth_adopted=False)

def run_condition(directory,candidate,plan_ref,case,condition):
    from .compact_runtime import CompactRuntime
    directory=Path(directory);p.need(not directory.exists(),'fresh probe directory only; never reset/retry')
    args=_birth_args(candidate,plan_ref,case,condition);runtime=CompactRuntime.fresh(directory,**args);calls=[]
    def callback(q):
        item=expected_item(case,args['seed'],len(calls),q);calls.append(copy.deepcopy(q));return item
    runtime.step(0,callback);runtime.step(0,callback);runtime.refresh_inventory(0);runtime.step(0,callback);runtime.step(0,callback)
    if case=='stale':
        runtime.refresh_inventory(0);runtime.step(1,callback);runtime.step(1,callback)
    else:
        runtime.step(1,callback)
        if case!='denied':runtime.step(2,callback)
    return export_capsule(runtime,plan_ref,case,condition)

def run_case(directory,candidate,plan_ref,case):
    directory=Path(directory);p.need(not directory.exists(),'fresh case directory; no overwrite')
    directory.mkdir(parents=True)
    value=dict(schema='compact-procedure-test-case-1',case=case,plan_ref=plan_ref,candidate_sha256=_hash(candidate),
               probe=run_condition(directory/'probe',candidate,plan_ref,case,'candidate'),baseline=None)
    if case in ('success','reversed'):value['baseline']=run_condition(directory/'baseline',candidate,plan_ref,case,'baseline')
    _json(value,MAX_CASE_BYTES)
    (directory/'capsule.json').write_bytes(_encode(value))
    return value

def verify_case(value,candidate,plan_ref,case):
    p.need(type(value) is dict and set(value)=={'schema','case','plan_ref','candidate_sha256','probe','baseline'},'case capsule fields')
    p.need(value['schema']=='compact-procedure-test-case-1' and value['case']==case and value['plan_ref']==plan_ref and value['candidate_sha256']==_hash(candidate),'case candidate/plan binding')
    _json(value,MAX_CASE_BYTES)
    probe=verify_capsule(value['probe'],candidate,plan_ref,case,'candidate');baseline=None
    if case in ('success','reversed'):
        baseline=verify_capsule(value['baseline'],candidate,plan_ref,case,'baseline')
        p.need(probe['instance_id']!=baseline['instance_id'],'distinct actual control instance')
    else:p.need(value['baseline'] is None,'unexpected baseline capsule')
    return dict(probe=probe,baseline=baseline,passed=probe['status']=='PASS' and (baseline is None or baseline['status']=='PASS'),
                changed_order=baseline is not None and probe['order']!=baseline['order'])
