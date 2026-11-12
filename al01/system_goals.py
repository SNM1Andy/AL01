"""Pure receipt-bound operational goals; no desires, truth adoption or authority.

Exploration progress means a new (listed relative path, content SHA) observation.
Inspect means capture of a named path, not understanding its contents. Query
resolution is deliberately not inferred from reply delivery. Caller persists the
returned JSON alongside its controller checkpoint before using an allocation.
"""
import copy
import math
from .folder_session import _hash
from .folder_cognition import validate_replay
from .folder_contracts import validate_ref,validate_relative_path
from .governance import EPS,MAX_AGE

SCHEMA='system-goals-1'
STATUSES={'active','waiting','blocked','satisfied','cancelled'}
KINDS={'explore','inspect','receive_reply','resolve_query'}
MAX_GOALS=16
MAX_RECORDS=256


def need(ok,message):
    if not ok:raise ValueError('SYSTEM_GOALS_INVALID: '+message)


def token(value):
    need(type(value) is str and 0<len(value)<=128,'identifier')
    value.encode('utf8');return value


def sha(value):
    need(type(value) is str and len(value)==64 and all(c in '0123456789abcdef' for c in value),'digest')


def initial_state(identity,scope_ref):
    token(identity);validate_ref(scope_ref,'scope')
    return dict(schema=SCHEMA,identity=identity,scope_ref=scope_ref,goals=[],sources=[],environment=[],records=[],allocation_head=None,ages={},suppressed=[])


def validate_state(state):
    need(type(state) is dict and set(state)==set(initial_state('validator','scope_'+'0'*32)),'state fields')
    need(state['schema']==SCHEMA,'schema');token(state['identity']);validate_ref(state['scope_ref'],'scope')
    need(type(state['goals']) is list and len(state['goals'])<=MAX_GOALS,'goals')
    ids=set()
    for g in state['goals']:
        need(type(g) is dict and set(g)=={'id','kind','target','limit','status','progress','evidence','reason','source_sha256'},'goal fields')
        token(g['id']);need(g['id'] not in ids,'duplicate goal');ids.add(g['id'])
        need(g['kind'] in KINDS and g['status'] in STATUSES,'goal kind/status')
        if g['kind']=='explore':need(g['target'] is None,'explore target')
        elif g['kind']=='inspect':validate_relative_path(g['target'])
        else:token(g['target'])
        need(type(g['limit']) is int and 1<=g['limit']<=32 and (g['kind']=='explore' or g['limit']==1),'goal limit')
        need(type(g['progress']) is int and 0<=g['progress']<=g['limit'],'progress')
        need(type(g['evidence']) is list and len(g['evidence'])==g['progress'] and len(set(g['evidence']))==len(g['evidence']),'progress evidence')
        for h in g['evidence']:sha(h)
        token(g['reason'])
        if g['source_sha256'] is not None:sha(g['source_sha256'])
        need((g['status']=='satisfied')==(g['progress']==g['limit']),'satisfaction evidence')
    need(type(state['sources']) is list and len(state['sources'])<=32,'sources')
    for h in state['sources']:sha(h)
    need(type(state['environment']) is list and len(state['environment'])<=32 and len(set(state['environment']))==len(state['environment']),'environment novelty')
    for h in state['environment']:sha(h)
    need(type(state['records']) is list and len(state['records'])<=MAX_RECORDS,'records')
    for r in state['records']:
        need(type(r) is dict and set(r)=={'goal_id','status','reason','source_sha256'},'record fields')
        need(r['goal_id'] in ids and r['status'] in STATUSES,'record goal/status');token(r['reason'])
        if r['source_sha256'] is not None:sha(r['source_sha256'])
    if state['allocation_head'] is not None:sha(state['allocation_head'])
    need(type(state['ages']) is dict and set(state['ages'])<=ids,'ages')
    need(all(type(v) is int and 0<=v<=MAX_AGE for v in state['ages'].values()),'bounded ages')
    need(type(state['suppressed']) is list and len(state['suppressed'])<=256 and len(set(state['suppressed']))==len(state['suppressed']),'suppression')
    for h in state['suppressed']:sha(h)
    return None


def record(state,g,reason,source):
    need(len(state['records'])<MAX_RECORDS,'record capacity')
    g['reason']=reason;g['source_sha256']=source
    state['records'].append(dict(goal_id=g['id'],status=g['status'],reason=reason,source_sha256=source))


def add_goal(state,goal_id,kind,target=None,limit=1):
    validate_state(state);out=copy.deepcopy(state)
    need(len(out['goals'])<MAX_GOALS and not any(g['id']==goal_id for g in out['goals']),'new goal capacity/id')
    g=dict(id=goal_id,kind=kind,target=target,limit=limit,status='active',progress=0,evidence=[],reason='explicit_goal',source_sha256=None)
    out['goals'].append(g);record(out,g,'explicit_goal',None);validate_state(out);return out


def transition(state,goal_id,status,reason,source_sha256=None):
    validate_state(state);out=copy.deepcopy(state)
    g=next((g for g in out['goals'] if g['id']==goal_id),None)
    need(g is not None and g['status'] not in {'satisfied','cancelled'},'unfinished goal required')
    need(status in {'active','waiting','blocked','cancelled'},'no operator-declared satisfaction')
    token(reason)
    if source_sha256 is not None:sha(source_sha256)
    g['status']=status;record(out,g,reason,source_sha256);validate_state(out);return out


def apply_cognition(state,cognition):
    validate_state(state)
    need(cognition.get('identity')==state['identity'] and cognition.get('scope_ref')==state['scope_ref'],'identity/scope')
    validate_replay(cognition,cognition['sources'])
    bindings=[_hash(s) for s in cognition['sources']]
    need(bindings[:len(state['sources'])]==state['sources'],'append-only source prefix')
    out=copy.deepcopy(state);paths={};seen=set()
    for i,s in enumerate(cognition['sources']):
        q=s['request'];r=s['capture']['result']
        if q['operation']=='LIST_SCOPE':
            paths={e['entry_ref']:e['relative_path'] for e in r['payload']['entries']} if r['status']=='OK' else {}
            if r['status']=='OK':
                inventory=sorted([(e['relative_path'],e['kind'],e['byte_size']) for e in r['payload']['entries']])
                novelty=_hash(['inventory',inventory,r['payload']['complete']])
                if novelty not in out['environment']:out['environment'].append(novelty)
        if q['operation']!='READ_TEXT' or r['status']!='OK' or q['entry_ref'] not in paths:continue
        path=paths[q['entry_ref']];key=_hash([path,r['payload']['content_sha256']]);novel=key not in seen;seen.add(key)
        if key not in out['environment']:out['environment'].append(key)
        if i<len(state['sources']) or not novel:continue
        for g in out['goals']:
            if g['status'] not in {'active','waiting'}:continue
            if g['kind']=='explore' or (g['kind']=='inspect' and g['target']==path):
                if key in g['evidence']:continue
                g['evidence'].append(key);g['progress']+=1
                g['status']='satisfied' if g['progress']==g['limit'] else 'active'
                record(out,g,'novel_path_content_capture',s['receipt_sha256'])
    out['sources']=bindings;validate_state(out);return out


def allocate(state,candidates,*,budget=2):
    """One advisory executor slot; budget/capabilities still checked by executor.

    A candidate is goal_id/action_id/cost/gain/urgency/source_sha256. The source
    must be the exact committed capture-prefix digest. Costs are fixed by typed
    operation, not a proposer. Dispatch IDs cannot bypass stable goal/operation/semantic-intent suppression.
    A new novel environmental token permits a later attempt; polls do not.
    """
    validate_state(state);need(type(budget) is int and 0<=budget<=4,'budget')
    need(type(candidates) is list and len(candidates)<=16,'candidate budget')
    out=copy.deepcopy(state);head=_hash(state['sources'])
    progress_head=_hash(state['environment']);fresh=progress_head!=state['allocation_head']
    goals={g['id']:g for g in state['goals']};rows=[];ids=set()
    costs={'LIST_SCOPE':1,'READ_TEXT':1,'WAIT':1,'ASK_LOCAL':2,'REQUEST_HELP':2,'INTERACT':2}
    for c in candidates:
        need(type(c) is dict and set(c)=={'goal_id','action_id','operation','cost','gain','urgency','source_sha256','intent'},'candidate fields')
        need(c['goal_id'] in goals,'candidate goal');token(c['action_id']);need(c['action_id'] not in ids,'unique action');ids.add(c['action_id'])
        need(c['operation'] in costs and type(c['cost']) is int and c['cost']==costs[c['operation']],'registered action cost')
        need(c['source_sha256']==head,'current source binding')
        for key,upper in [('gain',1),('urgency',.25)]:need(type(c[key]) in (int,float) and math.isfinite(c[key]) and 0<=c[key]<=upper,'bounded priority')
        intent=c['intent']
        need(type(intent) is dict and set(intent)=={'target','query'},'semantic intent fields')
        token(intent['target']);need(type(intent['query']) is str and len(intent['query'].encode('utf8'))<=1024,'bounded literal intent query')
        if c['operation']=='READ_TEXT':validate_relative_path(intent['target'])
        if c['operation']=='LIST_SCOPE':need(intent['target']==state['scope_ref'],'listing scope intent')
        if c['operation'] in {'READ_TEXT','LIST_SCOPE'}:need(intent['query']=='','no query for sensing/wait')
        intent_key=_hash({'identity':state['identity'],'scope_ref':state['scope_ref'],'goal_id':c['goal_id'],
                          'action':c['operation'],'recipient':None if c['operation']=='WAIT' else intent['target'],
                          'query':'' if c['operation']=='WAIT' else intent['query']})
        signature=_hash([intent_key,progress_head]);age=state['ages'].get(c['goal_id'],0)
        if goals[c['goal_id']]['status']=='active' and c['cost']<=budget and signature not in state['suppressed']:
            rows.append(dict(c,intent_key=intent_key,signature=signature,priority=c['gain']/(c['cost']+EPS)+c['urgency']+.025*age))
    rows.sort(key=lambda r:(-r['priority'],r['goal_id'],r['action_id']));winner=rows[0] if rows else None
    if fresh:
        out['ages']={g['id']:min(MAX_AGE,state['ages'].get(g['id'],0)+1) for g in state['goals'] if g['status']=='active'}
    if winner:
        need(len(out['suppressed'])<256,'allocation capacity');out['suppressed'].append(winner['signature']);out['ages'][winner['goal_id']]=0
    out['allocation_head']=progress_head;validate_state(out)
    return out,dict(source_sha256=head,selected=winner,executor_count=int(winner is not None),authority_granted=False)


def apply_actions(state,action_state):
    """A correlated reply satisfies delivery only, never a knowledge goal.

    Action history is replay-validated. The coordinator must separately resolve
    each request source hash to its committed joint snapshot before submission.
    No text contents are interpreted, and no cognition/maturity increment occurs.
    """
    from .system_actions import validate_state as validate_actions
    validate_state(state);actions=validate_actions(action_state)
    need(actions['identity']==state['identity'] and actions['scope_ref']==state['scope_ref'],'local action identity/scope')
    out=copy.deepcopy(state)
    for g in out['goals']:
        if g['kind']!='receive_reply' or g['status'] not in {'active','waiting'}:continue
        row=actions['requests'].get(g['target'])
        if row is None or row['status']=='PENDING':continue
        need(row['request']['goal_id']==g['id'],'reply goal correlation')
        receipt=row['outcome'];source=receipt['receipt_sha256']
        if row['status']=='RESPONDED':
            g['evidence']=[source];g['progress']=1;g['status']='satisfied'
            record(out,g,'correlated_untrusted_reply_delivered',source)
        elif row['status'] in {'DENIED','UNAVAILABLE','UNKNOWN','TIMED_OUT'}:
            g['status']='blocked';record(out,g,'local_request_'+row['status'].lower(),source)
    validate_state(out);return out
