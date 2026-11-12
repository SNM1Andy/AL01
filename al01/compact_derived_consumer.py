"""Pure phase12 n/Q component; no birth, journal writes, dispatch or adoption.

All state roots are owner-verified reducer inputs, not host authentication.
Existing reducers recheck bounded original REQUEST/START/RECEIPT feedback.
Every output remains NOT_RUN for individual learning until a new mode exists.
"""
import copy
from fractions import Fraction
import re

from . import compact_derived_observation as memory
from . import compact_skill_evidence as evidence
from .core import CVM, VERSION, transaction_payload
from .folder_contracts import validate_ref, make_request
from .folder_session import _encode, _hash
from .life_objects import _json, ObjectProfile
from .life_journal import LifeJournal, MAX_RECORD_BYTES

PROFILE='derived-feedback-choice-research-1'
CONTRACT='derived-consumer-contract-1'
STATE='derived-consumer-component-state-1'
ACTIONS=('FAILED_PATH','OTHER_PATH')
FEATURES=memory.RELATIONS
MAX_TASKS=16
MAX_TRAIN=8
MAX_STATE_BYTES=65536
MAX_BASE_BYTES=70000
WEIGHTS=dict(task_progress=[1,1],verified_information_gain=[0,1],actual_cost=[1,4],invalid_action_penalty=[1,1])


def need(ok,reason):
    if not ok:raise ValueError('DERIVED_CONSUMER_INVALID: '+reason)


def digest(x):
    return type(x) is str and re.fullmatch('[0-9a-f]{64}',x) is not None


def draft_contract(origin,tasks):
    c=dict(schema=CONTRACT,profile=PROFILE,status='DRAFT',run_status='NOT_RUN',origin=copy.deepcopy(origin),
        tasks=copy.deepcopy(tasks),environment_pin=None,measurement_pin=None,cl_version=VERSION,
        actions=list(ACTIONS),features=list(FEATURES),weights=copy.deepcopy(WEIGHTS),alpha='ONE_OVER_N',
        max_tasks=MAX_TASKS,max_train=MAX_TRAIN,minimum_support=1,authority='NONE')
    return validate_contract(c)


def validate_contract(c):
    _json(c,16384)
    fields={'schema','profile','status','run_status','origin','tasks','environment_pin','measurement_pin','cl_version',
            'actions','features','weights','alpha','max_tasks','max_train','minimum_support','authority'}
    need(type(c) is dict and set(c)==fields,'contract fields')
    need(c['schema']==CONTRACT and c['profile']==PROFILE and c['status']=='DRAFT'
         and c['run_status']=='NOT_RUN' and c['authority']=='NONE' and c['cl_version']==VERSION,'component version/status')
    need(c['environment_pin'] is None and c['measurement_pin'] is None,'world not frozen in this component')
    o=c['origin'];need(type(o) is dict and set(o)=={'identity','instance_id','scope_ref'},'origin fields')
    need(type(o['identity']) is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.-]{0,127}',o['identity']) is not None
         and type(o['instance_id']) is str and re.fullmatch('[0-9a-f]{32}',o['instance_id']) is not None,'source namespace')
    validate_ref(o['scope_ref'],'scope')
    need(_encode(c['actions'])==_encode(list(ACTIONS)) and _encode(c['features'])==_encode(list(FEATURES))
         and _encode(c['weights'])==_encode(WEIGHTS) and c['alpha']=='ONE_OVER_N'
         and type(c['minimum_support']) is int and c['minimum_support']==1
         and type(c['max_tasks']) is int and c['max_tasks']==MAX_TASKS
         and type(c['max_train']) is int and c['max_train']==MAX_TRAIN,'fixed learning equation/bounds')
    need(type(c['tasks']) is list and 1<=len(c['tasks'])<=MAX_TASKS,'task budget')
    ids=set();groups=set();trains=0
    for t in c['tasks']:
        need(type(t) is dict and set(t)=={'id','partition','group'},'task fields')
        need(all(type(t[k]) is str and re.fullmatch('[A-Za-z0-9_-]{1,64}',t[k]) for k in ('id','group'))
             and t['partition'] in ('TRAIN','VALIDATION','TEST'),'task types')
        need(t['id'] not in ids and t['group'] not in groups,'partition/group overlap')
        ids.add(t['id']);groups.add(t['group']);trains+=int(t['partition']=='TRAIN')
    need(1<=trains<=MAX_TRAIN,'training budget')
    return copy.deepcopy(c)


def _task(c,task_id):
    rows=[t for t in c['tasks'] if t['id']==task_id];need(len(rows)==1,'declared task');return rows[0]


def _policy():
    return dict(schema='derived-nq-policy-1',version=0,parent_hash=None,
        cells={f:{a:dict(n=0,reward_quarters_sum=0,Q=[0,1]) for a in ACTIONS} for f in FEATURES})


def _bump(policy,feature,action,reward_quarters):
    need(feature in FEATURES and action in ACTIONS and type(reward_quarters) is int
         and reward_quarters in (-1,3),'verified finite reward domain')
    out=copy.deepcopy(policy);out['parent_hash']=_hash(policy);out['version']+=1
    cell=out['cells'][feature][action];cell['n']+=1;cell['reward_quarters_sum']+=reward_quarters
    q=Fraction(cell['reward_quarters_sum'],4*cell['n']);cell['Q']=[q.numerator,q.denominator]
    return out


def initial(contract):
    validate_contract(contract)
    return dict(schema=STATE,contract_hash=_hash(contract),run_status='NOT_RUN',policy=_policy(),
        frozen=False,frozen_policy_hash=None,rows=[],authority='NONE',evidence_scope='COMPONENT_REPLAY_NOT_PERSONAL_UPDATE')


def validate_state(s,c):
    validate_contract(c);_json(s,MAX_STATE_BYTES)
    need(type(s) is dict and set(s)==set(initial(c)) and s['schema']==STATE and s['contract_hash']==_hash(c)
         and s['run_status']=='NOT_RUN' and s['authority']=='NONE'
         and s['evidence_scope']=='COMPONENT_REPLAY_NOT_PERSONAL_UPDATE','state version/contract/source')
    need(type(s['rows']) is list and len(s['rows'])<=MAX_TASKS and type(s['frozen']) is bool,'state budget')
    p=_policy();used=set();events=set();seq=0;evaluated=False
    for row in s['rows']:
        need(type(row) is dict and set(row)=={'task_id','partition','event_hash','proof_hash','receipt_seq',
            'decision_ref','capture_ref','derived_ref','feature','action','status','reward','reward_quarters',
            'policy_before','policy_after'},'evidence row fields')
        t=_task(c,row['task_id'])
        need(not (evaluated and t['partition']=='TRAIN'),'training cannot follow evaluation')
        evaluated=evaluated or t['partition']!='TRAIN'
        need(row['partition']==t['partition'] and row['task_id'] not in used and row['event_hash'] not in events
             and digest(row['event_hash']) and digest(row['proof_hash']) and type(row['receipt_seq']) is int
             and row['receipt_seq']>seq,'unique ordered original evidence')
        used.add(row['task_id']);events.add(row['event_hash']);seq=row['receipt_seq']
        need(row['feature'] in FEATURES and row['action'] in ACTIONS and row['policy_before']==_hash(p),'policy lineage')
        for key,kind in (('decision_ref','compact_decision'),('capture_ref','compact_capture'),('derived_ref',memory.KIND)):
            evidence._Objects(c['origin'],lambda ref:b'').codec._reference(row[key],kind)
        expected=_reward(row['status']);need(row['reward']==expected[0] and row['reward_quarters']==expected[1],'reward equation')
        if row['partition']=='TRAIN' and row['reward_quarters'] is not None:p=_bump(p,row['feature'],row['action'],row['reward_quarters'])
        need(row['policy_after']==_hash(p),'recomputed policy transition')
    need(s['policy']==p,'n/Q reconstructed from append-only rows')
    need(not evaluated or s['frozen'],'evaluation rows require frozen policy')
    need((s['frozen_policy_hash']==_hash(p)) if s['frozen'] else s['frozen_policy_hash'] is None,'frozen policy')
    return copy.deepcopy(s)


def freeze(s,c):
    s=validate_state(s,c)
    need({t['id'] for t in c['tasks'] if t['partition']=='TRAIN'}
         <= {r['task_id'] for r in s['rows']},'all declared train attempts required; no replacements')
    s['frozen']=True;s['frozen_policy_hash']=_hash(s['policy']);return validate_state(s,c)


class _NoJournal:
    def __getattr__(self,name):raise ValueError('DERIVED_CONSUMER_INVALID: mutable journal access '+name)


def _kernel(origin,objects,object_profile):
    from .compact_runtime import CompactRuntime
    r=object.__new__(CompactRuntime);r.identity=origin['identity'];r.instance_id=origin['instance_id']
    r._read=objects.read;r.journal=_NoJournal()
    r.objects=objects.codec;r.objects.profile=ObjectProfile(**object_profile)
    return r


def _base(c,before):
    _json(before,MAX_BASE_BYTES)
    need(type(before) is dict and set(before)=={'seq','perception_count','event_head','state','pending'},'owner checkpoint base')
    need(all(before['state'][k]==v for k,v in c['origin'].items()) and digest(before['event_head']),'owner root namespace')
    return before['state']


def _choose(policy,feature,native_order,train_index=None):
    need(type(native_order) is list and 1<=len(native_order)<=2 and len(set(native_order))==len(native_order)
         and set(native_order)<=set(ACTIONS),'bounded legal role options')
    if len(native_order)==1:return native_order[0],'ONLY_LEGAL_ACTION_NO_CHOICE'
    if train_index is not None:return native_order[train_index%2],'FIXED_EXPLORATION'
    cells=policy['cells'][feature]
    if any(cells[a]['n']<1 for a in ACTIONS):return native_order[0],'NATIVE_FALLBACK_INSUFFICIENT_SUPPORT'
    return max(native_order,key=lambda a:Fraction(*cells[a]['Q'])),'EMPIRICAL_Q'


def _stage(source,visible,target):
    cl=f'PROPOSE(OBSERVE(@{target}));';vm=CVM(('read','observe'))
    tx=vm.prepare(cl,dict(version=source['cognition_revision'],world_version=source['inventory_generation']),visible);vm.verify(tx)
    request=make_request('READ_TEXT',source['scope_ref'],request_id='req_'+_hash(dict(source=source,target=target,operation='READ_TEXT'))[:32],entry_ref=target)
    return dict(request=request,generated_cl=cl,transaction_payload=transaction_payload(tx))


def _propose(s,c,task_id,before,objects):
    task=_task(c,task_id);st=_base(c,before)
    abstain=lambda why:dict(status='ABSTAIN',reason=why,run_status='NOT_RUN',authority='NONE')
    if task['partition']=='TRAIN':need(not s['frozen'],'no training after freeze')
    else:need(s['frozen'],'evaluation requires freeze')
    if st['pending'] is not None or before['pending']:return abstain('PENDING_OR_UNKNOWN')
    if st.get('derived_observation_ref') is None:return abstain('NO_COMMITTED_DERIVED_FEATURE')
    r=_kernel(c['origin'],objects,st['config']['object_profile']);w=r._workflow(st)
    if ((w['goal'] is not None and w['goal']['status'] not in ('SATISFIED','BLOCKED'))
        or w['help']['episode'] is not None or w['interaction']['pending'] is not None
        or (w['typed_wait'] is not None and w['typed_wait']['status']=='WAITING')):return abstain('PROTECTED_WORK')
    record=objects.read(st['derived_observation_ref'],memory.KIND);memory.validate(record,c['origin'])
    need(record['commit']['receipt_seq']<=before['seq'],'feature precedes action')
    d=r._decision(copy.deepcopy(st),before['seq'],before['event_head'])
    if d is None or d['request']['operation']!='READ_TEXT' or d.get('investigation') is not None:return abstain('NO_ORDINARY_READ_OPTIONS')
    need(d['source']['journal_head']==before['event_head'] and d['source']['global_seq']==before['seq'],'pre-action source')
    options={};native=[]
    for ranked in d['ranking']['ranked']:
        ref=ranked['entity_ref'];path=next(m['path'] for m in d['candidate_metadata'] if m['entry_ref']==ref)
        action='FAILED_PATH' if path==record['path'] else 'OTHER_PATH'
        if action not in options:options[action]=ref;native.append(action)
    if not options:return abstain('NO_LEGAL_ROLE_OPTIONS')
    index=None
    if task['partition']=='TRAIN':index=[t['id'] for t in c['tasks'] if t['partition']=='TRAIN'].index(task_id)
    action,why=_choose(s['policy'],record['reference_relation'],native,index)
    visible=sorted(e['entry_ref'] for e in st['inventory']['entries'] if e['kind']=='file')
    return dict(status='STAGED_ONLY',run_status='NOT_RUN',authority='NONE',reason=why,
        contract_hash=_hash(c),policy_hash=_hash(s['policy']),task_id=task_id,feature=record['reference_relation'],
        derived_ref=copy.deepcopy(st['derived_observation_ref']),action=action,options=options,
        before_hash=_hash(before),**_stage(d['source'],visible,options[action]))


def propose(s,c,task_id,before,read_frame):
    s=validate_state(s,c);return _propose(s,c,task_id,before,evidence._Objects(c['origin'],read_frame))


def _reward(status):
    if status=='OK':progress,r4=1,3
    elif status in ('STALE_REF','NOT_FOUND','IO_ERROR'):progress,r4=0,-1
    elif status in ('DENIED','UNAVAILABLE','INVALID_TEXT','LIMIT_EXCEEDED'):progress,r4=None,None
    else:need(False,'unsupported result status')
    return dict(task_progress=progress,verified_information_gain=None,actual_cost=1,invalid_action_penalty=0),r4


def update(s,c,task_id,before,three_events,read_frame):
    s=validate_state(s,c);_base(c,before);_task(c,task_id)
    need(type(three_events) is list and len(three_events)<=3,'bounded three-event proof')
    for event in three_events:_json(event,MAX_RECORD_BYTES)
    if [e.get('kind') for e in three_events] in ([],['REQUEST'],['REQUEST','START']):
        return s,dict(status='UNSCORED_UNKNOWN',run_status='NOT_RUN',retry_authority='NONE')
    need([e.get('kind') for e in three_events]==['REQUEST','START','RECEIPT'],'original request/start/receipt only')
    proof_hash=_hash([before,three_events]);event=three_events[-1]
    duplicates=[row for row in s['rows'] if row['event_hash']==event['sha256'] or row['task_id']==task_id]
    if duplicates:
        need(len(duplicates)==1 and duplicates[0]['proof_hash']==proof_hash and duplicates[0]['task_id']==task_id,'reused receipt/task under altered attribution')
        return s,dict(status='DUPLICATE',run_status='NOT_RUN')
    need(len(s['rows'])<MAX_TASKS,'finite feedback capacity')
    objects=evidence._Objects(c['origin'],read_frame)
    proposal=_propose(s,c,task_id,before,objects);need(proposal['status']=='STAGED_ONLY','no supported pre-action feature/options')
    r=_kernel(c['origin'],objects,before['state']['config']['object_profile'])
    j=object.__new__(LifeJournal);j.identity=c['origin']['identity'];j.birth={'instance_id':c['origin']['instance_id']};j.reducer=r._reduce
    after=copy.deepcopy(before)
    for e in three_events:after=j._apply(after,e)
    dref=three_events[0]['payload']['decision_ref'];d=objects.read(dref,'compact_decision')
    for key in ('request','generated_cl','transaction_payload'):
        need(_encode(d[key])==_encode(proposal[key]),'actual CL action differs from recomputed component proposal')
    need(d['request']['operation']=='READ_TEXT' and d['governance']['external_action_cost']==1,'one admitted READ cost')
    capref=event['payload']['capture_ref'];item=objects.read(capref,'compact_capture')
    post=objects.read(after['state']['workflow_ref'],'compact_workflow')
    need(post['regulation']['source']['seq']==event['seq'] and post['regulation']['source']['capture_ref']==capref,'actual regulation cost receipt')
    reward,r4=_reward(item['capture']['result']['status'])
    before_hash=_hash(s['policy'])
    if _task(c,task_id)['partition']=='TRAIN' and r4 is not None:s['policy']=_bump(s['policy'],proposal['feature'],proposal['action'],r4)
    s['rows'].append(dict(task_id=task_id,partition=_task(c,task_id)['partition'],event_hash=event['sha256'],proof_hash=proof_hash,
        receipt_seq=event['seq'],decision_ref=dref,capture_ref=capref,derived_ref=proposal['derived_ref'],
        feature=proposal['feature'],action=proposal['action'],status=item['capture']['result']['status'],
        reward=reward,reward_quarters=r4,policy_before=before_hash,policy_after=_hash(s['policy'])))
    return validate_state(s,c),dict(status='COMPONENT_UPDATED' if before_hash!=_hash(s['policy']) else 'RECORDED_WITHOUT_POLICY_UPDATE',
        run_status='NOT_RUN',individual_learning=False,reward=reward,reward_quarters=r4)
