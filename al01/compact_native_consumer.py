"""Birth-gated one-READ task state. Pure transitions, no effects or I/O."""
import copy

from . import compact_derived_consumer as q
from . import compact_derived_observation as memory
from .compact_observer import validate_capture
from .folder_session import _hash,_encode
from .life_objects import LifeObjects,_json,MAX_HEADER,SCHEMA as OBJECT_SCHEMA
from . import compact_native_order_evidence as passive

PROFILE='native-derived-read-task-1'
REFRESH_PROFILE='native-task-refresh-read-2'
CANONICAL_PROFILE='native-five-condition-read-3'
PREFERENCE_PROFILE='native-five-condition-read-4'
REFRESH_PROFILES=(REFRESH_PROFILE,CANONICAL_PROFILE,PREFERENCE_PROFILE)
ROOT_KIND='compact_native_consumer'
POLICY_KIND='compact_native_nq_policy'
TASK_KIND='compact_native_read_task'
ROOT_BYTES=32768
POLICY_BYTES=8192
TASK_BYTES=8192
CONTROL_BYTES=8192
ROW_RESERVE=4096


def need(ok,why):
    if not ok:raise ValueError('NATIVE_CONSUMER_INVALID: '+why)


def config(c):
    _json(c,8192)
    if type(c) is dict and c.get('schema') in REFRESH_PROFILES:
        from . import compact_native_task_refresh as refresh
        return refresh.config(c)
    need(type(c) is dict and set(c)=={'schema','condition','tasks'} and c['schema']==PROFILE
         and c['condition'] in ('LEARNED','UNTRAINED','NO_MEMORY'),'birth profile')
    # Reuse task/group partition/type budgets, never its component status.
    q.draft_contract(dict(identity='validate',instance_id='0'*32,scope_ref='scope_'+'0'*32),c['tasks'])
    return copy.deepcopy(c)


def origin(state):return {k:state[k] for k in ('identity','instance_id','scope_ref')}


def ref(value,kind,o):
    codec=object.__new__(LifeObjects);codec.identity=o['identity'];codec.instance_id=o['instance_id']
    return codec._frame(kind,_json(value), 'json')[1]


def initial_policy(o):
    return dict(schema='native-nq-policy-1',origin=copy.deepcopy(o),revision=0,parent_ref=None,statistics=q._policy(),frozen=False)


def initial_root(o,policy_ref,c=None):
    if c is not None and c.get('schema') in REFRESH_PROFILES:
        from . import compact_native_task_refresh as refresh
        return refresh.initial_root(o,policy_ref,c)
    return dict(schema=PROFILE,origin=copy.deepcopy(o),version=0,parent_ref=None,policy_ref=copy.deepcopy(policy_ref),
        current=None,completed=[],frozen=False)


def validate(root,policy,o,c):
    if c.get('schema') in REFRESH_PROFILES:
        from . import compact_native_task_refresh as refresh
        return refresh.validate(root,policy,o,c)
    config(c);_json(root,ROOT_BYTES);_json(policy,POLICY_BYTES)
    need(type(root) is dict and set(root)==set(initial_root(o,None)) and root['schema']==PROFILE
         and root['origin']==o and type(root['version']) is int and root['version']>=0,'root source/version')
    need(type(policy) is dict and set(policy)==set(initial_policy(o)) and policy['schema']=='native-nq-policy-1'
         and policy['origin']==o and type(policy['revision']) is int and policy['revision']>=0
         and type(policy['frozen']) is bool and root['frozen']==policy['frozen'],'policy source/version')
    need(type(root['completed']) is list and len(root['completed'])<=len(c['tasks'])
         and type(root['frozen']) is bool,'bounded tasks')
    p=q._policy();ids=set();last_seq=0
    for row in root['completed']:
        need(type(row) is dict and set(row)=={'id','partition','seq','task_ref','decision_ref','capture_ref',
            'status','feature','action','reward','reward_quarters','policy_before','policy_after'},'row fields')
        t=task_spec(c,row['id']);need(row['partition']==t['partition'] and row['id'] not in ids
            and type(row['seq']) is int and row['seq']>last_seq,'task evidence order/partition')
        ids.add(row['id']);last_seq=row['seq']
        if c['condition']=='LEARNED' and row['partition']=='TRAIN' and row['reward_quarters'] is not None:
            need(row['feature'] in q.FEATURES and row['action'] in q.ACTIONS,'scored feature/action')
            p=q._bump(p,row['feature'],row['action'],row['reward_quarters'])
    need(policy['statistics']==p,'recomputed native n/Q')
    if root['current'] is not None:
        need(type(root['current']) is dict and set(root['current'])=={'id','task_ref'}
             and root['current']['id'] not in ids,'one current uncompleted task')
    return root,policy


def task_spec(c,task_id):
    rows=[t for t in c['tasks'] if t['id']==task_id];need(len(rows)==1,'declared task id');return rows[0]


def allowed(state,workflow):
    g=workflow['goal'];wait=workflow['typed_wait']
    return (state['pending'] is None and state['native_acquisition']['phase']=='DONE'
        and state['inventory']['valid'] and (g is None or g['status'] in ('BLOCKED','SATISFIED'))
        and workflow['help']['episode'] is None and workflow['interaction']['pending'] is None
        and not(wait is not None and wait['status']=='WAITING'))


def control(state,root,policy,c,kind,task_id,source,seq):
    if c.get('schema') in REFRESH_PROFILES:
        from . import compact_native_task_refresh as refresh
        return refresh.control(state,root,policy,c,kind,task_id,source,seq)
    o=origin(state);out=copy.deepcopy(root);out['parent_ref']=copy.deepcopy(state['native_consumer_ref']);out['version']+=1
    task=None;newpolicy=None
    if kind=='NATIVE_TASK_BEGIN':
        need(root['current'] is None,'one idle task lane')
        t=task_spec(c,task_id);used={x['id'] for x in root['completed']}
        need(task_id not in used,'task cannot be reused')
        if t['partition']=='TRAIN':need(not root['frozen'],'training after freeze prohibited')
        else:need(root['frozen'],'evaluation requires frozen policy')
        remaining=[x['id'] for x in c['tasks'] if x['id'] not in used]
        need(remaining and task_id==remaining[0],'fixed task order, no replacement')
        task=dict(schema='native-one-read-task-1',origin=o,id=task_id,partition=t['partition'],
            ordinal=[x['id'] for x in c['tasks'] if x['partition']=='TRAIN'].index(task_id) if t['partition']=='TRAIN' else None,
            begin_seq=seq,source=copy.deepcopy(source),policy_ref=copy.deepcopy(root['policy_ref']),
            protected_workflow_ref=copy.deepcopy(state['workflow_ref']),queue_sha256=_hash(state['semantic_queue']),
            deadline=state['logical_time']+4)
        out['current']=dict(id=task_id,task_ref=ref(task,TASK_KIND,o))
    elif kind=='NATIVE_POLICY_FREEZE':
        need(task_id is None and root['current'] is None and not root['frozen'],'idle single freeze')
        need({x['id'] for x in c['tasks'] if x['partition']=='TRAIN'}<={x['id'] for x in root['completed']},'all train attempts before freeze')
        newpolicy=copy.deepcopy(policy);newpolicy.update(revision=policy['revision']+1,parent_ref=copy.deepcopy(root['policy_ref']),frozen=True)
        out.update(policy_ref=ref(newpolicy,POLICY_KIND,o),frozen=True)
    elif kind=='NATIVE_TASK_CANCEL':
        need(task_id is None and root['current'] is not None,'one idle open task to cancel')
        t=task_spec(c,root['current']['id'])
        out['completed'].append(dict(id=t['id'],partition=t['partition'],seq=seq,task_ref=root['current']['task_ref'],
            decision_ref=None,capture_ref=None,status='CANCELLED_UNSCORED',feature=None,action=None,reward=None,reward_quarters=None,
            policy_before=root['policy_ref'],policy_after=root['policy_ref']))
        out['current']=None
    else:need(False,'unknown task control')
    return out,task,newpolicy


def select(state,root,policy,c,task,ranking,metadata,native_target,record):
    if c.get('schema') in REFRESH_PROFILES:
        from . import compact_native_task_refresh as refresh
        return refresh.select(state,root,policy,c,task,ranking,metadata,native_target,record)
    return _select_v1(state,root,policy,c,task,ranking,metadata,native_target,record)


def _select_v1(state,root,policy,c,task,ranking,metadata,native_target,record):
    need(root['current'] is not None and task['id']==root['current']['id'] and task['policy_ref']==root['policy_ref'],'task/policy binding')
    refs=[x['entity_ref'] for x in ranking['ranked']]
    need(native_target in refs and state['logical_time']<task['deadline'],'eligible target/deadline')
    feature=None;action=None;target=native_target;reason='NATIVE_FALLBACK_NO_FEATURE';options={};ordered=[]
    if record is not None and c['condition']!='NO_MEMORY':
        memory.validate(record,origin(state));feature=record['reference_relation']
        for r in [native_target]+[x for x in refs if x!=native_target]:
            path=next(x['path'] for x in metadata if x['entry_ref']==r)
            a='FAILED_PATH' if path==record['path'] else 'OTHER_PATH'
            if a not in options:options[a]=r;ordered.append(a)
        action=next(a for a,r in options.items() if r==native_target)
        if c['condition']=='LEARNED':
            action,reason=q._choose(policy['statistics'],feature,ordered,task['ordinal'] if task['partition']=='TRAIN' else None)
            target=options[action]
        else:reason='NATIVE_FALLBACK_UNTRAINED'
    elif c['condition']=='NO_MEMORY':reason='NATIVE_FALLBACK_NO_MEMORY'
    tag=dict(schema=PROFILE,root_ref=copy.deepcopy(state['native_consumer_ref']),task_ref=copy.deepcopy(root['current']['task_ref']),
        policy_ref=copy.deepcopy(root['policy_ref']),id=task['id'],partition=task['partition'],feature=feature,action=action,
        derived_ref=copy.deepcopy(state.get('derived_observation_ref')),reason=reason,native_target=native_target,target=target,
        eligible_sha256=_hash(refs))
    return target,tag


def receive(state,root,policy,c,task,decision,item,capture_ref,workflow_ref,seq,resolve):
    if c.get('schema') in REFRESH_PROFILES:
        from . import compact_native_task_refresh as refresh
        return refresh.receive(state,root,policy,c,task,decision,item,capture_ref,workflow_ref,seq,resolve)
    validate_capture(item);tag=decision.get('native_consumer');need(tag is not None,'real task decision')
    need(state['pending']['phase']=='UNKNOWN' and tag['root_ref']==state['native_consumer_ref']
        and tag['task_ref']==root['current']['task_ref'] and tag['policy_ref']==root['policy_ref']
        and tag['id']==task['id'] and tag['partition']==task['partition'],'receipt original task/policy')
    need(decision['request']==item['request'] and decision['request']['operation']=='READ_TEXT'
        and decision['request']['entry_ref']==tag['target'] and seq==decision['source']['global_seq']+3,'one real ordered READ')
    protected=resolve(task['protected_workflow_ref'],'compact_workflow');post=resolve(workflow_ref,'compact_workflow')
    need(post['goal']==protected['goal'] and post['chains']==protected['chains']
        and _hash(state['semantic_queue'])==task['queue_sha256'],'old goal/queue commitments')
    need(post['regulation']['source']['seq']==seq and post['regulation']['source']['capture_ref']==capture_ref
        and decision['governance']['external_action_cost']==1,'actual receipt cost')
    status=item['capture']['result']['status'];parts,r4=q._reward(status)
    if tag['feature'] is None or c['condition']!='LEARNED':r4=None
    newpolicy=None;out=copy.deepcopy(root)
    if task['partition']=='TRAIN' and r4 is not None:
        need(not root['frozen'],'no evaluation training')
        newpolicy=copy.deepcopy(policy);newpolicy.update(revision=policy['revision']+1,parent_ref=root['policy_ref'],
            statistics=q._bump(policy['statistics'],tag['feature'],tag['action'],r4))
        out['policy_ref']=ref(newpolicy,POLICY_KIND,origin(state))
    out['completed'].append(dict(id=task['id'],partition=task['partition'],seq=seq,task_ref=root['current']['task_ref'],
        decision_ref=copy.deepcopy(state['pending']['decision_ref']),capture_ref=copy.deepcopy(capture_ref),status=status,
        feature=tag['feature'],action=tag['action'],reward=parts,reward_quarters=r4,
        policy_before=root['policy_ref'],policy_after=out['policy_ref']))
    out.update(current=None,parent_ref=copy.deepcopy(state['native_consumer_ref']),version=root['version']+1)
    validate(out,newpolicy if newpolicy is not None else policy,origin(state),c)
    return out,newpolicy


def preflight(root):
    if root['schema'] in REFRESH_PROFILES:
        from . import compact_native_task_refresh as refresh
        return refresh.preflight(root)
    need(len(_encode(root))+ROW_RESERVE<=ROOT_BYTES,'root completion capacity before START')


def passive_list_capacity(state,decision,max_capture_bytes):
    """005 witness-shape proof, gated to the new no-derived-memory birth."""
    schema=state['config']['native_consumer_profile']['schema']
    task_list=(schema in REFRESH_PROFILES and decision.get('native_consumer',{}).get('phase')=='LIST')
    need(schema in (PROFILE,)+REFRESH_PROFILES
        and (decision.get('inventory_acquisition') is not None or task_list) and decision['request']['operation']=='LIST_SCOPE'
        and max_capture_bytes==400000,'native causal LIST size contract')
    root=passive.validate(state['native_order_evidence']);source=decision['source'];pending=state['pending']
    need(pending is not None and pending['phase']=='REQUESTED'
        and all(source[k]==state[k] for k in ('identity','instance_id','scope_ref')),'original pre-START source')
    codec=object.__new__(LifeObjects);codec.identity=state['identity'];codec.instance_id=state['instance_id']
    codec._reference(pending['decision_ref'],'compact_decision')
    cap=dict(schema=OBJECT_SCHEMA,identity=state['identity'],instance_id=state['instance_id'],kind='compact_capture',
        encoding='json',payload_bytes=max_capture_bytes,sha256='f'*64,byte_size=max_capture_bytes+MAX_HEADER+1)
    upper=copy.deepcopy(root)
    upper['lists']=(upper['lists']+[dict(capture_ref=cap,decision_ref=copy.deepcopy(pending['decision_ref']),
        receipt_seq=source['global_seq']+3,source_seq=source['global_seq'],source_head=source['journal_head'],
        generation=source['inventory_generation']+1)])[-2:]
    result=max(len(_encode(root)),len(_encode(upper)))
    need(result<=passive.MAX_BYTES,'passive root completion before START')
    return result
