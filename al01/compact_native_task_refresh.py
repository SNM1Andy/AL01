"""Schema2 bounded task LIST→READ. No transport, journal access or world labels."""
import copy
from . import compact_native_consumer as old
from . import compact_derived_consumer as q
from . import compact_derived_observation as memory
from .compact_observer import validate_capture
from .folder_session import _hash,_encode
from .life_objects import _json

PROFILE='native-task-refresh-read-2'
CANONICAL_PROFILE='native-five-condition-read-3'
PREFERENCE_PROFILE='native-five-condition-read-4'
SCORED_PROFILES=(CANONICAL_PROFILE,PREFERENCE_PROFILE)
PREFERENCE_CONDITIONS=('LEARNED','UNTRAINED','NO_MEMORY','FIXED_PREFERENCE_FAILED_PATH','FIXED_PREFERENCE_OTHER_PATH')
FIVE_CONDITIONS=('LEARNED','UNTRAINED','NO_MEMORY','FIXED_FAILED_PATH','FIXED_OTHER_PATH')
ROW_RESERVE=6144
PHASES=('NEED_LIST','READ_READY')


class RoleUnavailable(ValueError):
    """Actual selector diagnostics only; no request/result or retry authority."""
    def __init__(self,reason,requested,refs,options,native_target):
        super().__init__('NATIVE_CONSUMER_INVALID: '+reason+': '+requested)
        self.details=dict(reason=reason,requested_role=requested,eligible_refs=list(refs),
            options=copy.deepcopy(options),native_target=native_target)


def config(c):
    _json(c,8192)
    old.need(type(c) is dict and set(c)=={'schema','condition','tasks'} and c['schema'] in (PROFILE,)+SCORED_PROFILES,'refresh birth profile')
    supported=PREFERENCE_CONDITIONS if c['schema']==PREFERENCE_PROFILE else FIVE_CONDITIONS if c['schema']==CANONICAL_PROFILE else ('LEARNED','UNTRAINED','NO_MEMORY')
    old.need(c['condition'] in supported,'refresh birth profile/unsupported control')
    tasks=c['tasks'];old.need(type(tasks) is list,'task manifest')
    stripped=[];previous=23;phase=0
    for t in tasks:
        old.need(type(t) is dict and set(t)=={'id','partition','group','start','read_at'},'timed task fields')
        old.need(type(t['start']) is int and type(t['read_at']) is int
            and previous<t['start']<t['read_at']<=256 and t['read_at']<t['start']+4,'UNSUPPORTED_FOUR_SLOT_WINDOW')
        old.need(t['partition'] in ('TRAIN','VALIDATION','TEST'),'task partition')
        part=('TRAIN','VALIDATION','TEST').index(t['partition']);old.need(part>=phase,'partition chronology')
        phase=part;previous=t['read_at'];stripped.append({k:t[k] for k in ('id','partition','group')})
    q.draft_contract(dict(identity='validate',instance_id='0'*32,scope_ref='scope_'+'0'*32),stripped)
    if c['schema'] in SCORED_PROFILES:
        old.need(len(tasks)<=8 and sum(t['partition']=='TRAIN' for t in tasks)==4,'schema3 requires four TRAIN and at most eight tasks')
    return copy.deepcopy(c)


def initial_root(o,policy_ref,c=None):
    r=old.initial_root(o,policy_ref);r['schema']=PROFILE if c is None else c['schema'];return r


def validate(root,policy,o,c):
    config(c);_json(root,old.ROOT_BYTES);_json(policy,old.POLICY_BYTES)
    old.need(type(root) is dict and set(root)==set(initial_root(o,None)) and root['schema']==c['schema'] and root['origin']==o
        and type(root['version']) is int and root['version']>=0 and type(root['frozen']) is bool,'refresh root version/source')
    old.need(type(policy) is dict and set(policy)==set(old.initial_policy(o)) and policy['schema']=='native-nq-policy-1'
        and policy['origin']==o and policy['frozen']==root['frozen'],'refresh policy source')
    old.need(type(root['completed']) is list and len(root['completed'])<=len(c['tasks']),'refresh task count')
    p=q._policy();ids=set();seq=0
    for row in root['completed']:
        old.need(type(row) is dict and set(row)=={'id','partition','seq','task_ref','decision_ref','capture_ref','status',
            'feature','action','reward','reward_quarters','policy_before','policy_after','operation','list_decision_ref',
            'list_capture_ref','list_seq','generation','list_external_cost','read_external_cost'},'refresh evidence row')
        spec=old.task_spec(c,row['id'])
        old.need(row['partition']==spec['partition'] and row['id'] not in ids and type(row['seq']) is int and row['seq']>seq,'task chronology')
        ids.add(row['id']);seq=row['seq']
        old.need(row['operation'] in ('READ_TEXT','LIST_SCOPE','CANCELLED') and row['list_external_cost'] in (0,1)
            and row['read_external_cost']==int(row['operation']=='READ_TEXT'),'separate actual task costs')
        if row['operation']=='READ_TEXT':
            old.need(row['list_external_cost']==1 and type(row['list_seq']) is int and row['list_seq']<row['seq'],'real LIST before READ')
        else:old.need(row['reward_quarters'] is None,'LIST/cancel cannot update n/Q')
        if c['condition']=='LEARNED' and row['partition']=='TRAIN' and row['reward_quarters'] is not None:
            p=q._bump(p,row['feature'],row['action'],row['reward_quarters'])
    old.need(policy['statistics']==p,'recomputed refresh n/Q')
    cur=root['current']
    if cur is not None:
        old.need(type(cur) is dict and set(cur)=={'id','task_ref','phase','list_decision_ref','list_capture_ref','list_seq','generation'}
            and cur['id'] not in ids and cur['phase'] in PHASES,'refresh current phase')
        if cur['phase']=='NEED_LIST':old.need(all(cur[k] is None for k in ('list_decision_ref','list_capture_ref','list_seq','generation')),'no invented LIST')
        else:old.need(type(cur['list_seq']) is int and type(cur['generation']) is int
            and type(cur['list_decision_ref']) is dict and type(cur['list_capture_ref']) is dict,'real completed task LIST')
    return root,policy


def _row(root,task,seq,operation,status,decision_ref=None,capture_ref=None,feature=None,action=None,reward=None,r4=None,policy_after=None):
    cur=root['current']
    return dict(id=task['id'],partition=task['partition'],seq=seq,task_ref=cur['task_ref'],decision_ref=decision_ref,capture_ref=capture_ref,
        status=status,feature=feature,action=action,reward=reward,reward_quarters=r4,policy_before=root['policy_ref'],
        policy_after=root['policy_ref'] if policy_after is None else policy_after,operation=operation,
        list_decision_ref=cur['list_decision_ref'],list_capture_ref=cur['list_capture_ref'],list_seq=cur['list_seq'],
        generation=cur['generation'],list_external_cost=int(cur['list_capture_ref'] is not None),read_external_cost=int(operation=='READ_TEXT'))


def control(state,root,policy,c,kind,task_id,source,seq):
    o=old.origin(state);out=copy.deepcopy(root);out.update(parent_ref=copy.deepcopy(state['native_consumer_ref']),version=root['version']+1)
    task=None;newpolicy=None
    if kind=='NATIVE_TASK_BEGIN':
        old.need(root['current'] is None,'one idle refresh task')
        spec=old.task_spec(c,task_id);used={x['id'] for x in root['completed']}
        old.need(task_id not in used,'task cannot be reused')
        if spec['partition']=='TRAIN':old.need(not root['frozen'],'training after freeze prohibited')
        else:old.need(root['frozen'],'evaluation requires frozen policy')
        remaining=[t['id'] for t in c['tasks'] if t['id'] not in used]
        old.need(remaining and remaining[0]==task_id and state['logical_time']==spec['start'],'fixed task order/start')
        task=dict(schema=c['schema'],origin=o,id=task_id,partition=spec['partition'],
            ordinal=[x['id'] for x in c['tasks'] if x['partition']=='TRAIN'].index(task_id) if spec['partition']=='TRAIN' else None,
            begin_seq=seq,source=copy.deepcopy(source),policy_ref=copy.deepcopy(root['policy_ref']),
            protected_workflow_ref=copy.deepcopy(state['workflow_ref']),queue_sha256=_hash(state['semantic_queue']),
            start=spec['start'],read_at=spec['read_at'],deadline=spec['start']+4)
        out['current']=dict(id=task_id,task_ref=old.ref(task,old.TASK_KIND,o),phase='NEED_LIST',
            list_decision_ref=None,list_capture_ref=None,list_seq=None,generation=None)
    elif kind=='NATIVE_POLICY_FREEZE':
        old.need(task_id is None and root['current'] is None and not root['frozen'],'idle single freeze')
        old.need({t['id'] for t in c['tasks'] if t['partition']=='TRAIN'}<={r['id'] for r in root['completed']},'all train attempts before freeze')
        newpolicy=copy.deepcopy(policy);newpolicy.update(revision=policy['revision']+1,parent_ref=root['policy_ref'],frozen=True)
        out.update(policy_ref=old.ref(newpolicy,old.POLICY_KIND,o),frozen=True)
    elif kind=='NATIVE_TASK_CANCEL':
        old.need(task_id is None and root['current'] is not None,'one idle refresh task')
        spec=old.task_spec(c,root['current']['id']);out['completed'].append(_row(root,spec,seq,'CANCELLED','CANCELLED_UNSCORED'))
        out['current']=None
    else:old.need(False,'unknown refresh control')
    return out,task,newpolicy


def select(state,root,policy,c,task,ranking,metadata,native_target,record):
    cur=root['current'];old.need(task['id']==cur['id'] and task['policy_ref']==root['policy_ref'],'refresh task/policy')
    if cur['phase']=='NEED_LIST':
        old.need(state['logical_time']==task['start'] and native_target==state['scope_ref'] and ranking is None,'one exact start LIST')
        return native_target,dict(schema=c['schema'],root_ref=state['native_consumer_ref'],task_ref=cur['task_ref'],policy_ref=root['policy_ref'],
            id=task['id'],partition=task['partition'],feature=None,action=None,derived_ref=state.get('derived_observation_ref'),
            reason='TASK_NATIVE_LIST',native_target=native_target,target=native_target,eligible_sha256=_hash([native_target]),phase='LIST',
            list_decision_ref=None,list_capture_ref=None,list_seq=None,generation=None)
    old.need(state['logical_time']==task['read_at'] and state['inventory']['generation']==cur['generation']
        and state['inventory']['list_receipt_ref']==cur['list_capture_ref'],'READ bound to actual task LIST/time')
    if c['schema']==PREFERENCE_PROFILE:
        target,tag=select_preference(state,root,policy,c,task,ranking,metadata,native_target,record)
    elif c['schema']==CANONICAL_PROFILE:
        target,tag=select_canonical(state,root,policy,c,task,ranking,metadata,native_target,record)
    else:target,tag=old._select_v1(state,root,policy,c,task,ranking,metadata,native_target,record)
    tag.update(schema=c['schema'],phase='READ',**{k:copy.deepcopy(cur[k]) for k in ('list_decision_ref','list_capture_ref','list_seq','generation')})
    return target,tag


def exploration_role(ordinal):
    old.need(type(ordinal) is int and 0<=ordinal<4,'four fixed exploration ordinals')
    return q.ACTIONS[ordinal%2]


def select_canonical(state,root,policy,c,task,ranking,metadata,native_target,record):
    """Schema3 only: no world/seed/success labels, no expansion of legal set."""
    old.need(c['schema']==CANONICAL_PROFILE and task['id']==root['current']['id']
        and task['policy_ref']==root['policy_ref'],'canonical task/policy')
    refs=[x['entity_ref'] for x in ranking['ranked']]
    old.need(native_target in refs and state['logical_time']<task['deadline'],'native legal source/deadline')
    options={};ordered=[];feature=None;action=None;requested=None;target=native_target
    condition=c['condition'];reason='NATIVE_FALLBACK_NO_FEATURE'
    if record is not None and condition!='NO_MEMORY':
        memory.validate(record,old.origin(state));feature=record['reference_relation']
        for target_ref in [native_target]+[r for r in refs if r!=native_target]:
            path=next(m['path'] for m in metadata if m['entry_ref']==target_ref)
            role='FAILED_PATH' if path==record['path'] else 'OTHER_PATH'
            if role not in options:options[role]=target_ref;ordered.append(role)
        action=next(a for a,r in options.items() if r==native_target)
    if condition=='LEARNED':
        if task['partition']=='TRAIN':
            requested=exploration_role(task['ordinal'])
            if requested not in options:raise RoleUnavailable('EXPLORATION_ROLE_NOT_LEGAL_OR_UNBOUND',requested,refs,options,native_target)
            action=requested;target=options[action];reason='CANONICAL_ROLE_EXPLORATION'
        elif feature is not None:
            cells=policy['statistics']['cells'][feature]
            if all(cells[a]['n']>=2 for a in q.ACTIONS):
                action,reason=q._choose(policy['statistics'],feature,ordered)
                target=options[action]
            else:reason='NATIVE_FALLBACK_INCOMPLETE_FOUR_ATTEMPT_SUPPORT'
    elif condition in ('FIXED_FAILED_PATH','FIXED_OTHER_PATH'):
        requested='FAILED_PATH' if condition=='FIXED_FAILED_PATH' else 'OTHER_PATH'
        if requested not in options:raise RoleUnavailable('FIXED_ROLE_NOT_LEGAL_OR_UNBOUND',requested,refs,options,native_target)
        action=requested;target=options[action];reason=condition
    elif condition=='UNTRAINED':reason='NATIVE_FALLBACK_UNTRAINED'
    elif condition=='NO_MEMORY':reason='NATIVE_FALLBACK_NO_MEMORY'
    else:old.need(False,'unsupported canonical condition')
    return target,dict(schema=CANONICAL_PROFILE,root_ref=copy.deepcopy(state['native_consumer_ref']),
        task_ref=copy.deepcopy(root['current']['task_ref']),policy_ref=copy.deepcopy(root['policy_ref']),
        id=task['id'],partition=task['partition'],feature=feature,action=action,derived_ref=copy.deepcopy(state.get('derived_observation_ref')),
        reason=reason,native_target=native_target,target=target,eligible_sha256=_hash(refs),
        selection_version='canonical-role-choice-1',requested_role=requested,options=options,
        unavailable_roles=[a for a in q.ACTIONS if a not in options] if feature is not None else None)


def select_preference(state,root,policy,c,task,ranking,metadata,native_target,record):
    """Schema4 only. Legal preference is not schema3 strict fixed control."""
    config(c)
    old.need(c['schema']==PREFERENCE_PROFILE,'preference schema')
    condition=c['condition']
    preferred={'FIXED_PREFERENCE_FAILED_PATH':'FAILED_PATH',
               'FIXED_PREFERENCE_OTHER_PATH':'OTHER_PATH'}.get(condition)
    # Reuse immutable schema3 role binding and non-preference algorithms.
    # No synthetic history, candidate or policy observation is introduced.
    legacy=copy.deepcopy(c);legacy['schema']=CANONICAL_PROFILE
    if preferred is not None:
        old.need(record is not None,'preference requires bound history')
        legacy['condition']='UNTRAINED'
    target,tag=select_canonical(state,root,policy,legacy,task,ranking,metadata,native_target,record)
    fallback=None
    if preferred is not None:
        tag['requested_role']=preferred
        if preferred in tag['options']:
            target=tag['options'][preferred];tag['action']=preferred
            tag['reason']='FIXED_PREFERENCE_ROLE_AVAILABLE'
        else:
            # select_canonical has checked native_target is in ranked legal refs.
            target=native_target
            fallback='ROLE_NOT_IN_NATIVE_LEGAL_SET'
            tag['reason']='FIXED_PREFERENCE_NATIVE_FALLBACK'
        tag['target']=target
    tag.update(schema=PREFERENCE_PROFILE,selection_version='legal-role-preference-choice-1',
               preferred_role=preferred,actual_role=tag['action'],fallback_used=fallback is not None,fallback_reason=fallback)
    return target,tag


def receive(state,root,policy,c,task,decision,item,capture_ref,workflow_ref,seq,resolve):
    validate_capture(item);tag=decision['native_consumer'];cur=root['current'];op=decision['request']['operation']
    old.need(tag['schema']==c['schema'] and tag['root_ref']==state['native_consumer_ref'] and tag['task_ref']==cur['task_ref']
        and tag['policy_ref']==root['policy_ref'] and tag['id']==task['id'] and tag['partition']==task['partition']
        and state['pending']['phase']=='UNKNOWN' and item['request']==decision['request']
        and seq==decision['source']['global_seq']+3,'real ordered task effect')
    protected=resolve(task['protected_workflow_ref'],'compact_workflow');post=resolve(workflow_ref,'compact_workflow')
    old.need(post['goal']==protected['goal'] and post['chains']==protected['chains']
        and _hash(state['semantic_queue'])==task['queue_sha256'],'old goal/queue commitments')
    old.need(post['regulation']['source']['seq']==seq and post['regulation']['source']['capture_ref']==capture_ref
        and decision['governance']['external_action_cost']==1,'actual per-effect cost')
    out=copy.deepcopy(root);out.update(parent_ref=state['native_consumer_ref'],version=root['version']+1);newpolicy=None
    status=item['capture']['result']['status']
    if op=='LIST_SCOPE':
        old.need(cur['phase']=='NEED_LIST' and tag['phase']=='LIST' and state['logical_time']==task['start'],'one LIST phase')
        current=out['current'];current.update(list_decision_ref=state['pending']['decision_ref'],list_capture_ref=capture_ref,
            list_seq=seq,generation=state['inventory']['generation']+1)
        if status=='OK':current['phase']='READ_READY'
        else:
            out['completed'].append(_row(out,task,seq,'LIST_SCOPE',status,state['pending']['decision_ref'],capture_ref))
            out['current']=None
    else:
        old.need(op=='READ_TEXT' and cur['phase']=='READ_READY' and tag['phase']=='READ'
            and state['logical_time']==task['read_at'] and state['inventory']['generation']==cur['generation']
            and state['inventory']['list_receipt_ref']==cur['list_capture_ref']
            and all(tag[k]==cur[k] for k in ('list_decision_ref','list_capture_ref','list_seq','generation')),'one READ with task LIST provenance')
        parts,r4=q._reward(status)
        if c['schema'] not in SCORED_PROFILES and (tag['feature'] is None or c['condition']!='LEARNED'):r4=None
        if task['partition']=='TRAIN' and r4 is not None and c['condition']=='LEARNED' and tag['feature'] is not None:
            old.need(not root['frozen'],'evaluation cannot train')
            newpolicy=copy.deepcopy(policy);newpolicy.update(revision=policy['revision']+1,parent_ref=root['policy_ref'],
                statistics=q._bump(policy['statistics'],tag['feature'],tag['action'],r4))
            out['policy_ref']=old.ref(newpolicy,old.POLICY_KIND,old.origin(state))
        out['completed'].append(_row(root,task,seq,'READ_TEXT',status,state['pending']['decision_ref'],capture_ref,
            tag['feature'],tag['action'],parts,r4,out['policy_ref']))
        out['current']=None
    validate(out,newpolicy if newpolicy is not None else policy,old.origin(state),c)
    return out,newpolicy


def timing(root,task,now):
    """Returns LIST/READ/WAIT/CANCEL; contains no world/hidden law input."""
    if root['current']['phase']=='NEED_LIST':return 'LIST' if now==task['start'] else 'CANCEL'
    return 'WAIT' if now<task['read_at'] else 'READ' if now==task['read_at'] else 'CANCEL'


def preflight(root):old.need(len(_encode(root))+ROW_RESERVE<=old.ROOT_BYTES,'root completion capacity before START')
