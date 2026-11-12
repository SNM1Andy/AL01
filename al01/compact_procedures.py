"""Finite empirical conflict-inspection procedures; no general induction.

All claims remain unverified. Credits/pressure are engineering regulation,
not biological resources, emotions or permission grants.
"""
import copy
from .folder_session import _hash,_encode
from .core import Compiler,CVM,transaction_payload
from .compact_observer import validate_capture

SCHEMA='compact-investigation-workflow-3'
FAMILY='INSPECT_CONFLICT_PAIR'
ROLES=('EARLIER_SOURCE','LATER_SOURCE')
ROLE_TOKENS=dict(EARLIER_SOURCE='earlier_source',LATER_SOURCE='later_source')
MAX_WORKFLOW_BYTES=65536
MAX_CANDIDATE_BYTES=24576
MAX_CAPSULE_BYTES=4*1024*1024
MAX_CAPSULE_EVENTS=64
MAX_CAPSULE_OBJECTS=64
REGULATOR='engineering-inspection-regulator-1'
CAPACITY=4
CASES=(('success',41),('reversed',73),('denied',97),('stale',101),('counterevidence',131),('retention',149))
TEST_PROTOCOL=dict(schema='conflict-pair-validation-1',cases=[dict(case=c,seed=s) for c,s in CASES],
    criterion='Legal bounded acquisition and honest stop, never proposition truth',
    fixture_version='native-conflict-fixtures-1',evaluator_version='closed-procedure-reducer-evaluator-1',
    expectations=dict(success='Earlier source must be refreshed before later source becomes available; inspection must complete',
        reversed='Stable sources with reversed lexical/provenance order; inspection must complete',
        denied='First procedure read denied; goal must block without retry',stale='Later source removed by refreshed LIST; no procedure read may follow',
        counterevidence='New conflicting declarations during inspection; retain honest unresolved assessment',retention='Ordinary non-claim monitoring remains executable without procedure hijack'),
    max_effects_per_condition=6,max_logical_time=2,
    authority='TRUSTED_CONTROLLER_SYNTHETIC_EVIDENCE_NOT_HOST_AUTHENTICATION',max_events=MAX_CAPSULE_EVENTS)

def need(ok,why):
    if not ok:raise ValueError('COMPACT_PROCEDURE_INVALID: '+why)

def initial_workflow(probe=None):
    if probe is not None:validate_candidate(probe)
    return dict(schema=SCHEMA,generation=0,goal=None,last_goal=None,trace=[],chains=[],candidate_ref=None,candidate_archive=[],help=None,appraisal=None,interaction=None,typed_wait=None,typed_wait_count=0,
        active_ref=None,previous_ref=None,active_spec=None,previous_spec=None,active_evaluation_ref=None,previous_evaluation_ref=None,
        validation=None,probe=copy.deepcopy(probe),
        regulation=dict(version=REGULATOR,credits=CAPACITY,last_time=0,denial_pressure=0.,source=None,
                        K=.5,K_missing=True,K_evidence_count=0),
        waits=0,goal_transitions=0,last_wait=None)

def available(workflow,now):
    r=workflow['regulation'];need(type(now) is int and now>=r['last_time'],'monotonic regulation clock')
    return min(CAPACITY,r['credits']+now-r['last_time'])

def validate_workflow(w):
    need(type(w) is dict and set(w)==set(initial_workflow()) and w['schema']==SCHEMA,'workflow fields')
    if w['help'] is not None:
        from .compact_help import validate_help
        validate_help(w['help'])
    for key in ('generation','waits','goal_transitions','typed_wait_count'):need(type(w[key]) is int and 0<=w[key]<2**53,'workflow counter')
    wait=w['typed_wait']
    if wait is not None:
        need(type(wait) is dict and set(wait)=={'ref','scope','wake_at','deadline','status','settled_at'},'typed wait projection')
        need(wait['status'] in ('WAITING','WOKEN','TIMED_OUT','STALE'),'typed wait status')
        scope=wait['scope'];need(type(scope) is dict,'typed wait scope')
        if scope.get('kind')=='monitor':
            from .folder_contracts import validate_relative_path
            need(set(scope)=={'kind','path'},'typed wait monitor fields');validate_relative_path(scope['path'])
        else:
            need(set(scope)=={'kind','id','generation'} and scope['kind']=='goal' and type(scope['id']) is str and 1<=len(scope['id'])<=256
                 and type(scope['generation']) is int and 1<=scope['generation']<2**53,'typed wait goal fields')
        need(type(wait['wake_at']) is int and type(wait['deadline']) is int and 0<wait['wake_at']<wait['deadline']<2**53,'typed wait clock')
        need((wait['settled_at'] is None)==(wait['status']=='WAITING'),'one-time wait settlement')
        if wait['settled_at'] is not None:need(type(wait['settled_at']) is int and 0<=wait['settled_at']<2**53,'wait settlement clock')
    need(type(w['trace']) is list and len(w['trace'])<=2 and type(w['chains']) is list and len(w['chains'])<=2,'bounded extraction window')
    need(type(w['candidate_archive']) is list and len(w['candidate_archive'])<=4,'four admitted candidate references')
    if w['appraisal'] is not None:
        from .compact_regulation import validate_regulation
        validate_regulation(w['appraisal'])
    if w['interaction'] is not None:
        from .compact_interaction import validate_interaction
        validate_interaction(w['interaction'])
    r=w['regulation'];need(type(r) is dict and set(r)==set(initial_workflow()['regulation']) and r['version']==REGULATOR,'regulation schema')
    need(type(r['credits']) is int and 0<=r['credits']<=CAPACITY and type(r['last_time']) is int and 0<=r['last_time']<2**53,'engineering credits')
    need(type(r['denial_pressure']) is float and 0<=r['denial_pressure']<=1 and type(r['K']) is float and r['K']==.5 and r['K_missing'] is True
         and type(r['K_evidence_count']) is int and r['K_evidence_count']==0,'sourced non-social regulator')
    goal=w['goal']
    if goal is not None:
        fields={'id','generation','topic','bindings','status','reason','cursor','receipts','procedure_ref','episode_key','assessment'}
        need(type(goal) is dict and fields<=set(goal)<=fields|{'admission_context','receipt_provenance'},'goal fields')
        if 'admission_context' in goal:
            from .compact_guarded_procedures import validate_context
            validate_context(goal['admission_context'])
        need(goal['status'] in ('ACTIVE','PAUSED','WAITING','BLOCKED','SATISFIED','CANCELLED') and type(goal['cursor']) is int and 0<=goal['cursor']<=2,'goal status/cursor')
        need(type(goal['receipts']) is list and len(goal['receipts'])==goal['cursor'],'receipt-driven cursor')
        if 'receipt_provenance' in goal:
            steps=goal['receipt_provenance']
            need(type(steps) is list and len(steps)==goal['cursor'],'bounded actual receipt provenance')
            need(all(type(s) is dict and set(s)=={'decision_ref','capture_ref','receipt_seq','role'}
                     and s['role'] in ROLES and type(s['receipt_seq']) is int and s['receipt_seq']>0 for s in steps),'receipt provenance fields')
            need([s['capture_ref'] for s in steps]==goal['receipts'] and len({s['role'] for s in steps})==len(steps),'distinct actual role receipts')
        need(set(goal['bindings'])==set(ROLES) and len(set(goal['bindings'].values()))==2,'two source roles')
        need(goal['generation']<=w['generation'] and goal['assessment'] in ('NOT_INSPECTED','INSPECTED_UNRESOLVED','BLOCKED_UNRESOLVED'),'inspection is not truth')
    need(len(_encode(w))<=MAX_WORKFLOW_BYTES,'workflow byte budget')
    return copy.deepcopy(w)

def context(index,topic,goals):
    rows=[r for r in index['records'] if r['topic']==topic]
    first={}
    for row in rows:first.setdefault(row['path'],row['receipt_seq'])
    if len(first)!=2:return None
    paths=sorted(first,key=lambda p:(first[p],p));names=[g['path'] for g in goals]
    if not all(p in names for p in paths):return None
    investigation=index['investigations'].get(topic)
    if investigation is None:return None
    return dict(topic=topic,bindings=dict(zip(ROLES,[names.index(p) for p in paths])),
                episode_key=_hash([topic,investigation['epoch'],investigation['rounds']]))

def start_goal(w,ctx,identity,procedure_ref=None,*,track_receipts=False):
    w=validate_workflow(w)
    need(ctx is not None,'two distinct monitored sources required')
    need(w['goal'] is None or w['goal']['status'] in ('BLOCKED','SATISFIED','CANCELLED'),'unfinished investigation')
    w['generation']+=1;w['goal_transitions']+=1;w['last_goal']=copy.deepcopy(w['goal'])
    w['goal']=dict(id=identity+':goal:'+str(w['generation']),generation=w['generation'],topic=ctx['topic'],
        bindings=copy.deepcopy(ctx['bindings']),status='ACTIVE',reason=None,cursor=0,receipts=[],procedure_ref=procedure_ref,
        episode_key=ctx['episode_key'],assessment='NOT_INSPECTED')
    if track_receipts and procedure_ref is None:w['goal']['receipt_provenance']=[]
    w['trace']=[];w['last_wait']=None
    return validate_workflow(w)

def goal_control(w,operation):
    w=validate_workflow(w);g=w['goal'];need(g is not None,'goal required')
    if operation=='PAUSE':need(g['status'] in ('ACTIVE','WAITING'),'pause active goal');g['status']='PAUSED'
    elif operation=='RESUME':need(g['status'] in ('PAUSED','WAITING'),'resume paused/waiting goal');g['status']='ACTIVE'
    elif operation=='CANCEL':need(g['status'] not in ('SATISFIED','CANCELLED'),'cancel unfinished goal');g['status']='CANCELLED'
    else:need(False,'unknown goal control')
    g['reason']=operation;w['trace']=[];w['goal_transitions']+=1
    return validate_workflow(w)

def validate_candidate(c):
    if type(c) is dict and c.get('schema')=='compact-guarded-procedure-candidate-1':
        from .compact_guarded_procedures import validate_candidate as guarded_validate
        return guarded_validate(c)
    need(type(c) is dict and set(c)=={'schema','family','version','origin','source_workflow_ref','chains','order','templates','ir','preconditions','cost','termination','failure','authority'},'candidate fields')
    need(c['schema']=='compact-procedure-candidate-1' and c['family']==FAMILY and c['version']==1 and c['authority']=='NONE','finite family/version')
    need(type(c['order']) is list and len(c['order'])==2 and set(c['order'])==set(ROLES),'learned role permutation')
    expected=[f'PROPOSE(OBSERVE(@{ROLE_TOKENS[role]}));' for role in c['order']]
    need(c['templates']==expected and c['ir']==[Compiler(ROLE_TOKENS.values(),('read','observe')).compile(t) for t in expected],'executable typed CL templates')
    need(c['preconditions']==['CURRENT_VISIBLE_SOURCES','READ_OBSERVE_CAPABILITIES','SAME_CONFLICT_EPISODE','NOT_ALREADY_INSPECTED'],'fixed preconditions')
    need(c['cost']==dict(actions=2,logical_units=10) and c['termination']=='TWO_MATCHING_RECEIPTS_INSPECTED_UNRESOLVED' and c['failure']=='BLOCK_AND_RETAIN_EVIDENCE','cost/termination/failure')
    need(type(c['chains']) is list and len(c['chains'])==2 and len(_encode(c))<=MAX_CANDIDATE_BYTES,'two bounded source chains')
    return copy.deepcopy(c)

def executable_spec(candidate):
    validate_candidate(candidate)
    if candidate['schema']=='compact-guarded-procedure-candidate-1':
        return {k:copy.deepcopy(candidate[k]) for k in ('profile','branches')}
    return {k:copy.deepcopy(candidate[k]) for k in ('order','templates','ir')}

def admitted_orders(w,workflow_ref,origin,resolve):
    """Shared unchanged source proof checks; returns chronological permutations."""
    w=validate_workflow(w);need(len(w['chains'])==2,'two successful admitted chains required')
    need(resolve(workflow_ref,'compact_workflow')==w,'source workflow differs from archived admitted checkpoint')
    need(type(origin) is dict and set(origin)=={'identity','instance_id','scope_ref'},'source origin fields')
    need(all(workflow_ref[k]==origin[k] for k in ('identity','instance_id')),'workflow origin namespace')
    orders=[];seen=set()
    for chain in w['chains']:
        need(len(chain['steps'])==2 and chain['episode_key'] not in seen,'distinct two-step source chains');seen.add(chain['episode_key'])
        order=[]
        prior_seq=-1;goal_id=None
        for cursor,step in enumerate(chain['steps']):
            d=resolve(step['decision_ref'],'compact_decision');item=resolve(step['capture_ref'],'compact_capture');validate_capture(item)
            need(item['request']==d['request'] and item['capture']['result']['status']=='OK','actual successful committed request/result')
            need(d['investigation']['episode_key']==chain['episode_key'] and step['role']==d['investigation']['role'],'source role binding')
            need(all(d['source'][k]==origin[k] for k in origin),'decision origin binding')
            need(type(step['receipt_seq']) is int and step['receipt_seq']==d['source']['global_seq']+3 and step['receipt_seq']>prior_seq,'chronological committed receipt sequence')
            need(d['investigation']['cursor']==cursor and (goal_id is None or d['investigation']['goal_id']==goal_id),'ordered source goal cursor')
            goal_id=d['investigation']['goal_id'];prior_seq=step['receipt_seq']
            vm=CVM(('read','observe'));tx=vm.prepare(d['generated_cl'],dict(version=d['source']['cognition_revision'],world_version=d['source']['inventory_generation']),[d['request']['entry_ref']]);vm.verify(tx)
            # Recheck the whole staged result, including IR/dictionary/registry
            # versions and read versions, not only the final action target.
            # Canonical JSON also distinguishes Boolean and numeric aliases.
            need(_encode(transaction_payload(tx))==_encode(d['transaction_payload']),'source CL transaction payload')
            need(d['generated_cl']==f"PROPOSE(OBSERVE(@{d['request']['entry_ref']}));",'finite exact source grammar')
            order.append(step['role'])
        need(set(order)==set(ROLES),'both source roles inspected');orders.append(order)
    return orders

def extract(w,workflow_ref,origin,resolve):
    orders=admitted_orders(w,workflow_ref,origin,resolve)
    need(orders[0]==orders[1],'consistent empirically observed order')
    templates=[f'PROPOSE(OBSERVE(@{ROLE_TOKENS[role]}));' for role in orders[0]]
    c=dict(schema='compact-procedure-candidate-1',family=FAMILY,version=1,origin=copy.deepcopy(origin),source_workflow_ref=workflow_ref,
        chains=copy.deepcopy(w['chains']),order=orders[0],templates=templates,ir=[Compiler(ROLE_TOKENS.values(),('read','observe')).compile(t) for t in templates],
        preconditions=['CURRENT_VISIBLE_SOURCES','READ_OBSERVE_CAPABILITIES','SAME_CONFLICT_EPISODE','NOT_ALREADY_INSPECTED'],cost=dict(actions=2,logical_units=10),
        termination='TWO_MATCHING_RECEIPTS_INSPECTED_UNRESOLVED',failure='BLOCK_AND_RETAIN_EVIDENCE',authority='NONE')
    return validate_candidate(c)

def receive(w,d,item,decision_ref,capture_ref,seq,now):
    w=validate_workflow(w);validate_capture(item)
    source=dict(seq=seq,receipt_sha256=item['receipt_sha256'],capture_ref=capture_ref,decision_sha256=_hash(d))
    previous=w['regulation']['source']
    if previous is not None:
        if seq==previous['seq']:
            need(source==previous,'same regulation event with altered evidence');return w
        need(type(seq) is int and seq>previous['seq'],'monotonic regulation evidence')
    need(available(w,now)>=1,'admitted engineering credit')
    r=w['regulation'];ok=item['capture']['result']['status']=='OK'
    r.update(credits=available(w,now)-1,last_time=now,denial_pressure=.9*r['denial_pressure']+.1*float(not ok),
             source=source)
    info=d.get('investigation');g=w['goal']
    if info is None or not ok:
        w['trace']=[]
        if info is not None and g is not None and info['goal_id']==g['id']:
            g.update(status='BLOCKED',reason='DENIED_OR_FAILED_SOURCE',assessment='BLOCKED_UNRESOLVED');w['goal_transitions']+=1
        return validate_workflow(w)
    step=dict(decision_ref=decision_ref,capture_ref=capture_ref,receipt_seq=seq,role=info['role'])
    if g is not None and g['status'] in ('ACTIVE','WAITING') and info['goal_id']==g['id']:
        need(info['cursor']==g['cursor'] and info['episode_key']==g['episode_key'],'matching receipt advances current cursor')
        if 'receipt_provenance' in g:
            need(info['role'] in ROLES and info['role'] not in [s['role'] for s in g['receipt_provenance']], 'no duplicate investigation role completion')
            g['receipt_provenance'].append(copy.deepcopy(step))
        g['receipts'].append(capture_ref);g['cursor']+=1;g['status']='ACTIVE';g['reason']=None
        if g['cursor']==2:g.update(status='SATISFIED',reason='INSPECTION_COMPLETE_NOT_TRUTH',assessment='INSPECTED_UNRESOLVED');w['goal_transitions']+=1
    if w['trace'] and (w['trace'][0]['episode_key']!=info['episode_key'] or w['trace'][0]['step']['role']==info['role']):w['trace']=[]
    w['trace'].append(dict(episode_key=info['episode_key'],step=step))
    if len(w['trace'])==2:
        chain=dict(episode_key=info['episode_key'],steps=[x['step'] for x in w['trace']])
        if g is not None and g['episode_key']==info['episode_key'] and 'admission_context' in g:
            chain['admission_context']=copy.deepcopy(g['admission_context'])
        w['chains']=(w['chains']+[chain])[-2:];w['trace']=[]
    return validate_workflow(w)
