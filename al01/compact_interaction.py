"""Pure bounded synthetic-local interaction consistency, never transport.

Registry membership does not grant capabilities. The runtime must authorize the
specific action, freeze the registry at fresh birth, authenticate object refs
against its committed journal, and bind the current goal before admission.
Replies remain untrusted text and cannot grant authority or establish truth.
Only one live intent exists. UNKNOWN never authorizes another dispatch. Complete
request/outcome bodies and all duplicate/late evidence belong in immutable
objects and the outer journal, not this bounded live projection.
"""
import copy
import hashlib
import json
import re

from .core import LOCAL_ACTION_OPS, validate_local_action_request

SCHEMA = 'compact-local-interaction-1'
REQUEST_SCHEMA = 'compact-local-request-1'
OUTCOME_SCHEMA = 'compact-local-outcome-1'
REPLY_TYPE = 'untrusted_text_v1'
MAX_BYTES = 65536
MAX_INTENTS = 32
MAX_COUNTER = 2**53-1
ACTIONS = tuple(LOCAL_ACTION_OPS.values())
OUTCOMES = ('DELIVERED','RESPONDED','DENIED','UNAVAILABLE','UNKNOWN')
PENDING_STATUSES = ('QUEUED','DISPATCHED','DELIVERED','UNKNOWN')
BINDINGS = ('identity','instance_id','scope_ref')
_UNCHECKED_GOAL = object()


def need(ok, why):
    if not ok:
        raise ValueError('COMPACT_INTERACTION_INVALID: '+why)


def _keys(value, names):
    need(type(value) is dict and set(value) == set(names), 'exact fields')


def _integer(value, lo=0):
    need(type(value) is int and lo <= value <= MAX_COUNTER, 'bounded integer')


def _text(value, maximum=1024, nonempty=True):
    need(type(value) is str, 'text required')
    try:
        size = len(value.encode('utf8'))
    except UnicodeEncodeError:
        need(False, 'UTF-8 text required')
    need((1 if nonempty else 0) <= size <= maximum, 'text byte bound')


def _sha(value):
    need(type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None, 'digest')


def _json(value):
    try:
        raw = json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf8')
    except (ValueError,TypeError,UnicodeError,RecursionError):
        need(False, 'bounded JSON required')
    need(len(raw) <= MAX_BYTES, 'live projection byte budget')
    return raw


def _digest(value):
    return hashlib.sha256(_json(value)).hexdigest()


def _binding(value):
    _text(value['identity'],128)
    need(type(value['instance_id']) is str and re.fullmatch('[0-9a-f]{32}',value['instance_id']) is not None,'instance')
    need(type(value['scope_ref']) is str and re.fullmatch('scope_[0-9a-f]{32}',value['scope_ref']) is not None,'scope')


def _goal(goal):
    if goal is not None:
        _keys(goal,('id','generation'))
        _text(goal['id'],256)
        _integer(goal['generation'],1)


def _request_id(value):
    need(type(value) is str and re.fullmatch('local_[0-9a-f]{64}',value) is not None,'request correlation')


def _ref(ref, state, kind):
    # Consistency only: caller must verify these bytes in its object store/journal.
    _keys(ref,('schema','identity','instance_id','kind','encoding','payload_bytes','sha256','byte_size'))
    need(ref['schema']=='life-object-1' and ref['kind']==kind and ref['encoding']=='json','typed object reference')
    need(all(ref[k]==state[k] for k in ('identity','instance_id')),'reference identity')
    _sha(ref['sha256']);_integer(ref['payload_bytes'],1);_integer(ref['byte_size'],1)
    need(ref['payload_bytes'] <= MAX_BYTES and ref['payload_bytes'] < ref['byte_size'] <= ref['payload_bytes']+1025,'reference byte bounds')


def validate_registry(registry):
    need(type(registry) is list and len(registry)<=4,'at most four frozen synthetic endpoints')
    seen=set()
    for endpoint in registry:
        _keys(endpoint,('target','actions'))
        target=endpoint['target']
        need(type(target) is str and re.fullmatch('[a-z_][a-z0-9_]{0,127}',target) is not None,'visible EntityRef token')
        need(target not in seen,'distinct endpoints');seen.add(target)
        actions=endpoint['actions']
        need(type(actions) is list and 1<=len(actions)<=len(ACTIONS) and all(type(a) is str and a in ACTIONS for a in actions),'explicit endpoint actions')
        need(len(set(actions))==len(actions),'distinct endpoint actions')
    _json(registry)
    return copy.deepcopy(registry)


def initial_interaction(identity, instance_id, scope_ref):
    result=dict(schema=SCHEMA,identity=identity,instance_id=instance_id,scope_ref=scope_ref,
                generation=0,pending=None,last=None,completed=0,seen_intents=[])
    _binding(result)
    return result


def _outcome_refs(items, state):
    need(type(items) is list and len(items)<=len(OUTCOMES),'bounded distinct outcome references')
    seen=set()
    for item in items:
        _keys(item,('status','outcome_ref'))
        need(type(item['status']) is str and item['status'] in OUTCOMES and item['status'] not in seen,'distinct outcome identity')
        seen.add(item['status']);_ref(item['outcome_ref'],state,'compact_local_outcome')


def validate_interaction(state):
    _keys(state,('schema',*BINDINGS,'generation','pending','last','completed','seen_intents'))
    need(state['schema']==SCHEMA,'state schema');_binding(state)
    _integer(state['generation']);_integer(state['completed'])
    need(state['completed']<=state['generation'],'completed bound')
    seen=state['seen_intents']
    need(type(seen) is list and len(seen)<=MAX_INTENTS,'permanent intent capacity')
    for key in seen:_sha(key)
    need(len(set(seen))==len(seen) and state['generation']==len(seen),'permanent distinct admitted intents')
    pending=state['pending']
    if pending is not None:
        _keys(pending,('request_id','generation','deadline','goal','status','request_ref','outcomes'))
        _request_id(pending['request_id']);_integer(pending['generation'],1);_integer(pending['deadline'],1)
        _goal(pending['goal'])
        need(pending['generation']==state['generation'] and pending['status'] in PENDING_STATUSES,'live generation/status')
        _ref(pending['request_ref'],state,'compact_local_request')
        _outcome_refs(pending['outcomes'],state)
        if pending['status'] in ('QUEUED','DISPATCHED'):need(not pending['outcomes'],'unobserved dispatch phase')
    last=state['last']
    if last is not None:
        _keys(last,('request_id','generation','status','request_ref','outcome_ref','outcomes'))
        _request_id(last['request_id']);_integer(last['generation'],1)
        need(last['generation']<=state['generation'] and last['status'] in ('REPLIED','DENIED','UNAVAILABLE','EXPIRED','CANCELLED','LATE','STALE'),'terminal generation/status')
        if pending is not None:need(last['generation']<pending['generation'],'terminal precedes live request')
        _ref(last['request_ref'],state,'compact_local_request')
        _outcome_refs(last['outcomes'],state)
        if last['status'] in ('EXPIRED','CANCELLED'):
            need(last['outcome_ref'] is None,'undispatched termination is not a callback outcome')
        else:_ref(last['outcome_ref'],state,'compact_local_outcome')
    _json(state)
    return copy.deepcopy(state)


def validate_local_request(request, registry=None):
    _keys(request,('schema',*BINDINGS,'request_id','generation','action','target','request',
                   'source_sha256','created_at','deadline','expected_reply_type','goal'))
    need(request['schema']==REQUEST_SCHEMA and request['expected_reply_type']==REPLY_TYPE,'request schema/type')
    _binding(request);_request_id(request['request_id']);_integer(request['generation'],1)
    need(type(request['action']) is str and request['action'] in ACTIONS,'local action')
    need(type(request['target']) is str and re.fullmatch('[a-z_][a-z0-9_]{0,127}',request['target']) is not None,'target token')
    validate_local_action_request(request['request'])
    _sha(request['source_sha256']);_integer(request['created_at']);_integer(request['deadline'],1);_goal(request['goal'])
    need(request['created_at']<request['deadline'],'positive deadline interval')
    body={k:v for k,v in request.items() if k!='request_id'}
    need(request['request_id']=='local_'+_digest(body),'exact request content correlation')
    if registry is not None:
        entries=validate_registry(registry)
        need(any(e['target']==request['target'] and request['action'] in e['actions'] for e in entries),'registered endpoint/action')
    _json(request)
    return copy.deepcopy(request)


def semantic_intent_key(request):
    """No clocks, source snapshots or request identifiers can permit a resend."""
    request=validate_local_request(request)
    return _digest({k:request[k] for k in ('action','target','request','goal')})


def _new_intent(state, request):
    need(len(state['seen_intents'])<MAX_INTENTS,'permanent intent capacity exhausted; no eviction')
    key=semantic_intent_key(request)
    need(key not in state['seen_intents'],'semantic intent already admitted; no resend')
    return key


def make_request(state, registry, action, target, text, deadline, now, source_sha256, goal=None):
    state=validate_interaction(state)
    need(state['pending'] is None,'one pending intent/backpressure')
    request=dict(schema=REQUEST_SCHEMA,**{k:state[k] for k in BINDINGS},generation=state['generation']+1,
                 action=action,target=target,request=text,source_sha256=source_sha256,created_at=now,
                 deadline=deadline,expected_reply_type=REPLY_TYPE,goal=copy.deepcopy(goal))
    request['request_id']='local_'+_digest(request)
    request=validate_local_request(request,registry)
    _new_intent(state,request)
    return request


def queue_request(state, request, request_ref):
    state=validate_interaction(state);request=validate_local_request(request)
    need(state['pending'] is None,'one pending intent/backpressure')
    need(all(request[k]==state[k] for k in BINDINGS),'request namespace')
    need(request['generation']==state['generation']+1,'fresh generation')
    _ref(request_ref,state,'compact_local_request')
    key=_new_intent(state,request)
    state['seen_intents'].append(key)
    state['generation']=request['generation']
    state['pending']={k:copy.deepcopy(request[k]) for k in ('request_id','generation','deadline','goal')}
    state['pending'].update(status='QUEUED',request_ref=copy.deepcopy(request_ref),outcomes=[])
    return validate_interaction(state)


def mark_dispatched(state, request_id):
    state=validate_interaction(state);pending=state['pending']
    need(pending is not None and pending['request_id']==request_id and pending['status']=='QUEUED','exact queued intent only')
    pending['status']='DISPATCHED'
    return validate_interaction(state)


def validate_outcome(outcome, request):
    request=validate_local_request(request)
    _keys(outcome,('schema','request_id','endpoint','goal_generation','type','status','response','reason'))
    need(outcome['schema']==OUTCOME_SCHEMA and outcome['type']==request['expected_reply_type'],'outcome schema/type')
    need(outcome['request_id']==request['request_id'] and outcome['endpoint']==request['target'],'exact callback correlation')
    generation=None if request['goal'] is None else request['goal']['generation']
    need(type(outcome['goal_generation']) is type(generation) and outcome['goal_generation']==generation,'goal generation')
    need(type(outcome['status']) is str and outcome['status'] in OUTCOMES,'outcome status')
    if outcome['status']=='RESPONDED':_text(outcome['response'])
    else:need(outcome['response'] is None,'only a response carries reply text')
    if outcome['reason'] is not None:_text(outcome['reason'])
    _json(outcome)
    return copy.deepcopy(outcome)


def apply_outcome(state, request, outcome, request_ref, outcome_ref, now, *, current_goal=_UNCHECKED_GOAL):
    state=validate_interaction(state);request=validate_local_request(request);outcome=validate_outcome(outcome,request)
    _integer(now);need(now>=request['created_at'],'outcome clock')
    if current_goal is not _UNCHECKED_GOAL:_goal(current_goal)
    need(all(request[k]==state[k] for k in BINDINGS),'outcome namespace')
    _ref(request_ref,state,'compact_local_request');_ref(outcome_ref,state,'compact_local_outcome')
    report=dict(disposition='APPLIED',progress=False,truth=False,authority=False)
    pending=state['pending']
    if pending is None or request['request_id']!=pending['request_id']:
        need(request['generation']<=state['generation'],'unscheduled future outcome')
        last=state['last']
        report['disposition']='STALE'
        if last is not None and last['request_id']==request['request_id']:
            need(last['request_ref']==request_ref,'terminal request reference')
            prior=next((x for x in last['outcomes'] if x['status']==outcome['status']),None)
            if prior is not None:
                need(prior['outcome_ref']==outcome_ref,'conflicting same outcome identity')
                report['disposition']='DUPLICATE'
            else:
                # Retain distinct post-terminal evidence for idempotence only;
                # it cannot rewrite the terminal outcome or claim a dispatch.
                last['outcomes'].append(dict(status=outcome['status'],outcome_ref=copy.deepcopy(outcome_ref)))
        return validate_interaction(state),report
    need(pending['request_ref']==request_ref and all(pending[k]==request[k] for k in ('generation','deadline','goal')),'exact live request binding')
    need(pending['status']!='QUEUED','outcome requires committed dispatch barrier')
    prior=next((x for x in pending['outcomes'] if x['status']==outcome['status']),None)
    if prior is not None:
        need(prior['outcome_ref']==outcome_ref,'conflicting same outcome identity')
        report['disposition']='DUPLICATE'
        return state,report
    pending['outcomes'].append(dict(status=outcome['status'],outcome_ref=copy.deepcopy(outcome_ref)))
    status=outcome['status']
    disposition=None
    if current_goal is not _UNCHECKED_GOAL and request['goal'] is not None and current_goal!=request['goal']:
        disposition='STALE'
    elif now>=request['deadline']:
        disposition='LATE'
    if disposition is not None:
        report['disposition']=disposition
        if status in ('RESPONDED','DENIED','UNAVAILABLE'):
            # Explicit correlated terminal evidence resolves the pending effect,
            # but is not timely/current-goal progress and cannot permit a resend.
            state['last']=dict(request_id=request['request_id'],generation=request['generation'],status=disposition,
                request_ref=copy.deepcopy(request_ref),outcome_ref=copy.deepcopy(outcome_ref),outcomes=copy.deepcopy(pending['outcomes']))
            state['pending']=None
        else:
            pending['status']='UNKNOWN'
        return validate_interaction(state),report
    if status in ('UNKNOWN','DELIVERED'):
        if pending['status']==status:report['disposition']='DUPLICATE'
        # Delivery alone cannot resolve prior execution uncertainty.
        if pending['status']!='UNKNOWN' or status=='UNKNOWN':pending['status']=status
    else:
        terminal='REPLIED' if status=='RESPONDED' else status
        state['last']=dict(request_id=request['request_id'],generation=request['generation'],status=terminal,
                           request_ref=copy.deepcopy(request_ref),outcome_ref=copy.deepcopy(outcome_ref),outcomes=copy.deepcopy(pending['outcomes']))
        state['pending']=None
        if status=='RESPONDED':state['completed']+=1;report['progress']=True
    return validate_interaction(state),report


def cancel_queued(state, request_id):
    """Withdraw only a proven never-dispatched queued intent, never an effect."""
    state=validate_interaction(state);pending=state['pending']
    need(pending is not None and pending['request_id']==request_id and pending['status']=='QUEUED'
         and not pending['outcomes'],'only exact never-dispatched queued intent may cancel')
    state['last']=dict(request_id=pending['request_id'],generation=pending['generation'],status='CANCELLED',
                       request_ref=pending['request_ref'],outcome_ref=None,outcomes=[])
    state['pending']=None
    return validate_interaction(state)


def expire_queued(state, now):
    state=validate_interaction(state);_integer(now);pending=state['pending']
    if pending is not None and pending['status']=='QUEUED' and now>=pending['deadline']:
        state['last']=dict(request_id=pending['request_id'],generation=pending['generation'],status='EXPIRED',
                           request_ref=pending['request_ref'],outcome_ref=None,outcomes=[])
        state['pending']=None
    return validate_interaction(state)


# Descriptive aliases retained for the runtime integration handoff.
validate_local_outcome=validate_outcome
queue_interaction=queue_request
admit_interaction=apply_outcome
