"""Versioned read-only neuron registry and CL message envelope contracts."""
from __future__ import annotations
from dataclasses import dataclass
import copy, math

REGISTRY_VERSION = 'neurons-0.5'
@dataclass(frozen=True)
class Neuron:
    parameters: tuple
    types: tuple
    returns: str
    function: object
    module_id: str
    module_version: str = '1.0.0'
    local_state_ref: str | None = None
    required_capabilities: tuple = ('neuron',)
    compute_cost: int = 1

    @property
    def input_types(self):
        return self.types

def entity_identity(value):
    """Exact resolved ID only; no similarity, identity inference, or world lookup."""
    from .core import Ref
    return value.name if isinstance(value,Ref) else value['entity_ref']

# Only repository code can register modules. CL cannot import or mutate registry.
NEURONS = {
    'greater': Neuron(('left','right'), ('Int','Int'), 'Bool', lambda left,right: left>right, 'al01.greater'),
    'compare': Neuron(('left','right'), ('Int','Int'), 'Int', lambda left,right: (left>right)-(left<right), 'al01.compare'),
    'prediction_error': Neuron(('predicted','actual'), ('Float','Float'), 'Float', lambda predicted,actual: actual-predicted, 'al01.prediction_error'),
    'same_entity': Neuron(('left','right'), ('EntityRef','EntityRef'), 'Bool',
                          lambda left,right: entity_identity(left)==entity_identity(right), 'al01.same_entity'),
    'action_consequences': Neuron(('action_id',), ('Text',), 'ActionConsequenceRecord', None,
                                  'al01.action_consequences', local_state_ref='events',
                                  required_capabilities=('read','neuron')),
    'associate_entity': Neuron(('entity',), ('EntityRef',), 'EntityAssociationRecord', None,
                               'al01.associate_entity', local_state_ref='events', required_capabilities=('read','neuron')),
    'recall': Neuron(('query','limit'), ('Text','Int'), 'RecallRecord', None,
                     'al01.recall', local_state_ref='events', required_capabilities=('read','neuron')),
}

def execute_neuron(module,args,*,state=None,capabilities=('neuron',)):
    """Execute once and return validated result metadata for scalar or snapshot modules."""
    from .core import fail
    if not isinstance(module,str) or module not in NEURONS: fail('UNKNOWN_OPCODE','neuron module')
    spec=NEURONS[module]
    if not isinstance(args,(list,tuple)): fail('TYPE_ERROR','neuron arguments')
    validate_args(spec,args)
    if not isinstance(capabilities,(list,tuple,set,frozenset)) or any(not isinstance(c,str) for c in capabilities): fail('TYPE_ERROR','neuron capabilities')
    if set(spec.required_capabilities)-set(capabilities): fail('CAPABILITY_DENIED','neuron requirements')
    if module in ('action_consequences','associate_entity','recall'):
        from .neuron_queries import lookup_action_consequences,associate_entity,recall
        function={'action_consequences':lookup_action_consequences,'associate_entity':associate_entity,'recall':recall}[module]
        record=function(state,*args,capabilities=capabilities)
        validate_type(record['result'],spec.returns)
        record['api']='registered_neuron';record['input_types']=list(spec.types)
        return record
    result=spec.function(*args)
    version=0 if state is None else state.get('version') if isinstance(state,dict) else None
    return neuron_output(module,args,result,version,capabilities=capabilities,state=state)

def neuron_output(module, args, result, read_state_version, *, capabilities=('neuron',), evidence_refs=(), state=None):
    """Describe an already evaluated pure neuron result without evaluating it again.

    Cost is a deterministic logical operation unit, not measured CPU time. Current
    modules have no private state and cannot propose actions. Evidence is optional
    caller context, resolved against the exact read-state version when supplied.
    This validates shape/types, not authenticity or mathematical correctness.
    """
    from .core import fail,serializable
    if not isinstance(module,str) or module not in NEURONS: fail('UNKNOWN_OPCODE','neuron module')
    if module in ('action_consequences','associate_entity','recall'): fail('TYPE_ERROR','snapshot metadata requires execute_neuron and validated evidence')
    spec=NEURONS[module]
    if not isinstance(args,(list,tuple)): fail('TYPE_ERROR','neuron arguments')
    validate_args(spec,args); validate_type(result,spec.returns)
    if type(read_state_version) is not int or not 0<=read_state_version<=2**53: fail('TYPE_ERROR','read state version')
    if not isinstance(capabilities,(list,tuple,set,frozenset)) or any(not isinstance(c,str) for c in capabilities): fail('TYPE_ERROR','neuron capabilities')
    if set(spec.required_capabilities)-set(capabilities): fail('CAPABILITY_DENIED','neuron requirements')
    if not isinstance(evidence_refs,(list,tuple)) or any(not isinstance(r,str) for r in evidence_refs): fail('TYPE_ERROR','neuron evidence refs')
    if len(evidence_refs)>64: fail('BUDGET_EXCEEDED','neuron evidence refs')
    if len(set(evidence_refs))!=len(evidence_refs): fail('TYPE_ERROR','duplicate neuron evidence refs')
    if state is not None:
        if not isinstance(state,dict) or type(state.get('version')) is not int or state['version']!=read_state_version: fail('STATE_CONFLICT','neuron read state')
    if evidence_refs:
        if state is None or not isinstance(state.get('events'),list): fail('UNRESOLVED_REF','neuron evidence state required')
        from .routing import resolve_evidence
        resolved=resolve_evidence(evidence_refs,state)
    else: resolved=[]
    return copy.deepcopy({'module':module,'module_id':spec.module_id,'module_version':spec.module_version,
        'registry_version':REGISTRY_VERSION,'input_types':list(spec.input_types),'output_type':spec.returns,
        'inputs':serializable(list(args)),'local_state_ref':spec.local_state_ref,'required_capabilities':list(spec.required_capabilities),
        'result':result,'evidence_refs':list(evidence_refs),'resolved_evidence':resolved,'proposals':[],
        'compute_cost':spec.compute_cost,'read_state_version':read_state_version})

class TaskScheduler:
    """Bounded read-only dependency graph. No reentrant/self-exciting execution."""
    def __init__(self, limit=4):
        self.limit=limit
    def run(self, tasks, *, state=None, capabilities=('neuron',)):
        from .core import fail
        if len(tasks)>self.limit: fail('BUDGET_EXCEEDED','neuron tasks')
        ids=[t['id'] for t in tasks]
        if len(set(ids))!=len(ids): fail('TYPE_ERROR','duplicate task ID')
        outputs={}; remaining=copy.deepcopy(tasks)
        for _ in range(len(tasks)):
            ready=[t for t in remaining if all(d in outputs for d in t.get('depends_on',[]))]
            if not ready: break
            for t in ready:
                spec=NEURONS.get(t['module'])
                if spec is None: fail('UNKNOWN_OPCODE',t['module'])
                args=[]
                for arg in t['args']:
                    if isinstance(arg,dict) and set(arg)=={'task_ref'}:
                        if arg['task_ref'] not in t.get('depends_on',[]): fail('TYPE_ERROR','undeclared dependency')
                        arg=outputs[arg['task_ref']]
                    args.append(arg)
                result=execute_neuron(t['module'],args,state=state,capabilities=capabilities)['result']
                outputs[t['id']]=result; remaining.remove(t)
        if remaining: fail('DEPENDENCY_ERROR','missing or cyclic dependency')
        return outputs

def validate_type(value,typ):
    from .core import fail,Ref
    if typ in ('EntityAssociationRecord','RecallRecord'):
        from .neuron_queries import validate_memory_record
        try:validate_memory_record(value,typ)
        except (ValueError,TypeError,OverflowError,UnicodeError,RecursionError):fail('TYPE_ERROR',typ)
        return
    if typ=='ActionConsequenceRecord':
        from .neuron_queries import validate_consequence_record
        validate_consequence_record(value)
        return
    if typ=='Text':
        if not isinstance(value,str) or len(value)>65536: fail('TYPE_ERROR','neuron Text')
        try: value.encode('utf8')
        except UnicodeError: fail('TYPE_ERROR','neuron Text UTF-8')
        return
    if typ=='EntityRef':
        if not isinstance(value,Ref) and not (isinstance(value,dict) and set(value)=={'entity_ref'}): fail('TYPE_ERROR','neuron EntityRef')
        name=entity_identity(value)
        if not isinstance(name,str) or not 1<=len(name)<=256: fail('TYPE_ERROR','neuron EntityRef ID')
        try: name.encode('utf8')
        except UnicodeError: fail('TYPE_ERROR','neuron EntityRef UTF-8')
        return
    valid=(typ=='Bool' and type(value) is bool) or (typ=='Int' and type(value) is int and abs(value)<=2**53) or (typ=='Float' and type(value) is float and math.isfinite(value))
    if not valid: fail('TYPE_ERROR','neuron '+typ)

def validate_args(spec,args):
    from .core import fail
    if len(args)!=len(spec.types): fail('TYPE_ERROR','neuron argument count')
    for a,t in zip(args,spec.types): validate_type(a,t)

PACKET_FIELDS={'schema_version','packet_id','individual_id','logical_tick','origin_module','kind','typed_body','context_refs','provenance','uncertainty','capability_refs','trace_id'}

def validate_packet(packet,state,allowed_handles,allow_past=False):
    from .core import fail, serializable
    if not isinstance(packet,dict) or set(packet)!=PACKET_FIELDS: fail('PACKET_INVALID','fields')
    try: serializable(packet)
    except UnicodeError: fail('PACKET_INVALID','invalid UTF-8')
    if packet['schema_version']!='CL_PACKET-0.1': fail('PACKET_INVALID','version')
    if packet['individual_id']!=state['id']: fail('PACKET_INVALID','individual')
    if type(packet['logical_tick']) is not int or not 0<=packet['logical_tick']<=state['tick'] or (not allow_past and packet['logical_tick']!=state['tick']): fail('STATE_CONFLICT','packet tick')
    for key in ('packet_id','origin_module','trace_id','provenance'):
        if not isinstance(packet[key],str) or not 1<=len(packet[key])<=256: fail('PACKET_INVALID',key)
    if packet['kind'] not in ('OBSERVATION','HYPOTHESIS','QUERY','OPERATION','PROPOSAL'): fail('PACKET_INVALID','kind')
    if packet['uncertainty'] not in ('UNKNOWN','ESTIMATE','VERIFIED'): fail('PACKET_INVALID','uncertainty')
    for key in ('context_refs','capability_refs'):
        if not isinstance(packet[key],list) or len(packet[key])>64 or any(not isinstance(v,str) for v in packet[key]): fail('PACKET_INVALID',key)
    if len(set(packet['capability_refs']))!=len(packet['capability_refs']): fail('PACKET_INVALID','duplicate handles')
    if set(packet['capability_refs'])-set(allowed_handles): fail('CAPABILITY_DENIED','unissued capability handle')
    from .routing import resolve_evidence
    resolve_evidence(packet['context_refs'],state)
    body=packet['typed_body']
    if not isinstance(body,dict) or set(body)!={'source','ir'} or not isinstance(body['source'],str): fail('PACKET_INVALID','typed_body')
    return body

def validate_snapshot(data):
    """Reject structurally invalid checkpoints before constructing a live individual."""
    from .core import fail, VERSION, digest, canonical
    from .limits import MAX_LOG_BYTES, MAX_MESSAGE_BYTES, MAX_EVENTS, MAX_MESSAGES
    def require(condition,reason):
        if not condition: fail('CHECKPOINT_INVALID',reason)
    def integer(v): return type(v) is int and 0<=v<=2**53
    def position(p): return isinstance(p,list) and len(p)==2 and all(type(v) is int and abs(v)<=2**53 for v in p)
    require(isinstance(data,dict) and set(data)=={'schema','cl_version','state','world','capabilities','migrations'},'root fields')
    require(data['schema']==1 and data['cl_version']==VERSION,'version')
    require(isinstance(data['capabilities'],list) and all(isinstance(v,str) for v in data['capabilities']),'capabilities')
    require(len(set(data['capabilities']))==len(data['capabilities']) and not(set(data['capabilities'])-{'read','observe','emit','neuron','move'}),'capabilities')
    s=data['state']; w=data['world']
    fields={'id','version','world_version','tick','observed','pending','pending_action_id','requested_actions','goal_state','goal','events','avoid_positions','lifecycle','waiting','closed_loops','event_bytes','message_bytes','position','visited','inbox','packet_status','outbox','delivered','tasks','task_results'}
    require(isinstance(s,dict) and set(s)==fields,'state fields')
    g=s['goal_state']
    require(isinstance(g,dict) and set(g)=={'status','reason','no_progress','marker','updated_tick'},'goal fields')
    require(g['status'] in ('ACTIVE','WAITING','COMPLETED','BLOCKED') and isinstance(g['reason'],str) and type(g['no_progress']) is int and 0<=g['no_progress']<=3 and integer(g['updated_tick']) and integer(s['tick']) and g['updated_tick']<=s['tick'],'goal lifecycle')
    m=g['marker']
    require(isinstance(m,dict) and set(m)=={'knowledge','visited','tasks','world_version','frontier_distance'} and isinstance(m['knowledge'],str) and len(m['knowledge'])==64 and all(integer(m[k]) for k in ('visited','tasks','world_version','frontier_distance')),'goal marker')
    require((g['status']!='BLOCKED' or g['no_progress']==3),'blocked goal count')
    require(isinstance(w,dict) and set(w)=={'version','objects','local','width','height'} and integer(w['version']) and isinstance(w['objects'],dict),'world')
    require(len(w['objects'])<=10000,'world budget')
    require(type(w['local']) is bool and type(w['width']) is int and type(w['height']) is int and 1<=w['width']<=10000 and 1<=w['height']<=10000 and w['width']*w['height']<=10000,'world bounds')
    require(position(s['position']) and 0<=s['position'][0]<w['width'] and 0<=s['position'][1]<w['height'],'position')
    require(isinstance(s['visited'],list) and len(s['visited'])<=10000 and all(position(p) and 0<=p[0]<w['width'] and 0<=p[1]<w['height'] for p in s['visited']) and s['position'] in s['visited'],'visited cells')
    for key,obj in w['objects'].items():
        require(isinstance(key,str) and bool(key) and isinstance(obj,dict) and set(obj)=={'position','value'},'world object')
        require(position(obj['position']) and type(obj['value']) in (int,float) and math.isfinite(obj['value']),'world value')
    for field in ('version','world_version','tick','closed_loops'): require(integer(s[field]),field)
    require(s['version']>=s['tick'] and s['world_version']<=w['version'],'state version')
    require(isinstance(s['id'],str) and 0<len(s['id'])<=256,'identity')
    require(s['goal']=='observe_unknown' and s['lifecycle'] in ('ACTIVE','PAUSED','STOPPED') and type(s['waiting']) is bool,'lifecycle/goal')
    require(isinstance(s['events'],list) and len(s['events'])==s['tick'] and s['tick']<=MAX_EVENTS,'event count')
    require(isinstance(s['observed'],dict) and isinstance(s['pending'],list) and len(s['pending'])<=1,'memory/queue')
    require(s['closed_loops']>=len(s['observed']),'closed loop count')
    require(s['pending_action_id'] is None or (isinstance(s['pending_action_id'],str) and len(s['pending_action_id'])==68 and s['pending_action_id'].startswith('act_') and bool(s['pending'])),'pending action association')
    require(isinstance(s['avoid_positions'],list) and all(position(p) for p in s['avoid_positions']),'bias positions')
    for obs in list(s['observed'].values())+s['pending']:
        require(isinstance(obs,dict) and set(obs)=={'target','value','position'},'observation fields')
        require(isinstance(obs['target'],str) and obs['target'] in w['objects'] and position(obs['position']),'observation target')
        require(type(obs['value']) in (int,float) and math.isfinite(obs['value']),'observation value')
    for key,obs in s['observed'].items(): require(key==obs['target'],'observation key')
    previous='0'*64
    for index,event in enumerate(s['events']):
        require(isinstance(event,dict) and set(event)=={'index','tick','kind','payload','previous','hash'},'event fields')
        require(event['index']==index and type(event['index']) is int and event['tick']==index+1 and type(event['tick']) is int,'event order')
        require(event['kind'] in ('commit','feedback','packet_rejected','message_delivered','neuron_result','action_result') and isinstance(event['payload'],dict),'event content')
        require(all(isinstance(event[k],str) and len(event[k])==64 for k in ('previous','hash')),'event hash')
        require(event['previous']==previous and digest({k:v for k,v in event.items() if k!='hash'})==event['hash'],'event chain')
        previous=event['hash']

    from .actions import validate_request
    require(isinstance(s['requested_actions'],list) and len(s['requested_actions'])<=1 and not (s['requested_actions'] and s['pending']),'requested action queue')
    for request in s['requested_actions']:
        try:validate_request(request)
        except Exception:fail('CHECKPOINT_INVALID','requested action')
        require(request['requested_tick']<=s['tick'],'request tick')
        event=s['events'][request['requested_tick']-1]
        require(event['kind']=='commit' and event['payload'].get('action_requested')==request,'request commit binding')
        require(not any(e['payload'].get('action_id')==request['action_id'] for e in s['events'][request['requested_tick']:]),'completed request still queued')
    from .actions import validate_history
    try:validate_history(s)
    except Exception:fail('CHECKPOINT_INVALID','request/result history accounting')
    require(s['closed_loops']==sum(e['kind']=='feedback' for e in s['events']),'closed loop history')
    require(type(s['event_bytes']) is int and 0<=s['event_bytes']<=MAX_LOG_BYTES and s['event_bytes']==sum(len(canonical(e).encode('utf8')) for e in s['events']),'event byte accounting')
    require(type(s['message_bytes']) is int and 0<=s['message_bytes']<=MAX_MESSAGE_BYTES and s['message_bytes']==sum(len(canonical(e).encode('utf8')) for e in s['outbox']+s['delivered']),'message byte accounting')
    require(len(s['outbox'])+len(s['delivered'])<=MAX_MESSAGES,'message count')
    require(isinstance(data['migrations'],list) and len(data['migrations'])<=16,'migration receipts')
    for receipt in data['migrations']:
        require(isinstance(receipt,dict) and set(receipt)=={'source_version','target_version','source_sha256','source_event_head','tick'} and all(isinstance(receipt[k],str) for k in ('source_version','target_version','source_sha256','source_event_head')) and integer(receipt['tick']),'migration receipt')
    require(isinstance(s['inbox'],list) and len(s['inbox'])<=32 and isinstance(s['packet_status'],dict) and len(s['packet_status'])<=10000,'packet queues')
    require(isinstance(s['outbox'],list) and len(s['outbox'])<=64 and isinstance(s['delivered'],list),'message queues')
    require(isinstance(s['tasks'],list) and len(s['tasks'])<=32 and isinstance(s['task_results'],dict) and len(s['task_results'])+len(s['tasks'])<=10000,'task queues')
    ids=[]
    for packet in s['inbox']:
        try: validate_packet(packet,s,{'cap_'+c for c in data['capabilities']},allow_past=True)
        except Exception: fail('CHECKPOINT_INVALID','packet')
        ids.append(packet['packet_id'])
        require(s['packet_status'].get(packet['packet_id'])=='queued','packet status')
    require(len(set(ids))==len(ids),'duplicate packets')
    require(set(ids)=={k for k,v in s['packet_status'].items() if v=='queued'},'queued status mismatch')
    for key,status in s['packet_status'].items():
        require(isinstance(key,str) and isinstance(status,str) and (status in ('queued','committed') or status.startswith('rejected:')),'packet status')
    message_ids=[]
    for message in s['outbox']+s['delivered']:
        require(isinstance(message,dict) and set(message)=={'message_id','emitted_tick','value'},'message')
        require(isinstance(message['message_id'],str) and integer(message['emitted_tick']) and message['emitted_tick']<=s['tick'],'message fields')
        message_ids.append(message['message_id'])
    require(len(set(message_ids))==len(message_ids),'duplicate messages')
    from .routing import validate_task
    task_ids=set(s['task_results'])
    for task_id,result in s['task_results'].items():
        require(isinstance(task_id,str) and isinstance(result,dict) and result.get('status') in ('ok','error'),'task result')
        require(set(result)==({'status','value'} if result['status']=='ok' else {'status','code'}),'task result fields')
    for task in s['tasks']:
        try: validate_task(task,task_ids)
        except Exception: fail('CHECKPOINT_INVALID','task')
        task_ids.add(task['id'])
