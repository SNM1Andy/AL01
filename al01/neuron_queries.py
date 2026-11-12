"""Read-only snapshot queries also exposed through the registered neuron executor.

Action consequences mean recorded action/result links, not learned causality or
predictions. Only supplied hashed event history is read; no world or pending
feedback is inspected. Hash checks detect inconsistency, not malicious complete
history rewriting. NOT_FOUND means absent from this complete supplied history.
"""
import copy
import hashlib
import math
import re

from .limits import MAX_LOG_BYTES, MAX_EVENTS


def _verified_events(state,capabilities):
    """Detached full-chain validation; never consult mutable world or pending data."""
    from .core import fail,serializable,canonical,digest
    if not isinstance(capabilities,(list,tuple,set,frozenset)) or any(not isinstance(c,str) for c in capabilities):
        fail('TYPE_ERROR','snapshot capabilities')
    if {'read','neuron'}-set(capabilities):fail('CAPABILITY_DENIED','read and neuron required')
    if not isinstance(state,dict) or type(state.get('version')) is not int or not 0<=state['version']<=2**53:
        fail('TYPE_ERROR','snapshot version')
    events=state.get('events')
    if not isinstance(events,list):fail('TYPE_ERROR','snapshot events')
    if len(events)>MAX_EVENTS:fail('BUDGET_EXCEEDED','snapshot event count')
    if type(state.get('tick')) is not int or state['tick']!=len(events) or state['version']<state['tick']:
        fail('STATE_CONFLICT','snapshot count/version')
    checked=[];previous='0'*64;size=0
    try:
        for index,original in enumerate(events):
            event=serializable(original);text=canonical(event);size+=len(text.encode('utf8'))
            if size>MAX_LOG_BYTES:fail('BUDGET_EXCEEDED','snapshot scan bytes')
            if (not isinstance(event,dict) or set(event)!={'index','tick','kind','payload','previous','hash'}
                    or type(event['index']) is not int or event['index']!=index
                    or type(event['tick']) is not int or event['tick']!=index+1
                    or not isinstance(event['kind'],str) or not isinstance(event['payload'],dict)
                    or event['previous']!=previous or event['hash']!=digest({k:v for k,v in event.items() if k!='hash'})):
                fail('STATE_CONFLICT','snapshot event chain')
            previous=event['hash'];checked.append((event,text))
    except (TypeError,ValueError,OverflowError,UnicodeError,RecursionError):fail('STATE_CONFLICT','malformed snapshot event')
    return checked,size,previous


def _memory_output(module,state,inputs,result,selected,scan):
    from .contracts import NEURONS,REGISTRY_VERSION
    from .core import serializable
    spec=NEURONS[module];events,size,head=scan
    evidence=[{'ref':f"event_{e['index']}",'content_hash':e['hash'],'kind':e['kind'],'tick':e['tick']} for e in selected]
    validate_memory_record(result,spec.returns)
    return copy.deepcopy({'module':module,'module_id':spec.module_id,'module_version':spec.module_version,
        'registry_version':REGISTRY_VERSION,'api':'python_snapshot_query','input_types':list(spec.types),
        'output_type':spec.returns,'inputs':serializable(inputs),'local_state_ref':f"events@{state['version']}",
        'required_capabilities':['read','neuron'],'result':result,'evidence_refs':[e['ref'] for e in evidence],
        'resolved_evidence':evidence,'proposals':[],'compute_cost':len(events),
        'compute_usage':{'events_scanned':len(events),'bytes_scanned':size},
        'read_state_version':state['version'],'event_head':head})


def recall(state,query,limit,*,capabilities=('read','neuron')):
    """Literal case-sensitive substring recall, newest first; no semantic retrieval."""
    from .contracts import validate_type
    from .core import fail
    validate_type(query,'Text');validate_type(limit,'Int')
    if not 0<=limit<=64:fail('BUDGET_EXCEEDED','recall limit 0..64')
    scan=_verified_events(state,capabilities)
    selected=[]
    if limit:
        for event,text in reversed(scan[0]):
            if query in text:
                selected.append(event)
                if len(selected)==limit:break
    return _memory_output('recall',state,[query,limit],{'query':query,'limit':limit,'matches':selected},selected,scan)


def associate_entity(state,entity,*,capabilities=('read','neuron')):
    """Associate an exact entity token with its latest recorded observation only.

    NOT_RECORDED does not mean nonexistent. This is not identity inference,
    entity merging, learned association, or a grant of current world visibility.
    """
    from .contracts import validate_type,entity_identity
    validate_type(entity,'EntityRef');name=entity_identity(entity)
    scan=_verified_events(state,capabilities);selected=[];observation=None
    for event,_ in reversed(scan[0]):
        if event['kind']=='feedback' and event['payload'].get('target')==name:
            p=event['payload'];observation={k:p[k] for k in ('target','value','position') if k in p}
            selected=[event];break
    result={'entity':{'entity_ref':name},'status':'OBSERVED' if selected else 'NOT_RECORDED','observation':observation}
    return _memory_output('associate_entity',state,[entity],result,selected,scan)


def validate_memory_record(value,typ):
    """Structured boundary validation, not independent evidence authentication."""
    from .core import fail,serializable,canonical,digest
    from .contracts import validate_type,entity_identity
    def require(ok):
        if not ok:fail('TYPE_ERROR',typ)
    require(isinstance(value,dict))
    if typ=='EntityAssociationRecord':
        require(set(value)=={'entity','status','observation'});validate_type(value['entity'],'EntityRef')
        require(value['status'] in ('OBSERVED','NOT_RECORDED'))
        obs=value['observation']
        if value['status']=='NOT_RECORDED':require(obs is None)
        else:
            require(isinstance(obs,dict) and set(obs)=={'target','value','position'})
            require(obs['target']==entity_identity(value['entity']))
            require(type(obs['value']) in (int,float))
            try:require(math.isfinite(obs['value']))
            except OverflowError:fail('TYPE_ERROR',typ)
            require(isinstance(obs['position'],list) and len(obs['position'])==2 and all(type(x) is int and abs(x)<=2**53 for x in obs['position']))
    elif typ=='RecallRecord':
        require(set(value)=={'query','limit','matches'});validate_type(value['query'],'Text')
        require(type(value['limit']) is int and 0<=value['limit']<=64)
        require(isinstance(value['matches'],list) and len(value['matches'])<=value['limit'])
        last=MAX_EVENTS;size=0
        for e in value['matches']:
            require(isinstance(e,dict) and set(e)=={'index','tick','kind','payload','previous','hash'})
            require(type(e['index']) is int and 0<=e['index']<last and type(e['tick']) is int and e['tick']==e['index']+1)
            require(isinstance(e['kind'],str) and isinstance(e['payload'],dict))
            require(isinstance(e['previous'],str) and re.fullmatch('[0-9a-f]{64}',e['previous']) is not None)
            text=canonical(serializable(e));size+=len(text.encode('utf8'));require(size<=MAX_LOG_BYTES)
            require(value['query'] in text and e['hash']==digest({k:v for k,v in e.items() if k!='hash'}));last=e['index']
    else:fail('TYPE_ERROR','unsupported memory record')
    serializable(value)


def validate_consequence_record(value):
    """Validate the explicit structured result type, without pretending to verify evidence."""
    from .core import fail,serializable
    def require(condition):
        if not condition: fail('TYPE_ERROR','ActionConsequenceRecord')
    def position(v): return isinstance(v,list) and len(v)==2 and all(type(x) is int and abs(x)<=2**53 for x in v)
    require(isinstance(value,dict) and set(value)=={'action_id','status','proposal','consequence'})
    require(isinstance(value['action_id'],str) and re.fullmatch(r'act_[0-9a-f]{64}',value['action_id']) is not None)
    require(value['status'] in ('NOT_FOUND','AWAITING_FEEDBACK','AWAITING_RESULT','RECORDED'))
    proposal=value['proposal'];consequence=value['consequence']
    if value['status']=='NOT_FOUND': require(proposal is None and consequence is None)
    else:
        require(isinstance(proposal,dict))
        if proposal.get('kind')=='observe':
            require(value['status'] in ('AWAITING_FEEDBACK','RECORDED'))
            require(set(proposal)=={'kind','target'} and isinstance(proposal['target'],str) and bool(proposal['target']))
            if value['status']=='AWAITING_FEEDBACK': require(consequence is None)
            else:
                require(isinstance(consequence,dict) and set(consequence)=={'kind','target','value','position'})
                require(consequence['kind']=='observation' and consequence['target']==proposal['target'] and position(consequence['position']))
                require(type(consequence['value']) in (int,float))
                try: require(math.isfinite(consequence['value']))
                except OverflowError: fail('TYPE_ERROR','ActionConsequenceRecord numeric range')
        elif proposal.get('kind')=='move':
            require(set(proposal)=={'kind','dx','dy'} and all(type(proposal[k]) is int for k in ('dx','dy')))
            require(abs(proposal['dx'])+abs(proposal['dy'])==1 and value['status'] in ('AWAITING_RESULT','RECORDED'))
            if value['status']=='AWAITING_RESULT': require(consequence is None)
            else:
                require(isinstance(consequence,dict) and set(consequence)=={'kind','position'})
                require(consequence['kind']=='movement' and position(consequence['position']))
        else: fail('TYPE_ERROR','ActionConsequenceRecord proposal')
    try: serializable(value)
    except UnicodeError: fail('TYPE_ERROR','ActionConsequenceRecord UTF-8')


def lookup_action_consequences(state,action_id,*,capabilities=('read','neuron')):
    """Return bounded, evidence-linked metadata from a complete state snapshot.

    The registered action_consequences neuron uses this same evidence-checked
    implementation. It never dispatches, proposes actions, reads hidden world
    data, or infers a cause from a sign.
    """
    from .core import fail,canonical,digest,serializable
    from .contracts import REGISTRY_VERSION
    if not isinstance(capabilities,(list,tuple,set,frozenset)) or any(not isinstance(c,str) for c in capabilities):
        fail('TYPE_ERROR','snapshot query capabilities')
    if {'read','neuron'}-set(capabilities): fail('CAPABILITY_DENIED','snapshot query needs read and neuron')
    if not isinstance(action_id,str) or not re.fullmatch(r'act_[0-9a-f]{64}',action_id):
        fail('TYPE_ERROR','action ID')
    if not isinstance(state,dict) or type(state.get('version')) is not int or not 0<=state['version']<=2**53:
        fail('TYPE_ERROR','snapshot version')
    events=state.get('events')
    if not isinstance(events,list): fail('TYPE_ERROR','snapshot events')
    if len(events)>MAX_EVENTS: fail('BUDGET_EXCEEDED','snapshot query event count')
    if type(state.get('tick')) is not int or state['tick']!=len(events) or state['version']<state['tick']:
        fail('STATE_CONFLICT','snapshot event count/version')
    previous='0'*64; scanned_bytes=0; commit=None; feedback=None; movement=None; evidence=[]
    def conflict(reason): fail('STATE_CONFLICT','action history: '+reason)
    def position(value):
        return isinstance(value,list) and len(value)==2 and all(type(v) is int and abs(v)<=2**53 for v in value)
    for index,original in enumerate(events):
        try:
            event=serializable(original)
            if not isinstance(event,dict) or set(event)!={'index','tick','kind','payload','previous','hash'}:
                conflict('event fields')
            scanned_bytes+=len(canonical(event).encode('utf8'))
            if scanned_bytes>MAX_LOG_BYTES: fail('BUDGET_EXCEEDED','snapshot query scan bytes')
            if (type(event['index']) is not int or event['index']!=index or type(event['tick']) is not int
                    or event['tick']!=index+1 or event['previous']!=previous
                    or not isinstance(event['hash'],str) or not re.fullmatch(r'[0-9a-f]{64}',event['hash'])
                    or digest({k:v for k,v in event.items() if k!='hash'})!=event['hash']):
                conflict('event chain')
            previous=event['hash']
            payload=event['payload']
            if not isinstance(payload,dict) or not isinstance(event['kind'],str): conflict('event content')
            linked=False
            if event['kind']=='commit':
                ids=payload.get('action_ids',[])
                if not isinstance(ids,list) or any(not isinstance(v,str) for v in ids): conflict('action IDs')
                if action_id in ids:
                    if commit is not None or ids!=[action_id]: conflict('ambiguous action commit')
                    proposals=payload.get('proposals')
                    if not isinstance(proposals,list) or len(proposals)!=1 or not isinstance(proposals[0],dict): conflict('proposal')
                    proposal=proposals[0]
                    if proposal.get('kind')=='observe':
                        if set(proposal)!={'kind','target'} or not isinstance(proposal['target'],str) or not proposal['target']: conflict('observation proposal')
                    elif proposal.get('kind')=='move':
                        if (set(proposal)!={'kind','dx','dy'} or any(type(proposal[k]) is not int for k in ('dx','dy'))
                                or abs(proposal['dx'])+abs(proposal['dy'])!=1): conflict('movement proposal/result')
                    else: conflict('unsupported proposal')
                    request=payload.get('action_requested')
                    if request is not None:
                        if (not isinstance(request,dict) or set(request)!={'action_id','dedup_key','proposal','position','world_version','world_digest','requested_tick'}
                                or request['action_id']!=action_id or canonical(request['proposal'])!=canonical(proposal)
                                or not isinstance(request['dedup_key'],str) or not 1<=len(request['dedup_key'])<=256
                                or 'act_'+hashlib.sha256(request['dedup_key'].encode('utf8')).hexdigest()!=action_id
                                or not position(request['position']) or type(request['world_version']) is not int
                                or not 0<=request['world_version']<=2**53
                                or not isinstance(request['world_digest'],str) or not re.fullmatch(r'[0-9a-f]{64}',request['world_digest'])
                                or type(request['requested_tick']) is not int or request['requested_tick']!=event['tick']
                                or payload.get('movement') is not None): conflict('action request')
                    elif proposal['kind']=='move':
                        if not position(payload.get('movement')): conflict('legacy movement result')
                        movement={'kind':'movement','position':payload['movement']}
                    commit=payload; linked=True
            elif event['kind']=='feedback' and payload.get('action_id')==action_id:
                if commit is None or feedback is not None: conflict('orphan or duplicate feedback')
                if commit['proposals'][0]['kind']!='observe': conflict('feedback linked to non-observation')
                feedback={k:v for k,v in payload.items() if k not in ('action_id','goal_state')}
                if (set(feedback)!={'target','value','position'} or feedback['target']!=commit['proposals'][0]['target']
                        or not position(feedback['position']) or type(feedback['value']) not in (int,float)
                        or not math.isfinite(feedback['value'])): conflict('feedback result')
                linked=True
            elif event['kind']=='action_result' and payload.get('action_id')==action_id:
                if commit is None or movement is not None: conflict('orphan or duplicate movement result')
                if commit['proposals'][0]['kind']!='move' or commit.get('action_requested') is None:
                    conflict('movement result linked to incompatible commit')
                movement={k:v for k,v in payload.items() if k not in ('action_id','goal_state')}
                if set(movement)!={'kind','position'} or movement['kind']!='movement' or not position(movement['position']):
                    conflict('movement result')
                request=commit['action_requested'];proposal=commit['proposals'][0]
                if movement['position']!=[request['position'][0]+proposal['dx'],request['position'][1]+proposal['dy']]:
                    conflict('movement result position')
                linked=True
            if linked:evidence.append({'ref':f'event_{index}','content_hash':event['hash'],'kind':event['kind'],'tick':event['tick']})
        except (KeyError,TypeError,ValueError,OverflowError,UnicodeError,RecursionError):
            conflict('malformed event')
    result={'action_id':action_id,'status':'NOT_FOUND','proposal':None,'consequence':None}
    if commit is not None:
        proposal=commit['proposals'][0]; result['proposal']=proposal
        if proposal['kind']=='move':
            result.update(status='RECORDED' if movement is not None else 'AWAITING_RESULT',consequence=movement)
        elif feedback is not None:
            result.update(status='RECORDED',consequence=dict(kind='observation',**feedback))
        else: result['status']='AWAITING_FEEDBACK'
    return copy.deepcopy({'module':'action_consequences','module_id':'al01.action_consequences','module_version':'1.0.0',
        'registry_version':REGISTRY_VERSION,'api':'python_snapshot_query','input_types':['ActionId'],
        'output_type':'ActionConsequenceRecord','inputs':[action_id],'local_state_ref':f"events@{state['version']}",
        'required_capabilities':['read','neuron'],'result':result,'evidence_refs':[e['ref'] for e in evidence],
        'resolved_evidence':evidence,'proposals':[],'compute_cost':len(events),
        'compute_usage':{'events_scanned':len(events),'bytes_scanned':scanned_bytes},
        'read_state_version':state['version'],'event_head':previous})
