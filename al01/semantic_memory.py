"""Provisional evidence-linked semantic memory, not automatic fact resolution.

All unequal observation claims survive. Context comparisons use ONLY committed
request metadata, never current hidden world data. Hash checks establish internal
consistency, not authenticity against a host rewriting the complete history.
"""
import copy
import math
import re
from .core import canonical,digest,fail
from .neuron_queries import _verified_events,lookup_action_consequences

SCHEMA='semantic-conflicts-1'
MAX_RECORDS=128
MAX_ENTITIES=64
MAX_CLAIMS_PER_ENTITY=8
MAX_BYTES=1048576
MAX_REPLAY_SCAN_BYTES=32*1048576


def _need(ok,message):
    if not ok:fail('SEMANTIC_MEMORY_INVALID',message)


def _text(v):return isinstance(v,str) and 1<=len(v)<=256 and not any(0xD800<=ord(c)<=0xDFFF for c in v)
def _hash(v):return isinstance(v,str) and re.fullmatch('[0-9a-f]{64}',v) is not None
def _integer(v):return type(v) is int and 0<=v<=2**53
def _claim(value):
    _need(isinstance(value,dict) and set(value)=={'value','position'},'claim fields')
    v=value['value'];_need(type(v) in (int,float) and -1e100<=v<=1e100 and math.isfinite(v),'claim finite value')
    _need(type(v) is not float or not v.is_integer(),'integral observations use canonical integer value')
    p=value['position'];_need(isinstance(p,list) and len(p)==2 and all(type(x) is int and abs(x)<=2**53 for x in p),'claim position')


def initial_semantic_memory():return {'schema':SCHEMA,'ledger_id':None,'version':0,'records':[]}


def validate_semantic_memory(memory):
    _need(isinstance(memory,dict) and set(memory)=={'schema','ledger_id','version','records'} and memory['schema']==SCHEMA,'memory schema')
    records=memory['records'];_need(isinstance(records,list) and len(records)<=MAX_RECORDS,'record budget')
    _need(type(memory['version']) is int and memory['version']==len(records),'memory version')
    _need(_text(memory['ledger_id']) if records else memory['ledger_id'] is None,'ledger binding')
    previous=-1;actions=set();entities={}
    for row in records:
        _need(isinstance(row,dict) and set(row)=={'target','claim','claim_hash','action_id','context','commit_ref','feedback_ref'},'record fields')
        _need(_text(row['target']),'target');_claim(row['claim'])
        _need(row['claim_hash']==digest(row['claim']),'claim integrity')
        aid=row['action_id'];_need(isinstance(aid,str) and re.fullmatch('act_[0-9a-f]{64}',aid) is not None and aid not in actions,'unique action')
        actions.add(aid)
        for name in ('commit_ref','feedback_ref'):
            ref=row[name];_need(isinstance(ref,dict) and set(ref)=={'index','hash'} and _integer(ref['index']) and ref['index']<50000 and _hash(ref['hash']),'event ref')
        ci=row['commit_ref']['index'];fi=row['feedback_ref']['index']
        _need(ci<fi and fi>previous,'evidence order');previous=fi
        context=row['context']
        _need(context is None or isinstance(context,dict) and set(context)=={'world_version','world_digest'}
              and _integer(context['world_version']) and _hash(context['world_digest']),'world context')
        entities.setdefault(row['target'],set()).add(row['claim_hash'])
    _need(len(entities)<=MAX_ENTITIES and all(len(v)<=MAX_CLAIMS_PER_ENTITY for v in entities.values()),'entity/claim budget')
    _need(len(canonical(memory).encode('utf8'))<=MAX_BYTES,'semantic byte budget')


def apply_observation(memory,snapshot,event_index):
    """Apply one actual linked feedback; duplicate evidence is an exact no-op."""
    validate_semantic_memory(memory)
    _need(isinstance(snapshot,dict) and _text(snapshot.get('id')),'source ledger identity')
    _need(memory['ledger_id'] in (None,snapshot['id']),'different ledger requires separate memory')
    events,_,_=_verified_events(snapshot,('read','neuron'))
    _need(type(event_index) is int and 0<=event_index<len(events),'feedback index')
    event=events[event_index][0];_need(event['kind']=='feedback','only observation feedback admitted')
    for row in memory['records']:
        if row['feedback_ref']['index']==event_index:
            _need(row['feedback_ref']['hash']==event['hash'],'changed consumed evidence')
            return copy.deepcopy(memory)
    _need(not memory['records'] or event_index>memory['records'][-1]['feedback_ref']['index'],'out of order evidence')
    _need(len(memory['records'])<MAX_RECORDS,'record capacity; no silent eviction')
    aid=event['payload'].get('action_id');result=lookup_action_consequences(snapshot,aid)
    _need(result['result']['status']=='RECORDED' and f'event_{event_index}' in result['evidence_refs'],'linked recorded result')
    observation=result['result']['consequence'];_need(observation['kind']=='observation','observation consequence')
    commits=[e for e,_ in events if e['kind']=='commit' and aid in e['payload'].get('action_ids',[])]
    _need(len(commits)==1,'single source commit');commit=commits[0]
    request=commit['payload'].get('action_requested')
    context=None if request is None else {k:request[k] for k in ('world_version','world_digest')}
    value=observation['value']
    # Exact normalization only:1 and1.0 describe the same measured numeric value.
    # Original representation remains recoverable from the hashed source event.
    if type(value) is float and math.isfinite(value) and value.is_integer():value=int(value)
    claim={'value':value,'position':copy.deepcopy(observation['position'])};_claim(claim)
    row={'target':observation['target'],'claim':claim,'claim_hash':digest(claim),'action_id':aid,'context':context,
         'commit_ref':{'index':commit['index'],'hash':commit['hash']},'feedback_ref':{'index':event_index,'hash':event['hash']}}
    out=copy.deepcopy(memory);out['ledger_id']=snapshot['id'];out['records'].append(row);out['version']+=1
    validate_semantic_memory(out);return out


def _relation(earlier,later):
    a=earlier['context'];b=later['context']
    if a is None or b is None:return 'CONTEXT_UNKNOWN'
    # Same complete-world digest is stronger than a mere version label: no
    # recorded world-content change explains unequal observations.
    if a['world_digest']==b['world_digest']:return 'CONTRADICTORY_EVIDENCE'
    if a['world_version']==b['world_version']:return 'WORLD_CONTEXT_IDENTITY_CONFLICT'
    if b['world_version']>a['world_version']:return 'DIFFERENT_WORLD_CONTEXTS'
    return 'CONTEXT_ORDER_UNRESOLVED'


def query_entity(memory,entity_id,*,world_context=None):
    """Retain every distinct claim and pairwise context assessment, never latest-wins.

    CHANGED_WORLD_CONTEXT means recorded version+digest differ, not external truth
    or a causal mechanism. An explicit exact context may narrow provisional use;
    no automatic latest-context selection or numeric confidence calibration.
    """
    validate_semantic_memory(memory);_need(_text(entity_id),'query entity')
    if world_context is not None:
        _need(isinstance(world_context,dict) and set(world_context)=={'world_version','world_digest'}
              and _integer(world_context['world_version']) and _hash(world_context['world_digest']),'explicit query context')
    rows=[r for r in memory['records'] if r['target']==entity_id];groups={}
    for row in rows:
        group=groups.setdefault(row['claim_hash'],{'claim_hash':row['claim_hash'],'claim':copy.deepcopy(row['claim']),'evidence':[]})
        group['evidence'].append({k:copy.deepcopy(row[k]) for k in ('action_id','context','commit_ref','feedback_ref')})
    relations=[]
    for i,first in enumerate(rows):
        for second in rows[i+1:]:
            if first['claim_hash']==second['claim_hash']:continue
            relations.append({'earlier_feedback':first['feedback_ref']['index'],'later_feedback':second['feedback_ref']['index'],
                              'assessment':_relation(first,second)})
    assessments={r['assessment'] for r in relations}
    if not rows:status='NOT_RECORDED'
    elif len(groups)==1:status='CONSISTENT_RECORDED_CLAIM'
    elif 'CONTRADICTORY_EVIDENCE' in assessments:status='CONTRADICTORY_EVIDENCE'
    elif 'WORLD_CONTEXT_IDENTITY_CONFLICT' in assessments:status='WORLD_CONTEXT_IDENTITY_CONFLICT'
    elif assessments=={'DIFFERENT_WORLD_CONTEXTS'}:status='CHANGED_WORLD_CONTEXT'
    else:status='UNRESOLVED_CONFLICT'
    scoped=[r for r in rows if world_context is None or r['context']==world_context]
    scoped_claims={r['claim_hash'] for r in scoped};eligible=len(scoped_claims)==1
    result={'entity_id':entity_id,'ledger_id':memory['ledger_id'],'memory_version':memory['version'],
            'status':status,'claims':list(groups.values()),'relations':relations,'selected_claim':None,
            'requested_context':copy.deepcopy(world_context),'eligible_for_provisional_use':eligible,
            'provisional_claim':copy.deepcopy(scoped[0]['claim']) if eligible else None,
            'provisional_confidence':{'support_count':len(scoped),'conflicting_claim_count':max(0,len(scoped_claims)-1),
                'calibrated_probability':None,'basis':'recorded action-linked evidence, not verified external truth'},
            'precondition':'PROVISIONAL_EVIDENCE_AVAILABLE' if eligible else 'REFUSE_AMBIGUOUS_OR_MISSING',
            'resolution':'NO_AUTOMATIC_FACT_RESOLUTION','provisional':True}
    _need(len(canonical(result).encode('utf8'))<=MAX_BYTES,'query result byte budget')
    return result


def replay_semantic_memory(snapshot):
    """Rebuild from complete retained history under an explicit aggregate scan cap."""
    events,size,_=_verified_events(snapshot,('read','neuron'))
    indices=[e['index'] for e,_ in events if e['kind']=='feedback']
    _need(len(indices)<=MAX_RECORDS and size*(1+2*len(indices))<=MAX_REPLAY_SCAN_BYTES,'replay work budget')
    memory=initial_semantic_memory()
    for index in indices:memory=apply_observation(memory,snapshot,index)
    return memory


def validate_semantic_replay(memory,snapshot):
    """Verify complete-history memory equals deterministic evidence replay."""
    validate_semantic_memory(memory)
    _need(canonical(memory)==canonical(replay_semantic_memory(snapshot)),'semantic replay mismatch')
