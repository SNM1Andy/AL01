"""Deterministic engineering development embryo, not validated general learning.

C/T/P are bounded numerical traces, not human emotions. K is a neutral sandbox
interaction statistic, never inferred affection. Hashes establish consistency,
not authenticity against a host rewriting the entire evidence history.
All APIs are pure: explicit input in, detached serializable state out.
"""
import copy
import math
from .core import fail,canonical,digest
from .neuron_queries import _verified_events,lookup_action_consequences

SCHEMA='development-embryo-1'
MAX_UPDATES=256
HISTORY=16
BIRTH={'name':'OBSERVE','origin':'preloaded_birth_template','status':'PRELOADED_UNVALIDATED','learned':False,'operation':'observe','requires':['observe']}


def _require(ok,detail):
    if not ok:fail('DEVELOPMENT_INVALID',detail)


def _text(v):return isinstance(v,str) and 1<=len(v)<=256 and not any(0xD800<=ord(c)<=0xDFFF for c in v)
def _num(v,lo,hi):return type(v) in (int,float) and lo<=v<=hi and math.isfinite(v)
def _position(p):return isinstance(p,list) and len(p)==2 and all(type(v) is int and abs(v)<=2**53 for v in p)


def spatial_feature(position):
    """Visible position sign pair relative fixed origin(0,0), nine declared bins."""
    _require(_position(position),'visible position')
    return ','.join(str((x>0)-(x<0)) for x in position)


def initial_development():
    return {'schema':SCHEMA,'version':0,'processed':[],'history':[],
            'content':{'axes':{'C':0.,'T':0.,'P':0.},'relationships':{},'working':[],
                       'episodic':[],'semantic':{},'procedural':[copy.deepcopy(BIRTH)],'experience':{}}}


def _ref(ref):
    _require(isinstance(ref,dict) and set(ref)=={'ledger','index','hash','kind'},'evidence ref fields')
    _require(_text(ref['ledger']) and type(ref['index']) is int and 0<=ref['index']<50000,'evidence identity')
    _require(isinstance(ref['hash'],str) and len(ref['hash'])==64 and all(c in '0123456789abcdef' for c in ref['hash']),'evidence hash')
    _require(ref['kind'] in ('feedback','sandbox_interaction'),'evidence kind')


def _content(c):
    _require(isinstance(c,dict) and set(c)=={'axes','relationships','working','episodic','semantic','procedural','experience'},'content fields')
    a=c['axes'];_require(isinstance(a,dict) and set(a)=={'C','T','P'} and all(_num(a[k],0,1) for k in a),'axis bounds')
    _require(canonical(c['procedural'])==canonical([BIRTH]),'birth procedure is preloaded, not learned')
    for name,limit in (('working',8),('episodic',64)):
        _require(isinstance(c[name],list) and len(c[name])<=limit,'memory budget')
    for ref in c['working']:_ref(ref)
    for item in c['episodic']:
        _require(isinstance(item,dict) and set(item)=={'ref','target','position','value','reward','feature','appraisal'},'episode')
        _ref(item['ref']);_require(item['ref']['kind']=='feedback','episode needs feedback')
        _require(_text(item['target']) and _position(item['position']) and _num(item['value'],-1e100,1e100)
            and _num(item['reward'],-1,1) and item['feature']==spatial_feature(item['position']),'episode values')
        appraisal=item['appraisal']
        _require(isinstance(appraisal,dict) and set(appraisal)=={'continuity','prediction_error','novelty_proxy','query_cost','P_target'}
                 and all(_num(v,0,1) for v in appraisal.values()) and appraisal['continuity']==1.
                 and appraisal['query_cost']==.05 and appraisal['P_target']==max(0.,appraisal['novelty_proxy']-.05),'appraisal proxy')
    _require(isinstance(c['semantic'],dict) and len(c['semantic'])<=64,'semantic budget')
    for name,item in c['semantic'].items():
        _require(_text(name) and isinstance(item,dict) and set(item)=={'value','position','count','ref'},'semantic entry')
        _require(_position(item['position']) and _num(item['value'],-1e100,1e100) and type(item['count']) is int and 1<=item['count']<=MAX_UPDATES,'semantic values');_ref(item['ref'])
        _require(item['ref']['kind']=='feedback','semantic memory needs feedback')
    _require(isinstance(c['experience'],dict) and len(c['experience'])<=9,'experience budget')
    for feature,item in c['experience'].items():
        _require(feature in {f'{x},{y}' for x in (-1,0,1) for y in (-1,0,1)},'feature')
        _require(isinstance(item,dict) and set(item)=={'count','mean','source_refs'} and type(item['count']) is int and 1<=item['count']<=MAX_UPDATES and _num(item['mean'],-1,1),'experience values')
        _require(isinstance(item['source_refs'],list) and 1<=len(item['source_refs'])<=16,'experience refs')
        for ref in item['source_refs']:_ref(ref);_require(ref['kind']=='feedback','experience needs feedback')
    _require(isinstance(c['relationships'],dict) and len(c['relationships'])<=64,'relationship budget')
    for name,item in c['relationships'].items():
        _require(_text(name) and isinstance(item,dict) and set(item)=={'K','count','source_refs'} and _num(item['K'],0,1) and type(item['count']) is int and 1<=item['count']<=MAX_UPDATES,'relationship')
        _require(isinstance(item['source_refs'],list) and 1<=len(item['source_refs'])<=16,'relationship refs')
        for ref in item['source_refs']:_ref(ref);_require(ref['kind']=='sandbox_interaction','K needs interaction evidence')


def validate_development(state):
    _require(isinstance(state,dict) and set(state)=={'schema','version','processed','history','content'} and state['schema']==SCHEMA,'state fields')
    _require(type(state['version']) is int and 0<=state['version']<=512,'version budget')
    _require(isinstance(state['processed'],list) and len(state['processed'])<=MAX_UPDATES,'processed budget')
    seen=set()
    for ref in state['processed']:
        _ref(ref);key=(ref['ledger'],ref['index']);_require(key not in seen,'duplicate processed');seen.add(key)
    _content(state['content'])
    _require(isinstance(state['history'],list) and len(state['history'])<=HISTORY,'history budget')
    previous=-1
    for row in state['history']:
        _require(isinstance(row,dict) and set(row)=={'version','content','sha256','reason'},'history fields')
        _require(type(row['version']) is int and previous<row['version']<state['version'] and _text(row['reason']),'history version')
        _content(row['content']);_require(row['sha256']==digest(row['content']),'history integrity');previous=row['version']
    def check_refs(node):
        if isinstance(node,dict):
            if set(node)=={'ledger','index','hash','kind'}:_require(node in state['processed'],'unprocessed memory evidence')
            else:
                for value in node.values():check_refs(value)
        elif isinstance(node,list):
            for value in node:check_refs(value)
    check_refs(state['content'])
    for row in state['history']:check_refs(row['content'])
    _require(len(canonical(state).encode('utf8'))<=1048576,'development byte budget')


def _evidence(state,snapshot,index,kind):
    validate_development(state)
    _require(isinstance(snapshot,dict) and _text(snapshot.get('id')),'explicit ledger ID')
    events,_,_=_verified_events(snapshot,('read','neuron'))
    _require(type(index) is int and 0<=index<len(events),'event index')
    event=events[index][0];_require(event['kind']==kind,'event kind')
    ref={'ledger':snapshot['id'],'index':index,'hash':event['hash'],'kind':kind}
    for old in state['processed']:
        if (old['ledger'],old['index'])==(ref['ledger'],ref['index']):
            _require(old==ref,'changed already processed evidence')
            return event,ref,True
    _require(len(state['processed'])<MAX_UPDATES,'update budget')
    # Processing must preserve causal order within each supplied ledger.
    _require(not any(old['ledger']==ref['ledger'] and old['index']>index for old in state['processed']),'out of order evidence')
    return event,ref,False


def _begin(state,reason):
    result=copy.deepcopy(state)
    result['history'].append({'version':state['version'],'content':copy.deepcopy(state['content']),
                              'sha256':digest(state['content']),'reason':reason})
    result['history']=result['history'][-HISTORY:];result['version']+=1
    return result


def apply_feedback(state,snapshot,event_index):
    """Only action-linked, hash-checked actual observation feedback updates P3/P4."""
    event,ref,duplicate=_evidence(state,snapshot,event_index,'feedback')
    if duplicate:return copy.deepcopy(state)
    aid=event['payload'].get('action_id');record=lookup_action_consequences(snapshot,aid)
    _require(record['result']['status']=='RECORDED' and f'event_{event_index}' in record['evidence_refs'],'linked actual feedback')
    p=record['result']['consequence'];_require(p['kind']=='observation','observation only')
    _require(_num(p['value'],-1e100,1e100),'feedback value range')
    out=_begin(state,'verified_feedback');c=out['content'];name=p['target'];feature=spatial_feature(p['position']);reward=math.tanh(p['value'])
    q=c['experience'].get(feature,{'count':0,'mean':0.,'source_refs':[]});error=reward-q['mean']
    appraisal={'continuity':1.,'prediction_error':abs(error)/2,'novelty_proxy':1/(q['count']+1),'query_cost':.05,
               'P_target':max(0.,1/(q['count']+1)-.05)}
    q={'count':q['count']+1,'mean':q['mean']+(reward-q['mean'])/(q['count']+1),'source_refs':(q['source_refs']+[ref])[-16:]}
    c['experience'][feature]=q
    # C tracks verified continuity. P is novelty-minus-cost proxy, NOT entropy/reward/happiness.
    c['axes']={'C':.9*c['axes']['C']+.1,'T':.9*c['axes']['T']+.1*abs(error)/2,
               'P':.9*c['axes']['P']+.1*appraisal['P_target']}
    c['working']=(c['working']+[ref])[-8:]
    c['episodic']=(c['episodic']+[{'ref':ref,'target':name,'position':copy.deepcopy(p['position']),
                                'value':p['value'],'reward':reward,'feature':feature,'appraisal':appraisal}])[-64:]
    if name not in c['semantic']:_require(len(c['semantic'])<64,'semantic capacity; no silent eviction')
    c['semantic'][name]={'value':p['value'],'position':copy.deepcopy(p['position']),
                         'count':c['semantic'].get(name,{}).get('count',0)+1,'ref':ref}
    out['processed'].append(ref);validate_development(out);return out


def apply_interaction(state,sandbox_snapshot,event_index):
    """Explicit sandbox outcomes only. Observation/text cannot update neutral K."""
    event,ref,duplicate=_evidence(state,sandbox_snapshot,event_index,'sandbox_interaction')
    if duplicate:return copy.deepcopy(state)
    p=event['payload'];_require(set(p)=={'sandbox','interaction_id','object_id','outcome'} and p['sandbox'] is True
        and _text(p['interaction_id']) and _text(p['object_id']) and p['outcome'] in ('cooperate','decline','harm'),'sandbox outcome')
    # An outcome ID cannot be used again at a new event index in this ledger.
    events,_,_=_verified_events(sandbox_snapshot,('read','neuron'))
    _require(sum(e['kind']=='sandbox_interaction' and e['payload'].get('interaction_id')==p['interaction_id'] for e,_ in events)==1,'duplicate interaction outcome')
    out=_begin(state,'explicit_sandbox_interaction');relations=out['content']['relationships'];name=p['object_id']
    if name not in relations:_require(len(relations)<64,'relationship capacity')
    prior=relations.get(name,{'K':.5,'count':0,'source_refs':[]});signal={'cooperate':1.,'decline':.5,'harm':0.}[p['outcome']]
    relations[name]={'K':.9*prior['K']+.1*signal,'count':prior['count']+1,'source_refs':(prior['source_refs']+[ref])[-16:]}
    out['content']['working']=(out['content']['working']+[ref])[-8:];out['processed'].append(ref)
    validate_development(out);return out


def score_adjustment(state,visible_position):
    """Bounded0.1*empirical mean; no held-out improvement or causal Q-learning claim."""
    validate_development(state);feature=spatial_feature(visible_position)
    return .1*state['content']['experience'].get(feature,{}).get('mean',0.)


def relationship_value(state,object_id):
    validate_development(state);_require(_text(object_id),'relationship object')
    return state['content']['relationships'].get(object_id,{}).get('K',.5)


def emotion_adjustment(state,*,novelty,switching):
    """Declared P/T score modifier; max absolute0.05, no human emotion semantics."""
    validate_development(state);_require(_num(novelty,0,1) and _num(switching,0,1),'emotion candidate coefficients')
    axes=state['content']['axes']
    return .025*(axes['P']*novelty-axes['T']*switching)


def rollback(state,target_version):
    """Restore bounded historical content in a NEW version; evidence stays consumed."""
    validate_development(state);_require(type(target_version) is int,'rollback version')
    row=next((r for r in state['history'] if r['version']==target_version),None)
    _require(row is not None,'version not retained')
    out=_begin(state,'rollback_to_'+str(target_version));out['content']=copy.deepcopy(row['content'])
    validate_development(out);return out
