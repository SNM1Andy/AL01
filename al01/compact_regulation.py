"""Bounded engineering C/T/P candidates, not subjective emotion or truth.

Pure consistency checks do not authenticate inputs. The runtime must derive and
replay evidence from actual committed sources. P is an expected information
proxy, never verified information gain. No clock-only update API exists.
"""
import copy
import math
import re
from .folder_session import _encode,_hash

VERSION='compact-evidence-regulation-1'
ALPHA=.05
MAX_BYTES=8192
MAX_COUNTER=2**53-1


def need(ok,why):
    if not ok:raise ValueError('COMPACT_REGULATION_INVALID: '+why)

def keys(value,names):need(type(value) is dict and set(value)==set(names),'exact schema')
def integer(value,lo=0,hi=MAX_COUNTER):need(type(value) is int and lo<=value<=hi,'integer range')
def number(value,lo=0,hi=1):need(type(value) in (int,float) and math.isfinite(value) and lo<=value<=hi,'finite numeric range')
def sha(value):need(type(value) is str and re.fullmatch('[0-9a-f]{64}',value) is not None,'digest')
def binding(identity,instance_id,scope_ref):
    need(type(identity) is str and 0<len(identity.encode('utf8'))<=128,'identity')
    need(type(instance_id) is str and re.fullmatch('[0-9a-f]{32}',instance_id) is not None,'instance')
    need(type(scope_ref) is str and re.fullmatch('scope_[0-9a-f]{32}',scope_ref) is not None,'scope')


def initial_regulation(identity,instance_id,scope_ref):
    binding(identity,instance_id,scope_ref)
    return dict(version=VERSION,identity=identity,instance_id=instance_id,scope_ref=scope_ref,
        C=.5,T=.5,P=.5,K=.5,K_missing=True,K_evidence_count=0,
        missing=dict(C=True,T=True,P=True),updates=0,last=None)


def _evidence(e):
    keys(e,('identity','instance_id','scope_ref','seq','receipt_sha256','continuity','conflict','prediction','opportunity'))
    binding(e['identity'],e['instance_id'],e['scope_ref']);integer(e['seq'],1);sha(e['receipt_sha256'])
    c=e['continuity']
    if c is not None:
        keys(c,('request_bound','goal_consistent','cursor_consistent'))
        need(all(type(v) is bool for v in c.values()),'actual continuity check results')
    t=e['conflict']
    if t is not None:
        keys(t,('unresolved','considered'));integer(t['considered'],1,32);integer(t['unresolved'],0,t['considered'])
    error=e['prediction']
    if error is not None:
        keys(error,('error','model_sha256','commitment_sha256'));number(error['error'],0,1e6)
        sha(error['model_sha256']);sha(error['commitment_sha256'])
    p=e['opportunity']
    if p is not None:
        keys(p,('feasible','expected_reduction','registered_cost'))
        need(type(p['feasible']) is bool,'feasibility');number(p['expected_reduction']);integer(p['registered_cost'],1,16)
    need(len(_encode(e))<=4096,'evidence size')
    return copy.deepcopy(e)


def _targets(e):
    c=e['continuity'];t=e['conflict'];pred=e['prediction'];p=e['opportunity']
    tension=[]
    if t is not None:tension.append(t['unresolved']/t['considered'])
    if pred is not None:tension.append(pred['error']/(1+pred['error']))
    return dict(C=None if c is None else sum(c.values())/3,
        T=None if not tension else sum(tension)/len(tension),
        P=None if p is None else max(0.,p['expected_reduction']-p['registered_cost']/16) if p['feasible'] else 0.)


def validate_regulation(state):
    keys(state,initial_regulation('shape','0'*32,'scope_'+'0'*32))
    need(state['version']==VERSION,'version');binding(state['identity'],state['instance_id'],state['scope_ref'])
    for k in ('C','T','P','K'):number(state[k])
    need(type(state['K']) is float and state['K']==.5 and state['K_missing'] is True,'missing neutral relation')
    integer(state['K_evidence_count'],0,0);integer(state['updates'])
    keys(state['missing'],('C','T','P'));need(all(type(v) is bool for v in state['missing'].values()),'missing flags')
    last=state['last']
    if last is None:
        need(state['updates']==0 and all(state[k]==.5 for k in ('C','T','P')) and all(state['missing'].values()),'exact initial state')
    else:
        keys(last,('evidence','evidence_sha256','previous','targets'))
        e=_evidence(last['evidence']);need(all(e[k]==state[k] for k in ('identity','instance_id','scope_ref')),'source binding')
        sha(last['evidence_sha256'])
        need(state['updates']>=1 and last['evidence_sha256']==_hash(e),'source hash')
        keys(last['previous'],('C','T','P'))
        for v in last['previous'].values():number(v)
        keys(last['targets'],('C','T','P'))
        for v in last['targets'].values():
            if v is not None:number(v)
        targets=_targets(e);need(last['targets']==targets,'derived targets')
        for k,q in targets.items():
            expected=last['previous'][k] if q is None else (1-ALPHA)*last['previous'][k]+ALPHA*q
            need(state[k]==expected and state['missing'][k]==(q is None),'exact bounded update')
    need(len(_encode(state))<=MAX_BYTES,'state size')
    return copy.deepcopy(state)


def update_regulation(state,evidence):
    state=validate_regulation(state);e=_evidence(evidence)
    need(all(e[k]==state[k] for k in ('identity','instance_id','scope_ref')),'source binding')
    if state['last'] is not None:
        prior=state['last']['evidence']
        if e['seq']==prior['seq'] or e['receipt_sha256']==prior['receipt_sha256']:
            need(e==prior,'altered or reordered same source');return state
        need(e['seq']>prior['seq'],'monotonic evidence')
    need(state['updates']<MAX_COUNTER,'update capacity')
    targets=_targets(e);previous={k:state[k] for k in ('C','T','P')}
    for k,q in targets.items():
        state['missing'][k]=q is None
        if q is not None:state[k]=(1-ALPHA)*state[k]+ALPHA*q
    state['updates']+=1
    state['last']=dict(evidence=e,evidence_sha256=_hash(e),previous=previous,targets=targets)
    return validate_regulation(state)


def candidate_offset(state,features):
    state=validate_regulation(state)
    keys(features,('repair','verify','explore','permitted','no_progress'))
    need(all(type(v) is bool for v in features.values()),'typed candidate features')
    if not features['permitted'] or features['no_progress']:return 0.
    channels=((1-state['C']) if features['repair'] and not state['missing']['C'] else 0.,
              state['T'] if features['verify'] and not state['missing']['T'] else 0.,
              state['P'] if features['explore'] and not state['missing']['P'] else 0.)
    return .1*sum(channels)/3
