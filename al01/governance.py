"""Bounded pure §28 governance. Scheduling/approval never grants capabilities.

Evidence binding checks supplied committed history, not truth or hostile-host
provenance. Minimum service is one job per module when caps/budget permit; a
rotating start provides service under contention. Costs are logical registered
operation units, not CPU milliseconds. Conflict rounds request new evidence,
not votes; missing dissent is retained until explicitly revised with new evidence.
"""
import copy
import math
from .core import fail, canonical, digest
from .contracts import NEURONS
from .neuron_queries import _verified_events

SCHEMA='governance-1'
MAX_JOBS=64
MAX_AGE=8
MAX_CYCLES=10000
EPS=1e-9


def need(ok, reason):
    if not ok: fail('GOVERNANCE_INVALID',reason)


def number(value, low, high):
    need(type(value) in (int,float) and math.isfinite(value) and low<=value<=high,'bounded number')
    return float(value)


def token(value):
    need(type(value) is str and 0<len(value)<=128 and not any(0xD800<=ord(c)<=0xDFFF for c in value),'identifier')
    return value


def initial_state():
    return {'schema':SCHEMA,'cycle':0,'cursor':0,'ages':{},'completed':{}}


def _state(state):
    need(type(state) is dict and set(state)==set(initial_state()) and state['schema']==SCHEMA,'scheduler state')
    need(type(state['cycle']) is int and 0<=state['cycle']<=MAX_CYCLES,'cycle')
    need(type(state['cursor']) is int and 0<=state['cursor']<MAX_JOBS,'cursor')
    for key in ('ages','completed'):
        need(type(state[key]) is dict and len(state[key])<=MAX_JOBS,'scheduler records')
        for name,value in state[key].items():
            token(name)
            if key=='ages':need(type(value) is int and 0<=value<=MAX_AGE,'age')
            else:need(type(value) is str and len(value)==64 and all(c in '0123456789abcdef' for c in value),'completion signature')


def validate_state(state):
    _state(state)
    return None


def _source(snapshot):
    events,_,head=_verified_events(snapshot,('read','neuron'))
    token(snapshot.get('id'))
    return {'individual_id':snapshot['id'],'version':snapshot['version'],'tick':snapshot['tick'],'head':head},events


def _refs(refs,events):
    need(type(refs) is list and len(refs)<=16,'bounded evidence list')
    need(all(type(x) is int and 0<=x<len(events) for x in refs) and len(set(refs))==len(refs),'evidence indexes')
    return [{'index':x,'hash':events[x][0]['hash'],'kind':events[x][0]['kind']} for x in refs]


def schedule(state,jobs,snapshot,*,budget=4,module_caps=None,minimum_service=1):
    """Return detached updated scheduler state and actual eligible job IDs.

    Jobs: id/module/cost/gain, optional urgency [0,.25], evidence_refs (event
    indexes), action_id (candidate identifier or None). Only allocated candidate
    IDs are eligible for subsequent arbitration; executor checks remain separate.
    Same exact job at same evidence head is suppressed after successful allocation.
    """
    _state(state);source,events=_source(snapshot)
    need(state['cycle']<MAX_CYCLES,'cycle budget exhausted')
    need(type(budget) is int and 0<=budget<=64,'budget')
    need(type(minimum_service) is int and minimum_service in (0,1),'minimum service 0 or 1 job')
    need(type(jobs) is list and len(jobs)<=MAX_JOBS,'job count')
    caps=module_caps if module_caps is not None else {m:budget for m in NEURONS}
    need(type(caps) is dict and all(m in NEURONS and type(v) is int and 0<=v<=64 for m,v in caps.items()),'registered module caps')
    rows=[];ids=set()
    for job in jobs:
        need(type(job) is dict and {'id','module','cost','gain'}<=set(job) and not set(job)-{'id','module','cost','gain','urgency','evidence_refs','action_id'},'job fields')
        jid=token(job['id']);need(jid not in ids,'duplicate job');ids.add(jid)
        module=job['module'];need(type(module) is str and module in NEURONS,'registered module')
        need(type(job['cost']) is int and job['cost']==NEURONS[module].compute_cost and job['cost']>0,'registered cost')
        gain=number(job['gain'],0,1);urgency=number(job.get('urgency',0),0,.25)
        aid=job.get('action_id');token(aid) if aid is not None else None
        refs=job.get('evidence_refs',[]);evidence=_refs(refs,events)
        normalized=dict(id=jid,module=module,cost=job['cost'],gain=gain,urgency=urgency,evidence_refs=copy.deepcopy(refs),action_id=aid)
        signature=digest({'job':normalized,'individual_id':source['individual_id'],'event_head':source['head']})
        age=state['ages'].get(jid,0)
        rows.append(dict(normalized,priority=gain/(job['cost']+EPS)+urgency+.025*age,age=age,
                         evidence=evidence,source_kind='event_evidence' if refs else 'declared_rule',signature=signature))
    rows.sort(key=lambda row:(-row['priority'],row['id']))
    allocated=[];used={};spent=0;suppressed={r['id'] for r in rows if state['completed'].get(r['id'])==r['signature']}
    def take(row):
        nonlocal spent
        if row['id'] in suppressed or row in allocated:return False
        if spent+row['cost']>budget or used.get(row['module'],0)+row['cost']>caps.get(row['module'],0):return False
        allocated.append(row);spent+=row['cost'];used[row['module']]=used.get(row['module'],0)+row['cost'];return True
    modules=sorted({r['module'] for r in rows if r['id'] not in suppressed})
    if modules and minimum_service:
        start=state['cursor']%len(modules)
        for module in modules[start:]+modules[:start]:
            for row in rows:
                if row['module']==module and take(row):break
    for row in rows:take(row)
    out=initial_state();out.update(cycle=state['cycle']+1,cursor=(state['cursor']+1)%MAX_JOBS)
    deferred=[]
    for row in rows:
        jid=row['id']
        if row in allocated:out['completed'][jid]=row['signature']
        elif jid in suppressed:
            out['completed'][jid]=row['signature'];deferred.append(dict(row,reason='NO_NEW_EVIDENCE'))
        else:
            out['ages'][jid]=min(MAX_AGE,row['age']+1)
            deferred.append(dict(row,reason='MODULE_CAP' if used.get(row['module'],0)+row['cost']>caps.get(row['module'],0) else 'BUDGET'))
    _state(out)
    return copy.deepcopy({'state':out,'allocations':allocated,'deferred':deferred,'spent':spent,'budget':budget,
        'eligible_action_ids':sorted({r['action_id'] for r in allocated if r['action_id'] is not None}),
        'source':source,'authority':'ADVISORY_ONLY; single executor must independently validate capability and commit',
        'minimum_service':'one job per module where feasible; rotating start under contention'})


def _proposal(p,events):
    need(type(p) is dict and {'id','module','action_id','score','evidence_refs'}<=set(p) and not set(p)-{'id','module','action_id','score','evidence_refs','claim'},'proposal fields')
    token(p['id']);token(p['action_id']);need(type(p['module']) is str and p['module'] in NEURONS,'proposal module')
    number(p['score'],0,1);_refs(p['evidence_refs'],events)
    if 'claim' in p:
        c=p['claim'];need(type(c) is dict and set(c)=={'subject','predicate','scope','value'},'scoped claim')
        for key in ('subject','predicate','scope'):token(c[key])
        value=c['value'];need(type(value) in (str,bool,int,float),'scalar claim')
        if type(value) is str:token(value)
        elif type(value) is not bool:number(value,-1e12,1e12)
    return copy.deepcopy(p)


def arbitrate(proposals,snapshot,*,eligible_action_ids,dispute=None):
    """Arbitrate eligible actions, never establish truth by score or majority.

    A dispute retains prior proposals. Revising one requires later referenced
    evidence and unchanged module identity. At most two new-head supplementary
    rounds are permitted; same-head retries stop without spending another round.
    Return UNKNOWN with request_evidence=False when exhausted/stalled.
    """
    source,events=_source(snapshot)
    need(type(proposals) is list and len(proposals)<=MAX_JOBS,'proposal count')
    need(type(eligible_action_ids) is list and len(eligible_action_ids)<=MAX_JOBS,'eligibility list')
    for aid in eligible_action_ids:token(aid)
    need(len(set(eligible_action_ids))==len(eligible_action_ids),'duplicate eligibility')
    retained={};rounds=0;previous_source=None
    if dispute is not None:
        need(type(dispute) is dict and set(dispute)=={'schema','rounds','source','proposals'} and dispute['schema']==SCHEMA,'dispute fields')
        need(type(dispute['rounds']) is int and 0<=dispute['rounds']<=2,'dispute rounds');rounds=dispute['rounds']
        previous_source=dispute['source'];need(type(previous_source) is dict and set(previous_source)==set(source),'dispute source')
        tick=previous_source['tick'];need(type(tick) is int and 0<=tick<=len(events),'dispute tick')
        need(previous_source['individual_id']==source['individual_id'] and previous_source['head']==(events[tick-1][0]['hash'] if tick else '0'*64),'dispute prefix')
        need(type(previous_source['version']) is int and tick<=previous_source['version']<=source['version'],'dispute version')
        need(type(dispute['proposals']) is list and len(dispute['proposals'])<=MAX_JOBS,'retained proposals')
        for old in dispute['proposals']:
            p=_proposal(old,events[:tick]);need(p['id'] not in retained,'duplicate retained proposal');retained[p['id']]=p
    seen=set()
    if previous_source is not None and rounds==2:
        need(all(raw in dispute['proposals'] for raw in proposals),'dispute budget exhausted; retain UNKNOWN')
    for raw in proposals:
        p=_proposal(raw,events);pid=p['id'];need(pid not in seen,'duplicate proposal');seen.add(pid)
        if pid in retained and retained[pid]!=p:
            old=retained[pid];need(p['module']==old['module'],'cannot impersonate proposal owner')
            need(('claim' in p)==('claim' in old),'cannot discard disputed claim')
            if 'claim' in p:
                need(all(p['claim'][k]==old['claim'][k] for k in ('subject','predicate','scope')),'revision cannot change applicability scope')
            need(previous_source is not None and any(i>=previous_source['tick'] for i in p['evidence_refs']),'revision needs later evidence')
        retained[pid]=p
    need(len(retained)<=MAX_JOBS,'retained budget')
    rows=sorted(retained.values(),key=lambda p:p['id']);groups={}
    for p in rows:
        if 'claim' in p:
            c=p['claim'];groups.setdefault((c['subject'],c['predicate'],c['scope']),[]).append(p)
    conflicts=[]
    for key,group in sorted(groups.items()):
        values={(type(p['claim']['value']).__name__,canonical(p['claim']['value'])) for p in group}
        if len(values)>1:
            conflicts.append({'subject':key[0],'predicate':key[1],'scope':key[2],'proposal_ids':[p['id'] for p in group],
                              'reason':'TYPE_MISMATCH' if len({v[0] for v in values})>1 else 'CONTRADICTORY_VALUES'})
    changed=previous_source is None or (source['head']!=previous_source['head'] and
        any(i>=previous_source['tick'] for p in proposals for i in p['evidence_refs']))
    if previous_source is not None and changed:rounds=min(2,rounds+1)
    pending={'schema':SCHEMA,'rounds':rounds,'source':source if changed else previous_source,'proposals':rows}
    candidates=[p for p in rows if p['action_id'] in eligible_action_ids]
    candidates.sort(key=lambda p:(-p['score'],p['id']))
    status='UNKNOWN' if conflicts or not candidates else 'APPROVED'
    return copy.deepcopy({'status':status,'action_id':candidates[0]['action_id'] if status=='APPROVED' else None,
        'reason':'CONFLICT' if conflicts else 'NO_ELIGIBLE_ACTION' if not candidates else 'ELIGIBLE_PROPOSAL',
        'conflicts':conflicts,'dispute':pending if conflicts else None,
        'request_evidence':bool(conflicts and changed and rounds<2),
        'authority':'single executor only; approval does not grant capability or commit state',
        'truth_status':'UNDETERMINED; action arbitration is not factual verification','source':source})
