"""Registered-neuron-fed U0 rule integration, not trained neural intelligence.

Only a bounded recalled window is examined. A zero match count means no matching
feedback in that window, NOT unknown/nonexistent. Rankings confer no authority.
"""
import copy
import math
import re
from .core import fail,canonical,digest
from .contracts import execute_neuron
from .neuron_queries import lookup_action_consequences,_verified_events
from .development import spatial_feature

SCHEMA='registered-neuron-u0-1'
MAX_CANDIDATES=16
RECALL_LIMIT=16
REVISIT_PENALTY=.02


def _require(ok,detail):
    if not ok:fail('NEURAL_CONTROL_INVALID',detail)


def _inputs(snapshot,candidates):
    _require(isinstance(snapshot,dict) and isinstance(snapshot.get('id'),str) and 1<=len(snapshot['id'])<=256,'snapshot identity')
    try:snapshot['id'].encode('utf8')
    except UnicodeError:fail('NEURAL_CONTROL_INVALID','snapshot identity UTF-8')
    _require(isinstance(candidates,list) and 1<=len(candidates)<=MAX_CANDIDATES,'candidate budget 1..16')
    seen=set()
    for row in candidates:
        _require(isinstance(row,dict) and {'entity_ref','baseline_score'}<=set(row)
            and not set(row)-{'entity_ref','baseline_score','novelty','persistence','switching','position'},'candidate fields')
        name=row['entity_ref'];score=row['baseline_score']
        _require(isinstance(name,str) and re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,255}',name) is not None and name not in seen,'candidate reference')
        _require(type(score) in (int,float) and -1e6<=score<=1e6 and math.isfinite(score),'baseline score')
        if 'position' in row:spatial_feature(row['position'])
        for key in set(row)-{'entity_ref','baseline_score','position'}:
            value=row[key];_require(type(value) in (int,float) and 0<=value<=1 and math.isfinite(value),'candidate coefficient')
        seen.add(name)
    _require(len({'position' in c for c in candidates})==1,'consistent spatial metadata required')


def evaluate_u0(snapshot,candidates,*,capabilities=('read','neuron'),enabled=True):
    """Return focus/ranking causally derived from actual registered recall/compare.

    Caller supplies current visible legal metadata; this function cannot grant
    visibility or action permissions. Full chain is checked by registered recall.
    With position metadata, use reusable sign-pair spatial bins relative origin0;
    without it, use exact target IDs (diagnostic compatibility). Coordinates are
    caller-supplied visible metadata, never hidden reward values. Matching feedback
    must also resolve to a recorded action consequence. Offset
    is -0.02 * registered compare(recalled_count,0), or0 for declared ablation.
    All input snapshots/candidates remain untouched; no file/network/CVM mutation.
    """
    _inputs(snapshot,candidates);_require(type(enabled) is bool,'enabled flag')
    recalled=execute_neuron('recall',['feedback',RECALL_LIMIT],state=snapshot,capabilities=capabilities)
    spatial='position' in candidates[0]
    contexts={c['entity_ref']:spatial_feature(c['position']) if spatial else c['entity_ref'] for c in candidates}
    evidence=[];counts={context:0 for context in contexts.values()};checked_actions=set()
    scan_bytes=recalled['compute_usage']['bytes_scanned'];scan_events=recalled['compute_usage']['events_scanned']
    for event in recalled['result']['matches']:
        if event['kind']!='feedback':continue
        context=spatial_feature(event['payload'].get('position')) if spatial else event['payload'].get('target')
        if context not in counts:continue
        action=event['payload'].get('action_id')
        _require(isinstance(action,str),'recalled feedback requires linked action')
        if action not in checked_actions:
            consequence=lookup_action_consequences(snapshot,action,capabilities=capabilities)
            scan_bytes+=consequence['compute_usage']['bytes_scanned'];scan_events+=consequence['compute_usage']['events_scanned']
            _require(scan_bytes<=16*1024*1024,'aggregate evidence scan budget')
            _require(consequence['result']['status']=='RECORDED' and f"event_{event['index']}" in consequence['evidence_refs'],'recorded consequence link')
            checked_actions.add(action)
        counts[context]+=1
        evidence.append({'ref':f"event_{event['index']}",'content_hash':event['hash'],'target':event['payload']['target'],'context':context})
    ranked=[]
    for candidate in candidates:
        name=candidate['entity_ref'];count=counts[contexts[name]]
        compared=execute_neuron('compare',[count,0],state=snapshot,capabilities=capabilities)
        _require(compared['result'] in (0,1),'nonnegative-count comparison')
        offset=-REVISIT_PENALTY*compared['result'] if enabled else 0.
        ranked.append({'entity_ref':name,'context':contexts[name],'baseline_score':candidate['baseline_score'],'recalled_count':count,
                       'compare_contract':compared,'offset':offset,'score':candidate['baseline_score']+offset})
    ranked.sort(key=lambda row:(-row['score'],row['entity_ref']))
    source={'individual_id':snapshot['id'],'state_version':snapshot['version'],'tick':snapshot['tick'],
            'event_head':recalled['event_head'],'candidate_sha256':digest(candidates)}
    result={'schema':SCHEMA,'controller':'U0_RULE_CONTROLLER','trained':False,'enabled':enabled,'source':source,
            'candidate_inputs':copy.deepcopy(candidates),'recall_contract':recalled,'matched_evidence':evidence,
            'ranked':ranked,'focus':[r['entity_ref'] for r in ranked[:8]],'selected':ranked[0]['entity_ref'],
            'context_mode':'visible_position_sign_pair' if spatial else 'exact_target',
            'compute_usage':{'events_scanned':scan_events,'bytes_scanned':scan_bytes},
            'rule':{'recall_query':'feedback','recall_limit':RECALL_LIMIT,'revisit_penalty':REVISIT_PENALTY},
            'claim_scope':'bounded recorded-window revisit heuristic; no general intelligence or benefit claim'}
    _require(len(canonical(result).encode('utf8'))<=262144,'U0 result byte budget')
    return copy.deepcopy(result)


def validate_u0(result,snapshot,candidates,*,capabilities=('read','neuron')):
    """Strict replay binding to exact event head/version and candidate metadata.

    Recomputes registered outputs, detecting stale or mixed-source contracts.
    This is consistency validation, not hostile-host authentication.
    """
    _require(isinstance(result,dict) and type(result.get('enabled')) is bool,'result envelope')
    expected=evaluate_u0(snapshot,candidates,capabilities=capabilities,enabled=result['enabled'])
    _require(canonical(result)==canonical(expected),'stale or altered neuron/control result')


def candidate_offset(result,entity_ref):
    """Extract an offset from a previously validated result; never executes it."""
    _require(isinstance(result,dict) and result.get('schema')==SCHEMA,'U0 result schema')
    rows=result.get('ranked');_require(isinstance(rows,list) and 1<=len(rows)<=MAX_CANDIDATES,'ranking')
    matches=[row for row in rows if isinstance(row,dict) and row.get('entity_ref')==entity_ref]
    _require(len(matches)==1,'candidate not in bound result')
    value=matches[0].get('offset');_require(type(value) in (int,float) and -REVISIT_PENALTY<=value<=0,'offset bound')
    return value


def validate_u0_history(result,current_snapshot,*,expected_state_version=None,capabilities=('read','neuron')):
    """Replay a historical report against a verified prefix of retained events.

    Constructs only an explicit event-query view, NOT a fabricated full cognitive
    snapshot. Non-event state was never read by this bridge. Historical version
    must be bound to the coordinator's decision trace via expected_state_version;
    if omitted only its range can be checked (events do not encode admissions).
    """
    _require(isinstance(result,dict) and isinstance(result.get('source'),dict),'historical report')
    source=result['source'];events,_,_=_verified_events(current_snapshot,capabilities)
    tick=source.get('tick');version=source.get('state_version')
    _require(type(tick) is int and 0<=tick<=len(events) and type(version) is int
             and tick<=version<=current_snapshot['version'],'historical clock range')
    _require(source.get('individual_id')==current_snapshot.get('id'),'historical identity')
    if expected_state_version is not None:
        _require(type(expected_state_version) is int and version==expected_state_version,'historical decision version')
    head=events[tick-1][0]['hash'] if tick else '0'*64
    _require(source.get('event_head')==head,'historical event prefix')
    view={'id':current_snapshot['id'],'version':version,'tick':tick,'events':[e for e,_ in events[:tick]]}
    validate_u0(result,view,result.get('candidate_inputs'),capabilities=capabilities)
