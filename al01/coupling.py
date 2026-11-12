"""Isolated, bounded research coupling. Not production policy/resume integration.

Rankings never grant authority. Execution still uses ordinary CL compilation,
CVM preparation, Individual.commit and later feedback. No learning is claimed.
"""
import copy
from dataclasses import asdict
import hashlib
import json
import math
import re
import numpy as np
from .instinct import FrozenBias,Geometry,ReferenceCGL,EventDrive,FEATURES,features,InstinctError

VERSION='bounded-research-coupling-1'
MAX_CANDIDATES=16
CHANNELS=('exploration','persistence','switching')


def _number(value,low,high):
    if type(value) not in (int,float) or not low<=value<=high or not math.isfinite(value):
        raise InstinctError('candidate numeric bounds')
    return float(value)


def rank_candidates(candidates,feature_values,calibration,*,enabled=True):
    """Accept metadata only, not world values. Return a detached complete score trace.

    novelty, persistence and switching are declared coefficients in [0,1].
    Their concrete operational definitions belong to the candidate generator.
    Bounded offset is beta * mean(coefficient * channel_bias), never authority.
    """
    if not isinstance(calibration,FrozenBias) or type(enabled) is not bool:raise InstinctError('coupling configuration')
    if not isinstance(candidates,(list,tuple)) or not 1<=len(candidates)<=MAX_CANDIDATES:raise InstinctError('candidate count 1..16')
    biases=calibration.biases(feature_values);scored=[];seen=set()
    for candidate in candidates:
        if not isinstance(candidate,dict) or set(candidate)!={'entity_ref','baseline_score','novelty','persistence','switching'}:
            raise InstinctError('candidate metadata fields')
        name=candidate['entity_ref']
        if not isinstance(name,str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,255}',name) or name in seen:
            raise InstinctError('candidate identity')
        seen.add(name);baseline=_number(candidate['baseline_score'],-1e6,1e6)
        coefficients=[_number(candidate[k],0,1) for k in ('novelty','persistence','switching')]
        terms={channel:coefficients[i]*biases[channel] for i,channel in enumerate(CHANNELS)}
        delta=calibration.beta*sum(terms.values())/3 if enabled else 0.
        scored.append(dict(copy.deepcopy(candidate),bias_terms=terms,bias_delta=delta,final_score=baseline+delta))
    scored.sort(key=lambda c:(-c['final_score'],c['entity_ref']))
    config=asdict(calibration)
    return {'version':VERSION,'enabled':enabled,'selected':scored[0]['entity_ref'],'ranked':scored,
            'features':copy.deepcopy(feature_values),'feature_order':list(FEATURES),'biases':biases,
            'calibration':config,'calibration_sha256':hashlib.sha256(json.dumps(config,sort_keys=True).encode()).hexdigest(),
            'authority':'none; execute through ordinary CVM/commit'}


def visible_candidates(individual,previous_target=None):
    """Deterministic visible-only shortlist, evenly sampled sorted IDs, max16.

    Filter observed IDs BEFORE sampling, so exploration can cover more than16.
    Novelty is therefore1 for this generator: exploration contributes a common
    offset, not an independently tested action preference. Direction is the sign pair of the
    target position relative to the CURRENT visible-position centroid, not a
    hidden value. Persistence=1 for same direction bin as previous visible target;
    switching=1 for another bin. Both are0 without a previous visible target.
    Declared baseline is0.02 for unobserved targets, otherwise0. These arbitrary
    research coefficients are not learned traits or an optimal baseline.
    """
    visible=individual.world.visible(individual.state['position']);names=sorted(visible)
    if not names:return []
    unknown=[name for name in names if name not in individual.state['observed']]
    if not unknown:return []
    chosen=[unknown[i] for i in np.linspace(0,len(unknown)-1,min(len(unknown),MAX_CANDIDATES),dtype=int)]
    center=np.mean([visible[name]['position'] for name in names],axis=0)
    def direction(name):return tuple(np.sign(np.asarray(visible[name]['position'])-center).astype(int))
    prior=direction(previous_target) if previous_target in visible else None
    result=[]
    for name in chosen:
        novelty=float(name not in individual.state['observed']);same=prior==direction(name) if prior is not None else False
        result.append({'entity_ref':name,'baseline_score':.02*novelty,'novelty':novelty,
                       'persistence':float(prior is not None and same),'switching':float(prior is not None and not same)})
    return result


def execute_observation(individual,selection):
    """No direct adapter calls, bypass, or implicit authority from a score trace."""
    if individual.state['lifecycle']!='ACTIVE' or individual.state['goal_state']['status']=='BLOCKED':
        raise InstinctError('policy lifecycle/goal stopped')
    if individual.state['requested_actions'] or individual.state['pending']:
        raise InstinctError('outstanding action must resolve before selection')
    if not isinstance(selection,str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,255}',selection):raise InstinctError('selection')
    tx=individual.vm.prepare(f'PROPOSE(OBSERVE(@{selection}));',individual.state,individual.candidates())
    individual.commit(tx)
    return individual.state['events'][-1]['payload']['action_ids'][0]


def run_policy(individual,solver,calibration,*,decisions=16,enabled=True,replay_features=None):
    """Finite isolated run; no production persistence or resume claim.

    Live decisions read current field, then commit action and receive real result.
    Only afterwards is EventDrive rebuilt from committed history and field time
    advanced through that result's window. replay_features is an explicitly
    NONCAUSAL offline diagnostic; never label its outputs a live controller.
    """
    if type(decisions) is not int or not 1<=decisions<=64:raise InstinctError('decision budget 1..64')
    if not isinstance(solver,ReferenceCGL) or solver.time!=0 or individual.state['tick']!=0:
        raise InstinctError('isolated fresh field/individual required; resume unsupported')
    if solver.drive.records or solver.drive.source_head!='0'*64:
        raise InstinctError('initial drive must be empty; no future-event preload')
    if replay_features is not None and (not isinstance(replay_features,(list,tuple)) or len(replay_features)!=decisions):
        raise InstinctError('replay feature count')
    previous=None;traces=[];actions=[];switches=0;field_steps=0;stop_reason='decision_budget_complete'
    for index in range(decisions):
        if individual.state['lifecycle']!='ACTIVE':stop_reason='lifecycle_not_active';break
        if individual.state['goal_state']['status']=='BLOCKED':stop_reason='goal_blocked';break
        candidates=visible_candidates(individual,previous)
        if not candidates:stop_reason='no_visible_candidates';break
        current=features(solver.field)
        used=current if replay_features is None else copy.deepcopy(replay_features[index])
        trace=rank_candidates(candidates,used,calibration,enabled=enabled)
        trace.update(decision=index,field_time=solver.time,current_field_features=current,
                     candidate_inputs=copy.deepcopy(candidates),source_state_version=individual.state['version'],
                     source_tick=individual.state['tick'],source_event_head=individual.state['events'][-1]['hash'] if individual.state['events'] else '0'*64,
                     feature_mode='live_current_field' if replay_features is None else 'NONCAUSAL_REPLAY_DIAGNOSTIC')
        target=trace['selected'];action_id=execute_observation(individual,target)
        trace['commit_ref']=f"event_{individual.state['events'][-1]['index']}";trace['action_id']=action_id
        individual.step()
        feedback=individual.state['events'][-1]
        if feedback['kind']!='feedback' or feedback['payload'].get('action_id')!=action_id:
            raise InstinctError('missing action-linked feedback')
        trace['feedback_ref']=f"event_{feedback['index']}";trace['feedback_hash']=feedback['hash']
        solver.drive=EventDrive(individual.state['events'],tick_duration=.05)
        horizon=(individual.state['tick']+1)*.05-solver.time
        before=solver.steps;solver.run(horizon);field_steps+=solver.steps-before
        trace['drive_hash_after_feedback']=solver.drive.digest
        chosen=next(c for c in candidates if c['entity_ref']==target)
        switches+=int(chosen['switching']);traces.append(trace);actions.append(target);previous=target
    return {'scope':'isolated research experiment; no production checkpoint/resume integration',
            'stop_reason':stop_reason,
            'mode':'causal' if replay_features is None else 'NONCAUSAL_REPLAY_DIAGNOSTIC',
            'traces':traces,'actions':actions,'metrics':{'decisions':len(actions),'unique_observations':len(set(actions)),
                'direction_switches':switches,'direction_switch_rate':switches/max(1,len(actions)-1),
                'completed_feedback':individual.state['closed_loops'],'cognitive_ticks':individual.state['tick'],
                'field_steps':field_steps,'candidate_evaluations':sum(len(t['ranked']) for t in traces)},
            'field_manifest':solver.manifest(),'event_head':individual.state['events'][-1]['hash'] if individual.state['events'] else '0'*64}


def paired_ablations(*,seed=17,decisions=16):
    """ILLUSTRATIVE ONLY, NOT THE PREREGISTERED EXPERIMENT. Four paired arms.

    Same seed/environment initialization/baseline and coefficients. Real feedback
    histories may diverge because choices differ; they are not forced identical.
    Shuffled-time uses a deterministic permutation of enabled-run feature frames
    and is intentionally noncausal replay, not future-safe deployment evidence.
    """
    from .core import Individual,World
    y,x=np.indices((64,64));g=Geometry(np.cos(2*np.pi*x/64)+np.sin(4*np.pi*y/64))
    changed=Geometry(np.cos(8*np.pi*x/64)+np.sin(12*np.pi*y/64))
    cal=ReferenceCGL(g,seed=991,dt=.01);rows=[cal.step() for _ in range(64)]
    weights=((.1,-.1,0.,0.,.1,.2),(0.,.1,-.1,.1,0.,-.1),(-.1,0.,.1,0.,.2,.1))
    calibration=FrozenBias.calibrate(rows,calibration_id='independent-zero-drive-seed991-64steps-dt0.01',weights=weights,beta=.25)
    def arm(geometry,enabled=True,replay=None):
        return run_policy(Individual(World(64,hazard=True)),ReferenceCGL(geometry,seed=seed,dt=.01),calibration,
                          decisions=decisions,enabled=enabled,replay_features=replay)
    enabled=arm(g);disabled=arm(g,False);geometry=arm(changed)
    if len(enabled['traces'])!=decisions:raise InstinctError('illustrative replay incomplete: causal run stopped early')
    order=np.random.default_rng(seed+1).permutation(decisions).tolist()
    frames=[enabled['traces'][i]['features'] for i in order];shuffled=arm(g,replay=frames)
    return {'version':VERSION,'seed':seed,'calibration':asdict(calibration),'calibration_manifest':cal.manifest(),
            'candidate_operational_definitions':visible_candidates.__doc__,
            'arms':{'enabled':enabled,'disabled':disabled,'changed_geometry':geometry,'shuffled_feature_time':shuffled},
            'shuffle_permutation':order,'strategy_difference':{'enabled_vs_disabled':enabled['actions']!=disabled['actions'],
                'geometry_vs_enabled':geometry['actions']!=enabled['actions'],
                'shuffle_vs_enabled':shuffled['actions']!=enabled['actions']},
            'claim_limit':'finite hand-designed coupling; no learning, legacy-engine compatibility, or production resume claim'}
