"""Opt-in, finite within-goal evidence retention across safe monitoring reads.

This module is selected only by an immutable birth profile. The v0.37 reducer
remains unchanged, including all explicit interruption and failure semantics.
No history scan, new action, authority, quota, or native provenance is created.
"""
import copy

from . import compact_procedures as procedures
from .compact_observer import validate_capture


PROFILE = 'same-goal-monitor-subsequence-1'


def validate_profile(profile):
    procedures.need(profile is None or profile == PROFILE,
                    'supported versioned procedure learning profile')
    return profile


def _safe_monitor_gap(w, decision, item, state, index, resolve):
    """All inputs are from the admitted deterministic runtime transition.

    Preserve only a one-step prefix of the current goal. Re-resolving that
    prefix keeps its decision, receipt, namespace, cursor and role bound to
    the current goal instead of treating arbitrary old observations as skill.
    """
    goal = w['goal']
    request = item['request']
    if (decision.get('investigation') is not None
            or item['capture']['result']['status'] != 'OK'
            or request['operation'] != 'READ_TEXT'
            or goal is None or goal['status'] != 'ACTIVE'
            or goal['cursor'] != 1 or len(w['trace']) != 1
            or not state['inventory']['valid']):
        return False
    origin = {key: state[key] for key in ('identity', 'instance_id', 'scope_ref')}
    expected_origin = state['config']['capture_origin']
    if (item['capture']['capture_origin'] != expected_origin
            or request != decision['request']
            or request['scope_ref'] != origin['scope_ref']
            or any(decision['source'].get(k) != v for k, v in origin.items())):
        return False
    context = procedures.context(index, goal['topic'], state['goals'])
    if (context is None or context['episode_key'] != goal['episode_key']
            or context['bindings'] != goal['bindings']):
        return False
    entries = {entry['relative_path']: entry for entry in state['inventory']['entries']
               if entry['kind'] == 'file'}
    paths = {role: state['goals'][slot]['path']
             for role, slot in goal['bindings'].items()}
    if any(path not in entries for path in paths.values()):
        return False
    monitored_paths = {row['path'] for row in state['goals']}
    gap_paths = [path for path, entry in entries.items()
                 if entry['entry_ref'] == request['entry_ref']]
    if (len(gap_paths) != 1 or gap_paths[0] not in monitored_paths
            or gap_paths[0] in paths.values()):
        return False

    prefix = w['trace'][0]
    step = prefix['step']
    if (prefix['episode_key'] != goal['episode_key']
            or step['role'] not in procedures.ROLES
            or goal['receipts'] != [step['capture_ref']]):
        return False
    prior = resolve(step['decision_ref'], 'compact_decision')
    capture = resolve(step['capture_ref'], 'compact_capture')
    validate_capture(capture)
    info = prior.get('investigation')
    if (info is None or info['goal_id'] != goal['id'] or info['cursor'] != 0
            or info['episode_key'] != goal['episode_key']
            or info['role'] != step['role']
            or any(prior['source'].get(k) != v for k, v in origin.items())
            or prior['source']['inventory_generation'] != decision['source']['inventory_generation']
            or capture['capture']['capture_origin'] != expected_origin
            or capture['capture']['result']['status'] != 'OK'
            or capture['request'] != prior['request']
            or prior['request']['operation'] != 'READ_TEXT'
            or prior['request']['entry_ref'] != entries[paths[step['role']]]['entry_ref']
            or step['receipt_seq'] != prior['source']['global_seq'] + 3
            or step['receipt_seq'] > decision['source']['global_seq']):
        return False
    return True


def receive(w, decision, item, decision_ref, capture_ref, seq, now, *, state, index, resolve):
    """Run every legacy receipt effect; retain only a verified safe prefix.

    Regulation accounting, goal completion, failure blocking and duplicate
    checks remain in the original reducer. The trace still stores at most
    two steps, and chains/candidate budgets are unchanged.
    """
    validate_profile(state['config'].get('procedure_learning_profile'))
    procedures.need(state['config'].get('procedure_learning_profile') == PROFILE,
                    'explicit birth opt-in for interleaved learning')
    result = procedures.receive(w, decision, item, decision_ref, capture_ref, seq, now)
    if _safe_monitor_gap(w, decision, item, state, index, resolve):
        result['trace'] = copy.deepcopy(w['trace'])
    return procedures.validate_workflow(result)
