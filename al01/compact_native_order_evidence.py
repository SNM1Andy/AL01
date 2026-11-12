"""Passive, birth-gated native evidence. No policy, fitting, effects or journal scan.

Witnesses enter live state only through CompactRuntime's receipt reducer.
The original REQUEST/START/RECEIPT chain remains the authority for chronology.
"""
import copy

from .compact_observer import validate_capture
from .core import CVM, transaction_payload
from .folder_session import _encode
from .life_objects import _json

PROFILE = 'native-two-list-observation-1'
SCHEMA = 'native-order-observation-evidence-1'
ROLES = ('EARLIER_SOURCE', 'LATER_SOURCE')
MAX_BYTES = 8192


def need(ok, why):
    if not ok:
        raise ValueError('NATIVE_ORDER_EVIDENCE_INVALID: ' + why)


def initial():
    return dict(schema=SCHEMA, lists=[], last_refresh_seq=None)


def validate(root):
    _json(root, MAX_BYTES)
    need(type(root) is dict and set(root) == {'schema', 'lists', 'last_refresh_seq'}
         and root['schema'] == SCHEMA, 'root fields/version')
    refresh = root['last_refresh_seq']
    need(refresh is None or type(refresh) is int and refresh > 0, 'refresh sequence')
    need(type(root['lists']) is list and len(root['lists']) <= 2, 'two LIST witnesses maximum')
    previous = 0
    generation = 0
    for w in root['lists']:
        need(type(w) is dict and set(w) == {'capture_ref', 'decision_ref', 'receipt_seq', 'source_seq',
             'source_head', 'generation'}, 'witness fields')
        need(type(w['source_seq']) is int and type(w['receipt_seq']) is int
             and w['source_seq'] >= previous and w['receipt_seq'] == w['source_seq'] + 3,
             'ordered original request/start/receipt')
        need(type(w['generation']) is int and w['generation'] > generation, 'LIST generation order')
        previous, generation = w['receipt_seq'], w['generation']
    return root


def record_list(root, decision, item, capture_ref, decision_ref, receipt_seq, generation):
    validate(root)
    validate_capture(item)
    need(item['request'] == decision['request'], 'original request')
    if item['request']['operation'] != 'LIST_SCOPE' or item['capture']['result']['status'] != 'OK':
        return copy.deepcopy(root)
    source = decision['source']
    need(receipt_seq == source['global_seq'] + 3 and generation == source['inventory_generation'] + 1,
         'real receipt sequence/generation')
    out = copy.deepcopy(root)
    out['lists'] = (out['lists'] + [dict(capture_ref=copy.deepcopy(capture_ref),
        decision_ref=copy.deepcopy(decision_ref), receipt_seq=receipt_seq,
        source_seq=source['global_seq'], source_head=source['journal_head'], generation=generation)])[-2:]
    validate(out)
    return out


def record_refresh(root, seq):
    validate(root)
    out = copy.deepcopy(root)
    out['last_refresh_seq'] = seq
    validate(out)
    return out


def _list(w, state, resolve):
    origin = {k: state[k] for k in ('identity', 'instance_id', 'scope_ref')}
    for key, kind in (('capture_ref', 'compact_capture'), ('decision_ref', 'compact_decision')):
        ref = w[key]
        need(type(ref) is dict and ref.get('kind') == kind
             and all(ref.get(k) == origin[k] for k in ('identity', 'instance_id')), 'typed original source reference')
    item = resolve(w['capture_ref'], 'compact_capture')
    decision = resolve(w['decision_ref'], 'compact_decision')
    validate_capture(item)
    q, source = decision['request'], decision['source']
    need(all(source[k] == v for k, v in origin.items()) and q == item['request']
         and q['scope_ref'] == origin['scope_ref'] and q['operation'] == 'LIST_SCOPE'
         and item['capture']['capture_origin'] == state['config']['capture_origin']
         and item['capture']['result']['status'] == 'OK', 'original successful LIST capture')
    need(source['global_seq'] == w['source_seq'] and source['journal_head'] == w['source_head']
         and source['inventory_generation'] + 1 == w['generation'], 'LIST source chronology')
    need(decision['generated_cl'] == f"PROPOSE(OBSERVE(@{q['scope_ref']}));", 'exact LIST CL')
    vm = CVM(('read', 'observe'))
    tx = vm.prepare(decision['generated_cl'], dict(version=source['cognition_revision'],
        world_version=source['inventory_generation']), [q['scope_ref']])
    vm.verify(tx)
    need(_encode(transaction_payload(tx)) == _encode(decision['transaction_payload']), 'complete original LIST CL payload')
    return item['capture']['result']['payload']


def observe(state, workflow, resolve):
    """Address at most two LISTs + their decisions, never call journal lookup.

    Caller supplies the runtime-owned committed state/workflow, not an assertion
    that a span is complete/successful/independent. Feature is passive evidence;
    no implication about future validity or absence of all assistance follows.
    """
    need(state['config'].get('native_order_evidence_profile') == PROFILE, 'explicit birth profile required')
    root = validate(state['native_order_evidence'])
    result = dict(schema=SCHEMA, profile=PROFILE,
        source={k: state[k] for k in ('identity', 'instance_id', 'scope_ref')},
        status='UNKNOWN', reason='INSUFFICIENT_LIST_HISTORY', feature=None, role_paths=None, goal=None,
        witnesses=copy.deepcopy(root['lists']), workflow_ref=copy.deepcopy(state['workflow_ref']),
        last_refresh_seq=root['last_refresh_seq'],
        acquisition='REFRESH_OBSERVED' if root['last_refresh_seq'] is not None else 'NO_REFRESH_RECORDED',
        reliability='UNKNOWN', authority='NONE')
    # Validate available witnesses even when the feature is incomplete.
    lists = [_list(w, state, resolve) for w in root['lists']]
    if root['lists']:
        latest = root['lists'][-1]
        need(latest['generation'] <= state['inventory']['generation'], 'no future LIST')
        if latest['generation'] == state['inventory']['generation']:
            need(latest['capture_ref'] == state['inventory']['list_receipt_ref'], 'current LIST pointer')
    goal = workflow['goal']
    if goal is not None:
        result['goal'] = {k: copy.deepcopy(goal[k]) for k in ('id', 'generation', 'episode_key', 'bindings', 'cursor', 'status')}
        paths = {}
        for role in ROLES:
            index = goal['bindings'].get(role)
            need(type(index) is int and 0 <= index < len(state['goals']), 'existing goal role binding')
            paths[role] = state['goals'][index]['path']
        need(len(set(paths.values())) == 2, 'different role paths')
        result['role_paths'] = paths
    if state['pending'] is not None and state['pending']['phase'] == 'UNKNOWN':
        result['reason'] = 'EXECUTION_UNKNOWN'
        return result
    if len(lists) != 2:
        return result
    if not state['inventory']['valid'] or root['lists'][-1]['generation'] != state['inventory']['generation']:
        result['reason'] = 'CURRENT_LIST_NOT_VALID'
        return result
    if goal is None or goal['status'] != 'ACTIVE':
        result['reason'] = 'NO_ACTIVE_GOAL'
        return result
    pairs = []
    for payload in lists:
        if not payload['complete']:
            result['reason'] = 'INCOMPLETE_LIST'
            return result
        refs = []
        for role in ROLES:
            entries = [e for e in payload['entries'] if e['kind'] == 'file'
                       and e['relative_path'] == paths[role]]
            if len(entries) != 1:
                result['reason'] = 'ROLE_NOT_IN_BOTH_LISTS'
                return result
            refs.append(entries[0]['entry_ref'])
        pairs.append(refs)
    result.update(status='VERIFIED_PAST_REFERENCE_RELATION', reason='TWO_ORIGINAL_LISTS',
        feature=['SAME' if a == b else 'CHANGED' for a, b in zip(*pairs)])
    return result
