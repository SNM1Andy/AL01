"""Pure bounded receipt projection, not file understanding or OS authentication.

Sequence is controller admission order, not wall-clock capture order. The folder
event namespace is separate from grid action history. Literal text is retained
in source receipts and their event projections; semantic rows index untrusted text.
Application provenance labels are assertions of the trusted supplying broker,
not cryptographic proof of a real read. Synthetic labels are never upgraded.
"""
import copy
import hashlib
import json
import re

from .folder_contracts import validate_request, validate_result
from .contracts import execute_neuron
from .control_embryos import initial_control, update_control

SCHEMA = 'folder-cognition-1'
MAX_CAPTURES = 32
MAX_BYTES = 4 * 1024 * 1024


class FolderCognitionError(ValueError):
    code = 'FOLDER_COGNITION_INVALID'


def _need(ok, detail):
    if not ok:
        raise FolderCognitionError(detail)


def _raw(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(',', ':'),
                          ensure_ascii=False, allow_nan=False).encode('utf8')
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise FolderCognitionError('invalid bounded JSON') from exc


def _hash(value):
    return hashlib.sha256(_raw(value)).hexdigest()


def _capabilities(capabilities):
    _need(type(capabilities) in (list, tuple, set, frozenset)
          and all(type(x) is str for x in capabilities)
          and {'read', 'neuron'} <= set(capabilities), 'read and neuron required')


def initial_cognition(identity, scope_ref):
    _need(type(identity) is str and 1 <= len(identity) <= 128, 'identity')
    _raw(identity)
    _need(type(scope_ref) is str and re.fullmatch(r'scope_[0-9a-f]{32}', scope_ref), 'scope')
    return {'schema': SCHEMA, 'identity': identity, 'scope_ref': scope_ref,
            'version': 0, 'sources': [], 'events': [], 'working': [],
            'episodic': [], 'text_semantic': [], 'neuron_traces': [],
            'control': initial_control(), 'fact_adoption': False}


def _item(item, scope):
    _need(type(item) is dict and set(item) == {'sequence', 'request', 'capture', 'receipt_sha256'}, 'item fields')
    _need(type(item['sequence']) is int and 1 <= item['sequence'] <= MAX_CAPTURES, 'sequence')
    request, capture = item['request'], item['capture']
    validate_request(request)
    _need(request['scope_ref'] == scope, 'scope binding')
    _need(type(capture) is dict and set(capture) == {'schema', 'capture_origin', 'result'}
          and capture['schema'] == 'folder-session-1', 'capture wrapper')
    _need(capture['capture_origin'] in ('APPLICATION_BROKER_CAPTURE', 'SYNTHETIC_TEST_CAPTURE'), 'origin')
    validate_result(capture['result'], request)
    _need(item['receipt_sha256'] == _hash(capture), 'receipt hash')
    return copy.deepcopy(item)


def _bindings(sources):
    bindings = {}
    for source in sources:
        result = source['capture']['result']
        if result['operation'] == 'LIST_SCOPE' and result['status'] == 'OK':
            for entry in result['payload']['entries']:
                ref, path = entry['entry_ref'], entry['relative_path']
                _need(ref not in bindings or bindings[ref] == path, 'opaque ref rebound to different path')
                bindings[ref] = path
    return bindings


def _target(request, bindings):
    ref = request['entry_ref']
    if ref is None:
        return request['scope_ref']
    if ref in bindings:
        return 'file_' + _hash({'scope_ref': request['scope_ref'], 'relative_path': bindings[ref]})
    return ref  # Explicit unresolved token, never an invented filename.


def _append(state, item, capabilities):
    item = _item(item, state['scope_ref'])
    rid = item['request']['request_id']
    for old in state['sources']:
        if old['request']['request_id'] == rid:
            _need(old == item, 'conflicting duplicate')
            return state
    _need(len(state['sources']) < MAX_CAPTURES, 'capture budget exhausted; no eviction')
    _need(item['sequence'] == len(state['sources']) + 1, 'contiguous admission sequence required')
    request, capture = item['request'], item['capture']
    result = capture['result']
    bindings = _bindings(state['sources'])
    token = _target(request, bindings)
    # Only validated metadata, never text instructions, enters U0 focus.
    view = {'id': state['identity'], 'version': state['version'],
            'tick': len(state['events']), 'events': state['events']}
    recalled = execute_neuron('recall', ['', 32], state=view, capabilities=capabilities)
    matches = [e for e in recalled['result']['matches']
               if _target(e['payload']['request'], _bindings(state['sources'][:e['index']])) == token]
    compared = execute_neuron('compare', [len(matches), 0], state=view, capabilities=capabilities)
    novelty = float(compared['result'] == 0)
    focus = [token for e in matches[:8]]
    state['control'] = update_control(state['control'],
        {'tokens': [token, result['operation'], result['status']],
         'salience': .5, 'novelty': novelty}, focus)
    payload = {'request': copy.deepcopy(request), 'result': copy.deepcopy(result),
               'source_hash': item['receipt_sha256'], 'capture_origin': capture['capture_origin']}
    event = {'index': len(state['events']), 'tick': item['sequence'], 'kind': 'folder_capture',
             'payload': payload, 'previous': state['events'][-1]['hash'] if state['events'] else '0'*64}
    event['hash'] = _hash(event)
    evidence = {'event_index': event['index'], 'event_hash': event['hash'],
                'receipt_sha256': item['receipt_sha256'], 'request_id': rid,
                'capture_origin': capture['capture_origin'], 'sequence': item['sequence']}
    state['sources'].append(item)
    _bindings(state['sources'])  # Also reject conflicting listing bindings now.
    state['events'].append(event)
    state['working'] = (state['working'] + [dict(evidence, token=token)])[-8:]
    state['episodic'].append(dict(evidence, operation=result['operation'], status=result['status'],
                                  interpretation='CAPTURE_STATUS_ONLY'))
    if result['operation'] == 'READ_TEXT' and result['status'] == 'OK':
        state['text_semantic'].append(dict(evidence, entry_ref=token,
            raw_entry_ref=request['entry_ref'], scope_ref=state['scope_ref'],
            relative_path=bindings.get(request['entry_ref']),
            entity_status='LIST_BOUND' if request['entry_ref'] in bindings else 'UNRESOLVED_ENTRY',
            eligible_for_entity_query=request['entry_ref'] in bindings,
            content_sha256=result['payload']['content_sha256'],
            utf8_bytes=result['payload']['utf8_bytes'], classification='UNTRUSTED_TEXT',
            fact_adopted=False, source_index=event['index']))
    # Compact audited projection avoids quadratically duplicating literal texts.
    recall_audit = {k: copy.deepcopy(v) for k, v in recalled.items() if k != 'result'}
    recall_audit['result_sha256'] = _hash(recalled['result'])
    recall_audit['match_count'] = len(recalled['result']['matches'])
    state['neuron_traces'].append({'source_version': view['version'],
        'source_event_head': event['previous'], 'recall_audit': recall_audit, 'compare': compared,
        'focus_inputs': focus, 'novelty': novelty, 'logical_calls': 2})
    state['version'] += 1
    _need(len(_raw(state)) <= MAX_BYTES, 'state byte budget')
    return state


def replay_captures(identity, scope_ref, items, *, capabilities=('read', 'neuron')):
    """Replay explicit admission order; input is not sorted or silently deduped."""
    _capabilities(capabilities)
    _need(type(items) is list and len(items) <= MAX_CAPTURES, 'bounded source list')
    _need(len(_raw(items)) <= MAX_BYTES, 'source byte budget')
    state = initial_cognition(identity, scope_ref)
    for item in items:
        _append(state, item, capabilities)
    return state


def validate_cognition(state):
    _need(type(state) is dict and len(_raw(state)) <= MAX_BYTES, 'bounded state')
    _need({'identity', 'scope_ref', 'sources'} <= set(state), 'state fields')
    expected = replay_captures(state['identity'], state['scope_ref'], state['sources'])
    _need(_raw(expected) == _raw(state), 'derived state/source mismatch')
    return copy.deepcopy(state)


def apply_capture(state, item, *, capabilities=('read', 'neuron')):
    """Return a new state; exact duplicates are no-ops, failures leave input intact."""
    _capabilities(capabilities)
    checked = validate_cognition(state)
    return _append(checked, item, capabilities)


def validate_replay(state, items):
    validate_cognition(state)
    expected = replay_captures(state['identity'], state['scope_ref'], items)
    _need(_raw(expected) == _raw(state), 'external capture replay mismatch')
    return copy.deepcopy(state)
