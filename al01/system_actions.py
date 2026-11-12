"""Pure local protocol state machine for blueprint §26 action families.

No messages are sent and no environment is read or changed. A supplied approved
action ID is a caller assertion, not authentication or a capability grant. The
coordinator owns approval, clock, source resolution and any later authorized
transport. Replies remain untrusted claims, including synthetic interactions.
WAIT choice is recorded; polling neither fabricates cognition nor maturity.
"""
import copy
import hashlib
import json
import re

SCHEMA = 'local-system-actions-1'
ACTIONS = {'WAIT': 1, 'ASK_LOCAL': 2, 'REQUEST_HELP': 2, 'INTERACT': 2}
STATUSES = ('RESPONDED', 'DENIED', 'UNAVAILABLE', 'UNKNOWN')
MAX_REQUESTS = 32
MAX_PENDING = 4
MAX_EVENTS = 64
MAX_BYTES = 1024 * 1024
MAX_DEADLINE = 1024


class SystemActionError(ValueError):
    code = 'SYSTEM_ACTION_INVALID'


def _need(ok, reason):
    if not ok: raise SystemActionError(reason)


def _raw(value):
    try:
        nodes = 0; text_bytes = 0
        def visit(node, depth=0):
            nonlocal nodes, text_bytes
            nodes += 1
            _need(nodes <= 20000 and depth <= 16, 'JSON structural budget')
            if type(node) is dict:
                for key, child in node.items():
                    _need(type(key) is str, 'JSON string keys')
                    visit(key, depth+1); visit(child, depth+1)
            elif type(node) is list:
                for child in node: visit(child, depth+1)
            elif type(node) is str:
                _need(len(node) <= MAX_BYTES, 'string budget')
                text_bytes += len(node.encode('utf8'))
                _need(text_bytes <= MAX_BYTES, 'text byte budget')
            else:
                _need(node is None or type(node) is bool or type(node) is int and abs(node) < 2**53, 'JSON scalar type')
        visit(value)
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf8')
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise SystemActionError('strict JSON required') from exc
    _need(len(raw) <= MAX_BYTES, 'protocol byte budget')
    return raw


def digest(value): return hashlib.sha256(_raw(value)).hexdigest()
def _keys(value, fields): _need(type(value) is dict and set(value) == set(fields), 'exact fields required')
def _int(value, high=2**53-1): _need(type(value) is int and 0 <= value <= high, 'bounded integer required')
def _sha(value): _need(type(value) is str and re.fullmatch('[0-9a-f]{64}', value), 'SHA256 required')
def _text(value, size=4096):
    _need(type(value) is str and 1 <= len(value) <= size, 'bounded nonempty text required')
    _need(len(value.encode('utf8', errors='strict')) <= size, 'UTF8 text byte budget')
def _id(value):
    _need(type(value) is str and re.fullmatch('[A-Za-z][A-Za-z0-9_-]{0,127}', value), 'opaque identifier required')


def initialize(identity, scope_ref, local_recipients, *, budget=64):
    """Recipients map opaque IDs to LOCAL_USER, LOCAL_MODULE or SYNTHETIC_OBJECT.

    No teacher/network/address recipient type exists. All IDs are local labels,
    not names to resolve or destinations to contact. No initial I/O occurs.
    """
    _id(identity); _id(scope_ref); _int(budget, 64)
    _need(type(local_recipients) is dict and len(local_recipients) <= 16, 'recipient bound')
    for name, kind in local_recipients.items():
        _id(name); _need(kind in ('LOCAL_USER', 'LOCAL_MODULE', 'SYNTHETIC_OBJECT'), 'local recipient kind')
    return {'schema': SCHEMA, 'identity': identity, 'scope_ref': scope_ref,
            'local_recipients': copy.deepcopy(local_recipients), 'budget': budget,
            'spent': 0, 'version': 0, 'last_transition_time': 0, 'requests': {}, 'events': [],
            'teacher_enabled': False, 'cognition_ticks': 0, 'maturity_increment': 0,
            'authority': 'NONE_LOCAL_PROTOCOL_ONLY'}


def make_request(identity, scope_ref, action, request_id, source_sha256, *, goal_id, source_version,
                 deadline, recipient=None, query=None):
    """Query is exact scoped local text, never code, fact or capability.

    WAIT requires no recipient and uses query only as its recorded reason.
    Other actions require a configured recipient and bounded question/message.
    """
    _id(identity); _id(scope_ref); _id(request_id); _id(goal_id); _sha(source_sha256)
    _int(source_version); _int(deadline)
    _need(type(action) is str and action in ACTIONS, 'registered action')
    try: _text(query)
    except UnicodeError as exc: raise SystemActionError('invalid UTF8 query') from exc
    if action == 'WAIT': _need(recipient is None, 'WAIT has no recipient')
    else: _id(recipient)
    result = {'schema': SCHEMA, 'identity': identity, 'scope_ref': scope_ref,
              'action': action, 'request_id': request_id, 'source_sha256': source_sha256, 'goal_id': goal_id,
              'source_version': source_version, 'deadline': deadline, 'recipient': recipient,
              'query': query, 'logical_cost': ACTIONS[action]}
    # WAIT prose records a reason, not a typed condition. Changing that prose
    # cannot manufacture a new semantic intent or evade no-progress suppression.
    result['intent_key'] = digest({'identity': identity, 'scope_ref': scope_ref, 'goal_id': goal_id,
                                   'action': action, 'recipient': recipient,
                                   'query': '' if action == 'WAIT' else query})
    result['action_id'] = digest(result)
    return result


def validate_request(request):
    _keys(request, ('schema','identity','scope_ref','action','request_id','source_sha256','source_version',
                    'deadline','recipient','query','logical_cost','action_id','goal_id','intent_key'))
    expected = make_request(request['identity'], request['scope_ref'], request['action'], request['request_id'],
        request['source_sha256'], goal_id=request['goal_id'], source_version=request['source_version'], deadline=request['deadline'],
        recipient=request['recipient'], query=request['query'])
    _need(_raw(request) == _raw(expected), 'request type/cost/digest mismatch')
    return copy.deepcopy(request)


def make_response(request, *, response_id, responder, origin, status, text, source_sha256):
    validate_request(request); _id(response_id); _id(responder); _sha(source_sha256)
    _need(request['action'] != 'WAIT' and responder == request['recipient'], 'response target')
    _need(origin in ('LOCAL_USER', 'LOCAL_MODULE', 'SYNTHETIC_OBJECT'), 'response origin')
    _need(status in STATUSES, 'explicit response status')
    try: _text(text)
    except UnicodeError as exc: raise SystemActionError('invalid UTF8 response') from exc
    return {'schema': SCHEMA, 'response_id': response_id, 'request_id': request['request_id'],
            'request_sha256': digest(request), 'action_id': request['action_id'],
            'identity': request['identity'], 'scope_ref': request['scope_ref'],
            'responder': responder, 'origin': origin, 'status': status, 'text': text,
            'source_sha256': source_sha256, 'untrusted_data': True, 'fact_adopted': False}


def _event(state, kind, payload, now):
    _need(len(state['events']) < MAX_EVENTS, 'event budget')
    event = {'index': len(state['events']), 'kind': kind, 'time': now, 'payload': copy.deepcopy(payload),
             'previous': state['events'][-1]['hash'] if state['events'] else '0'*64}
    event['hash'] = digest(event); state['events'].append(event)
    state['version'] += 1; state['last_transition_time'] = now
    _raw(state)


def _clock(state, now):
    _int(now); _need(now >= state['last_transition_time'], 'clock predates last committed transition')


def _submit(state, request, now, approved_action_id):
    _clock(state, now); request = validate_request(request)
    _need(request['identity'] == state['identity'] and request['scope_ref'] == state['scope_ref'], 'identity/scope')
    _need(approved_action_id == request['action_id'], 'explicit matching approved action required')
    rid = request['request_id']
    if rid in state['requests']:
        _need(state['requests'][rid]['request'] == request, 'conflicting request ID')
        return state
    _need(not any(r['status'] == 'PENDING' and r['request']['intent_key'] == request['intent_key']
                  for r in state['requests'].values()), 'same intent already pending; new dispatch ID is not progress')
    _need(len(state['requests']) < MAX_REQUESTS, 'request capacity')
    _need(sum(r['status'] == 'PENDING' for r in state['requests'].values()) < MAX_PENDING, 'pending capacity')
    _need(now < request['deadline'] <= now + MAX_DEADLINE, 'finite future logical deadline')
    recipient = request['recipient']
    if recipient is not None: _need(recipient in state['local_recipients'], 'recipient outside configured local scope')
    cost = request['logical_cost']; _need(state['spent'] + cost <= state['budget'], 'logical budget exhausted')
    state['spent'] += cost
    state['requests'][rid] = {'request': request, 'status': 'PENDING', 'outcome': None,
                            'delivery_state': 'QUEUED_LOCAL_NOT_SENT'}
    _event(state, 'ACTION_REQUESTED', {'request': request, 'approved_action_id': approved_action_id}, now)
    return state


def _finish(state, rid, status, response, now):
    row = state['requests'][rid]; request = row['request']
    receipt = {'identity': state['identity'], 'scope_ref': state['scope_ref'], 'request_id': rid,
               'action_id': request['action_id'], 'request_sha256': digest(request),
               'goal_id': request['goal_id'], 'intent_key': request['intent_key'],
               'request_source_sha256': request['source_sha256'], 'status': status, 'time': now,
               'response': copy.deepcopy(response), 'verified_environment_effect': False,
               'cooperation_inferred': False, 'fact_adopted': False, 'maturity_increment': 0}
    receipt['receipt_sha256'] = digest(receipt)
    row['status'] = status; row['outcome'] = receipt
    row['delivery_state'] = 'RESPONSE_RECORDED_NO_DELIVERY_PROOF' if response is not None else 'CLOSED_WITHOUT_DELIVERY_PROOF'
    _event(state, 'ACTION_OUTCOME', {'receipt': receipt}, now)


def _respond(state, response, now):
    _clock(state, now)
    _keys(response, ('schema','response_id','request_id','request_sha256','action_id','identity','scope_ref',
                     'responder','origin','status','text','source_sha256','untrusted_data','fact_adopted'))
    rid = response['request_id']; _need(type(rid) is str and rid in state['requests'], 'unrequested response')
    row = state['requests'][rid]; request = row['request']
    expected = make_response(request, **{key: response[key] for key in
        ('response_id','responder','origin','status','text','source_sha256')})
    _need(_raw(expected) == _raw(response), 'response source/request/type binding')
    _need(state['local_recipients'][response['responder']] == response['origin'], 'origin cannot be promoted')
    if row['status'] != 'PENDING':
        _need(row['outcome']['response'] == response, 'terminal outcome cannot change or auto retry')
        return state
    _need(now < request['deadline'], 'late response; poll records timeout, no late adoption')
    for other in state['requests'].values():
        prior = other['outcome']
        _need(prior is None or prior['response'] is None or prior['response']['response_id'] != response['response_id'], 'response ID reuse')
    _finish(state, rid, response['status'], response, now)
    return state


def _poll(state, now):
    _clock(state, now)
    for rid, row in state['requests'].items():
        if row['status'] == 'PENDING' and now >= row['request']['deadline']:
            status = 'WAIT_ELAPSED' if row['request']['action'] == 'WAIT' else 'TIMED_OUT'
            _finish(state, rid, status, None, now)
    return state


def replay(identity, scope_ref, local_recipients, events, *, budget=64):
    state = initialize(identity, scope_ref, local_recipients, budget=budget)
    _need(type(events) is list and len(events) <= MAX_EVENTS, 'bounded event list'); _raw(events)
    for event in events:
        _keys(event, ('index','kind','time','payload','previous','hash'))
        _need(type(event['index']) is int and event['index'] == len(state['events']), 'event order')
        payload = event['payload']
        if event['kind'] == 'ACTION_REQUESTED':
            _keys(payload, ('request','approved_action_id'))
            _submit(state, payload['request'], event['time'], payload['approved_action_id'])
        elif event['kind'] == 'ACTION_OUTCOME':
            _keys(payload, ('receipt',)); receipt = payload['receipt']
            _need(type(receipt) is dict and 'request_id' in receipt and receipt['request_id'] in state['requests'], 'outcome target')
            rid = receipt['request_id']; row = state['requests'][rid]
            _need(row['status'] == 'PENDING', 'duplicate terminal event')
            if receipt.get('response') is not None: _respond(state, receipt['response'], event['time'])
            else:
                _clock(state, event['time']); _need(event['time'] >= row['request']['deadline'], 'premature timeout')
                status = 'WAIT_ELAPSED' if row['request']['action'] == 'WAIT' else 'TIMED_OUT'
                _finish(state, rid, status, None, event['time'])
        else: raise SystemActionError('unregistered event')
        _need(state['events'] and _raw(state['events'][-1]) == _raw(event), 'event hash/content mismatch')
    return state


def validate_state(state):
    _need(type(state) is dict, 'state'); _raw(state)
    _need({'identity','scope_ref','local_recipients','events','budget'} <= set(state), 'state fields')
    expected = replay(state['identity'], state['scope_ref'], state['local_recipients'], state['events'], budget=state['budget'])
    _need(_raw(state) == _raw(expected), 'state replay mismatch')
    return copy.deepcopy(state)


def submit(state, request, *, now, approved_action_id):
    """Pure queued intent only. Coordinator must durably save BEFORE any delivery.

    This API does not persist, deliver, or authorize notification transport.
    """
    return _submit(validate_state(state), request, now, approved_action_id)


def respond(state, response, *, now):
    return _respond(validate_state(state), response, now)


def poll(state, *, now):
    """No-op polling returns byte-identical state. No ticks or progress credited."""
    return _poll(validate_state(state), now)


def next_deadline(state):
    checked = validate_state(state)
    return min((r['request']['deadline'] for r in checked['requests'].values()
                if r['status'] == 'PENDING'), default=None)


def eligibility(state, *, now):
    """Advisory executor view, not permission to perform I/O or run two actions.

    A coordinator must also inspect its folder-request queue under the same
    atomic admission lock. A configured label proves no real human identity.
    """
    checked = validate_state(state); _clock(checked, now)
    pending = [(rid, r) for rid, r in checked['requests'].items() if r['status'] == 'PENDING']
    return {'pending_request_ids': [rid for rid, _ in pending],
            'pending_intent_keys': [r['request']['intent_key'] for _, r in pending],
            'terminal_intent_keys': sorted({r['request']['intent_key'] for r in checked['requests'].values() if r['status'] != 'PENDING'}),
            'due_request_ids': [rid for rid, r in pending if r['request']['deadline'] <= now],
            'next_deadline': min((r['request']['deadline'] for _, r in pending), default=None),
            'remaining_logical_budget': checked['budget']-checked['spent'],
            'exclusive_executor_busy': bool(pending),
            'can_admit_exclusive_action': not pending and len(checked['requests']) < MAX_REQUESTS and checked['spent'] < checked['budget'],
            'transport_available': False, 'notification_authorized': False,
            'identity_claim': 'configured local labels only; no verified human or cooperation'}
