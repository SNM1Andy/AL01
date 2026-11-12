"""Pure finite blocked-goal help policy, never an executor or evidence authority.

The runtime must derive the trigger from its committed blocked goal and archive,
authenticate every object reference, enforce the existing dispute-round budget,
and derive checks from ordinary approved LIST/READ receipts. This module checks
bounded value consistency only. A parsed reply is not a fact or a capability.
The original inspection goal is never mutated here, including on verification.
"""
import copy
import hashlib
import json
import re

from . import compact_interaction as local
from .folder_contracts import STATUSES, validate_ref, validate_relative_path

SCHEMA = 'compact-autonomous-help-1'
DIALECT = 'al01_source_hint_v1'
MAX_BYTES = 8192
MAX_TRIGGER_BYTES = 6144
MAX_INTENTS = 8
MAX_DEADLINE_SPAN = 32
MAX_COUNTER = 2**53 - 1
ROLES = ('EARLIER_SOURCE', 'LATER_SOURCE')
PHASES = ('PROPOSED', 'AWAITING_RESPONSE', 'AWAITING_LIST', 'AWAITING_READ', 'VERIFIED', 'FAILED')
REASONS = (None, 'DELIVERED', 'UNKNOWN', 'DENIED', 'UNAVAILABLE', 'DEADLINE',
           'STALE_GOAL', 'MALFORMED_HINT', 'NEGATIVE_HINT', 'UNTRUSTED_HINT',
           'CHECK_FAILED', 'SOURCE_NOT_VISIBLE', 'STALE_INVENTORY',
           'PRECONDITION_CHANGED', 'CHECKED_OPERATIONAL_ONLY')
GOAL_FIELDS = ('id', 'generation', 'episode_key', 'topic', 'bindings', 'cursor', 'receipts')
TRIGGER_FIELDS = ('identity', 'instance_id', 'scope_ref', 'goal', 'status', 'reason',
                  'source_workflow_ref', 'inventory_generation', 'source_sha256',
                  'role', 'goal_index', 'path', 'semantic_epoch', 'semantic_round',
                  'source_content_sha256')
EPISODE_FIELDS = ('trigger', 'endpoint', 'created_at', 'deadline', 'request_text',
                  'request_id', 'request_ref', 'phase', 'checks', 'response_ref',
                  'check_refs', 'reason')
CHECK_FIELDS = ('operation', 'status', 'request_id', 'receipt_sha256', 'capture_ref',
                'inventory_generation', 'path', 'original_preconditions')


def need(ok, reason):
    if not ok:
        raise ValueError('COMPACT_HELP_INVALID: ' + reason)


def _keys(value, fields):
    need(type(value) is dict and set(value) == set(fields), 'exact fields')


def _integer(value, low=0, high=MAX_COUNTER):
    need(type(value) is int and low <= value <= high, 'bounded integer')


def _text(value, maximum):
    need(type(value) is str and 1 <= len(value) <= maximum, 'bounded text')
    try:
        size = len(value.encode('utf8'))
    except UnicodeError:
        need(False, 'strict UTF-8')
    need(size <= maximum, 'text byte bound')


def _sha(value):
    need(type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None, 'digest')


def _encode(value, limit=MAX_BYTES):
    try:
        raw = json.dumps(value, sort_keys=True, separators=(',', ':'),
                         ensure_ascii=False, allow_nan=False).encode('utf8')
    except (TypeError, ValueError, UnicodeError, RecursionError):
        need(False, 'bounded JSON')
    need(len(raw) <= limit, 'byte budget')
    return raw


def _digest(value):
    return hashlib.sha256(_encode(value)).hexdigest()


def _ref(value, trigger, kind, limit):
    # No I/O and no authentication: the runtime must resolve the actual frame.
    _keys(value, ('schema', 'identity', 'instance_id', 'kind', 'encoding',
                  'payload_bytes', 'sha256', 'byte_size'))
    need(value['schema'] == 'life-object-1' and value['kind'] == kind
         and value['encoding'] == 'json', 'typed immutable reference')
    need(all(value[k] == trigger[k] for k in ('identity', 'instance_id')), 'reference namespace')
    _sha(value['sha256'])
    _integer(value['payload_bytes'], 1, limit)
    _integer(value['byte_size'], 1, limit + 1025)
    need(value['payload_bytes'] < value['byte_size'] <= value['payload_bytes'] + 1025,
         'reference frame bounds')


def validate_trigger(trigger):
    _keys(trigger, TRIGGER_FIELDS)
    _text(trigger['identity'], 128)
    need(type(trigger['instance_id']) is str
         and re.fullmatch('[0-9a-f]{32}', trigger['instance_id']) is not None, 'instance')
    validate_ref(trigger['scope_ref'], 'scope')
    need(trigger['status'] == 'BLOCKED' and type(trigger['reason']) is str
         and trigger['reason'] in ('SOURCE_NOT_CURRENTLY_VISIBLE', 'DENIED_OR_FAILED_SOURCE'),
         'actual unfinished source block only')
    goal = trigger['goal']
    _keys(goal, GOAL_FIELDS)
    _text(goal['id'], 256)
    _integer(goal['generation'], 1)
    _sha(goal['episode_key'])
    _sha(goal['topic'])
    _keys(goal['bindings'], ROLES)
    for index in goal['bindings'].values():
        _integer(index, 0, 7)
    need(len(set(goal['bindings'].values())) == 2, 'distinct existing source bindings')
    _integer(goal['cursor'], 0, 1)
    need(type(goal['receipts']) is list and len(goal['receipts']) == goal['cursor'],
         'preserved receipt-driven cursor')
    for ref in goal['receipts']:
        _ref(ref, trigger, 'compact_capture', 400000)
    need(type(trigger['role']) is str and trigger['role'] in ROLES, 'existing source role')
    _integer(trigger['goal_index'], 0, 7)
    need(goal['bindings'][trigger['role']] == trigger['goal_index'], 'original role binding')
    validate_relative_path(trigger['path'])
    need(trigger['path'].lower().endswith('.txt'), 'existing text-source path')
    _integer(trigger['inventory_generation'])
    _sha(trigger['semantic_epoch'])
    _integer(trigger['semantic_round'], 1, 1)
    _sha(trigger['source_sha256'])
    _sha(trigger['source_content_sha256'])
    _ref(trigger['source_workflow_ref'], trigger, 'compact_workflow', 65536)
    _encode(trigger, MAX_TRIGGER_BYTES)
    return copy.deepcopy(trigger)


def semantic_key(trigger):
    """A recreated goal, new deadline, endpoint or journal head cannot retry."""
    trigger = validate_trigger(trigger)
    goal = trigger['goal']
    return _digest(dict(namespace={k: trigger[k] for k in ('identity', 'instance_id', 'scope_ref')},
                        semantic_epoch=trigger['semantic_epoch'], topic=goal['topic'],
                        bindings=goal['bindings'], role=trigger['role']))


def _request_text(trigger):
    goal = trigger['goal']
    return _encode(dict(kind='SOURCE_RECHECK_REQUEST', goal_id=goal['id'],
                        generation=goal['generation'], role=trigger['role'],
                        source_sha256=trigger['source_sha256']), 1024).decode('utf8')


def initial_help():
    return dict(schema=SCHEMA, episode=None, used_goals=[])


def _validate_check(check, trigger):
    _keys(check, CHECK_FIELDS)
    need(type(check['operation']) is str and check['operation'] in ('LIST_SCOPE', 'READ_TEXT'),
         'ordinary observation only')
    need(type(check['status']) is str and check['status'] in STATUSES, 'typed folder status')
    validate_ref(check['request_id'], 'req')
    _sha(check['receipt_sha256'])
    _ref(check['capture_ref'], trigger, 'compact_capture', 400000)
    _integer(check['inventory_generation'])
    if check['path'] is not None:
        validate_relative_path(check['path'])
    need(type(check['original_preconditions']) is bool, 'explicit original preconditions')
    _encode(check)


def validate_help(state):
    _keys(state, ('schema', 'episode', 'used_goals'))
    need(state['schema'] == SCHEMA, 'state schema')
    used = state['used_goals']
    need(type(used) is list and len(used) <= MAX_INTENTS, 'permanent intent capacity')
    for key in used:
        _sha(key)
    need(len(set(used)) == len(used), 'distinct permanent semantic keys')
    episode = state['episode']
    if episode is not None:
        _keys(episode, EPISODE_FIELDS)
        trigger = validate_trigger(episode['trigger'])
        need(semantic_key(trigger) in used, 'admitted semantic key')
        need(type(episode['endpoint']) is str
             and re.fullmatch('[a-z_][a-z0-9_]{0,127}', episode['endpoint']) is not None,
             'registered endpoint token shape')
        _integer(episode['created_at'])
        _integer(episode['deadline'], 1)
        need(1 <= episode['deadline'] - episode['created_at'] <= MAX_DEADLINE_SPAN, 'bounded deadline')
        need(episode['request_text'] == _request_text(trigger), 'derived request text')
        need(type(episode['phase']) is str and episode['phase'] in PHASES, 'finite phase')
        need(episode['reason'] is None or (type(episode['reason']) is str
             and episode['reason'] in REASONS), 'finite reason')
        _integer(episode['checks'], 0, 2)
        need(type(episode['check_refs']) is list
             and len(episode['check_refs']) == episode['checks'], 'bounded check chain')
        checks = episode['check_refs']
        for index, check in enumerate(checks):
            _validate_check(check, trigger)
            need(check['operation'] == ('LIST_SCOPE', 'READ_TEXT')[index], 'LIST then READ')
        need(len({c['request_id'] for c in checks}) == len(checks)
             and len({c['receipt_sha256'] for c in checks}) == len(checks)
             and len({c['capture_ref']['sha256'] for c in checks}) == len(checks), 'distinct checks')
        if episode['phase'] == 'PROPOSED':
            need(episode['request_id'] is None and episode['request_ref'] is None
                 and episode['response_ref'] is None and not checks, 'proposal is not dispatch')
        else:
            need(type(episode['request_id']) is str
                 and re.fullmatch('local_[0-9a-f]{64}', episode['request_id']) is not None,
                 'exact local request id')
            _ref(episode['request_ref'], trigger, 'compact_local_request', local.MAX_BYTES)
        if episode['response_ref'] is not None:
            _ref(episode['response_ref'], trigger, 'compact_local_outcome', 16384)
        if episode['phase'] in ('AWAITING_LIST', 'AWAITING_READ', 'VERIFIED'):
            need(episode['response_ref'] is not None, 'recorded untrusted hint required')
        if episode['phase'] in ('PROPOSED', 'AWAITING_RESPONSE', 'AWAITING_LIST'):
            need(not checks, 'no observation before hint')
        if episode['phase'] in ('AWAITING_READ', 'VERIFIED'):
            need(len(checks) == (1 if episode['phase'] == 'AWAITING_READ' else 2), 'checked phase cursor')
        if len(checks) == 2 or episode['phase'] == 'AWAITING_READ':
            first = checks[0]
            need(first['status'] == 'OK' and first['path'] == trigger['path']
                 and first['inventory_generation'] > trigger['inventory_generation']
                 and first['original_preconditions'], 'independently refreshed original binding')
        if episode['phase'] == 'VERIFIED':
            last = checks[1]
            need(last['status'] == 'OK' and last['path'] == trigger['path']
                 and last['inventory_generation'] == checks[0]['inventory_generation']
                 and last['original_preconditions'], 'actual original source inspection only')
    _encode(state)
    return copy.deepcopy(state)



def _admit_completion_envelope(state):
    """Reserve worst-case retained value bytes before an external callback.

    A legal escaped identity can be much larger in JSON than its UTF-8 length.
    Account for the request/outcome and both future check references, not just
    the proposal currently present. This is value admission, not object I/O.
    """
    future = copy.deepcopy(state)
    episode = future['episode']
    trigger = episode['trigger']

    def maximum_ref(kind, payload_bytes):
        return dict(schema='life-object-1', identity=trigger['identity'],
                    instance_id=trigger['instance_id'], kind=kind, encoding='json',
                    payload_bytes=payload_bytes, byte_size=payload_bytes + 1025,
                    sha256='f' * 64)

    episode.update(request_id='local_' + 'f' * 64,
                   request_ref=maximum_ref('compact_local_request', local.MAX_BYTES),
                   response_ref=maximum_ref('compact_local_outcome', 16384),
                   phase='AWAITING_RESPONSE', checks=2, reason='CHECKED_OPERATIONAL_ONLY')
    episode['check_refs'] = [dict(operation=operation, status='LIMIT_EXCEEDED',
        request_id='req_' + 'f' * 32, receipt_sha256='f' * 64,
        capture_ref=maximum_ref('compact_capture', 400000),
        inventory_generation=MAX_COUNTER, path=trigger['path'], original_preconditions=False)
        for operation in ('LIST_SCOPE', 'READ_TEXT')]
    # Filling remaining keys is deliberately conservative for every episode.
    future['used_goals'] += ['f' * 64] * (MAX_INTENTS - len(future['used_goals']))
    _encode(future)


def propose_help(state, trigger, endpoint, now, deadline_span, *, registry, capabilities, check_budget):
    """Return one derived proposal or unchanged state/None; never publish or send.

    Initiation consumes the one remaining round of the original semantic
    dispute; LIST and READ are that round's two action steps. The runtime must
    atomically consume round two with publication. A round-two trigger is
    ineligible; no new episode, reset or retry can create another round.
    """
    state = validate_help(state)
    trigger = validate_trigger(trigger)
    _integer(now)
    _integer(deadline_span, 1, MAX_DEADLINE_SPAN)
    need(now + deadline_span <= MAX_COUNTER, 'deadline overflow')
    entries = local.validate_registry(registry)
    need(type(capabilities) is list and len(capabilities) <= 16
         and all(type(c) is str and 1 <= len(c) <= 128 for c in capabilities)
         and len(set(capabilities)) == len(capabilities), 'bounded explicit capabilities')
    _integer(check_budget, 0, 2)
    key = semantic_key(trigger)
    episode = state['episode']
    if (check_budget != 2 or not {'request_help', 'read', 'observe'} <= set(capabilities)
            or not any(e['target'] == endpoint and 'request_help' in e['actions'] for e in entries)
            or key in state['used_goals'] or len(state['used_goals']) >= MAX_INTENTS
            or (episode is not None and episode['phase'] not in ('VERIFIED', 'FAILED'))):
        return state, None
    text = _request_text(trigger)
    state['used_goals'].append(key)
    state['episode'] = dict(trigger=trigger, endpoint=endpoint, created_at=now,
                           deadline=now + deadline_span, request_text=text,
                           request_id=None, request_ref=None, phase='PROPOSED',
                           checks=0, response_ref=None, check_refs=[], reason=None)
    _admit_completion_envelope(state)
    return validate_help(state), text


def bind_request(state, exactrequest, ref):
    """Bind the ordinary local request after its immutable publication."""
    state = validate_help(state)
    request = local.validate_local_request(exactrequest)
    episode = state['episode']
    need(episode is not None, 'existing proposal')
    trigger = episode['trigger']
    _ref(ref, trigger, 'compact_local_request', local.MAX_BYTES)
    need(all(request[k] == trigger[k] for k in ('identity', 'instance_id', 'scope_ref'))
         and request['goal'] == {k: trigger['goal'][k] for k in ('id', 'generation')}
         and request['action'] == 'request_help' and request['target'] == episode['endpoint']
         and request['request'] == episode['request_text']
         and request['source_sha256'] == trigger['source_sha256']
         and request['created_at'] == episode['created_at']
         and request['deadline'] == episode['deadline'], 'exact derived local request')
    if episode['request_id'] is not None:
        need(episode['request_id'] == request['request_id'] and episode['request_ref'] == ref,
             'conflicting request rebind')
        return state
    need(episode['phase'] == 'PROPOSED', 'one request binding')
    episode.update(request_id=request['request_id'], request_ref=copy.deepcopy(ref), phase='AWAITING_RESPONSE')
    return validate_help(state)


def _pairs(pairs):
    value = {}
    for key, item in pairs:
        need(key not in value, 'duplicate hint key')
        value[key] = item
    return value


def _hint(response, trigger):
    _text(response, 1024)
    value = json.loads(response, object_pairs_hook=_pairs,
                       parse_constant=lambda _: need(False, 'nonfinite hint'))
    _keys(value, (DIALECT,))
    hint = value[DIALECT]
    _keys(hint, ('goal_id', 'generation', 'role', 'available'))
    _text(hint['goal_id'], 256)
    _integer(hint['generation'], 1)
    need(type(hint['role']) is str and hint['role'] in ROLES, 'hint role')
    need(type(hint['available']) is bool, 'typed availability hypothesis')
    need(hint['goal_id'] == trigger['goal']['id']
         and hint['generation'] == trigger['goal']['generation']
         and hint['role'] == trigger['role'], 'hint source identity')
    return hint


def assess_response(state, request_id, outcome, now, outcome_ref, *, current_goal, current_episode_key):
    """A timely exact hint permits checks only; it never resumes the goal."""
    state = validate_help(state)
    episode = state['episode']
    need(episode is not None and episode['request_id'] is not None, 'bound request required')
    trigger = episode['trigger']
    need(request_id == episode['request_id'], 'exact admitted request')
    _integer(now)
    need(now >= episode['created_at'], 'monotonic response time')
    _keys(outcome, ('schema', 'request_id', 'endpoint', 'goal_generation', 'type', 'status', 'response', 'reason'))
    need(outcome['schema'] == local.OUTCOME_SCHEMA and outcome['type'] == local.REPLY_TYPE
         and outcome['request_id'] == request_id and outcome['endpoint'] == episode['endpoint']
         and type(outcome['goal_generation']) is int
         and outcome['goal_generation'] == trigger['goal']['generation'], 'exact callback correlation')
    need(type(outcome['status']) is str and outcome['status'] in local.OUTCOMES, 'outcome status')
    if outcome['status'] == 'RESPONDED':
        _text(outcome['response'], 1024)
    else:
        need(outcome['response'] is None, 'only response carries text')
    if outcome['reason'] is not None:
        _text(outcome['reason'], 1024)
    _ref(outcome_ref, trigger, 'compact_local_outcome', 16384)
    _encode(outcome)
    # Local interaction handles duplicate/conflicting evidence. A late distinct
    # callback cannot reopen checks or rewrite an already terminal assessment.
    if episode['phase'] != 'AWAITING_RESPONSE':
        return state
    episode['response_ref'] = copy.deepcopy(outcome_ref)
    same = False
    if (type(current_goal) is dict and current_goal.get('status') == 'BLOCKED'
            and all(k in current_goal for k in GOAL_FIELDS)
            and current_episode_key == trigger['goal']['episode_key']):
        try:
            same = _encode({k: current_goal[k] for k in GOAL_FIELDS}) == _encode(trigger['goal'])
        except ValueError:
            pass
    if not same:
        episode.update(phase='FAILED', reason='STALE_GOAL')
    elif now >= episode['deadline']:
        episode.update(phase='FAILED', reason='DEADLINE')
    elif outcome['status'] in ('DENIED', 'UNAVAILABLE'):
        episode.update(phase='FAILED', reason=outcome['status'])
    elif outcome['status'] in ('DELIVERED', 'UNKNOWN'):
        # No timeout or acknowledgement grants another send. The local layer
        # retains its unresolved slot; later exact terminal evidence may arrive.
        if episode['reason'] != 'UNKNOWN' or outcome['status'] == 'UNKNOWN':
            episode['reason'] = outcome['status']
    else:
        try:
            hint = _hint(outcome['response'], trigger)
        except (ValueError, TypeError, UnicodeError, RecursionError):
            episode.update(phase='FAILED', reason='MALFORMED_HINT')
        else:
            episode.update(phase='AWAITING_LIST' if hint['available'] else 'FAILED',
                           reason='UNTRUSTED_HINT' if hint['available'] else 'NEGATIVE_HINT')
    return validate_help(state)


def record_check(state, check):
    """Fold a runtime-verified capture summary, never helper testimony.

    original_preconditions is a runtime-derived predicate, not an external hint.
    Actual captures, approval/receipt ancestry and current-goal checks remain the
    runtime's responsibility. An OK READ verifies inspection, never file truth.
    """
    state = validate_help(state)
    episode = state['episode']
    need(episode is not None, 'existing help episode')
    trigger = episode['trigger']
    _validate_check(check, trigger)
    for previous in episode['check_refs']:
        if (previous['request_id'] == check['request_id']
                or previous['receipt_sha256'] == check['receipt_sha256']
                or previous['capture_ref']['sha256'] == check['capture_ref']['sha256']):
            need(previous == check, 'conflicting repeated check')
            return state
    need(episode['phase'] in ('AWAITING_LIST', 'AWAITING_READ'), 'check not eligible')
    expected = 'LIST_SCOPE' if episode['phase'] == 'AWAITING_LIST' else 'READ_TEXT'
    need(check['operation'] == expected and episode['checks'] < 2, 'one LIST then one READ')
    episode['check_refs'].append(copy.deepcopy(check))
    episode['checks'] += 1
    reason = None
    if check['status'] != 'OK':
        reason = 'CHECK_FAILED'
    elif check['path'] != trigger['path']:
        reason = 'SOURCE_NOT_VISIBLE'
    elif not check['original_preconditions']:
        reason = 'PRECONDITION_CHANGED'
    elif ((expected == 'LIST_SCOPE' and check['inventory_generation'] <= trigger['inventory_generation'])
          or (expected == 'READ_TEXT' and check['inventory_generation'] != episode['check_refs'][0]['inventory_generation'])):
        reason = 'STALE_INVENTORY'
    if reason is not None:
        episode.update(phase='FAILED', reason=reason)
    elif expected == 'LIST_SCOPE':
        episode.update(phase='AWAITING_READ', reason='UNTRUSTED_HINT')
    else:
        episode.update(phase='VERIFIED', reason='CHECKED_OPERATIONAL_ONLY')
    return validate_help(state)
