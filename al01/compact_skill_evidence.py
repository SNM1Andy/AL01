"""Pure bounded evidence inspection; no runtime, learning, I/O or adoption.

A pinned journal checkpoint is a trusted archive integrity anchor, not host
authentication. No owning reducer is replayed here. Missing witnesses stay
UNKNOWN; invalid hashes/types/bindings raise SkillEvidenceError.
"""
import copy
import hashlib
import json

from . import compact_procedures as procedures
from . import compact_interaction as interaction
from .compact_observer import validate_capture
from .core import CVM, transaction_payload, CLError
from .folder_contracts import validate_request
from .folder_session import _hash, _encode
from .life_objects import LifeObjects, _load, _json, MAX_HEADER

SCHEMA = 'compact-skill-experience-evidence-1'
MAX_OBJECTS = 64
MAX_FRAME_BYTES = 401024
MAX_TOTAL_BYTES = 4 * 1024 * 1024
MAX_JOURNAL_FILE_BYTES = 512 * 1024
MAX_EVENTS = 128
_UNKNOWN = 'UNKNOWN'


class SkillEvidenceError(ValueError):
    pass


class _Unavailable(SkillEvidenceError):
    pass


def need(ok, reason):
    if not ok:
        raise SkillEvidenceError(reason)


class _Objects:
    def __init__(self, origin, read_frame):
        need(type(origin) is dict and set(origin) == {'identity', 'instance_id', 'scope_ref'}, 'ORIGIN_FIELDS')
        need(callable(read_frame), 'FRAME_READER_REQUIRED')
        self.origin, self.reader, self.cache, self.bytes = origin, read_frame, {}, 0
        # Only pure reference/frame methods; never LifeObjects.__init__/read/I/O.
        self.codec = object.__new__(LifeObjects)
        self.codec.identity, self.codec.instance_id = origin['identity'], origin['instance_id']

    def read(self, ref, kind):
        self.codec._reference(ref, kind)
        need(ref['encoding'] == 'json' and ref['byte_size'] <= MAX_FRAME_BYTES, 'FRAME_BOUND_OR_ENCODING')
        key = (kind, ref['sha256'])
        if key in self.cache:
            value, actual = self.cache[key]
            need(_encode(ref) == _encode(actual), 'REFERENCE_ALIAS')
            return copy.deepcopy(value)
        if len(self.cache) >= MAX_OBJECTS:
            raise _Unavailable('OBJECT_BUDGET')
        # Reserve the declared bounded frame before invoking its reader/hash.
        # The actual length must still match this declaration below.
        if self.bytes + ref['byte_size'] > MAX_TOTAL_BYTES:
            raise _Unavailable('TOTAL_FRAME_BUDGET')
        try:
            raw = self.reader(copy.deepcopy(ref))
        except FileNotFoundError as error:
            raise _Unavailable('MISSING_OBJECT') from error
        need(type(raw) is bytes and len(raw) == ref['byte_size']
             and hashlib.sha256(raw).hexdigest() == ref['sha256'], 'FRAME_CONTENT_ADDRESS')
        self.bytes += len(raw)
        if self.bytes > MAX_TOTAL_BYTES:
            raise _Unavailable('TOTAL_FRAME_BUDGET')
        header, sep, payload = raw.partition(b'\n')
        need(sep and len(header) <= MAX_HEADER, 'FRAME_HEADER')
        h, value = _load(header), _load(payload)
        need(_json(value) == payload, 'CANONICAL_PAYLOAD')
        expected, actual = self.codec._frame(kind, payload, 'json')
        need(raw == expected and _encode(actual) == _encode(ref)
             and h['payload_sha256'] == hashlib.sha256(payload).hexdigest(), 'FRAME_TYPE_OR_PAYLOAD')
        self.cache[key] = (value, actual)
        return copy.deepcopy(value)


def _journal_bytes(raw):
    need(type(raw) is bytes and len(raw) <= MAX_JOURNAL_FILE_BYTES, 'JOURNAL_FILE_BUDGET')


def _envelope(raw):
    _journal_bytes(raw)
    x = _load(raw)
    need(type(x) is dict and set(x) == {'data', 'sha256'} and _hash(x['data']) == x['sha256'], 'JOURNAL_DIGEST')
    return x['data']


def _journal(proof, origin):
    need(type(proof) is dict and set(proof) == {'birth', 'checkpoint', 'checkpoint_sha256', 'previous_segment'}, 'JOURNAL_PROOF_FIELDS')
    # Check every raw envelope before any journal hash/parse, including the
    # pinned checkpoint hash which previously preceded _envelope's guard.
    _journal_bytes(proof['birth'])
    _journal_bytes(proof['checkpoint'])
    if proof['previous_segment'] is not None:
        _journal_bytes(proof['previous_segment'])
    raw = proof['checkpoint']
    need(hashlib.sha256(raw).hexdigest() == proof['checkpoint_sha256'], 'PINNED_CHECKPOINT')
    birth, checkpoint = _envelope(proof['birth']), _envelope(raw)
    need(birth['schema'] == 'life-journal-1' and birth['identity'] == origin['identity']
         and birth['instance_id'] == origin['instance_id']
         and birth['initial_state']['scope_ref'] == origin['scope_ref'], 'JOURNAL_ORIGIN')
    need(checkpoint['schema'] == 'life-journal-1' and checkpoint['birth_sha256'] == _hash(birth), 'CHECKPOINT_BIRTH')
    sealed = 'events' in checkpoint
    events = checkpoint['events'] if sealed else checkpoint['active']
    need(type(events) is list and len(events) <= 64, 'CHECKPOINT_EVENTS')
    endpoint = checkpoint['anchor'] if sealed else checkpoint
    need(all(endpoint['state'][k] == v for k, v in origin.items()), 'CHECKPOINT_STATE_ORIGIN')
    if sealed:
        need(checkpoint['identity'] == origin['identity'] and type(checkpoint['index']) is int
             and len(events) == 64 and checkpoint['start'] == checkpoint['index'] * 64 + 1
             and checkpoint['end'] == (checkpoint['index'] + 1) * 64
             and endpoint['seq'] == checkpoint['end'] and events[0]['seq'] == checkpoint['start'], 'SEALED_RANGE')
    else:
        need(type(checkpoint['segment_count']) is int and checkpoint['segment_count'] >= 0
             and checkpoint['seq'] == checkpoint['segment_count'] * 64 + len(events), 'HEAD_RANGE')
    base_ref = None
    base_known = (checkpoint['index'] == 0 if sealed else checkpoint['segment_count'] == 0)
    previous = proof['previous_segment']
    if previous is not None:
        segment = _envelope(previous)
        parent = checkpoint['previous_segment'] if sealed else checkpoint['segment_head']
        index = checkpoint['index'] - 1 if sealed else checkpoint['segment_count'] - 1
        need(segment['schema'] == 'life-journal-1' and segment['identity'] == origin['identity']
             and segment['birth_sha256'] == _hash(birth) and segment['index'] == index
             and _hash(segment) == parent and len(segment['events']) == 64, 'PREVIOUS_SEGMENT_BINDING')
        events = segment['events'] + events
        base_known = index == 0
    need(len(events) <= MAX_EVENTS, 'SPAN_EVENT_BUDGET')
    need(type(endpoint['seq']) is int and (events or endpoint['seq'] == 0), 'CHECKPOINT_RANGE')
    prior = None
    for e in events:
        need(type(e) is dict and set(e) == {'schema', 'identity', 'instance_id', 'seq', 'perception_count',
             'kind', 'payload', 'effect', 'previous', 'sha256'}, 'EVENT_FIELDS')
        need(len(_encode(e)) <= 4096 and e['schema'] == 'life-journal-1'
             and e['identity'] == origin['identity'] and e['instance_id'] == origin['instance_id']
             and type(e['seq']) is int and type(e['perception_count']) is int
             and _hash({k: v for k, v in e.items() if k != 'sha256'}) == e['sha256'], 'EVENT_INTEGRITY')
        if prior is not None:
            need(e['seq'] == prior['seq'] + 1 and e['previous'] == prior['sha256'], 'EVENT_CHRONOLOGY')
        prior = e
    if events:
        need(events[-1]['seq'] == endpoint['seq'] and events[-1]['sha256'] == endpoint['event_head'], 'CHECKPOINT_ENDPOINT')
        if base_known:
            need(events[0]['seq'] == 1 and events[0]['previous'] == '0' * 64, 'BIRTH_EVENT_ANCHOR')
    if base_known:
        base_ref = birth['initial_state'].get('workflow_ref')
    return events, endpoint['seq'], base_ref, base_known


def _decision(objects, ref, origin):
    local = ref['kind'] == 'compact_local_decision'
    d = objects.read(ref, 'compact_local_decision' if local else 'compact_decision')
    source, q = d['source'], d['request']
    need(all(source[k] == v for k, v in origin.items()), 'DECISION_ORIGIN')
    need(type(source['global_seq']) is int and type(source['cognition_revision']) is int
         and type(source['inventory_generation']) is int, 'DECISION_VERSION_TYPES')
    if local:
        interaction.validate_local_request(q)
        need(all(q[k] == v for k, v in origin.items()), 'LOCAL_REQUEST_ORIGIN')
        need(objects.read(d['local_request_ref'], 'compact_local_request') == q, 'LOCAL_REQUEST_OBJECT')
        expected = 'PROPOSE(' + q['action'].upper() + '(@' + q['target'] + ',' + json.dumps(q['request'], ensure_ascii=False) + '));'
        proposals = [dict(kind=q['action'], target=q['target'], request=q['request'])]
        target, caps = q['target'], source['capabilities']
    else:
        validate_request(q)
        need(q['scope_ref'] == origin['scope_ref'] and q['operation'] in ('READ_TEXT', 'LIST_SCOPE'), 'FOLDER_REQUEST_SCOPE')
        target, caps = q['entry_ref'] or q['scope_ref'], ('read', 'observe')
        need(set(caps) <= set(source['capabilities']), 'SOURCE_CAPABILITIES')
        expected = f'PROPOSE(OBSERVE(@{target}));'
        proposals = [dict(kind='observe', target=target)]
    need(d['generated_cl'] == expected, 'CL_REQUEST_BINDING')
    vm = CVM(caps)
    tx = vm.prepare(d['generated_cl'], dict(version=source['cognition_revision'], world_version=source['inventory_generation']), [target])
    vm.verify(tx)
    need(_encode(transaction_payload(tx)) == _encode(d['transaction_payload'])
         and d['transaction_payload']['proposals'] == proposals, 'COMPLETE_CL_TRANSACTION')
    return d, local


def _path(objects, d, origin, capture_origin):
    listing = objects.read(d['source']['list_receipt_ref'], 'compact_capture')
    validate_capture(listing)
    need(listing['request']['operation'] == 'LIST_SCOPE' and listing['request']['scope_ref'] == origin['scope_ref']
         and listing['capture']['capture_origin'] == capture_origin and listing['capture']['result']['status'] == 'OK', 'SOURCE_LIST')
    entries = listing['capture']['result']['payload']['entries']
    need(_hash(entries) == d['source']['visible_sha256'], 'SOURCE_VISIBLE_DIGEST')
    matches = [e for e in entries if e['kind'] == 'file' and e['entry_ref'] == d['request']['entry_ref']]
    need(len(matches) == 1, 'SOURCE_LIST_PATH')
    path = matches[0]['relative_path']
    need([m['path'] for m in d['candidate_metadata'] if m['entry_ref'] == d['request']['entry_ref']] == [path], 'DECISION_PATH_BINDING')
    return path


def _same_goal(a, b):
    return a is not None and b is not None and all(_encode(a[k]) == _encode(b[k])
        for k in ('id', 'generation', 'topic', 'bindings', 'episode_key'))


def _completed_goal(goal, observed):
    """A recorded SATISFIED bit alone is not a verified completion boundary."""
    successful = [r for r in observed if r['status'] == 'OK' and r['help_check'] is None]
    return (goal is not None and goal['status'] == 'SATISFIED' and goal['cursor'] == 2
            and len(successful) == 2 and [r['cursor'] for r in successful] == [0, 1]
            and {r['role'] for r in successful} == set(procedures.ROLES)
            and goal['receipts'] == [r['capture_ref'] for r in successful])


def _verify_experience(first_decision_ref, *, expected_origin, read_frame, before_state=None,
                      journal_proof=None, end_seq=None):
    """Inspect one goal from its first real investigation (cursor0).

    read_frame(ref)->raw bytes, with no trusted decoded-object shortcuts.
    before_state is an optional full state whose hash must equal the original
    decision source hash. journal_proof is a pinned head/sealed segment plus
    at most one preceding segment, never a caller 'complete'/'independent' flag.
    Invalid evidence raises SkillEvidenceError (or owning contract ValueError/
    CLError); absent evidence produces explicit UNKNOWN fields.
    """
    objects = _Objects(copy.deepcopy(expected_origin), read_frame)
    origin = objects.origin
    d, local = _decision(objects, first_decision_ref, origin)
    info = d.get('investigation')
    need(not local and info is not None and type(info['cursor']) is int and info['cursor'] == 0
         and info['role'] in procedures.ROLES and d.get('help_check') is None, 'FIRST_INVESTIGATION_REQUIRED')
    result = dict(schema=SCHEMA, first_decision_ref=copy.deepcopy(first_decision_ref),
        origin=origin, features={r: dict(value=_UNKNOWN, reason='MISSING_BEFORE_STATE') for r in procedures.ROLES},
        outcome=_UNKNOWN, reasons=[], actual_receipts=[], assistance=_UNKNOWN, independence=_UNKNOWN,
        span=dict(value=_UNKNOWN, reason='MISSING_JOURNAL_PROOF'), authority='NONE')
    state, goal, feature_witnesses = None, None, {}
    if before_state is not None:
        need(len(_json(before_state, 65536)) <= 65536 and _hash(before_state) == d['source']['state_sha256'], 'BEFORE_STATE_HASH')
        state = copy.deepcopy(before_state)
        need(all(state[k] == v for k, v in origin.items()) and state['pending'] is None, 'BEFORE_STATE_ORIGIN')
        w = procedures.validate_workflow(objects.read(state['workflow_ref'], 'compact_workflow'))
        goal = w['goal']
        need(goal is not None and goal['id'] == info['goal_id'] and goal['episode_key'] == info['episode_key']
             and goal['cursor'] == 0 and goal['receipts'] == [], 'BEFORE_GOAL_BINDING')
        inv = state['inventory']
        need(inv['valid'] is True and _encode(inv['list_receipt_ref']) == _encode(d['source']['list_receipt_ref'])
             and _hash(inv['entries']) == d['source']['visible_sha256']
             and type(inv['generation']) is int and inv['generation'] == d['source']['inventory_generation']
             and state['cognition_revision'] == d['source']['cognition_revision'], 'BEFORE_INVENTORY_VERSION')
        for role in procedures.ROLES:
            path = state['goals'][goal['bindings'][role]]['path']
            entries = [e for e in inv['entries'] if e['kind'] == 'file' and e['relative_path'] == path]
            latest = state['latest_by_path'].get(path)
            if len(entries) != 1 or latest is None or latest['last_status'] != 'OK':
                result['features'][role] = dict(value=_UNKNOWN, reason='NO_CURRENT_SUCCESSFUL_READ')
                continue
            prior = objects.read(latest['last_receipt_ref'], 'compact_capture')
            validate_capture(prior)
            need(prior['request']['operation'] == 'READ_TEXT' and prior['request']['scope_ref'] == origin['scope_ref']
                 and prior['capture']['result']['status'] == 'OK'
                 and prior['capture']['capture_origin'] == state['config']['capture_origin']
                 and prior['capture']['result']['payload']['content_sha256'] == latest['content_sha256']
                 and type(latest['last_seq']) is int and latest['last_seq'] <= d['source']['global_seq'], 'LATEST_READ_WITNESS')
            # This is an observed ref relation, never a prediction of availability.
            result['features'][role] = dict(value='MATCH' if prior['request']['entry_ref'] == entries[0]['entry_ref'] else 'DIFFERENT',
                capture_ref=copy.deepcopy(latest['last_receipt_ref']))
            feature_witnesses[role] = (path, latest['last_seq'], latest['last_receipt_ref'])
    if journal_proof is None:
        result['reasons'].append('MISSING_JOURNAL_PROOF')
        for role in feature_witnesses:
            result['features'][role] = dict(value=_UNKNOWN, reason='MISSING_PRIOR_READ_ADMISSION')
    else:
        events, endpoint, current_ref, known_predecessor = _journal(journal_proof, origin)
        cut = endpoint if end_seq is None else end_seq
        need(type(cut) is int and 0 <= cut <= endpoint, 'END_SEQ')
        selected = [e for e in events if e['seq'] <= cut]
        by_seq = {e['seq']: e for e in selected}
        first_seq = d['source']['global_seq'] + 1
        first = by_seq.get(first_seq)
        need(first is None or (first['kind'] == 'REQUEST' and first['payload']['decision_ref'] == first_decision_ref
             and first['previous'] == d['source']['journal_head']), 'FIRST_REQUEST_BINDING')
        pending, observed, workflow_at_first, admitted_captures = {}, [], None, {}
        started = set()
        admission, terminal, intervened, uncertain, unsupported = None, None, False, False, False
        admitted_goal = None
        boundary = dict(value=_UNKNOWN, seq=None)
        prior_goal = None
        if current_ref is not None:
            prior_goal = procedures.validate_workflow(objects.read(current_ref, 'compact_workflow'))['goal']
        for e in selected:
            seq, kind, payload = e['seq'], e['kind'], e['payload']
            previous_goal = prior_goal
            if seq == first_seq:
                workflow_at_first = current_ref
            if kind == 'REQUEST':
                ref = payload['decision_ref']
                request_d, is_local = _decision(objects, ref, origin)
                need(request_d['source']['global_seq'] == seq - 1 and request_d['source']['journal_head'] == e['previous'], 'REQUEST_SEQUENCE')
                effect = dict(phase='REQUEST', id=origin['identity'] + ':' + str(seq))
                need(e['effect'] == effect and not pending, 'REQUEST_EFFECT_ORDER')
                pending[effect['id']] = (seq, ref, request_d, is_local)
            elif kind == 'START':
                effect = e['effect']
                need(effect is not None and effect['id'] in pending and effect['phase'] == 'START'
                     and seq == pending[effect['id']][0] + 1, 'START_EFFECT_ORDER')
                started.add(effect['id'])
            elif kind in ('RECEIPT', 'LOCAL_RECEIPT'):
                effect = e['effect']
                if effect is not None and effect['id'] in pending:
                    req_seq, ref, request_d, is_local = pending.pop(effect['id'])
                    need(effect['phase'] == 'RECEIPT' and seq == req_seq + 2
                         and by_seq[seq - 1]['kind'] == 'START'
                         and (kind == 'LOCAL_RECEIPT') == is_local, 'RECEIPT_EFFECT_ORDER')
                    if is_local:
                        outcome = objects.read(payload['outcome_ref'], 'compact_local_outcome')
                        interaction.validate_outcome(outcome, request_d['request'])
                        if admission is not None:
                            intervened = True
                            uncertain |= outcome['status'] in ('UNKNOWN', 'TIMED_OUT')
                    else:
                        item = objects.read(payload['capture_ref'], 'compact_capture')
                        validate_capture(item)
                        need(item['request'] == request_d['request'] and item['capture']['capture_origin'] ==
                             (state['config']['capture_origin'] if state else item['capture']['capture_origin']), 'CAPTURE_REQUEST_ORIGIN')
                        admitted_captures[payload['capture_ref']['sha256']] = (seq, request_d, item)
                        ri = request_d.get('investigation')
                        if ri is not None and ri['goal_id'] == info['goal_id']:
                            need(ri['episode_key'] == info['episode_key'] and ri['role'] in procedures.ROLES
                                 and type(ri['cursor']) is int, 'INVESTIGATION_EPISODE_CURSOR')
                            path = _path(objects, request_d, origin, item['capture']['capture_origin'])
                            if goal is not None:
                                need(ri['episode_key'] == goal['episode_key'] and ri['role'] in procedures.ROLES
                                     and path == state['goals'][goal['bindings'][ri['role']]]['path'], 'INVESTIGATION_ROLE_PATH')
                            observed.append(dict(decision_ref=ref, capture_ref=payload['capture_ref'], receipt_seq=seq,
                                role=ri['role'], cursor=ri['cursor'], status=item['capture']['result']['status'],
                                help_check=request_d.get('help_check')))
                        if admission is not None and request_d.get('help_check') is not None:
                            intervened = True
                elif admission is not None:
                    unsupported = True  # tail started after this request; no complete effect proof
            wf_ref = payload.get('workflow_ref') if kind in ('INITIALIZE', 'RECEIPT', 'LOCAL_RECEIPT') else None
            if 'record_ref' in payload:
                ref = payload['record_ref']
                if ref['kind'] == 'compact_workflow_control':
                    control = objects.read(ref, 'compact_workflow_control')
                    need(control['kind'] == kind and control['source']['global_seq'] == seq - 1
                         and control['source']['journal_head'] == e['previous']
                         and all(control['source'][k] == v for k, v in origin.items()), 'CONTROL_SOURCE_BINDING')
                    wf_ref = control['workflow_ref']
                elif admission is not None:
                    unsupported = True
            if wf_ref is not None:
                w = procedures.validate_workflow(objects.read(wf_ref, 'compact_workflow'))
                g = w['goal']
                if g is not None and g['id'] == info['goal_id']:
                    if admission is None and known_predecessor and (prior_goal is None or prior_goal['id'] != g['id']):
                        need(g['cursor'] == 0 and g['receipts'] == [], 'ADMISSION_EMPTY_PREFIX')
                        admission = seq
                        admitted_goal = copy.deepcopy(g)
                    terminal = g
                    h = None if w['help'] is None else w['help']['episode']
                    if h is not None and h['trigger']['goal']['id'] == g['id']:
                        intervened = True
                elif w['last_goal'] is not None and w['last_goal']['id'] == info['goal_id']:
                    terminal = w['last_goal']
                if terminal is not None and (goal is not None or admitted_goal is not None):
                    need(_same_goal(terminal, goal or admitted_goal), 'TERMINAL_GOAL_IDENTITY')
                current_ref, prior_goal, known_predecessor = wf_ref, g, True
            if admission is not None:
                if kind == 'REFRESH' or kind in ('QUEUE_HELP', 'QUEUE_LOCAL', 'LOCAL_RESPONSE'):
                    intervened = True
                if kind not in ('CLOCK', 'REQUEST', 'START', 'RECEIPT', 'LOCAL_RECEIPT', 'INITIALIZE',
                    'GOAL_BEGIN', 'GOAL_BLOCK', 'GOAL_CANCEL', 'REGULATOR_WAIT', 'REGULATOR_RESUME', 'LOCAL_RESOURCE_WAIT',
                    'REFRESH', 'QUEUE_HELP', 'QUEUE_LOCAL', 'LOCAL_RESPONSE', 'EXPIRE_HELP', 'EXPIRE_LOCAL'):
                    unsupported = True
            if wf_ref is not None and terminal is not None and first is not None:
                if _completed_goal(terminal, observed) and not pending:
                    boundary = dict(value='VERIFIED_COMPLETED', seq=seq)
                elif (kind == 'GOAL_CANCEL' and terminal['status'] == 'CANCELLED'
                      and terminal['reason'] == 'CANCEL' and _same_goal(terminal, previous_goal)
                      and terminal['cursor'] == previous_goal['cursor']
                      and terminal['receipts'] == previous_goal['receipts']
                      and terminal.get('receipt_provenance') == previous_goal.get('receipt_provenance')
                      and not pending):
                    boundary = dict(value='VERIFIED_CANCELLED', seq=seq)
                if boundary['value'] != _UNKNOWN:
                    # _journal already checked the complete supplied checkpoint
                    # and every event hash/link. Later goals need no semantic
                    # object reads and cannot affect this goal's evidence.
                    break
                if admission is not None and prior_goal is not None and prior_goal['id'] != info['goal_id']:
                    unsupported = True
                    boundary = dict(value=_UNKNOWN, seq=None, reason='TARGET_REPLACED_WITHOUT_VERIFIED_CLOSURE')
                    break
        complete_span = (first is not None and admission is not None and admission < first_seq
                         and workflow_at_first is not None and not unsupported)
        if state is not None and workflow_at_first is not None:
            need(workflow_at_first == state['workflow_ref'], 'COMMITTED_BEFORE_WORKFLOW')
        for role, (path, seq, ref) in feature_witnesses.items():
            witness = admitted_captures.get(ref['sha256'])
            if first is None:
                result['features'][role] = dict(value=_UNKNOWN, reason='MISSING_FIRST_REQUEST_ADMISSION')
            elif witness is None:
                result['features'][role] = dict(value=_UNKNOWN, reason='MISSING_PRIOR_READ_ADMISSION')
            else:
                need(witness[0] == seq and witness[0] <= d['source']['global_seq']
                     and _path(objects, witness[1], origin, state['config']['capture_origin']) == path,
                     'PRIOR_READ_ROLE_CHRONOLOGY')
        result['actual_receipts'] = observed
        result['boundary'] = boundary
        result['span'] = dict(value='VERIFIED' if complete_span else _UNKNOWN, admission_seq=admission,
            through_seq=boundary['seq'] if boundary['seq'] is not None else cut,
            event_count=len(events), reason=None if complete_span else 'MISSING_ADMISSION_OR_UNSUPPORTED_SPAN')
        result['assistance'] = 'PRESENT' if intervened else ('NONE_IN_GOAL_SPAN' if complete_span else _UNKNOWN)
        if terminal is not None and goal is not None:
            need(all(_encode(terminal[k]) == _encode(goal[k]) for k in
                ('id', 'generation', 'topic', 'bindings', 'episode_key')), 'TERMINAL_GOAL_IDENTITY')
        if not complete_span or state is None:
            result['reasons'].append('INCOMPLETE_SPAN_OR_BEFORE_STATE')
        elif any(eid in started for eid in pending) or uncertain:
            result['outcome'] = 'EXECUTION_UNKNOWN'
        elif pending:
            result['reasons'].append('REQUEST_NOT_STARTED')
        elif intervened:
            result['outcome'] = 'INTERVENED'
        elif boundary['value'] == 'VERIFIED_CANCELLED':
            result['outcome'] = 'CANCELLED'
        elif any(r['status'] == 'DENIED' for r in observed):
            result['outcome'] = 'AUTHORITY_REFUSAL'
        elif any(r['status'] == 'UNAVAILABLE' for r in observed):
            result['outcome'] = 'ADAPTER_UNAVAILABLE'
        else:
            successes = [r for r in observed if r['status'] == 'OK' and r['help_check'] is None]
            distinct = len(successes) == 2 and [r['cursor'] for r in successes] == [0, 1]
            distinct &= {r['role'] for r in successes} == set(procedures.ROLES)
            completed = _completed_goal(terminal, observed)
            if distinct and completed and len(observed) == 2:
                result['outcome'] = 'COMPLETE_UNASSISTED_IN_GOAL_SPAN'
            elif any(r['status'] in ('STALE_REF', 'NOT_FOUND') for r in observed):
                result['outcome'] = 'KNOWN_SOURCE_FAILURE_IN_GOAL_SPAN'
            else:
                result['reasons'].append('NO_PROVEN_DISTINCT_ROLE_COMPLETION')
    result['verification'] = dict(objects=len(objects.cache), object_bytes=objects.bytes,
        reducer_replayed=False, checkpoint_authentication='TRUSTED_PIN_NOT_HOST_AUTHENTICATION')
    return result


def verify_experience(first_decision_ref, *, expected_origin, read_frame, before_state=None,
                      journal_proof=None, end_seq=None):
    """Public API; see _verify_experience for witness contracts.

    Missing objects/resource bounds yield UNKNOWN, malformed or tampered
    evidence fails closed. No arbitrary complete/success/independent flags.
    """
    try:
        return _verify_experience(first_decision_ref, expected_origin=expected_origin, read_frame=read_frame,
            before_state=before_state, journal_proof=journal_proof, end_seq=end_seq)
    except _Unavailable as error:
        return dict(schema=SCHEMA, origin=copy.deepcopy(expected_origin), features={r: dict(value=_UNKNOWN,
            reason=str(error)) for r in procedures.ROLES}, outcome=_UNKNOWN, assistance=_UNKNOWN,
            independence=_UNKNOWN, span=dict(value=_UNKNOWN, reason=str(error)), reasons=[str(error)], authority='NONE')
