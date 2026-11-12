"""Bounded U0 rule-controller embryos, not a trained attention/SSM/GRU hybrid.

All operations are pure snapshots. Governance is an advisory scheduler, not an
authority source. Teacher access deliberately has no enabling path in this build.
"""
import copy
import hashlib
import math

from .contracts import NEURONS
from .core import fail

LABEL = 'U0_RULE_CONTROLLER'
MAX_TOKENS = 64


def _keys(value, required, optional=()):
    if type(value) is not dict or not set(required) <= value.keys() or set(value) - set(required) - set(optional):
        fail('TYPE_ERROR', 'control embryo object fields')


def _number(value, low=0, high=1):
    if type(value) not in (int, float) or not low <= value <= high or not math.isfinite(value):
        fail('TYPE_ERROR', 'finite bounded numeric value required')
    return float(value)


def _tokens(value, limit=MAX_TOKENS):
    if type(value) is not list or len(value) > limit:
        fail('TYPE_ERROR', 'bounded token list required')
    for token in value:
        if type(token) is not str or not 1 <= len(token) <= 128:
            fail('TYPE_ERROR', 'token must contain 1..128 characters')
        try:
            token.encode('utf8')
        except UnicodeError:
            fail('TYPE_ERROR', 'invalid UTF-8 token')
    return list(value)


def _features(tokens):
    vector = [0.0] * 8
    for token in tokens:
        raw = hashlib.sha256(token.encode('utf8')).digest()
        for i in range(8):
            vector[i] += raw[i] / 127.5 - 1
    return [x / max(1, len(tokens)) for x in vector]


def initial_control():
    return {'schema': 'control-embryos-0.1', 'controller': LABEL,
            'trained': False, 'steps': 0, 'focus': [], 'continuous': [0.0] * 8,
            'gate': 0.0, 'attention': []}


def _validate_control(state):
    _keys(state, initial_control())
    if state['schema'] != 'control-embryos-0.1' or state['controller'] != LABEL or state['trained'] is not False:
        fail('TYPE_ERROR', 'unsupported controller state')
    if type(state['steps']) is not int or not 0 <= state['steps'] <= 2**53 - 1:
        fail('TYPE_ERROR', 'bounded control step counter')
    focus = _tokens(state['focus'], 8)
    if len(set(focus)) != len(focus):
        fail('TYPE_ERROR', 'duplicate focus tokens')
    if type(state['continuous']) is not list or len(state['continuous']) != 8:
        fail('TYPE_ERROR', 'eight continuous dimensions required')
    for number in state['continuous']:
        _number(number, -1, 1)
    _number(state['gate'])
    if type(state['attention']) is not list or len(state['attention']) != len(focus):
        fail('TYPE_ERROR', 'focus/attention alignment')
    for number in state['attention']:
        _number(number)
    if focus and abs(sum(state['attention']) - 1) > 1e-9:
        fail('TYPE_ERROR', 'normalized attention required')


def update_control(state, observation_summary, memory_tokens):
    """Update heuristic focus and a deterministic leaky eight-dimensional state.

    Observation accepts only tokens, salience and novelty; all are optional.
    Token attention uses frequency/recency rules, not learned weights.
    """
    _validate_control(state)
    _keys(observation_summary, (), ('tokens', 'salience', 'novelty'))
    observed = _tokens(observation_summary.get('tokens', []))
    memory = _tokens(memory_tokens)
    salience = _number(observation_summary.get('salience', 0.5))
    novelty = _number(observation_summary.get('novelty', 0.0))
    if state['steps'] == 2**53 - 1:
        fail('BUDGET_EXCEEDED', 'control step counter exhausted')
    scores = {}
    for weight, tokens in ((1, state['focus']), (2, memory), (3 + salience, observed)):
        for token in tokens:
            scores[token] = scores.get(token, 0) + weight
    focus = sorted(scores, key=lambda token: (-scores[token], token))[:8]
    total = sum(scores[token] for token in focus)
    gate = 0.1 + 0.4 * salience + 0.3 * novelty
    features = _features(focus)
    result = initial_control()
    result.update(steps=state['steps'] + 1, focus=focus, gate=gate,
                  attention=[scores[token] / total for token in focus],
                  continuous=[(1 - gate) * old + gate * new
                              for old, new in zip(state['continuous'], features)])
    return result


def candidate_offset(control, candidate):
    """Small rule-based persistent-state contribution; never grants capability."""
    _validate_control(control)
    _keys(candidate, (), ('tokens', 'novelty'))
    tokens = _tokens(candidate.get('tokens', []))
    novelty = _number(candidate.get('novelty', 0))
    features = _features(tokens)
    alignment = sum(a * b for a, b in zip(control['continuous'], features)) / 8
    overlap = sum(weight for token, weight in zip(control['focus'], control['attention']) if token in tokens)
    return max(-0.03, min(0.03, 0.015 * alignment + 0.01 * overlap + 0.005 * control['gate'] * novelty))


def governance_allocate(jobs, budget=4):
    """Advisory gain/cost allocation. Does not run jobs or create permissions.

    Cost must match the registered module's cost. Gains are caller estimates,
    not verified utility. Conflicting claims are explicitly left unresolved.
    """
    if type(budget) is not int or not 0 <= budget <= 64:
        fail('TYPE_ERROR', 'governance budget must be 0..64')
    if type(jobs) is not list or len(jobs) > 64:
        fail('TYPE_ERROR', 'at most 64 governance jobs')
    valid, identifiers = [], set()
    for job in jobs:
        _keys(job, ('id', 'module', 'cost', 'gain'))
        _tokens([job['id']])
        if job['id'] in identifiers:
            fail('TYPE_ERROR', 'duplicate governance job ID')
        identifiers.add(job['id'])
        module = job['module']
        if type(module) is not str or module not in NEURONS:
            fail('UNKNOWN_OPCODE', 'unregistered governance module')
        if type(job['cost']) is not int or job['cost'] != NEURONS[module].compute_cost or job['cost'] <= 0:
            fail('TYPE_ERROR', 'cost must match registered compute cost')
        gain = _number(job['gain'])
        valid.append(dict(job, gain=gain))
    ordered = sorted(valid, key=lambda job: (-job['gain'] / job['cost'], job['id']))
    allocated, deferred, spent = [], [], 0
    for job in ordered:
        if job['gain'] > 0 and spent + job['cost'] <= budget:
            allocated.append(job)
            spent += job['cost']
        else:
            deferred.append(job)
    return {'allocations': allocated, 'deferred': deferred, 'spent': spent,
            'budget': budget, 'authority': 'ADVISORY_ONLY',
            'conflict_resolution': 'UNRESOLVED'}


TEACHER_PAYLOAD_FIELDS = {
    'TEACH_REQUEST': 'question',
    'LESSON_PROPOSAL': 'lesson',
    'STUDENT_TEST': 'prompt',
    'LEARNING_RESULT': 'result',
}


def validate_teacher_envelope(envelope):
    """Validate a bounded, inert four-kind proposal, never a lesson admission.

    Exact envelope fields: schema, kind, id, request_id, source, context_refs,
    payload. Payload requires the kind-specific text field and permits notes.
    Source and context references are unverified strings, not resolved evidence
    or permissions. Returned status is local metadata, not an envelope field.
    """
    _keys(envelope, ('schema', 'kind', 'id', 'request_id', 'source', 'context_refs', 'payload'))
    if envelope['schema'] != 'teacher-envelope-0.1':
        fail('TYPE_ERROR', 'unsupported teacher envelope schema')
    kind = envelope['kind']
    if type(kind) is not str or kind not in TEACHER_PAYLOAD_FIELDS:
        fail('TYPE_ERROR', 'unknown teacher envelope kind')
    _tokens([envelope['id'], envelope['request_id'], envelope['source']])
    refs = _tokens(envelope['context_refs'], 16)
    if len(set(refs)) != len(refs):
        fail('TYPE_ERROR', 'duplicate teacher context reference')
    payload = envelope['payload']
    _keys(payload, (TEACHER_PAYLOAD_FIELDS[kind],), ('notes',))
    for text in payload.values():
        if type(text) is not str or not 1 <= len(text) <= 4096:
            fail('TYPE_ERROR', 'teacher payload must be 1..4096 character text')
        try:
            text.encode('utf8')
        except UnicodeError:
            fail('TYPE_ERROR', 'teacher payload UTF-8')
    return {'envelope': copy.deepcopy(envelope), 'status': 'INERT_UNVERIFIED',
            'adopted': False, 'source_verified': False,
            'context_refs_resolved': False, 'network_used': False}


def teacher_status(core_state, development):
    """Caller assertions cannot unlock this gate, including fabricated evidence."""
    if type(core_state) is not dict or type(development) is not dict:
        fail('TYPE_ERROR', 'teacher gate requires state snapshots')
    # Missing or malformed counters are unknown, never truthy gate evidence.
    def counter(name):
        value = core_state.get(name)
        return value if type(value) is int and 0 <= value <= 2**53 - 1 else None
    tick, loops = counter('tick'), counter('closed_loops')
    return {'enabled': False, 'status': 'TEACHER_LOCKED',
            'tick': tick, 'closed_loops': loops,
            'offline_1000_ticks_observed': tick is not None and tick >= 1000,
            'closed_loop_observed': loops is not None and loops > 0,
            'counter_evidence': 'CALLER_SNAPSHOT_NOT_INDEPENDENTLY_VERIFIED',
            'restored_goal_evidence_verified': False,
            'held_out_learning_verified': False,
            'reason': 'Externally verified held-out learning evidence is absent; this build has no teacher enable path.',
            'network_available': False, 'lesson_acceptance': False}


def teacher_request(core_state, development, proposal=None):
    """No network, storage, or lesson ingestion, regardless of proposal content."""
    teacher_status(core_state, development)
    fail('TEACHER_LOCKED', 'external teacher access and lesson ingestion disabled')
