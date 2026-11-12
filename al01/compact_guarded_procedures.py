"""Opt-in finite admission-conditioned hypothesis. No truth or authority grant.

One empirical chain per branch is explicitly insufficient for a reliability
claim. This new candidate must still face the unchanged closed tests and
acceptance gate. No default order is manufactured for an unknown context.
"""
import copy

from . import compact_procedures as p
from . import compact_semantics as semantics
from .compact_observer import validate_capture
from .folder_session import _encode, _hash
from .core import Compiler

PROFILE = 'admission-guarded-pair-1'
SCHEMA = 'compact-guarded-procedure-candidate-1'
KINDS = ('DIRECT', 'DEFERRED')


def validate_profile(profile):
    p.need(profile is None or profile == PROFILE, 'supported guarded extraction profile')
    return profile


def enabled(state):
    probe = state['config'].get('procedure_probe')
    return (state['config'].get('procedure_extraction_profile') == PROFILE
            or (probe is not None and probe.get('schema') == SCHEMA))


def validate_context(c):
    p.need(type(c) is dict and set(c) == {'kind', 'topic', 'episode_key', 'goal_id',
        'trigger_decision_ref', 'trigger_capture_ref', 'prior_semantics_ref',
        'prior_workflow_ref', 'receipt_seq'}, 'bounded admission context fields')
    p.need(c['kind'] in KINDS and type(c['receipt_seq']) is int
           and 0 < c['receipt_seq'] < 2**53, 'known admission context')
    for key in ('topic', 'episode_key', 'goal_id'):
        p.need(type(c[key]) is str and 1 <= len(c[key]) <= 256, 'admission identifier')
    p.need(len(_encode(c)) <= 4096, 'admission context byte bound')
    return copy.deepcopy(c)


def attach(w, state, decision, item, kind):
    if not enabled(state):
        return w
    g = w['goal']
    capture_ref = semantics.capture_reference(item, state['identity'], state['instance_id'])
    g['admission_context'] = dict(kind=kind, topic=g['topic'], episode_key=g['episode_key'],
        goal_id=g['id'], trigger_decision_ref=state['pending']['decision_ref'],
        trigger_capture_ref=capture_ref,
        prior_semantics_ref=state['semantics_ref'], prior_workflow_ref=state['workflow_ref'],
        receipt_seq=decision['source']['global_seq'] + 3)
    return p.validate_workflow(w)


def _path(d):
    paths = [m['path'] for m in d['candidate_metadata'] if m['entry_ref'] == d['request']['entry_ref']]
    p.need(len(paths) == 1, 'one addressed monitored source')
    return paths[0]


def verify_context(c, chain, origin, resolve):
    validate_context(c)
    p.need(c['episode_key'] == chain['episode_key'], 'admission episode binding')
    d = resolve(c['trigger_decision_ref'], 'compact_decision')
    item = resolve(c['trigger_capture_ref'], 'compact_capture')
    before = semantics.validate_semantics(resolve(c['prior_semantics_ref'], 'compact_semantics'))
    prior = p.validate_workflow(resolve(c['prior_workflow_ref'], 'compact_workflow'))
    validate_capture(item)
    p.need(all(d['source'][k] == origin[k] == before[k] for k in origin), 'admission origin')
    for key in ('trigger_decision_ref', 'trigger_capture_ref', 'prior_semantics_ref', 'prior_workflow_ref'):
        p.need(all(c[key][k] == origin[k] for k in ('identity', 'instance_id')), 'admission reference namespace')
    p.need(item['request'] == d['request'] and item['request']['operation'] == 'READ_TEXT'
           and item['capture']['result']['status'] == 'OK', 'successful admission trigger')
    p.need(c['receipt_seq'] == d['source']['global_seq'] + 3
           and c['receipt_seq'] < chain['steps'][0]['receipt_seq'], 'admission chronology')
    first = resolve(chain['steps'][0]['decision_ref'], 'compact_decision')
    first_capture = resolve(chain['steps'][0]['capture_ref'], 'compact_capture')
    p.need(first['investigation']['goal_id'] == c['goal_id'], 'admission goal binding')
    p.need(first_capture['capture']['capture_origin'] == item['capture']['capture_origin'], 'admission capture provenance')
    p.need(before['overflow'] is None and c['topic'] not in before['investigations'], 'first admission only; no repeated rounds')
    index, report = semantics.apply_capture(before, item, c['trigger_capture_ref'], _path(d), c['receipt_seq'])
    current_topic = None if report.get('record') is None else report['record']['topic']
    kind = 'DIRECT' if current_topic == c['topic'] else 'DEFERRED'
    p.need(kind == c['kind'], 'receipt-derived admission kind')
    rows = [r for r in index['records'] if r['topic'] == c['topic']]
    p.need(len({r['path'] for r in rows}) == 2, 'two admitted source paths')
    claim = rows[0]['claim']
    p.need(semantics.query(index, claim['subject'], claim['predicate'])['status'] == 'UNRESOLVED_CONFLICT', 'actual same-context conflict')
    if kind == 'DEFERRED':
        oldrows = [r for r in before['records'] if r['topic'] == c['topic']]
        p.need(len({r['path'] for r in oldrows}) == 2, 'deferred conflict already observed')
        goal = prior['goal']
        p.need(goal is None or goal['status'] == 'SATISFIED' or (
            goal['status'] == 'ACTIVE' and goal['cursor'] == 1 and d['investigation'] is not None
            and d['investigation']['goal_id'] == goal['id'] and d['investigation']['cursor'] == 1
            and d['investigation']['episode_key'] == goal['episode_key']
            and len(prior['trace']) == 1
            and prior['trace'][0]['step']['capture_ref'] == goal['receipts'][0]
            and d['investigation']['role'] != prior['trace'][0]['step']['role']),
            'deferred trigger follows completion; no paused/cancelled/blocked revival')
    else:
        p.need(prior['goal'] is None or prior['goal']['status'] in ('SATISFIED', 'BLOCKED', 'CANCELLED'), 'direct admission after terminal goal')
    first_seen = {}
    for row in rows:
        first_seen.setdefault(row['path'], row['receipt_seq'])
    paths = sorted(first_seen, key=lambda path: (first_seen[path], path))
    inv_epoch = _hash(sorted({_hash(r['claim']) for r in rows}))
    p.need(c['episode_key'] == _hash([c['topic'], inv_epoch, 1]), 'first conflict episode identity')
    for step in chain['steps']:
        source = resolve(step['decision_ref'], 'compact_decision')
        p.need(_path(source) == paths[p.ROLES.index(step['role'])], 'role follows real first observation order')
    return kind


def validate_candidate(c):
    p.need(type(c) is dict and set(c) == {'schema', 'family', 'profile', 'origin',
        'source_workflow_ref', 'chains', 'contexts', 'branches', 'cost', 'termination',
        'failure', 'authority', 'support'}, 'guarded candidate fields')
    p.need(c['schema'] == SCHEMA and c['profile'] == PROFILE
           and c['family'] == p.FAMILY and c['authority'] == 'NONE', 'guarded finite family')
    p.need(type(c['chains']) is list and len(c['chains']) == 2
           and type(c['contexts']) is list and len(c['contexts']) == 2, 'two bounded contextual chains')
    for context in c['contexts']:
        validate_context(context)
    p.need({x['kind'] for x in c['contexts']} == set(KINDS), 'both admission contexts observed')
    p.need(type(c['branches']) is dict and set(c['branches']) == set(KINDS), 'no unobserved default branch')
    for branch in c['branches'].values():
        p.need(type(branch) is dict and set(branch) == {'order', 'templates', 'ir'}, 'finite branch fields')
        order = branch['order']
        p.need(type(order) is list and len(order) == 2 and set(order) == set(p.ROLES), 'branch permutation')
        templates = [f'PROPOSE(OBSERVE(@{p.ROLE_TOKENS[role]}));' for role in order]
        p.need(branch['templates'] == templates and branch['ir'] == [Compiler(p.ROLE_TOKENS.values(), ('read', 'observe')).compile(t) for t in templates], 'typed contextual CL')
    for chain, context in zip(c['chains'], c['contexts']):
        p.need(chain['episode_key'] == context['episode_key']
               and [s['role'] for s in chain['steps']] == c['branches'][context['kind']]['order'],
               'branch preserves its original chronological chain')
    p.need(c['support'] == dict(DIRECT=1, DEFERRED=1, reliability='UNKNOWN'), 'honest empirical support')
    p.need(c['cost'] == dict(actions=2, logical_units=10)
           and c['termination'] == 'TWO_MATCHING_RECEIPTS_INSPECTED_UNRESOLVED'
           and c['failure'] == 'BLOCK_AND_RETAIN_EVIDENCE', 'unchanged effect and stop bounds')
    p.need(len(_encode(c)) <= p.MAX_CANDIDATE_BYTES, 'candidate byte bound')
    return copy.deepcopy(c)


def extract(w, workflow_ref, origin, resolve, *, contexts=None):
    orders = p.admitted_orders(w, workflow_ref, origin, resolve)
    if contexts is None:
        contexts = [chain.get('admission_context') for chain in w['chains']]
    p.need(type(contexts) is list and len(contexts) == 2, 'two source admission contexts')
    branches = {}
    for chain, order, context in zip(w['chains'], orders, contexts):
        kind = verify_context(context, chain, origin, resolve)
        p.need(kind not in branches, 'distinct DIRECT and DEFERRED evidence required')
        templates = [f'PROPOSE(OBSERVE(@{p.ROLE_TOKENS[role]}));' for role in order]
        branches[kind] = dict(order=order, templates=templates,
            ir=[Compiler(p.ROLE_TOKENS.values(), ('read', 'observe')).compile(t) for t in templates])
    c = dict(schema=SCHEMA, family=p.FAMILY, profile=PROFILE, origin=copy.deepcopy(origin),
        source_workflow_ref=copy.deepcopy(workflow_ref), chains=copy.deepcopy(w['chains']),
        contexts=copy.deepcopy(contexts), branches=branches,
        cost=dict(actions=2, logical_units=10), termination='TWO_MATCHING_RECEIPTS_INSPECTED_UNRESOLVED',
        failure='BLOCK_AND_RETAIN_EVIDENCE', authority='NONE',
        support=dict(DIRECT=1, DEFERRED=1, reliability='UNKNOWN'))
    return validate_candidate(c)


def branch(spec, goal):
    if spec is None or spec.get('profile') != PROFILE:
        return spec
    context = None if goal is None else goal.get('admission_context')
    return None if context is None else spec['branches'].get(context['kind'])


def execution_problem(spec, goal, resolve):
    """Preserve recorded admissions; refuse unsupported execution prospectively.

    In particular a counterevidence receipt may start a second round, whose
    factual admission marker must remain in replay even though this candidate
    has never learned that context. Checking before the next effect avoids
    changing any receipt reducer or converting the unknown into a retry.
    """
    if spec is None or spec.get('profile') != PROFILE:
        return None
    if branch(spec, goal) is None:
        return 'UNOBSERVED_ADMISSION_CONTEXT'
    c = goal['admission_context']
    before = semantics.validate_semantics(resolve(c['prior_semantics_ref'], 'compact_semantics'))
    if before['overflow'] is not None or c['topic'] in before['investigations']:
        return 'UNOBSERVED_ADMISSION_CONTEXT'
    d = resolve(c['trigger_decision_ref'], 'compact_decision')
    item = resolve(c['trigger_capture_ref'], 'compact_capture')
    after, report = semantics.apply_capture(before, item, c['trigger_capture_ref'], _path(d), c['receipt_seq'])
    topic = None if report.get('record') is None else report['record']['topic']
    if c['kind'] != ('DIRECT' if topic == c['topic'] else 'DEFERRED'):
        return 'UNOBSERVED_ADMISSION_CONTEXT'
    rows = [r for r in after['records'] if r['topic'] == c['topic']]
    if len({r['path'] for r in rows}) != 2:
        return 'UNOBSERVED_ADMISSION_CONTEXT'
    claim = rows[0]['claim']
    if semantics.query(after, claim['subject'], claim['predicate'])['status'] != 'UNRESOLVED_CONFLICT':
        return 'UNOBSERVED_ADMISSION_CONTEXT'
    return None
