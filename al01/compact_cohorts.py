"""Pure finite prospective cohort/exposure values, not runtime authentication.

All functions copy their inputs and perform no I/O. Runtime admission must bind
source snapshots, requests, actual receipts, model/report contents and immutable
archives, reserve publication/journal space BEFORE effects, and publish roots
atomically. Hash inequality screens exact reuse only, not fixture independence.
The fixed path window never evicts obligations. This is at most three queued
lifetime cohorts and 256 exposed contents, not unbounded lifelong learning.
"""
import copy
from .folder_session import _encode, _hash
from .folder_contracts import validate_relative_path, validate_ref, STATUSES
from .compact_observer import validate_capture
from .compact_learning import ROLES, _reference

SCHEMA = 'compact-prospective-cohort-registry-2'
MAX_COHORTS = 3
MAX_QUEUE = 2
MAX_EXPOSURES = 256
MAX_BYTES = 65536
MAX_REGISTRY_BYTES = MAX_BYTES
MAX_COUNTER = 2**53-1
EXPOSURE_CAPACITY_REASON = 'EXPOSURE_CAPACITY_EXHAUSTED'
STATUSES_CURRENT = ('TRAINING', 'FROZEN', 'ACCEPTED', 'REJECTED', 'INVALID', 'ACTIVATED')
TERMINAL = ('REJECTED', 'INVALID', 'ACTIVATED')
RETENTION_CASES = ('success', 'reversed', 'denied', 'stale', 'counterevidence', 'retention', 'unknown')
REASONS = ('HELDOUT_BEFORE_FREEZE', 'PREVIOUSLY_EXPOSED_CONTENT',
           'FIRST_READ_NOT_SUCCESSFUL', 'INSUFFICIENT_TRANSITIONS',
           'EVALUATION_BANK_CONTENT_OVERLAP', 'EVALUATION_BANK_TRAINING_COLLISION')
SOURCE_FIELDS = {'seq', 'head', 'cognition_revision'}
QUEUED_FIELDS = {'generation', 'cohort_id', 'plan', 'queued_source'}
CURRENT_FIELDS = QUEUED_FIELDS | {'opened_source', 'incumbent_ref', 'protected_skills',
    'parent_archive_ref', 'status', 'reason', 'evidence', 'candidate_ref', 'report_ref',
    'retention_plan_ref', 'retention_results', 'retention_anchor_ref'}
REGISTRY_FIELDS = {'schema', 'identity', 'instance_id', 'scope_ref', 'paths', 'path_roles',
    'queued_count', 'queue', 'current', 'archives', 'exposures', 'eval_exposures', 'last_read_seq'}


def need(ok, why):
    if not ok:
        raise ValueError('COMPACT_COHORTS_INVALID: ' + why)


def _counter(value):
    need(type(value) is int and 0 <= value <= MAX_COUNTER, 'bounded counter')


def _digest(value, size=64):
    need(type(value) is str and len(value) == size and
         all(c in '0123456789abcdef' for c in value), 'digest')


def _source(value):
    need(type(value) is dict and set(value) == SOURCE_FIELDS, 'source fields')
    _counter(value['seq']); _counter(value['cognition_revision']); _digest(value['head'])


def _ref(value, state, kind):
    _reference(value, kind)
    need(all(value[k] == state[k] for k in ('identity', 'instance_id')), 'reference namespace')


def _slots(state):
    return {str(i) for i in range(len(state['paths']))}


def _plan(plan, state, *, paths=False):
    need(type(plan) is dict and set(plan) == set(ROLES), 'four explicit role lists')
    assigned = set(); result = {}
    for role in ROLES:
        values = plan[role]
        need(type(values) is list and 1 <= len(values) <= 5, 'bounded nonempty role')
        result[role] = []
        for value in values:
            need(type(value) is str, 'plan path/slot string')
            if paths:
                validate_relative_path(value)
                need(value in state['paths'], 'exact fixed monitored path')
                value = str(state['paths'].index(value))
            need(value in _slots(state) and value not in assigned, 'distinct fixed path slot')
            need(state['path_roles'].get(value, role) == role, 'permanent path role')
            assigned.add(value); result[role].append(value)
    need(len(assigned) <= 8, 'eight total plan targets')
    return result


def _cohort_id(state, generation, plan, source):
    return _hash(dict(identity=state['identity'], instance_id=state['instance_id'],
        scope_ref=state['scope_ref'], generation=generation, plan=plan, queued_source=source))


def _queued(record, state):
    need(type(record) is dict and QUEUED_FIELDS <= set(record), 'queued record fields')
    need(type(record['generation']) is int and 1 <= record['generation'] <= MAX_COHORTS,
         'cohort generation')
    _source(record['queued_source']); _plan(record['plan'], state)
    need(record['cohort_id'] == _cohort_id(state, record['generation'], record['plan'],
         record['queued_source']), 'immutable cohort identity/plan/source')
    for role, slots in record['plan'].items():
        need(all(state['path_roles'].get(slot) == role for slot in slots), 'registered permanent roles')


def validate_registry(state):
    need(type(state) is dict and set(state) == REGISTRY_FIELDS, 'registry fields')
    need(state['schema'] == SCHEMA, 'registry schema')
    need(type(state['identity']) is str and 1 <= len(state['identity']) <= 128, 'identity')
    _digest(state['instance_id'], 32); validate_ref(state['scope_ref'], 'scope')
    paths = state['paths']
    need(type(paths) is list and 1 <= len(paths) <= 8, 'fixed path capacity')
    for path in paths:
        validate_relative_path(path)
        need(path.lower().endswith('.txt'), 'text path')
    need(len({p.casefold() for p in paths}) == len(paths), 'casefold distinct fixed paths')
    slots = _slots(state)
    need(type(state['path_roles']) is dict and set(state['path_roles']) <= slots and
         all(type(v) is str and v in ROLES for v in state['path_roles'].values()), 'permanent role map')
    _counter(state['last_read_seq'])
    need(type(state['queued_count']) is int and 0 <= state['queued_count'] <= MAX_COHORTS,
         'lifetime cohort capacity')
    need(type(state['queue']) is list and len(state['queue']) <= MAX_QUEUE, 'queued capacity')
    need(type(state['archives']) is list and len(state['archives']) <= MAX_COHORTS, 'archive capacity')
    generations = []; cohort_ids = []; archive_refs = set()
    for archive in state['archives']:
        need(type(archive) is dict and set(archive) == {'generation', 'cohort_id', 'archive_ref'},
             'archive fields')
        need(type(archive['generation']) is int, 'archive generation')
        _digest(archive['cohort_id']); _ref(archive['archive_ref'], state, 'compact_cohort_archive')
        need(_hash(archive['archive_ref']) not in archive_refs, 'distinct archive references')
        archive_refs.add(_hash(archive['archive_ref']))
        generations.append(archive['generation']); cohort_ids.append(archive['cohort_id'])
    current = state['current']
    if current is not None:
        need(type(current) is dict and set(current) == CURRENT_FIELDS, 'current fields')
        _queued(current, state); generations.append(current['generation']); cohort_ids.append(current['cohort_id'])
        _source(current['opened_source'])
        need(current['opened_source']['seq'] >= current['queued_source']['seq'] and
             current['opened_source']['cognition_revision'] >= current['queued_source']['cognition_revision'],
             'OPEN after queue')
        _ref(current['incumbent_ref'], state, 'compact_model')
        _ref(current['retention_anchor_ref'], state, 'compact_cohort_anchor')
        need(type(current['protected_skills']) is list and len(current['protected_skills']) == 1,
             'bounded protected skill set')
        for ref in current['protected_skills']:
            _ref(ref, state, 'compact_procedure')
        need(len({_hash(r) for r in current['protected_skills']}) == len(current['protected_skills']),
             'distinct protected skills')
        parent = state['archives'][-1]['archive_ref'] if state['archives'] else None
        need(current['parent_archive_ref'] == parent, 'archive lineage')
        need(type(current['status']) is str and current['status'] in STATUSES_CURRENT, 'current status')
        need(current['reason'] is None or current['reason'] in REASONS, 'retained invalid reason')
        if current['status'] == 'INVALID':
            need(current['reason'] in REASONS, 'invalid reason required')
        elif current['status'] not in ('REJECTED',):
            need(current['reason'] is None, 'reason only for invalid/rejected cohort')
        planned = {s for values in current['plan'].values() for s in values}
        need(type(current['evidence']) is dict and set(current['evidence']) <= planned,
             'first receipt slots')
        seqs = set()
        for slot, evidence in current['evidence'].items():
            need(type(evidence) is dict and set(evidence) ==
                 {'capture_ref', 'seq', 'status', 'content_sha256'}, 'first receipt fields')
            _ref(evidence['capture_ref'], state, 'compact_capture'); _counter(evidence['seq'])
            need(current['opened_source']['seq'] < evidence['seq'] <= state['last_read_seq'] and
                 evidence['seq'] not in seqs, 'first receipt chronology')
            seqs.add(evidence['seq'])
            need(type(evidence['status']) is str and evidence['status'] in STATUSES, 'receipt status')
            if evidence['status'] == 'OK':
                _digest(evidence['content_sha256'])
            else:
                need(evidence['content_sha256'] is None, 'no denied content')
        need(type(current['retention_results']) is dict and
             set(current['retention_results']) <= set(RETENTION_CASES), 'fixed retention cases')
        for ref in current['retention_results'].values():
            _ref(ref, state, 'compact_skill_retention_case')
        need(len({_hash(ref) for ref in current['retention_results'].values()}) ==
             len(current['retention_results']), 'distinct retention capsule references')
        if current['retention_plan_ref'] is not None:
            _ref(current['retention_plan_ref'], state, 'compact_skill_retention_plan')
        else:
            need(not current['retention_results'], 'no retention results before frozen plan')
        need((current['candidate_ref'] is None) == (current['retention_plan_ref'] is None),
             'candidate/retention plan frozen together')
        if current['candidate_ref'] is not None:
            _ref(current['candidate_ref'], state, 'compact_model')
            need(current['candidate_ref'] != current['incumbent_ref'], 'new candidate required')
        if current['report_ref'] is not None:
            _ref(current['report_ref'], state, 'compact_cohort_report')
        if current['status'] in ('TRAINING', 'FROZEN', 'INVALID'):
            need(current['report_ref'] is None, 'unevaluated cohort has no report')
        if current['status'] == 'TRAINING':
            need(current['candidate_ref'] is None and current['report_ref'] is None,
                 'training has no candidate/report')
        if current['status'] in ('FROZEN', 'ACCEPTED', 'ACTIVATED'):
            need(current['candidate_ref'] is not None and
                 all(s in current['evidence'] for s in current['plan']['train']), 'frozen training evidence')
        if current['status'] in ('ACCEPTED', 'ACTIVATED'):
            need(current['report_ref'] is not None and set(current['evidence']) == planned,
                 'accepted complete evidence/report')
            need(set(current['retention_results']) == set(RETENTION_CASES), 'complete old-skill retention gate')
        if current['status'] == 'REJECTED':
            need(current['report_ref'] is not None, 'rejection report')
    for record in state['queue']:
        need(type(record) is dict and set(record) == QUEUED_FIELDS, 'queued fields')
        _queued(record, state); generations.append(record['generation']); cohort_ids.append(record['cohort_id'])
    need(generations == list(range(1, state['queued_count'] + 1)), 'continuous bounded cohort lineage')
    need(len(set(cohort_ids)) == len(cohort_ids), 'distinct cohort identities')
    need(type(state['exposures']) is list and len(state['exposures']) <= MAX_EXPOSURES,
         'exposure capacity')
    digests = set(); first_seqs = set(); previous_first_seq = 0
    for record in state['exposures']:
        need(type(record) is dict and set(record) ==
             {'content_sha256', 'first_seq', 'path_slot', 'path_mask', 'role_mask'}, 'exposure fields')
        _digest(record['content_sha256']); _counter(record['first_seq'])
        need(record['content_sha256'] not in digests and record['first_seq'] not in first_seqs,
             'distinct first exposure identities')
        need(record['first_seq'] > previous_first_seq, 'append-only exposure chronology')
        previous_first_seq = record['first_seq']
        digests.add(record['content_sha256']); first_seqs.add(record['first_seq'])
        need(0 < record['first_seq'] <= state['last_read_seq'] and
             type(record['path_slot']) is str and record['path_slot'] in slots, 'first exposure source')
        need(type(record['path_mask']) is int and 0 < record['path_mask'] < 2**len(paths) and
             record['path_mask'] & (1 << int(record['path_slot'])), 'exposure path mask')
        need(type(record['role_mask']) is int and 0 <= record['role_mask'] < 2**len(ROLES),
             'exposure role mask')
        possible = sum(1 << i for i, role in enumerate(ROLES) if any(
            record['path_mask'] & (1 << int(slot)) and assigned == role
            for slot, assigned in state['path_roles'].items()))
        need(record['role_mask'] == possible, 'complete role mask from observed assigned paths')
    need(type(state['eval_exposures']) is list and len(state['eval_exposures']) <= MAX_EXPOSURES,
         'evaluation exposure capacity')
    for digest in state['eval_exposures']:
        _digest(digest)
    need(state['eval_exposures'] == sorted(set(state['eval_exposures'])),
         'sorted unique evaluation exposure digests')
    evaluation_digests = set(state['eval_exposures'])
    need(len(digests | evaluation_digests) <= MAX_EXPOSURES, EXPOSURE_CAPACITY_REASON)
    if current is not None:
        by_digest = {r['content_sha256']: r for r in state['exposures']}
        for slot, evidence in current['evidence'].items():
            if evidence['status'] == 'OK':
                record = by_digest.get(evidence['content_sha256'])
                need(record is not None and record['first_seq'] <= evidence['seq'] and
                     record['path_mask'] & (1 << int(slot)), 'first evidence exposure retained')
                if current['status'] in ('TRAINING', 'FROZEN', 'ACCEPTED', 'ACTIVATED'):
                    need(record['first_seq'] == evidence['seq'] and record['path_slot'] == slot,
                         'valid cohort requires novel first content on its declared path')
                    need(evidence['content_sha256'] not in evaluation_digests,
                         'valid cohort excludes disclosed evaluation bank content')
            elif current['status'] in ('TRAINING', 'FROZEN', 'ACCEPTED', 'ACTIVATED'):
                need(False, 'valid cohort requires successful first receipts')
        if current['status'] == 'TRAINING':
            need(set(current['evidence']) <= set(current['plan']['train']), 'training before heldout')
    try:
        size = len(_encode(state))
    except (ValueError, TypeError, UnicodeError) as exc:
        raise ValueError('COMPACT_COHORTS_INVALID: strict JSON') from exc
    need(size <= MAX_BYTES, 'registry byte capacity')
    return copy.deepcopy(state)


def initial_registry(identity, instance_id, scope_ref, paths):
    return validate_registry(dict(schema=SCHEMA, identity=identity, instance_id=instance_id,
        scope_ref=scope_ref, paths=copy.deepcopy(paths), path_roles={}, queued_count=0,
        queue=[], current=None, archives=[], exposures=[], eval_exposures=[], last_read_seq=0))


def _max_ref(state, kind):
    return dict(schema='life-object-1', identity=state['identity'], instance_id=state['instance_id'],
        kind=kind, encoding='json', payload_bytes=8388608, sha256='f'*64, byte_size=8389633)


def _completion_capacity(state):
    """Reserve a conservative complete current cohort plus one next READ record.

    This is only JSON capacity. The runtime must separately reserve object-store
    and journal slots before an external effect or a root publication.
    """
    value = copy.deepcopy(state)
    value['last_read_seq'] = MAX_COUNTER
    current = value['current']
    if current is None and value['queue']:
        current = copy.deepcopy(value['queue'][0])
        current.update(opened_source=dict(seq=MAX_COUNTER, head='f'*64, cognition_revision=MAX_COUNTER),
            incumbent_ref=_max_ref(state, 'compact_model'),
            retention_anchor_ref=_max_ref(state, 'compact_cohort_anchor'),
            protected_skills=[_max_ref(state, 'compact_procedure')],
            parent_archive_ref=_max_ref(state, 'compact_cohort_archive'), status='ACTIVATED',
            reason=None, evidence={}, candidate_ref=None, report_ref=None, retention_plan_ref=None, retention_results={})
        value['current'] = current  # Queue is intentionally retained: conservative envelope.
    if current is not None:
        for slot in {s for values in current['plan'].values() for s in values}:
            current['evidence'][slot] = dict(capture_ref=_max_ref(state, 'compact_capture'),
                seq=MAX_COUNTER, status='LIMIT_EXCEEDED', content_sha256='f'*64)
        current['candidate_ref'] = _max_ref(state, 'compact_model')
        current['report_ref'] = _max_ref(state, 'compact_cohort_report')
        current['retention_plan_ref'] = _max_ref(state, 'compact_skill_retention_plan')
        current['retention_results'] = {case: _max_ref(state, 'compact_skill_retention_case') for case in RETENTION_CASES}
        current['reason'] = max(REASONS, key=len)
        current['status'] = max(STATUSES_CURRENT, key=len)
        value['archives'].append(dict(generation=MAX_COHORTS, cohort_id='f'*64,
            archive_ref=_max_ref(state, 'compact_cohort_archive')))
    value['exposures'].append(dict(content_sha256='f'*64, first_seq=MAX_COUNTER,
        path_slot='7', path_mask=255, role_mask=15))
    need(len(_encode(value)) <= MAX_BYTES, 'future completion byte capacity')


def _exposed_digests(state):
    return {record['content_sha256'] for record in state['exposures']} | set(state['eval_exposures'])


def invalidate_current(state, reason):
    """Retain a runtime-authenticated ancestral-training collision outcome.

    Only the runtime can establish actual fitted membership from authenticated
    model ancestry. Merely assigning a source path the train role does not.
    Existing INVALID evidence and its first reason are never overwritten.
    """
    state = validate_registry(state)
    need(type(reason) is str and reason == 'EVALUATION_BANK_TRAINING_COLLISION',
         'supported runtime-derived invalidation reason')
    current = state['current']
    need(current is not None and current['status'] in ('TRAINING', 'FROZEN', 'INVALID'),
         'unevaluated active cohort required for invalidation')
    if current['status'] != 'INVALID':
        current.update(status='INVALID', reason=reason)
    return validate_registry(state)


def register_evaluation_digests(state, digests):
    """Burn successful READ contents disclosed by verified child evaluations.

    Caller authenticates closed child captures and atomically commits this with
    the import. Child observations never acquire a fabricated parent sequence
    or path. Register after record_retention, as overlap can invalidate FROZEN.
    The 256 ceiling covers distinct contents across both provenance classes;
    the independent byte envelope can refuse earlier. No entries are cleared.
    """
    state = validate_registry(state)
    need(type(digests) is list and len(digests) <= MAX_EXPOSURES, 'bounded evaluation digest import')
    for digest in digests:
        _digest(digest)
    registered = set(state['eval_exposures']) | set(digests)
    need(len({record['content_sha256'] for record in state['exposures']} | registered) <=
         MAX_EXPOSURES, EXPOSURE_CAPACITY_REASON)
    state['eval_exposures'] = sorted(registered)
    current = state['current']
    if current is not None and current['status'] in ('TRAINING', 'FROZEN'):
        if any(evidence['content_sha256'] in registered for evidence in current['evidence'].values()
               if evidence['status'] == 'OK'):
            current.update(status='INVALID', reason='EVALUATION_BANK_CONTENT_OVERLAP')
    state = validate_registry(state)
    _completion_capacity(state)
    return state


def preflight_read(state):
    state = validate_registry(state)
    need(len(_exposed_digests(state)) < MAX_EXPOSURES, EXPOSURE_CAPACITY_REASON)
    need(state['last_read_seq'] < MAX_COUNTER, 'read sequence capacity')
    _completion_capacity(state)
    return True


def can_collect_read(state):
    """Validate and raise a stable refusal before READ, otherwise return True."""
    return preflight_read(state)


def queue_plan(state, plan, source):
    state = validate_registry(state); _source(source)
    need(state['queued_count'] < MAX_COHORTS and len(state['queue']) < MAX_QUEUE,
         'cohort/queue capacity exhausted')
    need(source['seq'] >= state['last_read_seq'], 'queue source precedes exposure')
    earlier_sources = [r['queued_source'] for r in state['queue']]
    if state['current'] is not None:
        earlier_sources.append(state['current']['opened_source'])
    need(all(source['seq'] >= earlier['seq'] and source['cognition_revision'] >= earlier['cognition_revision']
             for earlier in earlier_sources), 'queue source chronology')
    queued_plan = _plan(plan, state, paths=True)
    generation = state['queued_count'] + 1
    for role, slots in queued_plan.items():
        for slot in slots:
            state['path_roles'][slot] = role
    for exposure in state['exposures']:
        exposure['role_mask'] = sum(1 << i for i, role in enumerate(ROLES) if any(
            exposure['path_mask'] & (1 << int(slot)) and assigned == role
            for slot, assigned in state['path_roles'].items()))
    record = dict(generation=generation, cohort_id=_cohort_id(state, generation, queued_plan, source),
                  plan=queued_plan, queued_source=copy.deepcopy(source))
    state['queue'].append(record); state['queued_count'] = generation
    state = validate_registry(state); _completion_capacity(state)
    return state


def open_next(state, source, incumbent_ref, protected_skills, anchor_ref):
    state = validate_registry(state); _source(source)
    need(state['current'] is None and state['queue'], 'close current before OPEN next queued cohort')
    need(source['seq'] >= state['last_read_seq'], 'OPEN source precedes exposure')
    need(all(source['seq'] >= r['queued_source']['seq'] and
             source['cognition_revision'] >= r['queued_source']['cognition_revision']
             for r in state['queue']), 'OPEN source follows admitted queue')
    _ref(incumbent_ref, state, 'compact_model')
    _ref(anchor_ref, state, 'compact_cohort_anchor')
    current = state['queue'].pop(0)
    current.update(opened_source=copy.deepcopy(source), incumbent_ref=copy.deepcopy(incumbent_ref),
        protected_skills=copy.deepcopy(protected_skills), retention_anchor_ref=copy.deepcopy(anchor_ref),
        parent_archive_ref=copy.deepcopy(state['archives'][-1]['archive_ref']) if state['archives'] else None,
        status='TRAINING', reason=None, evidence={}, candidate_ref=None, report_ref=None, retention_plan_ref=None, retention_results={})
    state['current'] = current
    state = validate_registry(state); _completion_capacity(state)
    return state


def collect(state, path, item, capture_ref, seq):
    state = validate_registry(state)
    need(type(path) is str and path in state['paths'], 'exact fixed READ path')
    validate_capture(item)
    need(item['request']['operation'] == 'READ_TEXT' and
         item['request']['scope_ref'] == state['scope_ref'], 'scoped READ only')
    _ref(capture_ref, state, 'compact_capture'); _counter(seq)
    need(seq > state['last_read_seq'], 'strict actual READ sequence')
    # Caller must have admitted this before executing, including possible duplicates.
    preflight_read(state)
    slot = str(state['paths'].index(path)); result = item['capture']['result']
    content = result['payload']['content_sha256'] if result['status'] == 'OK' else None
    prior = next((r for r in state['exposures'] if r['content_sha256'] == content), None)
    current = state['current']
    if current is not None:
        need(seq > current['opened_source']['seq'], 'receipt must follow OPEN')
    if current is not None and current['status'] in ('TRAINING', 'FROZEN', 'INVALID'):
        role = next((r for r, slots in current['plan'].items() if slot in slots), None)
        if role is not None:
            need(seq > current['opened_source']['seq'], 'receipt must follow OPEN')
            if slot not in current['evidence']:
                current['evidence'][slot] = dict(capture_ref=copy.deepcopy(capture_ref), seq=seq,
                    status=result['status'], content_sha256=content)
                reason = None
                if role != 'train' and current['candidate_ref'] is None:
                    reason = 'HELDOUT_BEFORE_FREEZE'
                elif result['status'] != 'OK':
                    reason = 'FIRST_READ_NOT_SUCCESSFUL'
                elif len(result['payload']['text']) < 2:
                    reason = 'INSUFFICIENT_TRANSITIONS'
                elif prior is not None or content in state['eval_exposures']:
                    reason = 'PREVIOUSLY_EXPOSED_CONTENT'
                if reason is not None and current['status'] != 'INVALID':
                    current.update(status='INVALID', reason=reason)
    if content is not None:
        role = state['path_roles'].get(slot)
        role_mask = 0 if role is None else 1 << ROLES.index(role)
        if prior is None:
            state['exposures'].append(dict(content_sha256=content, first_seq=seq,
                path_slot=slot, path_mask=1 << int(slot), role_mask=role_mask))
        else:
            prior['path_mask'] |= 1 << int(slot); prior['role_mask'] |= role_mask
    state['last_read_seq'] = seq
    return validate_registry(state)


def mark_frozen(state, candidate_ref, retention_plan_ref):
    state = validate_registry(state); current = state['current']
    need(current is not None and current['status'] == 'TRAINING', 'valid TRAINING cohort')
    need(all(slot in current['evidence'] for slot in current['plan']['train']), 'complete first training receipts')
    _ref(candidate_ref, state, 'compact_model')
    need(candidate_ref != current['incumbent_ref'], 'new candidate required')
    _ref(retention_plan_ref, state, 'compact_skill_retention_plan')
    current.update(status='FROZEN', candidate_ref=copy.deepcopy(candidate_ref),
                   retention_plan_ref=copy.deepcopy(retention_plan_ref))
    return validate_registry(state)


def record_retention(state, case, capsule_ref):
    state = validate_registry(state); current = state['current']
    need(current is not None and current['status'] == 'FROZEN', 'frozen retention plan required')
    need(type(case) is str and case in RETENTION_CASES, 'fixed retention case')
    need(case not in current['retention_results'], 'first retention result cannot be replaced')
    _ref(capsule_ref, state, 'compact_skill_retention_case')
    need(capsule_ref not in current['retention_results'].values(), 'distinct retention capsules')
    current['retention_results'][case] = copy.deepcopy(capsule_ref)
    return validate_registry(state)


def mark_evaluated(state, report_ref, decision):
    state = validate_registry(state); current = state['current']
    need(current is not None and current['status'] in ('TRAINING', 'FROZEN', 'INVALID'),
         'unevaluated current cohort')
    need(type(decision) is str and decision in ('ACCEPT', 'REJECT'), 'derived evaluation decision')
    _ref(report_ref, state, 'compact_cohort_report')
    if decision == 'ACCEPT':
        need(set(current['retention_results']) == set(RETENTION_CASES), 'complete old-skill retention gate')
        need(current['status'] == 'FROZEN' and all(s in current['evidence']
             for slots in current['plan'].values() for s in slots), 'complete frozen acceptance')
    current.update(status='ACCEPTED' if decision == 'ACCEPT' else 'REJECTED',
                   report_ref=copy.deepcopy(report_ref))
    return validate_registry(state)


def mark_activated(state):
    state = validate_registry(state); current = state['current']
    need(current is not None and current['status'] == 'ACCEPTED', 'accepted current cohort')
    current['status'] = 'ACTIVATED'
    return validate_registry(state)


def close_current(state, archive_ref):
    state = validate_registry(state); current = state['current']
    need(current is not None and current['status'] in TERMINAL, 'terminal cohort required')
    _ref(archive_ref, state, 'compact_cohort_archive')
    state['archives'].append(dict(generation=current['generation'], cohort_id=current['cohort_id'],
                                 archive_ref=copy.deepcopy(archive_ref)))
    state['current'] = None
    return validate_registry(state)
