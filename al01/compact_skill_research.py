"""Unconnected research contract/condition hypothesis; no runtime or I/O.

No experiment may use fit_candidate until its external world/manifest is
reviewed and frozen. A syntactically FROZEN document is not host permission.
All proposals are staged CL, never dispatched, adopted or written here.
"""
import copy

from . import compact_skill_evidence as evidence
from . import compact_procedures as p
from . import compact_semantics as semantics
from .core import CVM, transaction_payload, VERSION
from .folder_session import _encode, _hash
from .life_objects import _json

PROFILE = 'direct-earlier-ref-conditions-1'
CONTRACT = 'compact-skill-conditions-research-contract-1'
CANDIDATE = 'compact-skill-conditions-research-candidate-1'
CONTROLS = ('LEARNED', 'UNTRAINED', 'ALWAYS_ABSTAIN', 'SHUFFLED_OUTCOMES')
CELLS = ('MATCH', 'DIFFERENT')
MAX_TRAIN = 3
MAX_DOCUMENT = 24576
MAX_RESEARCH_FRAMES = 512
MAX_RESEARCH_FRAME_BYTES = 32 * 1024 * 1024


def need(ok, reason):
    if not ok:
        raise ValueError('SKILL_RESEARCH_INVALID: ' + reason)


class _ReadBudget:
    def __init__(self, reader):
        self.reader, self.cache, self.size = reader, {}, 0
    def __call__(self, ref):
        key = ref['sha256']
        if key in self.cache:
            previous, raw = self.cache[key]
            need(_encode(previous) == _encode(ref), 'global reference alias')
            return raw
        if len(self.cache) >= MAX_RESEARCH_FRAMES or self.size + ref['byte_size'] > MAX_RESEARCH_FRAME_BYTES:
            raise evidence._Unavailable('RESEARCH_FRAME_BUDGET')
        raw = self.reader(ref)
        need(type(raw) is bytes and len(raw) == ref['byte_size'], 'bounded frame reader')
        self.cache[key] = (copy.deepcopy(ref), raw)
        self.size += len(raw)
        return raw


def direct_prefix_signatures():
    """Finite equality model of immediate DIRECT admission, not a live run.

    Only the pre-goal LIST can change either opaque reference. The second
    claim READ necessarily matches that LIST; no post-admission REFRESH.
    """
    rows = []
    for listed_again in (False, True):
        for earlier_changed in ((False, True) if listed_again else (False,)):
            for later_changed in ((False, True) if listed_again else (False,)):
                current = (int(earlier_changed), int(later_changed))
                last_reads = (0, current[1])
                signature = tuple('MATCH' if a == b else 'DIFFERENT' for a, b in zip(last_reads, current))
                rows.append(dict(pre_goal_list=listed_again, relations=signature,
                    acquisition_effects=4 if listed_again else 3, skill_effects=2))
    return rows


def draft_contract():
    return dict(schema=CONTRACT, profile=PROFILE, status='DRAFT', environment_sha256=None,
        source_identity=None, trials=[], min_support=2, shuffle_offset=1,
        controls=list(CONTROLS), max_effects_per_trial=6, max_train=MAX_TRAIN,
        max_source_topics=8, max_source_records=32, authority='NONE')


def validate_contract(contract, *, frozen=False, pinned_sha256=None):
    _json(contract, MAX_DOCUMENT)  # before hash/copy
    need(type(contract) is dict and set(contract) == set(draft_contract()), 'contract fields')
    need(contract['schema'] == CONTRACT and contract['profile'] == PROFILE
         and contract['status'] in ('DRAFT', 'FROZEN') and contract['authority'] == 'NONE', 'contract version')
    for key in ('min_support', 'shuffle_offset', 'max_effects_per_trial', 'max_train', 'max_source_topics', 'max_source_records'):
        need(type(contract[key]) is int and contract[key] == draft_contract()[key], 'fixed bound ' + key)
    need(contract['controls'] == list(CONTROLS), 'honest controls')
    trials = contract['trials']
    need(type(trials) is list and len(trials) <= 8, 'eight topic/32-record envelope')
    ids, groups, seeds, topics = set(), set(), set(), set()
    counts = dict(BASE=0, TRAIN=0, VALIDATION=0, TEST=0)
    for t in trials:
        need(type(t) is dict and set(t) == {'id', 'partition', 'group', 'schedule_sha256', 'world_seed', 'subject', 'predicate'}, 'trial fields')
        need(t['partition'] in counts, 'trial partition')
        for key in ('id', 'group', 'subject', 'predicate'):
            need(type(t[key]) is str and 1 <= len(t[key]) <= 128, 'trial text')
        need(type(t['schedule_sha256']) is str and len(t['schedule_sha256']) == 64
             and all(c in '0123456789abcdef' for c in t['schedule_sha256']), 'schedule hash')
        need(type(t['world_seed']) is int and 0 <= t['world_seed'] < 2**64, 'world seed')
        # Exclusion is governance isolation, never a learned seed feature.
        need(t['world_seed'] not in {19, 23, 29, 41, 73, 97, 101, 131, 149}, 'reserved source/evaluation seed')
        topic = semantics._topic(t['subject'], t['predicate'])
        need(t['id'] not in ids and t['group'] not in groups and t['world_seed'] not in seeds and topic not in topics, 'nonoverlapping declared trials')
        ids.add(t['id']); groups.add(t['group']); seeds.add(t['world_seed']); topics.add(topic)
        counts[t['partition']] += 1
    need(counts['BASE'] <= 2 and counts['TRAIN'] <= MAX_TRAIN
         and counts['VALIDATION'] <= 1 and counts['TEST'] <= 2, 'phase limits')
    if frozen:
        need(contract['status'] == 'FROZEN', 'CONTRACT_NOT_FROZEN')
        need(pinned_sha256 == _hash(contract), 'reviewed contract pin')
        need(type(contract['environment_sha256']) is str and len(contract['environment_sha256']) == 64
             and all(c in '0123456789abcdef' for c in contract['environment_sha256']), 'reviewed environment missing')
        need(type(contract['source_identity']) is str and 1 <= len(contract['source_identity']) <= 128, 'source identity missing')
        need(counts == dict(BASE=2, TRAIN=3, VALIDATION=1, TEST=2), 'incomplete experiment manifest')
    return copy.deepcopy(contract)


def verify_child(child_ref, *, origin, read_frame):
    """Local original skill hypothesis, not a foreign instance's experience."""
    objects = evidence._Objects(origin, read_frame)
    child = p.validate_candidate(objects.read(child_ref, 'compact_procedure'))
    need(child['schema'] == 'compact-procedure-candidate-1' and child['origin'] == origin, 'one local fixed-order child required')
    archived = objects.read(child['source_workflow_ref'], 'compact_workflow')
    need(_encode(p.extract(archived, child['source_workflow_ref'], origin, objects.read)) == _encode(child), 'recomputed chronological child')
    return child


def _predecision_context(*, origin, read_frame, journal_proof):
    """Verify a current pinned head before REQUEST; never needs future feedback.

    The narrower existing experience API requires an already admitted first
    REQUEST and cannot be reused as a live preselection oracle.
    """
    objects = evidence._Objects(origin, read_frame)
    events, endpoint, base_ref, base_known = evidence._journal(journal_proof, origin)
    cp = evidence._envelope(journal_proof['checkpoint'])
    need('active' in cp, 'current head, not a sealed historical cut')
    state = cp['state']
    unknown = dict(status='UNKNOWN', reason='PRECONDITIONS_UNPROVEN', feature='UNKNOWN', authority='NONE')
    cfg = state['config']
    if (state['pending'] is not None or cfg['mode'] != 'SYNTHETIC_CONTINUATION'
            or cfg.get('choice_profile') is not None or cfg.get('renewal_profile') is not None
            or cfg.get('native_binding') is not None or cfg.get('procedure_probe') is not None
            or cfg.get('autonomous_help') is not None or cfg.get('local_endpoints')):
        return unknown
    w = p.validate_workflow(objects.read(state['workflow_ref'], 'compact_workflow'))
    g = w['goal']
    if (g is None or g['status'] != 'ACTIVE' or g['cursor'] != 0 or g['receipts']
            or w['active_ref'] is not None or w['interaction']['pending'] is not None
            or w['typed_wait'] is not None or not state['inventory']['valid']):
        return unknown
    index = semantics.validate_semantics(objects.read(state['semantics_ref'], 'compact_semantics'))
    need(all(index[k] == v for k, v in origin.items()), 'semantic origin')
    ctx = p.context(index, g['topic'], state['goals'])
    if ctx is None or ctx['episode_key'] != g['episode_key'] or ctx['bindings'] != g['bindings'] or index['overflow'] is not None:
        return unknown
    by_seq = {e['seq']: e for e in events}
    inv = state['inventory']
    capture_origin = cfg['capture_origin']
    bindings, relations = {}, {}
    for role in p.ROLES:
        path = state['goals'][g['bindings'][role]]['path']
        entries = [e for e in inv['entries'] if e['kind'] == 'file' and e['relative_path'] == path]
        if len(entries) != 1:
            return unknown
        bindings[role] = entries[0]['entry_ref']
        latest = state['latest_by_path'].get(path)
        relations[role] = 'UNKNOWN'
        if latest is None or latest['last_status'] != 'OK':
            continue
        seq = latest['last_seq']
        request, start, receipt = (by_seq.get(seq - 2), by_seq.get(seq - 1), by_seq.get(seq))
        if request is None or start is None or receipt is None:
            continue
        need(request['kind'] == 'REQUEST' and start['kind'] == 'START' and receipt['kind'] == 'RECEIPT', 'query receipt barriers')
        eid = origin['identity'] + ':' + str(seq - 2)
        need(request['effect'] == dict(phase='REQUEST', id=eid) and start['effect'] == dict(phase='START', id=eid)
             and receipt['effect'] == dict(phase='RECEIPT', id=eid), 'query effect identity')
        d, local = evidence._decision(objects, request['payload']['decision_ref'], origin)
        need(not local and d['source']['global_seq'] == seq - 3 and d['source']['journal_head'] == request['previous'], 'query source chronology')
        need(receipt['payload']['capture_ref'] == latest['last_receipt_ref'], 'query latest receipt')
        item = objects.read(latest['last_receipt_ref'], 'compact_capture')
        evidence.validate_capture(item)
        need(item['request'] == d['request'] and item['capture']['result']['status'] == 'OK'
             and item['capture']['capture_origin'] == capture_origin
             and item['capture']['result']['payload']['content_sha256'] == latest['content_sha256']
             and evidence._path(objects, d, origin, capture_origin) == path,
             'query original request/source')
        relations[role] = 'MATCH' if item['request']['entry_ref'] == bindings[role] else 'DIFFERENT'
    # DIRECT only: the last admitted receipt is precisely the second source's
    # successful claim, and its workflow is this newly created empty goal.
    last = state['last_receipt']
    later = state['goals'][g['bindings']['LATER_SOURCE']]['path']
    latest = state['latest_by_path'].get(later)
    if last is None or latest is None or last['seq'] != latest['last_seq'] or last['workflow_ref'] != state['workflow_ref']:
        return unknown
    if relations['LATER_SOURCE'] != 'MATCH':
        return unknown
    # Prove this receipt actually created the target. In particular, an
    # ordinary READ after an in-goal REFRESH cannot masquerade as admission.
    prior_ref, known = base_ref, base_known
    for event in events:
        if event['seq'] > last['seq'] - 3:
            break
        if event['kind'] in ('INITIALIZE', 'RECEIPT', 'LOCAL_RECEIPT'):
            prior_ref, known = event['payload']['workflow_ref'], True
        elif 'record_ref' in event['payload']:
            ref = event['payload']['record_ref']
            if ref['kind'] != 'compact_workflow_control':
                return unknown
            record = objects.read(ref, 'compact_workflow_control')
            need(record['kind'] == event['kind'] and record['source']['global_seq'] == event['seq'] - 1
                 and record['source']['journal_head'] == event['previous']
                 and all(record['source'][k] == v for k, v in origin.items()), 'prior control source')
            prior_ref, known = record['workflow_ref'], True
    if not known or prior_ref is None:
        return unknown
    prior = p.validate_workflow(objects.read(prior_ref, 'compact_workflow'))['goal']
    if prior is not None and (prior['id'] == g['id'] or prior['status'] not in ('SATISFIED', 'BLOCKED', 'CANCELLED')):
        return unknown
    return dict(status='VERIFIED', origin=copy.deepcopy(origin), goal_id=g['id'], topic=g['topic'], episode_key=g['episode_key'],
        seq=endpoint, feature=relations['EARLIER_SOURCE'], relations=relations, bindings=bindings,
        version=state['cognition_revision'], world_version=inv['generation'],
        capabilities=copy.deepcopy(cfg['capabilities']), state_sha256=_hash(state), authority='NONE')


def predecision_context(*, origin, read_frame, journal_proof):
    try:
        return _predecision_context(origin=origin, read_frame=read_frame, journal_proof=journal_proof)
    except evidence._Unavailable as error:
        return dict(status='UNKNOWN', reason=str(error), feature='UNKNOWN', authority='NONE')


def _condition_table(rows, offset=0):
    """Declared finite rule. Outcome permutation is a separate control view;
    original evidence order, refs and outcome records are never changed."""
    table = {}
    for cell in CELLS:
        supporting, failures = set(), set()
        for i, row in enumerate(rows):
            label = rows[(i + offset) % len(rows)]['label'] if rows else 'UNKNOWN'
            if row['feature'] == cell:
                if label == 'SUCCESS': supporting.add(row['group'])
                if label == 'SOURCE_FAILURE': failures.add(row['group'])
        table[cell] = dict(support=len(supporting), failures=len(failures),
            decision='SUPPORTED_HYPOTHESIS' if len(supporting) >= 2 and not failures else 'UNKNOWN')
    # Without a different-cell failure contrast, no applicability claim.
    if not any(t['failures'] for t in table.values()):
        for t in table.values(): t['decision'] = 'UNKNOWN'
    return table


def evaluation_topic(contract, topic):
    """Protocol isolation only, identical for every control; never a model feature."""
    return any(t['partition'] in ('VALIDATION', 'TEST') and semantics._topic(t['subject'], t['predicate']) == topic
               for t in contract['trials'])


def fit_candidate(contract, *, contract_pin, origin, child_ref, base_examples, examples, read_frame):
    """Future frozen-contract entry only. Not called on training data this turn.

    Each arm must acquire its own BASE and TRAIN receipts in its own instance.
    Declared independent groups are audited manifest metadata, not a proof of
    causal independence or an input feature. Reliability remains UNKNOWN.
    """
    plan = validate_contract(contract, frozen=True, pinned_sha256=contract_pin)
    read_frame = read_frame if isinstance(read_frame, _ReadBudget) else _ReadBudget(read_frame)
    need(origin['identity'] == plan['source_identity'], 'source identity')
    child = verify_child(child_ref, origin=origin, read_frame=read_frame)
    base_end = max(s['receipt_seq'] for chain in child['chains'] for s in chain['steps'])
    base_trials = {t['id']: t for t in plan['trials'] if t['partition'] == 'BASE'}
    need(type(base_examples) is list and len(base_examples) == 2, 'complete base episode proofs required')
    base_ids = set()
    for chain, x in zip(child['chains'], base_examples):
        need(type(x) is dict and set(x) == {'trial_id', 'first_decision_ref', 'before_state', 'journal_proof'}
             and x['trial_id'] in base_trials and x['trial_id'] not in base_ids
             and x['first_decision_ref'] == chain['steps'][0]['decision_ref'], 'original base chain order/provenance')
        base_ids.add(x['trial_id'])
        b = evidence.verify_experience(x['first_decision_ref'], expected_origin=origin, read_frame=read_frame,
            before_state=x['before_state'], journal_proof=x['journal_proof'])
        if b['outcome'] != 'COMPLETE_UNASSISTED_IN_GOAL_SPAN':
            return dict(schema=CANDIDATE, profile=PROFILE, origin=copy.deepcopy(origin), child_ref=copy.deepcopy(child_ref),
                contract_sha256=_hash(plan), status='UNKNOWN_BASE_SPAN_OR_INTERVENTION', authority='NONE', reliability='UNKNOWN')
        base_goal = evidence._Objects(origin, read_frame).read(x['before_state']['workflow_ref'], 'compact_workflow')['goal']
        bt = base_trials[x['trial_id']]
        need(base_goal['topic'] == semantics._topic(bt['subject'], bt['predicate']), 'declared base topic')
    expected = {t['id']: t for t in plan['trials'] if t['partition'] == 'TRAIN'}
    need(type(examples) is list and len(examples) == len(expected) <= MAX_TRAIN, 'all declared training attempts required')
    rows, seen, seen_goals = [], set(), set()
    for x in examples:
        need(type(x) is dict and set(x) == {'trial_id', 'first_decision_ref', 'before_state', 'journal_proof'}, 'example fields')
        need(x['trial_id'] in expected and x['trial_id'] not in seen, 'unfiltered training coverage')
        seen.add(x['trial_id']); trial = expected[x['trial_id']]
        before = x['before_state']
        need(type(before) is dict, 'full own pre-state required')
        g = evidence._Objects(origin, read_frame).read(before['workflow_ref'], 'compact_workflow')['goal']
        need(g is not None and g['topic'] == semantics._topic(trial['subject'], trial['predicate'])
             and g['id'] not in seen_goals, 'unique declared own trial goal')
        seen_goals.add(g['id'])
        report = evidence.verify_experience(x['first_decision_ref'], expected_origin=origin, read_frame=read_frame,
            before_state=before, journal_proof=x['journal_proof'])
        actual = report.get('actual_receipts', [])
        if not actual or report.get('boundary', {}).get('value') not in ('VERIFIED_COMPLETED', 'VERIFIED_CANCELLED'):
            return dict(schema=CANDIDATE, profile=PROFILE, origin=copy.deepcopy(origin), child_ref=copy.deepcopy(child_ref),
                contract_sha256=_hash(plan), status='UNKNOWN_INCOMPLETE_EVIDENCE', reason='trial not provably ended',
                trial_id=x['trial_id'], authority='NONE', reliability='UNKNOWN')
        need(actual[0]['receipt_seq'] > base_end, 'training after source skill formation')
        feature = report['features']['EARLIER_SOURCE']['value']
        cfg, last = before['config'], before['last_receipt']
        later = before['goals'][g['bindings']['LATER_SOURCE']]['path']
        latest_later = before['latest_by_path'].get(later)
        direct = (cfg['mode'] == 'SYNTHETIC_CONTINUATION' and cfg.get('autonomous_help') is None
            and not cfg.get('local_endpoints') and cfg.get('choice_profile') is None
            and last is not None and latest_later is not None
            and last['seq'] == report['span']['admission_seq'] == latest_later['last_seq']
            and last['workflow_ref'] == before['workflow_ref']
            and report['features']['LATER_SOURCE']['value'] == 'MATCH')
        if not direct:
            feature = 'UNKNOWN'
        # No semantic feature coverage from an oracle; all labels recomputed.
        label = 'UNKNOWN'
        if (direct and report['span']['value'] == 'VERIFIED' and report['assistance'] == 'NONE_IN_GOAL_SPAN'
                and report.get('boundary', {}).get('value') in ('VERIFIED_COMPLETED', 'VERIFIED_CANCELLED')
                and [r['role'] for r in actual] == child['order'][:len(actual)]):
            if report['outcome'] == 'COMPLETE_UNASSISTED_IN_GOAL_SPAN': label = 'SUCCESS'
            elif any(r['status'] in ('STALE_REF', 'NOT_FOUND') for r in actual): label = 'SOURCE_FAILURE'
        rows.append(dict(trial_id=x['trial_id'], group=trial['group'], feature=feature, label=label,
            first_decision_ref=copy.deepcopy(x['first_decision_ref']), episode_key=g['episode_key'],
            end_seq=report['span']['through_seq'], report_sha256=_hash(report)))
    rows.sort(key=lambda r: r['end_seq'])  # true chronology, never outcome sorting
    need(len({r['end_seq'] for r in rows}) == len(rows), 'distinct ended trials')
    candidate = dict(schema=CANDIDATE, profile=PROFILE, origin=copy.deepcopy(origin), child_ref=copy.deepcopy(child_ref),
        contract_sha256=_hash(plan), rows=rows, table=_condition_table(rows), cl_version=VERSION,
        reliability='UNKNOWN', status='RESEARCH_HYPOTHESIS_NOT_ADOPTED', authority='NONE')
    _json(candidate, MAX_DOCUMENT)
    return candidate


def stage_choice(candidate, *, contract, contract_pin, origin, child_ref, base_examples, examples,
                 query_proof, read_frame, control='LEARNED'):
    """Reprove candidate + current context, then stage at most one CL proposal.
    No callback, runtime request, goal mutation or adoption path is present."""
    _json(candidate, MAX_DOCUMENT)
    read_frame = _ReadBudget(read_frame)
    need(control in CONTROLS, 'registered control')
    recomputed = fit_candidate(contract, contract_pin=contract_pin, origin=origin,
        child_ref=child_ref, base_examples=base_examples, examples=examples, read_frame=read_frame)
    need(_encode(candidate) == _encode(recomputed), 'candidate evidence recomputation')
    if candidate['status'] != 'RESEARCH_HYPOTHESIS_NOT_ADOPTED':
        return dict(selection='ABSTAIN', proposals=[], reason='PROTOCOL_INCOMPLETE_NOT_A_SCORED_CONTROL', authority='NONE', dispatched=False)
    context = predecision_context(origin=origin, read_frame=read_frame, journal_proof=query_proof)
    abstain = dict(selection='ABSTAIN', proposals=[], reason='UNKNOWN', authority='NONE', dispatched=False)
    if context['status'] != 'VERIFIED': return abstain
    need(evaluation_topic(contract, context['topic']), 'unseen declared evaluation topic required')
    need(context['seq'] > max(r['end_seq'] for r in candidate['rows']), 'future use after training')
    table = candidate['table'] if control != 'SHUFFLED_OUTCOMES' else _condition_table(candidate['rows'], contract['shuffle_offset'])
    use = control == 'UNTRAINED' or (control != 'ALWAYS_ABSTAIN' and context['feature'] in table
        and table[context['feature']]['decision'] == 'SUPPORTED_HYPOTHESIS')
    if not use: return abstain
    child = verify_child(child_ref, origin=origin, read_frame=read_frame)
    target = context['bindings'][child['order'][0]]
    cl = f'PROPOSE(OBSERVE(@{target}));'
    vm = CVM(('read', 'observe'))
    need({'read', 'observe'} <= set(context['capabilities']), 'current read/observe capabilities')
    tx = vm.prepare(cl, dict(version=context['version'], world_version=context['world_version']), list(context['bindings'].values()))
    vm.verify(tx)
    return dict(selection='SKILL_FIRST_STEP', generated_cl=cl, transaction_payload=transaction_payload(tx),
        proposals=transaction_payload(tx)['proposals'], source_state_sha256=context['state_sha256'],
        goal_id=context['goal_id'], authority='NONE', dispatched=False)
