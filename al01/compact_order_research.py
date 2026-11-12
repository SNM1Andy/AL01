"""Isolated finite OBSERVE-order research. No runtime, world, I/O or adoption.

Every trial resets a folder_cognition projection. Only the source ledger
persists across trials; this is not ordinary individual state continuity.
Hashes are trusted-controller integrity pins, not strong identity authority.
"""
import copy

from . import folder_cl, folder_cognition as cognition, compact_semantics as semantics
from .folder_contracts import make_request, validate_relative_path
from .folder_session import _hash, _encode
from .life_objects import _json, _load
from .core import Compiler, VERSION

PROFILE = 'two-list-two-order-research-1'
ROLES = ('EARLIER_SOURCE', 'LATER_SOURCE')
ORDERS = (ROLES, tuple(reversed(ROLES)))
FEATURES = (('CHANGED', 'SAME'), ('SAME', 'CHANGED'))
TIMES = (0, 2, 4, 6, 8, 10)
CONTROLS = ('LEARNED', 'EARLIER_FIRST', 'LATER_FIRST', 'ALWAYS_ABSTAIN')
MAX_ROW_BYTES = 65536
MAX_ATTEMPT_BYTES = 262144
MAX_LEDGER_BYTES = 3 * 1024 * 1024
MAX_CONTRACT_BYTES = 32768
MAX_CANDIDATE_BYTES = 16384
TRAINING_TRIALS = 8


class OrderResearchError(ValueError):
    pass


def need(ok, reason):
    if not ok:
        raise OrderResearchError(reason)


def _same(a, b):
    return _encode(a) == _encode(b)


def _text(x):
    return type(x) is str and 1 <= len(x) <= 128


def _sha(x):
    return type(x) is str and len(x) == 64 and all(c in '0123456789abcdef' for c in x)


def make_contract(source, training_ids, evaluation_ids, *, paths=None):
    """DRAFT only. Coordinator supplies real pins/groups before execution.
    No field for a world law, future mask or a supplied success label exists."""
    need(type(training_ids) is list and len(training_ids) == 8, 'eight fixed training ids')
    need(type(evaluation_ids) is list and len(evaluation_ids) <= 16, 'bounded evaluation ids')
    _json(source, 1024)
    if paths is not None: _json(paths, 4096)
    paths = dict(zip(ROLES, ('a.txt', 'b.txt'))) if paths is None else copy.deepcopy(paths)
    trials = []
    for i, trial in enumerate(training_ids):
        feature, order = FEATURES[(i % 4) // 2], ORDERS[i % 2]
        trials.append(dict(trial_id=trial, partition='TRAIN', group=trial,
            schedule_sha256=None, feature=list(feature), order=list(order)))
    for trial in evaluation_ids:
        trials.append(dict(trial_id=trial, partition='EVALUATION', group=trial,
            schedule_sha256=None, feature=None, order=None))
    c = dict(schema='order-research-contract-1', profile=PROFILE, status='DRAFT', source=copy.deepcopy(source),
        paths=paths, trials=trials, environment_sha256=None, implementation_sha256=None,
        times=list(TIMES), controls=list(CONTROLS), max_effects=6, authority='NONE')
    validate_contract(c)
    return c


def validate_contract(c, *, pin=None, frozen=False):
    _json(c, MAX_CONTRACT_BYTES)
    need(type(c) is dict and set(c) == {'schema', 'profile', 'status', 'source', 'paths', 'trials',
        'environment_sha256', 'implementation_sha256', 'times', 'controls', 'max_effects', 'authority'}, 'contract fields')
    need(c['schema'] == 'order-research-contract-1' and c['profile'] == PROFILE
         and c['status'] in ('DRAFT', 'FROZEN') and c['authority'] == 'NONE', 'contract version')
    s = c['source']
    need(type(s) is dict and set(s) == {'identity', 'instance_id'} and _text(s['identity'])
         and type(s['instance_id']) is str and len(s['instance_id']) == 32
         and all(x in '0123456789abcdef' for x in s['instance_id']), 'source identity/instance')
    need(type(c['paths']) is dict and set(c['paths']) == set(ROLES), 'two path roles')
    for path in c['paths'].values(): validate_relative_path(path)
    need(len({p.casefold() for p in c['paths'].values()}) == 2
         and all(p.lower().endswith('.txt') for p in c['paths'].values()), 'distinct txt paths')
    need(_same(c['times'], list(TIMES)) and _same(c['controls'], list(CONTROLS))
         and type(c['max_effects']) is int and c['max_effects'] == 6, 'fixed clock/effects/controls')
    need(type(c['trials']) is list and 8 <= len(c['trials']) <= 24, 'bounded trial manifest')
    ids, groups = set(), set()
    for i, t in enumerate(c['trials']):
        need(type(t) is dict and set(t) == {'trial_id', 'partition', 'group', 'schedule_sha256', 'feature', 'order'}, 'trial fields')
        need(_text(t['trial_id']) and _text(t['group']) and t['trial_id'] not in ids
             and t['group'] not in groups, 'unique trial namespace/group')
        ids.add(t['trial_id']); groups.add(t['group'])
        if i < 8:
            need(t['partition'] == 'TRAIN' and _same(t['feature'], list(FEATURES[(i % 4) // 2]))
                 and _same(t['order'], list(ORDERS[i % 2])), 'fixed exploration, no replacement')
        else:
            need(t['partition'] == 'EVALUATION' and t['feature'] is None and t['order'] is None, 'no evaluation answer labels')
        need(t['schedule_sha256'] is None or _sha(t['schedule_sha256']), 'schedule pin')
    if pin is not None: need(_sha(pin) and pin == _hash(c), 'contract pin')
    if frozen:
        need(c['status'] == 'FROZEN' and pin is not None, 'CONTRACT_NOT_FROZEN')
        need(_sha(c['environment_sha256']) and _sha(c['implementation_sha256'])
             and all(_sha(t['schedule_sha256']) for t in c['trials']), 'unreviewed environment/source/schedules')
    return copy.deepcopy(c)


def _trial(c, trial_id):
    matches = [t for t in c['trials'] if t['trial_id'] == trial_id]
    need(len(matches) == 1, 'declared trial required')
    return matches[0]


def trial_namespace(c, trial_id):
    validate_contract(c)
    _trial(c, trial_id)
    tag = _hash([PROFILE, c['source'], trial_id, _hash(c)])
    return dict(identity='order_trial_' + tag[:32], scope_ref='scope_' + tag[32:64], trial_tag=tag)


def _listing(state, paths):
    sources = [s for s in state['sources'] if s['request']['operation'] == 'LIST_SCOPE']
    if not sources or sources[-1]['capture']['result']['status'] != 'OK': return None
    entries = sources[-1]['capture']['result']['payload']['entries']
    if len(entries) != 2: return None
    result = {}
    for role in ROLES:
        found = [e for e in entries if e['kind'] == 'file' and e['relative_path'] == paths[role]]
        if len(found) != 1: return None
        result[role] = found[0]['entry_ref']
    return result


def _command(c, trial_id, state, index, order):
    ns = trial_namespace(c, trial_id)
    if index in (0, 2):
        q = make_request('LIST_SCOPE', ns['scope_ref'], request_id='req_' + _hash([ns['trial_tag'], 'LIST', index])[:32])
        return dict(time=TIMES[index], request=q, decision=None)
    listing = _listing(state, c['paths'])
    if listing is None: return None
    role = ROLES[0] if index == 1 else ROLES[1] if index == 3 else order[index - 4]
    d = folder_cl.prepare_decision(state, listing[role])
    return dict(time=TIMES[index], request=d['request'], decision=d)


def _verify_rows(c, trial_id, rows, order):
    need(type(rows) is list and len(rows) <= 6, 'six rows maximum')
    _json(rows, MAX_ATTEMPT_BYTES)
    ns = trial_namespace(c, trial_id)
    state = cognition.initial_cognition(ns['identity'], ns['scope_ref'])
    stopped = False
    for i, row in enumerate(rows):
        _json(row, MAX_ROW_BYTES)
        need(type(row) is dict and set(row) == {'time', 'request', 'decision', 'item'}, 'row fields')
        need(not stopped and (i < 4 or order is not None), 'no effects after stop/abstain')
        cmd = _command(c, trial_id, state, i, order)
        need(cmd is not None and _same({k: row[k] for k in cmd}, cmd), 'exact chronological CL command')
        if row['decision'] is not None:
            need(_same(folder_cl.validate_decision(row['decision'], state), row['request']), 'CL/request binding')
        item = row['item']
        if item is None:
            stopped = True  # prepared request, no known result; never infer a dispatch
            continue
        need(type(item) is dict and item.get('sequence') == i + 1 and type(item.get('sequence')) is int
             and _same(item.get('request'), row['request']), 'original item/request/sequence')
        need(item['capture']['capture_origin'] == 'SYNTHETIC_TEST_CAPTURE', 'synthetic component only')
        state = cognition.apply_capture(state, item)
        if item['capture']['result']['status'] != 'OK': stopped = True
    return state, stopped


def prefix_command(c, trial_id, rows):
    """Generate the next t0/t2/t4/t6 command, never dispatch it."""
    validate_contract(c); _trial(c, trial_id)
    need(type(rows) is list and len(rows) < 4, 'prefix slot')
    state, stopped = _verify_rows(c, trial_id, rows, None)
    return None if stopped else _command(c, trial_id, state, len(rows), None)


def _observe(c, trial_id, prefix_rows):
    need(type(prefix_rows) is list and len(prefix_rows) == 4, 'complete four-row prefix')
    state, stopped = _verify_rows(c, trial_id, prefix_rows, None)
    if stopped: return dict(status='UNKNOWN', feature=None), state
    first = _listing(cognition.replay_captures(state['identity'], state['scope_ref'], state['sources'][:1]), c['paths'])
    current = _listing(state, c['paths'])
    if first is None or current is None: return dict(status='UNKNOWN', feature=None), state
    # Equality only. No decoding, sorting or hashing opaque refs as features.
    feature = ['SAME' if first[r] == current[r] else 'CHANGED' for r in ROLES]
    # Independently check that the two actual read claims form a real conflict.
    index = semantics.initial_semantics(c['source']['identity'], c['source']['instance_id'], state['scope_ref'])
    topics = []
    for i, role in ((1, ROLES[0]), (3, ROLES[1])):
        raw = prefix_rows[i]['item']
        item = {k: raw[k] for k in ('request', 'capture', 'receipt_sha256')}
        index, parsed = semantics.apply_capture(index, item, semantics.capture_reference(item,
            c['source']['identity'], c['source']['instance_id']), c['paths'][role], i + 1)
        if parsed.get('record') is None: return dict(status='UNKNOWN', feature=feature), state
        topics.append(parsed['record']['topic'])
    if len(set(topics)) != 1: return dict(status='UNKNOWN', feature=feature), state
    claim = index['records'][0]['claim']
    report = semantics.query(index, claim['subject'], claim['predicate'])
    if report['status'] != 'UNRESOLVED_CONFLICT': return dict(status='UNKNOWN', feature=feature), state
    return dict(status='VERIFIED', feature=feature, topic=topics[0], prefix_sha256=_hash(prefix_rows)), state


def observe_prefix(c, trial_id, prefix_rows):
    validate_contract(c); _trial(c, trial_id)
    return _observe(c, trial_id, prefix_rows)[0]


def action_command(c, trial_id, prefix_rows, action_rows, order):
    """Prepare one of the two existing typed OBSERVE orders at t8/t10."""
    validate_contract(c); _trial(c, trial_id)
    need(_same(order, list(ORDERS[0])) or _same(order, list(ORDERS[1])), 'two legal orders')
    need(type(action_rows) is list and len(action_rows) < 2, 'two action slots')
    observed, _ = _observe(c, trial_id, prefix_rows)
    if observed['status'] != 'VERIFIED': return None
    rows = prefix_rows + action_rows
    state, stopped = _verify_rows(c, trial_id, rows, order)
    return None if stopped else _command(c, trial_id, state, len(rows), order)


def make_row(command, item):
    """item is the actual folder-session capture plus contiguous sequence.
    None explicitly preserves a prepared request without a known receipt."""
    _json(command, MAX_ROW_BYTES); _json(item, MAX_ROW_BYTES)
    row = dict(time=command['time'], request=copy.deepcopy(command['request']),
               decision=copy.deepcopy(command['decision']), item=copy.deepcopy(item))
    _json(row, MAX_ROW_BYTES)
    return row


def make_attempt(c, trial_id, rows, order):
    validate_contract(c); _trial(c, trial_id)
    _json(rows, MAX_ATTEMPT_BYTES); _json(order, 512)
    a = dict(schema='order-research-attempt-1', source=copy.deepcopy(c['source']), contract_pin=_hash(c),
        trial_id=trial_id, order=copy.deepcopy(order), rows=copy.deepcopy(rows))
    _json(a, MAX_ATTEMPT_BYTES)
    return dict(data=a, sha256=_hash(a))


def verify_attempt(c, wrapped):
    validate_contract(c)
    _json(wrapped, MAX_ATTEMPT_BYTES)
    need(type(wrapped) is dict and set(wrapped) == {'data', 'sha256'} and _hash(wrapped['data']) == wrapped['sha256'], 'attempt integrity')
    a = wrapped['data']
    need(type(a) is dict and set(a) == {'schema', 'source', 'contract_pin', 'trial_id', 'order', 'rows'}
         and a['schema'] == 'order-research-attempt-1' and _same(a['source'], c['source'])
         and a['contract_pin'] == _hash(c), 'attempt source/contract')
    t = _trial(c, a['trial_id'])
    order = a['order']
    need(order is None or any(_same(order, list(o)) for o in ORDERS), 'attempt order')
    if t['partition'] == 'TRAIN': need(_same(order, t['order']), 'fixed exploration order')
    _verify_rows(c, a['trial_id'], a['rows'], order)
    result = dict(feature=None, outcome='UNKNOWN', success=None, actual_statuses=[], complete=False)
    if len(a['rows']) < 4 or a['rows'][3]['item'] is None: return result
    observed, _ = _observe(c, a['trial_id'], a['rows'][:4])
    result['feature'] = observed['feature']
    if observed['status'] != 'VERIFIED': return result
    suffix = a['rows'][4:]
    if order is None:
        need(not suffix, 'abstention has no skill actions')
        result['outcome'] = 'ABSTAIN'
        return result
    for row in suffix:
        if row['item'] is None: return result
        result['actual_statuses'].append(row['item']['capture']['result']['status'])
    statuses = result['actual_statuses']
    if statuses and statuses[-1] in ('DENIED', 'UNAVAILABLE'):
        result['outcome'] = 'REFUSAL'
    elif statuses and statuses[-1] in ('STALE_REF', 'NOT_FOUND'):
        result.update(outcome='SOURCE_FAILURE', success=False, complete=True)
    elif len(statuses) == 2 and statuses == ['OK', 'OK']:
        result.update(outcome='TWO_VALID_OBSERVATIONS', success=True, complete=True)
    return result


def new_ledger(c, contract_pin):
    validate_contract(c, pin=contract_pin, frozen=True)
    d = dict(schema='order-research-ledger-1', source=copy.deepcopy(c['source']), contract_pin=contract_pin,
        phase='OPEN', records=[], head=_hash(['order-ledger-genesis', c['source'], contract_pin]))
    return d


def verify_ledger(c, ledger):
    validate_contract(c)
    _json(ledger, MAX_LEDGER_BYTES)
    need(type(ledger) is dict and set(ledger) == {'schema', 'source', 'contract_pin', 'phase', 'records', 'head'}
         and ledger['schema'] == 'order-research-ledger-1' and _same(ledger['source'], c['source'])
         and ledger['contract_pin'] == _hash(c) and ledger['phase'] in ('OPEN', 'FROZEN'), 'ledger source/version')
    validate_contract(c, pin=ledger['contract_pin'], frozen=True)
    need(type(ledger['records']) is list and len(ledger['records']) <= 8, 'bounded training history')
    head = _hash(['order-ledger-genesis', c['source'], ledger['contract_pin']])
    reports = []
    for i, record in enumerate(ledger['records']):
        need(type(record) is dict and set(record) == {'previous', 'attempt', 'sha256'} and record['previous'] == head
             and record['sha256'] == _hash({k: record[k] for k in ('previous', 'attempt')}), 'chronological ledger chain')
        need(record['attempt']['data']['trial_id'] == c['trials'][i]['trial_id'], 'all fixed attempts, no dropped replacement')
        reports.append(verify_attempt(c, record['attempt']))
        head = record['sha256']
    if ledger['phase'] == 'FROZEN':
        need(len(reports) == 8, 'freeze only after eight retained attempts')
        head = _hash(['freeze-order-ledger', head])
    need(ledger['head'] == head, 'ledger head')
    return reports


def append_attempt(c, ledger, attempt):
    validate_contract(c, pin=ledger['contract_pin'], frozen=True)
    verify_ledger(c, ledger)
    need(ledger['phase'] == 'OPEN' and len(ledger['records']) < 8, 'frozen/full ledger cannot update')
    verify_attempt(c, attempt)
    i = len(ledger['records'])
    need(attempt['data']['trial_id'] == c['trials'][i]['trial_id'], 'fixed next trial, no retries/replacement')
    out = copy.deepcopy(ledger)
    record = dict(previous=out['head'], attempt=copy.deepcopy(attempt))
    record['sha256'] = _hash(record)
    out['records'].append(record); out['head'] = record['sha256']
    _json(out, MAX_LEDGER_BYTES)
    return out


def freeze_ledger(c, ledger):
    verify_ledger(c, ledger)
    need(ledger['phase'] == 'OPEN' and len(ledger['records']) == 8, 'eight attempts before freeze')
    out = copy.deepcopy(ledger); out['phase'] = 'FROZEN'; out['head'] = _hash(['freeze-order-ledger', out['head']])
    return out


def dump_ledger(c, ledger):
    verify_ledger(c, ledger)
    return _json(ledger, MAX_LEDGER_BYTES)


def load_ledger(c, raw, *, expected_head):
    need(type(raw) is bytes and len(raw) <= MAX_LEDGER_BYTES, 'ledger bytes before parse/hash')
    ledger = _load(raw)
    need(_json(ledger, MAX_LEDGER_BYTES) == raw and ledger['head'] == expected_head, 'canonical pinned ledger')
    verify_ledger(c, ledger)
    return ledger


def fit_candidate(c, ledger):
    validate_contract(c, pin=ledger['contract_pin'], frozen=True)
    reports = verify_ledger(c, ledger)
    need(ledger['phase'] == 'FROZEN', 'no fitting an open ledger')
    coverage = all(_same(report['feature'], c['trials'][i]['feature']) for i, report in enumerate(reports))
    table = []
    for feature in FEATURES:
        cells = []
        for order in ORDERS:
            relevant = [(i, r) for i, r in enumerate(reports) if _same(r['feature'], list(feature))
                and _same(ledger['records'][i]['attempt']['data']['order'], list(order))]
            valid = [(i, r) for i, r in relevant if r['success'] is not None]
            cells.append(dict(order=list(order), valid=len(valid), successes=sum(r['success'] is True for _, r in valid),
                attempt_hashes=[ledger['records'][i]['attempt']['sha256'] for i, _ in relevant]))
        chosen = None
        if coverage and all(x['valid'] == 2 for x in cells) and cells[0]['successes'] != cells[1]['successes']:
            chosen = cells[int(cells[1]['successes'] > cells[0]['successes'])]['order']
        table.append(dict(feature=list(feature), cells=cells, order=chosen))
    # Feature coverage does not prove outcome coverage. Unknown/refused
    # outcomes stay unlabeled; retain supported feature rows independently.
    status = ('UNKNOWN_PROTOCOL_COVERAGE' if not coverage else
        'FROZEN_RESEARCH_HYPOTHESIS' if all(cell['valid'] == 2 for row in table for cell in row['cells']) else
        'FROZEN_PARTIAL_EVIDENCE')
    candidate = dict(schema='order-research-candidate-1', profile=PROFILE, source=copy.deepcopy(c['source']),
        contract_pin=_hash(c), ledger_head=ledger['head'], table=table, cl_version=VERSION,
        status=status, reliability='UNKNOWN', authority='NONE')
    _json(candidate, MAX_CANDIDATE_BYTES)
    return candidate


def choose(c, ledger, candidate, trial_id, prefix_rows, *, control='LEARNED'):
    """Prefix-only policy input. No world, future seed/mask or outcome input."""
    need(control in CONTROLS, 'registered control')
    _json(candidate, MAX_CANDIDATE_BYTES)
    need(_same(candidate, fit_candidate(c, ledger)), 'recompute candidate from frozen own evidence')
    need(_trial(c, trial_id)['partition'] == 'EVALUATION', 'unseen declared evaluation trial')
    observed = observe_prefix(c, trial_id, prefix_rows)
    order = None
    if observed['status'] == 'VERIFIED':
        if control == 'EARLIER_FIRST': order = list(ORDERS[0])
        elif control == 'LATER_FIRST': order = list(ORDERS[1])
        elif control == 'LEARNED':
            match = [r for r in candidate['table'] if _same(r['feature'], observed['feature'])]
            if len(match) == 1: order = match[0]['order']
    return dict(order=copy.deepcopy(order), prefix_sha256=_hash(prefix_rows), candidate_sha256=_hash(candidate),
        ledger_head=ledger['head'], trial_id=trial_id, control=control,
        reason='SUPPORTED_EMPIRICAL_ORDER' if order else 'ABSTAIN_UNKNOWN_OR_CONTROL', authority='NONE')
