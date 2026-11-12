"""Pure journal-replayable width prediction, never a reader or language model.

The caller must durably save PREPARE before dispatch. This module binds evidence
but cannot authenticate filesystem identity, source collection or persistence.
"""
import copy
import math
from .folder_cognition import validate_cognition, _hash, _raw, _bindings
from .folder_contracts import validate_request, validate_result
from .folder_learning import run_protocol, validate_model, evaluate, fit_model

SCHEMA = 'predictive-observer-1'
MAX_EVENTS = 64
MAX_BYTES = 8 * 1024 * 1024
MAX_SCORED_BYTES = 256 * 1024
LOGICAL_COST = 2


def need(ok, reason):
    if not ok: raise ValueError('PREDICTIVE_OBSERVER_INVALID: ' + reason)


def initial_observer(identity, scope_ref):
    # Reuse the cognition schema's identity/scope validation, without I/O.
    from .folder_cognition import initial_cognition
    initial_cognition(identity, scope_ref)
    return dict(schema=SCHEMA, identity=identity, scope_ref=scope_ref, events=[],
                active_model=None, previous_models=[], model_decisions=[],
                pending=None, observations=[], scored_bytes=0, source_prefix=[], studies=[])


def _source(state, cognition):
    c = validate_cognition(cognition)
    need(c['identity'] == state['identity'] and c['scope_ref'] == state['scope_ref'], 'source identity/scope')
    sources = c['sources']; prefix = [_hash(s) for s in sources]
    need(prefix[:len(state['source_prefix'])] == state['source_prefix'], 'source prefix regression/change')
    for observation in state['observations']:
        pos = observation['commitment']['source']['version']
        need(pos < len(sources) and _item(sources[pos]) == observation['item'], 'completed observation absent from admitted prefix')
    state['source_prefix'] = prefix
    return {'identity': c['identity'], 'scope_ref': c['scope_ref'], 'version': c['version'],
            'cognition_sha256': _hash(c), 'event_head': c['events'][-1]['hash'] if c['events'] else '0'*64}


def _item(source):
    return {k: copy.deepcopy(source[k]) for k in ('request', 'capture', 'receipt_sha256')}


def _known(captures, cognition):
    known = {_hash(_item(s)) for s in cognition['sources']}
    need(all(_hash(c) in known for c in captures), 'model/evaluation evidence not in already admitted source prefix')


def _validate_capture(item):
    need(type(item) is dict and set(item) == {'request', 'capture', 'receipt_sha256'}, 'capture fields')
    validate_request(item['request']); c = item['capture']
    need(type(c) is dict and set(c) == {'schema', 'capture_origin', 'result'}, 'capture envelope')
    need(c['schema'] == 'folder-session-1' and c['capture_origin'] in
         ('SYNTHETIC_TEST_CAPTURE', 'APPLICATION_BROKER_CAPTURE'), 'capture origin')
    need(_hash(c) == item['receipt_sha256'], 'capture digest')
    validate_result(c['result'], item['request'])


def _probabilities(model):
    if model is None: return [[.25] * 4 for _ in range(4)]
    validate_model(model)
    return [[(n+1)/(sum(row)+4) for n in row] for row in model['counts']]


def _score(commitment, item, remaining):
    result = item['capture']['result']
    base = dict(status='UNKNOWN', reason=None, transitions=0, scored_bytes=0,
                mean_log_loss=None, mean_entropy=None, bounded_surprise=None,
                receipt_sha256=item['receipt_sha256'], content_sha256=None,
                capture_origin=item['capture']['capture_origin'], reward=False,
                fact_adopted=False, maturity_increment=0)
    if result['status'] != 'OK': base['reason'] = 'NO_SUCCESSFUL_TEXT'; return base
    payload = result['payload']; base['content_sha256'] = payload['content_sha256']
    if commitment['mode'] == 'UNKNOWN_BUDGET' or payload['utf8_bytes'] > remaining:
        base['reason'] = 'NO_SCORING_BUDGET'; return base
    text = payload['text']
    if len(text) < 2: base['reason'] = 'INSUFFICIENT_TRANSITIONS'; return base
    probabilities = commitment['probabilities']; loss = entropy = 0.
    previous = len(text[0].encode('utf8'))
    # Each prediction uses only the previous observed scalar and the PREPARE
    # distribution. Targets are revealed in order; not fitted or looked ahead.
    for character in text[1:]:
        distribution = probabilities[previous-1]
        entropy -= sum(p*math.log(p) for p in distribution)
        target = len(character.encode('utf8'))
        loss -= math.log(distribution[target-1]); previous = target
    n = len(text)-1
    base.update(status='SCORED', reason=None, transitions=n, scored_bytes=payload['utf8_bytes'],
                mean_log_loss=loss/n, mean_entropy=entropy/n,
                bounded_surprise=min(1., loss/n/12.))
    return base


def _apply(state, kind, payload):
    if kind == 'DECLARE_STUDY':
        need(set(payload) == {'plan', 'cognition'}, 'study fields')
        need(state['pending'] is None and len(state['studies']) < 2 and
             not any(s['status'] == 'DECLARED' for s in state['studies']), 'study capacity/pending work')
        source = _source(state, payload['cognition']); plan = payload['plan']
        roles = ('heldout_mechanism', 'heldout_composition', 'retention')
        need(type(plan) is dict and set(plan) == {'old_train', 'new_train', *roles}, 'partition plan fields')
        c = payload['cognition']; known = {s['receipt_sha256']: _item(s) for s in c['sources']}
        training = {}
        for role in ('old_train', 'new_train'):
            refs = plan[role]
            need(type(refs) is list and 1 <= len(refs) <= 8 and all(type(r) is str and r in known for r in refs), 'known training refs')
            training[role] = [known[r] for r in refs]
        incumbent = fit_model(training['old_train'])
        candidate = fit_model(training['old_train']+training['new_train'], parent=incumbent)
        need(state['active_model'] is None or state['active_model'] == incumbent, 'frozen incumbent not active')
        listing = None
        for s in c['sources']:
            if s['request']['operation'] == 'LIST_SCOPE' and s['request']['entry_ref'] is None:
                listing = s['capture']['result']
        need(listing is not None and listing['status'] == 'OK', 'successful root LIST required')
        visible = {e['relative_path'] for e in listing['payload']['entries'] if e['kind'] == 'file'}
        read_paths = set()
        for i, s in enumerate(c['sources']):
            if s['request']['operation'] == 'READ_TEXT':
                path = _bindings(c['sources'][:i]).get(s['request']['entry_ref'])
                need(path is not None, 'prior read lacks lexical path binding')
                read_paths.add(path.casefold())
        assigned = set()
        for role in roles:
            paths = plan[role]
            need(type(paths) is list and 1 <= len(paths) <= 8, 'heldout role count')
            for path in paths:
                need(type(path) is str and path in visible and path.casefold() not in read_paths and path.casefold() not in assigned,
                     'heldout target must be disjoint LIST-bound unread lexical path')
                assigned.add(path.casefold())
        study = dict(plan=copy.deepcopy(plan), source=source, training=training,
                     incumbent_sha256=_hash(incumbent), candidate_sha256=_hash(candidate),
                     status='DECLARED', report_sha256=None)
        study['study_id'] = _hash(study); state['studies'].append(study)
    elif kind == 'PREPARE':
        need(set(payload) == {'request', 'cognition', 'logical_budget'}, 'prepare fields')
        need(state['pending'] is None, 'one pending prediction only')
        request = payload['request']; validate_request(request)
        need(request['operation'] == 'READ_TEXT' and request['scope_ref'] == state['scope_ref'], 'selected read/scope')
        need(not any(o['item']['request']['request_id'] == request['request_id'] for o in state['observations']), 'request already observed')
        source = _source(state, payload['cognition'])
        need(not any(s['request']['request_id'] == request['request_id'] for s in payload['cognition']['sources']), 'target already admitted before prediction')
        budget = payload['logical_budget']; need(type(budget) is int and 0 <= budget <= 4, 'logical budget 0..4')
        model = state['active_model']
        if model is not None: _known(model['training'], payload['cognition'])
        mode = ('UNKNOWN_BUDGET' if budget < LOGICAL_COST or state['scored_bytes'] >= MAX_SCORED_BYTES
                else 'UNIFORM_BASELINE' if model is None else 'FROZEN_EMPIRICAL_WIDTH')
        commit = dict(schema='width-prediction-commitment-1', request=copy.deepcopy(request),
                      request_sha256=_hash(request), source=source, model_sha256=None if model is None else _hash(model),
                      probabilities=None if mode == 'UNKNOWN_BUDGET' else _probabilities(model),
                      mode=mode, logical_cost=0 if mode == 'UNKNOWN_BUDGET' else LOGICAL_COST,
                      independence='No stable-file independence or unseen-content claim')
        commit['sha256'] = _hash(commit); state['pending'] = commit
    elif kind == 'OBSERVE':
        need(set(payload) == {'item', 'cognition_before'}, 'observe fields')
        commit = state['pending']; need(commit is not None, 'no committed prediction')
        c = validate_cognition(payload['cognition_before'])
        need(_hash(c) == commit['source']['cognition_sha256'], 'exact pre-read source required')
        item = payload['item']; _validate_capture(item)
        need(item['request'] == commit['request'], 'receipt differs from committed request')
        score = _score(commit, item, MAX_SCORED_BYTES-state['scored_bytes'])
        state['observations'].append(dict(commitment=commit, item=copy.deepcopy(item), score=score))
        state['scored_bytes'] += score['scored_bytes']; state['pending'] = None
    elif kind == 'PROMOTE':
        need(set(payload) == {'report', 'cognition'}, 'promotion fields')
        need(state['pending'] is None, 'cannot replace predictor during pending read')
        need(len(state['model_decisions']) < 4, 'model decision budget')
        source = _source(state, payload['cognition']); report = payload['report']
        need(type(report) is dict and 'partitions' in report, 'full frozen evaluation required')
        pending_studies = [s for s in state['studies'] if s['status'] == 'DECLARED']
        need(len(pending_studies) == 1, 'prospective committed study required; no retrospective adoption')
        study = pending_studies[0]; partitions = copy.deepcopy(study['training'])
        sources = payload['cognition']['sources']; found = {}
        for i, captured in enumerate(sources):
            if i < study['source']['version'] or captured['request']['operation'] != 'READ_TEXT': continue
            path = _bindings(sources[:i]).get(captured['request']['entry_ref'])
            if path is not None and path.casefold() not in found: found[path.casefold()] = _item(captured)
        for role in ('heldout_mechanism', 'heldout_composition', 'retention'):
            need(all(p.casefold() in found for p in study['plan'][role]), 'study targets incomplete')
            partitions[role] = [found[p.casefold()] for p in study['plan'][role]]
        need(_raw(partitions) == _raw(report['partitions']), 'partitions differ from predeclared first captures')
        expected = run_protocol(report['partitions'])
        need(_raw(report) == _raw(expected), 'altered evaluation/model/decision')
        need(_hash(report['incumbent']) == study['incumbent_sha256'] and
             _hash(report['candidate']) == study['candidate_sha256'], 'training/model changed after declaration')
        for captures in report['partitions'].values(): _known(captures, payload['cognition'])
        success = report['decision'] == 'PROMOTE_NARROW_PREDICTOR'
        if success:
            need(state['active_model'] is None or state['active_model'] == report['incumbent'], 'evaluation incumbent is not active predictor')
            state['previous_models'].append(copy.deepcopy(state['active_model']))
            state['active_model'] = copy.deepcopy(report['candidate'])
        state['model_decisions'].append(dict(kind='PROMOTE' if success else 'REJECT', source=source,
            report_sha256=_hash(report), model_sha256=None if state['active_model'] is None else _hash(state['active_model'])))
        study['status'] = 'PROMOTED' if success else 'REJECTED'; study['report_sha256'] = _hash(report)
    elif kind == 'ROLLBACK':
        need(set(payload) == {'captures', 'cognition'}, 'rollback fields')
        need(state['pending'] is None and state['active_model'] is not None and state['previous_models'], 'rollback model/history missing')
        need(len(state['model_decisions']) < 4, 'model decision budget')
        source = _source(state, payload['cognition']); captures = payload['captures']; _known(captures, payload['cognition'])
        # Evaluation is recomputed, never selected by a caller's passed Boolean.
        active = evaluate(state['active_model'], captures)
        previous = state['previous_models'][-1]
        baseline = math.log(4) if previous is None else evaluate(previous, captures)['mean_log_loss']
        training = state['active_model']['training']
        keys = {(x['request']['scope_ref'], x['request']['entry_ref']) for x in training}
        hashes = {x['capture']['result']['payload']['content_sha256'] for x in training}
        need(not any((x['request']['scope_ref'], x['request']['entry_ref']) in keys or
                     x['capture']['result']['payload']['content_sha256'] in hashes for x in captures),
             'retention source/content overlaps training')
        rollback = active['mean_log_loss']-baseline > .05
        if rollback: state['active_model'] = state['previous_models'].pop()
        state['model_decisions'].append(dict(kind='ROLLBACK' if rollback else 'KEEP', source=source,
            evaluated_model_sha256=active['model_sha256'], evidence_sha256=_hash(captures),
            active_loss=active['mean_log_loss'], previous_loss=baseline,
            selected_model_sha256=None if state['active_model'] is None else _hash(state['active_model'])))
    else: need(False, 'unregistered operation')


def validate_observer(state):
    need(type(state) is dict and 'events' in state and 'identity' in state and 'scope_ref' in state, 'state')
    need(type(state['events']) is list and len(state['events']) <= MAX_EVENTS and len(_raw(state)) <= MAX_BYTES, 'state budget')
    rebuilt = initial_observer(state['identity'], state['scope_ref']); previous = '0'*64
    for event in state['events']:
        need(type(event) is dict and set(event) == {'index','kind','payload','previous','sha256'}, 'event fields')
        need(event['index'] == len(rebuilt['events']) and event['previous'] == previous and
             event['sha256'] == _hash({k:v for k,v in event.items() if k != 'sha256'}), 'event chain')
        _apply(rebuilt, event['kind'], event['payload']); rebuilt['events'].append(copy.deepcopy(event)); previous = event['sha256']
    need(_raw(rebuilt) == _raw(state), 'derived observer differs from replay')
    return rebuilt


def _append(state, kind, payload):
    checked = validate_observer(state); need(len(checked['events']) < MAX_EVENTS, 'event budget')
    event = dict(index=len(checked['events']), kind=kind, payload=copy.deepcopy(payload),
                 previous=checked['events'][-1]['sha256'] if checked['events'] else '0'*64)
    event['sha256'] = _hash(event); _apply(checked, kind, event['payload']); checked['events'].append(event)
    need(len(_raw(checked)) <= MAX_BYTES, 'state byte budget'); return checked


def prepare_prediction(state, request, cognition, *, logical_budget=2):
    return _append(state, 'PREPARE', dict(request=request, cognition=cognition, logical_budget=logical_budget))


def declare_study(state, plan, cognition):
    return _append(state, 'DECLARE_STUDY', dict(plan=plan, cognition=cognition))


def observe_capture(state, item, cognition_before):
    checked = validate_observer(state)
    for observation in checked['observations']:
        if observation['item']['request']['request_id'] == item.get('request', {}).get('request_id'):
            need(observation['item'] == item and observation['commitment']['source']['cognition_sha256'] == _hash(cognition_before), 'conflicting duplicate capture/source')
            return checked
    return _append(checked, 'OBSERVE', dict(item=item, cognition_before=cognition_before))


def promote_model(state, report, cognition):
    return _append(state, 'PROMOTE', dict(report=report, cognition=cognition))


def rollback_model(state, retention_captures, cognition):
    return _append(state, 'ROLLBACK', dict(captures=retention_captures, cognition=cognition))
