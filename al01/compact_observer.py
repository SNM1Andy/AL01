"""Incremental measured-width observer, without retained full-history arrays.

Active prediction starts uniform and changes only through an external verified
prospective lifecycle. Online diagnostic counts never activate themselves.
Scores always use the frozen pre-read parameters, before diagnostic updates.
"""
import copy
import math
from .folder_session import _hash, _encode
from .folder_contracts import validate_request, validate_result, validate_ref

SCHEMA = 'compact-width-observer-2'
MAX_COUNTER = 2**53-1


def need(ok, reason):
    if not ok: raise ValueError('COMPACT_OBSERVER_INVALID: '+reason)


def initial_observer():
    return dict(schema=SCHEMA, active_kind='UNIFORM_BASELINE', active_model_version=0,
        candidate_counts=[[0]*4 for _ in range(4)], training_transitions=0,
        scored_reads=0, attempted_reads=0, latest=None, promotion='EXPLICIT_PROSPECTIVE_GATE',
        active_counts=[[0]*4 for _ in range(4)], active_digest=_active_digest(), last_scored_model=None)


def validate_observer(state):
    need(type(state) is dict and set(state) == set(initial_observer()), 'state fields')
    for key in ('schema','promotion'):
        need(type(state[key]) is type(initial_observer()[key]) and state[key] == initial_observer()[key], 'unsupported model/authority')
    need(type(state['active_model_version']) is int and 0<=state['active_model_version']<=MAX_COUNTER,'model version')
    need(state['active_kind'] in ('UNIFORM_BASELINE','FROZEN_EMPIRICAL_WIDTH'),'model kind')
    counts=state['active_counts']
    need(type(counts) is list and len(counts)==4 and all(type(r) is list and len(r)==4 for r in counts),'active counts')
    need(all(type(n) is int and 0<=n<=8*65535 for r in counts for n in r),'active count bounds')
    need(type(state['active_digest']) is str and len(state['active_digest'])==64 and all(c in '0123456789abcdef' for c in state['active_digest']),'active digest')
    if state['active_kind']=='UNIFORM_BASELINE':need(state['active_model_version']==0 and counts==[[0]*4 for _ in range(4)] and state['active_digest']==_active_digest(),'uniform model')
    else:need(state['active_model_version']>0 and sum(map(sum,counts))>0,'empirical model')
    rows = state['candidate_counts']
    need(type(rows) is list and len(rows) == 4 and all(type(r) is list and len(r) == 4 for r in rows), 'count shape')
    need(all(type(n) is int and 0 <= n <= MAX_COUNTER for row in rows for n in row), 'bounded counts')
    for key in ('training_transitions','scored_reads','attempted_reads'):
        need(type(state[key]) is int and 0 <= state[key] <= MAX_COUNTER, 'counter')
    need(sum(sum(r) for r in rows) == state['training_transitions'], 'count total')
    need(state['scored_reads'] <= state['attempted_reads'], 'read counters')
    need(state['training_transitions'] >= state['scored_reads'], 'training/read counters')
    latest = state['latest']
    if state['attempted_reads'] == 0:
        need(latest is None and state['last_scored_model'] is None and state['training_transitions'] == 0 and state['scored_reads'] == 0, 'empty observer')
    else:
        need(type(latest) is dict and set(latest) == {'status','reason','transitions','mean_log_loss',
             'mean_entropy','receipt_sha256','reward','model_sha256','capture_origin'}, 'latest score fields')
        need(type(latest['receipt_sha256']) is str and len(latest['receipt_sha256']) == 64 and
             all(c in '0123456789abcdef' for c in latest['receipt_sha256']), 'receipt digest')
        last=state['last_scored_model']
        need(type(last) is dict and set(last)=={'kind','sha256'} and last['kind'] in ('UNIFORM_BASELINE','FROZEN_EMPIRICAL_WIDTH'),'last scored model')
        need(type(last['sha256']) is str and len(last['sha256'])==64 and
             all(c in '0123456789abcdef' for c in last['sha256']), 'last scored model digest')
        if last['kind']=='UNIFORM_BASELINE':
            need(last['sha256']==_active_digest(), 'exact scored uniform baseline digest')
        need(latest['model_sha256'] == last['sha256'] and latest['reward'] is False and
             latest['capture_origin'] in ('SYNTHETIC_TEST_CAPTURE','APPLICATION_BROKER_CAPTURE'), 'score identity/origin')
        need(type(latest['transitions']) is int and 0 <= latest['transitions'] <= 65535, 'transition count')
        if latest['status'] == 'SCORED':
            need(latest['reason'] is None and 1 <= latest['transitions'] <= state['training_transitions']
                 and state['scored_reads'] > 0, 'scored counters')
            for key in ('mean_log_loss','mean_entropy'):
                value = latest[key]
                need(type(value) is float and math.isfinite(value) and 0<=value<=math.log(8*65535+4), 'finite model score')
                if last['kind']=='UNIFORM_BASELINE':need(abs(value-math.log(4))<1e-9,'uniform score')
        else:
            need(latest['status'] == 'UNKNOWN' and latest['reason'] in ('NOT_SUCCESSFUL_READ','INSUFFICIENT_TRANSITIONS')
                 and latest['transitions'] == 0 and latest['mean_log_loss'] is None and latest['mean_entropy'] is None
                 and state['attempted_reads'] > state['scored_reads'], 'unknown score/counters')
    need(len(_encode(state)) <= 4096, 'observer size')
    return copy.deepcopy(state)


def model_identity(state):
    validate_observer(state)
    return state['active_digest']


def _active_digest():
    from .compact_learning import uniform_model
    return _hash(uniform_model())

def activate(state, model):
    from .compact_learning import validate_model
    state=validate_observer(state);model=validate_model(model)
    state.update(active_kind=model['kind'],active_model_version=model['version'],active_counts=copy.deepcopy(model['counts']),active_digest=_hash(model))
    return validate_observer(state)


def prepare_prediction(state, request, source):
    validate_request(request); validate_observer(state)
    need(type(source) is dict and {'identity','instance_id','scope_ref','global_seq','journal_head','cognition_revision'} <= set(source), 'source binding')
    need(type(source['identity']) is str and 1 <= len(source['identity']) <= 128, 'source identity')
    for key, size in (('instance_id',32),('journal_head',64)):
        need(type(source[key]) is str and len(source[key]) == size and all(c in '0123456789abcdef' for c in source[key]), 'source digest/instance')
    validate_ref(source['scope_ref'],'scope')
    need(source['scope_ref'] == request['scope_ref'], 'source/request scope')
    for key in ('global_seq','cognition_revision'):
        need(type(source[key]) is int and 0 <= source[key] <= MAX_COUNTER, 'source counter')
    need(len(_encode(source)) <= 16384, 'source bytes')
    value = dict(schema='compact-prediction-1', request_sha256=_hash(request), source_sha256=_hash(source),
                 observer_state_sha256=_hash(state), model_sha256=model_identity(state), kind=state['active_kind'],
                 probabilities=[[(n+1)/(sum(row)+4) for n in row] for row in state['active_counts']])
    value['sha256'] = _hash(value)
    return value


def validate_capture(item):
    need(type(item) is dict and set(item) == {'request','capture','receipt_sha256'}, 'capture fields')
    c = item['capture']; need(type(c) is dict and set(c) == {'schema','capture_origin','result'}, 'wrapper')
    need(c['schema'] == 'folder-session-1' and c['capture_origin'] in
         ('SYNTHETIC_TEST_CAPTURE','APPLICATION_BROKER_CAPTURE'), 'origin')
    need(_hash(c) == item['receipt_sha256'], 'capture digest')
    validate_result(c['result'], item['request'])


def observe(state, commitment, item, source):
    state = validate_observer(state); validate_capture(item)
    need(commitment == prepare_prediction(state, item['request'], source), 'altered pre-read model/request/source')
    result = item['capture']['result']
    score = dict(status='UNKNOWN', reason='NOT_SUCCESSFUL_READ', transitions=0, mean_log_loss=None,
                 mean_entropy=None, receipt_sha256=item['receipt_sha256'], reward=False,
                 model_sha256=commitment['model_sha256'], capture_origin=item['capture']['capture_origin'])
    if item['request']['operation'] != 'READ_TEXT': return state, score
    need(state['attempted_reads'] < MAX_COUNTER, 'attempt counter exhausted')
    state['attempted_reads'] += 1
    if result['status'] == 'OK':
        text = result['payload']['text']
        if len(text) < 2: score['reason'] = 'INSUFFICIENT_TRANSITIONS'
        else:
            # Diagnostic counts never affect the committed active predictor.
            loss = 0.; entropy=0.; transitions = []
            previous = len(text[0].encode('utf8'))-1
            for character in text[1:]:
                target = len(character.encode('utf8'))-1
                probs=commitment['probabilities'][previous]
                loss -= math.log(probs[target]);entropy-=sum(p*math.log(p) for p in probs)
                transitions.append((previous,target)); previous = target
            need(state['training_transitions']+len(transitions) <= MAX_COUNTER, 'training counter exhausted')
            for previous,target in transitions: state['candidate_counts'][previous][target] += 1
            state['training_transitions'] += len(transitions); state['scored_reads'] += 1
            score.update(status='SCORED',reason=None,transitions=len(transitions),mean_log_loss=loss/len(transitions),mean_entropy=entropy/len(transitions))
    state['latest'] = score
    state['last_scored_model']=dict(kind=commitment['kind'],sha256=commitment['model_sha256'])
    return validate_observer(state), score
