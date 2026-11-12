"""Narrow measured-byte prediction experiment, not semantic learning/maturity.

No I/O, reward, authority, teacher, executable text, or live-policy mutation.
Source labels and split descriptions are controller assertions, not proof of
independent data collection. Held-out targets never update the fitted model.
"""
import copy
import math
from types import MappingProxyType
from .folder_contracts import validate_request, validate_result
from .folder_cognition import _hash, _raw

SCHEMA = 'folder-width-predictor-1'
PROTOCOL = MappingProxyType({'schema': 'folder-width-protocol-1', 'smoothing': 1,
            'target': 'next observed Unicode scalar UTF8 width, classes 1..4',
            'context': 'previous scalar UTF8 width; reset at each receipt',
            'metric': 'mean negative natural log probability per transition',
            'baseline': 'uniform probability 0.25, no fitting',
            'minimum_transitions_per_split': 32,
            'minimum_new_gain_over_incumbent': .005,
            'minimum_new_gain_over_uniform': .02,
            'minimum_new_gain_over_learned_unigram': .005,
            'maximum_old_loss_increase': .05})
ROLES = ('old_train', 'new_train', 'heldout_mechanism', 'heldout_composition', 'retention')


def need(ok, reason):
    if not ok:
        raise ValueError('FOLDER_LEARNING_INVALID: ' + reason)


def _capture(item):
    need(type(item) is dict and set(item) == {'request', 'capture', 'receipt_sha256'}, 'capture fields')
    request, capture = item['request'], item['capture']
    validate_request(request)
    need(type(capture) is dict and set(capture) == {'schema', 'capture_origin', 'result'}, 'wrapper fields')
    need(capture['schema'] == 'folder-session-1' and capture['capture_origin'] in
         ('SYNTHETIC_TEST_CAPTURE', 'APPLICATION_BROKER_CAPTURE'), 'capture provenance')
    need(item['receipt_sha256'] == _hash(capture), 'capture digest')
    result = capture['result']; validate_result(result, request)
    need(request['operation'] == 'READ_TEXT' and result['status'] == 'OK', 'successful captured text required')
    text = result['payload']['text']
    widths = [len(c.encode('utf8')) for c in text]
    need(len(widths) >= 2, 'at least two measured characters')
    return widths, result['payload']['content_sha256']


def _batch(captures, maximum=16):
    need(type(captures) is list and 1 <= len(captures) <= maximum, 'capture count')
    need(len(_raw(captures)) <= 2 * 1024 * 1024, 'capture byte budget')
    seen = set(); rows = []
    for capture in captures:
        widths, content = _capture(capture)
        request = capture['request']; identity = (request['scope_ref'], request['entry_ref'])
        need(identity not in seen and content not in seen and request['request_id'] not in seen,
             'duplicate source or content')
        seen.update((identity, content, request['request_id']))
        rows.append((widths, content))
    return rows


def fit_model(captures, *, parent=None):
    """Fit counts only from supplied experience; optional parent is lineage only.

    To extend a model pass its training captures plus new captures explicitly.
    No held-out result, success label or caller weight can modify the counts.
    """
    rows = _batch(captures)
    counts = [[0] * 4 for _ in range(4)]
    for widths, _ in rows:
        for previous, target in zip(widths, widths[1:]):
            counts[previous-1][target-1] += 1
    if parent is not None:
        validate_model(parent)
        need(parent['version'] < 16, 'model version budget')
        prefix = parent['training']
        need(captures[:len(prefix)] == prefix and len(captures) > len(prefix), 'extension must preserve prior experience')
    data = {'schema': SCHEMA, 'version': 1 if parent is None else parent['version']+1,
            'parent_sha256': None if parent is None else _hash(parent),
            'counts': counts, 'training': copy.deepcopy(captures),
            'protocol_sha256': _hash(dict(PROTOCOL)), 'authority': 'NONE'}
    return data


def validate_model(model):
    need(type(model) is dict and set(model) == {'schema', 'version', 'parent_sha256', 'counts',
         'training', 'protocol_sha256', 'authority'}, 'model fields')
    need(type(model['version']) is int and 1 <= model['version'] <= 16, 'model version')
    parent = model['parent_sha256']
    need((model['version'] == 1 and parent is None) or (model['version'] > 1 and type(parent) is str
         and len(parent) == 64 and all(c in '0123456789abcdef' for c in parent)), 'lineage')
    rebuilt = fit_model(model['training'])
    need(model['schema'] == SCHEMA and model['protocol_sha256'] == _hash(dict(PROTOCOL))
         and model['authority'] == 'NONE' and _raw(model['counts']) == _raw(rebuilt['counts']), 'model evidence differs')
    return copy.deepcopy(model)


def predict(model, previous_width):
    """Reusable conditional distribution; only already observed context is input."""
    validate_model(model)
    need(type(previous_width) is int and 1 <= previous_width <= 4, 'observed width class')
    row = model['counts'][previous_width-1]
    return [(count+1)/(sum(row)+4) for count in row]


def evaluate(model, captures, *, unigram=False):
    need(type(unigram) is bool, 'baseline flag')
    validate_model(model); rows = _batch(captures, 8)
    losses = []; count = 0
    for widths, content in rows:
        total = 0.
        for previous, target in zip(widths, widths[1:]):
            row = ([sum(r[i] for r in model['counts']) for i in range(4)]
                   if unigram else model['counts'][previous-1])
            total -= math.log((row[target-1]+1)/(sum(row)+4))
        n = len(widths)-1; count += n
        losses.append({'content_sha256': content, 'transitions': n, 'total_log_loss': total})
    need(count >= PROTOCOL['minimum_transitions_per_split'], 'insufficient measured transitions')
    return {'predictor': 'learned_unigram' if unigram else 'conditional_width',
            'transitions': count, 'mean_log_loss': sum(r['total_log_loss'] for r in losses)/count,
            'uniform_log_loss': math.log(4), 'per_source': losses,
            'model_sha256': _hash(model), 'captures_sha256': _hash(captures)}


def run_protocol(partitions):
    """Frozen paired experiment, preserving all partitions/metrics and failures.

    Caller must independently document generator/mechanism split. Different
    IDs/hashes alone cannot establish that stronger scientific claim.
    """
    need(type(partitions) is dict and set(partitions) == set(ROLES), 'five frozen partitions required')
    all_items = []
    for role in ROLES:
        _batch(partitions[role], 8); all_items.extend(partitions[role])
    _batch(all_items, 40)  # No shared entry, request, or content across any split.
    frozen = copy.deepcopy(partitions); dataset_hash = _hash(frozen)
    incumbent = fit_model(frozen['old_train'])
    candidate = fit_model(frozen['old_train'] + frozen['new_train'], parent=incumbent)
    metrics = {}
    for role in ROLES[2:]:
        metrics[role] = {'incumbent': evaluate(incumbent, frozen[role]),
                         'candidate': evaluate(candidate, frozen[role]),
                         'learned_unigram': evaluate(candidate, frozen[role], unigram=True)}
    reasons = []
    for role in ROLES[2:4]:
        old = metrics[role]['incumbent']['mean_log_loss']; new = metrics[role]['candidate']['mean_log_loss']
        if old-new < PROTOCOL['minimum_new_gain_over_incumbent']:
            reasons.append(role + ': no required gain over frozen incumbent')
        if math.log(4)-new < PROTOCOL['minimum_new_gain_over_uniform']:
            reasons.append(role + ': no required gain over uniform control')
        if metrics[role]['learned_unigram']['mean_log_loss']-new < PROTOCOL['minimum_new_gain_over_learned_unigram']:
            reasons.append(role + ': no required gain over learned unigram')
    retention = metrics['retention']
    if retention['candidate']['mean_log_loss']-retention['incumbent']['mean_log_loss'] > PROTOCOL['maximum_old_loss_increase']:
        reasons.append('retention: old capability loss exceeds frozen tolerance')
    promoted = not reasons
    return {'schema': 'folder-learning-evaluation-1', 'protocol': dict(PROTOCOL),
            'dataset_sha256': dataset_hash, 'partitions': frozen, 'metrics': metrics,
            'incumbent': incumbent, 'candidate': candidate,
            'decision': 'PROMOTE_NARROW_PREDICTOR' if promoted else 'REJECT_KEEP_INCUMBENT',
            'failure_reasons': reasons, 'active_model': copy.deepcopy(candidate if promoted else incumbent),
            'teacher_enabled': False, 'runtime_adoption': False,
            'scope': 'measured UTF8-width prediction proxy; no semantic/general learning or maturity claim'}


def rollback_if_degraded(active, fallback, retention_captures):
    """Evaluate later retention data and return a selection, never mutate runtime."""
    validate_model(active); validate_model(fallback)
    need(active['parent_sha256'] == _hash(fallback), 'rollback requires exact prior model')
    need(active['version'] == fallback['version']+1 and
         active['training'][:len(fallback['training'])] == fallback['training'] and
         len(active['training']) > len(fallback['training']), 'rollback lineage/training binding')
    _batch(active['training'] + retention_captures, 24)
    current = evaluate(active, retention_captures); previous = evaluate(fallback, retention_captures)
    rollback = current['mean_log_loss']-previous['mean_log_loss'] > PROTOCOL['maximum_old_loss_increase']
    return {'decision': 'ROLLBACK' if rollback else 'KEEP', 'active_metrics': current,
            'fallback_metrics': previous, 'retention_captures': copy.deepcopy(retention_captures),
            'selected_model': copy.deepcopy(fallback if rollback else active), 'runtime_adoption': False}
