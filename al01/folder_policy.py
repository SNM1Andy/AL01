"""Bounded metadata-only research policy. Scores never grant file authority.

The caller must separately reject outstanding requests and enforce actual broker
byte limits. Declared listing sizes are admission estimates, not future-read
guarantees. Parent-folder persistence is lexical metadata, not spatial inference.
"""
import copy
from dataclasses import asdict
from .folder_cognition import validate_cognition, _bindings, _hash, _raw, _target
from .control_embryos import candidate_offset
from .coupling import rank_candidates
from .instinct import FrozenBias

MAX_READS = 29
MAX_BYTES = 256 * 1024
MAX_CANDIDATES = 3


def _need(ok, reason):
    if not ok:
        raise ValueError('FOLDER_POLICY_INVALID: ' + reason)


def _parent(path):
    return path.rsplit('/', 1)[0] if '/' in path else ''


def select_candidate(cognition, feature_values, calibration, *, u0_enabled=True, field_enabled=True):
    _need(type(u0_enabled) is bool and type(field_enabled) is bool, 'ablation flags')
    _need(isinstance(calibration, FrozenBias), 'frozen calibration')
    calibration.biases(feature_values)  # Validate even if exhausted/disabled.
    state = validate_cognition(cognition)
    sources = state['sources']; listing = None; attempted = set(); reads = 0; used = 0; previous = None
    for index, source in enumerate(sources):
        request = source['request']; result = source['capture']['result']
        if request['operation'] == 'LIST_SCOPE' and request['entry_ref'] is None:
            listing = result['payload']['entries'] if result['status'] == 'OK' else None
        if request['operation'] == 'READ_TEXT':
            reads += 1
            path = _bindings(sources[:index]).get(request['entry_ref'])
            if path is not None:
                attempted.add(path); previous = _parent(path)
            if result['status'] == 'OK': used += result['payload']['utf8_bytes']
    _need(used <= MAX_BYTES and reads <= MAX_READS, 'recorded policy limits exceeded')
    visible = [] if listing is None else [e for e in listing if e['kind'] == 'file']
    eligible = [e for e in visible if e['relative_path'] not in attempted and e['byte_size'] <= MAX_BYTES-used]
    eligible.sort(key=lambda e: e['entry_ref'])
    chosen = eligible[:MAX_CANDIDATES] if reads < MAX_READS else []
    candidates = []; metadata = []
    for entry in chosen:
        path = entry['relative_path']; parent = _parent(path)
        token = _target({'entry_ref': entry['entry_ref'], 'scope_ref': state['scope_ref']}, _bindings(sources))
        tokens = [token, 'parent_'+_hash(parent)]
        raw_offset = candidate_offset(state['control'], {'tokens': tokens, 'novelty': 1.})
        offset = raw_offset if u0_enabled else 0.
        candidates.append({'entity_ref': entry['entry_ref'], 'baseline_score': .02+offset,
                           'novelty': 1., 'persistence': float(previous is not None and parent == previous),
                           'switching': float(previous is not None and parent != previous)})
        metadata.append({'entry_ref': entry['entry_ref'], 'relative_path': path, 'declared_bytes': entry['byte_size'],
                         'tokens': tokens, 'u0_raw_offset': raw_offset, 'u0_applied_offset': offset})
    ranked = rank_candidates(candidates, feature_values, calibration, enabled=field_enabled) if candidates else None
    return copy.deepcopy({'schema': 'folder-policy-1', 'source': {'identity': state['identity'],
        'scope_ref': state['scope_ref'], 'version': state['version'], 'cognition_sha256': _hash(state),
        'event_head': state['events'][-1]['hash'] if state['events'] else '0'*64},
        'u0_enabled': u0_enabled, 'field_enabled': field_enabled, 'candidate_inputs': candidates,
        'field_features': copy.deepcopy(feature_values), 'calibration_sha256': _hash(asdict(calibration)),
        'candidate_metadata': metadata, 'ranking': ranked, 'selected': ranked['selected'] if ranked else None,
        'status': 'CANDIDATE' if ranked else 'NO_ELIGIBLE_CANDIDATE',
        'budget': {'read_attempts': reads, 'captured_bytes': used, 'max_reads': MAX_READS, 'max_bytes': MAX_BYTES},
        'visible_file_count': len(visible), 'eligible_count': len(eligible), 'previous_parent': previous,
        'authority': 'NONE; caller must check outstanding requests and broker limits',
        'operational_definitions': {'shortlist': 'first 3 sorted opaque IDs after filtering latest complete root LIST',
            'unread': 'no prior READ_TEXT attempt for LIST-bound path',
            'novelty': 'constant 1 for unread shortlist; common field offset',
            'persistence': 'same lexical parent folder as previous LIST-bound read',
            'switching': 'different lexical parent folder; both zero without prior read',
            'u0': 'existing candidate_offset on stable path identity and hashed parent metadata',
            'capacity': '29 captured read attempts, 256KiB successful captured text; advertised bytes reserved'},
        'semantics': 'metadata heuristic; no benefit or learning claim'})


def validate_policy(trace, cognition, feature_values, calibration):
    _need(type(trace) is dict and type(trace.get('u0_enabled')) is bool and type(trace.get('field_enabled')) is bool, 'trace')
    expected = select_candidate(cognition, feature_values, calibration,
        u0_enabled=trace['u0_enabled'], field_enabled=trace['field_enabled'])
    _need(_raw(expected) == _raw(trace), 'stale or altered score trace')
    return copy.deepcopy(trace)
