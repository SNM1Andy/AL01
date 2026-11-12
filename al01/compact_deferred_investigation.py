"""Opt-in first investigation opportunities from an already admitted index.

No new evidence, retries, quota renewal, transport, or history reconstruction.
The caller admits the result together with the successful source receipt.
"""
from . import compact_semantics as semantics
from . import compact_procedures as procedures

PROFILE = 'first-observed-deferred-conflict-1'

def validate_profile(profile):
    procedures.need(profile is None or profile == PROFILE,
                    'supported versioned deferred investigation profile')
    return profile

def select(index, state):
    """Choose at most one never-investigated two-source topic, deterministically.

    An ineligible topic does not get marked investigated. All scanning is
    bounded by the existing index limits, not proportional to journal history.
    """
    semantics.validate_semantics(index)
    for key in ('identity', 'instance_id', 'scope_ref'):
        procedures.need(index[key] == state[key], 'deferred source namespace')
    if not state['inventory']['valid'] or index['overflow'] is not None:
        return index, [], None
    active = {g['path']: i for i, g in enumerate(state['goals']) if g['status'] == 'ACTIVE'}
    visible = {e['relative_path']: e for e in state['inventory']['entries'] if e['kind'] == 'file'}
    topics = {}
    for record in index['records']:
        topics.setdefault(record['topic'], []).append(record)
    ordered = sorted(topics.items(), key=lambda pair: (min(r['receipt_seq'] for r in pair[1]), pair[0]))
    for topic, records in ordered:
        if topic in index['investigations']:
            continue  # No implicit new rounds, retries, or cancelled-goal revival.
        paths = {r['path'] for r in records}
        if len(paths) != 2 or not paths <= active.keys() or not paths <= visible.keys():
            continue
        if len({visible[p]['entry_ref'] for p in paths}) != 2:
            continue
        if any(r['capture_origin'] != state['config']['capture_origin'] for r in records):
            continue
        claim = records[0]['claim']
        if semantics.query(index, claim['subject'], claim['predicate'])['status'] != 'UNRESOLVED_CONFLICT':
            continue  # Context-only ambiguity is not this profile's scope.
        updated, proposal = semantics.request_investigation(index, claim['subject'], claim['predicate'], state['inventory']['entries'])
        if len(proposal['proposals']) != 2:
            continue
        ctx = procedures.context(updated, topic, state['goals'])
        if ctx is None:
            continue
        queue = [dict(goal_index=active[q['path']], topic=q['topic'], intent_key=q['intent_key'], round=q['round'])
                 for q in proposal['proposals']]
        return updated, queue, ctx
    return index, [], None
