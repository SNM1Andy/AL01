"""Birth-gated, bounded provenance for existing unlearned-goal help continuation.

No history scan, recovery permission, learning label or new CL operation.
Archived LIST mappings establish historical roles even when current refs rotate.
"""
from . import compact_procedures as p
from .compact_observer import validate_capture
from .core import CVM, transaction_payload
from .folder_session import _encode

PROFILE = 'captured-remaining-role-1'


def check_receipt(goal, decision, item, seq, goals, origin, capture_origin, resolve):
    validate_capture(item)
    info = decision.get('investigation')
    p.need(info is not None and decision.get('help_check') is None, 'actual investigation, not help check')
    p.need(info['goal_id'] == goal['id'] and info['episode_key'] == goal['episode_key']
           and type(info['cursor']) is int and 0 <= info['cursor'] < 2 and info['role'] in p.ROLES,
           'captured goal/episode/cursor/role')
    q = decision['request']
    p.need(item['request'] == q and q['operation'] == 'READ_TEXT'
           and item['capture']['result']['status'] == 'OK'
           and item['capture']['capture_origin'] == capture_origin, 'successful original investigation capture')
    p.need(all(decision['source'][k] == v for k, v in origin.items())
           and q['scope_ref'] == origin['scope_ref'] and type(seq) is int
           and seq == decision['source']['global_seq'] + 3, 'original namespace and receipt sequence')
    listed = resolve(decision['source']['list_receipt_ref'], 'compact_capture')
    validate_capture(listed)
    p.need(listed['request']['operation'] == 'LIST_SCOPE' and listed['request']['scope_ref'] == origin['scope_ref']
           and listed['capture']['capture_origin'] == capture_origin
           and listed['capture']['result']['status'] == 'OK', 'original successful LIST')
    matches = [e for e in listed['capture']['result']['payload']['entries']
               if e['entry_ref'] == q['entry_ref'] and e['kind'] == 'file']
    path = goals[goal['bindings'][info['role']]]['path']
    p.need(len(matches) == 1 and matches[0]['relative_path'] == path, 'historical LIST source-role binding')
    p.need(decision['generated_cl'] == f"PROPOSE(OBSERVE(@{q['entry_ref']}));", 'exact original CL observation')
    vm = CVM(('read', 'observe'))
    tx = vm.prepare(decision['generated_cl'], dict(version=decision['source']['cognition_revision'],
        world_version=decision['source']['inventory_generation']), [q['entry_ref']])
    vm.verify(tx)
    p.need(_encode(transaction_payload(tx)) == _encode(decision['transaction_payload']), 'complete original CL payload')
    return info['role']


def remaining_role(goal, help_episode, goals, origin, capture_origin, resolve, *, current_workflow_ref):
    """At most one prefix step for an unfinished two-role goal; addressed reads only."""
    steps = goal.get('receipt_provenance')
    p.need(type(steps) is list and type(goal['cursor']) is int and len(steps) == goal['cursor'] < 2,
           'bounded captured prefix required')
    immutable = ('id', 'generation', 'episode_key', 'topic', 'bindings')
    progress = ('cursor', 'receipts', 'receipt_provenance')
    # Bind the complete evolving progress to its committed checkpoint. An empty
    # help-time prefix alone cannot prove that later receipts were not dropped.
    current = p.validate_workflow(resolve(current_workflow_ref, 'compact_workflow'))['goal']
    p.need(current is not None and all(current[k] == goal[k] for k in immutable + progress),
           'same committed goal and complete receipt progress')
    frozen = p.validate_workflow(resolve(help_episode['trigger']['source_workflow_ref'], 'compact_workflow'))['goal']
    p.need(frozen is not None and frozen['status'] == 'BLOCKED'
           and all(frozen[k] == goal[k] for k in immutable), 'same frozen blocked goal identity')
    p.need(all(frozen[k] == help_episode['trigger']['goal'][k] for k in immutable + ('cursor', 'receipts')),
           'same verified-help trigger')
    count = frozen['cursor']
    p.need(count <= goal['cursor'] and goal['receipts'][:count] == frozen['receipts']
           and steps[:count] == frozen['receipt_provenance'], 'preserved original captured receipt prefix')
    seen = []
    prior_seq = -1
    for cursor, step in enumerate(steps):
        for ref in (step['decision_ref'], step['capture_ref']):
            p.need(all(ref[k] == origin[k] for k in ('identity', 'instance_id')), 'prefix object namespace')
        d = resolve(step['decision_ref'], 'compact_decision')
        item = resolve(step['capture_ref'], 'compact_capture')
        role = check_receipt(goal, d, item, step['receipt_seq'], goals, origin, capture_origin, resolve)
        p.need(d['investigation']['cursor'] == cursor and step['receipt_seq'] > prior_seq
               and role == step['role'] and role not in seen
               and goal['receipts'][cursor] == step['capture_ref'], 'unique chronological captured roles')
        seen.append(role)
        prior_seq = step['receipt_seq']
    missing = [role for role in p.ROLES if role not in seen]
    p.need(len(missing) == 2 - goal['cursor'], 'remaining role is identifiable')
    return missing[0]
