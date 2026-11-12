"""Birth-only, one-lifetime STALE_REF -> new LIST. No learning or recovery rights."""
import copy

from .compact_observer import validate_capture
from .core import CVM, transaction_payload
from .folder_session import _hash, _encode
from .life_objects import _json
from .folder_contracts import MAX_ENTRIES, MAX_PATH_BYTES

PROFILE = 'stale-once-native-list-1'
SCHEMA = 'native-stale-list-acquisition-1'
MAX_BYTES = 8192
LIVE_STATE_LIMIT = 65536
# Valid paths forbid quote/backslash/control escapes; bound entry fields too.
INVENTORY_COMPLETION_BYTES = MAX_ENTRIES * (MAX_PATH_BYTES + 256) + 1024
RECEIPT_COMPLETION_BYTES = 8192
STOP = ('FAILED_STOP', 'SUPPRESSED_STOP', 'INVALIDATED_STOP')
PHASES = ('EMPTY', 'READY', 'LIST_REQUESTED', 'LIST_UNKNOWN', 'DONE') + STOP


def need(ok, reason):
    if not ok:
        raise ValueError('NATIVE_ACQUISITION_INVALID: ' + reason)


def initial():
    return dict(schema=SCHEMA, phase='EMPTY', cause=None, list_request_ref=None,
        list_receipt_ref=None, terminal_reason=None)


def completion_bytes(state, operation):
    """Bound both new roots, possible inventory and ordinary receipt growth."""
    from . import compact_native_order_evidence as passive
    size = len(_encode(state)) - len(_encode(state['native_acquisition'])) \
        - len(_encode(state['native_order_evidence'])) + MAX_BYTES + passive.MAX_BYTES
    if operation == 'LIST_SCOPE':
        size += INVENTORY_COMPLETION_BYTES - len(_encode(state['inventory']))
    return size + RECEIPT_COMPLETION_BYTES


def validate(root):
    _json(root, MAX_BYTES)
    need(type(root) is dict and set(root) == set(initial()) and root['schema'] == SCHEMA
         and root['phase'] in PHASES, 'bounded root/version')
    if root['phase'] == 'EMPTY':
        need(root == initial(), 'exact empty root')
    else:
        c = root['cause']
        need(type(c) is dict and set(c) == {'cause_id','identity','instance_id','scope_ref','receipt_seq',
            'failed_capture_ref','failed_decision_ref','list_capture_ref','inventory_generation','failed_entry_ref',
            'path','pre_workflow_ref','post_workflow_ref','queue_sha256'}, 'single exact cause')
        need(type(c['receipt_seq']) is int and c['receipt_seq'] > 0 and
             type(c['inventory_generation']) is int and c['inventory_generation'] > 0, 'cause sequence/generation')
        need(c['cause_id'] == _hash([PROFILE,c['identity'],c['instance_id'],c['scope_ref'],
             c['failed_capture_ref']['sha256'],c['receipt_seq'],c['inventory_generation']]), 'cause id')
        if root['phase'] in ('LIST_REQUESTED','LIST_UNKNOWN','DONE','FAILED_STOP'):
            need(type(root['list_request_ref']) is dict, 'one committed LIST request')
        if root['phase'] in ('DONE','FAILED_STOP'):
            need(type(root['list_receipt_ref']) is dict, 'actual LIST receipt')
        else:
            need(root['list_receipt_ref'] is None, 'no invented LIST receipt')
        if root['phase'] in ('READY','SUPPRESSED_STOP','INVALIDATED_STOP'):
            need(root['list_request_ref'] is None, 'unused entitlement has no LIST request')
        need((type(root['terminal_reason']) is str and len(root['terminal_reason']) <= 128)
             if root['phase'] in STOP else root['terminal_reason'] is None, 'terminal reason')
    return root


def _origin(state):
    return {k:state[k] for k in ('identity','instance_id','scope_ref')}


def _read(state, ref, kind, resolve):
    need(type(ref) is dict and ref.get('kind') == kind and
         all(ref.get(k) == state[k] for k in ('identity','instance_id')), 'typed original source reference')
    return resolve(ref, kind)


def _cl(d, target):
    s = d['source']
    need(d['generated_cl'] == f'PROPOSE(OBSERVE(@{target}));', 'original CL')
    vm = CVM(('read','observe'))
    tx = vm.prepare(d['generated_cl'],dict(version=s['cognition_revision'],
        world_version=s['inventory_generation']),[target])
    vm.verify(tx)
    need(_encode(transaction_payload(tx)) == _encode(d['transaction_payload']), 'complete original CL transaction')


def verify_cause(state, root, resolve):
    """Six addressed objects maximum; never reads/looks up journal history."""
    validate(root)
    c = root['cause']
    if c is None:
        return None
    need(all(c[k] == v for k,v in _origin(state).items()), 'cause namespace')
    d = _read(state,c['failed_decision_ref'],'compact_decision',resolve)
    item = _read(state,c['failed_capture_ref'],'compact_capture',resolve)
    listed = _read(state,c['list_capture_ref'],'compact_capture',resolve)
    pre = _read(state,c['pre_workflow_ref'],'compact_workflow',resolve)
    post = _read(state,c['post_workflow_ref'],'compact_workflow',resolve)
    validate_capture(item); validate_capture(listed)
    q, s = d['request'], d['source']
    need(all(s[k] == v for k,v in _origin(state).items()) and q == item['request']
         and q['scope_ref'] == state['scope_ref'] and q['operation'] == 'READ_TEXT'
         and item['capture']['result']['status'] == 'STALE_REF'
         and item['capture']['capture_origin'] == state['config']['capture_origin'], 'real original STALE_REF')
    need(s['global_seq'] + 3 == c['receipt_seq'] and s['list_receipt_ref'] == c['list_capture_ref']
         and s['inventory_generation'] == c['inventory_generation'] and q['entry_ref'] == c['failed_entry_ref'], 'cause chronology/LIST binding')
    need(listed['request']['operation'] == 'LIST_SCOPE' and listed['request']['scope_ref'] == state['scope_ref']
         and listed['capture']['result']['status'] == 'OK'
         and listed['capture']['capture_origin'] == state['config']['capture_origin'], 'historical successful LIST')
    entries = [e for e in listed['capture']['result']['payload']['entries']
               if e['kind'] == 'file' and e['entry_ref'] == q['entry_ref']]
    need(len(entries) == 1 and entries[0]['relative_path'] == c['path']
         and any(g['path'] == c['path'] for g in state['goals']), 'historical path')
    need(d['native_order_observation']['workflow_ref'] == c['pre_workflow_ref'], 'original workflow binding')
    _cl(d,q['entry_ref'])
    return d, pre, post


def blocker(state, workflow, root, resolve):
    validate(root)
    if root['phase'] in STOP:
        return root['terminal_reason']
    if root['phase'] == 'LIST_UNKNOWN':
        return 'EXECUTION_UNKNOWN'
    if root['phase'] != 'READY':
        return None
    d, pre, frozen = verify_cause(state,root,resolve)
    c = root['cause']
    if state['inventory']['generation'] != c['inventory_generation'] or state['inventory']['list_receipt_ref'] != c['list_capture_ref'] or not state['inventory']['valid']:
        return 'INVALIDATED_INVENTORY'
    if _encode(workflow['goal']) != _encode(frozen['goal']) or _encode(workflow['chains']) != _encode(frozen['chains']):
        return 'INVALIDATED_GOAL'
    if _hash(state['semantic_queue']) != c['queue_sha256']:
        return 'INVALIDATED_QUEUE'
    g = workflow['goal']
    if g is not None and g['status'] not in ('SATISFIED','BLOCKED'):
        return 'SUPPRESSED_GOAL_STATUS'
    if g is not None and g['status'] == 'BLOCKED':
        info, prior = d.get('investigation'), pre['goal']
        if not (info is not None and prior is not None and prior['status'] in ('ACTIVE','WAITING')
                and info['goal_id'] == prior['id'] == g['id'] and info['episode_key'] == g['episode_key']
                and g['reason'] == 'DENIED_OR_FAILED_SOURCE'
                and all(g[k] == prior[k] for k in ('generation','bindings','cursor','receipts','episode_key'))):
            return 'SUPPRESSED_UNRELATED_BLOCK'
    if any(g is None or q['topic'] != g['topic'] or q['goal_index'] not in g['bindings'].values()
           for q in state['semantic_queue']):
        return 'SUPPRESSED_OTHER_INVESTIGATION'
    if workflow['help']['episode'] is not None or workflow['interaction']['pending'] is not None:
        return 'SUPPRESSED_HELP_OR_LOCAL'
    if workflow['active_ref'] is not None or workflow['probe'] is not None or 'choice_profile' in state['config'] or 'continuation_ref' in state:
        return 'SUPPRESSED_OTHER_PROFILE'
    wait = workflow['typed_wait']
    if wait is not None and wait['status'] == 'WAITING':
        return 'WAITING'
    if max(state['service_debt']) >= 8:
        return 'FAIRNESS'
    return None


def recheck(state, workflow, root, resolve):
    validate(root)
    if root['phase'] != 'READY':
        return root
    reason = blocker(state,workflow,root,resolve)
    if reason is not None and reason not in ('WAITING','FAIRNESS'):
        root = copy.deepcopy(root)
        root.update(phase='INVALIDATED_STOP' if reason.startswith('INVALIDATED') else 'SUPPRESSED_STOP', terminal_reason=reason)
    return root


def tag(root):
    validate(root)
    c = root['cause']
    need(c is not None, 'cause before LIST')
    return dict(profile=PROFILE,cause_id=c['cause_id'],cause_receipt_ref=copy.deepcopy(c['failed_capture_ref']),
        inventory_generation=c['inventory_generation'])


def receive(state, root, d, item, capture_ref, decision_ref, seq, pre_workflow_ref, post_workflow_ref, resolve):
    validate(root); validate_capture(item)
    need(item['request'] == d['request'], 'original admitted request')
    if d.get('inventory_acquisition') is not None:
        need(root['phase'] == 'LIST_UNKNOWN' and root['list_request_ref'] == decision_ref
             and d['inventory_acquisition'] == tag(root) and item['request']['operation'] == 'LIST_SCOPE', 'one original LIST outcome')
        frozen = _read(state,root['cause']['post_workflow_ref'],'compact_workflow',resolve)
        post = _read(state,post_workflow_ref,'compact_workflow',resolve)
        need(post['goal'] == frozen['goal'] and post['chains'] == frozen['chains']
             and _hash(state['semantic_queue']) == root['cause']['queue_sha256'], 'preserved goal/queue prefix')
        out = copy.deepcopy(root)
        ok = item['capture']['result']['status'] == 'OK'
        out.update(phase='DONE' if ok else 'FAILED_STOP',list_receipt_ref=copy.deepcopy(capture_ref),
            terminal_reason=None if ok else 'COMMITTED_NON_OK_LIST')
        validate(out)
        return out
    if root['phase'] != 'EMPTY' or d['request']['operation'] != 'READ_TEXT' or item['capture']['result']['status'] != 'STALE_REF':
        return root
    s = d['source']
    need(state['inventory']['valid'] and state['inventory']['generation'] == s['inventory_generation']
         and state['inventory']['list_receipt_ref'] == s['list_receipt_ref'], 'committed current-generation failure')
    listed = _read(state,s['list_receipt_ref'],'compact_capture',resolve)
    entries = [e for e in listed['capture']['result']['payload']['entries']
               if e['kind'] == 'file' and e['entry_ref'] == d['request']['entry_ref']]
    need(len(entries) == 1, 'unique historical source')
    c = dict(_origin(state),receipt_seq=seq,failed_capture_ref=copy.deepcopy(capture_ref),
        failed_decision_ref=copy.deepcopy(decision_ref),list_capture_ref=copy.deepcopy(s['list_receipt_ref']),
        inventory_generation=s['inventory_generation'],failed_entry_ref=d['request']['entry_ref'],path=entries[0]['relative_path'],
        pre_workflow_ref=copy.deepcopy(pre_workflow_ref),post_workflow_ref=copy.deepcopy(post_workflow_ref),queue_sha256=_hash(state['semantic_queue']))
    c['cause_id'] = _hash([PROFILE,c['identity'],c['instance_id'],c['scope_ref'],capture_ref['sha256'],seq,c['inventory_generation']])
    out = initial(); out.update(phase='READY',cause=c)
    post = _read(state,post_workflow_ref,'compact_workflow',resolve)
    return recheck(state,post,out,resolve)


def request(root, d, decision_ref):
    validate(root)
    need(root['phase'] == 'READY' and d.get('inventory_acquisition') == tag(root)
         and d['request']['operation'] == 'LIST_SCOPE', 'single LIST claim')
    out = copy.deepcopy(root); out.update(phase='LIST_REQUESTED',list_request_ref=copy.deepcopy(decision_ref))
    validate(out)
    return out


def start(root, decision_ref):
    validate(root)
    need(root['phase'] == 'LIST_REQUESTED' and root['list_request_ref'] == decision_ref, 'one START/no retry')
    out = copy.deepcopy(root); out['phase'] = 'LIST_UNKNOWN'
    return out
