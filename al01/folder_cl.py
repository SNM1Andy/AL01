"""Generated OBSERVE -> typed READ_TEXT bridge; no environmental execution.

Persisted digests bind deterministic data, not authority. CVM HMAC seals are
ephemeral and verified only by their issuing VM, then deliberately discarded.
Listing generation is an admission ordinal, not a claimed filesystem version.
This is not Individual.commit and does not manufacture grid reward or feedback.
"""
import copy
import re
from .core import CVM, Compiler, transaction_payload, VERSION
from .folder_cognition import validate_cognition, _hash, _raw
from .folder_contracts import make_request

SCHEMA = 'folder-cl-observe-1'


def _need(ok, detail):
    if not ok:
        raise ValueError('FOLDER_CL_INVALID: '+detail)


def _caps(capabilities):
    _need(type(capabilities) in (list, tuple, set, frozenset)
          and all(type(c) is str for c in capabilities)
          and {'read', 'observe'} <= set(capabilities), 'existing read and observe required')
    # Least authority for this generated transaction, never a grant.
    return ('read', 'observe')


def prepare_decision(cognition, entry_ref, *, capabilities=('read', 'observe')):
    caps = _caps(capabilities)
    state = validate_cognition(cognition)
    _need(type(entry_ref) is str and re.fullmatch(r'entry_[0-9a-f]{32}', entry_ref), 'opaque entry reference')
    listing = None
    for source in state['sources']:
        request = source['request']
        if request['operation'] == 'LIST_SCOPE' and request['entry_ref'] is None:
            listing = source
    _need(listing is not None and listing['capture']['result']['status'] == 'OK', 'latest root LIST must succeed')
    visible = sorted(e['entry_ref'] for e in listing['capture']['result']['payload']['entries'] if e['kind'] == 'file')
    _need(entry_ref in visible, 'unresolved or stale file reference')
    world_version = listing['sequence']
    source = f'PROPOSE(OBSERVE(@{entry_ref}));'
    # Minimal execution context is sufficient for these two registered ops;
    # it is not represented as a complete Individual state or snapshot.
    context = {'version': state['version'], 'world_version': world_version}
    ir = Compiler(visible, caps).compile(source)
    vm = CVM(caps)
    tx = vm.prepare(source, context, visible)
    vm.verify(tx)
    payload = transaction_payload(tx)
    _need(payload['ir'] == ir and payload['state_version'] == state['version']
          and payload['world_version'] == world_version, 'compiler/source binding')
    _need(payload['proposals'] == [{'kind': 'observe', 'target': entry_ref}]
          and payload['messages'] == [] and payload['neuron_calls'] == []
          and payload['wait_reason'] is None, 'exactly one observation, no other effects')
    binding = {'identity': state['identity'], 'scope_ref': state['scope_ref'],
               'state_version': state['version'], 'world_version': world_version,
               'world_version_meaning': 'latest root LIST admission ordinal',
               'cognition_sha256': _hash(state), 'listing_receipt_sha256': listing['receipt_sha256'],
               'visible_refs_sha256': _hash(visible),
               'event_head': state['events'][-1]['hash']}
    request_id = 'req_'+_hash({'schema': SCHEMA, 'binding': binding, 'entry_ref': entry_ref})[:32]
    request = make_request('READ_TEXT', state['scope_ref'], request_id=request_id, entry_ref=entry_ref)
    decision = {'schema': SCHEMA, 'core_version': VERSION, 'source_binding': binding,
                'entry_ref': entry_ref, 'required_capabilities': list(caps),
                'generated_source': source, 'transaction_payload': payload,
                'transaction_sha256': _hash(payload), 'request': request,
                'seal_semantics': 'ephemeral issuing-CVM HMAC verified; not persisted',
                'authority': 'NONE; deterministic validation is not permission or execution'}
    decision['decision_sha256'] = _hash(decision)
    return copy.deepcopy(decision)


def validate_decision(decision, cognition, *, capabilities=('read', 'observe')):
    """Fresh compile/prepare/verify and exact replay; return only typed request.

    The surrounding durable runner still owns budgets, pending-request checks,
    request persistence before dispatch, and broker authorization.
    """
    _caps(capabilities)
    _need(type(decision) is dict and type(decision.get('entry_ref')) is str, 'decision')
    _need(len(_raw(decision)) <= 32768, 'decision byte budget')
    expected = prepare_decision(cognition, decision['entry_ref'], capabilities=capabilities)
    _need(_raw(decision) == _raw(expected), 'altered or stale prepared decision')
    return copy.deepcopy(expected['request'])
