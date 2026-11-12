"""Pure retrospective STALE-source reference observation, no policy authority.

Trusted pinned archive integrity + event/object links, NOT a reducer cold fold,
host authentication, content-change claim, future-validity or recovery license.
The caller's frame reader must be read-only. This module performs no own I/O.
"""
import copy

from . import compact_skill_evidence as evidence
from . import compact_native_acquisition as acquisition
from .compact_native_order_evidence import PROFILE as PASSIVE_PROFILE
from .compact_observer import validate_capture
from .core import VERSION
from .folder_contracts import validate_ref
from .folder_session import _hash, _encode
from .life_objects import _load, _json

SCHEMA = 'committed-stale-source-observation-2'
MAX_OBJECTS = 12
MAX_TOTAL_BYTES = 2 * 1024 * 1024
MAX_OUTPUT_BYTES = 16384


class SourceObservationError(ValueError):
    pass


def need(ok, reason):
    if not ok:
        raise SourceObservationError(reason)


def same(a, b):
    return _encode(a) == _encode(b)


class _Objects(evidence._Objects):
    """Reuse the frozen pure frame codec, with stricter observer-local budgets."""
    def read(self, ref, kind):
        self.codec._reference(ref, kind)
        key = (kind, ref['sha256'])
        if key not in self.cache:
            if len(self.cache) >= MAX_OBJECTS:
                raise evidence._Unavailable('OBJECT_BUDGET')
            if self.bytes + ref['byte_size'] > MAX_TOTAL_BYTES:
                raise evidence._Unavailable('TOTAL_FRAME_BUDGET')
        return super().read(ref, kind)


def _request(objects, ref, origin, byseq, policy):
    d, local = evidence._decision(objects, ref, origin)
    need(not local, 'FOLDER_DECISION_REQUIRED')
    s, q = d['source'], d['request']
    need(s['policy_version'] == policy, 'POLICY_VERSION_BINDING')
    seq = s['global_seq'] + 1
    if seq not in byseq:
        raise evidence._Unavailable('SPAN_NOT_COVERED')
    event = byseq[seq]
    need(event['kind'] == 'REQUEST' and same(event['payload'], dict(decision_ref=ref))
         and event['previous'] == s['journal_head']
         and event['effect'] == dict(phase='REQUEST', id=origin['identity'] + ':' + str(seq)), 'ORIGINAL_REQUEST_EVENT')
    g = d['governance']
    need(g['authority'] == 'EXISTING_CAPABILITIES_ONLY' and g['approved_request_sha256'] == _hash(q)
         and type(g['external_action_cost']) is int and g['external_action_cost'] == 1, 'ADMITTED_GOVERNANCE')
    return d, event


def _receipt(objects, ref, origin, byseq, capture_ref, capture_origin, policy):
    d, request = _request(objects, ref, origin, byseq, policy)
    start_seq, receipt_seq = request['seq'] + 1, request['seq'] + 2
    if start_seq not in byseq or receipt_seq not in byseq:
        raise evidence._Unavailable('SPAN_NOT_COVERED')
    start, receipt = byseq[start_seq], byseq[receipt_seq]
    effect_id = request['effect']['id']
    need(start['kind'] == 'START' and start['effect'] == dict(phase='START', id=effect_id)
         and receipt['kind'] == 'RECEIPT' and receipt['effect'] == dict(phase='RECEIPT', id=effect_id)
         and same(receipt['payload']['capture_ref'], capture_ref), 'ORIGINAL_START_RECEIPT_EVENTS')
    item = objects.read(capture_ref, 'compact_capture')
    validate_capture(item)
    need(same(item['request'], d['request']) and item['capture']['capture_origin'] == capture_origin,
         'ORIGINAL_CAPTURE_REQUEST_ORIGIN')
    return d, item, receipt


def _listing_commit(objects, capture_ref, events, origin, byseq, capture_origin, policy):
    matches = [e for e in events if e['kind'] == 'RECEIPT' and same(e['payload'].get('capture_ref'), capture_ref)]
    if not matches:
        raise evidence._Unavailable('SPAN_NOT_COVERED')
    need(len(matches) == 1, 'UNIQUE_LIST_COMMIT')
    seq = matches[0]['seq'] - 2
    if seq not in byseq:
        raise evidence._Unavailable('SPAN_NOT_COVERED')
    ref = byseq[seq]['payload'].get('decision_ref')
    d, item, event = _receipt(objects, ref, origin, byseq, capture_ref, capture_origin, policy)
    need(d['request']['operation'] == 'LIST_SCOPE', 'LIST_OPERATION')
    return ref, d, item, event


def _observe(result, objects, origin, proof):
    events, end, _, base_known = evidence._journal(proof, origin)
    birth = _load(proof['birth'])['data']
    checkpoint = _load(proof['checkpoint'])['data']
    endpoint = checkpoint['anchor'] if 'events' in checkpoint else checkpoint
    state = endpoint['state']; config = state['config']
    result.update(checkpoint_sha256=proof['checkpoint_sha256'], endpoint_seq=end,
        covered_first_seq=events[0]['seq'] if events else None, birth_event_covered=base_known,
        current_pending_phase=None if state['pending'] is None else state['pending']['phase'],
        versions=dict(observer=SCHEMA, runtime=state['schema'], reducer=birth['reducer_id'],
            policy=config['policy_version'], cl=VERSION, acquisition=config.get('native_acquisition_profile')))
    if state['schema'] != 'compact-continuation-6' or birth['reducer_id'] != 'compact-continuation-reducer-6':
        result['reason'] = 'UNSUPPORTED_PROTOCOL_VERSION'
        return
    root = state.get('native_acquisition')
    if root is None:
        need(config.get('native_acquisition_profile') is None, 'MISSING_REQUIRED_ROOT')
        result['reason'] = 'ACQUISITION_DISABLED'
        return
    if config.get('native_acquisition_profile') != acquisition.PROFILE or config.get('native_order_evidence_profile') != PASSIVE_PROFILE:
        result['reason'] = 'UNSUPPORTED_PROFILE_VERSION'
        return
    acquisition.validate(root)
    result.update(root_sha256=_hash(root), phase=root['phase'])
    c = root['cause']
    if c is None:
        result['reason'] = 'NO_COMMITTED_STALE_CAUSE'
        return
    need(all(c[k] == v for k,v in origin.items()), 'CAUSE_NAMESPACE')
    byseq = {e['seq']:e for e in events}
    capture_origin = config['capture_origin']; policy = config['policy_version']
    need(capture_origin == 'SYNTHETIC_TEST_CAPTURE', 'SUPPORTED_CAPTURE_ORIGIN')
    d, failed, failure = _receipt(objects,c['failed_decision_ref'],origin,byseq,
        c['failed_capture_ref'],capture_origin,policy)
    need(d['request']['operation'] == 'READ_TEXT' and failed['capture']['result']['status'] == 'STALE_REF'
         and failure['seq'] == c['receipt_seq'] and d['source']['inventory_generation'] == c['inventory_generation']
         and same(d['source']['list_receipt_ref'],c['list_capture_ref'])
         and d['request']['entry_ref'] == c['failed_entry_ref'], 'COMMITTED_STALE_BINDING')
    for key in ('pre_workflow_ref','post_workflow_ref'):
        objects.codec._reference(c[key], 'compact_workflow')
    need(same(d['native_order_observation']['workflow_ref'],c['pre_workflow_ref'])
         and same(failure['payload']['workflow_ref'],c['post_workflow_ref']), 'CAUSE_WORKFLOW_EVENT_BINDING')
    old_ref, old_d, old, old_event = _listing_commit(objects,c['list_capture_ref'],events,
        origin,byseq,capture_origin,policy)
    old_result = old['capture']['result']
    need(old_result['status'] == 'OK' and old_event['seq'] <= d['source']['global_seq']
         and old_d['source']['inventory_generation'] + 1 == c['inventory_generation'], 'ORIGINAL_SUCCESSFUL_LIST_VERSION')
    need(0 <= old_d['source']['cognition_revision'] < d['source']['cognition_revision'] < 2**53,
         'OBSERVED_COGNITION_VERSION_ORDER')
    path = evidence._path(objects,d,origin,capture_origin)
    need(path == c['path'], 'ORIGINAL_SOURCE_PATH')
    result.update(path=path,old_entry_ref=c['failed_entry_ref'],cause_id=c['cause_id'],
        provenance=dict(old_list_decision_ref=old_ref,old_list_capture_ref=c['list_capture_ref'],
            stale_decision_ref=c['failed_decision_ref'],stale_capture_ref=c['failed_capture_ref'],
            old_list_receipt_seq=old_event['seq'],stale_receipt_seq=failure['seq'],
            old_inventory_generation=c['inventory_generation'],pre_workflow_ref=c['pre_workflow_ref'],
            post_workflow_ref=c['post_workflow_ref']))
    if root['list_request_ref'] is None:
        result['reason'] = 'NO_COMMITTED_NEW_LIST_REQUEST'
        return
    nd, request = _request(objects,root['list_request_ref'],origin,byseq,policy)
    need(nd['request']['operation'] == 'LIST_SCOPE' and request['seq'] > failure['seq']
         and nd['request']['request_id'] not in (d['request']['request_id'],old_d['request']['request_id'])
         and same(nd.get('inventory_acquisition'),acquisition.tag(root))
         and nd['source']['inventory_generation'] == c['inventory_generation']
         and d['source']['cognition_revision'] < nd['source']['cognition_revision'] < 2**53
         and same(nd['source']['list_receipt_ref'],c['list_capture_ref'])
         and nd['source']['visible_sha256'] == _hash(old_result['payload']['entries']), 'CAUSAL_NEW_LIST_REQUEST')
    result['provenance'].update(new_list_decision_ref=root['list_request_ref'],new_list_request_seq=request['seq'])
    if root['list_receipt_ref'] is None:
        # Folder pending cannot progress beyond its one START without a receipt.
        # A pinned but internally inconsistent rolled-back root is not UNKNOWN.
        need(end <= request['seq']+1, 'PENDING_AFTER_COMMITTED_EVENTS')
        pending = state['pending']
        need(pending is not None and same(pending['decision_ref'],root['list_request_ref']), 'PENDING_NEW_LIST_BINDING')
        need(state['inventory']['generation'] == c['inventory_generation']
             and same(state['inventory']['list_receipt_ref'],c['list_capture_ref'])
             and state['inventory']['valid'] is True, 'PENDING_CURRENT_LIST_VERSION')
        start = byseq.get(request['seq']+1)
        if root['phase'] == 'LIST_UNKNOWN':
            need(pending['phase'] == 'UNKNOWN' and start is not None and start['kind'] == 'START'
                 and start['effect'] == dict(phase='START',id=request['effect']['id']), 'UNKNOWN_START_BINDING')
            result['reason'] = 'EXECUTION_UNKNOWN'
        else:
            need(root['phase'] == 'LIST_REQUESTED' and pending['phase'] == 'REQUESTED' and start is None,
                 'REQUESTED_WITHOUT_EXECUTION')
            result['reason'] = 'NO_COMMITTED_NEW_LIST_RECEIPT'
        return
    _, new, new_event = _receipt(objects,root['list_request_ref'],origin,byseq,
        root['list_receipt_ref'],capture_origin,policy)
    nr = new['capture']['result']; inv = state['inventory']
    need(root['phase'] == ('DONE' if nr['status'] == 'OK' else 'FAILED_STOP')
         and inv['generation'] == c['inventory_generation']+1
         and same(inv['list_receipt_ref'],root['list_receipt_ref'])
         and type(inv['valid']) is bool and inv['valid'] == (nr['status']=='OK')
         and same(inv['entries'],nr['payload']['entries'] if nr['status']=='OK' else []), 'CURRENT_ROOT_LIST_VERSION')
    result['provenance'].update(new_list_capture_ref=root['list_receipt_ref'],
        new_list_receipt_seq=new_event['seq'],new_inventory_generation=inv['generation'])
    result.update(new_list_result_status=nr['status'],new_list_result_reason=nr['reason'])
    if nr['status'] != 'OK':
        result.update(status='NO_SUCCESSFUL_NEW_LIST',reason='COMMITTED_NON_OK_LIST')
        return
    if not old_result['payload']['complete'] or not nr['payload']['complete']:
        result['reason'] = 'INCOMPLETE_LIST'
        return
    matches = [e for e in nr['payload']['entries'] if e['relative_path']==path]
    need(len(matches)<=1, 'UNIQUE_NEW_PATH')
    entry = matches[0] if matches else None
    new_ref = entry['entry_ref'] if entry and entry['kind']=='file' else None
    relation = ('PATH_NOT_LISTED' if entry is None else
        'PATH_PRESENT_NON_FILE' if entry['kind'] != 'file' else
        'SAME_REFERENCE' if new_ref == c['failed_entry_ref'] else 'DIFFERENT_REFERENCE')
    result.update(status='OBSERVED',reason='TWO_COMMITTED_LISTS_WITH_REAL_STALE_CAUSE',new_entry_ref=new_ref,
        new_entry_kind=None if entry is None else entry['kind'],reference_relation=relation)


def observe_failed_source(*, expected_origin, journal_proof, read_frame):
    """Observe a bounded pinned archive graph; missing -> UNKNOWN, invalid -> error."""
    result = dict(schema=SCHEMA,status='UNKNOWN',reason=None,reference_relation=None,path=None,
        evidence_scope='RETROSPECTIVE_DERIVED_VIEW_NOT_PERSONAL_EPISODE',
        source=copy.deepcopy(expected_origin),new_entry_ref=None,content_change='UNKNOWN',future_validity='UNKNOWN',
        learning='NOT_ESTABLISHED',independence='NOT_ESTABLISHED',assistance='NOT_ASSESSED',authority='NONE',
        state_replay='NOT_REPLAYED',objects_read=0,frame_bytes_read=0)
    objects = None
    try:
        need(type(expected_origin) is dict and set(expected_origin)=={'identity','instance_id','scope_ref'}, 'ORIGIN_FIELDS')
        need(type(expected_origin['identity']) is str and 1<=len(expected_origin['identity'])<=128
             and type(expected_origin['instance_id']) is str and len(expected_origin['instance_id'])==32
             and all(x in '0123456789abcdef' for x in expected_origin['instance_id']), 'ORIGIN_TYPES')
        validate_ref(expected_origin['scope_ref'],'scope')
        objects = _Objects(expected_origin,read_frame)
        _observe(result,objects,expected_origin,journal_proof)
    except evidence._Unavailable as error:
        result.update(status='UNKNOWN',reason=str(error),reference_relation=None,new_entry_ref=None)
    except PermissionError:
        result.update(status='UNKNOWN',reason='EVIDENCE_READER_ACCESS_DENIED',reference_relation=None,new_entry_ref=None)
    except OSError:
        result.update(status='UNKNOWN',reason='EVIDENCE_READER_ERROR',reference_relation=None,new_entry_ref=None)
    except SourceObservationError:
        raise
    except (ValueError,KeyError,TypeError,IndexError,AttributeError) as error:
        raise SourceObservationError(str(error)) from error
    if objects is not None:
        result.update(objects_read=len(objects.cache),frame_bytes_read=objects.bytes)
    _json(result,MAX_OUTPUT_BYTES)
    return result
