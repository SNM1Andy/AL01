"""Versioned internal derived memory from one actual acquisition receipt.

No journal access, clock, dispatcher, learning or authority. Object existence is
staging only; the owning RECEIPT reducer makes the unique reference committed.
"""
import copy

from . import compact_native_acquisition as acquisition
from . import compact_native_order_evidence as passive
from . import compact_source_observation as observer
from .compact_observer import validate_capture
from .core import VERSION
from .folder_session import _hash, _encode
from .life_objects import LifeObjects, MAX_HEADER, SCHEMA as OBJECT_SCHEMA, _json
from .folder_contracts import validate_relative_path, validate_ref

PROFILE = 'commit-source-observation-1'
SCHEMA = 'derived-source-observation-record-1'
DERIVATION = 'native-receipt-derived-memory-1'
OBSERVER = 'committed-stale-source-observation-2'
KIND = 'compact_derived_observation'
MAX_BYTES = 16384
RELATIONS = ('SAME_REFERENCE','DIFFERENT_REFERENCE','PATH_NOT_LISTED','PATH_PRESENT_NON_FILE')
REF_KINDS = dict(failed_decision_ref='compact_decision',failed_capture_ref='compact_capture',
    old_list_decision_ref='compact_decision',old_list_capture_ref='compact_capture',
    new_list_decision_ref='compact_decision',new_list_capture_ref='compact_capture',
    cause_workflow_ref='compact_workflow',new_workflow_ref='compact_workflow')


def state_reserve(state):
    # The only new live payload is this reference in two places. All text fields
    # are known now; payload/byte counters use their admitted upper bounds. Do
    # not reserve a whole generic MAX_HEADER twice when identity length is known.
    ref=dict(schema=OBJECT_SCHEMA,identity=state['identity'],instance_id=state['instance_id'],kind=KIND,
        encoding='json',payload_bytes=MAX_BYTES,sha256='f'*64,byte_size=MAX_BYTES+MAX_HEADER+1)
    return 2*len(_encode(ref))+128  # extra keys and None/last-receipt replacement


def passive_list_completion_bytes(state, decision, max_capture_bytes):
    """Upper bound for this one LIST's passive root, not a fabricated witness.

    record_list either retains the root (non-OK) or appends precisely the
    witness shape below and keeps its final two entries (OK). The new capture
    digest and two bounded integers are the only unknown values in that root.
    Nothing here is published, and no result/success flag is accepted.
    """
    need(state['config'].get('derived_observation_profile') == PROFILE
         and decision.get('inventory_acquisition') is not None
         and decision['request']['operation'] == 'LIST_SCOPE', 'bounded causal LIST only')
    root = passive.validate(state['native_order_evidence'])
    pending = state['pending']; source = decision['source']
    need(pending is not None and pending['phase'] == 'REQUESTED', 'pre-START capacity only')
    need(all(source[k] == state[k] for k in ('identity', 'instance_id', 'scope_ref'))
         and decision['request']['scope_ref'] == state['scope_ref'], 'capacity source namespace')
    codec = object.__new__(LifeObjects)
    codec.identity = state['identity']; codec.instance_id = state['instance_id']
    codec._reference(pending['decision_ref'], 'compact_decision')
    need(type(max_capture_bytes) is int and max_capture_bytes == 400000, 'capture publication bound')
    capture_upper = dict(schema=OBJECT_SCHEMA, identity=state['identity'], instance_id=state['instance_id'],
        kind='compact_capture', encoding='json', payload_bytes=max_capture_bytes,
        sha256='f'*64, byte_size=max_capture_bytes+MAX_HEADER+1)
    upper = copy.deepcopy(root)
    upper['lists'] = (upper['lists'] + [dict(capture_ref=capture_upper,
        decision_ref=copy.deepcopy(pending['decision_ref']), receipt_seq=source['global_seq']+3,
        source_seq=source['global_seq'], source_head=source['journal_head'],
        generation=source['inventory_generation']+1)])[-2:]
    # We measure an envelope, not an actual root: its upper-size reference is
    # not a content hash of a real object. Never publish or use it as evidence.
    return min(passive.MAX_BYTES, max(len(_encode(root)), len(_encode(upper))))


def need(ok, why):
    if not ok:
        raise ValueError('DERIVED_OBSERVATION_INVALID: ' + why)


def _origin(state):
    return {k:state[k] for k in ('identity','instance_id','scope_ref')}


def _id(record):
    return _hash([SCHEMA,DERIVATION,record['observer_schema'],record['cl_verifier_version'],
        record['origin'],record['cause_id'],{k:record['provenance'][k] for k in REF_KINDS}])


def validate(record, origin):
    _json(record,MAX_BYTES)
    keys={'schema','derivation','observer_schema','cl_verifier_version','runtime_schema','reducer_version',
        'policy_version','source_kind','origin','id','cause_id','reference_relation','path','old_entry_ref',
        'new_entry_ref','new_entry_kind','provenance','commit','uncertainty','authority','evidence_role'}
    need(type(record) is dict and set(record)==keys, 'record fields')
    need(record['schema']==SCHEMA and record['derivation']==DERIVATION and record['observer_schema']==OBSERVER
         and record['cl_verifier_version']==VERSION and record['runtime_schema']=='compact-continuation-6'
         and record['reducer_version']=='compact-continuation-reducer-6'
         and record['policy_version']=='global-eligible-prospective-renewal-7', 'record versions')
    need(_encode(record['origin'])==_encode(origin) and record['source_kind']=='internal_inference'
         and record['authority']=='NONE' and record['evidence_role']=='DERIVED_NOT_INDEPENDENT_EXPERIENCE', 'record source/type/authority')
    need(record['reference_relation'] in RELATIONS, 'supported relation')
    validate_relative_path(record['path']);validate_ref(record['old_entry_ref'],'entry')
    relation=record['reference_relation'];newref=record['new_entry_ref'];kind=record['new_entry_kind']
    if relation in ('SAME_REFERENCE','DIFFERENT_REFERENCE'):
        validate_ref(newref,'entry')
        need(kind=='file' and (newref==record['old_entry_ref'])==(relation=='SAME_REFERENCE'), 'file relation/ref')
    else:
        need(newref is None and (kind is None if relation=='PATH_NOT_LISTED' else kind=='directory'), 'absence/non-file relation')
    codec=object.__new__(LifeObjects);codec.identity=origin['identity'];codec.instance_id=origin['instance_id']
    for key,kind in REF_KINDS.items():codec._reference(record['provenance'][key],kind)
    p=record['provenance'];c=record['commit']
    need(set(p)==set(REF_KINDS)|{'old_list_receipt_seq','failed_receipt_seq','old_inventory_generation',
        'new_list_receipt_seq','new_inventory_generation'}, 'provenance fields')
    need(all(type(p[k]) is int and 0<p[k]<2**53 for k in ('old_list_receipt_seq','failed_receipt_seq',
        'new_list_receipt_seq','old_inventory_generation','new_inventory_generation')), 'provenance counters')
    need(type(c) is dict and set(c)=={'effect_id','anchor_seq','anchor_head','receipt_seq'}
         and type(c['anchor_seq']) is int and c['anchor_seq']==p['new_list_receipt_seq']-1
         and c['receipt_seq']==p['new_list_receipt_seq']
         and type(c['anchor_head']) is str and len(c['anchor_head'])==64
         and all(x in '0123456789abcdef' for x in c['anchor_head']), 'pre-receipt anchor')
    need(c['effect_id']==origin['identity']+':'+str(c['receipt_seq']-2), 'original effect id')
    need(p['old_list_receipt_seq']<p['failed_receipt_seq']<p['new_list_receipt_seq']
         and p['new_inventory_generation']==p['old_inventory_generation']+1, 'source chronology')
    need(record['uncertainty']==dict(content_change='UNKNOWN',future_validity='UNKNOWN',
        independence='NOT_ESTABLISHED',confidence='NOT_CALIBRATED'), 'no derived certainty')
    need(record['id']==_id(record), 'deterministic cause/version id')
    return record


def expected_ref(record):
    validate(record,record['origin'])
    codec=object.__new__(LifeObjects);codec.identity=record['origin']['identity'];codec.instance_id=record['origin']['instance_id']
    return codec._frame(KIND,_json(record,MAX_BYTES),'json')[1]


def make_record(state, decision, item, capture_ref, workflow_ref, receipt_seq, previous_head, resolve):
    """Pure same-RECEIPT derivation; no current journal/head or staged pointer read."""
    need(state['config'].get('derived_observation_profile')==PROFILE and observer.SCHEMA==OBSERVER, 'explicit supported birth/observer')
    if decision.get('inventory_acquisition') is None:
        return None
    root=state['native_acquisition'];acquisition.validate(root)
    need(root['phase']=='LIST_UNKNOWN' and root['list_request_ref']==state['pending']['decision_ref']
         and decision['inventory_acquisition']==acquisition.tag(root), 'actual pending acquisition')
    origin=_origin(state);q=decision['request'];source=decision['source']
    need(all(source[k]==v for k,v in origin.items()) and q['scope_ref']==state['scope_ref']
         and q['operation']=='LIST_SCOPE' and receipt_seq==source['global_seq']+3, 'new LIST source/sequence')
    actual=acquisition._read(state,state['pending']['decision_ref'],'compact_decision',resolve)
    need(_encode(actual)==_encode(decision), 'committed new decision object')
    actual=acquisition._read(state,capture_ref,'compact_capture',resolve)
    validate_capture(item)
    need(_encode(actual)==_encode(item) and item['request']==q
         and item['capture']['capture_origin']==state['config']['capture_origin'], 'actual new LIST capture')
    acquisition._cl(decision,q['scope_ref'])
    failed,_,frozen=acquisition.verify_cause(state,root,resolve)
    cause=root['cause']
    need(source['inventory_generation']==cause['inventory_generation']
         and source['list_receipt_ref']==cause['list_capture_ref']
         and cause['receipt_seq']<source['global_seq']+1, 'cause before new LIST')
    old_witness=[w for w in passive.validate(state['native_order_evidence'])['lists']
                 if w['capture_ref']==cause['list_capture_ref']]
    need(len(old_witness)==1, 'original retained LIST witness')
    old=passive._list(old_witness[0],state,resolve)
    post=acquisition._read(state,workflow_ref,'compact_workflow',resolve)
    need(post['goal']==frozen['goal'] and post['chains']==frozen['chains']
         and _hash(state['semantic_queue'])==cause['queue_sha256'], 'unchanged goal/queue')
    if item['capture']['result']['status']!='OK' or not old['complete']:
        return None
    new=item['capture']['result']['payload']
    if not new['complete']:
        return None
    matches=[e for e in new['entries'] if e['relative_path']==cause['path']]
    need(len(matches)<=1, 'unique observed path')
    entry=matches[0] if matches else None
    ref=entry['entry_ref'] if entry and entry['kind']=='file' else None
    relation=('PATH_NOT_LISTED' if entry is None else 'PATH_PRESENT_NON_FILE' if entry['kind']!='file'
              else 'SAME_REFERENCE' if ref==cause['failed_entry_ref'] else 'DIFFERENT_REFERENCE')
    record=dict(schema=SCHEMA,derivation=DERIVATION,observer_schema=OBSERVER,cl_verifier_version=VERSION,
        runtime_schema=state['schema'],reducer_version='compact-continuation-reducer-6',policy_version=state['config']['policy_version'],
        source_kind='internal_inference',origin=origin,cause_id=cause['cause_id'],reference_relation=relation,
        path=cause['path'],old_entry_ref=cause['failed_entry_ref'],new_entry_ref=ref,new_entry_kind=None if entry is None else entry['kind'],
        provenance=dict(failed_decision_ref=copy.deepcopy(cause['failed_decision_ref']),failed_capture_ref=copy.deepcopy(cause['failed_capture_ref']),
            old_list_decision_ref=copy.deepcopy(old_witness[0]['decision_ref']),old_list_capture_ref=copy.deepcopy(cause['list_capture_ref']),
            old_list_receipt_seq=old_witness[0]['receipt_seq'],failed_receipt_seq=cause['receipt_seq'],old_inventory_generation=cause['inventory_generation'],
            new_list_decision_ref=copy.deepcopy(state['pending']['decision_ref']),new_list_capture_ref=copy.deepcopy(capture_ref),
            new_list_receipt_seq=receipt_seq,new_inventory_generation=cause['inventory_generation']+1,
            cause_workflow_ref=copy.deepcopy(cause['post_workflow_ref']),new_workflow_ref=copy.deepcopy(workflow_ref)),
        commit=dict(effect_id=state['pending']['effect_id'],anchor_seq=receipt_seq-1,anchor_head=previous_head,receipt_seq=receipt_seq),
        uncertainty=dict(content_change='UNKNOWN',future_validity='UNKNOWN',independence='NOT_ESTABLISHED',confidence='NOT_CALIBRATED'),
        authority='NONE',evidence_role='DERIVED_NOT_INDEPENDENT_EXPERIENCE')
    record['id']=_id(record)
    validate(record,origin)
    return record


def render(record):
    validate(record,record['origin'])
    return f"Derived observation: {record['path']} {record['reference_relation']}; content/future validity UNKNOWN; no authority."
