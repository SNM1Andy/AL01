"""Component-only external semantic index for an explicit untrusted data format.

No TXT execution, truth adoption, filesystem access or action dispatch. The
caller must retain original capture objects and validate path-to-LIST bindings.
The entire bounded index is intended for an immutable object, not the hot head.
"""
import copy,hashlib,json
from .folder_contracts import validate_request,validate_result,validate_ref,validate_relative_path,validate_entry
from .folder_session import _hash,_encode
from .life_objects import SCHEMA as OBJECT_SCHEMA

SCHEMA='compact-semantics-1'
PARSER='al01_claim_v1'
MAX_TOPICS=8
MAX_RECORDS=32
MAX_INDEX_BYTES=65536
MAX_DECLARATION_BYTES=4096
MAX_QUERY_BYTES=131072


def need(ok,message):
    if not ok:raise ValueError('COMPACT_SEMANTICS_INVALID: '+message)


def text(value):
    need(type(value) is str and 0<len(value.encode('utf8'))<=256,'bounded scalar text')


def digest(value,size=64):
    need(type(value) is str and len(value)==size and all(c in '0123456789abcdef' for c in value),'digest')


def initial_semantics(identity,instance_id,scope_ref):
    text(identity);need(len(identity.encode('utf8'))<=128,'object identity bound');digest(instance_id,32);validate_ref(scope_ref,'scope')
    return dict(schema=SCHEMA,identity=identity,instance_id=instance_id,scope_ref=scope_ref,records=[],investigations={},overflow=None)


def _topic(subject,predicate):return _hash([subject,predicate])


def capture_reference(item,identity,instance_id):
    """Reproduce the existing immutable compact_capture frame; no I/O or grant."""
    payload=_encode(item)
    header=dict(schema=OBJECT_SCHEMA,identity=identity,instance_id=instance_id,kind='compact_capture',encoding='json',payload_bytes=len(payload),payload_sha256=hashlib.sha256(payload).hexdigest())
    raw=_encode(header)+b'\n'+payload
    return dict(schema=OBJECT_SCHEMA,identity=identity,instance_id=instance_id,kind='compact_capture',encoding='json',payload_bytes=len(payload),sha256=hashlib.sha256(raw).hexdigest(),byte_size=len(raw))


def _claim(claim):
    need(type(claim) is dict and set(claim)=={'subject','predicate','value','context'},'declaration fields')
    for key in ('subject','predicate','value'):text(claim[key])
    if claim['context'] is not None:text(claim['context'])


def validate_semantics(state):
    need(type(state) is dict and set(state)=={'schema','identity','instance_id','scope_ref','records','investigations','overflow'} and state['schema']==SCHEMA,'index fields')
    text(state['identity']);need(len(state['identity'].encode('utf8'))<=128,'object identity bound');digest(state['instance_id'],32);validate_ref(state['scope_ref'],'scope')
    records=state['records'];need(type(records) is list and len(records)<=MAX_RECORDS,'record capacity')
    topics=set();ids=set();requests=set();last_seq=0
    for r in records:
        need(type(r) is dict and set(r)=={'schema','record_id','identity','instance_id','scope_ref','topic','claim','claim_sha256','path','receipt_seq','request_id','capture_ref','receipt_sha256','content_sha256','byte_span','source_kind','capture_origin','parser','confidence_status','derived_from'},'record fields')
        need(r['schema']==SCHEMA and r['identity']==state['identity'] and r['instance_id']==state['instance_id'] and r['scope_ref']==state['scope_ref'],'record identity')
        _claim(r['claim']);need(r['claim_sha256']==_hash(r['claim']) and r['topic']==_topic(r['claim']['subject'],r['claim']['predicate']),'claim/topic binding')
        validate_relative_path(r['path']);validate_ref(r['request_id'],'req')
        need(type(r['receipt_seq']) is int and last_seq<r['receipt_seq']<=2**53-1,'global receipt order');last_seq=r['receipt_seq']
        need(type(r['byte_span']) is list and len(r['byte_span'])==2 and type(r['byte_span'][0]) is int and r['byte_span'][0]==0 and type(r['byte_span'][1]) is int and 1<=r['byte_span'][1]<=MAX_DECLARATION_BYTES,'whole-file UTF8 span')
        need(r['source_kind']=='external_claim_in_captured_text' and r['confidence_status']=='UNVERIFIED' and r['parser']==PARSER and r['derived_from']==[],'passive source semantics')
        need(r['capture_origin'] in ('SYNTHETIC_TEST_CAPTURE','APPLICATION_BROKER_CAPTURE'),'origin')
        for k in ('receipt_sha256','content_sha256','record_id'):digest(r[k])
        need(r['record_id']==_hash({k:v for k,v in r.items() if k!='record_id'}),'record hash')
        ref=r['capture_ref'];need(type(ref) is dict and set(ref)=={'schema','identity','instance_id','kind','encoding','payload_bytes','sha256','byte_size'},'capture reference shape')
        need(ref['schema']==OBJECT_SCHEMA and ref['identity']==state['identity'] and ref['instance_id']==state['instance_id'] and ref['kind']=='compact_capture' and ref['encoding']=='json','capture reference namespace')
        digest(ref['sha256']);need(type(ref['payload_bytes']) is int and type(ref['byte_size']) is int and 0<ref['payload_bytes']<ref['byte_size']<=400000,'capture reference byte bounds')
        need(r['record_id'] not in ids and r['request_id'] not in requests,'duplicate source record');ids.add(r['record_id']);requests.add(r['request_id']);topics.add(r['topic'])
    need(len(topics)<=MAX_TOPICS,'topic capacity')
    inv=state['investigations'];need(type(inv) is dict and set(inv)<=topics,'investigation topics')
    for key,value in inv.items():
        need(type(value) is dict and set(value)=={'epoch','rounds','evidence_head'} and type(value['rounds']) is int and 0<=value['rounds']<=2,'two-round bound');digest(value['epoch']);digest(value['evidence_head'])
    overflow=state['overflow']
    if overflow is not None:
        need(type(overflow) is dict and set(overflow)=={'first_seq','receipt_sha256'} and type(overflow['first_seq']) is int and 1<=overflow['first_seq']<=2**53-1,'overflow marker');digest(overflow['receipt_sha256'])
    need(len(_encode(state))<=MAX_INDEX_BYTES,'index byte bound')
    return copy.deepcopy(state)


def _pairs(pairs):
    out={}
    for key,value in pairs:
        need(key not in out,'duplicate JSON key');out[key]=value
    return out


def apply_capture(state,item,capture_ref,path,receipt_seq):
    state=validate_semantics(state);validate_relative_path(path)
    need(type(receipt_seq) is int and 1<=receipt_seq<=2**53-1,'receipt sequence')
    need(type(item) is dict and set(item)=={'request','capture','receipt_sha256'},'capture item')
    q=item['request'];c=item['capture'];validate_request(q)
    need(q['scope_ref']==state['scope_ref'],'capture scope')
    need(type(c) is dict and set(c)=={'schema','capture_origin','result'} and c['schema']=='folder-session-1','capture envelope')
    need(c['capture_origin'] in ('SYNTHETIC_TEST_CAPTURE','APPLICATION_BROKER_CAPTURE'),'capture origin')
    validate_result(c['result'],q);need(item['receipt_sha256']==_hash(c),'receipt integrity')
    need(capture_ref==capture_reference(item,state['identity'],state['instance_id']),'exact immutable capture frame binding')
    result=c['result']
    if q['operation']!='READ_TEXT' or result['status']!='OK':return state,dict(status='NO_SUCCESSFUL_TEXT',record=None)
    raw=result['payload']['text'].encode('utf8')
    if len(raw)>MAX_DECLARATION_BYTES:return state,dict(status='UNPARSED_BYTE_LIMIT',record=None)
    try:
        value=json.loads(raw,object_pairs_hook=_pairs,parse_constant=lambda x:(_ for _ in ()).throw(ValueError('nonfinite JSON')))
        need(type(value) is dict and set(value)=={PARSER},'declared data envelope');claim=value[PARSER];_claim(claim)
    except (ValueError,TypeError,UnicodeError,RecursionError):return state,dict(status='UNPARSED_EXTERNAL_TEXT',record=None)
    record=dict(schema=SCHEMA,identity=state['identity'],instance_id=state['instance_id'],scope_ref=state['scope_ref'],topic=_topic(claim['subject'],claim['predicate']),claim=claim,claim_sha256=_hash(claim),path=path,receipt_seq=receipt_seq,request_id=q['request_id'],capture_ref=copy.deepcopy(capture_ref),receipt_sha256=item['receipt_sha256'],content_sha256=result['payload']['content_sha256'],byte_span=[0,len(raw)],source_kind='external_claim_in_captured_text',capture_origin=c['capture_origin'],parser=PARSER,confidence_status='UNVERIFIED',derived_from=[])
    record['record_id']=_hash(record)
    for prior in state['records']:
        if prior['request_id']==q['request_id']:
            need(prior==record,'conflicting repeated receipt');return state,dict(status='DUPLICATE',record=copy.deepcopy(prior))
    need(not state['records'] or receipt_seq>state['records'][-1]['receipt_seq'],'new receipt must follow global order')
    # This index is not an exhaustive episode ledger. The outer journal must
    # retain every current capture even when its semantic source is coalesced.
    duplicate_fields=('identity','instance_id','scope_ref','path','content_sha256',
                      'claim_sha256','claim','capture_origin','parser','byte_span')
    for prior in state['records']:
        if all(prior[k]==record[k] for k in duplicate_fields):
            return state,dict(status='DUPLICATE_OBSERVATION',record=copy.deepcopy(prior),
                first_record_id=prior['record_id'],first_receipt_sha256=prior['receipt_sha256'],
                capture_ref=copy.deepcopy(capture_ref),receipt_sha256=item['receipt_sha256'],
                request_id=q['request_id'],receipt_seq=receipt_seq,semantic_progress=False,
                obligation='PRESERVE_CURRENT_CAPTURE_AND_RECEIPT_IN_OUTER_JOURNAL')
    topics={r['topic'] for r in state['records']}
    out=copy.deepcopy(state);out['records'].append(record)
    if len(out['records'])>MAX_RECORDS or (record['topic'] not in topics and len(topics)>=MAX_TOPICS) or len(_encode(out))>MAX_INDEX_BYTES-256:
        state['overflow']=state['overflow'] or {'first_seq':receipt_seq,'receipt_sha256':item['receipt_sha256']}
        return validate_semantics(state),dict(status='INDEX_CAPACITY',record=None,capture_ref=copy.deepcopy(capture_ref),receipt_seq=receipt_seq,
                          obligation='PRESERVE_CAPTURE_AND_MARK_QUERY_INCOMPLETE_OR_BLOCK_SEMANTIC_WORK')
    return validate_semantics(out),dict(status='INDEXED_UNVERIFIED',record=copy.deepcopy(record))


def verify_record(record,item):
    """Resolve original externally, then recompute exact span/parser/ref binding."""
    need(type(record) is dict and {'identity','instance_id','scope_ref','capture_ref','path','receipt_seq'}<=set(record),'record source fields')
    state=initial_semantics(record['identity'],record['instance_id'],record['scope_ref'])
    checked=copy.deepcopy(state);checked['records']=[record];validate_semantics(checked)
    _,result=apply_capture(state,item,record['capture_ref'],record['path'],record['receipt_seq'])
    need(result.get('record')==record,'record differs from original captured declaration')
    return copy.deepcopy(record)


def query(state,subject,predicate,*,context=None,complete=True):
    state=validate_semantics(state);text(subject);text(predicate)
    if context is not None:text(context)
    need(type(complete) is bool,'completeness flag')
    complete=complete and state['overflow'] is None
    rows=[r for r in state['records'] if r['topic']==_topic(subject,predicate)]
    scoped=[r for r in rows if context is None or r['claim']['context']==context]
    groups={}
    for r in scoped:
        groups.setdefault(r['claim_sha256'],dict(claim=copy.deepcopy(r['claim']),records=[],content_hashes=set()))['records'].append(r['record_id'])
        groups[r['claim_sha256']]['content_hashes'].add(r['content_sha256'])
    alternatives=[dict(claim_hash=h,claim=v['claim'],record_ids=v['records'],distinct_content_count=len(v['content_hashes']),independent_support_count=None) for h,v in sorted(groups.items())]
    relations=[];relation_count=0;assessments=set()
    for i,a in enumerate(scoped):
        for b in scoped[i+1:]:
            if a['claim']['value']==b['claim']['value']:continue
            ca,cb=a['claim']['context'],b['claim']['context']
            assessment='DECLARED_CONTEXT_CONFLICT' if ca is not None and ca==cb else 'CONTEXT_UNKNOWN' if ca is None or cb is None else 'DIFFERENT_DECLARED_CONTEXTS'
            relation_count+=1;assessments.add(assessment)
            if len(relations)<32:relations.append(dict(first=a['record_id'],second=b['record_id'],assessment=assessment))
    if not complete:status='INCOMPLETE_UNKNOWN'
    elif not scoped:status='NOT_RECORDED_IN_INDEX'
    elif 'DECLARED_CONTEXT_CONFLICT' in assessments:status='UNRESOLVED_CONFLICT'
    elif relations:status='UNRESOLVED_CONTEXT'
    else:status='UNVERIFIED_CLAIM_AVAILABLE'
    result=dict(status=status,topic=_topic(subject,predicate),context=context,complete=complete,alternatives=alternatives,relations=relations,
                evidence=[{k:copy.deepcopy(r[k]) for k in ('record_id','capture_ref','receipt_seq','receipt_sha256','content_sha256','byte_span','path','capture_origin')} for r in scoped],
                selected_claim=None,truth_adopted=False,resolution='NO_AUTOMATIC_FACT_RESOLUTION',relation_count=relation_count,relations_complete=relation_count==len(relations))
    need(len(_encode(result))<=MAX_QUERY_BYTES,'query byte budget');return result


def request_investigation(state,subject,predicate,current_inventory):
    state=validate_semantics(state);report=query(state,subject,predicate);topic=report['topic']
    need(type(current_inventory) is list and len(current_inventory)<=32,'bounded inventory')
    paths={};refs=set();folded_paths=set()
    for e in current_inventory:
        validate_entry(e)
        folded=e['relative_path'].casefold()
        need(folded not in folded_paths and e['entry_ref'] not in refs,'ambiguous inventory')
        paths[e['relative_path']]=e;refs.add(e['entry_ref']);folded_paths.add(folded)
    if not report['complete']:return state,dict(status='UNRESOLVED_INDEX_INCOMPLETE',proposals=[],authority='NONE')
    if report['status'] not in ('UNRESOLVED_CONFLICT','UNRESOLVED_CONTEXT'):return state,dict(status='NO_UNRESOLVED_DISPUTE',proposals=[],authority='NONE')
    epoch=_hash(sorted(a['claim_hash'] for a in report['alternatives']))
    prior=state['investigations'].get(topic);rounds=prior['rounds'] if prior and prior['epoch']==epoch else 0
    evidence_head=_hash(sorted({(r['path'],r['claim_sha256'],r['capture_origin']) for r in state['records'] if r['topic']==topic}))
    if rounds>=2:return state,dict(status='UNRESOLVED_ROUND_LIMIT',proposals=[],authority='NONE')
    if prior and prior['epoch']==epoch and prior['evidence_head']==evidence_head:
        return state,dict(status='UNRESOLVED_NO_NEW_EVIDENCE',proposals=[],authority='NONE')
    eligible=sorted({r['path'] for r in state['records'] if r['topic']==topic and r['path'] in paths and paths[r['path']]['kind']=='file'})
    if not eligible:return state,dict(status='UNRESOLVED_NO_VISIBLE_SOURCE',proposals=[],authority='NONE')
    out=copy.deepcopy(state);out['investigations'][topic]={'epoch':epoch,'rounds':rounds+1,'evidence_head':evidence_head}
    proposals=[dict(operation='READ_TEXT',entry_ref=paths[p]['entry_ref'],path=p,topic=topic,evidence_record_ids=[r['record_id'] for r in state['records'] if r['topic']==topic and r['path']==p],round=rounds+1,intent_key=_hash([topic,epoch,p,rounds+1]),requires='CURRENT_INVENTORY_CVM_GOVERNANCE_AND_DURABLE_APPROVAL') for p in eligible[:2]]
    return validate_semantics(out),dict(status='PROPOSALS_ONLY',proposals=proposals,authority='NONE')


def assess_evidence(state,subject,predicate,*,context=None,complete=True):
    """Read-only declaration relations; no truth, independence or action grant."""
    state=validate_semantics(state)
    base=query(state,subject,predicate,context=context,complete=complete)
    rows=[r for r in state['records'] if r['topic']==base['topic'] and (context is None or r['claim']['context']==context)]
    kinds=('COMPATIBLE_DECLARATIONS','DECLARED_CONTEXT_CONFLICT','DIFFERENT_DECLARED_CONTEXTS','CONTEXT_UNKNOWN')
    counts=dict.fromkeys(kinds,0);pairs=[]
    for i,a in enumerate(rows):
        for b in rows[i+1:]:
            ca,cb=a['claim']['context'],b['claim']['context']
            kind=('CONTEXT_UNKNOWN' if ca is None or cb is None else
                  'DIFFERENT_DECLARED_CONTEXTS' if ca!=cb else
                  'COMPATIBLE_DECLARATIONS' if a['claim']['value']==b['claim']['value'] else
                  'DECLARED_CONTEXT_CONFLICT')
            counts[kind]+=1
            if len(pairs)<32:pairs.append(dict(first=a['record_id'],second=b['record_id'],assessment=kind))
    if not base['complete']:action,reason='INSUFFICIENT_EVIDENCE','INDEX_INCOMPLETE'
    elif counts['DECLARED_CONTEXT_CONFLICT']:action,reason='INSPECT_ORIGINALS','SAME_DECLARED_CONTEXT_DIFFERENT_VALUES'
    elif counts['CONTEXT_UNKNOWN'] or counts['DIFFERENT_DECLARED_CONTEXTS']:action,reason='CLARIFY_CONTEXT','APPLICABILITY_NOT_ESTABLISHED'
    elif not pairs:action,reason='INSUFFICIENT_EVIDENCE','FEWER_THAN_TWO_RECORDS'
    else:action,reason='NO_DISCREPANCY_REREAD','COMPATIBLE_DECLARATIONS_NOT_VERIFIED_TRUTH'
    total=sum(counts.values())
    result=dict(schema='semantic-assessment-1',topic=base['topic'],context=context,complete=base['complete'],
        pair_counts=counts,pairs=pairs,pair_count=total,pairs_complete=total==len(pairs),evidence=base['evidence'],
        truth_adopted=False,independent_support_count=None,temporal_change_established=False,
        recommendation=dict(action=action,reason=reason,authority='NONE',executed=False))
    need(len(_encode(result))<=MAX_QUERY_BYTES,'assessment byte budget')
    return result
