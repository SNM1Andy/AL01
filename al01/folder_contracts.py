"""Pure future folder-adapter contracts. NO filesystem access or OS enforcement.

Validation cannot establish confinement, resolve handles, inspect symlinks/reparse
points, or prove access rights. The required OS sandbox is presently unavailable;
an adapter must report UNAVAILABLE, not read under a weaker boundary. Folder text
is untrusted data, never CL, instructions, code, or authorization.
"""
import hashlib
import json
import re
import unicodedata

SCHEMA='bounded-folder-contract-1'
MAX_TEXT_BYTES=65536
MAX_ENTRIES=32
MAX_DEPTH=2
MAX_PATH_BYTES=512
OPERATIONS=frozenset({'LIST_SCOPE','READ_TEXT','CHECK_ACCESS'})
INTENTS=frozenset({'list','read_text','write','delete','execute','network','export_state','leave_scope'})
FORBIDDEN_INTENTS=INTENTS-{'list','read_text'}
STATUSES=frozenset({'OK','DENIED','UNAVAILABLE','NOT_FOUND','INVALID_TEXT','LIMIT_EXCEEDED','STALE_REF','IO_ERROR'})
REASONS={
    'DENIED':frozenset({'ACCESS_DENIED','OUTSIDE_SCOPE','UNSAFE_ENTRY','UNSUPPORTED_OPERATION','POLICY_ACTION_FORBIDDEN'}),
    'UNAVAILABLE':frozenset({'SANDBOX_UNAVAILABLE','ADAPTER_UNAVAILABLE','SCOPE_UNAVAILABLE'}),
    'NOT_FOUND':frozenset({'ENTRY_NOT_FOUND'}),
    'INVALID_TEXT':frozenset({'NOT_STRICT_UTF8','BINARY_CONTENT'}),
    'LIMIT_EXCEEDED':frozenset({'BYTE_LIMIT','ENTRY_LIMIT','DEPTH_LIMIT','PATH_LIMIT'}),
    'STALE_REF':frozenset({'REFERENCE_EXPIRED','ENTRY_CHANGED'}),
    'IO_ERROR':frozenset({'READ_FAILED','LIST_FAILED','ACCESS_CHECK_FAILED'}),
}
LIMITS={'max_text_bytes':MAX_TEXT_BYTES,'max_entries':MAX_ENTRIES,'max_depth':MAX_DEPTH}


class FolderContractError(ValueError):
    """Invalid values only; never presented as OS access-denial evidence."""
    def __init__(self,detail):
        self.code='FOLDER_CONTRACT_INVALID'
        super().__init__(detail)


def _require(ok,detail):
    if not ok:raise FolderContractError(detail)


def _keys(value,keys,label):
    _require(type(value) is dict and set(value)==set(keys),label+' fields')


def _utf8(value,limit,label):
    _require(type(value) is str,label+' must be text')
    # Every Unicode scalar requires at least one UTF-8 byte, allowing an early cap.
    _require(len(value)<=limit,label+' length limit')
    try:raw=value.encode('utf8',errors='strict')
    except UnicodeError as exc:raise FolderContractError(label+' is not strict UTF-8') from exc
    _require(len(raw)<=limit,label+' byte limit')
    return raw


def validate_ref(value,kind):
    _require(kind in ('scope','entry','req'),'reference kind')
    _require(type(value) is str and re.fullmatch(kind+r'_[0-9a-f]{32}',value) is not None,'opaque '+kind+' reference required')


def validate_relative_path(path):
    """Conservative root-relative lexical policy; NOT a path-resolution sandbox.

    Empty/root aliases, traversal, Windows reserved names/ADS/UNC/device syntax,
    control characters and ambiguous compatibility-normalized names are rejected.
    Ordinary NFC Unicode names are supported. No decoding or normalization occurs.
    """
    _utf8(path,MAX_PATH_BYTES,'relative path')
    _require(bool(path) and not path.startswith('/') and '\\' not in path,'relative slash-separated path required')
    _require(not any(c in path for c in ':*?"<>|%'),'device/ADS/wildcard/encoded path forbidden')
    _require(not any(unicodedata.category(c).startswith('C') for c in path),'path controls forbidden')
    _require(unicodedata.normalize('NFKC',path)==path,'ambiguous normalized path forbidden')
    parts=path.split('/');_require(1<=len(parts)<=MAX_DEPTH,'root-relative depth limit')
    for part in parts:
        _require(part not in ('','.','..') and part==part.strip() and not part.endswith(('.', ' ')),'ambiguous path component')
        _utf8(part,255,'path component')
        stem=part.split('.')[0].upper()
        _require(stem not in {'CON','PRN','AUX','NUL','CONIN$','CONOUT$','CLOCK$'}
                 and re.fullmatch(r'(COM|LPT)[1-9]',stem) is None,'Windows reserved component')
    return path


def validate_request(request):
    _require(type(request) is dict,'request object')
    keys={'schema','request_id','operation','scope_ref','entry_ref','limits'}
    if request.get('operation')=='CHECK_ACCESS':keys.add('intent')
    _keys(request,keys,'request')
    _require(request['schema']==SCHEMA,'request schema')
    validate_ref(request['request_id'],'req');validate_ref(request['scope_ref'],'scope')
    _require(type(request['operation']) is str and request['operation'] in OPERATIONS,'unsupported operation: no write/delete/exec/network/export')
    if request['entry_ref'] is not None:validate_ref(request['entry_ref'],'entry')
    _require(request['operation']!='READ_TEXT' or request['entry_ref'] is not None,'READ_TEXT requires opaque entry ref')
    if request['operation']=='CHECK_ACCESS':
        _require(type(request['intent']) is str and request['intent'] in INTENTS,'explicit access intent')
    _keys(request['limits'],LIMITS,'request limits')
    _require(all(type(request['limits'][key]) is int and request['limits'][key]==value for key,value in LIMITS.items()),'fixed safe budgets')


def make_request(operation,scope_ref,*,request_id,entry_ref=None,intent=None):
    request={'schema':SCHEMA,'request_id':request_id,'operation':operation,'scope_ref':scope_ref,
             'entry_ref':entry_ref,'limits':dict(LIMITS)}
    if operation=='CHECK_ACCESS':request['intent']=intent
    else:_require(intent is None,'intent only valid for CHECK_ACCESS')
    validate_request(request);return request


def request_sha256(request):
    validate_request(request)
    return hashlib.sha256(json.dumps(request,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf8')).hexdigest()


def validate_entry(entry):
    _keys(entry,{'entry_ref','relative_path','kind','byte_size'},'entry')
    validate_ref(entry['entry_ref'],'entry');validate_relative_path(entry['relative_path'])
    _require(type(entry['kind']) is str and entry['kind'] in ('file','directory'),'symlink/device/reparse/unknown kinds forbidden')
    if entry['kind']=='directory':_require(entry['byte_size'] is None,'directory byte size must be unknown/null')
    else:
        _require(entry['relative_path'].lower().endswith('.txt'),'file scope is .txt only, case-insensitive suffix')
        _require(type(entry['byte_size']) is int and 0<=entry['byte_size']<=MAX_TEXT_BYTES,'file metadata exceeds bounded read scope')


def text_payload(raw):
    """Strict UTF-8 decoding of already supplied bytes, never reads a file."""
    _require(type(raw) is bytes and len(raw)<=MAX_TEXT_BYTES,'bounded bytes required')
    try:text=raw.decode('utf8',errors='strict')
    except UnicodeError as exc:raise FolderContractError('NOT_STRICT_UTF8') from exc
    _require('\x00' not in text,'binary NUL content forbidden')
    return {'text':text,'utf8_bytes':len(raw),'content_sha256':hashlib.sha256(raw).hexdigest(),'untrusted_data':True}


def validate_result(result,request):
    validate_request(request)
    _keys(result,{'schema','request_id','request_sha256','operation','scope_ref','entry_ref','status','reason','payload'},'result')
    _require(result['schema']==SCHEMA and result['request_id']==request['request_id']
        and result['request_sha256']==request_sha256(request) and result['operation']==request['operation']
        and result['scope_ref']==request['scope_ref'] and result['entry_ref']==request['entry_ref'],'result/request binding')
    status=result['status'];_require(type(status) is str and status in STATUSES,'explicit result status')
    if status!='OK':
        _require(result['payload'] is None and type(result['reason']) is str and result['reason'] in REASONS[status],'failure has typed reason and no data')
        return
    _require(result['reason'] is None,'success has no failure reason');payload=result['payload']
    _require(request['operation']!='CHECK_ACCESS' or request['intent'] not in FORBIDDEN_INTENTS,'forbidden intent cannot succeed')
    if request['operation']=='LIST_SCOPE':
        _keys(payload,{'entries','complete','untrusted_data'},'listing')
        _require(payload['complete'] is True and payload['untrusted_data'] is True,'no silent truncated listing; untrusted metadata')
        entries=payload['entries'];_require(type(entries) is list and len(entries)<=MAX_ENTRIES,'listing entry limit')
        refs=set();names=set()
        for entry in entries:
            validate_entry(entry);ref=entry['entry_ref'];name=entry['relative_path'].casefold()
            _require(ref not in refs and name not in names,'duplicate/Windows case-colliding listing')
            refs.add(ref);names.add(name)
    elif request['operation']=='READ_TEXT':
        _keys(payload,{'text','utf8_bytes','content_sha256','untrusted_data'},'text')
        raw=_utf8(payload['text'],MAX_TEXT_BYTES,'file text')
        _require('\x00' not in payload['text'],'binary NUL content forbidden')
        _require(type(payload['utf8_bytes']) is int and payload['utf8_bytes']==len(raw) and payload['untrusted_data'] is True,'text metadata')
        _require(payload['content_sha256']==hashlib.sha256(raw).hexdigest(),'captured text integrity')
    else:
        _keys(payload,{'readable','listable','untrusted_data'},'access result')
        _require(type(payload['readable']) is bool and type(payload['listable']) is bool and payload['untrusted_data'] is True,'access flags are advisory only')


def make_result(request,status,*,payload=None,reason=None):
    validate_request(request)
    result={key:request[key] for key in ('schema','request_id','operation','scope_ref','entry_ref')}
    result.update(request_sha256=request_sha256(request),status=status,reason=reason,payload=payload)
    # Validate original types/budgets FIRST, so tuples/coercions or giant invalid
    # nested objects cannot sneak through JSON conversion or consume extra work.
    validate_result(result,request)
    return json.loads(json.dumps(result,ensure_ascii=False,allow_nan=False))


def policy_denial(request):
    """Pure policy denial, with NO existence/access probe and no kernel claim.

    None means this policy did not reject the intent, not OS authorization or an
    available sandbox. The adapter must separately establish the required boundary.
    """
    validate_request(request)
    if request['operation']=='CHECK_ACCESS' and request['intent'] in FORBIDDEN_INTENTS:
        return make_result(request,'DENIED',reason='POLICY_ACTION_FORBIDDEN')
    return None
