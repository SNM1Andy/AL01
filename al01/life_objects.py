"""Immutable controller payloads; integrity, not hostile-host authentication.

Publish before journaling a reference. No atomic transaction across the object
store and LifeJournal is implied. Failed writes can consume reserved capacity;
orphan objects/staging files remain inert and are never automatically deleted.
A bounded publication lease can hold the existing exclusive store lock across
an external effect and its result publications, preventing supported writers
from consuming the admitted envelope. It does not precharge or refund capacity.
Normal publication reads a bounded accounting head and one addressed object,
not the growing inventory. Startup streams a bounded inventory audit.

Profile:8MiB payload,<=1025B framing overhead, depth24/300000 JSON nodes,
Default4096 lifetime reservations and2GiB reserved disk bytes. Each reservation
permanently charges two physical frames (staging plus final worst case), even
after staging unlink. Thus useful capacity is conservative, not infinite.
Interrupted link-before-unlink leaves same-inode aliases: singly-linked path
checks fail closed and manual diagnosis is required, without automatic deletion.
JSON kinds are caller-declared typed tags, not domain schema validation; callers
must validate their payload before put and after read. No Windows power-loss
durability, authentication, migration or existing-runtime integration is claimed.
"""
import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from contextlib import contextmanager
from threading import Lock
from .controller_paths import check_directory, check_file

SCHEMA = 'life-object-1'
MAX_PAYLOAD = 8 * 1024 * 1024
MAX_HEADER = 1024
MAX_FILE = MAX_PAYLOAD + MAX_HEADER + 1
MAX_RESERVATIONS = 4096
MAX_RESERVED_BYTES = 2 * 1024 * 1024 * 1024
MAX_NODES = 300000
MAX_DEPTH = 24


class LifeObjectError(ValueError):
    code = 'LIFE_OBJECT_INVALID'


def need(ok, detail):
    if not ok: raise LifeObjectError(detail)


@dataclass(frozen=True)
class ObjectProfile:
    """Frozen lifetime quotas, not payload limits or a promise of endurance.

    Caller must supply the same profile on reopen. Default reserved bytes allow
    1000 objects averaging about1MiB per frame (two copies charged), not1000
    worst-case8MiB payloads. Failed reservations permanently consume this budget.
    """
    max_reservations: int = MAX_RESERVATIONS
    max_reserved_bytes: int = MAX_RESERVED_BYTES

    def __post_init__(self):
        need(type(self.max_reservations) is int and 1<=self.max_reservations<=8192,'profile slot bound1..8192')
        need(type(self.max_reserved_bytes) is int and 1<=self.max_reserved_bytes<=16*1024**3,'profile byte bound1..16GiB')


DEFAULT_PROFILE = ObjectProfile()


def _digest(raw): return hashlib.sha256(raw).hexdigest()


def _json(value, limit=MAX_PAYLOAD):
    nodes=0; text_bytes=0
    def visit(node, depth=0):
        nonlocal nodes,text_bytes
        nodes+=1; need(nodes<=MAX_NODES and depth<=MAX_DEPTH,'JSON structural budget')
        if type(node) is dict:
            for key, child in node.items():
                need(type(key) is str,'JSON string keys'); visit(key,depth+1); visit(child,depth+1)
        elif type(node) is list:
            for child in node: visit(child,depth+1)
        elif type(node) is str:
            need(len(node)<=limit,'string budget');text_bytes+=len(node.encode('utf8'))
            need(text_bytes<=limit,'text byte budget')
        elif type(node) is int: need(abs(node)<=2**53,'integer budget')
        else: need(node is None or type(node) in (bool,float),'JSON scalar type')
    try:
        visit(value)
        raw=json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf8')
    except (ValueError,TypeError,UnicodeError,RecursionError,OverflowError) as exc:
        raise LifeObjectError('invalid bounded JSON') from exc
    need(len(raw)<=limit,'JSON encoded byte budget');return raw


def _load(raw):
    def pairs(items):
        result={}
        for key,value in items:
            need(key not in result,'duplicate JSON key');result[key]=value
        return result
    try:return json.loads(raw.decode('utf8'),object_pairs_hook=pairs)
    except (ValueError,UnicodeError,RecursionError) as exc:raise LifeObjectError('malformed JSON') from exc


def _label(value): need(type(value) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}',value),'opaque identity/kind')


class _PublicationLease:
    """Bounded publication handle, valid only while its store lock is held.

    Admission reserves exclusive capacity, not accounting charges. Every call
    consumes this handle's conservative envelope, including duplicates and
    failures; actual durable store charges still occur only in _publish_locked.
    The guard serializes shared-handle calls and closes before the store unlocks.
    This is a supported-writer protocol, not confinement from hostile Python.
    """
    def __init__(self, store, max_objects, max_reserved_bytes):
        self._store=store;self._remaining_objects=max_objects
        self._remaining_bytes=max_reserved_bytes;self._active=True;self._guard=Lock()

    def _close(self):
        with self._guard:self._active=False

    def _put(self,kind,payload,encoding):
        with self._guard:
            need(self._active,'publication lease is closed')
            raw,ref=self._store._frame(kind,payload,encoding)
            cost=2*len(raw)
            need(self._remaining_objects>0 and cost<=self._remaining_bytes,'publication lease envelope exhausted')
            # Claim before any publication attempt; exceptions cannot refund a
            # possibly executed attempt or make the handle's envelope unbounded.
            self._remaining_objects-=1;self._remaining_bytes-=cost
            return self._store._publish_locked(raw,ref)

    def put_json(self,kind,value):
        return self._put(kind,_json(value),'json')

    def put_bytes(self,kind,value):
        need(type(value) is bytes and len(value)<=MAX_PAYLOAD,'bounded bytes required')
        return self._put(kind,value,'bytes')


class LifeObjects:
    def __init__(self,directory,*,identity,instance_id,profile=DEFAULT_PROFILE):
        _label(identity);_label(instance_id)
        need(type(profile) is ObjectProfile,'explicit frozen object profile')
        self.profile=ObjectProfile(profile.max_reservations,profile.max_reserved_bytes)
        self.identity=identity;self.instance_id=instance_id
        self.directory=Path(os.path.abspath(directory));check_directory(self.directory)
        self.head=self.directory/'objects.json';self.lock=self.directory/'objects.lock'
        if self.directory.exists():
            with os.scandir(self.directory) as entries:
                if next(entries,None) is not None:need(self.head.exists(),'missing store accounting; no reset')
        self.directory.mkdir(parents=True,exist_ok=True)
        with self._locked():
            if not self.head.exists():self._save_head(self._initial())
            self._refresh();self._audit()

    def _initial(self):
        return {'schema':SCHEMA,'identity':self.identity,'instance_id':self.instance_id,
                'reservations':0,'reserved_bytes':0,'max_payload':MAX_PAYLOAD,
                'max_reservations':self.profile.max_reservations,'max_reserved_bytes':self.profile.max_reserved_bytes}

    @contextmanager
    def _locked(self):
        check_directory(self.directory);check_file(self.lock)
        fd=os.open(self.lock,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        try:yield
        finally:os.close(fd);self.lock.unlink()

    @staticmethod
    def _sync(directory):
        if os.name=='posix':
            fd=os.open(directory,os.O_RDONLY|getattr(os,'O_DIRECTORY',0))
            try:os.fsync(fd)
            finally:os.close(fd)

    def _save_head(self,data):
        raw=_json({'data':data,'sha256':_digest(_json(data))},4096)
        path=self.directory/'.account-pending'
        check_file(path);need(not path.exists(),'unfinished accounting publication; manual diagnosis')
        check_file(self.head)
        with open(path,'xb') as stream:
            need(stream.write(raw)==len(raw),'short accounting write');stream.flush();os.fsync(stream.fileno())
        os.replace(path,self.head);self._sync(self.directory)

    def _refresh(self):
        check_file(self.head);need(self.head.is_file() and self.head.stat().st_size<=4096,'missing/oversize accounting')
        wrapper=_load(self.head.read_bytes())
        need(type(wrapper) is dict and set(wrapper)=={'data','sha256'} and wrapper['sha256']==_digest(_json(wrapper['data'])),'accounting hash')
        data=wrapper['data'];expected=self._initial()
        need(type(data) is dict and set(data)==set(expected),'accounting fields')
        need(all(data[k]==v for k,v in expected.items() if k not in ('reservations','reserved_bytes')),'identity/profile mismatch')
        n=data['reservations'];size=data['reserved_bytes']
        need(type(n) is int and 0<=n<=self.profile.max_reservations and type(size) is int and 0<=size<=self.profile.max_reserved_bytes,'accounting bounds')
        need((n==0)==(size==0),'accounting consistency')
        prior=getattr(self,'accounting',None)
        if prior is not None:
            need(n>=prior['reservations'] and size>=prior['reserved_bytes'],'accounting high-water regression')
            need((n==prior['reservations'])==(size==prior['reserved_bytes']),'reservation/byte transition mismatch')
        self.accounting=data

    def _audit(self):
        with os.scandir(self.directory) as entries:
            self._audit_entries(entries)

    def _audit_entries(self,entries):
        count=0;size=0
        for index,entry in enumerate(entries):
            need(index<2*self.profile.max_reservations+3,'directory entry budget')
            path=self.directory/entry.name
            if path.name in ('objects.json','objects.lock'):continue
            check_file(path);need(path.is_file(),'unexpected store directory')
            name=path.name
            if name=='.account-pending':
                need(False,'unfinished accounting publication; manual diagnosis')
            need(re.fullmatch(r'(obj-[0-9a-f]{64}\.bin|\.object-[0-9a-f]{32})',name),'unexpected store file')
            count+=1;size+=path.stat().st_size
            # A successful reservation creates one frame, either staging or final.
            # Interrupted link aliases are already rejected by check_file, so
            # two surviving singly-linked frames cannot share one reservation.
            # Missing failed frames remain charged; never infer refunds here.
            need(path.stat().st_size<=MAX_FILE and count<=self.accounting['reservations']
                 and 2*size<=self.accounting['reserved_bytes'],'physical inventory exceeds reservations')
            if name.startswith('obj-'):
                _,ref=self._read_file(path)
                need(name=='obj-'+ref['sha256']+'.bin','object filename/hash mismatch')

    def _frame(self,kind,payload,encoding):
        _label(kind);need(len(payload)<=MAX_PAYLOAD,'payload byte budget')
        header={'schema':SCHEMA,'identity':self.identity,'instance_id':self.instance_id,
                'kind':kind,'encoding':encoding,'payload_bytes':len(payload),'payload_sha256':_digest(payload)}
        raw=_json(header,MAX_HEADER)+b'\n'+payload
        ref={key:header[key] for key in ('schema','identity','instance_id','kind','encoding','payload_bytes')}
        ref.update(sha256=_digest(raw),byte_size=len(raw))
        return raw,ref

    def _reference(self,ref,expected_kind=None):
        need(type(ref) is dict and set(ref)=={'schema','identity','instance_id','kind','encoding','payload_bytes','sha256','byte_size'},'reference fields')
        need(ref['schema']==SCHEMA and ref['identity']==self.identity and ref['instance_id']==self.instance_id,'reference identity')
        _label(ref['kind']);_label(expected_kind);need(ref['kind']==expected_kind,'reference kind mismatch')
        need(ref['encoding'] in ('json','bytes'),'encoding')
        need(type(ref['sha256']) is str and re.fullmatch('[0-9a-f]{64}',ref['sha256']),'reference digest')
        need(type(ref['payload_bytes']) is int and 0<=ref['payload_bytes']<=MAX_PAYLOAD
             and type(ref['byte_size']) is int and ref['payload_bytes']<ref['byte_size']<=MAX_FILE,'reference size')

    def _read_file(self,path):
        check_file(path);need(path.is_file() and path.stat().st_size<=MAX_FILE,'missing/oversize object')
        raw=path.read_bytes();head,sep,payload=raw.partition(b'\n')
        need(sep and len(head)<=MAX_HEADER,'object header')
        header=_load(head)
        need(type(header) is dict and set(header)=={'schema','identity','instance_id','kind','encoding','payload_bytes','payload_sha256'},'header fields')
        need(header['identity']==self.identity and header['instance_id']==self.instance_id and header['schema']==SCHEMA,'object identity')
        encoding=header['encoding'];need(encoding in ('json','bytes'),'object encoding')
        expected,ref=self._frame(header['kind'],payload,encoding)
        need(raw==expected,'object type/size/hash/canonical header')
        if encoding=='json':
            value=_load(payload);need(_json(value)==payload,'noncanonical object JSON')
        else:value=payload
        return value,ref

    def _publish_locked(self,raw,ref):
        """Existing publication protocol; caller owns this store's lock."""
        path=self.directory/('obj-'+ref['sha256']+'.bin')
        self._refresh();check_file(path)
        if path.exists():
            _,stored=self._read_file(path);need(stored==ref,'immutable conflict')
            # Windows _commit/FlushFileBuffers requires a writable handle.
            # r+b neither truncates nor writes the already verified bytes.
            with open(path,'r+b' if os.name=='nt' else 'rb') as stream:os.fsync(stream.fileno())
            self._sync(self.directory);return ref
        data=dict(self.accounting)
        need(data['reservations']<self.profile.max_reservations and data['reserved_bytes']+2*len(raw)<=self.profile.max_reserved_bytes,'store quota exhausted; no automatic GC')
        # Reserve temp+final worst case permanently before creating either.
        data['reservations']+=1;data['reserved_bytes']+=2*len(raw)
        self._save_head(data);self.accounting=data
        temporary=self.directory/('.object-'+uuid.uuid4().hex)
        with open(temporary,'xb') as stream:
            need(stream.write(raw)==len(raw),'short object write');stream.flush();os.fsync(stream.fileno())
        os.link(temporary,path)  # Never overwrite a competing final object.
        temporary.unlink();self._sync(self.directory)
        return ref

    def _put(self,kind,payload,encoding):
        raw,ref=self._frame(kind,payload,encoding)
        with self._locked():return self._publish_locked(raw,ref)

    @contextmanager
    def publication_lease(self,*,max_objects,max_reserved_bytes):
        """Hold exclusive worst-case capacity until the caller exits.

        No charge is made merely for admission. The caller must choose a
        conservative two-frame envelope before an external effect. Only this
        yielded handle may publish while the lease is active; ordinary writers
        fail on the existing lock. Actual publication retains its durable charge
        before writing. Exceptions release the live lock but not any charges;
        process death can leave a stale lock requiring manual diagnosis.
        """
        need(type(max_objects) is int and 1<=max_objects<=self.profile.max_reservations,'lease object count')
        need(type(max_reserved_bytes) is int and 1<=max_reserved_bytes<=self.profile.max_reserved_bytes,'lease byte envelope')
        with self._locked():
            self._refresh()
            need(self.accounting['reservations']+max_objects<=self.profile.max_reservations
                 and self.accounting['reserved_bytes']+max_reserved_bytes<=self.profile.max_reserved_bytes,
                 'insufficient publication lease capacity')
            lease=_PublicationLease(self,max_objects,max_reserved_bytes)
            try:yield lease
            finally:lease._close()

    def put_json(self,kind,value):return self._put(kind,_json(value),'json')
    def put_bytes(self,kind,value):
        need(type(value) is bytes and len(value)<=MAX_PAYLOAD,'bounded bytes required')
        return self._put(kind,value,'bytes')

    def read(self,ref,*,expected_kind):
        self._reference(ref,expected_kind)
        value,actual=self._read_file(self.directory/('obj-'+ref['sha256']+'.bin'))
        need(actual==ref,'object/reference mismatch');return value

    def verify(self,ref,*,expected_kind):
        self.read(ref,expected_kind=expected_kind);return dict(ref)

    def usage(self):
        with self._locked():self._refresh();return dict(self.accounting)
