"""Durable host-controller protocol; NO environment reader or OS broker.

Submit persists intent only, without invoking any backend. A trusted host
controller may supply an application-broker receipt afterwards. These are typed
untrusted data and application provenance labels, NOT verified OS confinement.
Consumption is at-most-once delivery ATTEMPT: marking precedes return, so a crash
can lose delivery but never authorizes automatic duplicate delivery or rereading.
All filesystem operations concern this controller's own persistence directory.
"""
import copy
import hashlib
import json
import os
import re
import tempfile
from contextlib import contextmanager
from pathlib import Path
from .folder_contracts import validate_request,validate_result,request_sha256,make_result
from .controller_paths import check_file,check_session_layout

SCHEMA='folder-session-1'
MAX_REQUESTS=32
MAX_CHECKPOINT_BYTES=262144
MAX_RECEIPT_BYTES=524288
MAX_STORAGE_BYTES=32*MAX_RECEIPT_BYTES


class FolderSessionError(ValueError):
    def __init__(self,code,detail=''):
        self.code=code;super().__init__(code+': '+detail)


def _need(ok,message):
    if not ok:raise FolderSessionError('FOLDER_SESSION_INVALID',message)
def _encode(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf8')
def _hash(value):return hashlib.sha256(_encode(value)).hexdigest()
def _digest(value):return type(value) is str and re.fullmatch('[0-9a-f]{64}',value) is not None


class FolderSession:
    def __init__(self,directory):
        check_session_layout(directory)
        self.directory=Path(os.path.abspath(directory));self.directory.mkdir(parents=True,exist_ok=True)
        self.path=self.directory/'session.json';self.lock_path=self.directory/'session.lock'
        self.receipts=self.directory/'receipts';self.receipts.mkdir(exist_ok=True)
        _need(not self.receipts.is_symlink(),'receipt directory cannot be symlink')
        with self._exclusive():
            if not self.path.exists():
                _need(not any(self.receipts.iterdir()),'receipt storage without checkpoint; manual diagnosis required')
                self.records={};self.generation=0;self._save()
            self._refresh()

    @contextmanager
    def _exclusive(self):
        check_session_layout(self.directory)
        try:fd=os.open(self.lock_path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        except FileExistsError:raise FolderSessionError('FOLDER_SESSION_BUSY','stale/live lock requires manual diagnosis')
        try:yield
        finally:os.close(fd);self.lock_path.unlink()

    @staticmethod
    def _sync(directory):
        # POSIX durability only; Windows directory durability is unverified.
        if os.name=='posix':
            fd=os.open(directory,os.O_RDONLY|getattr(os,'O_DIRECTORY',0))
            try:os.fsync(fd)
            finally:os.close(fd)

    def _save(self):
        check_session_layout(self.directory)
        data={'schema':SCHEMA,'generation':self.generation,'records':self.records}
        self._validate(data);raw=_encode({'data':data,'sha256':_hash(data)})
        _need(len(raw)<=MAX_CHECKPOINT_BYTES,'checkpoint byte budget')
        staged=list(self.directory.glob('.session-*'))
        _need(all(p.is_file() and not p.is_symlink() for p in staged),'unsafe checkpoint staging')
        _need(len(staged)<8 and sum(p.stat().st_size for p in staged)+len(raw)<=8*MAX_CHECKPOINT_BYTES,'checkpoint staging budget; manual diagnosis required')
        fd,name=tempfile.mkstemp(prefix='.session-',dir=self.directory)
        try:
            with os.fdopen(fd,'wb') as stream:
                if stream.write(raw)!=len(raw):raise OSError('short checkpoint write')
                stream.flush();os.fsync(stream.fileno())
            os.replace(name,self.path);self._sync(self.directory)
        finally:
            if os.path.exists(name):os.unlink(name)

    @staticmethod
    def _validate(data):
        _need(type(data) is dict and set(data)=={'schema','generation','records'} and data['schema']==SCHEMA,'checkpoint schema')
        _need(type(data['generation']) is int and 0<=data['generation']<=3*MAX_REQUESTS,'generation')
        records=data['records'];_need(type(records) is dict and len(records)<=MAX_REQUESTS,'request budget')
        expected=0
        for rid,row in records.items():
            _need(type(row) is dict and set(row)=={'request','request_sha256','status','receipt_sha256','consumption_id'},'record fields')
            validate_request(row['request']);_need(rid==row['request']['request_id'] and row['request_sha256']==request_sha256(row['request']),'request identity')
            status=row['status'];_need(status in ('REQUESTED','RECEIPT_READY','CONSUMED'),'record status')
            expected+=1
            if status=='REQUESTED':_need(row['receipt_sha256'] is None and row['consumption_id'] is None,'unfulfilled request')
            else:
                expected+=1;_need(_digest(row['receipt_sha256']),'receipt digest')
                if status=='CONSUMED':
                    expected+=1
                    _need(row['consumption_id']=='consume_'+_hash({'request_id':rid,'receipt_sha256':row['receipt_sha256']}),'consumption binding')
                else:_need(row['consumption_id'] is None,'unconsumed receipt')
        _need(data['generation']==expected,'generation transition count')

    def _receipt_path(self,rid):return self.receipts/('receipt-'+rid+'.json')

    def _inventory(self):
        check_session_layout(self.directory)
        count=0;size=0
        for path in self.receipts.iterdir():
            _need(not path.is_symlink() and path.is_file(),'receipt inventory regular files only')
            _need(re.fullmatch(r'receipt-req_[0-9a-f]{32}\.json',path.name) is not None or path.name.startswith('.receipt-'),'unexpected receipt inventory')
            count+=1;size+=path.stat().st_size
        _need(count<=MAX_REQUESTS+8 and size<=MAX_STORAGE_BYTES,'receipt storage budget including abandoned staging')
        return count,size

    @staticmethod
    def _read(path,limit):
        check_file(path)
        _need(path.is_file() and not path.is_symlink() and path.stat().st_size<=limit,'missing/unsafe/oversize persistence file')
        try:
            wrapper=json.loads(path.read_text(encoding='utf8'))
            _need(type(wrapper) is dict and set(wrapper)=={'data','sha256'} and _hash(wrapper['data'])==wrapper['sha256'],'persistence integrity')
            return wrapper['data'],wrapper['sha256']
        except (ValueError,TypeError,UnicodeError,RecursionError,OverflowError) as exc:
            if isinstance(exc,FolderSessionError):raise
            raise FolderSessionError('FOLDER_SESSION_INVALID','malformed persistence') from exc

    def _receipt(self,rid,row):
        data,sha=self._read(self._receipt_path(rid),MAX_RECEIPT_BYTES)
        _need(type(data) is dict and set(data)=={'schema','capture_origin','result'} and data['schema']==SCHEMA,'receipt fields')
        _need(data['capture_origin'] in ('APPLICATION_BROKER_CAPTURE','SYNTHETIC_TEST_CAPTURE'),'capture origin')
        validate_result(data['result'],row['request'])
        return data,sha

    def _refresh(self,*,persist_reconciliation=True):
        self._inventory();data,_=self._read(self.path,MAX_CHECKPOINT_BYTES);self._validate(data)
        self.records=data['records'];self.generation=data['generation'];changed=False
        # A committed receipt may lead the checkpoint; unknown final receipts do
        # not authorize silently recreating lost requests or redoing consumption.
        for path in self.receipts.glob('receipt-*.json'):
            rid=path.name[len('receipt-'):-len('.json')];_need(rid in self.records,'receipt without committed request')
        for rid,row in self.records.items():
            path=self._receipt_path(rid)
            if path.exists():
                _,sha=self._receipt(rid,row)
                if row['status']=='REQUESTED':
                    row['status']='RECEIPT_READY';row['receipt_sha256']=sha;self.generation+=1;changed=True
                else:_need(sha==row['receipt_sha256'],'captured receipt changed')
            else:_need(row['status']=='REQUESTED','recorded receipt missing; fail closed')
        if changed and persist_reconciliation:self._save()

    def _publish_receipt(self,result,origin):
        rid=result['request_id'];row=self.records[rid];validate_result(result,row['request'])
        path=self._receipt_path(rid)
        if path.exists():
            existing,sha=self._receipt(rid,row)
            _need(_encode(existing['result'])==_encode(result),'immutable receipt conflict')
            return sha
        data={'schema':SCHEMA,'capture_origin':origin,'result':result};raw=_encode({'data':data,'sha256':_hash(data)})
        _need(len(raw)<=MAX_RECEIPT_BYTES,'receipt byte budget');count,size=self._inventory()
        _need(count+2<=MAX_REQUESTS+8 and size+2*len(raw)<=MAX_STORAGE_BYTES,'staging capacity')
        fd,name=tempfile.mkstemp(prefix='.receipt-',dir=self.receipts)
        try:
            with os.fdopen(fd,'wb') as stream:
                if stream.write(raw)!=len(raw):raise OSError('short receipt write')
                stream.flush();os.fsync(stream.fileno())
            os.link(name,path) # Exclusive publication; never overwrite captured bytes.
            os.unlink(name);self._sync(self.receipts)
        finally:
            if os.path.exists(name):os.unlink(name)
        return _hash(data)

    def _record(self,result,origin):
        rid=result.get('request_id') if type(result) is dict else None
        _need(type(rid) is str and rid in self.records,'receipt requires committed request');row=self.records[rid]
        sha=self._publish_receipt(result,origin)
        if row['status']=='REQUESTED':
            row['status']='RECEIPT_READY';row['receipt_sha256']=sha;self.generation+=1;self._save()
        else:_need(row['receipt_sha256']==sha,'receipt identity')
        return self._status(rid)

    def _status(self,rid):
        _need(type(rid) is str and rid in self.records,'unknown request')
        row=self.records[rid]
        return {'request_id':rid,'request_sha256':row['request_sha256'],'status':row['status'],
                'receipt_sha256':row['receipt_sha256'],'consumption_id':row['consumption_id']}

    def submit(self,request):
        validate_request(request);request=copy.deepcopy(request);rid=request['request_id']
        with self._exclusive():
            self._refresh()
            if rid in self.records:
                _need(self.records[rid]['request_sha256']==request_sha256(request),'duplicate ID with different request')
                return self._status(rid)
            _need(len(self.records)<MAX_REQUESTS,'request capacity; no eviction')
            self.records[rid]={'request':request,'request_sha256':request_sha256(request),'status':'REQUESTED','receipt_sha256':None,'consumption_id':None}
            self.generation+=1;self._save() # Must succeed before any external broker stage.
            return self._status(rid)

    def record_receipt(self,result,*,capture_origin='APPLICATION_BROKER_CAPTURE'):
        """Supply captured DATA; labels never prove OS confinement or authorize I/O."""
        _need(capture_origin in ('APPLICATION_BROKER_CAPTURE','SYNTHETIC_TEST_CAPTURE'),'capture origin')
        with self._exclusive():
            self._refresh();return self._record(result,capture_origin)

    def status(self,request_id):
        with self._exclusive():self._refresh();return self._status(request_id)

    def list_requests(self):
        """Read-only validated controller projection, sorted by opaque request ID."""
        with self._exclusive():
            self._refresh(persist_reconciliation=False)
            return [dict(self._status(rid),request=copy.deepcopy(self.records[rid]['request'])) for rid in sorted(self.records)]

    def read_capture(self,request_id):
        """Replayable trusted-observer projection of existing immutable data only.

        Never performs the environment read again or changes consumption state.
        Consumers rebuilding their own persistent perception must deduplicate by
        request/receipt hash. Unlike consume, this projection can be read repeatedly.
        """
        with self._exclusive():
            self._refresh(persist_reconciliation=False);status=self._status(request_id);row=self.records[request_id]
            capture=None if row['status']=='REQUESTED' else self._receipt(request_id,row)[0]
            return dict(status,capture=copy.deepcopy(capture),projection_only=True)

    def consume(self,request_id):
        with self._exclusive():
            self._refresh();_need(type(request_id) is str and request_id in self.records,'unknown request');row=self.records[request_id]
            if row['status']=='CONSUMED':return dict(self._status(request_id),delivery=None)
            _need(row['status']=='RECEIPT_READY','no captured receipt')
            data,sha=self._receipt(request_id,row)
            row['status']='CONSUMED';row['consumption_id']='consume_'+_hash({'request_id':request_id,'receipt_sha256':sha})
            self.generation+=1;self._save() # Failure/uncertainty is NEVER automatic redelivery.
            return dict(self._status(request_id),delivery=copy.deepcopy(data))
