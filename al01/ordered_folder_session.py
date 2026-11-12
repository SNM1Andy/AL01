"""Ordered controller-only receipt storage. Never reads the sensed environment.

New v2 sessions have an explicit sequence for every submit/capture/consume
transition. Legacy captures remain an unordered set and are never silently
upgraded. Captured-result delivery is at-most-once attempt, as in FolderSession;
read_capture/list_capture_order are replayable observer projections, not delivery.
"""
import copy
import json
import os
import tempfile
from pathlib import Path
from .folder_contracts import validate_request,validate_result,request_sha256
from .controller_paths import check_session_layout
from .folder_session import (FolderSession,FolderSessionError,_need,_encode,_hash,_digest,
    MAX_REQUESTS,MAX_CHECKPOINT_BYTES,MAX_RECEIPT_BYTES,MAX_STORAGE_BYTES)

SCHEMA='ordered-folder-session-2'
LEGACY_SCHEMA='folder-session-1'


class OrderedFolderSession(FolderSession):
    def __init__(self,directory):
        check_session_layout(directory)
        path=Path(os.path.abspath(directory))/'session.json'
        if path.exists():
            data,_=self._read(path,MAX_CHECKPOINT_BYTES)
            if type(data) is dict and data.get('schema')==LEGACY_SCHEMA:
                code='UNORDERED_LEGACY_IMPORT_REQUIRED' if data.get('records') else 'LEGACY_EMPTY_SESSION_REQUIRES_NEW_DIRECTORY'
                raise FolderSessionError(code,'legacy session unchanged; choose a fresh v2 directory')
        super().__init__(directory)

    @staticmethod
    def _validate(data):
        _need(type(data) is dict and set(data)=={'schema','generation','records'} and data['schema']==SCHEMA,'ordered checkpoint schema')
        _need(type(data['generation']) is int and 0<=data['generation']<=3*MAX_REQUESTS,'ordered generation')
        rows=data['records'];_need(type(rows) is dict and len(rows)<=MAX_REQUESTS,'request budget')
        sequences=[]
        for rid,row in rows.items():
            _need(type(row) is dict and set(row)=={'request','request_sha256','status','receipt_sha256','consumption_id','submit_seq','receipt_seq','consume_seq','ordered_record_sha256'},'ordered record fields')
            validate_request(row['request'])
            _need(rid==row['request']['request_id'] and row['request_sha256']==request_sha256(row['request']),'request identity')
            status=row['status'];_need(status in ('REQUESTED','RECEIPT_READY','CONSUMED'),'ordered status')
            seq=row['submit_seq'];_need(type(seq) is int and 1<=seq<=data['generation'],'submit sequence');sequences.append(seq)
            if status=='REQUESTED':
                _need(all(row[k] is None for k in ('receipt_sha256','consumption_id','receipt_seq','consume_seq','ordered_record_sha256')),'requested fields')
            else:
                receipt=row['receipt_seq'];_need(type(receipt) is int and seq<receipt<=data['generation'] and _digest(row['receipt_sha256']) and _digest(row['ordered_record_sha256']),'receipt sequence/digest');sequences.append(receipt)
                if status=='CONSUMED':
                    consumed=row['consume_seq'];_need(type(consumed) is int and receipt<consumed<=data['generation'],'consume sequence');sequences.append(consumed)
                    _need(row['consumption_id']=='consume_'+_hash({'request_id':rid,'receipt_sha256':row['receipt_sha256']}),'consumption identity')
                else:_need(row['consume_seq'] is None and row['consumption_id'] is None,'unconsumed fields')
        _need(sorted(sequences)==list(range(1,data['generation']+1)),'unique contiguous global transitions')

    def _save(self):
        check_session_layout(self.directory)
        data={'schema':SCHEMA,'generation':self.generation,'records':self.records}
        self._validate(data);raw=_encode({'data':data,'sha256':_hash(data)})
        _need(len(raw)<=MAX_CHECKPOINT_BYTES,'checkpoint byte budget')
        staged=list(self.directory.glob('.session-*'))
        _need(all(p.is_file() and not p.is_symlink() for p in staged),'unsafe checkpoint staging')
        _need(len(staged)<8 and sum(p.stat().st_size for p in staged)+len(raw)<=8*MAX_CHECKPOINT_BYTES,'checkpoint staging budget')
        fd,name=tempfile.mkstemp(prefix='.session-',dir=self.directory)
        try:
            with os.fdopen(fd,'wb') as stream:
                if stream.write(raw)!=len(raw):raise OSError('short checkpoint write')
                stream.flush();os.fsync(stream.fileno())
            os.replace(name,self.path);self._sync(self.directory)
        finally:
            if os.path.exists(name):os.unlink(name)

    def _receipt_record(self,rid,row):
        data,sha=self._read(self._receipt_path(rid),MAX_RECEIPT_BYTES)
        _need(type(data) is dict and set(data)=={'schema','receipt_seq','capture','capture_sha256'} and data['schema']=='ordered-folder-receipt-1','ordered receipt fields')
        capture=data['capture']
        _need(type(capture) is dict and set(capture)=={'schema','capture_origin','result'} and capture['schema']==LEGACY_SCHEMA,'legacy-compatible capture shape')
        _need(capture['capture_origin'] in ('APPLICATION_BROKER_CAPTURE','SYNTHETIC_TEST_CAPTURE'),'capture origin')
        _need(data['capture_sha256']==_hash(capture),'capture integrity')
        seq=data['receipt_seq'];_need(type(seq) is int and row['submit_seq']<seq<=3*MAX_REQUESTS,'published receipt sequence')
        if row['receipt_seq'] is not None:
            _need(seq==row['receipt_seq'] and sha==row['ordered_record_sha256'],'ordered receipt sequence/hash binding')
        validate_result(capture['result'],row['request']);return data,sha

    def _receipt(self,rid,row):
        record,_=self._receipt_record(rid,row)
        return record['capture'],record['capture_sha256']

    def _refresh(self,*,persist_reconciliation=True):
        self._inventory();data,_=self._read(self.path,MAX_CHECKPOINT_BYTES);self._validate(data)
        self.records=data['records'];self.generation=data['generation'];ahead=[]
        for path in self.receipts.glob('receipt-*.json'):
            rid=path.name[len('receipt-'):-len('.json')];_need(rid in self.records,'receipt without committed request')
        for rid,row in self.records.items():
            path=self._receipt_path(rid)
            if path.exists():
                receipt,record_sha=self._receipt_record(rid,row)
                sha=receipt['capture_sha256']
                if row['status']=='REQUESTED':ahead.append((receipt['receipt_seq'],rid,sha,record_sha))
                else:_need(sha==row['receipt_sha256'],'captured receipt changed')
            else:_need(row['status']=='REQUESTED','recorded receipt missing; fail closed')
        # Sort only by stored transition evidence, never filename or filesystem time.
        for seq,rid,sha,record_sha in sorted(ahead):
            _need(seq==self.generation+1,'receipt-leading sequence gap or collision')
            row=self.records[rid];row.update(status='RECEIPT_READY',receipt_sha256=sha,receipt_seq=seq,ordered_record_sha256=record_sha);self.generation=seq
        self._validate({'schema':SCHEMA,'generation':self.generation,'records':self.records})
        if ahead and persist_reconciliation:self._save()

    def _publish_receipt(self,result,origin):
        rid=result['request_id'];row=self.records[rid];validate_result(result,row['request']);path=self._receipt_path(rid)
        if path.exists():
            existing,sha=self._receipt(rid,row)
            _need(_encode(existing['result'])==_encode(result),'immutable receipt conflict')
            return sha
        _need(row['status']=='REQUESTED','new receipt requires outstanding request')
        capture={'schema':LEGACY_SCHEMA,'capture_origin':origin,'result':result}
        data={'schema':'ordered-folder-receipt-1','receipt_seq':self.generation+1,'capture':capture,'capture_sha256':_hash(capture)}
        raw=_encode({'data':data,'sha256':_hash(data)})
        _need(len(raw)<=MAX_RECEIPT_BYTES,'receipt byte budget');count,size=self._inventory()
        _need(count+2<=MAX_REQUESTS+8 and size+2*len(raw)<=MAX_STORAGE_BYTES,'staging capacity')
        fd,name=tempfile.mkstemp(prefix='.receipt-',dir=self.receipts)
        try:
            with os.fdopen(fd,'wb') as stream:
                if stream.write(raw)!=len(raw):raise OSError('short receipt write')
                stream.flush();os.fsync(stream.fileno())
            os.link(name,path);os.unlink(name);self._sync(self.receipts)
        finally:
            if os.path.exists(name):os.unlink(name)
        return data['capture_sha256']

    def _record(self,result,origin):
        rid=result.get('request_id') if type(result) is dict else None
        _need(type(rid) is str and rid in self.records,'receipt requires committed request');row=self.records[rid]
        sha=self._publish_receipt(result,origin)
        if row['status']=='REQUESTED':
            receipt,record_sha=self._receipt_record(rid,row);seq=receipt['receipt_seq']
            _need(seq==self.generation+1,'receipt next transition')
            row.update(status='RECEIPT_READY',receipt_sha256=sha,receipt_seq=seq,ordered_record_sha256=record_sha);self.generation=seq;self._save()
        else:_need(row['receipt_sha256']==sha,'receipt identity')
        return self._status(rid)

    def _status(self,rid):
        status=super()._status(rid);row=self.records[rid]
        return dict(status,submit_seq=row['submit_seq'],receipt_seq=row['receipt_seq'],consume_seq=row['consume_seq'],ordered_record_sha256=row['ordered_record_sha256'])

    def submit(self,request):
        validate_request(request);request=copy.deepcopy(request);rid=request['request_id']
        with self._exclusive():
            self._refresh()
            if rid in self.records:
                _need(self.records[rid]['request_sha256']==request_sha256(request),'duplicate ID with different request')
                return self._status(rid)
            _need(len(self.records)<MAX_REQUESTS,'request capacity')
            self.generation+=1
            self.records[rid]={'request':request,'request_sha256':request_sha256(request),'status':'REQUESTED','receipt_sha256':None,
                'consumption_id':None,'submit_seq':self.generation,'receipt_seq':None,'consume_seq':None,'ordered_record_sha256':None}
            self._save();return self._status(rid)

    def consume(self,request_id):
        with self._exclusive():
            self._refresh();_need(type(request_id) is str and request_id in self.records,'unknown request');row=self.records[request_id]
            if row['status']=='CONSUMED':return dict(self._status(request_id),delivery=None)
            _need(row['status']=='RECEIPT_READY','no captured receipt');data,sha=self._receipt(request_id,row)
            self.generation+=1;row.update(status='CONSUMED',consume_seq=self.generation,
                consumption_id='consume_'+_hash({'request_id':request_id,'receipt_sha256':sha}))
            self._save();return dict(self._status(request_id),delivery=copy.deepcopy(data))

    def list_requests(self):
        with self._exclusive():
            self._refresh(persist_reconciliation=False)
            return [dict(self._status(rid),request=copy.deepcopy(row['request']))
                    for rid,row in sorted(self.records.items(),key=lambda pair:pair[1]['submit_seq'])]

    def list_capture_order(self):
        """Read-only projection ordered by explicit recorded capture transition."""
        with self._exclusive():
            self._refresh(persist_reconciliation=False)
            result=[]
            for rid,row in sorted(self.records.items(),key=lambda pair:pair[1]['receipt_seq'] or 3*MAX_REQUESTS+1):
                if row['receipt_seq'] is not None:
                    capture,_=self._receipt(rid,row)
                    result.append(dict(self._status(rid),request=copy.deepcopy(row['request']),capture=copy.deepcopy(capture),projection_only=True))
            return result


def legacy_import_manifest(old_directory):
    """Read-only validated legacy capture set; never assigns historical order.

    Reads controller persistence only, never root/entry paths from requests or
    broker metadata. Actor identity belongs to a separate actor.json and is not
    inferred or read here. An unmatched final receipt may lead the old checkpoint;
    it is included as such, without modifying that checkpoint or implying order.
    """
    old=object.__new__(FolderSession)
    check_session_layout(old_directory)
    old.directory=Path(os.path.abspath(old_directory));old.path=old.directory/'session.json';old.receipts=old.directory/'receipts'
    _need(old.directory.is_dir() and old.receipts.is_dir() and not old.receipts.is_symlink(),'legacy controller directory')
    _need(not (old.directory/'session.lock').exists(),'legacy session busy; inspect only while stopped')
    old._inventory();data,checkpoint_sha=old._read(old.path,MAX_CHECKPOINT_BYTES);FolderSession._validate(data)
    old.records=data['records'];old.generation=data['generation']
    for path in old.receipts.glob('receipt-*.json'):
        rid=path.name[len('receipt-'):-len('.json')];_need(rid in old.records,'legacy orphan receipt')
    captures=[];requests=[]
    # Sorting canonicalizes a SET manifest, explicitly not a chronology.
    for rid,row in sorted(old.records.items()):
        requests.append({'request_id':rid,'request_sha256':row['request_sha256'],'recorded_status':row['status']})
        if old._receipt_path(rid).exists():
            receipt,sha=old._receipt(rid,row)
            if row['status']!='REQUESTED':_need(sha==row['receipt_sha256'],'legacy receipt digest changed')
            captures.append({'request_id':rid,'request_sha256':row['request_sha256'],'receipt_sha256':sha,
                'capture_origin':receipt['capture_origin'],'checkpoint_status':row['status'],
                'receipt_leads_checkpoint':row['status']=='REQUESTED','request':copy.deepcopy(row['request']),'result':copy.deepcopy(receipt['result'])})
        else:_need(row['status']=='REQUESTED','legacy recorded receipt missing')
    # Detect a cooperative writer changing the snapshot while we inspect; not an
    # adversarial concurrent-writer guarantee. Never create an inspection lock.
    _,after=old._read(old.path,MAX_CHECKPOINT_BYTES);_need(after==checkpoint_sha,'legacy source changed during inspection')
    manifest={'schema':'unordered-legacy-capture-manifest-1','source_schema':LEGACY_SCHEMA,'source_checkpoint_sha256':checkpoint_sha,
        'source_generation':data['generation'],'identity':None,'identity_status':'ACTOR_IDENTITY_NOT_IN_SESSION',
        'ordering':'UNORDERED_CAPTURE_SET; list order is canonicalization only, not historical sequence',
        'chronological_replay_allowed':False,'request_count':len(requests),'capture_count':len(captures),'requests':requests,'captures':captures}
    manifest['capture_set_sha256']=_hash(captures)
    return manifest
