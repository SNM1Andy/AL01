"""Durable dispatch contract for explicit sandbox adapters only.

Not connected to host actions or the cognition loop. A persisted DISPATCHED action
is never automatically retried: recovery conservatively marks it UNKNOWN.
"""
import copy, hashlib, json, os, tempfile
from contextlib import contextmanager
from functools import wraps
from pathlib import Path
from .core import canonical, digest, fail, serializable

MAX_ACTIONS = 1024
MAX_JOURNAL_BYTES = 1048576
STATES = {'REQUESTED','DISPATCHED','SUCCEEDED','FAILED','UNKNOWN'}

def locked_latest(method):
    @wraps(method)
    def wrapped(self,*args,**kwargs):
        with self._exclusive():
            self._reload()
            return method(self,*args,**kwargs)
    return wrapped

def json_value(value):
    """Bounded JSON values only, with stable types across restart."""
    try:
        value=serializable(value)
        def check(v):
            if isinstance(v,dict):
                if any(not isinstance(k,str) for k in v): fail('DISPATCH_INVALID','JSON keys must be text')
                for item in v.values(): check(item)
            elif isinstance(v,list):
                for item in v: check(item)
            elif v is not None and type(v) not in (bool,int,float,str): fail('DISPATCH_INVALID','JSON value type')
        check(value)
        canonical(value).encode('utf8')
        return value
    except (ValueError,TypeError,UnicodeError,RecursionError,OverflowError):
        fail('DISPATCH_INVALID','invalid JSON value')

class DispatchJournal:
    def __init__(self, path):
        self.path=Path(path).resolve()
        self.lock_path=self.path.with_name(self.path.name+'.lock')
        self._reload()

    def _reload(self):
        """Refresh atomically published data; recovery itself performs no write."""
        self.records={}
        if self.path.exists():
            if self.path.stat().st_size>MAX_JOURNAL_BYTES: fail('DISPATCH_INVALID','journal size')
            try:
                wrapper=json.loads(self.path.read_text(encoding='utf8'))
                if set(wrapper)!={'data','sha256'} or wrapper['sha256']!=digest(wrapper['data']): fail('DISPATCH_INVALID','integrity')
                data=wrapper['data']
                if set(data)!={'schema','records'} or data['schema']!='dispatch-0.1': fail('DISPATCH_INVALID','schema')
                self.records=data['records']; self._validate()
            except (ValueError,KeyError,TypeError,UnicodeError,AttributeError,RecursionError): fail('DISPATCH_INVALID','malformed journal')
            for record in self.records.values():
                if record['status']=='DISPATCHED':
                    record['status']='UNKNOWN'; record['error']='interrupted_after_dispatch'

    @contextmanager
    def _exclusive(self):
        """Never remove somebody else's lock or automatically recover stale locks.

        An abandoned lock requires operator diagnosis of process/action state.
        The lock spans adapter invocation, including final result persistence.
        """
        self.path.parent.mkdir(parents=True,exist_ok=True)
        try: fd=os.open(self.lock_path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        except FileExistsError: fail('DISPATCH_BUSY','journal lock exists; diagnose owner/crash before manual removal')
        try:
            os.write(fd,str(os.getpid()).encode('ascii'))
            yield
        finally:
            os.close(fd)
            self.lock_path.unlink()

    def _sync_directory(self):
        # POSIX rename durability needs the parent directory fsynced as well.
        # Windows directory durability is not claimed or verified here.
        if os.name=='posix':
            fd=os.open(self.path.parent,os.O_RDONLY|getattr(os,'O_DIRECTORY',0))
            try: os.fsync(fd)
            finally: os.close(fd)

    def _validate(self):
        if not isinstance(self.records,dict) or len(self.records)>MAX_ACTIONS: fail('DISPATCH_INVALID','records')
        keys=set()
        for action_id,r in self.records.items():
            if not isinstance(r,dict) or set(r)!={'action_id','dedup_key','operation','payload','status','result','error'}: fail('DISPATCH_INVALID','record')
            if not isinstance(r['dedup_key'],str) or not 1<=len(r['dedup_key'])<=256 or r['dedup_key'] in keys: fail('DISPATCH_INVALID','dedup key')
            keys.add(r['dedup_key'])
            if not isinstance(action_id,str) or r['action_id']!=action_id or action_id!='act_'+hashlib.sha256(r['dedup_key'].encode()).hexdigest(): fail('DISPATCH_INVALID','identity')
            if r['operation'] not in ('sandbox_observe','sandbox_move') or not isinstance(r['payload'],dict) or not isinstance(r['status'],str) or r['status'] not in STATES: fail('DISPATCH_INVALID','operation/status')
            if r['error'] is not None and not isinstance(r['error'],str): fail('DISPATCH_INVALID','error')
            json_value(r)

    def _save(self):
        self._validate()
        data={'schema':'dispatch-0.1','records':self.records}
        raw=canonical({'data':data,'sha256':digest(data)}).encode('utf8')
        if len(raw)>MAX_JOURNAL_BYTES: fail('BUDGET_EXCEEDED','dispatch journal')
        self.path.parent.mkdir(parents=True,exist_ok=True)
        name=None
        try:
            with tempfile.NamedTemporaryFile(dir=self.path.parent,delete=False) as f:
                name=f.name; f.write(raw); f.flush(); os.fsync(f.fileno())
            os.replace(name,self.path); name=None
            self._sync_directory()
        finally:
            if name is not None: Path(name).unlink(missing_ok=True)

    @locked_latest
    def request(self,dedup_key,operation,payload):
        if not isinstance(dedup_key,str) or not 1<=len(dedup_key)<=256: fail('DISPATCH_INVALID','dedup key')
        if operation not in ('sandbox_observe','sandbox_move') or not isinstance(payload,dict): fail('CAPABILITY_DENIED','sandbox adapter only')
        try: dedup_key.encode('utf8')
        except UnicodeError: fail('DISPATCH_INVALID','dedup key UTF-8')
        payload=json_value(payload)
        action_id='act_'+hashlib.sha256(dedup_key.encode()).hexdigest()
        existing=self.records.get(action_id)
        if existing:
            if existing['operation']!=operation or existing['payload']!=payload: fail('STATE_CONFLICT','dedup payload differs')
            return copy.deepcopy(existing)
        if len(self.records)>=MAX_ACTIONS: fail('BUDGET_EXCEEDED','dispatch history')
        self.records[action_id]={'action_id':action_id,'dedup_key':dedup_key,'operation':operation,'payload':payload,'status':'REQUESTED','result':None,'error':None}
        try: self._save()
        except Exception:
            self.records.pop(action_id); raise
        return copy.deepcopy(self.records[action_id])

    @locked_latest
    def dispatch(self,action_id,adapter):
        """adapter is trusted repository code, never a CL-provided callable.

        Durable write failure BEFORE calling the adapter does not execute it.
        Exceptions after invocation are UNKNOWN, never definite failure.
        """
        if not isinstance(action_id,str) or action_id not in self.records: fail('DISPATCH_INVALID','unknown action')
        r=self.records[action_id]
        if r['status']!='REQUESTED': return copy.deepcopy(r)
        r['status']='DISPATCHED'
        try: self._save()
        except Exception:
            r['status']='REQUESTED'; raise
        try:
            result=json_value(adapter(r['operation'],copy.deepcopy(r['payload']),action_id))
            r['result']=result; r['status']='SUCCEEDED'
            self._save()
        except Exception:
            r['result']=None; r['status']='UNKNOWN'; r['error']='adapter_or_result_persistence_uncertain'
            # If this also fails, the durable DISPATCHED record still prevents retries.
            self._save()
        return copy.deepcopy(r)

    @locked_latest
    def reconcile(self,action_id,*,succeeded,result=None):
        """Explicit trusted result query only. Does not execute an action.

        Caller must verify outcome through its adapter. No auto reconciliation.
        """
        if type(succeeded) is not bool: fail('DISPATCH_INVALID','result status')
        if not isinstance(action_id,str): fail('DISPATCH_INVALID','action ID')
        r=self.records.get(action_id)
        if r is None or r['status']!='UNKNOWN': fail('STATE_CONFLICT','only unknown results can be reconciled')
        old=copy.deepcopy(r)
        r.update(status='SUCCEEDED' if succeeded else 'FAILED',result=json_value(result),error=None)
        try: self._save()
        except Exception:
            self.records[action_id]=old; raise
        return copy.deepcopy(r)
