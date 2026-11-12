"""Atomic receipt-derived cognition/U0 + reference CGL, not a new action policy.

Only OrderedFolderSession captures are admitted. Admission ordinal is the rank
of an explicit receipt transition, not a wall clock or a fabricated legacy order.
The broker remains the finite, acknowledged application-level reader. This module
never reopens a sensed file to rebuild cognition; controller receipts may lead
this checkpoint and are applied exactly once when it catches up.
"""
import copy
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from dataclasses import asdict
import numpy as np
from .folder_runner import FolderRunner,check_private_ancestors,atomic_json,check_execution_owner
from .ordered_folder_session import OrderedFolderSession
from .folder_session import _hash,_encode,FolderSessionError,MAX_CHECKPOINT_BYTES
from .folder_cognition import initial_cognition,apply_capture,validate_replay
from .folder_field import FolderEventDrive,TICK_DURATION
from .instinct import ReferenceCGL,Geometry,Parameters,VERSION as CGL_VERSION

SCHEMA='folder-joint-reference-1'
MAX_JOINT_BYTES=8*1024*1024


class FolderJointError(ValueError):
    code='FOLDER_JOINT_INVALID'


def need(ok,message):
    if not ok:raise FolderJointError(message)


def matrix(value):
    need(type(value) is list and len(value)==64 and all(type(r) is list and len(r)==64 for r in value),'field matrix shape')
    need(all(type(v) in (int,float) and np.isfinite(v) for r in value for v in r),'finite numeric field matrix')
    return np.array(value,dtype=np.float64)


class FolderJointRunner(FolderRunner):
    session_type=OrderedFolderSession

    def __init__(self,root,state_directory,*,application_checks_acknowledged=False,field_seed=None):
        if field_seed is not None:need(type(field_seed) is int and 0<=field_seed<2**64,'field seed')
        # Refuse legacy history before the base can create any actor/session files.
        prior=Path(state_directory).absolute()/'session'/'session.json'
        if prior.exists():
            old,_=OrderedFolderSession._read(prior,MAX_CHECKPOINT_BYTES)
            if type(old) is dict and old.get('schema')=='folder-session-1':
                raise FolderSessionError('UNORDERED_LEGACY_IMPORT_REQUIRED','original actor and session left unchanged')
        # A partial prior birth is not a fresh generation. In particular, do not
        # let the base replace a lost actor or infer a new seed from surviving
        # captures. Interrupted first initialization requires manual diagnosis;
        # no automatic repair/migration is claimed by this reference runtime.
        base=Path(state_directory).absolute()
        birth_artifacts=[base/'actor.json',prior,base/'folder-joint-origin.json',base/'folder-joint.json']
        present=[p.exists() or p.is_symlink() for p in birth_artifacts]
        if any(present):
            need(all(present),'incomplete existing joint birth; no reset or inferred generation')
        super().__init__(root,state_directory,application_checks_acknowledged=application_checks_acknowledged)
        self.joint_path=self.directory/'folder-joint.json';self.joint_sha=None
        self.origin_path=self.directory/'folder-joint-origin.json'
        with self._joint_lock():
            if self.origin_path.exists():
                need(self.joint_path.exists(),'previously initialized joint checkpoint missing; no reset')
                origin,_=OrderedFolderSession._read(self.origin_path,65536)
                need(type(origin) is dict and set(origin)=={'schema','identity','scope_ref','field_seed'}
                     and origin['schema']==SCHEMA and origin['identity']==self.actor['id'] and origin['scope_ref']==self.actor['scope_ref'],'joint birth binding')
                self.birth_seed=origin['field_seed']
                need(type(self.birth_seed) is int and 0<=self.birth_seed<2**64,'birth seed')
            else:
                need(not self.joint_path.exists(),'joint birth manifest missing; no inferred genesis')
                self.birth_seed=0 if field_seed is None else field_seed
                origin={'schema':SCHEMA,'identity':self.actor['id'],'scope_ref':self.actor['scope_ref'],'field_seed':self.birth_seed}
                atomic_json(self.origin_path,{'data':origin,'sha256':_hash(origin)});self.session._sync(self.directory)
            if not self.joint_path.exists():
                self.field_seed=self.birth_seed
                self.cognition=initial_cognition(self.actor['id'],self.actor['scope_ref'])
                self.bindings=[];self.solver=self._fresh_field(self.field_seed)
                self._save_joint()
            self._load_joint()
            need(field_seed is None or field_seed==self.field_seed,'restored field seed differs')
            self._catch_up_locked()

    @contextmanager
    def _joint_lock(self):
        check_execution_owner(self.directory,self.execution_owner,allow_initializing=getattr(self,'_system_initializing',False))
        check_private_ancestors(self.directory)
        fd=os.open(self.lock,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        try:yield
        finally:os.close(fd);self.lock.unlink()

    @staticmethod
    def _fresh_field(seed):
        return ReferenceCGL(Geometry(np.zeros((64,64))),seed=seed,dt=.01,
                           drive=FolderEventDrive([],committed_tick=0))

    @staticmethod
    def _binding(row):
        return {key:row[key] for key in ('request_id','receipt_seq','receipt_sha256','ordered_record_sha256')}

    @staticmethod
    def _item(row,ordinal):
        return {'sequence':ordinal,'request':copy.deepcopy(row['request']),
                'capture':copy.deepcopy(row['capture']),'receipt_sha256':row['receipt_sha256']}

    def _field_data(self):
        real=self.solver.field.real.tolist();imag=self.solver.field.imag.tolist()
        return {'manifest':self.solver.manifest(),'real':real,'imag':imag,
                'field_sha256':_hash({'real':real,'imag':imag}),'drive':self.solver.drive.manifest()}

    def _restore_field(self,data,cognition,seed):
        need(type(data) is dict and set(data)=={'manifest','real','imag','field_sha256','drive'},'field fields')
        need(data['field_sha256']==_hash({'real':data['real'],'imag':data['imag']}),'field content integrity')
        real=matrix(data['real']);imag=matrix(data['imag']);manifest=data['manifest'];count=len(cognition['sources'])
        drive=FolderEventDrive.from_manifest(data['drive'],cognition['events'],committed_tick=count)
        need(type(manifest) is dict and manifest.get('solver')==CGL_VERSION and manifest.get('numpy')==np.__version__,'field implementation version')
        need(type(manifest.get('seed')) is int and manifest['seed']==seed,'field seed binding')
        need(_encode(manifest.get('parameters'))==_encode(asdict(Parameters())) and manifest.get('method')=='rk4'
             and type(manifest.get('dt')) is float and manifest['dt']==.01 and manifest.get('amplitude_limit')==10.,'fixed reference field profile')
        expected_time=(count+1)*TICK_DURATION if count else 0.
        expected_steps=5*(count+1) if count else 0
        need(type(manifest.get('time')) in (int,float) and abs(manifest['time']-expected_time)<1e-12
             and type(manifest.get('steps')) is int and manifest['steps']==expected_steps,'field/capture clock')
        solver=ReferenceCGL(Geometry(np.zeros((64,64))),seed=seed,dt=.01,drive=drive,initial=real+1j*imag)
        solver.time=manifest['time'];solver.steps=manifest['steps']
        need(_encode(solver.manifest())==_encode(manifest),'field manifest/geometry/features mismatch')
        return solver

    def _payload(self):
        return {'schema':SCHEMA,'identity':self.actor['id'],'scope_ref':self.actor['scope_ref'],
                'parent_joint_sha256':self.joint_sha,'field_seed':self.field_seed,
                'applied_count':len(self.bindings),'receipt_bindings':copy.deepcopy(self.bindings),
                'cognition':copy.deepcopy(self.cognition),'field':self._field_data(),
                'scope':'receipt-derived U0 and numerical field; no learned semantics or policy influence claim'}

    def _validate_joint(self,data,rows):
        need(type(data) is dict and set(data)=={'schema','identity','scope_ref','parent_joint_sha256','field_seed',
             'applied_count','receipt_bindings','cognition','field','scope'} and data['schema']==SCHEMA,'joint schema')
        need(data['identity']==self.actor['id'] and data['scope_ref']==self.actor['scope_ref'],'same actor/scope binding')
        seed=data['field_seed'];need(type(seed) is int and seed==self.birth_seed,'field initialization/birth seed')
        count=data['applied_count'];need(type(count) is int and 0<=count<=32 and count<=len(rows),'captured prefix count')
        parent=data['parent_joint_sha256'];need(parent is None or type(parent) is str and len(parent)==64 and all(c in '0123456789abcdef' for c in parent),'parent digest')
        need((parent is None)==(count==0),'joint parent lineage presence')
        need(data['receipt_bindings']==[self._binding(row) for row in rows[:count]],'ordered source prefix differs')
        cognition=data['cognition'];need(type(cognition) is dict and cognition.get('identity')==data['identity']
            and cognition.get('scope_ref')==data['scope_ref'],'cognition identity')
        validate_replay(cognition,[self._item(row,i+1) for i,row in enumerate(rows[:count])])
        solver=self._restore_field(data['field'],cognition,seed)
        return cognition,solver

    def _save_joint(self):
        data=self._payload();rows=self.session.list_capture_order();self._validate_joint(data,rows)
        wrapper={'data':data,'sha256':_hash(data)};raw=_encode(wrapper)
        need(len(raw)<=MAX_JOINT_BYTES,'joint byte budget')
        fd,name=tempfile.mkstemp(prefix='.folder-joint-',dir=self.directory)
        try:
            with os.fdopen(fd,'wb') as stream:
                if stream.write(raw)!=len(raw):raise OSError('short joint write')
                stream.flush();os.fsync(stream.fileno())
            os.replace(name,self.joint_path);self.session._sync(self.directory);self.joint_sha=wrapper['sha256']
        finally:
            if os.path.exists(name):os.unlink(name)

    def _load_joint(self):
        try:
            need(self.joint_path.is_file() and not self.joint_path.is_symlink()
                 and self.joint_path.stat().st_size<=MAX_JOINT_BYTES,'missing/unsafe/oversize joint')
            wrapper=json.loads(self.joint_path.read_text(encoding='utf8'))
            need(type(wrapper) is dict and set(wrapper)=={'data','sha256'} and wrapper['sha256']==_hash(wrapper['data']),'joint integrity')
            data=wrapper['data'];cognition,solver=self._validate_joint(data,self.session.list_capture_order())
            self.cognition=copy.deepcopy(cognition);self.solver=solver;self.field_seed=data['field_seed']
            self.bindings=copy.deepcopy(data['receipt_bindings']);self.joint_sha=wrapper['sha256']
        except (KeyError,TypeError,UnicodeError,OverflowError,RecursionError,json.JSONDecodeError) as exc:
            raise FolderJointError('malformed joint: '+type(exc).__name__) from exc

    def _catch_up_locked(self):
        self._load_joint();rows=self.session.list_capture_order();count=len(self.bindings)
        if count==len(rows):return False
        try:
            for i,row in enumerate(rows[count:],count+1):
                self.cognition=apply_capture(self.cognition,self._item(row,i))
                self.solver.drive=FolderEventDrive(self.cognition['events'],committed_tick=i)
                self.solver.run((i+1)*TICK_DURATION-self.solver.time)
                self.bindings.append(self._binding(row))
            self._save_joint();return True
        except Exception:
            self._load_joint();raise

    def reconcile(self):
        """Only replay existing controller captures; never invokes the broker."""
        with self._joint_lock():return self._catch_up_locked()

    def _step_locked(self):
        self._catch_up_locked() # Receipt-leading recovery before any next read.
        progressed=super()._step_locked()
        self._catch_up_locked()
        return progressed

    def _report_locked(self):
        self._load_joint();base=super().report()
        return dict(base,joint_schema=SCHEMA,joint_sha256=self.joint_sha,
            projected_captures=len(self.bindings),projection_order='explicit receipt transition order, mapped to admission ordinals',
            cognition=copy.deepcopy(self.cognition),field_manifest=self.solver.manifest(),
            field_profile='new reference CGL with fixed zero geometry; not restored legacy field geometry',
            joint_scope='same actor/scope receipt cognition+U0 and numerical CGL; no policy influence or full baseline claim')

    def report(self):
        with self._joint_lock():return self._report_locked()
