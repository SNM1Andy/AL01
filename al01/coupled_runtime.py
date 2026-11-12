"""Bounded sandbox joint checkpoint protocol; no legacy or production compatibility.

One joint snapshot is authoritative. actions.json is a separately durable receipt
ledger, allowed to lead that snapshot, never claimed atomic with it. Fresh runs
only; fixed dt .01/event tick .05; maximum64 observation decisions. No teacher.
"""
import copy
import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
import numpy as np
from .core import Individual,World,CVM,VERSION as CORE_VERSION,canonical,digest,fail
from .contracts import validate_snapshot
from .instinct import ReferenceCGL,Geometry,Parameters,FrozenBias,EventDrive,VERSION as FIELD_VERSION
from .coupling import VERSION as POLICY_VERSION,visible_candidates,rank_candidates,execute_observation
from .durable import DurableRunner,WorldDispatcher
from .dispatch import DispatchJournal

SCHEMA='joint-sandbox-1'
MAX_BYTES=24*1024*1024
VERSIONS={'schema':SCHEMA,'core':CORE_VERSION,'field':FIELD_VERSION,'policy':POLICY_VERSION,
          'numpy':np.__version__,'rng':'numpy-PCG64-initialization-only; no ongoing RNG state'}


def require(condition,message):
    if not condition:fail('JOINT_INVALID',message)


def matrix(value):
    require(isinstance(value,list) and len(value)==64 and all(isinstance(row,list) and len(row)==64 for row in value),'matrix shape')
    require(all(type(v) in (int,float) for row in value for v in row),'matrix numeric types')
    result=np.array(value,dtype=np.float64)
    require(np.isfinite(result).all(),'matrix finite')
    return result


class CoupledRuntime:
    def __init__(self,directory,*,individual=None,solver=None,calibration=None,enabled=True):
        self.directory=Path(directory).resolve()
        if individual is None:require(self.directory.is_dir(),'missing joint directory')
        else:self.directory.mkdir(parents=True,exist_ok=True)
        self.path=self.directory/'joint.json';self.journal_path=self.directory/'actions.json';self.lock_path=self.directory/'joint.lock'
        with self._exclusive():
            if individual is not None:
                require(not self.path.exists() and not self.journal_path.exists(),'new joint run requires empty destination')
                require(isinstance(individual,Individual) and isinstance(solver,ReferenceCGL) and isinstance(calibration,FrozenBias),'initial objects')
                require(individual.state['tick']==0 and individual.state['version']==0 and not individual.state['inbox']
                        and not individual.state['tasks'] and not individual.state['pending'] and not individual.state['requested_actions'],'fresh individual required')
                require(solver.time==0 and solver.steps==0 and solver.dt==.01 and not solver.stopped
                        and not solver.drive.records and solver.drive.source_head=='0'*64,'fresh fixed-clock field required')
                require(type(enabled) is bool,'enabled flag')
                self.individual=individual;self.solver=solver;self.calibration=calibration;self.enabled=enabled
                self.phase='READY';self.previous_target=None;self.traces=[];self.sha=None
                self._save()
            self._load();self._association()

    @contextmanager
    def _exclusive(self):
        try:fd=os.open(self.lock_path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        except FileExistsError:fail('DISPATCH_BUSY','joint lock exists; manual diagnosis required')
        try:yield
        finally:os.close(fd);self.lock_path.unlink()

    def _sync_directory(self):
        if os.name=='posix':
            fd=os.open(self.directory,os.O_RDONLY|getattr(os,'O_DIRECTORY',0))
            try:os.fsync(fd)
            finally:os.close(fd)

    def _payload(self):
        state=self.individual.state;applied=state['tick']-(self.phase=='REQUESTED')
        real=self.solver.field.real.tolist();imag=self.solver.field.imag.tolist()
        return {'versions':VERSIONS,'parent_joint_sha256':self.sha,'phase':self.phase,'enabled':self.enabled,
            'individual':self.individual.snapshot(),'previous_target':self.previous_target,'decision_traces':self.traces,
            'calibration':asdict(self.calibration),'calibration_sha256':digest(asdict(self.calibration)),'field':{'manifest':self.solver.manifest(),
                'real':real,'imag':imag,'sha256':digest({'real':real,'imag':imag}),
                'geometry':self.solver._geometry.tolist()},
            'field_applied_event_tick':applied,'field_applied_event_index':applied-1,
            'field_applied_event_head':state['events'][applied-1]['hash'] if applied else '0'*64}

    def _save(self):
        data=self._payload();self._decode(data) # Validate before publishing or invoking an adapter.
        wrapper={'data':data,'sha256':digest(data)};encoded=canonical(wrapper).encode('utf8')
        require(len(encoded)<=MAX_BYTES,'joint byte budget')
        fd,name=tempfile.mkstemp(prefix='.joint-',dir=self.directory)
        try:
            with os.fdopen(fd,'wb') as stream:
                if stream.write(encoded)!=len(encoded):raise OSError('short joint write')
                stream.flush();os.fsync(stream.fileno())
            os.replace(name,self.path);self._sync_directory();self.sha=wrapper['sha256']
        finally:
            if os.path.exists(name):os.unlink(name)

    def _load(self):
        require(self.path.is_file() and self.path.stat().st_size<=MAX_BYTES,'missing/oversize joint snapshot')
        try:
            wrapper=json.loads(self.path.read_text(encoding='utf8'))
            require(isinstance(wrapper,dict) and set(wrapper)=={'data','sha256'} and digest(wrapper['data'])==wrapper['sha256'],'joint integrity')
            decoded=self._decode(wrapper['data'])
            self.individual,self.solver,self.calibration=decoded
            data=wrapper['data'];self.phase=data['phase'];self.enabled=data['enabled'];self.previous_target=data['previous_target']
            self.traces=data['decision_traces'];self.sha=wrapper['sha256']
        except (KeyError,ValueError,TypeError,OverflowError,UnicodeError,RecursionError,AttributeError) as exc:
            fail('JOINT_INVALID',str(exc))

    @staticmethod
    def _decode(data,*,multi_action=False):
        require(isinstance(data,dict) and set(data)=={'versions','parent_joint_sha256','phase','enabled','individual','previous_target',
            'decision_traces','calibration','calibration_sha256','field','field_applied_event_tick','field_applied_event_index','field_applied_event_head'},'joint fields')
        expected_versions=dict(VERSIONS,schema='joint-sandbox-multi-1') if multi_action else VERSIONS
        require(data['versions']==expected_versions,'joint version mismatch')
        parent=data['parent_joint_sha256']
        require(parent is None or isinstance(parent,str) and len(parent)==64 and all(c in '0123456789abcdef' for c in parent),'parent hash')
        snapshot=data['individual'];validate_snapshot(snapshot)
        require(snapshot['cl_version']==CORE_VERSION and type(snapshot['schema']) is int and snapshot['schema']==1,'core version')
        state=snapshot['state'];phase=data['phase'];requested=phase=='REQUESTED'
        require((parent is None)==(state['tick']==0),'parent lineage presence')
        require(phase in ('READY','REQUESTED') and type(data['enabled']) is bool,'phase/enable')
        require(state['tick']<=128 and state['version']==state['tick'] and state['tick']%2==int(requested),'clock phase')
        require(bool(state['requested_actions'])==requested and not state['pending'] and not state['inbox'] and not state['tasks'],'unsupported queues')
        completed=state['tick']//2
        traces=data['decision_traces'];require(isinstance(traces,list) and len(traces)==completed+int(requested),'trace count')
        for index,event in enumerate(state['events']):
            require(event['kind']=='commit' if index%2==0 else event['kind'] in (('feedback','action_result') if multi_action else ('feedback',)),'joint alternating action history')
            if index%2==0:
                request=event['payload'].get('action_requested');require(isinstance(request,dict) and request['proposal']['kind'] in (('observe','move') if multi_action else ('observe',)),'request shape')
                proposal=request['proposal']
                movement_names={(1,0):'move_east',(-1,0):'move_west',(0,1):'move_south',(0,-1):'move_north'}
                selected=proposal['target'] if proposal['kind']=='observe' else movement_names.get((proposal['dx'],proposal['dy']))
                trace=traces[index//2]
                require(type(trace['source_state_version']) is int and type(trace['source_tick']) is int
                        and trace['source_state_version']==index and trace['source_tick']==index and trace['action_id']==request['action_id']
                        and trace['selected']==selected,'decision request binding')
                require(trace['source_event_head']==(state['events'][index-1]['hash'] if index else '0'*64),'trace source prefix')
                expected_time=0. if index==0 else (index+1)*.05
                require(type(trace['field_time']) in (int,float) and abs(trace['field_time']-expected_time)<1e-12,'decision field clock')
        observations=[e for e in state['events'][:2*completed] if e['kind']=='feedback']
        previous=observations[-1]['payload']['target'] if observations else None
        require(data['previous_target']==previous,'previous target')
        applied=2*completed
        require(type(data['field_applied_event_tick']) is int and data['field_applied_event_tick']==applied
                and type(data['field_applied_event_index']) is int and data['field_applied_event_index']==applied-1
                and data['field_applied_event_head']==(state['events'][applied-1]['hash'] if applied else '0'*64),'field applied prefix')
        raw=data['field'];require(isinstance(raw,dict) and set(raw)=={'manifest','real','imag','geometry','sha256'},'field fields')
        require(raw['sha256']==digest({'real':raw['real'],'imag':raw['imag']}),'field integrity')
        manifest=raw['manifest'];require(manifest['solver']==FIELD_VERSION and manifest['numpy']==np.__version__
                and manifest['shape']==[64,64] and type(manifest['dx']) is int and manifest['dx']==1
                and type(manifest['dy']) is int and manifest['dy']==1 and manifest['boundary']=='periodic'
                and type(manifest['dt']) is float and manifest['dt']==.01 and manifest['stopped'] is False and manifest['stop_reason'] is None,'field manifest')
        expected_time=(2*completed+1)*.05 if completed else 0.
        require(type(manifest['time']) in (int,float) and abs(manifest['time']-expected_time)<1e-12
                and type(manifest['steps']) is int and manifest['steps']==(10*completed+5 if completed else 0),'field clock')
        geometry=matrix(raw['geometry'])
        require(geometry.shape==(64,64) and np.isfinite(geometry).all() and abs(geometry).max()<=1+1e-14 and abs(geometry.mean())<1e-12,'geometry bounds')
        # Preserve exact normalized bytes, NEVER normalize stored geometry again.
        g=object.__new__(Geometry);g._values=geometry.copy();g._values.flags.writeable=False
        g.digest=hashlib.sha256(g.version.encode()+geometry.astype('<f8').tobytes()).hexdigest()
        require(manifest['geometry_version']==g.version and manifest['geometry_hash']==g.digest,'geometry hash')
        real=matrix(raw['real']);imag=matrix(raw['imag'])
        require(real.shape==(64,64) and imag.shape==(64,64) and np.isfinite(real).all() and np.isfinite(imag).all(),'complex field')
        drive=EventDrive(state['events'][:applied],tick_duration=.05)
        require(manifest['drive_hash']==drive.digest and manifest['drive_version']==drive.version
                and manifest['source_event_head']==drive.source_head,'field drive prefix')
        solver=ReferenceCGL(g,seed=manifest['seed'],parameters=Parameters(**manifest['parameters']),method=manifest['method'],
                            dt=.01,amplitude_limit=manifest['amplitude_limit'],drive=drive,initial=real+1j*imag)
        solver.time=manifest['time'];solver.steps=manifest['steps']
        require(solver.manifest()==manifest,'field manifest mismatch')
        cal=data['calibration'];require(data['calibration_sha256']==digest(cal),'calibration integrity')
        calibration=FrozenBias(cal['calibration_id'],tuple(cal['mean']),tuple(cal['scale']),
                                                     tuple(tuple(row) for row in cal['weights']),tuple(cal['offset']),cal['beta'])
        require(set(cal)==set(asdict(calibration)),'calibration fields')
        for trace in traces:
            ranking=rank_candidates(trace['candidate_inputs'],trace['features'],calibration,enabled=data['enabled'])
            require(set(trace)==set(ranking)|{'candidate_inputs','source_state_version','source_tick','source_event_head','field_time','action_id'},'trace fields')
            require(all(canonical(trace.get(key))==canonical(value) for key,value in ranking.items()),'score trace mismatch')
        if requested:
            require(traces[-1]['features']==manifest['features'],'requested field/score binding')
        individual=Individual(World(0));individual.state=copy.deepcopy(state);world=snapshot['world']
        individual.world.objects=copy.deepcopy(world['objects']);individual.world.version=world['version']
        individual.world.local=world['local'];individual.world.width=world['width'];individual.world.height=world['height']
        individual.vm=CVM(snapshot['capabilities']);individual.migrations=copy.deepcopy(snapshot['migrations'])
        return individual,solver,calibration

    def _association(self):
        # Reuse ledger validation only, not DurableRunner's separate checkpoint.
        check=object.__new__(DurableRunner);check.individual=self.individual;check.journal_path=self.journal_path
        check._check_association()

    def step(self):
        with self._exclusive():
            self._load();self._association()
            try:
                dispatcher=WorldDispatcher(self.individual.world,DispatchJournal(self.journal_path))
                if self.phase=='READY':
                    if len(self.traces)>=64 or self.individual.state['lifecycle']!='ACTIVE' or self.individual.state['goal_state']['status']=='BLOCKED':return False
                    candidates=visible_candidates(self.individual,self.previous_target)
                    if not candidates:return False
                    trace=rank_candidates(candidates,self.solver.manifest()['features'],self.calibration,enabled=self.enabled)
                    trace.update(candidate_inputs=copy.deepcopy(candidates),source_state_version=self.individual.state['version'],
                                 source_tick=self.individual.state['tick'],field_time=self.solver.time,
                                 source_event_head=self.individual.state['events'][-1]['hash'] if self.individual.state['events'] else '0'*64)
                    trace['action_id']=execute_observation(self.individual,trace['selected'])
                    self.traces.append(json.loads(canonical(trace)));self.phase='REQUESTED'
                    dispatcher.prepare_request(self.individual.state['requested_actions'][0])
                    self._save() # NO dispatch in this phase, even if rename outcome is uncertain.
                else:
                    self.individual.action_dispatcher=dispatcher
                    require(self.individual.step(),'requested result not processed')
                    self.previous_target=self.individual.state['events'][-1]['payload']['target']
                    self.solver.drive=EventDrive(self.individual.state['events'],tick_duration=.05)
                    self.solver.run((self.individual.state['tick']+1)*.05-self.solver.time)
                    self.phase='READY';self._save()
                return True
            except Exception:
                self._load() # Ledger may be ahead; never roll it back or silently retry UNKNOWN.
                raise
