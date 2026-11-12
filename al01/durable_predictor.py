"""Durable finite predictive observer backed by journal and verified object refs.

No environmental reader is built in. Only an explicitly supplied callback can
execute after a durable UNKNOWN marker. This is not a full-life runtime or an
unbounded observer; the observer's existing64-transition cap is unchanged.
"""
import copy,os
from pathlib import Path
from contextlib import contextmanager
from .life_journal import LifeJournal
from .life_objects import LifeObjects
from .folder_session import _hash,_encode
from .folder_runner import atomic_json
from .controller_paths import check_file,check_directory
from . import predictive_observer as observer

SCHEMA='durable-predictor-1'
REDUCER_ID='durable-predictor-reducer-1'


def need(ok,msg):
    if not ok:raise ValueError('DURABLE_PREDICTOR_INVALID: '+msg)


class DurablePredictor:
    def __init__(self,directory,*,identity,scope_ref,capture_origin=None,reconcile=True):
        self.directory=Path(os.path.abspath(directory));check_directory(self.directory)
        self.identity=identity;self.scope_ref=scope_ref
        need(capture_origin is None or capture_origin in ('APPLICATION_BROKER_CAPTURE','SYNTHETIC_TEST_CAPTURE'),'capture origin')
        self.capture_origin=capture_origin
        observer.initial_observer(identity,scope_ref)
        self.journal_dir=self.directory/'journal';self.objects_dir=self.directory/'objects'
        self.capture_path=self.directory/'capture.json';self.lock_path=self.directory/'predictor.lock'
        need(type(reconcile) is bool,'reconcile Boolean')
        if not reconcile:
            need(all(p.exists() for p in (self.journal_dir/'birth.json',self.journal_dir/'head.json',self.objects_dir/'objects.json')),'validation-only restore requires complete existing birth')
        existing=self.directory.exists()
        if existing:
            with os.scandir(self.directory) as entries:
                for i,e in enumerate(entries):
                    need(i<4 and e.name in {'journal','objects','capture.json','predictor.lock'},'unexpected predictor entries')
            if any(p.exists() for p in (self.journal_dir,self.objects_dir,self.capture_path)):
                need((self.journal_dir/'birth.json').exists() and (self.journal_dir/'head.json').exists() and (self.objects_dir/'objects.json').exists(),'incomplete predictor birth; no reset')
        self.directory.mkdir(parents=True,exist_ok=True)
        with self._locked():
            if (self.journal_dir/'birth.json').exists():
                birth=LifeJournal._read(self.journal_dir/'birth.json')
                need(birth['identity']==identity,'journal identity')
                self.instance_id=birth['instance_id']
                saved_origin=birth['initial_state']['capture_origin']
                need(capture_origin is None or capture_origin==saved_origin,'frozen capture origin')
                self.capture_origin=saved_origin
                self.objects=LifeObjects(self.objects_dir,identity=identity,instance_id=self.instance_id)
                self.journal=LifeJournal(self.journal_dir,reducer=self._reduce,reducer_id=REDUCER_ID,identity=identity)
                need(self.journal.snapshot()['state']['observer_ref'] is not None,'incomplete observer initialization')
            else:
                self.capture_origin=capture_origin or 'APPLICATION_BROKER_CAPTURE'
                initial=dict(schema=SCHEMA,identity=identity,scope_ref=scope_ref,capture_origin=self.capture_origin,observer_ref=None,pending_input_ref=None,pending_effect=None)
                self.journal=LifeJournal(self.journal_dir,reducer=self._reduce,reducer_id=REDUCER_ID,identity=identity,initial_state=initial)
                self.instance_id=self.journal.birth['instance_id']
                self.objects=LifeObjects(self.objects_dir,identity=identity,instance_id=self.instance_id)
                self._commit('INITIALIZE',{})
            self._check_pending()
            self._reconcile_locked(commit=reconcile)

    @contextmanager
    def _locked(self):
        check_directory(self.directory);check_file(self.lock_path);check_file(self.capture_path)
        fd=os.open(self.lock_path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        try:yield
        finally:os.close(fd);self.lock_path.unlink()

    def _resolve(self,ref,kind):
        need(type(ref) is dict and ref.get('identity')==self.identity and ref.get('instance_id')==self.instance_id,'object instance binding')
        return self.objects.read(ref,expected_kind=kind)

    def _observer(self,state=None):
        state=self.journal.snapshot()['state'] if state is None else state
        need(state['identity']==self.identity and state['scope_ref']==self.scope_ref,'predictor identity/scope')
        value=self._resolve(state['observer_ref'],'observer_state')
        observer.validate_observer(value)
        need(value['identity']==self.identity and value['scope_ref']==self.scope_ref,'observer identity/scope')
        return value

    def _next(self,current,operation,args):
        fields={'INITIALIZE':set(),'PREPARE':{'request','cognition','logical_budget'},'OBSERVE':{'item','cognition_before'},
                'DECLARE_STUDY':{'plan','cognition'},'PROMOTE':{'report','cognition'},'ROLLBACK':{'retention_captures','cognition'},'START':set()}
        need(operation in fields and type(args) is dict and set(args)==fields[operation],'exact operation arguments')
        if operation=='INITIALIZE':
            need(current is None and args=={},'initial observer only')
            return observer.initial_observer(self.identity,self.scope_ref)
        need(current is not None,'observer initialized')
        if operation=='PREPARE':return observer.prepare_prediction(current,args['request'],args['cognition'],logical_budget=args['logical_budget'])
        if operation=='OBSERVE':return observer.observe_capture(current,args['item'],args['cognition_before'])
        if operation=='DECLARE_STUDY':return observer.declare_study(current,args['plan'],args['cognition'])
        if operation=='PROMOTE':return observer.promote_model(current,args['report'],args['cognition'])
        if operation=='ROLLBACK':return observer.rollback_model(current,args['retention_captures'],args['cognition'])
        if operation=='START':need(args=={},'start args');return current
        need(False,'unknown observer operation')

    def _reduce(self,state,event):
        need(type(state) is dict and set(state)=={'schema','identity','scope_ref','capture_origin','observer_ref','pending_input_ref','pending_effect'} and state['schema']==SCHEMA,'compact state fields')
        need(state['identity']==self.identity and state['scope_ref']==self.scope_ref and state['capture_origin']==self.capture_origin,'compact identity/scope/origin')
        p=event['payload'];need(type(p) is dict and set(p)=={'input_ref','observer_ref'},'transition ref fields')
        inp=self._resolve(p['input_ref'],'observer_transition')
        need(type(inp) is dict and set(inp)=={'schema','identity','instance_id','operation','arguments','parent_observer_ref'},'transition object fields')
        op=event['kind'];need(inp['schema']==SCHEMA and inp['identity']==self.identity and inp['instance_id']==self.instance_id and inp['operation']==op,'transition object source')
        need(inp['parent_observer_ref']==state['observer_ref'],'exact observer ancestry')
        current=None if state['observer_ref'] is None else self._observer(state)
        expected=self._next(current,op,inp['arguments'])
        need(event['perception_count']==len(expected['observations']),'perception count equals actual captured observations')
        result=self._resolve(p['observer_ref'],'observer_state')
        need(_encode(result)==_encode(expected),'observer transition exact replay')
        out=copy.deepcopy(state);out['observer_ref']=p['observer_ref'];effect=event['effect']
        if op=='PREPARE':
            need(state['pending_effect'] is None and effect is not None and effect['phase']=='REQUEST','prepare effect request')
            out['pending_effect']=effect['id'];out['pending_input_ref']=p['input_ref']
        elif op=='START':
            need(state['pending_effect'] is not None and effect=={'phase':'START','id':state['pending_effect']},'start exact effect')
        elif op=='OBSERVE':
            need(inp['arguments']['item']['capture']['capture_origin']==self.capture_origin,'replayed capture origin differs from immutable configuration')
            need(state['pending_effect'] is not None and effect=={'phase':'RECEIPT','id':state['pending_effect']},'capture exact effect')
            before=self._resolve(state['pending_input_ref'],'observer_transition')['arguments']
            need(inp['arguments']['cognition_before']==before['cognition'] and inp['arguments']['item']['request']==before['request'],'capture pre-read source binding')
            out['pending_effect']=None;out['pending_input_ref']=None
        else:need(effect is None,'control cannot claim external effect')
        return out

    def _commit(self,operation,args,effect=None):
        state=self.journal.snapshot()['state'];current=None if state['observer_ref'] is None else self._observer(state)
        next_state=self._next(current,operation,args)
        inp=dict(schema=SCHEMA,identity=self.identity,instance_id=self.instance_id,operation=operation,arguments=copy.deepcopy(args),parent_observer_ref=state['observer_ref'])
        input_ref=self.objects.put_json('observer_transition',inp)
        output_ref=self.objects.put_json('observer_state',next_state)
        # Publishing objects alone never changes the authoritative observer.
        return self.journal.append(operation,dict(input_ref=input_ref,observer_ref=output_ref),
            perception_count=self.journal.snapshot()['perception_count']+(operation=='OBSERVE'),effect=effect)

    def _check_pending(self):
        head=self.journal.snapshot();s=head['state'];self._observer(s)
        need(set(head['pending'])==({s['pending_effect']} if s['pending_effect'] is not None else set()),'pending effect/index binding')
        if s['pending_effect'] is not None:
            inp=self._resolve(s['pending_input_ref'],'observer_transition')
            need(inp['operation']=='PREPARE' and head['pending'][s['pending_effect']]['request']['input_ref']==s['pending_input_ref'],'pending pre-read object binding')

    def prepare(self,request,cognition,*,logical_budget=2):
        with self._locked():
            self._check_pending();self._reconcile_locked()
            state=self.journal.snapshot()['state']
            if state['pending_effect'] is not None:
                prior=self._resolve(state['pending_input_ref'],'observer_transition')['arguments']
                need(prior==dict(request=request,cognition=cognition,logical_budget=logical_budget),'conflicting pending preparation')
                return copy.deepcopy(self._observer()['pending'])
            self._commit('PREPARE',dict(request=request,cognition=cognition,logical_budget=logical_budget),effect={'phase':'REQUEST'})
            return copy.deepcopy(self._observer()['pending'])

    def _start_locked(self):
        self._check_pending();s=self.journal.snapshot()['state'];eid=s['pending_effect']
        need(eid is not None,'no prepared read')
        status=self.journal.snapshot()['pending'][eid]['status']
        need(status=='REQUESTED','UNKNOWN effect must not be dispatched or reread')
        self._commit('START',{},effect={'phase':'START','id':eid})
        return copy.deepcopy(self._observer()['pending']['request'])

    def mark_started(self):
        with self._locked():return self._start_locked()

    def execute(self,callback):
        """Explicit caller callback only; callback must return exact capture item."""
        with self._locked():
            request=self._start_locked() # durable UNKNOWN before callback
            item=callback(copy.deepcopy(request))
            return self._capture_locked(item)

    def _capture_locked(self,item):
        self._check_pending();s=self.journal.snapshot()['state'];eid=s['pending_effect']
        if eid is None:
            for row in self._observer()['observations']:
                if row['item']['request']['request_id']==item.get('request',{}).get('request_id'):
                    need(row['item']==item,'conflicting completed capture');return copy.deepcopy(item)
        need(eid is not None and self.journal.snapshot()['pending'][eid]['status']=='UNKNOWN','capture requires started pending effect')
        args=self._resolve(s['pending_input_ref'],'observer_transition')['arguments']
        # Validate actual domain transition before admitting any durable pointer.
        need(item['capture']['capture_origin']==self.capture_origin,'capture origin differs from birth configuration')
        observer.observe_capture(self._observer(),item,args['cognition'])
        capture_ref=self.objects.put_json('prediction_capture',dict(schema=SCHEMA,identity=self.identity,instance_id=self.instance_id,effect_id=eid,item=item))
        pointer=dict(schema=SCHEMA,identity=self.identity,instance_id=self.instance_id,effect_id=eid,capture_ref=capture_ref)
        if self.capture_path.exists():
            old=LifeJournal._read(self.capture_path)
            if old.get('effect_id')==eid:need(old==pointer,'conflicting duplicate capture')
        atomic_json(self.capture_path,{'data':pointer,'sha256':_hash(pointer)});LifeJournal._sync(self.directory)
        self._reconcile_locked();return copy.deepcopy(item)

    def capture(self,item):
        with self._locked():return self._capture_locked(item)

    def _reconcile_locked(self,*,commit=True):
        if not self.capture_path.exists():return False
        pointer=LifeJournal._read(self.capture_path)
        need(type(pointer) is dict and set(pointer)=={'schema','identity','instance_id','effect_id','capture_ref'} and pointer['schema']==SCHEMA and pointer['identity']==self.identity and pointer['instance_id']==self.instance_id,'capture pointer identity')
        captured=self._resolve(pointer['capture_ref'],'prediction_capture')
        need(type(captured) is dict and set(captured)=={'schema','identity','instance_id','effect_id','item'} and all(captured[k]==pointer[k] for k in ('schema','identity','instance_id','effect_id')),'capture object binding')
        item=captured['item'];need(item['capture']['capture_origin']==self.capture_origin,'frozen capture origin')
        state=self.journal.snapshot()['state'];eid=state['pending_effect']
        if eid!=pointer['effect_id']:
            matched=False
            for event in self.journal.iter_events():
                if event['kind']=='OBSERVE' and event['effect']=={'phase':'RECEIPT','id':pointer['effect_id']}:
                    inp=self._resolve(event['payload']['input_ref'],'observer_transition')
                    matched=inp['arguments']['item']==item
            need(matched,'orphan capture pointer cannot grant authority')
            return False
        need(self.journal.snapshot()['pending'][eid]['status']=='UNKNOWN','capture pointer without durable dispatch start')
        args=self._resolve(state['pending_input_ref'],'observer_transition')['arguments']
        observer.observe_capture(self._observer(),item,args['cognition'])
        if not commit:return False # validated pending capture; no publication or progress
        self._commit('OBSERVE',dict(item=item,cognition_before=args['cognition']),effect={'phase':'RECEIPT','id':eid})
        self._check_pending();return True

    def reconcile(self):
        with self._locked():return self._reconcile_locked()

    def declare_study(self,plan,cognition):
        with self._locked():return self._commit('DECLARE_STUDY',dict(plan=plan,cognition=cognition))

    def promote_model(self,report,cognition):
        with self._locked():return self._commit('PROMOTE',dict(report=report,cognition=cognition))

    def rollback_model(self,retention_captures,cognition):
        with self._locked():return self._commit('ROLLBACK',dict(retention_captures=retention_captures,cognition=cognition))

    def snapshot(self):
        with self._locked():
            self._check_pending()
            return dict(identity=self.identity,instance_id=self.instance_id,scope_ref=self.scope_ref,journal=self.journal.snapshot(),observer=self._observer(),limits='finite observer64 events; not full runtime integration or maturity')

    def captured_item(self,request_id):
        """Exact completed receipt for ledger repair; never dispatches or senses."""
        with self._locked():
            for row in self._observer()['observations']:
                if row['item']['request']['request_id']==request_id:return copy.deepcopy(row['item'])
            return None

    def source_cognition(self,request_id):
        """Persisted pre-read source, including pending committed predictions."""
        with self._locked():
            for event in self._observer()['events']:
                if event['kind']=='PREPARE' and event['payload']['request']['request_id']==request_id:
                    return copy.deepcopy(event['payload']['cognition'])
            return None

    @property
    def observer_state(self):
        with self._locked():return copy.deepcopy(self._observer())
