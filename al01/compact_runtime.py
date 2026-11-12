"""Bounded synthetic cognition with one authoritative individual journal.

An explicit one-time receipt-continuation grant can extend the same current
compact identity without resetting its v0.32 history. No original grid,
v0.14 cloud, or Windows individual is migrated here. Local interaction uses
only explicitly registered synthetic endpoints; this module has no native
filesystem reader, external transport, or teacher access. Learning and
continuation remain finite experiments, not indefinite lifelong operation.
"""
import copy
import json
import os
from pathlib import Path
from contextlib import contextmanager
from dataclasses import asdict
from .life_journal import LifeJournal
from . import life_journal as journal_limits
from .life_objects import LifeObjects, ObjectProfile, MAX_HEADER, _json as object_json
from .folder_runner import atomic_json
from .folder_session import _hash, _encode
from .folder_contracts import make_request, validate_relative_path
from .core import Compiler, CVM, CLError, transaction_payload
from .contracts import execute_neuron, NEURONS
from .control_embryos import initial_control, update_control, candidate_offset, governance_allocate
from .folder_agent import independent_calibration, decode_calibration
from .coupling import rank_candidates
from .controller_paths import check_directory, check_file
from .compact_field import initial_field, validate_field, advance_field
from . import compact_field as field_limits
from . import instinct as numerical_limits
from . import compact_observer as observer
from . import compact_learning as learning
from . import compact_semantics as semantics
from . import compact_procedures as procedures
from . import compact_interleaving as interleaving
from . import compact_deferred_investigation as deferred
from . import compact_guarded_procedures as guarded
from . import compact_regulation as regulation
from . import compact_interaction as interaction
from . import compact_help as help_policy
from . import compact_help_roles as help_roles
from . import compact_choice_learning as choice_learning
from . import compact_continuation as continuation
from . import compact_native_order_evidence as native_order_evidence
from . import compact_native_acquisition as acquisition
from . import compact_derived_observation as derived
from . import compact_native_consumer as consumer

SCHEMA = 'compact-continuation-6'
REDUCER = 'compact-continuation-reducer-6'
POLICY = 'global-eligible-prospective-renewal-7'
ERROR_ATTENTION_CAP = .02
SERVICE_DEBT_LIMIT = 8
MAX_DECISION_BYTES = 16384
MAX_LOCAL_DECISION_BYTES = 32768
MAX_FIELD_BYTES = 262144
MAX_CAPTURE_BYTES = 400000
V026_PROFILE = ObjectProfile(max_reservations=8192,max_reserved_bytes=2*1024**3)
MAX_COUNTER = 2**53-1
RENEWAL_PROFILE = 'finite-3-cohort-256-exposure-1'
REFRESH_BLOCK_PROFILE = 'block-unbound-after-refresh-1'
MAX_COHORT_BYTES = 65536
PUBLICATION_ENVELOPE = 2*(MAX_CAPTURE_BYTES+MAX_FIELD_BYTES+semantics.MAX_INDEX_BYTES+procedures.MAX_WORKFLOW_BYTES+4*(MAX_HEADER+1))


def need(ok, why):
    if not ok: raise ValueError('COMPACT_RUNTIME_INVALID: '+why)


def _counter(n): need(type(n) is int and 0 <= n <= MAX_COUNTER, 'bounded counter')
def _token(path): return 'file_'+_hash(path)


def _birth(identity, scope_ref, goals, seed, field_enabled, u0_enabled, neuron_feedback_enabled=True, prediction_error_enabled=True,procedure_probe=None,probe_contract=None,regulation_enabled=True,local_endpoints=None,regulation_channels=None,autonomous_help=None,renewal_profile=None,retention_probe=None,choice_profile=None,choice_context=None,native_binding=None,procedure_learning_profile=None,investigation_queue_profile=None,procedure_extraction_profile=None,investigation_refresh_profile=None,help_continuation_profile=None,native_order_evidence_profile=None,native_acquisition_profile=None,derived_observation_profile=None,native_consumer_profile=None):
    from .folder_cognition import initial_cognition
    initial_cognition(identity, scope_ref)
    semantics.initial_semantics(identity,'0'*32,scope_ref)  # Validate combined birth before any write.
    procedures.initial_workflow(procedure_probe)
    interleaving.validate_profile(procedure_learning_profile)
    deferred.validate_profile(investigation_queue_profile)
    guarded.validate_profile(procedure_extraction_profile)
    need(investigation_refresh_profile is None or investigation_refresh_profile==REFRESH_BLOCK_PROFILE,'supported investigation refresh profile')
    need(help_continuation_profile is None or help_continuation_profile==help_roles.PROFILE,'supported help continuation profile')
    endpoints=interaction.validate_registry([] if local_endpoints is None else local_endpoints)
    channels=['C','T','P'] if regulation_channels is None else copy.deepcopy(regulation_channels)
    need(type(channels) is list and len(channels)<=3 and all(type(c) is str and c in ('C','T','P') for c in channels) and len(set(channels))==len(channels),'declared appraisal channels')
    need((procedure_probe is None and probe_contract is None) or type(probe_contract) is dict or type(retention_probe) is dict,'probe contract required')
    need(type(seed) is int and 0 <= seed < 2**64 and seed != 991, 'independent live seed')
    need(all(type(x) is bool for x in (field_enabled,u0_enabled,neuron_feedback_enabled,prediction_error_enabled,regulation_enabled)), 'ablation flags')
    need(type(goals) is list and 1 <= len(goals) <= 8, 'one to eight persistent monitoring goals')
    names = set(); checked = []
    for g in goals:
        need(type(g) is dict and set(g) == {'path','period'}, 'goal fields')
        validate_relative_path(g['path'])
        need(g['path'].lower().endswith('.txt') and g['path'].casefold() not in names, 'distinct text goal')
        need(type(g['period']) is int and 1 <= g['period'] <= 10000, 'positive logical freshness period')
        names.add(g['path'].casefold()); checked.append(dict(g,status='ACTIVE',changes=0))
    if autonomous_help is not None:
        need(type(autonomous_help) is dict and set(autonomous_help)=={'endpoint','deadline_span'},'help config fields')
        need(type(autonomous_help['deadline_span']) is int and 1<=autonomous_help['deadline_span']<=32,'help deadline span')
        need(any(e['target']==autonomous_help['endpoint'] and 'request_help' in e['actions'] for e in endpoints),'explicit registered help endpoint')
    need(renewal_profile is None or renewal_profile==RENEWAL_PROFILE,'explicit finite renewal profile')
    if retention_probe is not None:
        from .compact_skill_retention import validate_probe, probe_help_config
        validate_probe(retention_probe,procedure_probe)
        need(choice_context==retention_probe.get('choice_context'),'exact retention choice context')
        need(renewal_profile is None and probe_contract is None and (endpoints,autonomous_help)==probe_help_config(retention_probe['contract']),'separate closed retention probe mode')
        need(all((field_enabled,u0_enabled,neuron_feedback_enabled,prediction_error_enabled,regulation_enabled)) and channels==['C','T','P'],'all-enabled retention context')
    calibration, provenance = independent_calibration()
    result = dict(schema=SCHEMA, identity=identity, scope_ref=scope_ref, instance_id=None,
        config=dict(renewal_profile=renewal_profile,retention_probe=copy.deepcopy(retention_probe),seed=seed,field_enabled=field_enabled,u0_enabled=u0_enabled,
                    neuron_feedback_enabled=neuron_feedback_enabled,prediction_error_enabled=prediction_error_enabled,regulation_enabled=regulation_enabled,regulation_channels=channels,appraisal_policy='source-sampling-opportunity-1',
                    autonomous_help=copy.deepcopy(autonomous_help),compute_budget=4,external_action_budget=2 if endpoints else 1,local_endpoints=endpoints,
                    capabilities=['read','observe','neuron']+sorted({a for e in endpoints for a in e['actions']}),policy_version=POLICY,
                    calibration=asdict(calibration),calibration_provenance=provenance,
                    capture_origin='SYNTHETIC_TEST_CAPTURE',object_profile=asdict(V026_PROFILE),
                    storage_profile='v030-cohort-8192-2GiB',semantic_parser=semantics.PARSER,
                    procedure_probe=copy.deepcopy(procedure_probe),probe_contract=copy.deepcopy(probe_contract),
                    mode='SYNTHETIC_RETENTION_PROBE' if retention_probe else 'SYNTHETIC_PROCEDURE_PROBE' if probe_contract else 'SYNTHETIC_CONTINUATION'),
        logical_time=0,cognition_revision=0,perceptions=0,governed_attempts=0,
        effective_feedback_updates=0,completed_change_loops=0,field_steps=0,
        inventory=dict(valid=False,generation=0,list_receipt_ref=None,entries=[],next_list_time=0),
        goals=checked,latest_by_path={},working=[],control=initial_control(),observer=observer.initial_observer(),
        field_ref=None,field_features=None,active_model_ref=None,pending=None,last_receipt=None,last_path=None,
        learning=dict(study=None,retention=None,previous_model_ref=None,last_control_ref=None),
        u0_investigation=[0.]*len(checked),service_debt=[0]*len(checked),
        semantics_ref=None,semantic_status='EMPTY',semantic_queue=[],
        cohorts_ref=None,workflow_ref=None,last_service_family=None,last_local_receipt=None,local_outcomes=0,local_responses=0,
        module_status=dict(ucm='PERSISTENT_DETERMINISTIC_U0_NOT_TRAINED',
            registered_neurons=['compare','prediction_error'],archive_recall='ADDRESSED_OBJECT_READ_NOT_LEGACY_RECALL',
            emotions=dict(status='BOUNDED_EVIDENCE_CTP_PROXIES',version=regulation.VERSION,K=.5,K_missing=True),
            semantic='BOUNDED_UNVERIFIED_DECLARATIONS_WITH_CONFLICTS',procedural='FINITE_EMPIRICAL_PAIR_EXTRACTION_WITH_TEST_GATE',
            validated_model_adoption='PROSPECTIVE_MEASURED_WIDTH_ONLY',local_actions='EXPLICIT_SYNTHETIC_ENDPOINTS_ONLY',
            teacher='LOCKED',metabolism='LOGICAL_QUOTAS_ONLY_NO_HOMEOSTASIS',legacy_migration='NOT_PERFORMED'))
    if help_continuation_profile is not None:
        need(all(x is None for x in (procedure_probe,probe_contract,retention_probe,renewal_profile,choice_profile,choice_context)), 'captured help continuation is source-only')
        result['config']['help_continuation_profile']=help_continuation_profile
    if native_order_evidence_profile is not None:
        need(native_order_evidence_profile==native_order_evidence.PROFILE,'supported passive native order evidence profile')
        need(all(x is None for x in (procedure_probe,probe_contract,retention_probe,renewal_profile,
             choice_profile,choice_context,native_binding,procedure_learning_profile,investigation_queue_profile,
             procedure_extraction_profile,investigation_refresh_profile,help_continuation_profile,autonomous_help))
             and not endpoints,'isolated passive native order evidence profile')
        result['config']['native_order_evidence_profile']=native_order_evidence_profile
        result['native_order_evidence']=native_order_evidence.initial()
    if native_acquisition_profile is not None:
        need(native_acquisition_profile==acquisition.PROFILE and native_order_evidence_profile==native_order_evidence.PROFILE,
             'supported acquisition profile requires passive evidence birth')
        result['config']['native_acquisition_profile']=native_acquisition_profile
        result['native_acquisition']=acquisition.initial()
    if derived_observation_profile is not None:
        need(derived_observation_profile==derived.PROFILE and native_acquisition_profile==acquisition.PROFILE
             and native_order_evidence_profile==native_order_evidence.PROFILE,'derived memory requires explicit acquisition/evidence birth')
        result['config']['derived_observation_profile']=derived_observation_profile
        result['derived_observation_ref']=None
    if native_consumer_profile is not None:
        need(native_acquisition_profile==acquisition.PROFILE and native_order_evidence_profile==native_order_evidence.PROFILE,
             'native consumer requires isolated acquisition birth')
        result['config']['native_consumer_profile']=consumer.config(native_consumer_profile)
        result['native_consumer_ref']=None
    if investigation_refresh_profile is not None:
        need(all(x is None for x in (procedure_probe,probe_contract,retention_probe,renewal_profile,choice_profile,choice_context)), 'refresh blocking is source-only')
        result['config']['investigation_refresh_profile']=investigation_refresh_profile
    if procedure_extraction_profile is not None:
        need(all(x is None for x in (procedure_probe,probe_contract,retention_probe,renewal_profile,choice_context)), 'guarded extraction is source-only')
        result['config']['procedure_extraction_profile']=procedure_extraction_profile
    if investigation_queue_profile is not None:
        need(all(x is None for x in (procedure_probe,probe_contract,retention_probe,renewal_profile,choice_context)), 'deferred profile is source-only, not closed probe or renewal')
        result['config']['investigation_queue_profile']=investigation_queue_profile
    if procedure_learning_profile is not None:
        result['config']['procedure_learning_profile']=procedure_learning_profile
    if retention_probe is not None:result['observer']=observer.activate(result['observer'],retention_probe['model'])
    if choice_profile is not None:
        need(renewal_profile is None and choice_context is None,'separate finite choice profile')
        result['config']['choice_profile']=choice_learning.validate_profile(choice_profile,checked)
        result['choice_ref']=None
    if choice_context is not None:
        result['config']['choice_context']=choice_learning.validate_context(choice_context)
    if native_binding is not None:
        from .compact_native import validate_binding
        binding=validate_binding(native_binding)
        need(all(x is None for x in (procedure_probe,probe_contract,retention_probe,renewal_profile,choice_context)),
             'native bridge excludes probes, renewal and imported choice context')
        need({e['target'] for e in endpoints}=={e['target'] for e in binding['endpoints']},'exact native endpoint registry')
        result['config'].update(native_binding=binding,capture_origin='APPLICATION_BROKER_CAPTURE',mode='APPLICATION_BROKER_CONTINUATION')
        result['module_status']['local_actions']='EXPLICIT_LOOPBACK_ENDPOINTS_ONLY'
    return json.loads(_encode(result))


class CompactRuntime:
    @classmethod
    def fresh(cls, directory, *, identity, scope_ref, goals, seed=17, field_enabled=True, u0_enabled=True, neuron_feedback_enabled=True,prediction_error_enabled=True,procedure_probe=None,probe_contract=None,regulation_enabled=True,local_endpoints=None,regulation_channels=None,autonomous_help=None,renewal_profile=None,retention_probe=None,choice_profile=None,choice_context=None,native_binding=None,procedure_learning_profile=None,investigation_queue_profile=None,procedure_extraction_profile=None,investigation_refresh_profile=None,help_continuation_profile=None,native_order_evidence_profile=None,native_acquisition_profile=None,derived_observation_profile=None,native_consumer_profile=None):
        return cls(directory, _initial=_birth(identity,scope_ref,goals,seed,field_enabled,u0_enabled,neuron_feedback_enabled,prediction_error_enabled,procedure_probe,probe_contract,regulation_enabled,local_endpoints,regulation_channels,autonomous_help,renewal_profile,retention_probe,choice_profile,choice_context,native_binding,procedure_learning_profile,investigation_queue_profile,procedure_extraction_profile,investigation_refresh_profile,help_continuation_profile,native_order_evidence_profile,native_acquisition_profile,derived_observation_profile,native_consumer_profile))

    def __init__(self, directory, *, _initial=None):
        self.directory = Path(os.path.abspath(directory)); check_directory(self.directory)
        self.journal_dir = self.directory/'journal'; self.objects_dir = self.directory/'objects'
        self.lock_path = self.directory/'runtime.lock'; self.capture_path = self.directory/'capture.json'
        if _initial is not None:
            need(not self.directory.exists(), 'fresh directory must not exist')
            self.directory.mkdir(parents=True,exist_ok=False)
            self.identity = _initial['identity']; self.instance_id = None
            self.journal = LifeJournal(self.journal_dir,reducer=self._reduce,reducer_id=REDUCER,
                                       identity=self.identity,initial_state=_initial)
            self.instance_id = self.journal.birth['instance_id']
            self.objects = LifeObjects(self.objects_dir,identity=self.identity,instance_id=self.instance_id,profile=V026_PROFILE)
            field = initial_field(_initial['config']['seed'])
            ref = self._put('compact_field',field,MAX_FIELD_BYTES)
            model = learning.uniform_model() if _initial['config']['retention_probe'] is None else _initial['config']['retention_probe']['model']
            model_ref = self._put('compact_model',model,learning.MAX_MODEL_BYTES)
            semantic_ref=self._put('compact_semantics',semantics.initial_semantics(self.identity,self.instance_id,_initial['scope_ref']),semantics.MAX_INDEX_BYTES)
            workflow_ref=self._put('compact_workflow',self._workflow_birth(_initial),procedures.MAX_WORKFLOW_BYTES)
            payload={'field_ref':ref,'model_ref':model_ref,'semantics_ref':semantic_ref,'workflow_ref':workflow_ref}
            if 'native_consumer_ref' in _initial:
                o=dict(identity=self.identity,instance_id=self.instance_id,scope_ref=_initial['scope_ref'])
                policy_ref=self._put(consumer.POLICY_KIND,consumer.initial_policy(o),consumer.POLICY_BYTES)
                payload['native_consumer_ref']=self._put(consumer.ROOT_KIND,consumer.initial_root(o,policy_ref,_initial['config']['native_consumer_profile']),consumer.ROOT_BYTES)
            if 'choice_profile' in _initial['config']:
                payload['choice_ref']=self._put('compact_choice_state',choice_learning.initial(),choice_learning.MAX_BYTES)
            if _initial['config']['renewal_profile'] is not None:
                from . import compact_cohorts as cohorts
                payload['cohorts_ref']=self._put('compact_cohorts',cohorts.initial_registry(self.identity,self.instance_id,_initial['scope_ref'],[g['path'] for g in _initial['goals']]),MAX_COHORT_BYTES)
            self.journal.append('INITIALIZE',payload)
        else:
            need(self.directory.is_dir(), 'missing compact birth')
            with os.scandir(self.directory) as entries:
                for i,e in enumerate(entries):
                    need(i < 4 and e.name in {'journal','objects','runtime.lock','capture.json'}, 'runtime layout')
            for path in (self.journal_dir/'birth.json',self.journal_dir/'head.json',self.objects_dir/'objects.json'):
                check_file(path); need(path.is_file(), 'partial birth; no inferred reset')
            birth = LifeJournal._read(self.journal_dir/'birth.json')
            need(birth.get('reducer_id')==REDUCER, 'legacy reducer is not migrated')
            self.identity = birth['identity']; self.instance_id = birth['instance_id']
            need(birth['initial_state']['config']['object_profile']==asdict(V026_PROFILE),'exact versioned storage profile')
            self.objects = LifeObjects(self.objects_dir,identity=self.identity,instance_id=self.instance_id,profile=V026_PROFILE)
            self.journal = LifeJournal(self.journal_dir,reducer=self._reduce,reducer_id=REDUCER,identity=self.identity)
            need(self.journal.snapshot()['state']['field_ref'] is not None, 'incomplete initialized birth')
        with self._locked(): self._reconcile_locked()

    @contextmanager
    def _locked(self):
        check_directory(self.directory); check_file(self.lock_path); check_file(self.capture_path)
        fd = os.open(self.lock_path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        try:
            need(_hash(LifeJournal._read(self.journal.head_path)) == self.journal.head_sha, 'stale runtime; reopen')
            self._registry_read_cache={}
            self._continuation_read_cache={}
            yield
        finally:
            self._registry_read_cache=None
            self._continuation_read_cache=None
            os.close(fd); self.lock_path.unlink()

    def _put(self, kind, data, limit, *, lease=None):
        need(len(_encode(data)) <= limit, kind+' byte budget')
        return (self.objects if lease is None else lease).put_json(kind,data)

    def _read(self, ref, kind):
        need(type(ref) is dict and ref.get('identity') == self.identity and ref.get('instance_id') == self.instance_id, 'object identity/instance')
        cache=(getattr(self,'_continuation_read_cache',None) if kind=='compact_receipt_continuation' else
               getattr(self,'_registry_read_cache',None) if kind in ('compact_cohorts','compact_choice_state') else None)
        key=_hash(ref)
        if cache is not None and key in cache:return copy.deepcopy(cache[key])
        value=self.objects.read(ref,expected_kind=kind)
        if cache is not None:
            if len(cache)>=2:cache.pop(next(iter(cache)))
            cache[key]=copy.deepcopy(value)
        return value

    def _read_decision(self,ref):
        kind=ref.get('kind') if type(ref) is dict else None
        need(kind in ('compact_decision','compact_local_decision'),'typed decision reference')
        value=self._read(ref,kind);local=kind=='compact_local_decision'
        need(('local_request_ref' in value)==local,'decision family/reference mismatch')
        need(value.get('schema')==('compact-local-decision-1' if local else 'compact-decision-1'),'decision schema')
        need(len(_encode(value))<=(MAX_LOCAL_DECISION_BYTES if local else MAX_DECISION_BYTES),'decision family byte bound')
        return value

    def _source(self, state, seq, head):
        source=dict(identity=state['identity'],instance_id=state['instance_id'],scope_ref=state['scope_ref'],
            global_seq=seq,journal_head=head,cognition_revision=state['cognition_revision'],
            state_sha256=_hash(state),inventory_generation=state['inventory']['generation'],
            list_receipt_ref=state['inventory']['list_receipt_ref'],visible_sha256=_hash(state['inventory']['entries']),
            capabilities=state['config']['capabilities'],policy_version=POLICY,
            model_ref=state['active_model_ref'],
            model_sha256=observer.model_identity(state['observer']))
        if 'choice_context' in state['config']:source['choice_policy_sha256']=state['config']['choice_context']['policy_sha256']
        if 'choice_profile' in state['config']:
            source['choice_state_ref']=state['choice_ref']
            source['choice_policy_sha256']=_hash(self._choice(state)['policy'])
        if 'native_consumer_ref' in state:source['native_consumer_ref']=state['native_consumer_ref']
        return source

    def _consumer(self,state):
        need('native_consumer_ref' in state,'explicit native consumer birth')
        root=self._read(state['native_consumer_ref'],consumer.ROOT_KIND)
        policy=self._read(root['policy_ref'],consumer.POLICY_KIND)
        consumer.validate(root,policy,consumer.origin(state),state['config']['native_consumer_profile'])
        return root,policy

    def _consumer_control(self,kind,task_id=None):
        with self._locked():return self._consumer_control_locked(kind,task_id)

    def _consumer_control_locked(self,kind,task_id=None):
        self._reconcile_locked();head=self.journal.snapshot();state=head['state'];root,policy=self._consumer(state)
        need(state['pending'] is None,'task controls cannot bypass UNKNOWN/pending')
        if kind=='NATIVE_POLICY_FREEZE' and root['frozen']:return copy.deepcopy(root)
        if kind=='NATIVE_TASK_BEGIN' and root['current'] is not None and root['current']['id']==task_id:return copy.deepcopy(root)
        if kind=='NATIVE_TASK_BEGIN':need(consumer.allowed(state,self._workflow(state)),'safe post-acquisition task begin')
        source=self._source(state,head['seq'],head['event_head'])
        nxt,task,newpolicy=consumer.control(state,root,policy,state['config']['native_consumer_profile'],kind,task_id,source,head['seq']+1)
        artifacts=[]
        for obj,k,limit in ((task,consumer.TASK_KIND,consumer.TASK_BYTES),(newpolicy,consumer.POLICY_KIND,consumer.POLICY_BYTES),(nxt,consumer.ROOT_KIND,consumer.ROOT_BYTES)):
            if obj is not None:
                frame,ref=self.objects._frame(k,object_json(obj,limit),'json');artifacts.append((k,obj,ref,len(frame)))
        rootref=artifacts[-1][2];projected=copy.deepcopy(state);projected['native_consumer_ref']=rootref
        need(len(_encode(projected))<=65536,'consumer control live capacity')
        record=dict(schema=state['config']['native_consumer_profile']['schema'],kind=kind,task_id=task_id,source=source,root_ref=rootref)
        frame,recordref=self.objects._frame('compact_native_consumer_control',object_json(record,consumer.CONTROL_BYTES),'json')
        required=2*(len(frame)+sum(a[3] for a in artifacts))
        with self.objects.publication_lease(max_objects=len(artifacts)+1,max_reserved_bytes=required) as lease:
            for k,obj,ref,_ in artifacts:need(lease.put_json(k,obj)==ref,'consumer control artifact')
            need(lease.put_json('compact_native_consumer_control',record)==recordref,'consumer control source')
            self.journal.append(kind,dict(record_ref=recordref))
        return copy.deepcopy(nxt)

    def consumer_task_begin(self,task_id,now=None):
        with self._locked():
            self._reconcile_locked();state=self.journal.snapshot()['state'];root,_=self._consumer(state)
            cfg=state['config']['native_consumer_profile']
            if cfg['schema'] in consumer.REFRESH_PROFILES:
                spec=consumer.task_spec(cfg,task_id);start=spec['start'] if now is None else now
                need(type(start) is int and start==spec['start'],'exact declared task start')
                if root['current'] is None or root['current']['id']!=task_id:
                    need(consumer.allowed(state,self._workflow(state)),'safe post-acquisition task begin')
                    need(task_id not in {r['id'] for r in root['completed']},'task cannot be reused')
                    if spec['partition']=='TRAIN':need(not root['frozen'],'training after freeze prohibited')
                    else:need(root['frozen'],'evaluation requires frozen policy')
                    remaining=[t['id'] for t in cfg['tasks'] if t['id'] not in {r['id'] for r in root['completed']}]
                    need(root['current'] is None and remaining[0]==task_id and state['logical_time']<=start,'fixed task order/start')
                    if state['logical_time']<start:self.journal.append('CLOCK',{'now':start})
            else:need(now is None,'timed begin requires refresh profile')
            return self._consumer_control_locked('NATIVE_TASK_BEGIN',task_id)
    def consumer_freeze(self):return self._consumer_control('NATIVE_POLICY_FREEZE')
    def consumer_task_cancel(self):return self._consumer_control('NATIVE_TASK_CANCEL')
    def consumer_snapshot(self):
        with self._locked():
            state=self.journal.snapshot()['state'];root,policy=self._consumer(state)
            return dict(root=copy.deepcopy(root),policy=copy.deepcopy(policy),root_ref=copy.deepcopy(state['native_consumer_ref']),
                campaign='NOT_RUN',authority='NONE')

    def _choice(self,state):
        return choice_learning.bounded(self._read(state['choice_ref'],'compact_choice_state'))

    def choice_snapshot(self):
        with self._locked():return self._choice(self.journal.snapshot()['state'])

    def _continuation(self,state):
        return continuation.bounded(self._read(state['continuation_ref'],'compact_receipt_continuation'))

    def continuation_snapshot(self):
        with self._locked():
            state=self.journal.snapshot()['state']
            return self._continuation(state) if 'continuation_ref' in state else None

    def _require_native_adopted_choice(self,state,root):
        if state['config'].get('native_binding') is not None and state['config']['choice_profile'].get('source_adoption') is True:
            need('adoption_ref' in root and self._choice_adoption(root)['status']=='ADOPTED' and root['policy']==root['candidate'],
                 'native source use requires adopted candidate')

    def _continuation_transition(self,state,kind,args,source):
        need('choice_profile' in state['config'],'continuation choice profile')
        if kind=='CONTINUATION_GRANT':
            need(state['pending'] is None and 'continuation_ref' not in state,'single idle lifetime grant')
            need(set(args)=={'limit','outcome_review'},'grant arguments')
            root=self._choice(state)
            self._require_native_adopted_choice(state,root)
            return continuation.grant(root,source,state['choice_ref'],**args)
        root=self._continuation(state)
        if kind=='CONTINUATION_VETO':
            need(set(args)=={'veto'} and type(args['veto']) is bool,'trusted veto flag')
            need(state['pending'] is None or state['pending']['phase']=='REQUESTED','cannot change unknown effect')
            root['veto']=args['veto'];return continuation.bounded(root)
        need(kind=='CONTINUATION_EXPIRE' and args=={} and state['pending'] is None,'idle expiration')
        return continuation.expire(root,state['logical_time'])

    def _continuation_control_locked(self,kind,args):
        head=self.journal.snapshot();state=head['state'];source=self._source(state,head['seq'],head['event_head'])
        root=self._continuation_transition(state,kind,args,source)
        frame,ref=self.objects._frame('compact_receipt_continuation',object_json(root),'json')
        projected=copy.deepcopy(state);projected['continuation_ref']=ref
        need(len(_encode(projected))+1024<=65536,'continuation control head capacity')
        need(head['seq']<(journal_limits.MAX_SEGMENTS+1)*journal_limits.SEGMENT_RECORDS,'continuation journal capacity')
        payload=dict(args=args,source=source,continuation_ref=ref)
        path=self.objects.directory/('obj-'+ref['sha256']+'.bin');check_file(path)
        if path.exists():
            need(self.objects.read(ref,expected_kind='compact_receipt_continuation')==root,'verified immutable continuation retry object')
            self.journal.append(kind,payload)
        else:
            with self.objects.publication_lease(max_objects=1,max_reserved_bytes=2*len(frame)) as lease:
                need(lease.put_json('compact_receipt_continuation',root)==ref,'continuation control publication')
                self.journal.append(kind,payload)
        return root

    def grant_continuation(self,limit=16,outcome_review=True):
        with self._locked():
            self._reconcile_locked()
            return self._continuation_control_locked('CONTINUATION_GRANT',dict(limit=limit,outcome_review=outcome_review))

    def set_continuation_veto(self,veto):
        with self._locked():
            self._reconcile_locked()
            return self._continuation_control_locked('CONTINUATION_VETO',dict(veto=veto))

    def _next_continuation(self,state,decision,item,ref,path):
        return continuation.capture(self._continuation(state),self._choice(state),state['config']['choice_profile'],
            item,ref,path,state['logical_time'],[g['path'] for g in state['goals']],
            [e['relative_path'] for e in state['inventory']['entries'] if e['kind']=='file'],decision)

    def _choice_control(self,kind,role=None):
        with self._locked():
            self._reconcile_locked();head=self.journal.snapshot();state=head['state']
            need('choice_profile' in state['config'] and state['pending'] is None and 'continuation_ref' not in state,'idle legacy choice profile required')
            w=self._workflow(state)
            need(w['goal'] is None or w['goal']['status'] in ('SATISFIED','CANCELLED'),'protected goal blocks choice')
            need(w['interaction']['pending'] is None and not self._protect_help_goal(w),'protected interaction blocks choice')
            if kind=='CHOICE_BEGIN':
                need(state['inventory']['valid'] and max(state['service_debt'])<SERVICE_DEBT_LIMIT-1,'service existing obligations before trial')
                if role=='use':self._require_native_adopted_choice(state,self._choice(state))
                if 'adoption_ref' in self._choice(state) and role in ('validation','test'):
                    need(self._choice_adoption(self._choice(state))['status']=='FROZEN','retention plan frozen before heldout')
            root=choice_learning.control(self._choice(state),kind,role,state['logical_time'])
            source=self._source(state,head['seq'],head['event_head'])
            if state['config']['choice_profile'].get('source_adoption') is True:
                need(head['seq']<(journal_limits.MAX_SEGMENTS+1)*journal_limits.SEGMENT_RECORDS,'source choice journal capacity')
                frame,ref=self.objects._frame('compact_choice_state',object_json(root),'json')
                projected=copy.deepcopy(state);projected['choice_ref']=ref
                need(len(_encode(projected))<=65536,'source choice head capacity')
                with self.objects.publication_lease(max_objects=1,max_reserved_bytes=2*len(frame)) as lease:
                    need(lease.put_json('compact_choice_state',root)==ref,'exact source choice control root')
                    self.journal.append(kind,dict(role=role,source=source,choice_ref=ref))
            else:
                ref=self._put('compact_choice_state',root,choice_learning.MAX_BYTES)
                self.journal.append(kind,dict(role=role,source=source,choice_ref=ref))
            return copy.deepcopy(root)

    def begin_choice(self,role):return self._choice_control('CHOICE_BEGIN',role)
    def freeze_choice(self):
        root=self.choice_snapshot()
        if 'adoption_ref' in root:
            if root['candidate'] is None:self._choice_control('CHOICE_FREEZE')
            return self._choice_adoption_control('FREEZE_CHOICE_ADOPTION')
        return self._choice_control('CHOICE_FREEZE')

    def _choice_adoption(self,root):
        need('adoption_ref' in root,'source adoption not enabled')
        value=self._read(root['adoption_ref'],'compact_choice_adoption')
        need(type(value) is dict and set(value)=={'schema','status','anchor','plan_ref','results','report_ref','previous_policy','invalid'} and value['schema']=='compact-choice-adoption-1','adoption root schema')
        need(value['status'] in ('TRAINING','FROZEN','ACCEPTED','REJECTED','ADOPTED') and type(value['results']) is dict and len(value['results'])<=12 and len(_encode(value))<=65536,'bounded adoption root')
        return copy.deepcopy(value)

    def choice_adoption_snapshot(self):
        with self._locked():return self._choice_adoption(self._choice(self.journal.snapshot()['state']))

    def _choice_receipt_rows(self,state,root):
        """Re-derive values from locally framed receipts in reducer-admitted roots.

        Every root is constructed by RECEIPT replay, never supplied by an API.
        Cold journal replay proves admission before these frame checks; scanning
        the journal from its own reducer would be recursive and is forbidden.
        """
        uniform=choice_learning.uniform_policy(); fitted=copy.deepcopy(uniform)
        fitted.update(version=1,parent_sha256=_hash(uniform))
        for role,rows in (('train',root['training']),('evaluation',root['evaluation'])):
            for index,row in enumerate(rows):
                for key in ('capture_ref','descriptor_ref'):
                    need(all(row[key][k]==state[k] for k in ('identity','instance_id')),'source admitted choice namespace')
                item=self._read(row['capture_ref'],'compact_capture'); di=self._read(row['descriptor_ref'],'compact_capture')
                need(item['capture']['capture_origin']==state['config']['capture_origin'] and di['capture']['capture_origin']==state['config']['capture_origin'],'source choice capture origin')
                dr=di['capture']['result'];need(di['request']['operation']=='READ_TEXT' and dr['status']=='OK','actual descriptor READ')
                d=json.loads(dr['payload']['text']); paths=[o['path'] for o in d['options']]
                d=choice_learning.descriptor(dr['payload']['text'],paths,paths)
                policy=uniform if role=='train' else root['candidate']
                selection=choice_learning.choose(policy,d,index if role=='train' else None)
                result=item['capture']['result'];success=False
                if result['status']=='OK':
                    try:
                        body=json.loads(result['payload']['text'])
                        success=type(body) is dict and type(body.get('context')) is int and body==dict(schema='compact-choice-evidence-1',nonce=d['nonce'],context=d['context'],type=d['required_type'])
                    except (ValueError,TypeError):pass
                # Path-to-opaque-entry and final CL target agreement is enforced
                # by the admitting RECEIPT reducer, using its actual decision.
                need(item['request']['operation']=='READ_TEXT' and row['context']==d['context'] and row['source_class']==selection['source_class'] and row['policy_sha256']==_hash(policy) and row['success']==success and row['fresh'],'rederived source choice outcome')
                expected_role='train' if role=='train' else ('validation' if index<16 else 'test')
                n=index if role=='train' else index%16
                need(row['role']==expected_role and row['task_id']==expected_role+'-'+str(n//2) and row['stage']==n%2,'frozen prospective membership')
                if role=='train':
                    cell=fitted['counts'][row['context']][row['source_class']];cell[0]+=int(success);cell[1]+=1;fitted['training'].append(row['capture_ref'])
        need(len(root['training'])==16 and all(c[1]==4 for r in fitted['counts'] for c in r) and root['candidate']==choice_learning.validate_policy(fitted),'exact local receipt-fitted candidate')
        return fitted

    def _choice_adoption_materials(self,state,root,registry):
        from . import compact_skill_retention as retention
        w=self._workflow(state);a=registry['anchor']
        need(w['active_ref']==a['skill_ref'] and w['active_evaluation_ref']==a['acceptance_ref'] and state['active_model_ref']==a['width_ref'],'unchanged protected skill acceptance and width')
        need(root['policy']==a['incumbent'] and root['use_count']==0,'unchanged incumbent before adoption')
        skill=self._verified_procedure(a['skill_ref']);accepted=self._read(a['acceptance_ref'],'compact_procedure_acceptance')
        need(accepted['decision']=='ACCEPT' and accepted['candidate_ref']==a['skill_ref'] and _hash(skill)==a['skill_sha256'],'actual accepted source skill')
        width=self._read(a['width_ref'],'compact_model')
        need(_hash(width)==a['width_sha256'] and a['protocol']==retention.SOURCE_CHOICE_PROTOCOL,'frozen width and retention protocol')
        return skill,width

    def _choice_adoption_evaluation(self,state,root,registry):
        from . import compact_skill_retention as retention
        skill,width=self._choice_adoption_materials(state,root,registry)
        self._choice_receipt_rows(state,root)
        need(registry['plan_ref'] is not None,'frozen plan required')
        plan=self._read(registry['plan_ref'],'compact_skill_retention_plan')
        frozen=self._read(plan['source']['choice_state_ref'],'compact_choice_state')
        need(frozen['candidate']==root['candidate'] and frozen['training']==root['training'] and frozen['evaluation']==[] and plan['choice_models']['candidate']==choice_learning.context(root['candidate']),'exact frozen candidate lineage')
        need(len(root['evaluation'])==32,'complete source evaluation')
        scores={}
        for role in ('validation','test'):
            pairs=[[r for r in root['evaluation'] if r['task_id']==role+'-'+str(i)] for i in range(8)]
            need(all(len(p)==2 and [r['stage'] for r in p]==[0,1] and {r['context'] for r in p}=={0,1} for p in pairs),'complete paired source evaluation')
            scores[role]=sum(all(r['success'] for r in p) for p in pairs)
        values={case:self._read(ref,'compact_skill_retention_case') for case,ref in registry['results'].items()}
        measured=retention.verify_suite(values,registry['plan_ref'],plan,skill,width,width)
        accept=root['invalid'] is None and registry['invalid'] is None and all(n==8 for n in scores.values()) and measured['decision']=='ACCEPT'
        return dict(schema='compact-choice-adoption-report-1',decision='ACCEPT' if accept else 'REJECT',scores=scores,
                    training_sha256=_hash(root['training']),evaluation_sha256=_hash(root['evaluation']),candidate_sha256=_hash(root['candidate']),
                    plan_ref=registry['plan_ref'],results=copy.deepcopy(registry['results']),retention_sha256=_hash(measured),invalid=registry['invalid'])

    @staticmethod
    def _source_learning_mode(state):
        config=state['config']
        return config['mode']=='SYNTHETIC_CONTINUATION' or (
            config['mode']=='APPLICATION_BROKER_CONTINUATION' and
            config.get('native_binding') is not None and
            config.get('choice_profile',{}).get('source_adoption') is True)

    def _choice_adoption_transition(self,state,kind,source,provided=None):
        need('native_acquisition' not in state,'acquisition research excludes learning/adoption controls')
        need('continuation_ref' not in state,'continuation owns incumbent')
        from . import compact_skill_retention as retention
        state=copy.deepcopy(state);root=self._choice(state);w=self._workflow(state)
        need(state['pending'] is None and root['current'] is None and self._source_learning_mode(state),'idle main source adoption control')
        need(not self._unknown_local(w) and w['interaction']['pending'] is None and not self._protect_help_goal(w) and (w['goal'] is None or w['goal']['status'] in ('SATISFIED','CANCELLED')),'protected or uncertain work blocks adoption')
        artifacts={};result=None
        def emit(name,kind,value,limit):
            need(len(_encode(value))<=limit,'adoption artifact bound')
            frame,ref=self.objects._frame(kind,object_json(value),'json');artifacts[name]=dict(kind=kind,value=copy.deepcopy(value),ref=ref,frame_bytes=len(frame));return ref
        if kind=='ENABLE_CHOICE_ADOPTION':
            need(state['config']['choice_profile'].get('source_adoption') is True,'source adoption exposure tracking from birth required')
            need('adoption_ref' not in root and root['trials']==0 and root['candidate'] is None and root['invalid'] is None,'enable before all choice trials')
            need(state['config']['choice_profile']['condition']=='candidate','source candidate evaluation profile')
            need(w['active_ref'] is not None and w['active_evaluation_ref'] is not None,'actual accepted old skill required')
            skill=self._verified_procedure(w['active_ref']);accepted=self._read(w['active_evaluation_ref'],'compact_procedure_acceptance')
            need(accepted['decision']=='ACCEPT' and accepted['candidate_ref']==w['active_ref'],'actual source skill acceptance')
            width=self._read(state['active_model_ref'],'compact_model')
            anchor=dict(source=source,skill_ref=w['active_ref'],acceptance_ref=w['active_evaluation_ref'],skill_sha256=_hash(skill),width_ref=state['active_model_ref'],width_sha256=_hash(width),incumbent=copy.deepcopy(root['policy']),protocol=copy.deepcopy(retention.SOURCE_CHOICE_PROTOCOL),thresholds=dict(validation=8,test=8,paired_cases=12,maximum_decline=0))
            registry=dict(schema='compact-choice-adoption-1',status='TRAINING',anchor=anchor,plan_ref=None,results={},report_ref=None,previous_policy=None,invalid=None)
            root['use_count']=0
        else:
            registry=self._choice_adoption(root)
            skill,width=self._choice_adoption_materials(state,root,registry)
            if kind=='FREEZE_CHOICE_ADOPTION':
                need(registry['status']=='TRAINING','single adoption freeze')
                need(root['candidate'] is not None and root['evaluation']==[],'persisted candidate before heldout')
                self._choice_receipt_rows(state,root)
                plan=retention.make_plan(source,registry['anchor']['skill_ref'],skill,registry['anchor']['acceptance_ref'],state['active_model_ref'],width,state['active_model_ref'],width,include_help=True,choice_models=dict(incumbent=choice_learning.context(root['policy']),candidate=choice_learning.context(root['candidate'])),source_bound=True)
                registry['plan_ref']=emit('plan','compact_skill_retention_plan',plan,retention.MAX_PLAN_BYTES);registry['status']='FROZEN'
            elif kind=='IMPORT_CHOICE_RETENTION':
                need(registry['status']=='FROZEN' and provided is not None,'frozen source retention import')
                plan=self._read(registry['plan_ref'],'compact_skill_retention_plan');case=provided.get('case')
                need(case not in registry['results'],'one immutable result per case')
                measured=retention.verify_case(provided,registry['plan_ref'],plan,skill,width,width,case)
                digests=measured['evaluation_content_sha256']
                training=set()
                for row in root['training']:
                    for k in ('capture_ref','descriptor_ref'):
                        item=self._read(row[k],'compact_capture');r=item['capture']['result']
                        if r['status']=='OK':training.add(r['payload']['content_sha256'])
                root['exposed']=sorted(set(root['exposed'])|set(digests))
                if training.intersection(digests):registry['invalid']='EVALUATION_BANK_TRAINING_COLLISION'
                if not measured['passed']:registry['invalid']=registry['invalid'] or 'OLD_SKILL_RETENTION_FAILED'
                registry['results'][case]=emit('case','compact_skill_retention_case',provided,retention.MAX_CASE_BYTES)
                result=dict(case=case,passed=measured['passed'],invalid=registry['invalid'])
            elif kind in ('EVALUATE_CHOICE_ADOPTION','ADOPT_CHOICE'):
                need(registry['status'] in ('FROZEN','ACCEPTED','REJECTED'),'frozen uneffected adoption')
                result=self._choice_adoption_evaluation(state,root,registry)
                if kind=='EVALUATE_CHOICE_ADOPTION':
                    registry['report_ref']=emit('report','compact_choice_adoption_report',result,65536);registry['status']='ACCEPTED' if result['decision']=='ACCEPT' else 'REJECTED'
                else:
                    need(registry['status']=='ACCEPTED' and result['decision']=='ACCEPT' and self._read(registry['report_ref'],'compact_choice_adoption_report')==result,'recomputed accepted source adoption')
                    registry['previous_policy']=copy.deepcopy(root['policy']);root['policy']=copy.deepcopy(root['candidate']);registry['status']='ADOPTED'
            else:need(False,'unknown adoption control')
        root['adoption_ref']=emit('adoption','compact_choice_adoption',registry,65536)
        root=choice_learning.bounded(root)
        return state,root,artifacts,result

    def _choice_adoption_control(self,kind,provided=None):
        with self._locked():
            self._reconcile_locked();head=self.journal.snapshot();state=head['state'];source=self._source(state,head['seq'],head['event_head'])
            projected,root,artifacts,result=self._choice_adoption_transition(state,kind,source,provided)
            need(head['seq']<(journal_limits.MAX_SEGMENTS+1)*journal_limits.SEGMENT_RECORDS,'adoption journal capacity')
            frame,root_ref=self.objects._frame('compact_choice_state',object_json(root),'json');projected['choice_ref']=root_ref
            record=dict(kind=kind,source=source,choice_ref=root_ref,artifacts={name:a['ref'] for name,a in artifacts.items()},result_sha256=_hash(result))
            control_frame,control_ref=self.objects._frame('compact_choice_adoption_control',object_json(record),'json')
            need(len(_encode(projected))<=65536 and len(_encode(record))<=16384,'adoption head/control caps')
            batch=list(artifacts.values())+[
                dict(kind='compact_choice_state',value=root,ref=root_ref,frame_bytes=len(frame)),
                dict(kind='compact_choice_adoption_control',value=record,ref=control_ref,frame_bytes=len(control_frame))]
            remaining=[]
            for a in batch:
                path=self.objects.directory/('obj-'+a['ref']['sha256']+'.bin');check_file(path)
                if path.exists():need(self.objects.read(a['ref'],expected_kind=a['kind'])==a['value'],'verified immutable adoption retry object')
                else:remaining.append(a)
            # Final immutable objects surviving an interrupted publication were
            # already charged. Verify and reuse them without re-admitting their
            # slots. A charged but missing object remains a conservative halt.
            if remaining:
                required=2*sum(a['frame_bytes'] for a in remaining)
                with self.objects.publication_lease(max_objects=len(remaining),max_reserved_bytes=required) as lease:
                    for a in remaining:need(lease.put_json(a['kind'],a['value'])==a['ref'],'exact adoption batch object')
                    self.journal.append(kind,dict(record_ref=control_ref))
            else:
                self.journal.append(kind,dict(record_ref=control_ref))
            return copy.deepcopy(result if result is not None else root)

    def enable_choice_adoption(self):return self._choice_adoption_control('ENABLE_CHOICE_ADOPTION')
    def evaluate_choice_adoption(self):return self._choice_adoption_control('EVALUATE_CHOICE_ADOPTION')
    def adopt_choice(self):return self._choice_adoption_control('ADOPT_CHOICE')
    def import_choice_retention(self,capsule):return self._choice_adoption_control('IMPORT_CHOICE_RETENTION',capsule)
    def run_choice_retention(self,directory,case):
        from . import compact_skill_retention as retention
        with self._locked():
            state=self.journal.snapshot()['state'];root=self._choice(state);registry=self._choice_adoption(root)
            w=self._workflow(state)
            need(state['pending'] is None and root['current'] is None and not self._unknown_local(w) and w['interaction']['pending'] is None and not self._protect_help_goal(w) and (w['goal'] is None or w['goal']['status'] in ('SATISFIED','CANCELLED')),'idle certain source before child execution')
            skill,width=self._choice_adoption_materials(state,root,registry)
            need(registry['status']=='FROZEN' and case not in registry['results'],'frozen untested source case')
            plan=self._read(registry['plan_ref'],'compact_skill_retention_plan')
        capsule=retention.run_case(directory,registry['plan_ref'],plan,skill,width,width,case)
        return self.import_choice_retention(capsule)

    def _next_choice(self,state,decision,item,ref,path):
        return choice_learning.capture(self._choice(state),state['config']['choice_profile'],item,ref,path,state['logical_time'],
            [g['path'] for g in state['goals']],[e['relative_path'] for e in state['inventory']['entries'] if e['kind']=='file'],decision)

    def _workflow(self,state):return procedures.validate_workflow(self._read(state['workflow_ref'],'compact_workflow'))

    @staticmethod
    def _unknown_local(w):
        pending=w['interaction']['pending']
        return pending is not None and pending['status'] in ('UNKNOWN','DISPATCHED')

    def _workflow_birth(self,state):
        w=procedures.initial_workflow(state['config']['procedure_probe'])
        w['appraisal']=regulation.initial_regulation(self.identity,self.instance_id,state['scope_ref'])
        w['interaction']=interaction.initial_interaction(self.identity,self.instance_id,state['scope_ref'])
        w['help']=help_policy.initial_help()
        return w

    def _procedure(self,w):
        return guarded.branch(w['probe'] if w['probe'] is not None else w['active_spec'],w['goal'])

    def _decision(self,state,seq,head):
        workflow=self._workflow(state);pending=workflow['interaction']['pending']
        if 'native_acquisition' in state and state['native_acquisition']['phase'] in acquisition.STOP:return None
        if pending is not None and pending['status'] in ('UNKNOWN','DISPATCHED'):return None
        monitor=self._monitor_decision(state,seq,head,workflow)
        if pending is None or pending['status']!='QUEUED' or (monitor is not None and state['last_service_family']=='local'):return monitor
        q=interaction.validate_local_request(self._read(pending['request_ref'],'compact_local_request'),state['config']['local_endpoints'])
        if state['logical_time']>=q['deadline']:return monitor
        if q['goal'] is not None:
            if q['goal']!=self._local_goal_binding(workflow):return monitor
        need(q['action'] in state['config']['capabilities'],'local capability denied')
        allocation=governance_allocate([dict(id=q['target'],module='compare',cost=1,gain=1.)],budget=1)
        if not allocation.get('allocations'):return monitor
        need(allocation['allocations']==[dict(id=q['target'],module='compare',cost=1,gain=1.)],'exact local allocation')
        credit=procedures.available(workflow,state['logical_time'])
        if credit<2:
            resume_at=state['logical_time']+2-credit
            if credit!=1 or resume_at>=q['deadline']:return monitor
            return dict(schema='compact-local-resource-hold-1',marker=dict(logical_time=state['logical_time'],
                reason='RESERVE_LOCAL_COST',request_id=q['request_id'],available=credit,required=2,resume_at=resume_at))
        visible=[e['target'] for e in state['config']['local_endpoints']]
        cl='PROPOSE('+q['action'].upper()+'(@'+q['target']+','+json.dumps(q['request'],ensure_ascii=False)+'));'
        caps=state['config']['capabilities'];compiled=Compiler(visible,caps).compile(cl);vm=CVM(caps)
        tx=vm.prepare(cl,dict(version=state['cognition_revision'],world_version=state['inventory']['generation']),visible);vm.verify(tx);payload=transaction_payload(tx)
        need(payload['ir']==compiled and payload['proposals']==[dict(kind=q['action'],target=q['target'],request=q['request'])]
             and not payload['messages'] and not payload['neuron_calls'] and payload['wait_reason'] is None,'one typed local proposal')
        return dict(schema='compact-local-decision-1',source=self._source(state,seq,head),request=q,local_request_ref=pending['request_ref'],
                    generated_cl=cl,transaction_payload=payload,governance=dict(authority='EXISTING_CAPABILITIES_ONLY',action_allocation=allocation,external_action_cost=2))

    def _monitor_decision(self, state, seq, head,workflow):
        need(state['pending'] is None and state['field_ref'] is not None, 'one initialized executor')
        source = self._source(state,seq,head); inventory = state['inventory']; ranking = None; metadata = []
        episode=workflow['goal'];procedure=self._procedure(workflow)
        if procedures.available(workflow,state['logical_time'])<1:return None
        choice=None;choice_path=None
        if 'choice_profile' in state['config']:
            root=self._choice(state);trial=root['current']
            if trial is not None:
                need(state['logical_time']<trial['deadline'],'choice deadline exhausted')
                need(episode is None or episode['status'] in ('SATISFIED','CANCELLED'),'choice cannot bypass protected goal')
                need(workflow['interaction']['pending'] is None and not self._protect_help_goal(workflow),'choice cannot bypass protected help')
                need(max(state['service_debt'])<SERVICE_DEBT_LIMIT,'service fairness blocks choice')
                choice_path=state['config']['choice_profile']['descriptor_path'] if trial['phase']=='DESCRIPTOR' else trial['selection']['path']
                choice=dict(root_ref=state['choice_ref'],phase=trial['phase'],descriptor_ref=trial['descriptor_ref'],selection=trial['selection'],evidence_read_budget=1)
        continuation_choice=False
        if 'continuation_ref' in state:
            c=self._continuation(state);t=c['current']
            if (t is not None and not c['veto'] and state['logical_time']<t['deadline']
                and (episode is None or episode['status'] in ('SATISFIED','CANCELLED'))
                and workflow['interaction']['pending'] is None and not self._protect_help_goal(workflow)
                and max(state['service_debt'])<SERVICE_DEBT_LIMIT):
                choice_path=t['selection']['path'];continuation_choice=True
                choice=dict(root_ref=state['continuation_ref'],phase='EVIDENCE',descriptor_ref=t['descriptor_ref'],selection=t['selection'],evidence_read_budget=1)
        desired=None
        excluded=set()
        if episode is not None and episode['status']=='PAUSED':
            excluded=set(episode['bindings'].values())
        if episode is not None and episode['status']=='ACTIVE' and procedure is not None:
            desired=episode['bindings'][procedure['order'][episode['cursor']]]
        repaired=self._help_episode(workflow)
        if episode is not None and episode['status']=='ACTIVE' and procedure is None and repaired is not None and repaired['phase']=='VERIFIED' and repaired['trigger']['goal']['id']==episode['id']:
            role=(self._help_remaining_role(state,workflow) if state['config'].get('help_continuation_profile')==help_roles.PROFILE
                  else procedures.ROLES[episode['cursor']])
            desired=episode['bindings'][role]
            excluded=set(episode['bindings'].values())-{desired}
        wait=workflow['typed_wait']
        if wait is not None and wait['status']=='WAITING':
            scope=wait['scope']
            if scope['kind']=='monitor':excluded|={i for i,g in enumerate(state['goals']) if g['path']==scope['path']}
            elif episode is not None and scope==dict(kind='goal',id=episode['id'],generation=episode['generation']):excluded|=set(episode['bindings'].values())
        help_episode=self._help_episode(workflow)
        help_phase=None if help_episode is None else help_episode['phase']
        acquire=None
        task_list=False
        if state['config'].get('native_consumer_profile',{}).get('schema') in consumer.REFRESH_PROFILES:
            task_root,_=self._consumer(state)
            task_list=task_root['current'] is not None and task_root['current']['phase']=='NEED_LIST'
        if 'native_acquisition' in state and state['native_acquisition']['phase']=='READY':
            reason=acquisition.blocker(state,workflow,state['native_acquisition'],self._read)
            if reason=='WAITING':return None
            if reason is None:acquire=acquisition.tag(state['native_acquisition'])
            elif reason!='FAIRNESS':return None
        if not inventory['valid'] or help_phase=='AWAITING_LIST' or acquire is not None or task_list:
            if state['logical_time'] < inventory['next_list_time']: return None
            operation = 'LIST_SCOPE'; target = state['scope_ref']; visible = [target]
        else:
            bypath = {e['relative_path'].casefold():e for e in inventory['entries'] if e['kind'] == 'file'}
            candidates = []
            investigation={q['goal_index'] for q in state['semantic_queue'] if state['goals'][q['goal_index']]['path'].casefold() in bypath}
            if episode is not None and episode['status'] in ('PAUSED','WAITING','BLOCKED','CANCELLED'):investigation=set()
            if desired is not None:investigation={desired}
            if help_phase=='AWAITING_READ':investigation={help_episode['trigger']['goal_index']}
            embargo=set()
            if state['config']['renewal_profile'] is not None:
                current=self._cohorts(state)['current']
                if current is not None and current['status']=='TRAINING':
                    embargo={int(slot) for role,slots in current['plan'].items() if role!='train' for slot in slots}
            for goal_index,goal in enumerate(state['goals']):
                if goal_index in embargo:continue
                entry = bypath.get(goal['path'].casefold())
                if goal['status'] != 'ACTIVE' or entry is None or goal_index in excluded: continue
                prior = state['latest_by_path'].get(goal['path'])
                age = max(goal['period'],state['logical_time']) if prior is None else state['logical_time']-prior['last_attempt_time']
                if age < goal['period'] and goal_index not in investigation and goal['path']!=choice_path: continue
                rate = 0. if prior is None else prior['changes']/max(1,prior['successful_reads']-1)
                novelty = 1. if prior is None else 1./(1+prior['successful_reads'])
                offset = candidate_offset(state['control'],{'tokens':[_token(goal['path'])],'novelty':novelty}) if state['config']['u0_enabled'] else 0.
                attention=ERROR_ATTENTION_CAP*state['u0_investigation'][goal_index]
                no_progress=prior is not None and prior['successful_reads']>=2 and prior['changes']==0
                components=dict(C=0.,T=0.,P=0.)
                if state['config']['regulation_enabled']:
                    modes={'C':dict(repair=prior is not None and prior['last_status']!='OK',verify=False,explore=False),
                           'T':dict(repair=False,verify=goal_index in investigation,explore=False),
                           'P':dict(repair=False,verify=False,explore=goal_index not in investigation)}
                    for channel in state['config']['regulation_channels']:
                        components[channel]=regulation.candidate_offset(workflow['appraisal'],dict(modes[channel],permitted=True,no_progress=no_progress))
                        if channel in ('T','P'):components[channel]*=novelty
                appraisal=sum(components.values())
                baseline = .02*min(age/goal['period'],8.)+.05*rate+offset+attention+appraisal
                candidates.append(dict(entity_ref=entry['entry_ref'],baseline_score=baseline,novelty=novelty,
                    persistence=float(state['last_path']==goal['path']),switching=float(state['last_path'] is not None and state['last_path']!=goal['path'])))
                metadata.append(dict(path=goal['path'],entry_ref=entry['entry_ref'],age=age,change_rate=rate,u0_offset=offset,
                                     investigation_offset=attention,service_debt=state['service_debt'][goal_index],appraisal_offset=appraisal,appraisal_components=components))
            if not candidates: return None
            overdue={m['entry_ref'] for m in metadata if m['service_debt']>=SERVICE_DEBT_LIMIT}
            if overdue:candidates=[c for c in candidates if c['entity_ref'] in overdue]
            elif investigation:
                preferred={bypath[state['goals'][i]['path'].casefold()]['entry_ref'] for i in investigation}
                filtered=[c for c in candidates if c['entity_ref'] in preferred]
                if filtered:candidates=filtered
            if choice_path is not None:
                selected_entries={m['entry_ref'] for m in metadata if m['path']==choice_path}
                selected=[c for c in candidates if c['entity_ref'] in selected_entries]
                if continuation_choice and not selected:
                    choice=None;choice_path=None;continuation_choice=False
                else:
                    candidates=selected
                    need(len(candidates)==1,'exact authorized choice target')
            ranking = rank_candidates(candidates,state['field_features'],decode_calibration(state['config']['calibration']),enabled=state['config']['field_enabled'])
            target = ranking['selected']; operation = 'READ_TEXT'
            visible = sorted(e['entry_ref'] for e in inventory['entries'] if e['kind']=='file')
        action_jobs = ([dict(id=target,module='compare',cost=NEURONS['compare'].compute_cost,gain=1.)]
            if ranking is None else [dict(id=row['entity_ref'],module='compare',cost=NEURONS['compare'].compute_cost,
                                          gain=max(0.,min(1.,.25+row['final_score']))) for row in ranking['ranked']])
        action_allocation = governance_allocate(action_jobs,budget=1)
        allocated = action_allocation.get('allocations',[])
        if not allocated: return None
        need(len(allocated)==1 and allocated[0] in action_jobs,'exact eligible action allocation')
        target = allocated[0]['id']
        consumer_tag=None
        if (operation=='READ_TEXT' or task_list) and 'native_consumer_ref' in state:
            root,policy=self._consumer(state)
            if root['current'] is not None:
                task=self._read(root['current']['task_ref'],consumer.TASK_KIND)
                record=(self._read(state['derived_observation_ref'],derived.KIND)
                    if state.get('derived_observation_ref') is not None else None)
                target,consumer_tag=consumer.select(state,root,policy,state['config']['native_consumer_profile'],task,
                    ranking,metadata,target,record)
                chosen=[job for job in action_jobs if job['id']==target]
                need(len(chosen)==1,'consumer stays in native eligible allocation')
                action_allocation=governance_allocate(chosen,budget=1)
                need(action_allocation['allocations']==chosen,'complete selected governance allocation')
        error_allocation = governance_allocate([dict(id='prediction_error',module='prediction_error',
            cost=NEURONS['prediction_error'].compute_cost,gain=1.)],budget=1)
        if not error_allocation.get('allocations'): return None
        need([x['module'] for x in error_allocation['allocations']]==['prediction_error'],'exact error allocation')
        action_cost={'LIST_SCOPE':1,'READ_TEXT':1}[operation]
        if action_cost>state['config']['external_action_budget']:return None
        caps = ('read','observe'); need(set(caps) <= set(state['config']['capabilities']), 'capability denied')
        generated = f'PROPOSE(OBSERVE(@{target}));'
        investigation_info=None
        if operation=='READ_TEXT' and episode is not None and episode['status']=='ACTIVE':
            index=next(i for i,g in enumerate(state['goals']) if any(m['path']==g['path'] and m['entry_ref']==target for m in metadata))
            roles=[role for role,i in episode['bindings'].items() if i==index]
            if roles and (index==desired or (desired is None and any(q['goal_index']==index for q in state['semantic_queue']))):
                investigation_info=dict(goal_id=episode['id'],cursor=episode['cursor'],episode_key=episode['episode_key'],role=roles[0],
                                        procedure_ref=workflow['active_ref'],probe=workflow['probe'] is not None)
                if procedure is not None:
                    generated=procedure['templates'][episode['cursor']].replace('@'+procedures.ROLE_TOKENS[roles[0]],'@'+target)
                    need(generated==f'PROPOSE(OBSERVE(@{target}));','learned CL bound to current role')
        context = dict(version=state['cognition_revision'],world_version=inventory['generation'])
        compiled = Compiler(visible,caps).compile(generated); vm = CVM(caps)
        tx = vm.prepare(generated,context,visible); vm.verify(tx); payload = transaction_payload(tx)
        need(payload['ir'] == compiled and payload['proposals'] == [{'kind':'observe','target':target}]
             and payload['messages']==[] and payload['neuron_calls']==[] and payload['wait_reason'] is None,'exact one CL observation')
        request = make_request(operation,state['scope_ref'],request_id='req_'+_hash({'source':source,'target':target,'operation':operation})[:32],
                               entry_ref=target if operation=='READ_TEXT' else None)
        decision = dict(schema='compact-decision-1',source=source,request=request,generated_cl=generated,
                    transaction_payload=payload,ranking=ranking,candidate_metadata=metadata,
                    governance=dict(action_allocation=action_allocation,prediction_error_allocation=error_allocation,
                                    neuron_modules=['compare','prediction_error'],prediction_reserve=2,compute_budget=4,
                                    external_action_cost=action_cost,external_action_budget=1,total_reserved_logical_units=5,
                                    approved_request_sha256=_hash(request),authority='EXISTING_CAPABILITIES_ONLY'),
                    prediction=observer.prepare_prediction(state['observer'],request,source))
        decision['investigation']=investigation_info
        if acquire is not None:decision['inventory_acquisition']=acquire
        if consumer_tag is not None:decision['native_consumer']=consumer_tag
        if choice is not None:
            need(operation=='READ_TEXT' and any(m['path']==choice_path and m['entry_ref']==target for m in metadata),'final allocated CL choice target')
            decision['continuation' if continuation_choice else 'choice']=choice
        if help_phase=='AWAITING_LIST' and operation=='LIST_SCOPE':decision['help_check']=help_episode['request_id']
        elif help_phase=='AWAITING_READ' and operation=='READ_TEXT' and any(m['entry_ref']==target and m['path']==help_episode['trigger']['path'] for m in metadata):decision['help_check']=help_episode['request_id']
        if state['config'].get('native_order_evidence_profile')==native_order_evidence.PROFILE:
            decision['native_order_observation']=native_order_evidence.observe(state,workflow,self._read)
        return json.loads(_encode(decision))

    def _next_workflow(self,state,decision,item,capture_ref,seq,index,queue):
        previous=self._workflow(state)
        g=previous['goal']
        if (state['config'].get('help_continuation_profile')==help_roles.PROFILE and g is not None
                and 'receipt_provenance' in g and decision.get('investigation') is not None
                and item['capture']['result']['status']=='OK'):
            help_roles.check_receipt(g,decision,item,seq,state['goals'],
                {k:state[k] for k in ('identity','instance_id','scope_ref')},state['config']['capture_origin'],self._read)
        profile=interleaving.validate_profile(state['config'].get('procedure_learning_profile'))
        if profile is None:
            w=procedures.receive(previous,decision,item,state['pending']['decision_ref'],capture_ref,seq,state['logical_time'])
        else:
            w=interleaving.receive(previous,decision,item,state['pending']['decision_ref'],capture_ref,seq,state['logical_time'],
                                   state=state,index=index,resolve=self._read)
        before=previous['goal'];after=w['goal'];info=decision.get('investigation')
        goal_consistent=(after is None) if before is None else (after is not None and all(before[k]==after[k] for k in ('id','generation','bindings')))
        expected_cursor=None if before is None else before['cursor']+int(info is not None and item['capture']['result']['status']=='OK')
        cursor_consistent=after is None if expected_cursor is None else after['cursor']==expected_cursor
        need(goal_consistent and cursor_consistent and item['request']==decision['request'],'checked source/goal/cursor continuity')
        if decision.get('inventory_acquisition') is None and decision.get('native_consumer') is None and queue and not self._protect_help_goal(w) and (w['goal'] is None or w['goal']['status'] in ('SATISFIED','BLOCKED','CANCELLED')):
            ctx=procedures.context(index,queue[0]['topic'],state['goals'])
            if ctx is not None and (w['goal'] is None or ctx['episode_key']!=w['goal']['episode_key']):
                w=procedures.start_goal(w,ctx,self.identity,w['active_ref'],track_receipts=state['config'].get('help_continuation_profile')==help_roles.PROFILE)
                w=guarded.attach(w,state,decision,item,'DIRECT')
        h=self._help_episode(w)
        if decision.get('help_check') is not None:
            need(h is not None and decision['help_check']==h['request_id'],'exact pending help check')
            operation=item['request']['operation'];result=item['capture']['result'];target=h['trigger']['path']
            visible={e['relative_path'] for e in (result['payload']['entries'] if operation=='LIST_SCOPE' and result['status']=='OK' else state['inventory']['entries']) if e['kind']=='file'}
            observed_path=(target if target in visible and result['status']=='OK' else None) if operation=='LIST_SCOPE' else next(m['path'] for m in decision['candidate_metadata'] if m['entry_ref']==item['request']['entry_ref'])
            check=dict(operation=operation,status=result['status'],request_id=item['request']['request_id'],receipt_sha256=item['receipt_sha256'],capture_ref=capture_ref,
                inventory_generation=state['inventory']['generation']+int(operation=='LIST_SCOPE'),path=observed_path,
                original_preconditions=self._help_original(state,w,index,visible) and (operation=='LIST_SCOPE' or (result['status']=='OK' and result['payload']['content_sha256']==h['trigger']['source_content_sha256'])))
            w['help']=help_policy.record_check(w['help'],check)
            if w['help']['episode']['phase']=='VERIFIED':
                w['goal'].update(status='ACTIVE',reason='OBSERVED_SOURCE_PRECONDITION_RESTORED');w['goal_transitions']+=1;w['trace']=[]
        if item['request']['operation']=='READ_TEXT':
            path=next(m['path'] for m in decision['candidate_metadata'] if m['entry_ref']==item['request']['entry_ref'])
            prior=state['latest_by_path'].get(path);result=item['capture']['result']
            new_evidence=prior is None or prior['last_status']!=result['status'] or (result['status']=='OK' and prior['content_sha256']!=result['payload']['content_sha256'])
            if new_evidence:
                _,score=observer.observe(state['observer'],decision['prediction'],item,decision['source'])
                groups={}
                for record in index['records']:
                    key=(record['topic'],record['claim']['context'])
                    groups.setdefault(key,set()).add(_hash(record['claim']))
                pred=None if score['status']!='SCORED' else dict(error=max(0.,score['mean_log_loss']-score['mean_entropy']),
                    model_sha256=score['model_sha256'],commitment_sha256=_hash(decision['prediction']))
                evidence=dict(identity=self.identity,instance_id=self.instance_id,scope_ref=state['scope_ref'],seq=seq,receipt_sha256=item['receipt_sha256'],
                    continuity=dict(request_bound=item['request']==decision['request'],cursor_consistent=cursor_consistent,goal_consistent=goal_consistent),
                    conflict=None if not groups else dict(unresolved=sum(len(x)>1 for x in groups.values()),considered=len(groups)),prediction=pred,
                    opportunity=dict(feasible=result['status']=='OK',expected_reduction=1./(1+(0 if prior is None else prior['successful_reads'])),registered_cost=1))
                w['appraisal']=regulation.update_regulation(w['appraisal'],evidence)
        return w

    def _deferred_receipt(self,state,decision,item,index,queue,workflow):
        if deferred.validate_profile(state['config'].get('investigation_queue_profile')) is None:
            return index,queue,workflow
        # This runs only after ordinary receipt and workflow validation. In
        # particular, a pre-receipt cursor of one is never proof of completion.
        if (queue or item['request']['operation']!='READ_TEXT' or item['capture']['result']['status']!='OK'
                or not state['inventory']['valid'] or not {'read','observe'}<=set(state['config']['capabilities'])
                or 'choice' in decision or 'continuation' in decision or 'continuation_ref' in state
                or workflow['interaction']['pending'] is not None or self._protect_help_goal(workflow)
                or (workflow['typed_wait'] is not None and workflow['typed_wait']['status']=='WAITING')):
            return index,queue,workflow
        goal=workflow['goal']
        if goal is not None:
            if goal['status']!='SATISFIED':return index,queue,workflow
            current=procedures.context(index,goal['topic'],state['goals'])
            if current is None or current['episode_key']!=goal['episode_key'] or current['bindings']!=goal['bindings']:
                return index,queue,workflow
        updated,proposals,ctx=deferred.select(index,state)
        if not proposals:return index,queue,workflow
        started=procedures.start_goal(workflow,ctx,self.identity,workflow['active_ref'],track_receipts=state['config'].get('help_continuation_profile')==help_roles.PROFILE)
        return updated,proposals,guarded.attach(started,state,decision,item,'DEFERRED')

    def _cohorts(self,state):
        from . import compact_cohorts as cohorts
        need(state['config']['renewal_profile']==RENEWAL_PROFILE and state['cohorts_ref'] is not None,'fresh renewal profile required')
        return cohorts.validate_registry(self._read(state['cohorts_ref'],'compact_cohorts'))

    def _next_cohorts(self,state,path,item,capture_ref,seq):
        from . import compact_cohorts as cohorts
        registry=self._cohorts(state)
        return registry if path is None else cohorts.collect(registry,path,item,capture_ref,seq)

    def _publication_profile(self,state,decision):
        extra=int(state['config']['renewal_profile'] is not None and 'local_request_ref' not in decision)
        choice_extra=int('choice_profile' in state['config'] and 'local_request_ref' not in decision)
        cont=int('continuation_ref' in state and 'local_request_ref' not in decision)
        memory=int('derived_observation_ref' in state and decision.get('inventory_acquisition') is not None)
        native_extra=2*int(decision.get('native_consumer') is not None)
        return 4+extra+choice_extra+cont+memory+native_extra,cont*2*(continuation.MAX_BYTES+MAX_HEADER+1)+PUBLICATION_ENVELOPE+extra*2*(MAX_COHORT_BYTES+MAX_HEADER+1)+choice_extra*2*(choice_learning.MAX_BYTES+MAX_HEADER+1)+memory*2*(derived.MAX_BYTES+MAX_HEADER+1)+int(native_extra>0)*2*(consumer.ROOT_BYTES+consumer.POLICY_BYTES+2*(MAX_HEADER+1))

    def _apply_capture(self, state, decision, item, capture_ref, next_field, field_ref, semantic_ref,workflow_ref,event,cohorts_ref=None,choice_ref=None,continuation_ref=None,derived_observation_ref=None,native_consumer_ref=None):
        if 'native_consumer_ref' in state:
            expected_ref=state['native_consumer_ref']
            if decision.get('native_consumer') is not None:
                root,policy=self._consumer(state);task=self._read(root['current']['task_ref'],consumer.TASK_KIND)
                nxt,newpolicy=consumer.receive(state,root,policy,state['config']['native_consumer_profile'],task,decision,item,capture_ref,workflow_ref,event['seq'],self._read)
                if newpolicy is not None:need(self._read(nxt['policy_ref'],consumer.POLICY_KIND)==newpolicy,'recomputed native n/Q policy')
                expected_ref=consumer.ref(nxt,consumer.ROOT_KIND,consumer.origin(state))
                need(self._read(expected_ref,consumer.ROOT_KIND)==nxt,'recomputed consumer receipt root')
            need(native_consumer_ref==expected_ref,'receipt-bound native consumer root')
            # Keep the old root until all original receipt verification passes.
        else:need(native_consumer_ref is None,'unexpected native consumer root')
        # All derivation inputs are this event and its committed prior roots.
        # Never consult self.journal/latest head inside the reducer.
        if 'derived_observation_ref' in state:
            record=derived.make_record(state,decision,item,capture_ref,workflow_ref,event['seq'],event['previous'],self._read)
            expected=state['derived_observation_ref']
            if record is not None:
                ref=derived.expected_ref(record)
                need(expected is None or expected==ref,'one immutable derived record')
                expected=ref
            need(derived_observation_ref==expected,'receipt-bound derived reference')
            if expected is not None:
                stored=self._read(expected,derived.KIND);derived.validate(stored,{k:state[k] for k in ('identity','instance_id','scope_ref')})
                if record is not None:need(_encode(stored)==_encode(record),'recomputed derived record differs')
            state['derived_observation_ref']=expected
        else:need(derived_observation_ref is None,'unexpected derived memory reference')
        pre_workflow_ref=state['workflow_ref']
        observer.validate_capture(item)
        need(item['request'] == decision['request'] and item['capture']['capture_origin'] == state['config']['capture_origin'], 'exact request/origin receipt')
        result = item['capture']['result']; operation = item['request']['operation']; path = None
        if operation == 'READ_TEXT':
            path = next(m['path'] for m in decision['candidate_metadata'] if m['entry_ref']==item['request']['entry_ref'])
        if 'continuation_ref' in state:
            expected=self._next_continuation(state,decision,item,capture_ref,path)
            need(self._read(continuation_ref,'compact_receipt_continuation')==expected,'receipt-derived continuation')
            state['continuation_ref']=continuation_ref
        else:need(continuation_ref is None,'unexpected continuation root')
        if 'choice_profile' in state['config']:
            expected_choice=self._next_choice(state,decision,item,capture_ref,path)
            need(self._read(choice_ref,'compact_choice_state')==expected_choice,'receipt-derived choice transition')
            state['choice_ref']=choice_ref
        else:need(choice_ref is None,'unexpected choice root')
        if state['config']['renewal_profile'] is not None:
            expected_registry=self._next_cohorts(state,path,item,capture_ref,event['seq'])
            need(self._read(cohorts_ref,'compact_cohorts')==expected_registry,'exact admitted exposure/cohort transition');state['cohorts_ref']=cohorts_ref
        else:need(cohorts_ref is None,'unexpected registry reference')
        prior = state['latest_by_path'].get(path) if path is not None else None
        first = operation == 'READ_TEXT' and (prior is None or prior['successful_reads']==0)
        old_field = self._read(state['field_ref'],'compact_field')
        expected_field = advance_field(old_field,item,perception_index=state['perceptions']+1,first_seen=first)
        need(_hash(expected_field) == _hash(next_field),'numerical field continuation differs')
        expected_semantics,semantic_result,semantic_queue=self._semantic_capture(state,item,capture_ref,path,event['seq'],allow_investigation=decision.get('native_consumer') is None)
        if decision.get('native_consumer') is not None:semantic_queue=copy.deepcopy(state['semantic_queue'])
        if state['config'].get('investigation_queue_profile') is None:
            # Preserve legacy validation order, including failure behavior.
            need(self._read(semantic_ref,'compact_semantics')==expected_semantics,'semantic provenance/index transition differs')
            expected_workflow=self._next_workflow(state,decision,item,capture_ref,event['seq'],expected_semantics,semantic_queue)
        else:
            expected_workflow=self._next_workflow(state,decision,item,capture_ref,event['seq'],expected_semantics,semantic_queue)
            expected_semantics,semantic_queue,expected_workflow=self._deferred_receipt(state,decision,item,expected_semantics,semantic_queue,expected_workflow)
            need(self._read(semantic_ref,'compact_semantics')==expected_semantics,'semantic provenance/index transition differs')
        need(self._read(workflow_ref,'compact_workflow')==expected_workflow,'receipt-bound workflow/regulation differs')
        state['workflow_ref']=workflow_ref
        state['semantics_ref']=semantic_ref;state['semantic_status']=semantic_result['status'];state['semantic_queue']=semantic_queue
        state['observer'],score = observer.observe(state['observer'],decision['prediction'],item,decision['source'])
        if operation == 'LIST_SCOPE':
            if state['config'].get('native_order_evidence_profile')==native_order_evidence.PROFILE:
                state['native_order_evidence']=native_order_evidence.record_list(state['native_order_evidence'],
                    decision,item,capture_ref,state['pending']['decision_ref'],event['seq'],state['inventory']['generation']+1)
            state['inventory'] = dict(valid=result['status']=='OK',generation=state['inventory']['generation']+1,
                list_receipt_ref=capture_ref,entries=copy.deepcopy(result['payload']['entries']) if result['status']=='OK' else [],
                next_list_time=state['logical_time']+1)
        else:
            need(decision['governance']['neuron_modules']==['compare','prediction_error'] and
                 decision['governance']['action_allocation']['allocations'][0]['id']==item['request']['entry_ref'],
                 'neurons and action must actually be allocated')
            known = 0 if prior is None else prior['successful_reads']
            compare = execute_neuron('compare',[known,0],state={'version':state['cognition_revision']},capabilities=('neuron',))
            changed = result['status']=='OK' and prior is not None and prior['content_sha256'] is not None and prior['content_sha256']!=result['payload']['content_sha256']
            informative = result['status']=='OK' and (first or changed)
            prediction_error = None if score['status']!='SCORED' else execute_neuron('prediction_error',
                [score['mean_entropy'],score['mean_log_loss']],state={'version':state['cognition_revision']},capabilities=('neuron',))
            previous_hash = None if prior is None else prior['content_sha256']
            record = dict(last_attempt_time=state['logical_time'],successful_reads=known+int(result['status']=='OK'),
                changes=(0 if prior is None else prior['changes'])+int(changed),
                content_sha256=result['payload']['content_sha256'] if result['status']=='OK' else previous_hash,
                last_receipt_ref=capture_ref,last_seq=event['seq'],last_status=result['status'])
            state['latest_by_path'][path] = record; state['last_path'] = path
            for goal in state['goals']:
                if goal['path']==path: goal['changes'] += int(changed)
            state['effective_feedback_updates'] += int(informative); state['completed_change_loops'] += int(changed)
            error = 0. if prediction_error is None or not state['config']['prediction_error_enabled'] else min(1.,abs(prediction_error['result']))
            # One-shot bounded U0 investigation appraisal: surprise is not reward.
            residual=0. if prediction_error is None else max(0.,prediction_error['result'])
            goal_index=next(i for i,g in enumerate(state['goals']) if g['path']==path)
            state['u0_investigation'][goal_index]=(residual/(1+residual)
                if informative and state['config']['prediction_error_enabled'] else 0.)
            salience = .35+.15*(1-compare['result'])+.25*error if state['config']['neuron_feedback_enabled'] else .5
            novelty = float(informative)*(1. if compare['result']==0 else .75) if state['config']['neuron_feedback_enabled'] else float(informative)
            state['control'] = update_control(state['control'],dict(tokens=[_token(path)],salience=salience,novelty=novelty),
                                             [_token(x['path']) for x in state['working']])
            state['working'] = (state['working']+[dict(path=path,receipt_ref=capture_ref,seq=event['seq'],
                changed=changed,informative=informative,compare=compare,prediction_error=prediction_error)])[-8:]
        state['perceptions'] += 1; state['cognition_revision'] += 1
        state['field_ref'] = field_ref; state['field_features'] = next_field['manifest']['features']; state['field_steps'] = next_field['manifest']['steps']
        state['last_receipt'] = dict(effect_id=state['pending']['effect_id'],capture_ref=capture_ref,field_ref=field_ref,semantics_ref=semantic_ref,workflow_ref=workflow_ref,seq=event['seq'])
        if cohorts_ref is not None:state['last_receipt']['cohorts_ref']=cohorts_ref
        if choice_ref is not None:state['last_receipt']['choice_ref']=choice_ref
        if continuation_ref is not None:state['last_receipt']['continuation_ref']=continuation_ref
        if 'derived_observation_ref' in state:state['last_receipt']['derived_observation_ref']=state['derived_observation_ref']
        if 'native_consumer_ref' in state:
            state['native_consumer_ref']=native_consumer_ref
            state['last_receipt']['native_consumer_ref']=native_consumer_ref
        self._collect_learning(state,path,capture_ref,event['seq'])
        if 'native_acquisition' in state:
            state['native_acquisition']=acquisition.receive(state,state['native_acquisition'],decision,item,capture_ref,
                state['pending']['decision_ref'],event['seq'],pre_workflow_ref,workflow_ref,self._read)
        state['pending'] = None
        return state

    def _check_effect_capacity(self, state, decision=None):
        if 'native_acquisition' in state:
            if decision is None:decision=self._read_decision(state['pending']['decision_ref'])
            completion=acquisition.completion_bytes(state,decision['request']['operation'])
            if 'derived_observation_ref' in state:
                completion+=derived.state_reserve(state)
                if decision.get('inventory_acquisition') is not None:
                    completion+=derived.passive_list_completion_bytes(state,decision,MAX_CAPTURE_BYTES)-native_order_evidence.MAX_BYTES
            if 'native_consumer_ref' in state:
                completion+=1024
                if 'derived_observation_ref' not in state and decision.get('inventory_acquisition') is not None:
                    completion+=consumer.passive_list_capacity(state,decision,MAX_CAPTURE_BYTES)-native_order_evidence.MAX_BYTES
                if decision.get('native_consumer',{}).get('phase')=='LIST':
                    completion+=consumer.passive_list_capacity(state,decision,MAX_CAPTURE_BYTES)-native_order_evidence.MAX_BYTES
            need(completion<=acquisition.LIVE_STATE_LIMIT,
                 'acquisition complete state capacity before effect')
        if decision is not None and decision.get('native_consumer') is not None:
            root,_=self._consumer(state);consumer.preflight(root)
        workflow=self._workflow(state)
        need(procedures.available(workflow,state['logical_time'])>=1,'engineering credits before effect')
        need(workflow['appraisal']['updates']<regulation.MAX_COUNTER,'appraisal counter capacity before effect')
        if self._help_episode(workflow) is not None:
            need(len(_encode(workflow))-len(_encode(workflow['help']))+help_policy.MAX_BYTES<=procedures.MAX_WORKFLOW_BYTES,'reserved help completion fits workflow before effect')
        if decision is None:decision = self._read_decision(state['pending']['decision_ref'])
        if 'choice_profile' in state['config'] or 'choice_context' in state['config']:
            # Optional context may add large immutable policy provenance. Keep
            # the legacy cap: admit worst next head growth BEFORE any callback.
            reserve=24576 if decision['request'].get('operation')=='LIST_SCOPE' else 8192
            need(len(_encode(state))+reserve<=65536,'optional choice head completion capacity before effect')
        if 'local_request_ref' in decision:
            need(procedures.available(workflow,state['logical_time'])>=2,'local engineering cost before effect')
            need(state['local_outcomes']<MAX_COUNTER and state['local_responses']<MAX_COUNTER and state['cognition_revision']<MAX_COUNTER,'local counter capacity')
            return
        need(state['perceptions'] < field_limits.MAX_PERCEPTIONS and
             state['field_steps']+5 <= numerical_limits.MAX_STEPS, 'field continuation capacity before effect')
        need(state['cognition_revision'] < MAX_COUNTER, 'cognition counter capacity before effect')
        if 'continuation_ref' in state:
            c=self._continuation(state)
            need(len(_encode(c))+continuation.MAX_COMPLETION_BYTES<=continuation.MAX_BYTES,'continuation completion capacity before effect')
            if decision.get('continuation') is not None:need(not c['veto'],'trusted veto stops dispatch')
        if decision['request']['operation']=='READ_TEXT':
            if 'choice_profile' in state['config']:
                choice_root=self._choice(state)
                need(len(choice_root['exposed'])<choice_learning.MAX_EXPOSURES,'choice exposure capacity before effect')
                # Exclusive continuation ownership leaves only one digest append.
                # No legacy trial, training row, nonce or use_count can grow.
                growth=128 if 'continuation_ref' in state else choice_learning.MAX_COMPLETION_BYTES
                if 'continuation_ref' in state:need(choice_root['current'] is None,'exclusive continuation choice ownership')
                need(len(_encode(choice_root))+growth<=choice_learning.MAX_BYTES,'choice completion byte capacity before effect')
            if state['config']['renewal_profile'] is not None:
                from .compact_cohorts import preflight_read
                preflight_read(self._cohorts(state))
            observed=state['observer']
            need(observed['attempted_reads'] < observer.MAX_COUNTER and observed['scored_reads'] < observer.MAX_COUNTER
                 and observed['training_transitions']+65535 <= observer.MAX_COUNTER, 'observer worst-case counter capacity before effect')
            need(state['control']['steps'] < MAX_COUNTER and state['effective_feedback_updates'] < MAX_COUNTER
                 and state['completed_change_loops'] < MAX_COUNTER, 'controller/feedback counter capacity before effect')

    @staticmethod
    def _help_episode(w):
        return None if w['help'] is None else w['help']['episode']

    @staticmethod
    def _protect_help_goal(w):
        h=CompactRuntime._help_episode(w);g=w['goal']
        return h is not None and g is not None and h['trigger']['goal']['id']==g['id'] and h['trigger']['goal']['generation']==g['generation'] and h['phase'] in ('PROPOSED','AWAITING_RESPONSE','AWAITING_LIST','AWAITING_READ')

    def _help_trigger(self,state,w):
        cfg=state['config']['autonomous_help'];g=w['goal']
        if cfg is None or g is None or g['status']!='BLOCKED' or g['cursor']>=2 or g['reason'] not in ('SOURCE_NOT_CURRENTLY_VISIBLE','DENIED_OR_FAILED_SOURCE'):return None
        if w['interaction']['pending'] is not None:return None
        index=self._read(state['semantics_ref'],'compact_semantics');ctx=procedures.context(index,g['topic'],state['goals'])
        if ctx is None or ctx['episode_key']!=g['episode_key'] or ctx['bindings']!=g['bindings']:return None
        inv=index['investigations'][g['topic']]
        if inv['rounds']!=1:return None
        visible={e['relative_path'] for e in state['inventory']['entries'] if e['kind']=='file'}
        spec=self._procedure(w);roles=[spec['order'][g['cursor']]] if spec is not None else list(procedures.ROLES)
        role=next((r for r in roles if (state['goals'][g['bindings'][r]]['path'] not in visible if g['reason']=='SOURCE_NOT_CURRENTLY_VISIBLE' else
             state['latest_by_path'].get(state['goals'][g['bindings'][r]]['path'],{}).get('last_status')=='DENIED')),None)
        if role is None:return None
        source_content=state['latest_by_path'].get(state['goals'][g['bindings'][role]]['path'],{}).get('content_sha256')
        if source_content is None:return None
        return dict(source_content_sha256=source_content,identity=self.identity,instance_id=self.instance_id,scope_ref=state['scope_ref'],status='BLOCKED',
            goal={k:copy.deepcopy(g[k]) for k in ('id','generation','episode_key','topic','bindings','cursor','receipts')},reason=g['reason'],
            source_workflow_ref=state['workflow_ref'],inventory_generation=state['inventory']['generation'],source_sha256=_hash(state),
            semantic_epoch=inv['epoch'],semantic_round=inv['rounds'],role=role,goal_index=g['bindings'][role],path=state['goals'][g['bindings'][role]]['path'])

    def _help_proposal(self,state,w):
        trigger=self._help_trigger(state,w)
        if trigger is None:return None
        cfg=state['config']['autonomous_help']
        hs,text=help_policy.propose_help(w['help'],trigger,cfg['endpoint'],state['logical_time'],cfg['deadline_span'],
            registry=state['config']['local_endpoints'],capabilities=state['config']['capabilities'],check_budget=2)
        return None if text is None else (hs,text)

    def _help_response(self,state,w,q,outcome,outcome_ref,report):
        h=self._help_episode(w)
        if h is not None and h['request_id']==q['request_id']:
            w['help']=help_policy.assess_response(w['help'],q['request_id'],outcome,state['logical_time'],outcome_ref,
                current_goal=w['goal'],current_episode_key=None if w['goal'] is None else w['goal']['episode_key'])
            report=copy.deepcopy(report);report['progress']=False
        return w,report

    def _help_original(self,state,w,index,visible):
        h=self._help_episode(w);g=w['goal']
        if h is None or g is None:return False
        frozen=h['trigger']['goal'];inv=index['investigations'].get(g['topic'])
        ctx=procedures.context(index,g['topic'],state['goals'])
        return (g['status']=='BLOCKED' and all(g[k]==frozen[k] for k in frozen)
            and inv is not None and inv['epoch']==h['trigger']['semantic_epoch'] and inv['rounds']==2
            and index['overflow'] is None and _hash(sorted({r['claim_sha256'] for r in index['records'] if r['topic']==g['topic']}))==h['trigger']['semantic_epoch']
            and g['episode_key']==_hash([g['topic'],h['trigger']['semantic_epoch'],h['trigger']['semantic_round']])
            and ctx is not None and ctx['episode_key']==_hash([g['topic'],h['trigger']['semantic_epoch'],2])
            and ctx is not None and ctx['bindings']==g['bindings']
            and all(state['goals'][i]['path'] in visible for i in g['bindings'].values())
            and {'read','observe'}<=set(state['config']['capabilities']))

    def _help_remaining_role(self,state,w):
        return help_roles.remaining_role(w['goal'],self._help_episode(w),state['goals'],
            {k:state[k] for k in ('identity','instance_id','scope_ref')},state['config']['capture_origin'],self._read,
            current_workflow_ref=state['workflow_ref'])

    def _workflow_problem(self,state,w):
        g=w['goal']
        if g is None or g['status'] not in ('ACTIVE','WAITING'):return None
        spec=w['probe'] if w['probe'] is not None else w['active_spec']
        guarded_problem=guarded.execution_problem(spec,g,self._read)
        if guarded_problem is not None:return guarded_problem
        if not {'read','observe'}<=set(state['config']['capabilities']):return 'CAPABILITY_REFUSED'
        if not state['inventory']['valid']:return None
        visible={e['relative_path'] for e in state['inventory']['entries'] if e['kind']=='file'}
        if any(state['goals'][i]['path'] not in visible for i in g['bindings'].values()):return 'SOURCE_NOT_CURRENTLY_VISIBLE'
        index=self._read(state['semantics_ref'],'compact_semantics');ctx=procedures.context(index,g['topic'],state['goals'])
        h=self._help_episode(w)
        repaired=h is not None and h['phase']=='VERIFIED' and h['trigger']['goal']['id']==g['id']
        if repaired:
            if state['config'].get('help_continuation_profile')==help_roles.PROFILE and self._procedure(w) is None:
                try:self._help_remaining_role(state,w)
                except (ValueError,KeyError,TypeError,IndexError,OSError,CLError):return 'HELP_ROLE_PROVENANCE_INVALID'
            inv=index['investigations'].get(g['topic'])
            if inv is None or inv['epoch']!=h['trigger']['semantic_epoch'] or inv['rounds']!=2 or ctx is None or ctx['bindings']!=g['bindings']:return 'SOURCE_EPISODE_CHANGED'
        elif ctx is None or ctx['episode_key']!=g['episode_key'] or ctx['bindings']!=g['bindings']:return 'SOURCE_EPISODE_CHANGED'
        if (state['config'].get('investigation_refresh_profile')==REFRESH_BLOCK_PROFILE
                and self._procedure(w) is None and not repaired and not self._protect_help_goal(w)
                and not (w['typed_wait'] is not None and w['typed_wait']['status']=='WAITING')
                and not any(q['topic']==g['topic'] and q['goal_index'] in g['bindings'].values()
                            for q in state['semantic_queue'])):
            # A valid new LIST does not restore invalidated investigation
            # proposals. Do not relabel ordinary monitoring or invent a retry.
            return 'INVESTIGATION_SCHEDULE_UNAVAILABLE'
        return None

    def _workflow_transition(self,state,kind,args,artifact_ref=None,provided=None,source_context=None):
        if 'native_acquisition' in state:
            need(kind not in ('EXTRACT_PROCEDURE','SELECT_PROCEDURE_CANDIDATE','DECLARE_PROCEDURE_VALIDATION','IMPORT_PROCEDURE_TEST','ADOPT_PROCEDURE'),
                 'acquisition research excludes procedure activation')
        need(state['pending'] is None,'idle workflow control');w=self._workflow(state);artifact=None;artifact_kind=None;report=None
        need(kind=='LOCAL_RESPONSE' or not self._unknown_local(w),'unresolved local outcome permits only correlated response/clock')
        if kind=='LOCAL_RESOURCE_WAIT':
            need(args=={} and source_context is not None,'local resource source context')
            choice=self._decision(state,source_context['global_seq'],source_context['journal_head'])
            need(choice is not None and choice.get('schema')=='compact-local-resource-hold-1','actual eligible local reservation')
            marker=choice['marker'];need(w['last_wait']!=marker,'repeated reservation poll')
            w['last_wait']=marker;w['waits']+=1;w['trace']=[]
        elif kind=='BEGIN_WAIT':
            need(type(args) is dict and set(args)=={'wake_at','deadline','scope'},'typed wait fields')
            wake=args['wake_at'];deadline=args['deadline'];_counter(wake);_counter(deadline)
            need(state['logical_time']<wake<deadline<=state['logical_time']+1024,'bounded future logical-time condition')
            need(w['typed_wait'] is None or w['typed_wait']['status']!='WAITING','one live typed wait')
            scope=args['scope'];need(type(scope) is dict,'wait scope')
            if scope.get('kind')=='monitor':
                need(set(scope)=={'kind','path'} and any(g['path']==scope['path'] for g in state['goals']),'existing monitor scope')
            else:
                g=w['goal'];need(g is not None and g['status'] in ('ACTIVE','PAUSED','WAITING') and scope==dict(kind='goal',id=g['id'],generation=g['generation']),'exact nonterminal current goal scope')
            condition=dict(schema='logical-time-wait-1',condition='AT_OR_AFTER',wake_at=wake,deadline=deadline,scope=scope)
            reason=json.dumps(condition,sort_keys=True,separators=(',',':'),ensure_ascii=False)
            cl='WAIT('+json.dumps(reason,ensure_ascii=False)+');';caps=state['config']['capabilities']
            compiled=Compiler([],caps).compile(cl);vm=CVM(caps);tx=vm.prepare(cl,dict(version=state['cognition_revision'],world_version=state['inventory']['generation']),[]);vm.verify(tx);payload=transaction_payload(tx)
            need(payload['ir']==compiled and payload['wait_reason']==reason and not payload['proposals'] and not payload['messages'] and not payload['neuron_calls'],'exact native no-effect wait')
            jobs=[dict(id='typed_wait',module='compare',cost=1,gain=1.)];allocation=governance_allocate(jobs,budget=1)
            need(allocation.get('allocations')==jobs,'wait requires exact governance allocation')
            artifact=dict(condition=condition,generated_cl=cl,transaction_payload=payload,source_sha256=_hash(state),governance=allocation,external_action_cost=0)
            artifact_kind='compact_typed_wait';w['typed_wait']=dict(ref=artifact_ref,scope=scope,wake_at=wake,deadline=deadline,status='WAITING',settled_at=None)
            need(w['typed_wait_count']<MAX_COUNTER,'typed wait count');w['typed_wait_count']+=1
        elif kind=='SETTLE_WAIT':
            need(args=={} and w['typed_wait'] is not None and w['typed_wait']['status']=='WAITING','live wait settlement')
            status=self._wait_status(w,state['logical_time']);need(status!='WAITING','wait condition not reached')
            w['typed_wait'].update(status=status,settled_at=state['logical_time'])
        elif kind=='EXPIRE_HELP':
            h=self._help_episode(w);need(args=={} and h is not None and h['phase']=='AWAITING_RESPONSE' and state['logical_time']>=h['deadline'],'exact expired help response hold')
            h.update(phase='FAILED',reason='DEADLINE');w['help']=help_policy.validate_help(w['help'])
        elif kind=='QUEUE_HELP':
            need(args=={},'automatic help has no caller-selected body')
            proposal=self._help_proposal(state,w);need(proposal is not None,'actual eligible blocked-source help')
            w['help'],text=proposal;h=w['help']['episode'];g=w['goal']
            artifact=interaction.make_request(w['interaction'],state['config']['local_endpoints'],'request_help',h['endpoint'],text,h['deadline'],state['logical_time'],_hash(state),dict(id=g['id'],generation=g['generation']))
            artifact_kind='compact_local_request'
            if artifact_ref is not None:
                w['interaction']=interaction.queue_request(w['interaction'],artifact,artifact_ref)
                w['help']=help_policy.bind_request(w['help'],artifact,artifact_ref)
        elif kind=='QUEUE_LOCAL':
            need(type(args) is dict and set(args)=={'action','target','request','deadline','goal'},'exact local intent')
            g=w['goal'];need(args['goal'] is None or (g is not None and g['status'] in ('ACTIVE','PAUSED','WAITING') and args['goal']=={'id':g['id'],'generation':g['generation']}),'current nonterminal goal binding')
            artifact=interaction.make_request(w['interaction'],state['config']['local_endpoints'],args['action'],args['target'],args['request'],args['deadline'],state['logical_time'],_hash(state),args['goal'])
            artifact_kind='compact_local_request'
            if artifact_ref is not None:w['interaction']=interaction.queue_request(w['interaction'],artifact,artifact_ref)
        elif kind=='LOCAL_RESPONSE':
            need(args=={},'local response fields')
            artifact=provided if provided is not None else self._read(artifact_ref,'compact_local_outcome')
            active=w['interaction']['pending'];last=w['interaction']['last']
            owner=next((x for x in (active,last) if x is not None and x['request_id']==artifact.get('request_id')),None)
            need(owner is not None,'reply must bind live or latest terminal request; older history is not reopened')
            q=self._read(owner['request_ref'],'compact_local_request');artifact=interaction.validate_outcome(artifact,q)
            artifact_kind='compact_local_outcome'
            exact_ref=artifact_ref if artifact_ref is not None else self.objects._frame(artifact_kind,object_json(artifact),'json')[1]
            current=self._local_goal_binding(w)
            w['interaction'],report=interaction.apply_outcome(w['interaction'],q,artifact,owner['request_ref'],exact_ref,state['logical_time'],current_goal=current)
            w,report=self._help_response(state,w,q,artifact,exact_ref,report)
            need(state['local_outcomes']<MAX_COUNTER and state['local_responses']<MAX_COUNTER and state['cognition_revision']<MAX_COUNTER,'local control counters')
        elif kind=='EXPIRE_LOCAL':
            need(args=={},'expiry fields');w['interaction']=interaction.expire_queued(w['interaction'],state['logical_time'])
        elif kind=='CANCEL_LOCAL':
            need(type(args) is dict and set(args)=={'request_id'},'queued cancellation fields')
            w['interaction']=interaction.cancel_queued(w['interaction'],args['request_id'])
        elif kind in ('GOAL_PAUSE','GOAL_RESUME','GOAL_CANCEL'):
            need(args=={},'goal control arguments');w=procedures.goal_control(w,kind[5:])
            pending=w['interaction']['pending'];g=w['goal']
            if kind=='GOAL_CANCEL' and pending is not None and pending['status']=='QUEUED' and pending['goal']==dict(id=g['id'],generation=g['generation']):
                w['interaction']=interaction.cancel_queued(w['interaction'],pending['request_id'])
        elif kind=='GOAL_BEGIN':
            need(type(args) is dict and set(args)=={'topic'},'goal topic')
            need(any(q['topic']==args['topic'] for q in state['semantic_queue']),'existing bounded investigation proposal')
            ctx=procedures.context(self._read(state['semantics_ref'],'compact_semantics'),args['topic'],state['goals'])
            need(w['goal'] is None or w['goal']['episode_key']!=ctx['episode_key'],'no reset of completed investigation round')
            w=procedures.start_goal(w,ctx,self.identity,w['active_ref'],track_receipts=state['config'].get('help_continuation_profile')==help_roles.PROFILE)
        elif kind=='GOAL_BLOCK':
            reason=self._workflow_problem(state,w);need(args=={} and reason is not None,'actual failed precondition')
            w['goal'].update(status='BLOCKED',reason=reason,assessment='BLOCKED_UNRESOLVED');w['trace']=[];w['goal_transitions']+=1
        elif kind=='REGULATOR_WAIT':
            need(args=={} and procedures.available(w,state['logical_time'])<1,'actual resource depletion')
            marker=dict(logical_time=state['logical_time'],reason='ENGINEERING_CREDITS_DEPLETED',resume_at=w['regulation']['last_time']+1)
            need(w['last_wait']!=marker,'unchanged poll cannot add wait credit')
            w['last_wait']=marker;w['waits']+=1;w['trace']=[]
            if w['goal'] is not None and w['goal']['status']=='ACTIVE':w['goal'].update(status='WAITING',reason=marker['reason']);w['goal_transitions']+=1
        elif kind=='REGULATOR_RESUME':
            need(args=={} and w['goal'] is not None and w['goal']['status']=='WAITING' and procedures.available(w,state['logical_time'])>=1,'actual refill condition')
            w['goal'].update(status='ACTIVE',reason='RECORDED_CLOCK_REFILL');w['goal_transitions']+=1
        elif kind=='EXTRACT_PROCEDURE':
            need(args=={} and self._source_learning_mode(state) and (w['validation'] is None or w['validation']['status']!='DECLARED'),'idle main-instance extraction')
            need(len(w['candidate_archive'])<4,'finite four-candidate catalog capacity')
            extractor=guarded.extract if state['config'].get('procedure_extraction_profile')==guarded.PROFILE else procedures.extract
            artifact=extractor(w,state['workflow_ref'],dict(identity=self.identity,instance_id=self.instance_id,scope_ref=state['scope_ref']),self._read)
            artifact_kind='compact_procedure';w['candidate_ref']=artifact_ref
            if artifact_ref is not None:w['candidate_archive'].append(artifact_ref)
        elif kind=='SELECT_PROCEDURE_CANDIDATE':
            need(type(args) is dict and set(args)=={'candidate_ref'} and args['candidate_ref'] in w['candidate_archive'],'previously admitted extraction only')
            need(w['validation'] is None or w['validation']['status']!='DECLARED','unfinished suite')
            self._verified_procedure(args['candidate_ref']);w['candidate_ref']=args['candidate_ref']
        elif kind=='DECLARE_PROCEDURE_VALIDATION':
            need(args=={} and self._source_learning_mode(state) and w['candidate_ref'] is not None,'actual main source candidate required')
            need(w['validation'] is None or w['validation']['status']!='DECLARED','unfinished test suite')
            candidate=self._verified_procedure(w['candidate_ref'])
            need(state['config']['seed'] not in dict(procedures.CASES).values(),'held-out seeds differ from source seed')
            artifact=dict(schema='compact-procedure-plan-1',candidate_ref=w['candidate_ref'],candidate_sha256=_hash(candidate),
                          incumbent_ref=w['active_ref'],source=dict(workflow_ref=state['workflow_ref'],state_sha256=_hash(state),cognition_revision=state['cognition_revision']),protocol=copy.deepcopy(procedures.TEST_PROTOCOL))
            artifact_kind='compact_procedure_plan'
            w['validation']=dict(plan_ref=artifact_ref,candidate_ref=w['candidate_ref'],incumbent_ref=w['active_ref'],results={},status='DECLARED')
        elif kind=='IMPORT_PROCEDURE_TEST':
            from . import compact_procedure_probe as probe
            need(type(args) is dict and set(args)=={'case'} and w['validation'] is not None and w['validation']['status']=='DECLARED','declared test suite')
            validation=w['validation'];case=args['case'];need(case in dict(procedures.CASES) and case not in validation['results'],'one first result per registered case')
            need(w['active_ref']==validation['incumbent_ref'] and w['candidate_ref']==validation['candidate_ref'],'frozen candidate/incumbent')
            candidate=self._verified_procedure(validation['candidate_ref']);plan=self._read(validation['plan_ref'],'compact_procedure_plan')
            need(plan['protocol']==procedures.TEST_PROTOCOL and plan['candidate_sha256']==_hash(candidate),'frozen protocol/candidate')
            artifact=provided if provided is not None else self._read(artifact_ref,'compact_procedure_test')
            report=probe.verify_case(artifact,candidate,validation['plan_ref'],case);artifact_kind='compact_procedure_test'
            validation['results'][case]=dict(capsule_ref=artifact_ref,outcome=report)
        elif kind=='ADOPT_PROCEDURE':
            from . import compact_procedure_probe as probe
            need(args=={} and w['validation'] is not None and w['validation']['status']=='DECLARED','complete prospective suite required')
            validation=w['validation'];need(set(validation['results'])==set(dict(procedures.CASES)),'all declared test/retention cases required')
            need(w['active_ref']==validation['incumbent_ref'] and w['candidate_ref']==validation['candidate_ref'],'incumbent/candidate unchanged')
            candidate=self._verified_procedure(validation['candidate_ref']);outcomes={};instances=set()
            for case,_ in procedures.CASES:
                result=validation['results'][case];capsule=self._read(result['capsule_ref'],'compact_procedure_test')
                outcome=probe.verify_case(capsule,candidate,validation['plan_ref'],case)
                need(outcome==result['outcome'],'recomputed test outcome differs')
                for measured in (outcome['probe'],outcome['baseline']):
                    if measured is not None:
                        need(measured['instance_id'] not in instances,'distinct actual fixture instances');instances.add(measured['instance_id'])
                outcomes[case]=outcome
            accepted=all(o['passed'] for o in outcomes.values()) and any(o['changed_order'] for o in outcomes.values())
            artifact=dict(schema='compact-procedure-evaluation-1',plan_ref=validation['plan_ref'],candidate_ref=validation['candidate_ref'],
                          results=copy.deepcopy(validation['results']),decision='ACCEPT' if accepted else 'REJECT',authority='NONE')
            artifact_kind='compact_procedure_acceptance';validation['status']=artifact['decision'];report=dict(decision=artifact['decision'])
            if accepted:
                w['previous_ref']=w['active_ref'];w['previous_spec']=w['active_spec'];w['previous_evaluation_ref']=w['active_evaluation_ref']
                w['active_ref']=validation['candidate_ref'];w['active_spec']=procedures.executable_spec(candidate);w['active_evaluation_ref']=artifact_ref
        else:need(False,'unknown workflow control')
        return procedures.validate_workflow(w),artifact,artifact_kind,report

    def _verified_procedure(self,ref):
        candidate=procedures.validate_candidate(self._read(ref,'compact_procedure'))
        need(candidate['origin']['identity']==self.identity and candidate['origin']['instance_id']==self.instance_id,'source candidate origin')
        archived=self._read(candidate['source_workflow_ref'],'compact_workflow')
        extractor=guarded.extract if candidate['schema']==guarded.SCHEMA else procedures.extract
        expected=extractor(archived,candidate['source_workflow_ref'],candidate['origin'],self._read)
        need(expected==candidate,'recomputed lived source procedure');return candidate

    def _procedure_bank_registry(self,state,artifact):
        # Called only after full closed verify_case replay, never on caller claims.
        from . import compact_cohorts as cohorts
        digests=set()
        for name in ('probe','baseline'):
            capsule=artifact[name]
            if capsule is None:continue
            objects={row['ref']['sha256']:row for row in capsule['objects']}
            for event in capsule['events']:
                if event['kind']!='RECEIPT':continue
                ref=event['payload']['capture_ref'];row=objects[ref['sha256']]
                need(row['ref']==ref,'verified bank capture reference')
                item=row['value']
                if item['request']['operation']=='READ_TEXT' and item['capture']['result']['status']=='OK':
                    digests.add(item['capture']['result']['payload']['content_sha256'])
        registry=cohorts.register_evaluation_digests(self._cohorts(state),sorted(digests))
        if registry['current'] is not None and registry['current']['status'] in ('TRAINING','FROZEN') and self._training_bank_collisions(registry):
            registry=cohorts.invalidate_current(registry,'EVALUATION_BANK_TRAINING_COLLISION')
        return registry

    def _choice_procedure_bank(self,state,artifact):
        # _workflow_transition has authenticated every capsule by closed replay.
        root=self._choice(state);digests=set()
        for name in ('probe','baseline'):
            capsule=artifact[name]
            if capsule is None:continue
            objects={row['ref']['sha256']:row for row in capsule['objects']}
            for event in capsule['events']:
                if event['kind']!='RECEIPT':continue
                ref=event['payload']['capture_ref'];row=objects[ref['sha256']]
                need(row['ref']==ref,'authenticated choice procedure bank reference')
                item=row['value'];result=item['capture']['result']
                if item['request']['operation']=='READ_TEXT' and result['status']=='OK':digests.add(result['payload']['content_sha256'])
        # Training collisions are checked even if a bank is imported late.
        for row in root['training']:
            for key in ('capture_ref','descriptor_ref'):
                item=self._read(row[key],'compact_capture');result=item['capture']['result']
                if result['status']=='OK' and result['payload']['content_sha256'] in digests:root['invalid']='REUSED_EVIDENCE'
        root['exposed']=sorted(set(root['exposed'])|digests)
        return choice_learning.bounded(root)

    def _workflow_control_locked(self,kind,args,provided=None):
        head=self.journal.snapshot();state=head['state'];source=self._source(state,head['seq'],head['event_head'])
        if 'native_acquisition' in state:
            need(kind not in ('EXTRACT_PROCEDURE','SELECT_PROCEDURE_CANDIDATE','DECLARE_PROCEDURE_VALIDATION','IMPORT_PROCEDURE_TEST','ADOPT_PROCEDURE'),
                 'acquisition research excludes procedure activation')
        w,artifact,artifact_kind,report=self._workflow_transition(state,kind,args,provided=provided,source_context=source)
        need(head['seq']<(journal_limits.MAX_SEGMENTS+1)*journal_limits.SEGMENT_RECORDS,'workflow control journal capacity')
        artifact_frame,artifact_ref=(b'',None) if artifact is None else self.objects._frame(artifact_kind,object_json(artifact),'json')
        w,_,_,_=self._workflow_transition(state,kind,args,artifact_ref,provided=provided,source_context=source)
        if self._help_episode(w) is not None:
            need(len(_encode(w))-len(_encode(w['help']))+help_policy.MAX_BYTES<=procedures.MAX_WORKFLOW_BYTES,'reserved help completion fits workflow before publication')
        wf_frame,wf_ref=self.objects._frame('compact_workflow',object_json(w),'json')
        record=dict(kind=kind,args=copy.deepcopy(args),source=source,
                    artifact_ref=artifact_ref,artifact_kind=artifact_kind,workflow_ref=wf_ref,report=report)
        bank=None;bank_frame=b'';bank_ref=None
        if kind=='IMPORT_PROCEDURE_TEST' and state['config']['renewal_profile'] is not None:
            bank=self._procedure_bank_registry(state,artifact)
            bank_frame,bank_ref=self.objects._frame('compact_cohorts',object_json(bank),'json');record['evaluation_registry_ref']=bank_ref
        choice_bank=None;choice_frame=b'';choice_ref=None
        if kind=='IMPORT_PROCEDURE_TEST' and state['config'].get('choice_profile',{}).get('source_adoption') is True:
            choice_bank=self._choice_procedure_bank(state,artifact)
            choice_frame,choice_ref=self.objects._frame('compact_choice_state',object_json(choice_bank),'json');record['choice_bank_ref']=choice_ref
        help_index=None;help_frame=b'';help_ref=None
        if kind=='QUEUE_HELP':
            help_index=copy.deepcopy(self._read(state['semantics_ref'],'compact_semantics'))
            topic=w['help']['episode']['trigger']['goal']['topic'];need(help_index['investigations'][topic]['rounds']==1,'remaining shared help round')
            help_index['investigations'][topic]['rounds']=2;semantics.validate_semantics(help_index)
            help_frame,help_ref=self.objects._frame('compact_semantics',object_json(help_index),'json');record['help_semantics_ref']=help_ref
        frame,ref=self.objects._frame('compact_workflow_control',object_json(record),'json')
        projected=copy.deepcopy(state);projected['workflow_ref']=wf_ref
        if help_ref is not None:projected['semantics_ref']=help_ref
        if bank_ref is not None:projected['cohorts_ref']=bank_ref
        if choice_ref is not None:projected['choice_ref']=choice_ref
        need(len(_encode(projected))<=65536 and len(_encode(record))<=16384,'workflow control bounds')
        with self.objects.publication_lease(max_objects=2+int(artifact is not None)+int(help_ref is not None)+int(bank_ref is not None)+int(choice_ref is not None),max_reserved_bytes=2*(len(frame)+len(wf_frame)+len(artifact_frame)+len(help_frame)+len(bank_frame)+len(choice_frame))) as lease:
            if choice_ref is not None:need(lease.put_json('compact_choice_state',choice_bank)==choice_ref,'exact choice procedure exposure registry')
            if bank_ref is not None:need(lease.put_json('compact_cohorts',bank)==bank_ref,'exact evaluation bank registry')
            if help_ref is not None:need(lease.put_json('compact_semantics',help_index)==help_ref,'exact reserved semantic round')
            if artifact is not None:need(lease.put_json(artifact_kind,artifact)==artifact_ref,'exact workflow artifact')
            need(lease.put_json('compact_workflow',w)==wf_ref,'exact workflow checkpoint')
            need(lease.put_json('compact_workflow_control',record)==ref,'exact workflow control')
            self.journal.append(kind,{'record_ref':ref})
        return artifact_ref if artifact is not None else copy.deepcopy(w)

    def workflow_snapshot(self):
        with self._locked():return self._workflow(self.journal.snapshot()['state'])

    def queue_local(self,action,target,request,deadline,goal=None):
        with self._locked():
            return self._workflow_control_locked('QUEUE_LOCAL',dict(action=action,target=target,request=request,deadline=deadline,goal=goal))

    def cancel_local(self,request_id=None):
        with self._locked():
            pending=self._workflow(self.journal.snapshot()['state'])['interaction']['pending']
            need(pending is not None,'no queued local intent')
            return self._workflow_control_locked('CANCEL_LOCAL',dict(request_id=pending['request_id'] if request_id is None else request_id))

    def begin_wait(self,wake_at,deadline,scope):
        with self._locked():return self._workflow_control_locked('BEGIN_WAIT',dict(wake_at=wake_at,deadline=deadline,scope=scope))

    @staticmethod
    def _local_goal_binding(w):
        g=w['goal']
        if g is None:return None
        h=CompactRuntime._help_episode(w)
        help_bound=(g['status']=='BLOCKED' and h is not None and h['phase']=='AWAITING_RESPONSE' and h['trigger']['goal']['id']==g['id'] and h['trigger']['goal']['generation']==g['generation'])
        if g['status']!='ACTIVE' and not help_bound:return None
        wait=w['typed_wait']
        if wait is not None and wait['status']=='WAITING' and wait['scope']==dict(kind='goal',id=g['id'],generation=g['generation']):return None
        return dict(id=g['id'],generation=g['generation'])

    @staticmethod
    def _wait_status(w,now):
        wait=w['typed_wait'];scope=wait['scope'];g=w['goal']
        if scope['kind']=='goal' and (g is None or scope!=dict(kind='goal',id=g['id'],generation=g['generation']) or g['status'] in ('BLOCKED','CANCELLED','SATISFIED')):return 'STALE'
        if now>=wait['deadline']:return 'TIMED_OUT'
        return 'WOKEN' if now>=wait['wake_at'] else 'WAITING'

    def poll_local(self,now):
        with self._locked():
            state=self.journal.snapshot()['state'];_counter(now);need(now>=state['logical_time'],'local clock rollback')
            if now>state['logical_time']:
                pending=state['pending']
                if pending is not None:
                    d=self._read_decision(pending['decision_ref'])
                    need(pending['phase']=='UNKNOWN' and 'local_request_ref' in d,'poll cannot change a prepared or folder effect')
                self.journal.append('LOCAL_CLOCK' if pending is not None else 'CLOCK',{'now':now})
            state=self.journal.snapshot()['state'];w=self._workflow(state);pending=w['interaction']['pending']
            if self._unknown_local(w):return copy.deepcopy(w['interaction'])
            if state['pending'] is None and w['typed_wait'] is not None and w['typed_wait']['status']=='WAITING' and self._wait_status(w,now)!='WAITING':
                self._workflow_control_locked('SETTLE_WAIT',{});state=self.journal.snapshot()['state'];w=self._workflow(state);pending=w['interaction']['pending']
            if state['pending'] is None and pending is not None and pending['status']=='QUEUED' and now>=pending['deadline']:
                return self._workflow_control_locked('EXPIRE_LOCAL',{})
            return copy.deepcopy(w['interaction'])

    def admit_local_response(self,outcome):
        with self._locked():
            self._reconcile_locked();state=self.journal.snapshot()['state'];pending=state['pending']
            if pending is not None:
                decision=self._read_decision(pending['decision_ref'])
                need(pending['phase']=='UNKNOWN' and 'local_request_ref' in decision,'response cannot replace an unstarted or folder effect')
                interaction.validate_outcome(outcome,decision['request'])
                need(self.journal.snapshot()['seq']<(journal_limits.MAX_SEGMENTS+1)*journal_limits.SEGMENT_RECORDS,'local recovery receipt capacity')
                with self.objects.publication_lease(max_objects=2,max_reserved_bytes=2*(16384+procedures.MAX_WORKFLOW_BYTES+2*(MAX_HEADER+1))) as lease:
                    return self._capture_local_locked(outcome,lease)
            _,artifact,_,report=self._workflow_transition(state,'LOCAL_RESPONSE',{},provided=outcome)
            if report['disposition']=='DUPLICATE':return self.objects._frame('compact_local_outcome',object_json(artifact),'json')[1]
            return self._workflow_control_locked('LOCAL_RESPONSE',{},provided=outcome)

    def pause_goal(self):
        with self._locked():return self._workflow_control_locked('GOAL_PAUSE',{})
    def resume_goal(self):
        with self._locked():return self._workflow_control_locked('GOAL_RESUME',{})
    def cancel_goal(self):
        with self._locked():return self._workflow_control_locked('GOAL_CANCEL',{})
    def extract_procedure(self):
        with self._locked():return self._workflow_control_locked('EXTRACT_PROCEDURE',{})
    def declare_procedure_validation(self):
        with self._locked():return self._workflow_control_locked('DECLARE_PROCEDURE_VALIDATION',{})
    def select_procedure_candidate(self,ref):
        with self._locked():return self._workflow_control_locked('SELECT_PROCEDURE_CANDIDATE',{'candidate_ref':ref})
    def adopt_procedure(self):
        with self._locked():return self._workflow_control_locked('ADOPT_PROCEDURE',{})
    def import_procedure_validation(self,capsule):
        need(type(capsule) is dict and 'case' in capsule,'test capsule')
        with self._locked():return self._workflow_control_locked('IMPORT_PROCEDURE_TEST',{'case':capsule['case']},provided=capsule)
    def run_procedure_validation(self,directory,case):
        from . import compact_procedure_probe as probe
        with self._locked():
            state=self.journal.snapshot()['state'];need(state['pending'] is None,'idle validation initiation');w=self._workflow(state)
            need(w['validation'] is not None and w['validation']['status']=='DECLARED' and case not in w['validation']['results'],'declared untested case')
            candidate=self._verified_procedure(w['validation']['candidate_ref']);plan_ref=w['validation']['plan_ref']
        capsule=probe.run_case(directory,candidate,plan_ref,case)
        return self.import_procedure_validation(capsule)

    def _cohort_materials(self,registry):
        current=registry['current'];need(current is not None,'open cohort required')
        anchor=self._read(current['retention_anchor_ref'],'compact_cohort_anchor')
        skill=self._verified_procedure(anchor['skill_ref'])
        acceptance=self._read(anchor['acceptance_ref'],'compact_procedure_acceptance')
        need(acceptance['decision']=='ACCEPT' and acceptance['candidate_ref']==anchor['skill_ref'],'accepted protected skill')
        incumbent=learning.validate_model(self._read(current['incumbent_ref'],'compact_model'))
        return current,anchor,skill,incumbent

    def _training_bank_collisions(self,registry):
        current=registry['current'];ref=current['candidate_ref'] or current['incumbent_ref']
        bank=set(registry['eval_exposures']);model=learning.validate_model(self._read(ref,'compact_model'));visited=set();collisions=set()
        for _ in range(4):
            collisions.update(row['content_sha256'] for row in model['training'] if row['content_sha256'] in bank)
            parent_ref=model['parent_ref']
            if parent_ref is None:break
            need(parent_ref['sha256'] not in visited,'acyclic bounded model ancestry');visited.add(parent_ref['sha256'])
            model=learning.validate_model(self._read(parent_ref,'compact_model'))
        else:need(False,'finite cumulative lineage bound')
        return sorted(collisions)

    def _cohort_evaluation(self,state,registry):
        from . import compact_skill_retention as retention
        current,anchor,skill,incumbent=self._cohort_materials(registry)
        if current['status']=='INVALID':return dict(schema='compact-cohort-evaluation-1',decision='REJECT',reason=current['reason'],width=None,old_skill=None)
        need(current['status'] in ('FROZEN','ACCEPTED'),'frozen cohort required')
        need(all(slot in current['evidence'] for slots in current['plan'].values() for slot in slots),'all first receipts required')
        candidate=learning.validate_model(self._read(current['candidate_ref'],'compact_model'))
        collisions=self._training_bank_collisions(registry)
        if collisions:return dict(schema='compact-cohort-evaluation-1',decision='REJECT',reason='EVALUATION_BANK_TRAINING_COLLISION',collisions=sorted(collisions),width=None,old_skill=None)
        records=[(registry['paths'][int(slot)],current['evidence'][slot]['capture_ref']) for slot in current['plan']['train']]
        need(candidate==learning.fit_cumulative(records,lambda ref:self._read(ref,'compact_capture'),incumbent,current['incumbent_ref']),'recomputed cumulative candidate')
        partitions={role:[self._read(current['evidence'][slot]['capture_ref'],'compact_capture') for slot in current['plan'][role]] for role in learning.ROLES[1:]}
        width=learning.evaluate(candidate,incumbent,partitions)
        if width['decision']=='REJECT':return dict(schema='compact-cohort-evaluation-1',decision='REJECT',reason='WIDTH_GATE_REJECTED',width=width,old_skill=None,anchor_ref=current['retention_anchor_ref'])
        need(anchor['protocol']==retention.PROTOCOL,'predeclared protected task protocol')
        plan=self._read(current['retention_plan_ref'],'compact_skill_retention_plan')
        need(set(current['retention_results'])==set(dict(retention.CASES)),'all original skill retention contexts required')
        values={case:self._read(ref,'compact_skill_retention_case') for case,ref in current['retention_results'].items()}
        skill_result=retention.verify_suite(values,current['retention_plan_ref'],plan,skill,incumbent,candidate)
        # Full measured outcomes remain reproducible from immutable closed capsules.
        # The bounded report keeps their exact hashes, references and decisions.
        skill_result=dict(decision=skill_result['decision'],plan_ref=current['retention_plan_ref'],protocol_sha256=_hash(retention.PROTOCOL),
            results={case:dict(passed=value['passed'],failure_reasons=value['failure_reasons'],outcome_sha256=_hash(value),capsule_ref=current['retention_results'][case]) for case,value in skill_result['results'].items()},
            truth_adopted=False,interpretation=retention.PROTOCOL['interpretation'])
        return dict(schema='compact-cohort-evaluation-1',decision='ACCEPT' if width['decision']==skill_result['decision']=='ACCEPT' else 'REJECT',
            reason=None,width=width,old_skill=skill_result,anchor_ref=current['retention_anchor_ref'])

    def _cohort_transition(self,state,kind,args,source,provided=None):
        need('native_acquisition' not in state,'acquisition research excludes learning/adoption controls')
        from . import compact_cohorts as cohorts
        from . import compact_skill_retention as retention
        state=copy.deepcopy(state);need(state['pending'] is None,'idle cohort control')
        w=self._workflow(state);need(not self._unknown_local(w),'UNKNOWN cannot alter cohort/model')
        need(state['config']['mode']=='SYNTHETIC_CONTINUATION','main-instance cohort controls only')
        registry=self._cohorts(state);artifacts={};result=None
        def emit(name,object_kind,value,limit):
            need(len(_encode(value))<=limit,'cohort artifact byte budget')
            frame,ref=self.objects._frame(object_kind,object_json(value),'json');artifacts[name]=dict(kind=object_kind,value=value,ref=ref,frame_bytes=len(frame));return ref
        short_source=dict(seq=source['global_seq'],head=source['journal_head'],cognition_revision=source['cognition_revision'])
        if kind=='QUEUE_COHORT':
            need(state['inventory']['valid'],'current successful LIST required')
            visible={e['relative_path'] for e in state['inventory']['entries'] if e['kind']=='file'}
            need(type(args) is dict and set(args)==set(learning.ROLES) and all(type(paths) is list and all(p in visible for p in paths) for paths in args.values()),'LIST-bound declared cohorts')
            registry=cohorts.queue_plan(registry,args,short_source)
        elif kind=='OPEN_COHORT':
            need(args=={} and state['inventory']['valid'],'idle listed cohort open')
            need(w['active_ref'] is not None and w['active_evaluation_ref'] is not None,'actual accepted old skill required')
            need(w['validation'] is None or w['validation']['status']!='DECLARED','unfinished skill validation')
            skill=self._verified_procedure(w['active_ref']);accepted=self._read(w['active_evaluation_ref'],'compact_procedure_acceptance')
            need(accepted['decision']=='ACCEPT' and accepted['candidate_ref']==w['active_ref'],'protected accepted procedure')
            need(registry['queue'],'queued plan required')
            visible={e['relative_path'] for e in state['inventory']['entries'] if e['kind']=='file'}
            need(all(registry['paths'][int(slot)] in visible for slots in registry['queue'][0]['plan'].values() for slot in slots),'queued sources still LIST-visible at OPEN')
            anchor=dict(schema='compact-cohort-anchor-1',source=source,skill_ref=w['active_ref'],acceptance_ref=w['active_evaluation_ref'],
                skill_sha256=_hash(skill),protocol=copy.deepcopy(retention.PROTOCOL),incumbent_ref=state['active_model_ref'])
            anchor_ref=emit('anchor','compact_cohort_anchor',anchor,16384)
            registry=cohorts.open_next(registry,short_source,state['active_model_ref'],[w['active_ref']],anchor_ref)
        elif kind=='FREEZE_COHORT':
            need(args=={},'freeze has no caller model');current,anchor,skill,incumbent=self._cohort_materials(registry)
            need(current['status']=='TRAINING' and current['incumbent_ref']==state['active_model_ref'],'prospective training with unchanged incumbent')
            need(w['active_ref']==anchor['skill_ref'] and w['active_evaluation_ref']==anchor['acceptance_ref'],'protected skill unchanged')
            need(all(slot in current['evidence'] for slot in current['plan']['train']),'training first receipts required')
            records=[(registry['paths'][int(slot)],current['evidence'][slot]['capture_ref']) for slot in current['plan']['train']]
            model=learning.fit_cumulative(records,lambda ref:self._read(ref,'compact_capture'),incumbent,current['incumbent_ref'])
            model_ref=emit('model','compact_model',model,learning.MAX_MODEL_BYTES)
            need(anchor['protocol']==retention.PROTOCOL,'frozen retention protocol')
            plan=retention.make_plan(source,anchor['skill_ref'],skill,anchor['acceptance_ref'],current['incumbent_ref'],incumbent,model_ref,model)
            plan_ref=emit('retention_plan','compact_skill_retention_plan',plan,16384)
            registry=cohorts.mark_frozen(registry,model_ref,plan_ref)
        elif kind=='IMPORT_COHORT_RETENTION':
            need(type(args) is dict and set(args)=={'case'} and provided is not None,'exact supplied retention case')
            current,anchor,skill,incumbent=self._cohort_materials(registry)
            need(current['status']=='FROZEN' and current['incumbent_ref']==state['active_model_ref'],'frozen unchanged retention context')
            candidate=self._read(current['candidate_ref'],'compact_model');plan=self._read(current['retention_plan_ref'],'compact_skill_retention_plan')
            measured=retention.verify_case(provided,current['retention_plan_ref'],plan,skill,incumbent,candidate,args['case'])
            ref=emit('case','compact_skill_retention_case',provided,retention.MAX_CASE_BYTES)
            registry=cohorts.record_retention(registry,args['case'],ref)
            registry=cohorts.register_evaluation_digests(registry,measured['evaluation_content_sha256'])
            if registry['current']['status']=='FROZEN' and self._training_bank_collisions(registry):
                registry=cohorts.invalidate_current(registry,'EVALUATION_BANK_TRAINING_COLLISION')
            result=dict(case=args['case'],passed=measured['passed'])
        elif kind in ('EVALUATE_COHORT','ACTIVATE_COHORT'):
            need(args=={},'no caller evaluation decision');current=registry['current'];need(current is not None,'current cohort')
            need(current['incumbent_ref']==state['active_model_ref'],'incumbent unchanged')
            anchor=self._read(current['retention_anchor_ref'],'compact_cohort_anchor')
            need(w['active_ref']==anchor['skill_ref'] and w['active_evaluation_ref']==anchor['acceptance_ref'],'protected procedure unchanged')
            result=self._cohort_evaluation(state,registry)
            if kind=='EVALUATE_COHORT':
                ref=emit('report','compact_cohort_report',result,65536);registry=cohorts.mark_evaluated(registry,ref,result['decision'])
            else:
                need(current['status']=='ACCEPTED' and result['decision']=='ACCEPT' and self._read(current['report_ref'],'compact_cohort_report')==result,'recomputed accepted model and old-skill evidence')
                model=self._read(current['candidate_ref'],'compact_model');state['learning']['previous_model_ref']=state['active_model_ref'];state['active_model_ref']=current['candidate_ref']
                state['observer']=observer.activate(state['observer'],model);registry=cohorts.mark_activated(registry)
        elif kind=='ABORT_COHORT':
            need(args=={} and registry['current'] is not None and registry['current']['status'] in ('TRAINING','FROZEN','INVALID'),'unfinished cohort abort')
            result=dict(schema='compact-cohort-evaluation-1',decision='REJECT',reason='CONTROLLER_ABORT',width=None,old_skill=None)
            report_ref=emit('report','compact_cohort_report',result,65536);registry=cohorts.mark_evaluated(registry,report_ref,'REJECT')
        elif kind=='CLOSE_COHORT':
            need(args=={} and registry['current'] is not None,'terminal current cohort')
            archive=dict(schema='compact-cohort-archive-1',source=source,current=copy.deepcopy(registry['current']),previous_archive_ref=registry['archives'][-1]['archive_ref'] if registry['archives'] else None,
                active_model_ref=state['active_model_ref'],protected_skill_ref=w['active_ref'])
            ref=emit('archive','compact_cohort_archive',archive,MAX_COHORT_BYTES);registry=cohorts.close_current(registry,ref)
        else:need(False,'unknown cohort control')
        return state,cohorts.validate_registry(registry),artifacts,result

    def _cohort_control(self,kind,args,provided=None):
        with self._locked():
            head=self.journal.snapshot();state=head['state'];source=self._source(state,head['seq'],head['event_head'])
            projected,registry,artifacts,result=self._cohort_transition(state,kind,args,source,provided)
            need(head['seq']<(journal_limits.MAX_SEGMENTS+1)*journal_limits.SEGMENT_RECORDS,'cohort journal capacity')
            registry_frame,registry_ref=self.objects._frame('compact_cohorts',object_json(registry),'json');projected['cohorts_ref']=registry_ref
            record=dict(kind=kind,args=copy.deepcopy(args),source=source,registry_ref=registry_ref,artifacts={name:a['ref'] for name,a in artifacts.items()},result_sha256=_hash(result))
            frame,ref=self.objects._frame('compact_cohort_control',object_json(record),'json')
            need(len(_encode(projected))<=65536 and len(_encode(record))<=16384,'cohort control/state caps')
            required=2*(len(frame)+len(registry_frame)+sum(a['frame_bytes'] for a in artifacts.values()))
            with self.objects.publication_lease(max_objects=2+len(artifacts),max_reserved_bytes=required) as lease:
                for a in artifacts.values():need(lease.put_json(a['kind'],a['value'])==a['ref'],'exact cohort artifact')
                need(lease.put_json('compact_cohorts',registry)==registry_ref,'exact cohort root')
                need(lease.put_json('compact_cohort_control',record)==ref,'exact cohort control')
                self.journal.append(kind,{'record_ref':ref})
            return copy.deepcopy(result if result is not None else registry['current'] if registry['current'] is not None else registry)

    def cohort_snapshot(self):
        with self._locked():return self._cohorts(self.journal.snapshot()['state'])
    def queue_cohort(self,plan):return self._cohort_control('QUEUE_COHORT',plan)
    def open_cohort(self):return self._cohort_control('OPEN_COHORT',{})
    def freeze_cohort_candidate(self):return self._cohort_control('FREEZE_COHORT',{})
    def evaluate_cohort(self):return self._cohort_control('EVALUATE_COHORT',{})
    def activate_cohort(self):return self._cohort_control('ACTIVATE_COHORT',{})
    def close_cohort(self):return self._cohort_control('CLOSE_COHORT',{})
    def abort_cohort(self):return self._cohort_control('ABORT_COHORT',{})
    def import_cohort_retention(self,capsule):
        need(type(capsule) is dict and 'case' in capsule,'typed retention case')
        return self._cohort_control('IMPORT_COHORT_RETENTION',{'case':capsule['case']},capsule)
    def run_cohort_retention(self,directory,case):
        from . import compact_skill_retention as retention
        with self._locked():
            state=self.journal.snapshot()['state'];need(state['pending'] is None,'idle test initiation')
            registry=self._cohorts(state);current,anchor,skill,incumbent=self._cohort_materials(registry)
            need(current['status']=='FROZEN' and case not in current['retention_results'],'declared untested context')
            candidate=self._read(current['candidate_ref'],'compact_model');plan=self._read(current['retention_plan_ref'],'compact_skill_retention_plan');plan_ref=current['retention_plan_ref']
        capsule=retention.run_case(directory,plan_ref,plan,skill,incumbent,candidate,case)
        return self.import_cohort_retention(capsule)

    def _collect_learning(self,state,path,capture_ref,seq):
        if path is None:return
        path=str(next(i for i,g in enumerate(state['goals']) if g['path']==path))
        study=state['learning']['study']
        if study is not None and study['status'] in ('DECLARED','FROZEN'):
            role=next((role for role,paths in study['plan'].items() if path in paths),None)
            if role is not None and path not in study['evidence']:
                study['evidence'][path]=copy.deepcopy(capture_ref)
                if role!='train' and study['status']=='DECLARED':
                    study['status']='INVALID';study['reason']='Held-out first receipt admitted before candidate freeze'
        retention=state['learning']['retention']
        if retention is not None and retention['status']=='DECLARED' and path in retention['paths'] and path not in retention['evidence']:
            retention['evidence'][path]=copy.deepcopy(capture_ref)
            if state['config']['renewal_profile'] is not None:retention['receipt_seqs'][path]=seq

    def _learning_transition(self,state,kind,args,model_ref=None,source_seq=None):
        need('native_acquisition' not in state,'acquisition research excludes learning/adoption controls')
        state=copy.deepcopy(state);need(state['pending'] is None,'model controls require idle executor')
        need(not self._unknown_local(self._workflow(state)),'unresolved local outcome blocks model changes')
        if state['config']['renewal_profile'] is not None:
            need(kind in ('DECLARE_RETENTION','EVALUATE_ROLLBACK'),'renewal profile uses prospective cohort controls')
            need(self._cohorts(state)['current'] is None,'close cohort before retrospective rollback check')
        need(state['config']['retention_probe'] is None,'probe cannot mutate model controls')
        life=state['learning'];study=life['study'];new_model=None;report=None
        resolve=lambda ref:self._read(ref,'compact_capture')
        active=learning.validate_model(self._read(state['active_model_ref'],'compact_model'))
        need(observer.model_identity(state['observer'])==_hash(active),'active observer/model binding')
        if kind=='DECLARE_STUDY':
            need(study is None or study['status'] in ('ACTIVATED','REJECTED','INVALID'),'unfinished study')
            need(life['retention'] is None or life['retention']['status']!='DECLARED','unfinished retention')
            plan=learning.validate_plan(args,state)
            plan={role:[str(next(i for i,g in enumerate(state['goals']) if g['path']==p)) for p in paths] for role,paths in plan.items()}
            life['study']=dict(plan=plan,status='DECLARED',reason=None,evidence={},candidate_ref=None,
                incumbent_ref=state['active_model_ref'],declared_revision=state['cognition_revision'],
                list_ref=state['inventory']['list_receipt_ref'],protocol=dict(learning.PROTOCOL),report=None)
        elif kind=='DECLARE_RETENTION':
            need(life['previous_model_ref'] is not None,'no verified previous active model')
            need(life['retention'] is None or life['retention']['status']!='DECLARED','unfinished retention')
            need(study is None or study['status'] in ('ACTIVATED','REJECTED','INVALID'),'unfinished study')
            need(type(args) is list and 1<=len(args)<=8 and len(set(args))==len(args),'retention path bounds')
            visible={e['relative_path'] for e in state['inventory']['entries'] if e['kind']=='file'}
            train={r['path'] for r in active['training']}
            need(state['inventory']['valid'] and all(p in visible and p in {g['path'] for g in state['goals']} and p not in train for p in args),'LIST-bound non-training retention')
            life['retention']=dict(paths=[str(next(i for i,g in enumerate(state['goals']) if g['path']==p)) for p in args],evidence={},status='DECLARED',
                active_ref=state['active_model_ref'],parent_ref=life['previous_model_ref'],
                declared_revision=state['cognition_revision'],list_ref=state['inventory']['list_receipt_ref'])
            if state['config']['renewal_profile'] is not None:
                registry=self._cohorts(state)
                need(type(source_seq) is int and source_seq>=registry['last_read_seq'],'retention declaration sequence')
                need(all(registry['path_roles'].get(p)=='retention' for p in life['retention']['paths']),'permanent retention roles required')
                life['retention'].update(declared_seq=source_seq,receipt_seqs={})
        elif kind=='EVALUATE_ROLLBACK':
            need(args=={} and life['retention'] is not None and life['retention']['status']=='DECLARED','declared retention required')
            r=life['retention'];need(r['active_ref']==state['active_model_ref'] and r['parent_ref']==life['previous_model_ref'],'retention activation changed')
            need(set(r['evidence'])==set(r['paths']),'retention incomplete')
            parent=learning.validate_model(self._read(r['parent_ref'],'compact_model'))
            need(active['parent_ref']==r['parent_ref'],'exact rollback parent')
            items=[resolve(r['evidence'][p]) for p in r['paths']]
            try:
                hashes={x['content_sha256'] for x in active['training']};seen=set()
                for slot,item in zip(r['paths'],items):
                    learning.measured(item);content=item['capture']['result']['payload']['content_sha256']
                    if state['config']['renewal_profile'] is not None:
                        registry=self._cohorts(state);receipt_seq=r['receipt_seqs'][slot]
                        exposure=next((x for x in registry['exposures'] if x['content_sha256']==content),None)
                        need(registry['path_roles'].get(slot)=='retention' and receipt_seq>r['declared_seq'] and exposure is not None and exposure['first_seq']==receipt_seq and exposure['path_slot']==slot and content not in registry['eval_exposures'],'renewal retention requires fresh post-declaration first exposure')
                    need(content not in hashes and content not in seen,'retention content overlap');seen.add(content)
                current=learning.metric(active,items);old=learning.metric(parent,items)
                rollback=current['mean_log_loss']-old['mean_log_loss']>learning.PROTOCOL['maximum_old_loss_increase']
                report=dict(decision='ROLLBACK' if rollback else 'KEEP',active=current,previous=old,reason=None)
            except ValueError as exc:report=dict(decision='REJECT_EVIDENCE',active=None,previous=None,reason=str(exc));rollback=False
            r['status']=report['decision']
            if rollback:
                state['active_model_ref']=r['parent_ref'];life['previous_model_ref']=parent['parent_ref']
                state['observer']=observer.activate(state['observer'],parent)
        else:
            need(args=={} and study is not None,'existing study and no caller outcome')
            need(state['active_model_ref']==study['incumbent_ref'],'study incumbent changed')
            if kind=='FREEZE_CANDIDATE':
                need(study['status']=='DECLARED' and all(p in study['evidence'] for p in study['plan']['train']),'training complete before heldout')
                try:
                    new_model=learning.fit([(state['goals'][int(p)]['path'],study['evidence'][p]) for p in study['plan']['train']],resolve,active,state['active_model_ref'])
                    study['candidate_ref']=model_ref;study['status']='FROZEN'
                except ValueError as exc:
                    study['status']='REJECTED';report=dict(decision='REJECT',failure_reasons=[str(exc)],metrics={});study['report']=_hash(report)
            elif kind=='EVALUATE_STUDY':
                need(study['status'] in ('FROZEN','INVALID'),'candidate frozen or retained invalid study')
                if study['status']=='INVALID':report=dict(decision='REJECT',failure_reasons=[study['reason']],metrics={})
                else:
                    need(all(p in study['evidence'] for paths in study['plan'].values() for p in paths),'all first receipts required')
                    candidate=learning.validate_model(self._read(study['candidate_ref'],'compact_model'))
                    expected=learning.fit([(state['goals'][int(p)]['path'],study['evidence'][p]) for p in study['plan']['train']],resolve,active,study['incumbent_ref'])
                    need(candidate==expected,'candidate training evidence changed')
                    report=learning.evaluate(candidate,active,{role:[resolve(study['evidence'][p]) for p in study['plan'][role]] for role in learning.ROLES[1:]})
                study['status']='ACCEPTED' if report['decision']=='ACCEPT' else 'REJECTED';study['report']=_hash(report)
            elif kind=='ACTIVATE_MODEL':
                need(study['status']=='ACCEPTED','accepted prospective evaluation required')
                candidate=learning.validate_model(self._read(study['candidate_ref'],'compact_model'))
                report=learning.evaluate(candidate,active,{role:[resolve(study['evidence'][p]) for p in study['plan'][role]] for role in learning.ROLES[1:]})
                need(_hash(report)==study['report'] and report['decision']=='ACCEPT','recomputed accepted report required')
                life['previous_model_ref']=state['active_model_ref'];state['active_model_ref']=study['candidate_ref']
                state['observer']=observer.activate(state['observer'],candidate);study['status']='ACTIVATED'
            else:need(False,'unknown learning control')
        return state,new_model,report

    def _model_control(self,kind,args):
        with self._locked():
            head=self.journal.snapshot();state=head['state']
            need('native_acquisition' not in state,'acquisition research excludes model controls')
            need(state['pending'] is None,'idle-only model controls')
            remaining=(journal_limits.MAX_SEGMENTS+1)*journal_limits.SEGMENT_RECORDS-head['seq']
            need(remaining>=1,'journal capacity before model publications')
            _,model,report=self._learning_transition(state,kind,args,source_seq=head['seq'])
            model_frame,model_ref=(b'',None) if model is None else self.objects._frame('compact_model',object_json(model),'json')
            record=dict(kind=kind,args=copy.deepcopy(args),source=self._source(state,head['seq'],head['event_head']),model_ref=model_ref,report=report,
                        study_snapshot=copy.deepcopy(state['learning']['study']),retention_snapshot=copy.deepcopy(state['learning']['retention']))
            frame,ref=self.objects._frame('compact_learning_control',object_json(record),'json')
            projected,_,_=self._learning_transition(state,kind,args,model_ref,source_seq=head['seq'])
            projected['learning']['last_control_ref']=ref
            need(len(_encode(projected))<=65536,'model control live-state capacity')
            need(len(_encode(record))<=24576 and (model is None or len(_encode(model))<=learning.MAX_MODEL_BYTES),'control object budgets')
            with self.objects.publication_lease(max_objects=1+int(model is not None),max_reserved_bytes=2*(len(frame)+len(model_frame))) as lease:
                if model is not None:need(lease.put_json('compact_model',model)==model_ref,'exact predicted model reference')
                need(lease.put_json('compact_learning_control',record)==ref,'exact predicted control reference')
                self.journal.append(kind,{'record_ref':ref})
            return copy.deepcopy(report if report is not None else self.journal.snapshot()['state']['learning'])

    def declare_study(self,plan):return self._model_control('DECLARE_STUDY',plan)
    def freeze_candidate(self):return self._model_control('FREEZE_CANDIDATE',{})
    def evaluate_study(self):return self._model_control('EVALUATE_STUDY',{})
    def activate_model(self):return self._model_control('ACTIVATE_MODEL',{})
    def declare_retention(self,paths):return self._model_control('DECLARE_RETENTION',paths)
    def evaluate_rollback(self):return self._model_control('EVALUATE_ROLLBACK',{})

    def _semantic_proposal(self,state,args):
        need(state['pending'] is None and not state['semantic_queue'] and state['inventory']['valid'],'idle valid inventory without queued investigation')
        need(not self._unknown_local(self._workflow(state)),'unresolved local outcome blocks investigation controls')
        need(type(args) is dict and set(args)=={'subject','predicate'},'semantic topic fields')
        index=self._read(state['semantics_ref'],'compact_semantics')
        updated,report=semantics.request_investigation(index,args['subject'],args['predicate'],state['inventory']['entries'])
        queue=[]
        for proposal in report['proposals']:
            matches=[i for i,g in enumerate(state['goals']) if g['path']==proposal['path'] and g['status']=='ACTIVE']
            need(len(matches)==1,'investigation remains within existing monitored goals')
            queue.append(dict(goal_index=matches[0],topic=proposal['topic'],intent_key=proposal['intent_key'],round=proposal['round']))
        need(len(queue)<=2,'bounded investigation queue')
        return updated,report,queue

    def _semantic_capture(self,state,item,capture_ref,path,seq,allow_investigation=True):
        index=self._read(state['semantics_ref'],'compact_semantics');queue=copy.deepcopy(state['semantic_queue'])
        if path is None:return index,dict(status='NO_SUCCESSFUL_TEXT'),queue
        index,result=semantics.apply_capture(index,item,capture_ref,path,seq)
        record=result.get('record')
        if allow_investigation and record is not None and not queue and state['inventory']['valid']:
            claim=record['claim']
            index,proposal=semantics.request_investigation(index,claim['subject'],claim['predicate'],state['inventory']['entries'])
            for q in proposal['proposals']:
                matches=[i for i,g in enumerate(state['goals']) if g['path']==q['path'] and g['status']=='ACTIVE']
                need(len(matches)==1,'automatic investigation stays in monitored goals')
                queue.append(dict(goal_index=matches[0],topic=q['topic'],intent_key=q['intent_key'],round=q['round']))
        return index,result,queue

    def semantic_query(self,subject,predicate,*,context=None):
        with self._locked():
            index=self._read(self.journal.snapshot()['state']['semantics_ref'],'compact_semantics')
            for record in index['records']:semantics.verify_record(record,self._read(record['capture_ref'],'compact_capture'))
            return semantics.query(index,subject,predicate,context=context)

    def semantic_assessment(self,subject,predicate,*,context=None):
        with self._locked():
            index=self._read(self.journal.snapshot()['state']['semantics_ref'],'compact_semantics')
            for record in index['records']:semantics.verify_record(record,self._read(record['capture_ref'],'compact_capture'))
            return semantics.assess_evidence(index,subject,predicate,context=context)

    def request_investigation(self,subject,predicate):
        with self._locked():
            head=self.journal.snapshot();state=head['state'];args=dict(subject=subject,predicate=predicate)
            index,report,queue=self._semantic_proposal(state,args)
            if not queue:return report
            need(head['seq']<(journal_limits.MAX_SEGMENTS+1)*journal_limits.SEGMENT_RECORDS,'journal room for investigation')
            index_frame,index_ref=self.objects._frame('compact_semantics',object_json(index),'json')
            record=dict(args=args,source=self._source(state,head['seq'],head['event_head']),index_ref=index_ref,report=report)
            frame,ref=self.objects._frame('compact_semantic_control',object_json(record),'json')
            projected=copy.deepcopy(state);projected.update(semantics_ref=index_ref,semantic_queue=queue)
            need(len(_encode(projected))<=65536 and len(_encode(record))<=16384,'semantic control bounds')
            with self.objects.publication_lease(max_objects=2,max_reserved_bytes=2*(len(frame)+len(index_frame))) as lease:
                need(lease.put_json('compact_semantics',index)==index_ref,'exact semantic index publication')
                need(lease.put_json('compact_semantic_control',record)==ref,'exact semantic control publication')
                self.journal.append('SEMANTIC_INVESTIGATION',{'record_ref':ref})
            return report

    def _reduce(self, state, event):
        need(state['schema']==SCHEMA and state['identity']==self.identity,'state identity/schema')
        kind = event['kind']; p = event['payload']; effect = event['effect']
        if kind == 'INITIALIZE':
            need(state['field_ref'] is None and set(p)==({'field_ref','model_ref','semantics_ref','workflow_ref'}|({'cohorts_ref'} if state['config']['renewal_profile'] is not None else set())|({'choice_ref'} if 'choice_profile' in state['config'] else set())|({'native_consumer_ref'} if 'native_consumer_ref' in state else set())) and effect is None,'single birth initialization')
            expected_birth = _birth(state['identity'],state['scope_ref'],
                [{'path':g['path'],'period':g['period']} for g in state['goals']],state['config']['seed'],
                state['config']['field_enabled'],state['config']['u0_enabled'],state['config']['neuron_feedback_enabled'],state['config']['prediction_error_enabled'],state['config']['procedure_probe'],state['config']['probe_contract'],state['config']['regulation_enabled'],state['config']['local_endpoints'],state['config']['regulation_channels'],state['config']['autonomous_help'],state['config']['renewal_profile'],state['config']['retention_probe'],state['config'].get('choice_profile'),state['config'].get('choice_context'),state['config'].get('native_binding'),state['config'].get('procedure_learning_profile'),state['config'].get('investigation_queue_profile'),state['config'].get('procedure_extraction_profile'),state['config'].get('investigation_refresh_profile'),state['config'].get('help_continuation_profile'),state['config'].get('native_order_evidence_profile'),state['config'].get('native_acquisition_profile'),state['config'].get('derived_observation_profile'),state['config'].get('native_consumer_profile'))
            need(_hash(state)==_hash(expected_birth),'exact declared fresh birth; no fabricated continuity')
            field = self._read(p['field_ref'],'compact_field')
            need(_hash(field)==_hash(initial_field(state['config']['seed'])),'exact seeded initial field')
            model = self._read(p['model_ref'],'compact_model')
            expected_model=learning.uniform_model() if state['config']['retention_probe'] is None else state['config']['retention_probe']['model']
            need(model == expected_model,'exact declared initial model object')
            if state['config']['renewal_profile'] is not None:
                from . import compact_cohorts as cohorts
                expected_registry=cohorts.initial_registry(self.identity,self.instance_id,state['scope_ref'],[g['path'] for g in state['goals']])
                need(self._read(p['cohorts_ref'],'compact_cohorts')==expected_registry,'exact empty exposure registry birth');state['cohorts_ref']=p['cohorts_ref']
            state['active_model_ref']=p['model_ref']
            if 'choice_profile' in state['config']:
                need(self._read(p['choice_ref'],'compact_choice_state')==choice_learning.initial(),'exact empty choice birth')
                state['choice_ref']=p['choice_ref']
            need(self._read(p['semantics_ref'],'compact_semantics')==semantics.initial_semantics(self.identity,self.instance_id,state['scope_ref']),'exact empty semantic birth')
            state['semantics_ref']=p['semantics_ref']
            need(self._read(p['workflow_ref'],'compact_workflow')==self._workflow_birth(state),'exact workflow birth')
            state['workflow_ref']=p['workflow_ref']
            state['instance_id']=event['instance_id']; state['field_ref']=p['field_ref']; state['field_features']=field['manifest']['features']
            if 'native_consumer_ref' in state:
                o=consumer.origin(state);root=self._read(p['native_consumer_ref'],consumer.ROOT_KIND)
                policy=self._read(root['policy_ref'],consumer.POLICY_KIND)
                need(policy==consumer.initial_policy(o) and root==consumer.initial_root(o,root['policy_ref'],state['config']['native_consumer_profile']),'exact native consumer birth')
                state['native_consumer_ref']=p['native_consumer_ref']
        elif kind in ('NATIVE_TASK_BEGIN','NATIVE_POLICY_FREEZE','NATIVE_TASK_CANCEL'):
            need(set(p)=={'record_ref'} and effect is None and state['pending'] is None,'idle native consumer control')
            record=self._read(p['record_ref'],'compact_native_consumer_control');object_json(record,consumer.CONTROL_BYTES)
            need(set(record)=={'schema','kind','task_id','source','root_ref'} and record['schema']==state['config']['native_consumer_profile']['schema']
                 and record['kind']==kind and record['source']==self._source(state,event['seq']-1,event['previous']),'native control source/fields')
            root,policy=self._consumer(state)
            if kind=='NATIVE_TASK_BEGIN':need(consumer.allowed(state,self._workflow(state)),'safe post-acquisition task begin')
            nxt,task,newpolicy=consumer.control(state,root,policy,state['config']['native_consumer_profile'],kind,record['task_id'],record['source'],event['seq'])
            if task is not None:need(self._read(nxt['current']['task_ref'],consumer.TASK_KIND)==task,'recomputed native task')
            if newpolicy is not None:need(self._read(nxt['policy_ref'],consumer.POLICY_KIND)==newpolicy,'recomputed native freeze policy')
            need(consumer.ref(nxt,consumer.ROOT_KIND,consumer.origin(state))==record['root_ref']
                 and self._read(record['root_ref'],consumer.ROOT_KIND)==nxt,'recomputed native control root')
            state['native_consumer_ref']=record['root_ref']
        elif kind in ('CONTINUATION_GRANT','CONTINUATION_VETO','CONTINUATION_EXPIRE'):
            need(set(p)=={'args','source','continuation_ref'} and effect is None,'continuation control fields')
            need(p['source']==self._source(state,event['seq']-1,event['previous']),'continuation source binding')
            expected=self._continuation_transition(state,kind,p['args'],p['source'])
            need(self._read(p['continuation_ref'],'compact_receipt_continuation')==expected,'rederived continuation control')
            state['continuation_ref']=p['continuation_ref']
        elif kind in ('CHOICE_BEGIN','CHOICE_FREEZE'):
            need('continuation_ref' not in state,'continuation owns incumbent')
            need(set(p)=={'role','source','choice_ref'} and effect is None and state['pending'] is None and 'choice_profile' in state['config'],'idle choice control')
            need(p['source']==self._source(state,event['seq']-1,event['previous']),'exact choice source')
            w=self._workflow(state)
            need(w['goal'] is None or w['goal']['status'] in ('SATISFIED','CANCELLED'),'protected goal blocks choice')
            need(w['interaction']['pending'] is None and not self._protect_help_goal(w),'protected interaction blocks choice')
            if kind=='CHOICE_BEGIN':
                need(state['inventory']['valid'] and max(state['service_debt'])<SERVICE_DEBT_LIMIT-1,'service existing obligations before trial')
                if p['role']=='use':self._require_native_adopted_choice(state,self._choice(state))
                if 'adoption_ref' in self._choice(state) and p['role'] in ('validation','test'):
                    need(self._choice_adoption(self._choice(state))['status']=='FROZEN','retention plan frozen before heldout')
            expected=choice_learning.control(self._choice(state),kind,p['role'],state['logical_time'])
            need(self._read(p['choice_ref'],'compact_choice_state')==expected,'rederived choice control')
            state['choice_ref']=p['choice_ref']
        elif kind in ('ENABLE_CHOICE_ADOPTION','FREEZE_CHOICE_ADOPTION','IMPORT_CHOICE_RETENTION','EVALUATE_CHOICE_ADOPTION','ADOPT_CHOICE'):
            need(set(p)=={'record_ref'} and effect is None,'adoption control event')
            record=self._read(p['record_ref'],'compact_choice_adoption_control')
            need(type(record) is dict and set(record)=={'kind','source','choice_ref','artifacts','result_sha256'} and record['kind']==kind and record['source']==self._source(state,event['seq']-1,event['previous']),'exact source adoption control')
            provided=self._read(record['artifacts']['case'],'compact_skill_retention_case') if kind=='IMPORT_CHOICE_RETENTION' else None
            projected,root,artifacts,result=self._choice_adoption_transition(state,kind,record['source'],provided)
            need(record['artifacts']=={name:a['ref'] for name,a in artifacts.items()} and record['result_sha256']==_hash(result),'rederived adoption artifacts and result')
            for a in artifacts.values():need(self._read(a['ref'],a['kind'])==a['value'],'exact adoption artifact')
            need(self._read(record['choice_ref'],'compact_choice_state')==root,'rederived adoption root')
            state=projected;state['choice_ref']=record['choice_ref']
        elif kind in ('EXPIRE_HELP','QUEUE_HELP','CANCEL_LOCAL','LOCAL_RESOURCE_WAIT','BEGIN_WAIT','SETTLE_WAIT','QUEUE_LOCAL','LOCAL_RESPONSE','EXPIRE_LOCAL','GOAL_BEGIN','GOAL_PAUSE','GOAL_RESUME','GOAL_CANCEL','GOAL_BLOCK','REGULATOR_WAIT','REGULATOR_RESUME','EXTRACT_PROCEDURE','SELECT_PROCEDURE_CANDIDATE','DECLARE_PROCEDURE_VALIDATION','IMPORT_PROCEDURE_TEST','ADOPT_PROCEDURE'):
            need(set(p)=={'record_ref'} and effect is None,'workflow control event')
            record=self._read(p['record_ref'],'compact_workflow_control')
            need(type(record) is dict and set(record)==({'kind','args','source','artifact_ref','artifact_kind','workflow_ref','report'}|({'help_semantics_ref'} if kind=='QUEUE_HELP' else set())|({'evaluation_registry_ref'} if kind=='IMPORT_PROCEDURE_TEST' and state['config']['renewal_profile'] is not None else set())|({'choice_bank_ref'} if kind=='IMPORT_PROCEDURE_TEST' and state['config'].get('choice_profile',{}).get('source_adoption') is True else set())) and
                 record['kind']==kind and record['source']==self._source(state,event['seq']-1,event['previous']),'workflow control source')
            w,artifact,artifact_kind,report=self._workflow_transition(state,kind,record['args'],record['artifact_ref'],source_context=record['source'])
            need(report==record['report'] and artifact_kind==record['artifact_kind'],'workflow derived outcome')
            if artifact is not None:need(self._read(record['artifact_ref'],artifact_kind)==artifact,'recomputed workflow artifact')
            else:need(record['artifact_ref'] is None,'unexpected workflow artifact')
            need(self._read(record['workflow_ref'],'compact_workflow')==w,'exact workflow state transition')
            if kind=='QUEUE_HELP':
                expected=copy.deepcopy(self._read(state['semantics_ref'],'compact_semantics'));topic=w['help']['episode']['trigger']['goal']['topic']
                need(expected['investigations'][topic]['rounds']==1,'shared round was not available');expected['investigations'][topic]['rounds']=2
                need(self._read(record['help_semantics_ref'],'compact_semantics')==expected,'exact shared round reservation')
                state['semantics_ref']=record['help_semantics_ref'];state['semantic_queue']=[]
            if kind=='IMPORT_PROCEDURE_TEST' and state['config']['renewal_profile'] is not None:
                expected=self._procedure_bank_registry(state,artifact)
                need(self._read(record['evaluation_registry_ref'],'compact_cohorts')==expected,'exact disclosed evaluation bank')
                state['cohorts_ref']=record['evaluation_registry_ref']
            if kind=='IMPORT_PROCEDURE_TEST' and state['config'].get('choice_profile',{}).get('source_adoption') is True:
                expected=self._choice_procedure_bank(state,artifact)
                need(self._read(record['choice_bank_ref'],'compact_choice_state')==expected,'exact authenticated choice bank exposure')
                state['choice_ref']=record['choice_bank_ref']
            state['workflow_ref']=record['workflow_ref']
            if kind=='LOCAL_RESPONSE':
                need(report['disposition']!='DUPLICATE','duplicate reply must not publish a new transition')
                state['local_outcomes']+=1;state['local_responses']+=int(report['progress']);state['cognition_revision']+=int(report['progress'])
            if kind in ('GOAL_CANCEL','GOAL_BLOCK'):state['semantic_queue']=[]
        elif kind=='SEMANTIC_INVESTIGATION':
            need(set(p)=={'record_ref'} and effect is None,'semantic control payload')
            record=self._read(p['record_ref'],'compact_semantic_control')
            need(type(record) is dict and set(record)=={'args','source','index_ref','report'} and record['source']==self._source(state,event['seq']-1,event['previous']),'semantic control source')
            index,report,queue=self._semantic_proposal(state,record['args'])
            need(queue and report==record['report'] and self._read(record['index_ref'],'compact_semantics')==index,'exact recomputed semantic investigation')
            state.update(semantics_ref=record['index_ref'],semantic_queue=queue)
        elif kind in ('QUEUE_COHORT','OPEN_COHORT','FREEZE_COHORT','IMPORT_COHORT_RETENTION','EVALUATE_COHORT','ACTIVATE_COHORT','CLOSE_COHORT','ABORT_COHORT'):
            need(set(p)=={'record_ref'} and effect is None,'cohort control event')
            record=self._read(p['record_ref'],'compact_cohort_control')
            need(type(record) is dict and set(record)=={'kind','args','source','registry_ref','artifacts','result_sha256'} and record['kind']==kind and record['source']==self._source(state,event['seq']-1,event['previous']),'cohort exact source')
            provided=self._read(record['artifacts']['case'],'compact_skill_retention_case') if kind=='IMPORT_COHORT_RETENTION' else None
            projected,registry,artifacts,result=self._cohort_transition(state,kind,record['args'],record['source'],provided)
            need(record['artifacts']=={name:a['ref'] for name,a in artifacts.items()} and record['result_sha256']==_hash(result),'recomputed cohort artifacts/outcome')
            for a in artifacts.values():need(self._read(a['ref'],a['kind'])==a['value'],'cohort immutable artifact mismatch')
            need(self._read(record['registry_ref'],'compact_cohorts')==registry,'cohort registry transition mismatch')
            state=projected;state['cohorts_ref']=record['registry_ref']
        elif kind in ('DECLARE_STUDY','FREEZE_CANDIDATE','EVALUATE_STUDY','ACTIVATE_MODEL','DECLARE_RETENTION','EVALUATE_ROLLBACK'):
            need(set(p)=={'record_ref'} and effect is None and state['pending'] is None,'idle model control')
            record=self._read(p['record_ref'],'compact_learning_control')
            need(type(record) is dict and set(record)=={'kind','args','source','model_ref','report','study_snapshot','retention_snapshot'},'control record fields')
            need(record['study_snapshot']==state['learning']['study'] and record['retention_snapshot']==state['learning']['retention'],'exact immutable control evidence snapshot')
            need(record['kind']==kind and record['source']==self._source(state,event['seq']-1,event['previous']),'exact control source')
            state,model,report=self._learning_transition(state,kind,record['args'],record['model_ref'],source_seq=event['seq']-1)
            need(report==record['report'],'recomputed evaluation differs')
            if model is not None:need(self._read(record['model_ref'],'compact_model')==model,'derived model differs')
            else:need(record['model_ref'] is None,'unexpected model publication')
            state['learning']['last_control_ref']=p['record_ref']
            if kind=='ACTIVATE_MODEL':
                state['learning']['study']=dict(status='ACTIVATED',archive_ref=p['record_ref'])
        elif kind == 'CLOCK':
            need(set(p)=={'now'} and effect is None,'clock fields'); _counter(p['now'])
            need(p['now']>state['logical_time'] and state['pending'] is None,'monotonic idle clock')
            state['logical_time']=p['now']
        elif kind=='LOCAL_CLOCK':
            need(set(p)=={'now'} and effect is None,'local clock fields');_counter(p['now'])
            pending=state['pending'];need(pending is not None and pending['phase']=='UNKNOWN' and p['now']>state['logical_time'],'unknown local clock only')
            need('local_request_ref' in self._read_decision(pending['decision_ref']),'not a folder effect clock')
            state['logical_time']=p['now']
        elif kind == 'REFRESH':
            need(p=={} and effect is None and state['pending'] is None,'refresh admission')
            need('native_acquisition' not in state,'acquisition research excludes external refresh')
            if 'choice_profile' in state['config']:need(self._choice(state)['current'] is None,'active choice binds current LIST generation')
            if 'continuation_ref' in state:need(self._continuation(state)['current'] is None,'active continuation binds LIST generation')
            need(not self._unknown_local(self._workflow(state)),'unresolved local outcome blocks inventory changes')
            if state['config'].get('native_order_evidence_profile')==native_order_evidence.PROFILE:
                state['native_order_evidence']=native_order_evidence.record_refresh(state['native_order_evidence'],event['seq'])
            state['inventory']['valid']=False; state['inventory']['entries']=[]; state['inventory']['next_list_time']=state['logical_time']
            state['semantic_queue']=[]  # Prior inventory-bound proposals are no longer eligible.
        elif kind == 'REQUEST':
            need(set(p)=={'decision_ref'} and effect=={'phase':'REQUEST','id':self.identity+':'+str(event['seq'])},'request fields/effect')
            decision=self._read_decision(p['decision_ref'])
            expected=self._decision(state,event['seq']-1,event['previous'])
            need(expected is not None and _hash(decision)==_hash(expected),'decision/source/governance/prediction/CL mismatch')
            state['pending']=dict(effect_id=effect['id'],decision_ref=p['decision_ref'],phase='REQUESTED')
            if decision.get('inventory_acquisition') is not None:
                state['native_acquisition']=acquisition.request(state['native_acquisition'],decision,p['decision_ref'])
            if 'local_request_ref' in decision:
                state['last_service_family']='local'
            elif decision['request']['operation']=='READ_TEXT':
                state['last_service_family']='monitor'
                selected=decision['request']['entry_ref']
                for row in decision['candidate_metadata']:
                    i=next(i for i,g in enumerate(state['goals']) if g['path']==row['path'])
                    state['service_debt'][i]=0 if row['entry_ref']==selected else min(SERVICE_DEBT_LIMIT,state['service_debt'][i]+1)
                    if row['entry_ref']==selected:state['u0_investigation'][i]=0.
                selected_index=next(i for i,g in enumerate(state['goals']) if any(m['path']==g['path'] and m['entry_ref']==selected for m in decision['candidate_metadata']))
                if decision.get('native_consumer') is None:
                    state['semantic_queue']=[q for q in state['semantic_queue'] if q['goal_index']!=selected_index]
            state['governed_attempts']+=1
        elif kind == 'START':
            need(set(p)=={'publication_admission'} and state['pending'] is not None and state['pending']['phase']=='REQUESTED'
                 and effect=={'phase':'START','id':state['pending']['effect_id']},'exact start; UNKNOWN never retry')
            admission=p['publication_admission']; need(type(admission) is dict and set(admission)=={'max_objects','max_reserved_bytes','accounting'},'admission fields')
            decision=self._read_decision(state['pending']['decision_ref'])
            expected_objects,expected_bytes=self._publication_profile(state,decision)
            need(type(admission['max_objects']) is int and admission['max_objects']==expected_objects and
                 type(admission['max_reserved_bytes']) is int and admission['max_reserved_bytes']==expected_bytes,'worst-case two-frame envelope')
            accounting=admission['accounting']; expected_accounting=self.objects._initial()
            need(type(accounting) is dict and set(accounting)==set(expected_accounting) and
                 all(accounting[k]==v for k,v in expected_accounting.items() if k not in ('reservations','reserved_bytes')),'admitted store identity/profile')
            for key in ('reservations','reserved_bytes'):_counter(accounting[key])
            need(accounting['reservations']+expected_objects<=accounting['max_reservations'] and
                 accounting['reserved_bytes']+expected_bytes<=accounting['max_reserved_bytes'],'pre-effect capacity admission')
            need(event['seq'] < (journal_limits.MAX_SEGMENTS+1)*journal_limits.SEGMENT_RECORDS,
                 'START must leave capacity for its RECEIPT')
            self._check_effect_capacity(state,decision=decision)
            state['pending']['phase']='UNKNOWN'
            if 'native_acquisition' in state and decision.get('inventory_acquisition') is not None:
                state['native_acquisition']=acquisition.start(state['native_acquisition'],state['pending']['decision_ref'])
        elif kind == 'RECEIPT':
            need(set(p)==({'capture_ref','field_ref','semantics_ref','workflow_ref'}|({'cohorts_ref'} if state['config']['renewal_profile'] is not None else set())|({'choice_ref'} if 'choice_profile' in state['config'] else set())|({'continuation_ref'} if 'continuation_ref' in state else set())|({'derived_observation_ref'} if 'derived_observation_ref' in state else set())|({'native_consumer_ref'} if 'native_consumer_ref' in state else set())) and state['pending'] is not None and state['pending']['phase']=='UNKNOWN'
                 and effect=={'phase':'RECEIPT','id':state['pending']['effect_id']},'exact receipt admission')
            decision=self._read_decision(state['pending']['decision_ref'])
            item=self._read(p['capture_ref'],'compact_capture'); field=self._read(p['field_ref'],'compact_field')
            state=self._apply_capture(state,decision,item,p['capture_ref'],field,p['field_ref'],p['semantics_ref'],p['workflow_ref'],event,p.get('cohorts_ref'),p.get('choice_ref'),p.get('continuation_ref'),p.get('derived_observation_ref'),p.get('native_consumer_ref'))
        elif kind=='LOCAL_RECEIPT':
            need(set(p)=={'outcome_ref','workflow_ref'} and state['pending'] is not None and state['pending']['phase']=='UNKNOWN'
                 and effect=={'phase':'RECEIPT','id':state['pending']['effect_id']},'exact local receipt barrier')
            decision=self._read_decision(state['pending']['decision_ref'])
            outcome=self._read(p['outcome_ref'],'compact_local_outcome')
            w,report=self._local_workflow(state,decision,outcome,p['outcome_ref'],event['seq'])
            need(self._read(p['workflow_ref'],'compact_workflow')==w,'recomputed local workflow')
            state['workflow_ref']=p['workflow_ref'];state['local_outcomes']+=1
            state['local_responses']+=int(report['progress']);state['cognition_revision']+=int(report['progress'])
            state['last_local_receipt']=dict(effect_id=state['pending']['effect_id'],outcome_ref=p['outcome_ref'],workflow_ref=p['workflow_ref'],seq=event['seq'])
            state['pending']=None
        else: need(False,'unknown transition')
        if 'native_acquisition' in state:
            state['native_acquisition']=acquisition.recheck(state,self._workflow(state),state['native_acquisition'],self._read)
        need(event['perception_count']==state['perceptions'],'perception counter only on validated receipt')
        for key in ('logical_time','cognition_revision','perceptions','governed_attempts','effective_feedback_updates','completed_change_loops','field_steps'):
            _counter(state[key])
        need(len(state['latest_by_path'])<=8 and len(state['working'])<=8 and len(_encode(state))<=65536,'bounded live roots')
        return state

    def _prepare_locked(self, now):
        self._reconcile_locked(); head=self.journal.snapshot(); state=head['state']; _counter(now)
        need(now>=state['logical_time'],'logical clock rollback')
        if state['pending'] is not None:
            return self._read_decision(state['pending']['decision_ref'])
        # Fence automatic CLOCK/control/admission, preserving pending-first.
        if 'native_acquisition' in state and state['native_acquisition']['phase'] in acquisition.STOP:
            return None
        if state['config'].get('native_consumer_profile',{}).get('schema') in consumer.REFRESH_PROFILES:
            root,_=self._consumer(state)
            if root['current'] is None and now>23:return None
        w=self._workflow(state)
        if self._unknown_local(w):return None
        if now>state['logical_time']:
            self.journal.append('CLOCK',{'now':now}); head=self.journal.snapshot(); state=head['state']
        if 'native_consumer_ref' in state:
            root,_=self._consumer(state)
            if root['current'] is not None:
                task=self._read(root['current']['task_ref'],consumer.TASK_KIND)
                if state['config']['native_consumer_profile']['schema'] in consumer.REFRESH_PROFILES:
                    from . import compact_native_task_refresh as refresh
                    phase=refresh.timing(root,task,now)
                    if phase=='WAIT':return None
                    if phase=='CANCEL':
                        self._consumer_control_locked('NATIVE_TASK_CANCEL');return None
                if now>=task['deadline']:
                    self._consumer_control_locked('NATIVE_TASK_CANCEL');return None
                if not consumer.allowed(state,self._workflow(state)):return None
                decision=self._decision(state,head['seq'],head['event_head'])
                if decision is None:return None
                need(decision.get('native_consumer') is not None,'one READ task cannot dispatch another action')
                ref=self._put('compact_decision',decision,MAX_DECISION_BYTES)
                self.journal.append('REQUEST',{'decision_ref':ref},effect={'phase':'REQUEST'})
                return decision
        if 'continuation_ref' in state:
            t=self._continuation(state)['current']
            if t is not None and now>=t['deadline']:
                self._continuation_control_locked('CONTINUATION_EXPIRE',{});head=self.journal.snapshot();state=head['state']
        if w['typed_wait'] is not None and w['typed_wait']['status']=='WAITING' and self._wait_status(w,now)!='WAITING':
            self._workflow_control_locked('SETTLE_WAIT',{});head=self.journal.snapshot();state=head['state'];w=self._workflow(state)
        local=w['interaction']['pending']
        if local is not None and local['status']=='QUEUED' and now>=local['deadline']:
            self._workflow_control_locked('EXPIRE_LOCAL',{});head=self.journal.snapshot();state=head['state'];w=self._workflow(state)
        h=self._help_episode(w)
        if h is not None and h['phase']=='AWAITING_RESPONSE' and now>=h['deadline']:
            self._workflow_control_locked('EXPIRE_HELP',{});head=self.journal.snapshot();state=head['state'];w=self._workflow(state)
        if self._workflow_problem(state,w) is not None:
            self._workflow_control_locked('GOAL_BLOCK',{});return None
        if state['config']['autonomous_help'] is not None and self._help_proposal(state,w) is not None:
            self._workflow_control_locked('QUEUE_HELP',{});head=self.journal.snapshot();state=head['state'];w=self._workflow(state)
        if procedures.available(w,now)<1:
            marker=dict(logical_time=now,reason='ENGINEERING_CREDITS_DEPLETED',resume_at=w['regulation']['last_time']+1)
            if w['last_wait']!=marker:self._workflow_control_locked('REGULATOR_WAIT',{})
            return None
        if w['goal'] is not None and w['goal']['status']=='WAITING':
            self._workflow_control_locked('REGULATOR_RESUME',{});head=self.journal.snapshot();state=head['state'];w=self._workflow(state)
        if not ('native_acquisition' in state and state['native_acquisition']['phase']=='READY') and state['semantic_queue'] and not self._protect_help_goal(w) and (w['goal'] is None or w['goal']['status'] in ('SATISFIED','BLOCKED','CANCELLED')):
            ctx=procedures.context(self._read(state['semantics_ref'],'compact_semantics'),state['semantic_queue'][0]['topic'],state['goals'])
            if ctx is not None and (w['goal'] is None or ctx['episode_key']!=w['goal']['episode_key']):
                self._workflow_control_locked('GOAL_BEGIN',{'topic':ctx['topic']});head=self.journal.snapshot();state=head['state']
        decision=self._decision(state,head['seq'],head['event_head'])
        if decision is None:return None
        if decision.get('schema')=='compact-local-resource-hold-1':
            if w['last_wait']!=decision['marker']:self._workflow_control_locked('LOCAL_RESOURCE_WAIT',{})
            return None
        local='local_request_ref' in decision
        ref=self._put('compact_local_decision' if local else 'compact_decision',decision,MAX_LOCAL_DECISION_BYTES if local else MAX_DECISION_BYTES)
        self.journal.append('REQUEST',{'decision_ref':ref},effect={'phase':'REQUEST'})
        return decision

    def prepare(self, now):
        with self._locked():return self._prepare_locked(now)

    def _local_workflow(self,state,decision,outcome,outcome_ref,seq):
        need('local_request_ref' in decision,'local effect decision')
        q=interaction.validate_local_request(self._read(decision['local_request_ref'],'compact_local_request'),state['config']['local_endpoints'])
        need(q==decision['request'],'exact local request object')
        w=self._workflow(state);local=interaction.mark_dispatched(w['interaction'],q['request_id'])
        current=self._local_goal_binding(w)
        w['interaction'],report=interaction.apply_outcome(local,q,outcome,decision['local_request_ref'],outcome_ref,state['logical_time'],current_goal=current)
        w,report=self._help_response(state,w,q,outcome,outcome_ref,report)
        credits=procedures.available(w,state['logical_time']);need(credits>=2,'local charged cost')
        w['regulation'].update(credits=credits-2,last_time=state['logical_time'],
            source=dict(seq=seq,kind='LOCAL_OUTCOME',outcome_sha256=_hash(outcome),outcome_ref=outcome_ref,decision_sha256=_hash(decision)))
        w['trace']=[]
        return procedures.validate_workflow(w),report

    def _capture_local_locked(self,outcome,lease):
        state=self.journal.snapshot()['state'];pending=state['pending']
        need(pending is not None and pending['phase']=='UNKNOWN','local capture needs START')
        decision=self._read_decision(pending['decision_ref'])
        outcome=interaction.validate_outcome(outcome,decision['request'])
        outcome_ref=self._put('compact_local_outcome',outcome,16384,lease=lease)
        workflow,_=self._local_workflow(state,decision,outcome,outcome_ref,self.journal.snapshot()['seq']+1)
        workflow_ref=self._put('compact_workflow',workflow,procedures.MAX_WORKFLOW_BYTES,lease=lease)
        pointer=dict(schema=SCHEMA,identity=self.identity,instance_id=self.instance_id,kind='LOCAL_OUTCOME',
                     effect_id=pending['effect_id'],outcome_ref=outcome_ref,workflow_ref=workflow_ref)
        atomic_json(self.capture_path,{'data':pointer,'sha256':_hash(pointer)});LifeJournal._sync(self.directory)
        self._reconcile_locked();return copy.deepcopy(outcome)

    def _capture_locked(self, item, lease):
        state=self.journal.snapshot()['state']; pending=state['pending']
        need(pending is not None and pending['phase']=='UNKNOWN','capture requires started effect')
        decision=self._read_decision(pending['decision_ref']); observer.validate_capture(item)
        need(item['request']==decision['request'] and item['capture']['capture_origin']==state['config']['capture_origin'],'capture request/origin')
        path=next((m['path'] for m in decision['candidate_metadata'] if m['entry_ref']==item['request']['entry_ref']),None)
        prior=state['latest_by_path'].get(path)
        first=item['request']['operation']=='READ_TEXT' and (prior is None or prior['successful_reads']==0)
        field=advance_field(self._read(state['field_ref'],'compact_field'),item,perception_index=state['perceptions']+1,first_seen=first)
        capture_ref=self._put('compact_capture',item,MAX_CAPTURE_BYTES,lease=lease)
        field_ref=self._put('compact_field',field,MAX_FIELD_BYTES,lease=lease)
        previous_semantics=self._read(state['semantics_ref'],'compact_semantics')
        next_seq=self.journal.snapshot()['seq']+1
        next_semantics,_,queue=self._semantic_capture(state,item,capture_ref,path,next_seq,allow_investigation=decision.get('native_consumer') is None)
        if state['config'].get('investigation_queue_profile') is None:
            # Preserve legacy publication order, including orphan accounting.
            semantic_ref=state['semantics_ref'] if next_semantics==previous_semantics else self._put('compact_semantics',next_semantics,semantics.MAX_INDEX_BYTES,lease=lease)
            workflow=self._next_workflow(state,decision,item,capture_ref,next_seq,next_semantics,queue)
        else:
            workflow=self._next_workflow(state,decision,item,capture_ref,next_seq,next_semantics,queue)
            next_semantics,queue,workflow=self._deferred_receipt(state,decision,item,next_semantics,queue,workflow)
            semantic_ref=state['semantics_ref'] if next_semantics==previous_semantics else self._put('compact_semantics',next_semantics,semantics.MAX_INDEX_BYTES,lease=lease)
        workflow_ref=self._put('compact_workflow',workflow,procedures.MAX_WORKFLOW_BYTES,lease=lease)
        pointer=dict(schema=SCHEMA,identity=self.identity,instance_id=self.instance_id,effect_id=pending['effect_id'],capture_ref=capture_ref,field_ref=field_ref,semantics_ref=semantic_ref,workflow_ref=workflow_ref)
        if 'derived_observation_ref' in state:
            receipt_head=self.journal.snapshot()
            record=derived.make_record(state,decision,item,capture_ref,workflow_ref,next_seq,receipt_head['event_head'],self._read)
            pointer['derived_observation_ref']=(state['derived_observation_ref'] if record is None else
                self._put(derived.KIND,record,derived.MAX_BYTES,lease=lease))
        if 'native_consumer_ref' in state:
            pointer['native_consumer_ref']=state['native_consumer_ref']
            if decision.get('native_consumer') is not None:
                root,policy=self._consumer(state);task=self._read(root['current']['task_ref'],consumer.TASK_KIND)
                nxt,newpolicy=consumer.receive(state,root,policy,state['config']['native_consumer_profile'],task,decision,item,capture_ref,workflow_ref,next_seq,self._read)
                if newpolicy is not None:need(self._put(consumer.POLICY_KIND,newpolicy,consumer.POLICY_BYTES,lease=lease)==nxt['policy_ref'],'native policy publication')
                pointer['native_consumer_ref']=self._put(consumer.ROOT_KIND,nxt,consumer.ROOT_BYTES,lease=lease)
        if state['config']['renewal_profile'] is not None:
            previous=self._cohorts(state);registry=self._next_cohorts(state,path,item,capture_ref,next_seq)
            pointer['cohorts_ref']=state['cohorts_ref'] if registry==previous else self._put('compact_cohorts',registry,MAX_COHORT_BYTES,lease=lease)
        if 'choice_profile' in state['config']:
            pointer['choice_ref']=self._put('compact_choice_state',self._next_choice(state,decision,item,capture_ref,path),choice_learning.MAX_BYTES,lease=lease)
        if 'continuation_ref' in state:
            pointer['continuation_ref']=self._put('compact_receipt_continuation',self._next_continuation(state,decision,item,capture_ref,path),continuation.MAX_BYTES,lease=lease)
        atomic_json(self.capture_path,{'data':pointer,'sha256':_hash(pointer)});LifeJournal._sync(self.directory)
        self._reconcile_locked();return copy.deepcopy(item)

    def _execute_locked(self, callback):
        head=self.journal.snapshot();state=head['state']; pending=state['pending']
        need(pending is not None and pending['phase']=='REQUESTED','UNKNOWN/no request: no callback retry')
        remaining=(journal_limits.MAX_SEGMENTS-head['segment_count'])*journal_limits.SEGMENT_RECORDS + journal_limits.SEGMENT_RECORDS-len(head['active'])
        need(remaining>=2,'journal capacity must admit START and RECEIPT before effect')
        self._check_effect_capacity(state)
        decision=self._read_decision(pending['decision_ref'])
        max_objects,max_bytes=self._publication_profile(state,decision)
        with self.objects.publication_lease(max_objects=max_objects,max_reserved_bytes=max_bytes) as lease:
            admission=dict(max_objects=max_objects,max_reserved_bytes=max_bytes,accounting=copy.deepcopy(self.objects.accounting))
            self.journal.append('START',{'publication_admission':admission},effect={'phase':'START','id':pending['effect_id']})
            result=callback(copy.deepcopy(decision['request']))
            return self._capture_local_locked(result,lease) if 'local_request_ref' in decision else self._capture_locked(result,lease)

    def execute(self, callback):
        need(callable(callback),'explicit synthetic callback')
        with self._locked():return self._execute_locked(callback)

    def step(self, now, callback):
        need(callable(callback),'explicit synthetic callback')
        with self._locked():
            if self._prepare_locked(now) is None:return False
            self._execute_locked(callback);return True

    def _reconcile_locked(self):
        if not self.capture_path.exists():return False
        pointer=LifeJournal._read(self.capture_path)
        if type(pointer) is dict and pointer.get('kind')=='LOCAL_OUTCOME':
            need(set(pointer)=={'schema','identity','instance_id','kind','effect_id','outcome_ref','workflow_ref'}
                 and pointer['schema']==SCHEMA and pointer['identity']==self.identity and pointer['instance_id']==self.instance_id,'local pointer binding')
            state=self.journal.snapshot()['state'];pending=state['pending'];last=state['last_local_receipt']
            if pending is None or pending['effect_id']!=pointer['effect_id']:
                need(last is not None and all(pointer[k]==last[k] for k in ('effect_id','outcome_ref','workflow_ref')),'unbound local pointer')
                return False
            need(pending['phase']=='UNKNOWN','local pointer requires START')
            self.journal.append('LOCAL_RECEIPT',{'outcome_ref':pointer['outcome_ref'],'workflow_ref':pointer['workflow_ref']},
                perception_count=state['perceptions'],effect={'phase':'RECEIPT','id':pending['effect_id']})
            return True
        state=self.journal.snapshot()['state']; pending=state['pending']; last=state['last_receipt']
        extra=({'cohorts_ref'} if state['config']['renewal_profile'] is not None else set())|({'choice_ref'} if 'choice_profile' in state['config'] else set())
        if 'derived_observation_ref' in state:extra|={'derived_observation_ref'}
        if 'native_consumer_ref' in state:extra|={'native_consumer_ref'}
        if 'continuation_ref' in state and type(pointer) is dict and ((pending is not None and pending['effect_id']==pointer.get('effect_id')) or (last is not None and 'continuation_ref' in last)):extra|={'continuation_ref'}
        need(type(pointer) is dict and set(pointer)==({'schema','identity','instance_id','effect_id','capture_ref','field_ref','semantics_ref','workflow_ref'}|extra)
             and pointer['schema']==SCHEMA and pointer['identity']==self.identity and pointer['instance_id']==self.instance_id,'capture pointer binding')
        if pending is None or pending['effect_id']!=pointer['effect_id']:
            need(last is not None and all(pointer[k]==last[k] for k in set(('effect_id','capture_ref','field_ref','semantics_ref','workflow_ref'))|extra),'unbound old capture pointer')
            return False
        need(pending['phase']=='UNKNOWN','capture pointer requires durable START')
        payload={k:pointer[k] for k in {'capture_ref','field_ref','semantics_ref','workflow_ref'}|extra}
        self.journal.append('RECEIPT',payload,
            perception_count=state['perceptions']+1,effect={'phase':'RECEIPT','id':pending['effect_id']})
        return True

    def reconcile(self):
        with self._locked():return self._reconcile_locked()

    def refresh_inventory(self, now):
        with self._locked():
            self._reconcile_locked(); state=self.journal.snapshot()['state'];_counter(now)
            need('native_acquisition' not in state,'acquisition research excludes external refresh')
            need(state['pending'] is None and now>=state['logical_time'],'idle monotonic inventory refresh')
            need(not self._unknown_local(self._workflow(state)),'unresolved local outcome blocks inventory refresh')
            # Reject before CLOCK as well as in replay: failed refresh is atomic.
            if 'choice_profile' in state['config']:need(self._choice(state)['current'] is None,'active choice binds current LIST generation')
            if 'continuation_ref' in state:need(self._continuation(state)['current'] is None,'active continuation binds LIST generation')
            if now>state['logical_time']:self.journal.append('CLOCK',{'now':now})
            self.journal.append('REFRESH',{})

    def snapshot(self):
        with self._locked():
            result=self.journal.snapshot()
            self.objects._refresh()
            return dict(journal=result,live_state_bytes=len(_encode(result['state'])),
                object_accounting=copy.deepcopy(self.objects.accounting),scope='SYNTHETIC_CONTINUATION_ARCHITECTURE_NOT_COMPLETE_LIFE',
                teacher_enabled=False,history_lookup='cold verified journal stream; never used for normal choice')

    def read_evidence(self, ref, kind):
        with self._locked():return self._read(ref,kind)

    def native_order_observation_snapshot(self):
        """Read-only addressed evidence; does not request an action or learn."""
        with self._locked():
            state=self.journal.snapshot()['state']
            return native_order_evidence.observe(state,self._workflow(state),self._read)

    def native_acquisition_snapshot(self):
        """Bounded addressed acquisition proof; no effects, history scan or learning."""
        with self._locked():
            state=self.journal.snapshot()['state']
            need('native_acquisition' in state,'explicit acquisition birth required')
            root=state['native_acquisition'];acquisition.verify_cause(state,root,self._read)
            return dict(root=copy.deepcopy(root),blocker=acquisition.blocker(state,self._workflow(state),root,self._read),
                pending=copy.deepcopy(state['pending']),authority='NONE',learning=False,
                lifetime_list_requests=int(root['list_request_ref'] is not None))

    def _derived_records(self,state):
        need(state['config'].get('derived_observation_profile')==derived.PROFILE,'explicit derived memory birth required')
        ref=state['derived_observation_ref']
        if ref is None:return []
        record=self._read(ref,derived.KIND)
        derived.validate(record,{k:state[k] for k in ('identity','instance_id','scope_ref')})
        return [copy.deepcopy(record)]

    def derived_observation_records(self):
        with self._locked():return self._derived_records(self.journal.snapshot()['state'])

    def derived_observation_snapshot(self):
        with self._locked():
            state=self.journal.snapshot()['state'];records=self._derived_records(state)
            return dict(records=records,record_count=len(records),reference=copy.deepcopy(state['derived_observation_ref']),
                acquisition_phase=state['native_acquisition']['phase'],
                status='COMMITTED_DERIVED_OBSERVATION' if records else 'NO_DERIVED_RECORD',
                nonrecord_reason=None if records else 'NO_SUPPORTED_COMPLETE_RELATION_COMMITTED',
                authority='NONE',learning=False,independent_experiences_added=0)

    def render_derived_observation(self):
        with self._locked():
            records=self._derived_records(self.journal.snapshot()['state'])
            return None if not records else derived.render(records[0])

    def read_event(self, seq):
        with self._locked():return self.journal.lookup(seq)
