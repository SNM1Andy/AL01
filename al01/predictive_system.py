"""Fresh governed runtime plus durable pre-read numerical prediction.

One existing broker is wrapped, not replaced with a second reader. Captured
predictor receipts can repair a lagging folder ledger; uncertain attempts are
never retried. No prediction-to-policy benefit or maturity claim is made.
"""
import copy
import json
import os
from pathlib import Path
from contextlib import contextmanager
from .system_coordinator import SystemCoordinator
from .ordered_folder_session import OrderedFolderSession
from .folder_runner import atomic_json
from .folder_session import _hash
from .controller_paths import check_directory, check_file
from .durable_predictor import DurablePredictor
from .life_journal import LifeJournal

SCHEMA = 'predictive-system-1'


def need(ok, reason):
    if not ok: raise ValueError('PREDICTIVE_SYSTEM_INVALID: '+reason)


class _PredictionSession(OrderedFolderSession):
    capture_origin = 'APPLICATION_BROKER_CAPTURE'

    def __init__(self, directory):
        self._validating_birth = True
        try: super().__init__(directory)
        finally: self._validating_birth = False

    def _refresh(self, *, persist_reconciliation=True):
        # Existing receipt-leading metadata may be inspected during complete
        # bundle validation, but not published before other components pass.
        return super()._refresh(persist_reconciliation=
            persist_reconciliation and not self._validating_birth)

    def record_receipt(self, result, *, capture_origin=None):
        need(capture_origin is None or capture_origin == self.capture_origin, 'frozen receipt origin')
        return super().record_receipt(result, capture_origin=self.capture_origin)


class _BrokerObserver:
    def __init__(self, owner, broker):
        self.owner = owner; self.broker = broker

    def __getattr__(self, name): return getattr(self.broker, name)

    def handle(self, request):
        if request['operation'] != 'READ_TEXT': return self.broker.handle(request)
        owner = self.owner
        pending = owner.predictor.observer_state['pending']
        need(pending is not None and pending['request'] == request, 'durable exact prediction missing')
        need(pending['source']['cognition_sha256'] == _hash(owner.cognition), 'pre-read source changed')
        approved = owner.system_state['approved']
        need(approved is not None and approved['request'] == request, 'governance request differs')
        def callback(exact):
            need(exact == request, 'effect request differs from approved read')
            result = self.broker.handle(exact)
            capture = dict(schema='folder-session-1', capture_origin=owner.capture_origin, result=result)
            return dict(request=exact, capture=capture, receipt_sha256=_hash(capture))
        item = owner.predictor.execute(callback)
        return copy.deepcopy(item['capture']['result'])


class PredictiveSystem(SystemCoordinator):
    execution_owner = SCHEMA
    session_type = _PredictionSession

    @classmethod
    def fresh(cls, root, bundle_directory, **kwargs):
        return cls(root, bundle_directory, create=True, **kwargs)

    def __init__(self, root, bundle_directory, *, create=False, capture_origin=None, **kwargs):
        need(type(create) is bool, 'create Boolean')
        need(kwargs.get('application_checks_acknowledged') is True, 'application boundary acknowledgment required')
        need(capture_origin is None or capture_origin in ('SYNTHETIC_TEST_CAPTURE','APPLICATION_BROKER_CAPTURE'), 'capture origin')
        self.bundle = Path(os.path.abspath(bundle_directory)); check_directory(self.bundle)
        sensed = Path(os.path.abspath(root))
        need(sensed != self.bundle and sensed not in self.bundle.parents and self.bundle not in sensed.parents,
             'sensed root and entire private bundle must be separate scopes')
        self.manifest_path = self.bundle/'predictive-system.json'
        self.runtime_directory = self.bundle/'runtime'; self.predictor_directory = self.bundle/'predictor'
        self._predictive_initializing = True
        if create:
            need(not self.bundle.exists(), 'fresh bundle must not exist; no replacement/adoption')
            self.capture_origin = capture_origin or 'APPLICATION_BROKER_CAPTURE'
            self.bundle.mkdir(parents=True, exist_ok=False)
            manifest = None
        else:
            manifest = self._check_bundle()
            need(manifest['root'] == os.path.abspath(root), 'bundle root differs')
            need(capture_origin is None or capture_origin == manifest['capture_origin'], 'frozen capture origin')
            self.capture_origin = manifest['capture_origin']
            # Validate existing predictor data before base constructors can
            # reconcile receipt-leading runtime state. This never publishes an
            # observer receipt; the normal locked repair happens afterwards.
            self.predictor = DurablePredictor(self.predictor_directory, identity=manifest['identity'],
                scope_ref=manifest['scope_ref'], capture_origin=self.capture_origin, reconcile=False)
            need(manifest['predictor_instance_id'] == self.predictor.instance_id, 'bundle predictor instance binding')
        super().__init__(root, self.runtime_directory, **kwargs)
        self.session.capture_origin = self.capture_origin
        if manifest is None:
            self.predictor = DurablePredictor(self.predictor_directory, identity=self.actor['id'],
                                             scope_ref=self.actor['scope_ref'], capture_origin=self.capture_origin)
        if manifest is None:
            manifest = dict(schema=SCHEMA, root=str(self.root), identity=self.actor['id'],
                            scope_ref=self.actor['scope_ref'], capture_origin=self.capture_origin,
                            predictor_instance_id=self.predictor.instance_id)
            atomic_json(self.manifest_path, {'data':manifest,'sha256':_hash(manifest)})
            self.session._sync(self.bundle)
        need(manifest['identity'] == self.actor['id'] and manifest['scope_ref'] == self.actor['scope_ref']
             and manifest['predictor_instance_id'] == self.predictor.instance_id, 'bundle identity/instance binding')
        self._predictive_initializing = False
        self.broker = _BrokerObserver(self, self.broker)
        with self._joint_lock():
            self._repair_prediction_receipts()
            self._sync_system()

    def _check_bundle(self):
        check_directory(self.bundle)
        need(self.bundle.is_dir(), 'bundle missing; no implicit birth')
        with os.scandir(self.bundle) as entries:
            names = set()
            for index, entry in enumerate(entries):
                need(index < 3 and entry.name in {'runtime','predictor','predictive-system.json'}, 'bundle layout')
                names.add(entry.name)
        need(names == {'runtime','predictor','predictive-system.json'}, 'partial bundle birth; no replacement files')
        check_directory(self.runtime_directory); check_directory(self.predictor_directory)
        required = [self.runtime_directory/'actor.json', self.runtime_directory/'system-controller.json',
                    self.runtime_directory/'folder-joint-origin.json', self.runtime_directory/'folder-joint.json',
                    self.runtime_directory/'folder-agent-config.json', self.runtime_directory/'folder-agent-decisions.json',
                    self.runtime_directory/'session'/'session.json',
                    self.predictor_directory/'journal'/'birth.json', self.predictor_directory/'journal'/'head.json',
                    self.predictor_directory/'objects'/'objects.json',
                    self.manifest_path]
        for path in required:
            check_file(path); need(path.is_file(), 'bundle component missing; no reset')
        check_directory(self.predictor_directory/'objects')
        need((self.predictor_directory/'objects').is_dir(), 'predictor objects missing')
        manifest, _ = OrderedFolderSession._read(self.manifest_path, 65536)
        need(type(manifest) is dict and set(manifest) == {'schema','root','identity','scope_ref','capture_origin','predictor_instance_id'}
             and manifest['schema'] == SCHEMA, 'bundle manifest')
        actor_path = self.runtime_directory/'actor.json'
        need(actor_path.stat().st_size <= 262144, 'actor byte limit')
        actor = json.loads(actor_path.read_text(encoding='utf8'))
        need(actor.get('execution_owner') == SCHEMA and actor.get('id') == manifest['identity']
             and actor.get('scope_ref') == manifest['scope_ref'] and actor.get('root') == manifest['root'], 'bundle actor binding')
        if hasattr(self, 'predictor'):
            persisted = LifeJournal._read(self.predictor_directory/'journal'/'head.json')
            need(_hash(persisted) == self.predictor.journal.head_sha, 'stale predictor handle; reopen before reporting or acting')
        return manifest

    def _catch_up_locked(self):
        if self._predictive_initializing:
            # Constructor validation must not repair one ledger while another
            # component has not yet passed validation. Captures remain intact.
            self._load_joint()
            return False
        return super()._catch_up_locked()

    @contextmanager
    def _joint_lock(self):
        if not self._predictive_initializing: self._check_bundle()
        with super()._joint_lock():
            if not self._predictive_initializing: self._check_bundle()
            yield

    def step(self):
        self._check_bundle()
        return super().step()

    def _plan(self):
        request = super()._plan()  # Exact governance approval and prepared CL.
        if request is not None and request['operation'] == 'READ_TEXT':
            pending = self.predictor.observer_state['pending']
            if pending is None:
                self.predictor.prepare(request, self.cognition, logical_budget=2)
            else:
                need(pending['request'] == request and pending['source']['cognition_sha256'] == _hash(self.cognition),
                     'existing prediction/source differs')
        return request

    def _repair_prediction_receipts(self):
        """Only republishes a verified already captured item, never calls handle."""
        self.predictor.reconcile()
        observations = self.predictor.observer_state['observations']
        rows = {r['request_id']:r for r in self.session.list_requests()}
        for observation in observations:
            item = observation['item']; request = item['request']; rid = request['request_id']
            need(rid in rows and rows[rid]['request'] == request, 'prediction capture lacks committed folder request')
            source = self.predictor.source_cognition(rid)
            decisions = [d for d in self.agent_data['decisions'] if d['request'] == request]
            need(len(decisions) == 1 and decisions[0]['source_cognition_sha256'] == _hash(source), 'prediction/CL source mismatch')
            if rows[rid]['status'] == 'REQUESTED':
                approved = self.system_state['approved']
                need(approved is not None and approved['request'] == request, 'capture repair lacks exact governance approval')
                self.session.record_receipt(item['capture']['result'], capture_origin=item['capture']['capture_origin'])
            actual = self.session.read_capture(rid)['capture']
            need(actual == item['capture'], 'folder/predictor captured receipt differs')
        actor = json.loads(self.actor_path.read_text(encoding='utf8'))
        marker = actor.get('dispatch_started')
        if marker is not None:
            item = self.predictor.captured_item(marker['request_id'])
            if item is not None:
                need(marker['request_sha256'] == item['capture']['result']['request_sha256'], 'capture dispatch marker mismatch')
                self.session.consume(marker['request_id'])
                actor['dispatch_started'] = None; atomic_json(self.actor_path, actor); self.actor = actor
        for row in self.session.list_requests():
            if row['request']['operation'] == 'READ_TEXT' and row['status'] != 'REQUESTED':
                need(self.predictor.captured_item(row['request_id']) is not None, 'captured read lacks numerical observation')

    def _step_locked(self):
        self._check_bundle(); self._repair_prediction_receipts()
        result = super()._step_locked()
        self._repair_prediction_receipts()
        return result

    def reconcile(self):
        with self._joint_lock():
            before = self.joint_sha
            self._repair_prediction_receipts(); self._sync_system()
            return before != self.joint_sha

    def declare_study(self, plan):
        with self._joint_lock():
            self._repair_prediction_receipts(); self._sync_system()
            return self.predictor.declare_study(plan, self.cognition)

    def promote_model(self, report):
        with self._joint_lock():
            self._repair_prediction_receipts(); self._sync_system()
            return self.predictor.promote_model(report, self.cognition)

    def rollback_model(self, retention_captures):
        with self._joint_lock():
            self._repair_prediction_receipts(); self._sync_system()
            return self.predictor.rollback_model(retention_captures, self.cognition)

    def _report_locked(self):
        result = super()._report_locked()
        result.update(predictive_schema=SCHEMA, prediction=self.predictor.snapshot(),
                      prediction_to_policy_effect=False,
                      prediction_scope='durable measured-width predictions, not language understanding or maturity')
        return result

    def report(self):
        with self._joint_lock():
            self._load_joint(); self._load_policy(); self._load_system()
            result = self._report_locked()
            result.update(system_schema='system-coordinator-1', system=copy.deepcopy(self.system_state),
                system_event_sha256=self.system['events'][-1]['sha256'] if self.system['events'] else None,
                local_transport='NONE: queued controller values are not sent or delivered', teacher_enabled=False,
                prediction_logical_cost='2 reserved logical units; not registered-neuron or physical-time benchmark',
                system_scope='bounded governed read plus numerical observation, not full blueprint baseline')
            return result
