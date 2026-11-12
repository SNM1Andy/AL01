"""Opted-in native callback bridge for CompactRuntime, not another actor.

The existing runtime chooses every operation and commits START/receipts. This
adapter only executes an exact pending request, using FolderBroker and a fixed
literal-loopback HTTP endpoint. No prompts, goals, stages, retries, redirects,
proxy discovery, credentials, or alternative paths are supplied here.
Application checks are not an OS sandbox. Host authorization is still required.
"""
import copy
import http.client
import json
import os
import re
from pathlib import Path

from . import compact_interaction as interaction
from .folder_broker import FolderBroker
from .folder_contracts import validate_request
from .folder_runner import atomic_json
from .folder_session import _hash, _encode
from .life_journal import LifeJournal
from .controller_paths import check_file

MAX_STATE_BYTES = 65536
MAX_REPLY_BYTES = 16384


def need(ok, reason):
    if not ok:
        raise ValueError('COMPACT_NATIVE_INVALID: ' + reason)


def validate_binding(value):
    need(type(value) is dict and set(value) == {'root_binding', 'endpoints'}, 'binding fields')
    need(type(value['root_binding']) is str and re.fullmatch('[0-9a-f]{64}', value['root_binding']), 'root digest')
    entries = value['endpoints']
    need(type(entries) is list and len(entries) <= 4, 'bounded endpoints')
    targets = set()
    for e in entries:
        need(type(e) is dict and set(e) == {'target', 'port'}, 'endpoint fields')
        need(type(e['target']) is str and re.fullmatch('[a-z_][a-z0-9_]{0,127}', e['target']), 'endpoint token')
        need(e['target'] not in targets, 'distinct endpoints')
        need(type(e['port']) is int and 1 <= e['port'] <= 65535, 'literal loopback port')
        targets.add(e['target'])
    return copy.deepcopy(value)


def native_binding(root, endpoints):
    """Pure binding creation, no filesystem I/O or permission grant."""
    broker = FolderBroker(os.fspath(root))
    return validate_binding(dict(root_binding=broker.export_metadata()['root_binding'], endpoints=endpoints))


class CompactNativeBridge:
    def __init__(self, runtime, root, state_path, *, application_checks_acknowledged=False, timeout=1.0):
        need(application_checks_acknowledged is True, 'application checks acknowledgment required')
        need(type(timeout) in (int, float) and 0 < timeout <= 10, 'bounded timeout')
        self.runtime = runtime
        self.timeout = timeout
        state = runtime.snapshot()['journal']['state']
        self.binding = validate_binding(state['config'].get('native_binding'))
        self.broker = FolderBroker(os.fspath(root), application_checks_acknowledged=True, scope_ref=state['scope_ref'])
        need(self.broker.export_metadata()['root_binding'] == self.binding['root_binding'], 'frozen root binding')
        self.path = Path(os.path.abspath(state_path))
        root_path = Path(self.broker.root_path)
        # Private metadata must never become a monitored input or runtime file.
        for private in (self.path, runtime.directory):
            need(private != root_path and root_path not in private.parents and private not in root_path.parents, 'separate private/source scopes')
        need(self.path != runtime.directory and runtime.directory not in self.path.parents, 'sidecar outside runtime')
        check_file(self.path)
        self.namespace = dict(identity=runtime.identity, instance_id=runtime.instance_id,
                              scope_ref=state['scope_ref'], binding=self.binding)
        if self.path.exists():
            need(self.path.stat().st_size <= MAX_STATE_BYTES, 'sidecar byte bound')
            self.saved = LifeJournal._read(self.path)
            need(type(self.saved) is dict and set(self.saved) == {'schema', 'namespace', 'last_effect_id', 'broker'}, 'sidecar fields')
            need(self.saved['schema'] == 'compact-native-1' and self.saved['namespace'] == self.namespace, 'sidecar namespace')
            need(self.saved['last_effect_id'] is None or type(self.saved['last_effect_id']) is str, 'effect barrier')
            self.broker.restore_metadata(self.saved['broker'])
        else:
            # Lost metadata cannot silently create a replacement executor.
            need(state['governed_attempts'] == 0 and state['pending'] is None, 'missing sidecar after activity')
            self.saved = dict(schema='compact-native-1', namespace=self.namespace, last_effect_id=None,
                              broker=self.broker.export_metadata())
            self._save()

    def _save(self):
        self.saved['broker'] = self.broker.export_metadata()
        need(len(_encode(self.saved)) <= MAX_STATE_BYTES - 128, 'sidecar capacity')
        atomic_json(self.path, dict(data=self.saved, sha256=_hash(self.saved)))
        LifeJournal._sync(self.path.parent)

    def _port(self, request):
        interaction.validate_local_request(request, self.runtime.journal.snapshot()['state']['config']['local_endpoints'])
        return next(e['port'] for e in self.binding['endpoints'] if e['target'] == request['target'])

    def _http(self, port, method, path, body=None):
        # http.client has no automatic redirects, proxy lookup or request retry.
        conn = http.client.HTTPConnection('127.0.0.1', port, timeout=self.timeout)
        try:
            conn.request(method, path, body=body, headers={'Content-Type': 'application/json'})
            response = conn.getresponse()
            raw = response.read(MAX_REPLY_BYTES + 1)
            need(len(raw) <= MAX_REPLY_BYTES, 'reply byte bound')
            need(response.status == 200, 'endpoint status; no redirect/fallback')
            return json.loads(raw)
        finally:
            conn.close()

    @staticmethod
    def _unknown(request):
        return dict(schema=interaction.OUTCOME_SCHEMA, request_id=request['request_id'],
                    endpoint=request['target'], goal_generation=None if request['goal'] is None else request['goal']['generation'],
                    type=request['expected_reply_type'], status='UNKNOWN', response=None, reason='LOOPBACK_RESULT_UNCONFIRMED')

    def __call__(self, request):
        state = self.runtime.journal.snapshot()['state']
        pending = state['pending']
        need(pending is not None and pending['phase'] == 'UNKNOWN', 'committed START required')
        decision = self.runtime._read_decision(pending['decision_ref'])
        need(decision['request'] == request, 'exact pending request')
        local = 'local_request_ref' in decision
        if local:
            port = self._port(request)
        else:
            validate_request(request)
            need(request['scope_ref'] == self.namespace['scope_ref'] and request['operation'] in ('LIST_SCOPE', 'READ_TEXT'), 'ordinary fixed-scope observation')
        # Another adapter object may have been opened before this callback.
        # Consult the durable barrier, not its potentially stale Python cache.
        saved = LifeJournal._read(self.path)
        need(saved['namespace'] == self.namespace, 'sidecar namespace changed')
        need(saved['last_effect_id'] != pending['effect_id'], 'effect already attempted; no resend')
        self.broker.restore_metadata(saved['broker'])
        self.saved = saved
        # Persist before I/O. A failure after this barrier stays UNKNOWN.
        self.saved['last_effect_id'] = pending['effect_id']
        self._save()
        if local:
            try:
                return interaction.validate_outcome(self._http(port, 'POST', '/al01/help', _encode(request)), request)
            except (OSError, http.client.HTTPException, ValueError, UnicodeError):
                return self._unknown(request)
        result = self.broker.handle(request)
        self._save()  # Retain opaque broker references before runtime publication.
        capture = dict(schema='folder-session-1', capture_origin='APPLICATION_BROKER_CAPTURE', result=result)
        return dict(request=copy.deepcopy(request), capture=capture, receipt_sha256=_hash(capture))

    def check_pending(self):
        """Query the original endpoint by exact ID; never POST or pick a new route.

        False means unresolved. Correlated evidence goes through the runtime's
        existing admission, including duplicate/stale/deadline/goal checks.
        """
        state = self.runtime.snapshot()['journal']['state']
        pending = state['pending']
        if pending is not None:
            need(pending['phase'] == 'UNKNOWN', 'query requires uncertain effect')
            decision = self.runtime._read_decision(pending['decision_ref'])
            need('local_request_ref' in decision, 'folder UNKNOWN requires diagnosis')
            request = decision['request']
        else:
            local = self.runtime.workflow_snapshot()['interaction']['pending']
            if local is None or local['status'] not in ('UNKNOWN', 'DELIVERED'):
                return False
            request = self.runtime.read_evidence(local['request_ref'], 'compact_local_request')
        try:
            outcome = interaction.validate_outcome(self._http(self._port(request), 'GET',
                '/al01/outcomes/' + request['request_id']), request)
        except (OSError, http.client.HTTPException, ValueError, UnicodeError):
            return False
        self.runtime.admit_local_response(outcome)
        return True
