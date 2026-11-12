"""Versioned folder-capture -> numerical CGL forcing, entirely in memory.

Cells are hash encodings, NOT physical locations. Amplitude is a bounded byte/
entry novelty signal, NOT reward, emotion, pain, semantic understanding, or a
trained weight. Capture-origin labels are preserved, not independently certified.
"""
import copy
import hashlib
from dataclasses import asdict

from .core import canonical, digest
from .folder_contracts import validate_request, validate_result
from .instinct import EventDrive, DriveEvent, InstinctError

VERSION = 'folder-capture-cell-1'
TICK_DURATION = .05
MAX_CAPTURES = 128
MAX_LEDGER_BYTES = 16 * 1024 * 1024
ORIGINS = ('APPLICATION_BROKER_CAPTURE', 'SYNTHETIC_TEST_CAPTURE')


def _need(condition, message):
    if not condition:
        raise InstinctError(message)


def _safe_size(value):
    """Preflight bounded JSON without exponential expansion or recursive loops."""
    nodes = 0
    text_bytes = 0
    def visit(node, depth):
        nonlocal nodes, text_bytes
        nodes += 1
        _need(nodes <= 100000 and depth <= 16, 'folder ledger structural budget')
        if type(node) is dict:
            for key, child in node.items():
                _need(type(key) is str, 'folder ledger JSON keys')
                visit(key, depth + 1)
                visit(child, depth + 1)
        elif type(node) in (tuple, list):
            for child in node:
                visit(child, depth + 1)
        elif type(node) is str:
            _need(len(node) <= 65536, 'folder ledger string budget')
            text_bytes += len(node.encode('utf8'))
            _need(text_bytes <= MAX_LEDGER_BYTES, 'folder ledger text budget')
        elif type(node) is int:
            _need(abs(node) <= 2**53, 'folder ledger integer budget')
        else:
            _need(node is None or type(node) is bool, 'folder ledger JSON scalar type')
    visit(value, 0)


class FolderEventDrive(EventDrive):
    """Verified complete committed prefix only; no future capture preload.

    Payload is exactly request/result/capture_origin/source_hash. source_hash is
    the existing folder-session receipt-data hash. One event acts on the half-
    open interval [tick*.05,(tick+1)*.05). The caller's committed_tick must match
    the supplied prefix length; it is not an authentication proof.
    """
    version = VERSION

    def __init__(self, events=(), *, committed_tick, tick_duration=TICK_DURATION):
        _need(type(tick_duration) in (int, float) and tick_duration == TICK_DURATION,
              'folder drive requires fixed .05 clock')
        _need(type(events) in (list, tuple) and len(events) <= MAX_CAPTURES, 'folder capture count')
        _need(type(committed_tick) is int and 0 <= committed_tick <= MAX_CAPTURES
              and committed_tick == len(events), 'future/incomplete folder capture prefix')
        try:
            _safe_size(events)
            previous, seen_requests, seen_entries = '0' * 64, set(), set()
            records, provenance = [], []
            for index, event in enumerate(events):
                _need(type(event) is dict and set(event) == {'index', 'tick', 'kind', 'payload', 'previous', 'hash'}, 'folder event fields')
                _need(type(event['index']) is int and event['index'] == index
                      and type(event['tick']) is int and event['tick'] == index + 1
                      and event['kind'] == 'folder_capture' and event['previous'] == previous,
                      'folder event ordering')
                _need(event['hash'] == digest({k: v for k, v in event.items() if k != 'hash'}), 'folder event hash')
                payload = event['payload']
                _need(type(payload) is dict and set(payload) == {'request', 'result', 'capture_origin', 'source_hash'}, 'folder capture payload')
                request, result, origin = payload['request'], payload['result'], payload['capture_origin']
                validate_request(request)
                validate_result(result, request)
                _need(origin in ORIGINS, 'folder capture origin')
                _need(payload['source_hash'] == digest({'schema': 'folder-session-1', 'capture_origin': origin, 'result': result}), 'folder receipt source hash')
                _need(request['request_id'] not in seen_requests, 'duplicate folder capture request')
                seen_requests.add(request['request_id'])
                # Scope participates so an entry token in another root has a
                # separate encoding. Opaque hashes are not spatial coordinates.
                key = request['scope_ref'] + ':' + (request['entry_ref'] or 'scope')
                hashed = hashlib.sha256(('folder-cell-1:' + key).encode('utf8')).digest()
                amplitude, signal = 0., 'neutral_no_reward'
                if result['status'] == 'OK' and request['operation'] == 'READ_TEXT':
                    amplitude = .125 * min(1., result['payload']['utf8_bytes'] / 65536) + (.125 if key not in seen_entries else 0.)
                    signal = 'read_byte_fraction_and_first_entry_proxy'
                    seen_entries.add(key)
                elif result['status'] == 'OK' and request['operation'] == 'LIST_SCOPE':
                    amplitude = .1 * len(result['payload']['entries']) / 32
                    signal = 'listed_entry_fraction'
                record = DriveEvent((index + 1) * TICK_DURATION, (index + 2) * TICK_DURATION,
                                    index, event['hash'], hashed[0] % 64, hashed[1] % 64, amplitude)
                records.append(record)
                provenance.append({'event_index': index, 'event_hash': event['hash'], 'source_hash': payload['source_hash'],
                    'capture_origin': origin, 'request_id': request['request_id'], 'operation': request['operation'],
                    'status': result['status'], 'signal': signal})
                previous = event['hash']
            _need(len(canonical(events).encode('utf8')) <= MAX_LEDGER_BYTES, 'folder ledger encoded budget')
        except (KeyError, TypeError, ValueError, OverflowError, UnicodeError, RecursionError) as exc:
            if isinstance(exc, InstinctError):
                raise
            raise InstinctError('invalid folder capture drive') from exc
        self.records = tuple(records)
        self.tick_duration = TICK_DURATION
        self.committed_tick = committed_tick
        self.source_head = previous
        self._provenance = provenance
        self.digest = digest(self._manifest_data())

    def _manifest_data(self):
        return {'schema': VERSION, 'tick_duration': self.tick_duration, 'committed_tick': self.committed_tick,
                'source_head': self.source_head, 'encoding': 'opaque_hash_cells_not_physical_positions',
                'meaning': 'bounded_capture_statistics_not_reward_emotion_or_trained_weights',
                'records': [asdict(record) for record in self.records], 'provenance': copy.deepcopy(self._provenance)}

    def manifest(self):
        return dict(self._manifest_data(), sha256=self.digest)

    @classmethod
    def from_manifest(cls, manifest, events, *, committed_tick):
        drive = cls(events, committed_tick=committed_tick)
        _need(type(manifest) is dict and manifest == drive.manifest()
              and canonical(manifest) == canonical(drive.manifest()), 'folder drive manifest/replay mismatch')
        return drive

    def at(self, time):
        _need(type(time) in (int, float) and 0 <= time <= (self.committed_tick + 1) * TICK_DURATION + 1e-12,
              'folder drive cannot advance beyond captured causal horizon')
        return super().at(time)

    def boundaries(self, start, end):
        _need(type(start) in (int, float) and type(end) in (int, float)
              and 0 <= start <= end <= (self.committed_tick + 1) * TICK_DURATION + 1e-12,
              'folder integration interval exceeds captured causal horizon')
        return super().boundaries(start, end)
