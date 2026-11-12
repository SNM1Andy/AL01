"""Learned two-action spatial procedures with controlled, persisted fixture tests.

This is empirical pattern extraction, not general learning or teacher maturity.
Hashes check consistency, not authenticity against complete host rewriting.
"""
import copy
import math
from pathlib import Path

from .core import Individual, World, canonical, digest, fail
from .actions import validate_history
from .neuron_queries import _verified_events
from .multi_action_policy import DIRECTIONS, execute_action

SCHEMA = 'procedural-skill-1'
PRECONDITIONS = {'local_world': True, 'idle': True, 'no_visible_unknown_before_move': True,
                 'in_bounds': True, 'observe_only_visible_unknown': True}


def _require(condition, message):
    if not condition:
        fail('SKILL_INVALID', message)


def _pair(events):
    """Recognize exactly linked MOVE/result then OBSERVE/result, not suggestions."""
    if len(events) != 4 or [e['kind'] for e in events] != ['commit', 'action_result', 'commit', 'feedback']:
        return None
    move, result, observe, feedback = [e['payload'] for e in events]
    mr, obs = move.get('action_requested'), observe.get('action_requested')
    if not isinstance(mr, dict) or not isinstance(obs, dict):
        return None
    mp, op = mr['proposal'], obs['proposal']
    if mp['kind'] != 'move' or op['kind'] != 'observe':
        return None
    if (result.get('action_id') != mr['action_id'] or feedback.get('action_id') != obs['action_id']
            or feedback.get('target') != op['target'] or obs['position'] != result.get('position')):
        return None
    offset = [feedback['position'][i] - result['position'][i] for i in range(2)]
    # This minimum mechanism learns only moves which reveal a new nearby target.
    if sum(abs(x) for x in offset) > 1 or abs(mp['dx'] + offset[0]) + abs(mp['dy'] + offset[1]) <= 1:
        return None
    return [{'kind': 'move', 'dx': mp['dx'], 'dy': mp['dy']},
            {'kind': 'observe_relative_unknown', 'offset': offset}]


def extract_candidate(core_state):
    """Extract the latest successful pair from at most 128 recent real events."""
    _verified_events(core_state, ('read', 'neuron'))
    validate_history(core_state)
    events = core_state['events']
    for end in range(len(events), max(3, len(events) - 128), -1):
        pair = events[end - 4:end]
        pattern = _pair(pair)
        if pattern is None or pair[-1]['payload']['value'] <= 0:
            continue
        source = {'individual_id': core_state['id'], 'events': copy.deepcopy(pair)}
        return {'schema': SCHEMA, 'id': 'skill_' + digest({'pattern': pattern, 'source': source}),
                'version': 1, 'status': 'CANDIDATE_UNVALIDATED', 'pattern': pattern,
                'requires': ['move', 'observe'], 'preconditions': copy.deepcopy(PRECONDITIONS),
                'source': source, 'test': None}
    return None


def _bounded_json(value):
    nodes, size = 0, 0
    def visit(node, depth):
        nonlocal nodes, size
        nodes += 1
        _require(nodes <= 20000 and depth <= 32, 'skill record structural budget')
        if type(node) is dict:
            for key, child in node.items():
                _require(type(key) is str, 'JSON object key')
                visit(key, depth + 1)
                visit(child, depth + 1)
        elif type(node) is list:
            for child in node:
                visit(child, depth + 1)
        elif type(node) is str:
            _require(len(node) <= 16384, 'skill text budget')
            size += len(node.encode('utf8'))
            _require(size <= 256 * 1024, 'skill text total budget')
        elif type(node) is int:
            _require(abs(node) <= 2**53, 'skill integer budget')
        elif type(node) is float:
            _require(math.isfinite(node), 'finite skill number')
        else:
            _require(node is None or type(node) is bool, 'JSON skill values')
    visit(value, 0)


def _fixture(skill, positive):
    start = [3, 3] if skill['source']['events'][0]['payload']['action_requested']['position'] == [2, 2] else [2, 2]
    name = 'heldout_other' if skill['source']['events'][-1]['payload']['target'] == 'heldout_target' else 'heldout_target'
    move, observe = skill['pattern']
    target = [start[0] + move['dx'] + observe['offset'][0], start[1] + move['dy'] + observe['offset'][1]]
    world = {'version': 0, 'local': True, 'width': 7, 'height': 7,
             'objects': {name: {'position': target, 'value': 1 if positive else -1}}}
    return start, world


def validate_skill(skill, source_state=None):
    try:
        _bounded_json(skill)
        return _validate_skill(skill, source_state)
    except (KeyError, TypeError, ValueError, OverflowError, UnicodeError, RecursionError) as exc:
        fail('SKILL_INVALID', 'malformed skill record: ' + type(exc).__name__)


def _validate_skill(skill, source_state=None):
    _require(type(skill) is dict and set(skill) == {'schema', 'id', 'version', 'status', 'pattern',
             'requires', 'preconditions', 'source', 'test'}, 'record fields')
    _require(skill['schema'] == SCHEMA and skill['requires'] == ['move', 'observe']
             and canonical(skill['preconditions']) == canonical(PRECONDITIONS), 'schema/capabilities/preconditions')
    source = skill['source']
    _require(type(source) is dict and set(source) == {'individual_id', 'events'}, 'source fields')
    _require(type(source['individual_id']) is str and 1 <= len(source['individual_id']) <= 256, 'source identity')
    events = source['events']
    _require(type(events) is list and len(events) == 4, 'source window')
    previous = None
    for event in events:
        _require(type(event) is dict and set(event) == {'index', 'tick', 'kind', 'payload', 'previous', 'hash'}, 'event fields')
        _require(type(event['index']) is int and 0 <= event['index'] < 50000 and event['tick'] == event['index'] + 1, 'event index')
        _require(event['hash'] == digest({k: v for k, v in event.items() if k != 'hash'}), 'source event hash')
        if previous is not None:
            _require(event['index'] == previous['index'] + 1 and event['previous'] == previous['hash'], 'source chain')
        previous = event
    # Validate request/result linkage in a normalized four-event history.
    validate_history({'events': events, 'requested_actions': []})
    pattern = _pair(events)
    _require(pattern is not None and canonical(skill['pattern']) == canonical(pattern) and events[-1]['payload']['value'] > 0, 'successful pattern source')
    _require(skill['id'] == 'skill_' + digest({'pattern': pattern, 'source': source}), 'skill identity')
    if source_state is not None:
        _verified_events(source_state, ('read', 'neuron'))
        _require(source_state['id'] == source['individual_id'], 'source state identity')
        for event in events:
            _require(event['index'] < len(source_state['events']) and source_state['events'][event['index']] == event, 'source binding')
    if skill['status'] == 'CANDIDATE_UNVALIDATED':
        _require(type(skill['version']) is int and skill['version'] == 1 and skill['test'] is None, 'candidate test absent')
    else:
        _require(skill['status'] in ('TESTED_PASS', 'TESTED_FAIL') and type(skill['version']) is int and skill['version'] == 2, 'tested version/status')
        test = skill['test']
        _require(type(test) is dict and set(test) == {'scope', 'snapshot', 'receipts', 'checkpoint_sha256', 'journal_sha256'}, 'test fields')
        _require(test['scope'] == 'CONTROLLED_HELDOUT_FIXTURE_NOT_GENERALIZATION', 'test scope')
        snapshot = test['snapshot']
        from .contracts import validate_snapshot
        validate_snapshot(snapshot)
        state = snapshot['state']
        _verified_events(state, ('read', 'neuron'))
        validate_history(state)
        _require(state['id'] != source['individual_id'] and len(state['events']) == 4, 'independent four-tick test')
        _require(_pair(state['events']) == pattern, 'tested pattern differs')
        _require(test['checkpoint_sha256'] == digest(snapshot) and test['journal_sha256'] == digest(test['receipts']), 'test hashes')
        receipts = test['receipts']
        _require(type(receipts) is dict and len(receipts) == 2, 'two actual receipts required')
        from .actions import journal_payload
        for commit, result in ((state['events'][0], state['events'][1]), (state['events'][2], state['events'][3])):
            request = commit['payload']['action_requested']
            receipt = receipts.get(request['action_id'])
            expected_result = result['payload']['position'] if result['kind'] == 'action_result' else {k: v for k, v in result['payload'].items() if k not in ('action_id', 'goal_state')}
            _require(isinstance(receipt, dict) and receipt.get('status') == 'SUCCEEDED'
                     and receipt.get('payload') == journal_payload(request) and receipt.get('result') == expected_result, 'test receipt binding')
            _require(set(receipt) == {'action_id', 'dedup_key', 'operation', 'payload', 'status', 'result', 'error'}
                     and receipt['action_id'] == request['action_id'] and receipt['dedup_key'] == request['dedup_key']
                     and receipt['operation'] == 'sandbox_' + request['proposal']['kind'] and receipt['error'] is None,
                     'successful receipt operation/identity')
        passed = state['events'][-1]['payload']['value'] > 0
        start, expected_world = _fixture(skill, passed)
        _require(snapshot['world'] == expected_world and state['events'][0]['payload']['action_requested']['position'] == start,
                 'controlled heldout fixture context')
        _require(state['id'] == 'heldout_' + skill['id'], 'heldout trace provenance')
        _require(skill['status'] == ('TESTED_PASS' if passed else 'TESTED_FAIL'), 'test outcome mismatch')
    return True


def _select(skill, individual, step_index):
    _require(type(step_index) is int and step_index in (0, 1), 'step index')
    state, world = individual.state, individual.world
    if not {'move', 'observe'} <= set(individual.vm.capabilities):
        return None
    if not world.local or state['lifecycle'] != 'ACTIVE' or state['goal_state']['status'] == 'BLOCKED' or state['requested_actions'] or state['pending']:
        return None
    visible = world.visible(state['position'])
    move = skill['pattern'][0]
    if step_index == 0:
        if any(name not in state['observed'] for name in visible):
            return None
        target = [state['position'][0] + move['dx'], state['position'][1] + move['dy']]
        if not (0 <= target[0] < world.width and 0 <= target[1] < world.height):
            return None
        # Check the complete learned relative geometry before starting. This
        # uses public grid bounds only, never hidden target existence or value.
        offset = skill['pattern'][1]['offset']
        future_observation = [target[i] + offset[i] for i in range(2)]
        if not (0 <= future_observation[0] < world.width and 0 <= future_observation[1] < world.height):
            return None
        name = next(name for name, delta in DIRECTIONS.items() if delta == (move['dx'], move['dy']))
        action = copy.deepcopy(move)
    else:
        if len(state['events']) < 2:
            return None
        commit, result = state['events'][-2:]
        request = commit['payload'].get('action_requested')
        if (commit['kind'] != 'commit' or result['kind'] != 'action_result' or request is None
                or request['proposal'] != move or result['payload'].get('action_id') != request['action_id']):
            return None
        offset = skill['pattern'][1]['offset']
        target = [state['position'][i] + offset[i] for i in range(2)]
        matches = sorted(name for name, row in visible.items() if row['position'] == target and name not in state['observed'])
        if not matches:
            return None
        name = matches[0]
        action = {'kind': 'observe', 'target': name}
    return {'skill_id': skill['id'], 'step_index': step_index, 'candidate_id': name,
            'action': action, 'position': target, 'authority': 'NONE_CVM_REQUIRED'}


def select_skill_action(skill, individual, step_index=0):
    validate_skill(skill)
    if skill['status'] != 'TESTED_PASS':
        return None
    return _select(skill, individual, step_index)


def execute_skill_action(skill, individual, step_index=0):
    selection = select_skill_action(skill, individual, step_index)
    if selection is None:
        fail('SKILL_PRECONDITION', 'tested procedure unavailable; no action committed')
    return execute_action(individual, selection['candidate_id'], {selection['candidate_id']: selection['action']})


def test_candidate(skill, directory, *, positive=True):
    """Run two real actions with persisted requests, receipts, results and reloads.

    Fixture is deliberately controlled and independent of training IDs/position.
    Positive/negative labels set fixture feedback, never a claimed test result.
    """
    validate_skill(skill)
    _require(skill['status'] == 'CANDIDATE_UNVALIDATED' and type(positive) is bool, 'untested candidate and bool fixture polarity required')
    from .durable import DurableRunner, WorldDispatcher
    from .dispatch import DispatchJournal
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=False)
    start, context = _fixture(skill, positive)
    world = World(0, local=True)
    world.width = world.height = 7
    world.objects = context['objects']
    individual = Individual(world, individual_id='heldout_' + skill['id'])
    individual.state['position'] = start
    individual.state['visited'] = [list(start)]
    from .goals import initial
    individual.state['goal_state'] = initial(individual.state, world)
    runner = DurableRunner(path, individual)
    for index in (0, 1):
        with runner._exclusive():
            runner.individual = Individual.load(runner.checkpoint)
            runner._check_association()
            selected = _select(skill, runner.individual, index)
            _require(selected is not None, 'controlled fixture precondition')
            execute_action(runner.individual, selected['candidate_id'], {selected['candidate_id']: selected['action']})
            dispatcher = WorldDispatcher(runner.individual.world, DispatchJournal(runner.journal_path))
            dispatcher.prepare_request(runner.individual.state['requested_actions'][0])
            runner.individual.save(runner.checkpoint)
        runner = DurableRunner(path)
        _require(runner.step(), 'heldout result missing')
        runner = DurableRunner(path)
    snapshot = runner.individual.snapshot()
    receipts = DispatchJournal(runner.journal_path).records
    result = copy.deepcopy(skill)
    result.update(version=2, status='TESTED_PASS' if snapshot['state']['events'][-1]['payload']['value'] > 0 else 'TESTED_FAIL',
                  test={'scope': 'CONTROLLED_HELDOUT_FIXTURE_NOT_GENERALIZATION', 'snapshot': snapshot,
                        'receipts': copy.deepcopy(receipts), 'checkpoint_sha256': digest(snapshot), 'journal_sha256': digest(receipts)})
    validate_skill(result)
    return result
