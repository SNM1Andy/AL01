"""Bounded visible-only OBSERVE/MOVE heuristic; no planner or learned policy.

Observe-first determines eligibility. CGL/U0 may rank eligible alternatives, not
the action family. Grid bounds are public; hidden object records are never read.
Execution only prepares and commits a normal CL proposal, never dispatches it.
"""
import copy
import re

from .core import fail
from .coupling import visible_candidates

MAX_GRID_CELLS = 4096
MAX_CANDIDATES = 16
DIRECTIONS = {'move_east': (1, 0), 'move_west': (-1, 0),
              'move_north': (0, -1), 'move_south': (0, 1)}


def _position(value, width, height):
    if (type(value) is not list or len(value) != 2 or
            any(type(x) is not int for x in value) or
            not (0 <= value[0] < width and 0 <= value[1] < height)):
        fail('POLICY_INVALID', 'position outside public grid')
    return tuple(value)


def _empty(reason):
    return {'policy': 'VISIBLE_LOCAL_HEURISTIC', 'candidates': [], 'actions': {},
            'positions': {}, 'stop_reason': reason}


def _movement_guard(events, width, height):
    """Guard uses committed movement results since last observation feedback."""
    cells = width * height
    trajectory = []
    # At most one commit and one result per move; extra events cannot permit an
    # unbounded scan. A saturated lookback conservatively stops the policy.
    examined = 0
    for event in reversed(events):
        examined += 1
        if examined > 4 * cells + 2:
            return 'MOVEMENT_LOOKBACK_EXHAUSTED'
        if event['kind'] == 'feedback':
            break
        payload = event['payload']
        if event['kind'] == 'action_result' and payload.get('kind') == 'movement':
            trajectory.append(_position(payload.get('position'), width, height))
            if len(trajectory) >= 2 * cells:
                return 'MOVEMENT_BUDGET_EXHAUSTED'
    if len(trajectory) >= 2 and trajectory[0] == trajectory[1]:
        return 'NO_MOVEMENT_PROGRESS'
    if len(trajectory) >= 6 and trajectory[:6] == trajectory[:2] * 3:
        return 'MOVEMENT_OSCILLATION'
    return None


def action_candidates(individual, previous_target=None):
    """Pure detached candidate/action bundle for the existing rank_candidates API.

    Unknown currently visible observations take priority. Once none remain, a
    local-world unit step approaches the nearest unvisited public grid cell.
    No destination is an inferred hidden-object location. Ties use (distance,x,y).
    """
    if previous_target is not None and type(previous_target) is not str:
        fail('POLICY_INVALID', 'previous action candidate ID')
    state, world = individual.state, individual.world
    if state['lifecycle'] != 'ACTIVE' or state['goal_state']['status'] == 'BLOCKED':
        return _empty('LIFECYCLE_OR_GOAL_BLOCKED')
    if state['requested_actions'] or state['pending']:
        return _empty('ACTION_PENDING')
    width, height = world.width, world.height
    if type(width) is not int or type(height) is not int or width <= 0 or height <= 0:
        fail('POLICY_INVALID', 'public grid dimensions')
    if width * height > MAX_GRID_CELLS:
        return _empty('GRID_BUDGET_EXHAUSTED')
    current = _position(state['position'], width, height)
    visible = world.visible(list(current))
    if type(visible) is not dict or len(visible) > MAX_GRID_CELLS:
        fail('POLICY_INVALID', 'bounded visible perception')
    for name, record in visible.items():
        if type(name) is not str or name in DIRECTIONS or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,255}', name):
            fail('POLICY_INVALID', 'visible entity identity')
        if type(record) is not dict or set(record) != {'position'}:
            fail('POLICY_INVALID', 'perception must not include hidden values')
        _position(record['position'], width, height)
    # Preserve v0.17 visible-only sampling and direction-bin coefficients.
    # Distinct object identities can share a spatial direction; treating every
    # new identity as switching would erase the existing T-axis mechanism.
    observations = visible_candidates(individual, previous_target)
    bundle = _empty(None)

    def add(name, action, position, novelty):
        bundle['candidates'].append({'entity_ref': name, 'baseline_score': .02 * novelty,
            'novelty': float(novelty), 'persistence': float(previous_target == name),
            'switching': float(previous_target is not None and previous_target != name)})
        bundle['actions'][name] = action
        bundle['positions'][name] = list(position)

    for candidate in observations:
        name = candidate['entity_ref']
        bundle['candidates'].append(candidate)
        bundle['actions'][name] = {'kind': 'observe', 'target': name}
        bundle['positions'][name] = list(visible[name]['position'])
    if observations:
        return copy.deepcopy(bundle)
    if not world.local:
        return _empty('VISIBLE_ENVIRONMENT_EXHAUSTED')
    reason = _movement_guard(state['events'], width, height)
    if reason:
        return _empty(reason)
    if type(state['visited']) is not list or len(state['visited']) > MAX_GRID_CELLS:
        fail('POLICY_INVALID', 'bounded visited coordinates')
    visited = {_position(p, width, height) for p in state['visited']}
    if current not in visited:
        fail('POLICY_INVALID', 'current position missing from visited record')
    x, y = current
    frontier = [(abs(cx - x) + abs(cy - y), cx, cy)
                for cy in range(height) for cx in range(width) if (cx, cy) not in visited]
    if not frontier:
        return _empty('LOCAL_GRID_EXHAUSTED')
    _, cx, cy = min(frontier)
    dx = (cx > x) - (cx < x)
    dy = 0 if dx else (cy > y) - (cy < y)
    name = next(name for name, delta in DIRECTIONS.items() if delta == (dx, dy))
    add(name, {'kind': 'move', 'dx': dx, 'dy': dy}, [x + dx, y + dy], float((x + dx, y + dy) not in visited))
    return copy.deepcopy(bundle)


def execute_action(individual, candidate_id, actions):
    """Commit a typed proposal only; ordinary runtime owns dispatch and results."""
    if type(candidate_id) is not str or type(actions) is not dict or candidate_id not in actions:
        fail('POLICY_INVALID', 'selected action identity')
    state = individual.state
    if state['lifecycle'] != 'ACTIVE' or state['goal_state']['status'] == 'BLOCKED':
        fail('POLICY_BLOCKED', 'lifecycle or goal blocked')
    if state['requested_actions'] or state['pending']:
        fail('POLICY_BLOCKED', 'outstanding action must resolve first')
    action = actions[candidate_id]
    if type(action) is not dict:
        fail('POLICY_INVALID', 'selected action')
    if action.get('kind') == 'observe':
        if (set(action) != {'kind', 'target'} or action['target'] != candidate_id or
                candidate_id in DIRECTIONS or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,255}', candidate_id)):
            fail('POLICY_INVALID', 'observation identity')
        if candidate_id not in individual.world.visible(state['position']):
            fail('UNRESOLVED_REF', 'observation outside local perception')
        source = f'PROPOSE(OBSERVE(@{candidate_id}));'
    elif action.get('kind') == 'move':
        if (set(action) != {'kind', 'dx', 'dy'} or candidate_id not in DIRECTIONS or
                type(action['dx']) is not int or type(action['dy']) is not int or
                (action['dx'], action['dy']) != DIRECTIONS[candidate_id]):
            fail('POLICY_INVALID', 'unit movement identity')
        source = f"PROPOSE(MOVE(dx={action['dx']},dy={action['dy']}));"
    else:
        fail('POLICY_INVALID', 'unsupported action family')
    tx = individual.vm.prepare(source, state, individual.candidates())
    individual.commit(tx)
    return individual.state['events'][-1]['payload']['action_ids'][0]
