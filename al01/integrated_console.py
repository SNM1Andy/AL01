"""Finite reference-run CLI and inert, read-only HTML observation export.

No server, background worker, network operation, or existing-individual migration.
"""
import argparse
import copy
import html
import json
from pathlib import Path

import numpy as np

from .core import CLError, Individual, World, fail
from .control_embryos import teacher_status
from .instinct import Geometry, ReferenceCGL, FrozenBias
from .integrated import IntegratedLife


# Classifications describe this executable slice, never whole-stage acceptance.
MODULES = [
    {'id': 'M01', 'name': 'Typed CL', 'classification': 'functional', 'scope': 'Bounded typed language.'},
    {'id': 'M02', 'name': 'CVM', 'classification': 'functional', 'scope': 'Validated transaction execution.'},
    {'id': 'M03', 'name': 'Registered neurons', 'classification': 'functional', 'scope': 'Registered recall and compare causally feed bounded U0 context/focus; ranking has no execution authority.'},
    {'id': 'M04', 'name': 'U0 cognition', 'classification': 'limited', 'scope': 'Persistent deterministic rule controller, not a trained hybrid.'},
    {'id': 'M05', 'name': 'Reference CGL', 'classification': 'functional', 'scope': 'Numerical reference solver; legacy equivalence unverified.'},
    {'id': 'M06', 'name': 'Engineering emotion and relationships', 'classification': 'limited', 'scope': 'Bounded engineering C/T/P; fixture-only K, not affection or social intelligence.'},
    {'id': 'M07', 'name': 'Memory', 'classification': 'limited', 'scope': 'Evidence-preserving semantic claims; extracted MOVE/OBSERVE candidates require independent controlled tests before use.'},
    {'id': 'M08', 'name': 'Experience adjustment', 'classification': 'functional', 'scope': 'Limited empirical spatial Q/count score adjustment; no held-out transfer.'},
    {'id': 'M09', 'name': 'Governance', 'classification': 'limited', 'scope': 'Persistent bounded urgency/aging/minimum-service scheduling controls eligibility; scoped conflicts retain UNKNOWN, sole CVM executor.'},
    {'id': 'M10', 'name': 'Environment and human intake', 'classification': 'functional', 'scope': 'OBSERVE/MOVE sandbox with local perception; human text remains inert UNVERIFIED data.'},
    {'id': 'M11', 'name': 'Teacher gate', 'classification': 'locked', 'scope': 'Four typed envelopes are inert; gate locked and network absent.'},
    {'id': 'M12', 'name': 'Observation interface', 'classification': 'functional', 'scope': 'Read-only exported snapshot, not a live service.'},
    {'id': 'M13', 'name': 'Persistence', 'classification': 'functional', 'scope': 'Atomic shared checkpoint and receipt recovery; no existing-individual migration.'},
    {'id': 'M14', 'name': 'Safety boundaries', 'classification': 'functional', 'scope': 'Capabilities, validation and finite budgets fail closed.'},
]


def create_reference(directory, objects=48, seed=3, local=False):
    """Create only at a new explicit path; never adopt or replace an individual."""
    if type(local) is not bool:fail('TYPE_ERROR','local perception flag')
    if type(objects) is not int or not 1 <= objects <= 256:
        fail('TYPE_ERROR', 'reference object count must be 1..256')
    if type(seed) is not int or not 0 <= seed < 2**32:
        fail('TYPE_ERROR', 'reference seed must be a uint32')
    path = Path(directory)
    # Exclusive mkdir also rejects an existing empty directory or symlink.
    path.mkdir(parents=True, exist_ok=False)
    y, x = np.indices((64, 64))
    geometry = Geometry(np.cos(2 * np.pi * x / 64) + np.sin(4 * np.pi * y / 64))
    calibration_solver = ReferenceCGL(geometry, seed=991, dt=.01)
    rows = [calibration_solver.step() for _ in range(64)]
    weights = ((.1, -.1, 0., 0., .1, .2), (0., .1, -.1, .1, 0., -.1),
               (-.1, 0., .1, 0., .2, .1))
    calibration = FrozenBias.calibrate(rows,
        calibration_id='reference-independent-zero-drive-seed991-64steps-unvalidated-policy',
        weights=weights, beta=.1)
    return IntegratedLife(path, individual=Individual(World(objects,local=local)),
                          solver=ReferenceCGL(geometry, seed=seed, dt=.01), calibration=calibration)


def step_bounded(life, steps):
    if type(steps) is not int or not 1 <= steps <= 128:
        fail('TYPE_ERROR', 'step count must be 1..128')
    completed = 0
    for _ in range(steps):
        if not life.step():
            break
        completed += 1
    return {'requested_steps': steps, 'completed_steps': completed,
            'stop_reason': 'batch_complete' if completed == steps else 'runtime_stopped_or_paused',
            'continuous_operation': False}


def observation_snapshot(life):
    """One consistent locked read; no checkpoint, field or cognitive-state write."""
    with life._exclusive():
        life._load()
        life._association()
        state, modules = life.individual.state, life.modules
        development = modules['development']
        result = {
            'scope': 'Fresh reference sandbox only; not a migrated or production individual.',
            'limitations': 'All-block initial embryo, not full P3-P6 acceptance, trained hybrid cognition, consciousness, or continuous operation.',
            'module_classifications': MODULES,
            'identity_and_audit': {'individual_id': state['id'], 'generation': life.generation,
                'bundle_sha256': life.sha, 'parent_bundle_sha256': life.parent_bundle,
                'tick': state['tick'], 'closed_loops': state['closed_loops'], 'phase': life.phase,
                'paused': modules['paused'], 'lifecycle': state['lifecycle'],
                'event_count': len(state['events']),
                'event_head': state['events'][-1]['hash'] if state['events'] else None,
                'control_audit': modules['control_audit']},
            'field': {'time': life.solver.time, 'features': life.solver.manifest()['features'],
                      'calibration_id': life.calibration.calibration_id,
                      'meaning': 'Numerical reference field; policy benefit unvalidated.'},
            'score_decompositions': modules['score_audit'],
            'ranking_traces': life.traces,
            'emotion_C_T_P': development['content']['axes'],
            'relationships_K': development['content']['relationships'],
            'memories': {name: development['content'][name]
                        for name in ('working', 'episodic', 'semantic', 'procedural', 'experience')},
            'development_version': development['version'],
            'semantic_claim_memory':modules['semantic_memory'],'procedural_skills':modules['skills'],
            'persistent_scheduler':modules['scheduler'],'compute_budget':modules['compute_budget'],
            'U0_control': modules['control'], 'governance': modules['governance'],
            'teacher': teacher_status(state, development),
            'human_statements': modules['human_statements'],
            'goal_state': state['goal_state'], 'module_switches': modules['switches'],
        }
        return copy.deepcopy(result)


def render_html(snapshot):
    """No scripts, remote assets, forms, or raw user HTML; all data is escaped."""
    sections = []
    for title, value in snapshot.items():
        text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)
        sections.append('<section><h2>' + html.escape(title.replace('_', ' ')) + '</h2><pre>'
                        + html.escape(text, quote=True) + '</pre></section>')
    return ('<!doctype html><html lang="en"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<meta http-equiv="Content-Security-Policy" content="default-src &#39;none&#39;; style-src &#39;unsafe-inline&#39;">'
            '<title>AL-01 integrated reference observer</title><style>'
            'body{font:16px system-ui;background:#111827;color:#e5e7eb;max-width:1100px;margin:2rem auto;padding:1rem}'
            'section{background:#1f2937;padding:1rem;margin:1rem 0;border-radius:12px}'
            'h1,h2{color:#93c5fd}h2{font-size:1.1rem}pre{white-space:pre-wrap;overflow-wrap:anywhere;font-size:.85rem}'
            '</style></head><body><h1>AL-01 all-module reference observer</h1>'
            '<p>Read-only exported snapshot. No live service or automatic refresh.</p>'
            + ''.join(sections) + '</body></html>')


def export_html(life, path):
    """Write a new observer artifact outside the runtime directory, never replace."""
    target = Path(path).resolve()
    if target == life.directory or life.directory in target.parents:
        fail('TYPE_ERROR', 'observer export must be outside the reference runtime directory')
    output = render_html(observation_snapshot(life))
    with target.open('x', encoding='utf8') as stream:
        stream.write(output)
    return {'export': str(target), 'read_only_snapshot': True}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True, help='Explicit reference runtime path; never a production path')
    commands = parser.add_subparsers(dest='command', required=True)
    create = commands.add_parser('create', help='Create a fresh reference at a nonexistent directory')
    create.add_argument('--objects', type=int, default=48)
    create.add_argument('--seed', type=int, default=3)
    create.add_argument('--local',action='store_true',help='Use local perception and bounded public-grid navigation')
    step = commands.add_parser('step', help='Run one finite batch, at most 128 steps')
    step.add_argument('--steps', type=int, required=True)
    for command in ('status', 'pause', 'resume', 'teacher-status'):
        commands.add_parser(command)
    export = commands.add_parser('export', help='Write a new escaped, inert HTML snapshot outside runtime directory')
    export.add_argument('--output', required=True)
    statement = commands.add_parser('statement', help='Record inert UNVERIFIED text, not a lesson')
    statement.add_argument('text')
    query=commands.add_parser('semantic-query');query.add_argument('entity')
    budget=commands.add_parser('budget');budget.add_argument('value',type=int)
    args = parser.parse_args(argv)
    try:
        if args.command == 'create':
            life = create_reference(args.directory, args.objects, args.seed, args.local)
            result = observation_snapshot(life)
        else:
            life = IntegratedLife(args.directory)
            if args.command == 'step': result = step_bounded(life, args.steps)
            elif args.command == 'status': result = observation_snapshot(life)
            elif args.command == 'export': result = export_html(life, args.output)
            elif args.command == 'statement': result = life.statement(args.text)
            elif args.command == 'semantic-query':result=life.semantic_query(args.entity)
            elif args.command == 'budget':life.set_compute_budget(args.value);result={'compute_budget':life.modules['compute_budget']}
            elif args.command == 'teacher-status': result = observation_snapshot(life)['teacher']
            else:
                getattr(life, args.command)()
                result = {'control': args.command, 'paused': life.modules['paused'], 'generation': life.generation}
        print(json.dumps(result, ensure_ascii=True, sort_keys=True, allow_nan=False))
        return 0
    except (CLError, OSError, ValueError, UnicodeError) as exc:
        print(json.dumps({'error': str(exc), 'command': args.command}, ensure_ascii=True))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
