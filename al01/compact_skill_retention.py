"""Frozen old-skill regression under exact width and choice-policy contexts.

These are fresh synthetic test individuals, not restored source individuals.
Closed replay establishes controller-local integrity, not hostile-host
provenance. Reused fixtures test pipeline retention, not new-task learning or
catastrophic-forgetting resistance of a general policy. Claims stay unverified.
Protocol 3 also binds a live source choice root and training namespace. The
source runtime must independently authenticate its admitted history before
freezing, importing, evaluating or activating; this helper cannot prove that
history from a self-consistent copied policy alone.
"""
import copy
from pathlib import Path

from . import compact_learning as learning
from . import compact_observer as observer
from . import compact_procedures as procedures
from . import compact_procedure_probe as legacy_probe
from .folder_session import _hash, _encode
from .life_journal import LifeJournal
from .life_objects import LifeObjects, _json

CASES = procedures.CASES + (('unknown', 173),)
CONDITIONS = ('incumbent', 'candidate')
MAX_PLAN_BYTES = 16384
MAX_CASE_BYTES = legacy_probe.MAX_CASE_BYTES
FLAGS = dict(field_enabled=True, u0_enabled=True, neuron_feedback_enabled=True,
             prediction_error_enabled=True, regulation_enabled=True)
PROTOCOL = dict(
    schema='compact-old-skill-retention-protocol-1',
    cases=[dict(case=case, seed=seed) for case, seed in CASES],
    fixture_version='fixed-old-conflict-bank-1',
    reused_fixture_protocol_sha256=_hash(procedures.TEST_PROTOCOL),
    criterion='Legal CL execution, honest refusal and UNKNOWN, source continuity; never claim truth',
    maximum_success_decline=0, maximum_failure_increase=0, maximum_action_cost_increase=0,
    require_incumbent_pass=True, require_candidate_pass=True,
    max_effects_per_condition=6, max_logical_time=2,
    max_events=procedures.MAX_CAPSULE_EVENTS, max_objects=procedures.MAX_CAPSULE_OBJECTS,
    max_capsule_bytes=procedures.MAX_CAPSULE_BYTES,
    interpretation='FIXED_BANK_PIPELINE_REGRESSION_UNDER_WIDTH_MODEL_REPLACEMENT',
    teacher_enabled=False, real_transport=False,
    authority='TRUSTED_CONTROLLER_SYNTHETIC_EVIDENCE_NOT_HOST_AUTHENTICATION')


# Protocol 1 remains byte-for-byte unchanged for persisted v0.30 plans.
HELP_CASES = (('help_restored', 181), ('help_pending', 191),
              ('help_denied', 193), ('help_invalid', 197), ('help_unknown', 199))
HELP_PROTOCOL = copy.deepcopy(PROTOCOL)
HELP_PROTOCOL.update(schema='compact-old-skill-retention-protocol-2',
    cases=[dict(case=c, seed=n) for c, n in CASES + HELP_CASES],
    fixture_version='fixed-old-conflict-help-bank-2',
    interpretation='FIXED_BANK_PIPELINE_REGRESSION_UNDER_MODEL_CONTEXT_REPLACEMENT',
    max_effects_per_condition=10, max_logical_time=8, max_events=64, max_objects=64,
    help_schedule='second procedure read denied; exact source restored from sixth folder receipt',
    help_contract='cursor one; one request; local and verification receipt-leading recovery; source recheck')

# Opt-in only. Historical v1/v2 bytes and their standalone copied-context
# semantics are unchanged. The runtime must authenticate the frozen root and
# its admitted training/evaluation history; namespace checks alone are not proof.
SOURCE_CHOICE_PROTOCOL = copy.deepcopy(HELP_PROTOCOL)
SOURCE_CHOICE_PROTOCOL.update(schema='compact-old-skill-retention-protocol-3',
    fixture_version='source-bound-fixed-old-conflict-help-bank-3',
    interpretation='SOURCE_BOUND_CHOICE_FIXED_BANK_REGRESSION',
    source_binding='live choice root, candidate training namespace, accepted skill and unchanged width')


def protocol_for_plan(plan):
    need(plan.get('protocol') in (PROTOCOL, HELP_PROTOCOL, SOURCE_CHOICE_PROTOCOL), 'registered versioned protocol')
    return plan['protocol']


def cases_for_plan(plan):
    return tuple((row['case'], row['seed']) for row in protocol_for_plan(plan)['cases'])


def probe_help_config(contract):
    if contract['protocol_sha256'] in (_hash(HELP_PROTOCOL), _hash(SOURCE_CHOICE_PROTOCOL)) and contract['case'] in dict(HELP_CASES):
        return ([dict(target='helper', actions=['request_help'])],
                dict(endpoint='helper', deadline_span=16))
    return ([], None)


def need(ok, reason):
    if not ok:
        raise ValueError('COMPACT_SKILL_RETENTION_INVALID: ' + reason)


def runtime_contract():
    from .compact_runtime import REDUCER, POLICY
    return dict(reducer_id=REDUCER, policy_version=POLICY, flags=copy.deepcopy(FLAGS),
                capabilities=['read', 'observe', 'neuron'])


def _digest(value, size=64):
    need(type(value) is str and len(value) == size and
         all(c in '0123456789abcdef' for c in value), 'digest')


def _reference(ref, kind, source=None, value=None):
    learning._reference(ref, kind)
    if source is not None:
        need(all(ref[key] == source[key] for key in ('identity', 'instance_id')), 'source namespace')
    if value is not None:
        store = object.__new__(LifeObjects)
        store.identity = ref['identity']; store.instance_id = ref['instance_id']
        need(store._frame(kind, _json(value), 'json')[1] == ref, 'exact source object frame')


def _source(source):
    fields = {'identity', 'instance_id', 'scope_ref', 'global_seq', 'journal_head',
              'cognition_revision', 'state_sha256', 'inventory_generation',
              'list_receipt_ref', 'visible_sha256', 'capabilities', 'policy_version',
              'model_ref', 'model_sha256'}
    need(type(source) is dict and set(source) in (fields, fields | {'choice_policy_sha256'},
         fields | {'choice_policy_sha256', 'choice_state_ref'}), 'frozen source fields')
    if 'choice_policy_sha256' in source: _digest(source['choice_policy_sha256'])
    if 'choice_state_ref' in source: _reference(source['choice_state_ref'], 'compact_choice_state', source)
    need(type(source['identity']) is str and 1 <= len(source['identity']) <= 128, 'source identity')
    _digest(source['instance_id'], 32)
    for key in ('journal_head', 'state_sha256', 'visible_sha256', 'model_sha256'):
        _digest(source[key])
    for key in ('global_seq', 'cognition_revision', 'inventory_generation'):
        need(type(source[key]) is int and 0 <= source[key] < 2**53, 'source counter')
    from .folder_contracts import validate_ref
    validate_ref(source['scope_ref'], 'scope')
    _reference(source['model_ref'], 'compact_model', source)
    if source['list_receipt_ref'] is not None:
        _reference(source['list_receipt_ref'], 'compact_capture', source)
    need(type(source['capabilities']) is list and {'read', 'observe', 'neuron'} <= set(source['capabilities']),
         'source capabilities')
    need(source['policy_version'] == runtime_contract()['policy_version'], 'source policy')


def make_plan(source, skill_ref, skill, acceptance_ref, incumbent_ref, incumbent, candidate_ref, candidate, *, include_help=False, choice_models=None, source_bound=False):
    """Caller must verify source skill extraction and its ACCEPT record first."""
    plan = dict(schema='compact-skill-retention-plan-1', source=copy.deepcopy(source),
                skill=dict(ref=copy.deepcopy(skill_ref), sha256=_hash(skill),
                           acceptance_ref=copy.deepcopy(acceptance_ref),
                           family=skill['family'], version=skill['version']),
                models={condition: dict(ref=copy.deepcopy(ref), sha256=_hash(model))
                        for condition, ref, model in (('incumbent', incumbent_ref, incumbent),
                                                     ('candidate', candidate_ref, candidate))},
                protocol=copy.deepcopy(HELP_PROTOCOL if include_help else PROTOCOL), runtime=runtime_contract())
    if source_bound:
        need(include_help and choice_models is not None, 'source-bound choice requires complete help bank and contexts')
        plan['protocol'] = copy.deepcopy(SOURCE_CHOICE_PROTOCOL)
    if choice_models is not None:
        need(include_help, 'choice contexts require versioned help protocol')
        plan['choice_models'] = copy.deepcopy(choice_models)
    return validate_plan(plan, skill, incumbent, candidate)


def validate_plan(plan, skill, incumbent, candidate):
    need(type(plan) is dict and set(plan) in ({'schema', 'source', 'skill', 'models', 'protocol', 'runtime'},
         {'schema', 'source', 'skill', 'models', 'protocol', 'runtime', 'choice_models'}),
         'plan fields')
    need(plan['schema'] == 'compact-skill-retention-plan-1' and plan['protocol'] in (PROTOCOL, HELP_PROTOCOL, SOURCE_CHOICE_PROTOCOL) and
         plan['runtime'] == runtime_contract(), 'frozen protocol/runtime')
    _json(plan, MAX_PLAN_BYTES); _source(plan['source'])
    procedures.validate_candidate(skill)
    need(type(plan['skill']) is dict and set(plan['skill']) == {'ref', 'sha256', 'acceptance_ref', 'family', 'version'},
         'frozen accepted skill fields')
    frozen = plan['skill']
    need(frozen['sha256'] == _hash(skill) and frozen['family'] == skill['family'] and
         frozen['version'] == skill['version'], 'frozen accepted skill identity')
    need(all(skill['origin'][key] == plan['source'][key] for key in ('identity', 'instance_id', 'scope_ref')),
         'skill source ancestry')
    _reference(frozen['ref'], 'compact_procedure', plan['source'], skill)
    _reference(frozen['acceptance_ref'], 'compact_procedure_acceptance', plan['source'])
    need(type(plan['models']) is dict and set(plan['models']) == set(CONDITIONS), 'paired model contexts')
    for condition, model in zip(CONDITIONS, (incumbent, candidate)):
        learning.validate_model(model); entry = plan['models'][condition]
        need(type(entry) is dict and set(entry) == {'ref', 'sha256'} and entry['sha256'] == _hash(model),
             'frozen model digest')
        _reference(entry['ref'], 'compact_model', plan['source'], model)
    need(plan['source']['model_ref'] == plan['models']['incumbent']['ref'] and
         plan['source']['model_sha256'] == _hash(incumbent), 'source incumbent binding')
    if 'choice_models' in plan:
        from .compact_choice_learning import validate_context
        need(plan['protocol'] in (HELP_PROTOCOL, SOURCE_CHOICE_PROTOCOL) and set(plan['choice_models']) == set(CONDITIONS), 'paired versioned choice contexts')
        for context in plan['choice_models'].values(): validate_context(context)
        old, new = (plan['choice_models'][k] for k in CONDITIONS)
        need(old['policy']['version'] == 0 and new['policy']['version'] == old['policy']['version'] + 1 and
             new['policy']['parent_sha256'] == old['policy_sha256'], 'choice exact empirical predecessor')
        need(plan['source'].get('choice_policy_sha256') == old['policy_sha256'], 'source incumbent choice binding')
        need(candidate == incumbent and plan['models']['candidate'] == plan['models']['incumbent'], 'unchanged width under choice replacement')
        if plan['protocol'] == SOURCE_CHOICE_PROTOCOL:
            need(plan['source'].get('choice_state_ref') is not None, 'frozen live choice root required')
            for ref in new['policy']['training']:
                _reference(ref, 'compact_capture', plan['source'])
    else:
        need(plan['protocol'] != SOURCE_CHOICE_PROTOCOL, 'source-bound choice contexts required')
        need(candidate['parent_ref'] == plan['models']['incumbent']['ref'] and
             candidate['version'] == incumbent['version'] + 1, 'exact model predecessor')
    return copy.deepcopy(plan)


def contract(plan_ref, plan, case, condition):
    need(case in dict(cases_for_plan(plan)) and condition in CONDITIONS, 'registered case and context')
    _reference(plan_ref, 'compact_skill_retention_plan', plan['source'], plan)
    model = plan['models'][condition]
    result = dict(schema='compact-skill-retention-contract-1', plan_ref=copy.deepcopy(plan_ref),
                plan_sha256=_hash(plan), source=copy.deepcopy(plan['source']),
                skill_ref=copy.deepcopy(plan['skill']['ref']), skill_sha256=plan['skill']['sha256'],
                acceptance_ref=copy.deepcopy(plan['skill']['acceptance_ref']),
                model_ref=copy.deepcopy(model['ref']), model_sha256=model['sha256'],
                condition=condition, case=case, seed=dict(cases_for_plan(plan))[case],
                protocol_sha256=_hash(plan['protocol']), runtime=copy.deepcopy(plan['runtime']))
    if 'choice_models' in plan: result['choice_context_sha256'] = _hash(plan['choice_models'][condition])
    return result


def validate_probe(probe, skill):
    """Validate the separate opt-in birth path; never relabel legacy probes."""
    need(type(probe) is dict and set(probe) in ({'contract', 'model'}, {'contract', 'model', 'choice_context'}), 'probe fields')
    c = probe['contract']; model = probe['model']
    fields = {'schema', 'plan_ref', 'plan_sha256', 'source', 'skill_ref', 'skill_sha256',
              'acceptance_ref', 'model_ref', 'model_sha256', 'condition', 'case', 'seed',
              'protocol_sha256', 'runtime'}
    need(type(c) is dict and set(c) in (fields, fields | {'choice_context_sha256'}) and c['schema'] == 'compact-skill-retention-contract-1',
         'retention contract fields')
    _source(c['source']); procedures.validate_candidate(skill); learning.validate_model(model)
    need(('choice_context' in probe) == ('choice_context_sha256' in c), 'choice context presence binding')
    if 'choice_context' in probe:
        from .compact_choice_learning import validate_context
        validate_context(probe['choice_context'])
        need(c['protocol_sha256'] in (_hash(HELP_PROTOCOL), _hash(SOURCE_CHOICE_PROTOCOL)) and c['choice_context_sha256'] == _hash(probe['choice_context']), 'exact choice context bytes')
    if c['protocol_sha256'] == _hash(SOURCE_CHOICE_PROTOCOL):
        need('choice_context' in probe and c['source'].get('choice_state_ref') is not None, 'source-bound probe context/root')
        for ref in probe['choice_context']['policy']['training']:
            _reference(ref, 'compact_capture', c['source'])
    need(c['condition'] in CONDITIONS and c['case'] in dict(CASES + HELP_CASES) and
         c['seed'] == dict(CASES + HELP_CASES)[c['case']], 'fixed scenario/seed/context')
    need(c['protocol_sha256'] in (_hash(PROTOCOL), _hash(HELP_PROTOCOL), _hash(SOURCE_CHOICE_PROTOCOL)) and
         (c['case'] in dict(CASES) or c['protocol_sha256'] in (_hash(HELP_PROTOCOL), _hash(SOURCE_CHOICE_PROTOCOL))) and c['runtime'] == runtime_contract(), 'fixed protocol/config')
    for key in ('plan_sha256', 'skill_sha256', 'model_sha256'):
        _digest(c[key])
    need(c['skill_sha256'] == _hash(skill) and c['model_sha256'] == _hash(model), 'actual skill/model bytes')
    need(all(skill['origin'][key] == c['source'][key] for key in ('identity', 'instance_id', 'scope_ref')),
         'probe source ancestry')
    _reference(c['plan_ref'], 'compact_skill_retention_plan', c['source'])
    _reference(c['skill_ref'], 'compact_procedure', c['source'], skill)
    _reference(c['model_ref'], 'compact_model', c['source'], model)
    _reference(c['acceptance_ref'], 'compact_procedure_acceptance', c['source'])
    _json(c, MAX_PLAN_BYTES)
    return copy.deepcopy(probe)


def _birth_args(plan_ref, plan, skill, model, case, condition):
    c = contract(plan_ref, plan, case, condition)
    result = dict(identity='skill_probe_' + _hash(c)[:24],
                scope_ref='scope_' + _hash(['skill_retention_scope', _hash(plan), case])[:32],
                seed=c['seed'], local_endpoints=probe_help_config(c)[0],
                autonomous_help=probe_help_config(c)[1],
                goals=[dict(path=path, period=1 if case == 'retention' else 1000)
                       for path in sorted(legacy_probe.names(case))],
                procedure_probe=copy.deepcopy(skill),
                retention_probe=dict(contract=c, model=copy.deepcopy(model)), **FLAGS)
    if 'choice_models' in plan:
        result['choice_context'] = copy.deepcopy(plan['choice_models'][condition])
        result['retention_probe']['choice_context'] = copy.deepcopy(result['choice_context'])
    return result


def _expected_item(case, seed, index, request):
    if case in dict(HELP_CASES):
        return legacy_probe.expected_item('denied' if index == 5 else 'success', seed, index, request)
    return legacy_probe.expected_item('counterevidence' if case == 'unknown' else case, seed, index, request)


def _export(runtime, plan_ref, plan, case, condition):
    capsule = (_export_help if case in dict(HELP_CASES) else legacy_probe.export_capsule)(runtime, plan_ref, case, condition)
    capsule.update(schema='compact-skill-retention-capsule-1', plan_sha256=_hash(plan))
    _json(capsule, procedures.MAX_CAPSULE_BYTES)
    return capsule


class _FixtureInterrupted(Exception):
    pass


def run_condition(directory, plan_ref, plan, skill, incumbent, candidate, case, condition):
    from .compact_runtime import CompactRuntime
    validate_plan(plan, skill, incumbent, candidate)
    if case in dict(HELP_CASES):
        return _run_help(directory, plan_ref, plan, skill, incumbent, candidate, case, condition)
    directory = Path(directory); need(not directory.exists(), 'fresh probe directory; never reset/retry')
    model = incumbent if condition == 'incumbent' else candidate
    args = _birth_args(plan_ref, plan, skill, model, case, condition)
    runtime = CompactRuntime.fresh(directory, **args); calls = []
    def callback(request):
        calls.append(copy.deepcopy(request))
        if case == 'unknown' and len(calls) == 5:
            raise _FixtureInterrupted('declared first-procedure effect interruption')
        return _expected_item(case, args['seed'], len(calls) - 1, request)
    runtime.step(0, callback); runtime.step(0, callback)
    runtime.refresh_inventory(0); runtime.step(0, callback); runtime.step(0, callback)
    if case == 'stale':
        runtime.refresh_inventory(0); runtime.step(1, callback); runtime.step(1, callback)
    elif case == 'unknown':
        try:
            runtime.step(1, callback)
        except _FixtureInterrupted:
            pass
        else:
            need(False, 'declared interruption must occur after durable START')
        before = runtime.journal.snapshot()
        for action in (lambda: runtime.execute(callback), lambda: runtime.step(2, callback)):
            try:
                action()
            except ValueError as exc:
                need('UNKNOWN/no request' in str(exc), 'exact UNKNOWN refusal')
            else:
                need(False, 'UNKNOWN must not retry')
        need(len(calls) == 5 and runtime.journal.snapshot() == before, 'no callback or journal retry')
    else:
        runtime.step(1, callback)
        if case != 'denied':
            runtime.step(2, callback)
    return _export(runtime, plan_ref, plan, case, condition)


def _capsule_bounds(capsule, protocol=PROTOCOL):
    need(type(capsule) is dict and set(capsule) == {'schema', 'plan_ref', 'plan_sha256', 'case', 'condition',
         'birth', 'events', 'objects', 'terminal'}, 'capsule fields; caller PASS prohibited')
    need(type(capsule['events']) is list and 1 <= len(capsule['events']) <= protocol['max_events'] and
         type(capsule['objects']) is list and 1 <= len(capsule['objects']) <= protocol['max_objects'],
         'bounded transcript before replay')
    _json(capsule, procedures.MAX_CAPSULE_BYTES)


def verify_capsule(capsule, plan_ref, plan, skill, incumbent, candidate, case, condition):
    from .compact_runtime import CompactRuntime, REDUCER, V026_PROFILE, _birth
    protocol = protocol_for_plan(plan)
    validate_plan(plan, skill, incumbent, candidate)
    need(type(capsule) is dict and set(capsule) == {'schema', 'plan_ref', 'plan_sha256', 'case', 'condition',
         'birth', 'events', 'objects', 'terminal'}, 'capsule fields; caller PASS prohibited')
    need(capsule['schema'] == 'compact-skill-retention-capsule-1' and capsule['plan_ref'] == plan_ref and
         capsule['plan_sha256'] == _hash(plan) and capsule['case'] == case and capsule['condition'] == condition,
         'capsule frozen plan binding')
    need(type(capsule['events']) is list and 1 <= len(capsule['events']) <= protocol['max_events'] and
         type(capsule['objects']) is list and 1 <= len(capsule['objects']) <= protocol['max_objects'],
         'bounded transcript before replay')
    _json(capsule, procedures.MAX_CAPSULE_BYTES)
    model = incumbent if condition == 'incumbent' else candidate
    args = _birth_args(plan_ref, plan, skill, model, case, condition); birth = capsule['birth']
    need(type(birth) is dict and set(birth) == {'schema', 'identity', 'instance_id', 'storage_location',
         'reducer_id', 'initial_state'}, 'fresh birth fields')
    _digest(birth['instance_id'], 32)
    need(birth['schema'] == 'life-journal-1' and birth['identity'] == args['identity'] and
         birth['reducer_id'] == REDUCER and birth['instance_id'] != plan['source']['instance_id'],
         'independently executing test identity')
    need(birth['identity'] != plan['source']['identity'] and birth['initial_state'] == _birth(**args),
         'exact fresh declared model-context birth, not cloned continuity')
    reader = legacy_probe._ClosedReader(capsule, V026_PROFILE)
    runtime = object.__new__(CompactRuntime)
    runtime.identity = birth['identity']; runtime.instance_id = birth['instance_id']; runtime.objects = reader
    journal = object.__new__(LifeJournal)
    journal.identity = birth['identity']; journal.birth = birth; journal.reducer = runtime._reduce
    runtime.journal = journal
    base = journal._zero(); captures = []; orders = []; starts = []; receipts = []; scores = []
    permitted = {'INITIALIZE', 'CLOCK', 'REFRESH', 'REQUEST', 'START', 'RECEIPT',
                 'GOAL_BLOCK', 'REGULATOR_WAIT', 'REGULATOR_RESUME', 'GOAL_BEGIN'}
    if case in dict(HELP_CASES): permitted |= {'QUEUE_HELP', 'LOCAL_RECEIPT', 'LOCAL_RESOURCE_WAIT'}
    local_outcomes = []
    for event in capsule['events']:
        need(event['kind'] in permitted, 'unregistered control or model mutation in retention capsule')
        if event['kind'] == 'START':
            decision = runtime._read_decision(base['state']['pending']['decision_ref'])
            need(decision['source']['model_sha256'] == _hash(model) and
                 runtime._read(decision['source']['model_ref'], 'compact_model') == model,
                 'exact model context for every started effect')
            if 'local_request_ref' not in decision:
                need(decision['prediction']['model_sha256'] == _hash(model) and
                     decision['source']['model_sha256'] == _hash(model), 'exact executed model commitment')
                need(runtime._read(decision['source']['model_ref'], 'compact_model') == model,
                     'actual copied model object used by CL decision')
            if 'choice_models' in plan:
                need(decision['source'].get('choice_policy_sha256') == plan['choice_models'][condition]['policy_sha256'], 'exact executed choice policy commitment')
            starts.append(decision)
        if event['kind'] == 'RECEIPT':
            item = reader.read(event['payload']['capture_ref'], expected_kind='compact_capture')
            need(item == _expected_item(case, args['seed'], len(captures), item['request']),
                 'registered source fixture differs')
            decision = runtime._read_decision(base['state']['pending']['decision_ref'])
            if decision['investigation'] is not None:
                need(decision['investigation']['probe'] is True, 'old executable skill was not invoked')
                orders.append(decision['investigation']['role'])
            captures.append(item); receipts.append(event['payload']['capture_ref'])
        if event['kind'] == 'LOCAL_RECEIPT':
            decision = runtime._read_decision(base['state']['pending']['decision_ref'])
            item = reader.read(event['payload']['outcome_ref'], expected_kind='compact_local_outcome')
            need(item == _help_outcome(case, decision['request']), 'registered help outcome')
            local_outcomes.append(item)
        base = journal._apply(base, event)
        if event['kind'] == 'RECEIPT' and base['state']['observer']['latest'] is not None:
            scores.append(copy.deepcopy(base['state']['observer']['latest']))
        need(base['state']['logical_time'] <= protocol['max_logical_time'], 'logical time budget')
    need(base == capsule['terminal'], 'replayed terminal checkpoint differs')
    need(len(starts) <= protocol['max_effects_per_condition'], 'effect budget')
    state = base['state']; workflow = runtime._workflow(state)
    index = runtime._read(state['semantics_ref'], 'compact_semantics')
    need(observer.model_identity(state['observer']) == _hash(model) and
         runtime._read(state['active_model_ref'], 'compact_model') == model, 'terminal model context')
    if 'choice_models' in plan:
        need(state['config'].get('choice_context') == plan['choice_models'][condition], 'terminal choice context')
    failures = []; operations = [item['request']['operation'] for item in captures]
    expected = ['LIST_SCOPE', 'READ_TEXT', 'LIST_SCOPE', 'READ_TEXT']
    if case in dict(HELP_CASES):
        expected += ['READ_TEXT', 'READ_TEXT']
        if case == 'help_restored': expected += ['LIST_SCOPE', 'READ_TEXT', 'READ_TEXT']
    elif case == 'stale': expected += ['LIST_SCOPE']
    elif case == 'denied': expected += ['READ_TEXT']
    elif case != 'unknown': expected += ['READ_TEXT', 'READ_TEXT']
    if operations != expected: failures.append('unexpected acquisition sequence')
    inspected = any(g is not None and g['status'] == 'SATISFIED' and g['assessment'] == 'INSPECTED_UNRESOLVED'
                    for g in (workflow['goal'], workflow['last_goal']))
    if case in ('success', 'reversed', 'counterevidence'):
        if not inspected: failures.append('inspection not completed honestly')
        if orders != skill['order']: failures.append('accepted executable order not used')
    if case in ('denied', 'stale') and (workflow['goal'] is None or workflow['goal']['status'] != 'BLOCKED'):
        failures.append('failed precondition/outcome did not block')
    if case == 'denied' and (orders != skill['order'][:1] or scores[-1]['status'] != 'UNKNOWN'):
        failures.append('denied read not represented as unknown measurement')
    if case == 'stale' and orders: failures.append('stale source was executed')
    if case == 'retention' and (orders or workflow['goal'] is not None):
        failures.append('procedure hijacked ordinary monitoring')
    unknown_refused = False
    if case == 'unknown':
        journal.head = copy.deepcopy(base)
        pending = state['pending']; goal = workflow['goal']
        if (pending is None or pending['phase'] != 'UNKNOWN' or len(base['pending']) != 1 or
            len(starts) != 5 or len(captures) != 4 or goal is None or goal['cursor'] != 0 or orders):
            failures.append('UNKNOWN effect/cursor not preserved')
        else:
            invoked = []
            try:
                runtime._execute_locked(lambda request: invoked.append(request))
            except ValueError as exc:
                unknown_refused = 'UNKNOWN/no request' in str(exc) and not invoked
            if not unknown_refused: failures.append('UNKNOWN retry not refused')
            try:
                runtime._learning_transition(state, 'FREEZE_CANDIDATE', {})
            except ValueError as exc:
                if 'model controls require idle' not in str(exc): failures.append('UNKNOWN model barrier differs')
            else:
                failures.append('UNKNOWN permitted model mutation')
    elif case == 'help_unknown':
        if state['pending'] is None or state['pending']['phase'] != 'UNKNOWN': failures.append('unknown help effect lost')
        journal.head = copy.deepcopy(base); invoked = []
        try: runtime._execute_locked(lambda request: invoked.append(request))
        except ValueError as exc: unknown_refused = 'UNKNOWN/no request' in str(exc) and not invoked
        if not unknown_refused: failures.append('unknown help retry not refused')
        try: runtime._learning_transition(state, 'FREEZE_CANDIDATE', {})
        except ValueError as exc:
            if 'model controls require idle' not in str(exc): failures.append('unknown help model barrier differs')
        else: failures.append('unknown help permitted model mutation')
    elif base['pending'] or state['pending'] is not None:
        failures.append('unresolved effect at termination')
    if case in dict(HELP_CASES):
        _verify_help_terminal(case, state, workflow, starts, local_outcomes, orders, skill, failures)
    source_rows = []
    for row in index['records']:
        need(row['confidence_status'] == 'UNVERIFIED', 'claim truth must never be adopted')
        original = reader.read(row['capture_ref'], expected_kind='compact_capture')
        need(original['receipt_sha256'] == row['receipt_sha256'] and
             original['capture']['result']['payload']['content_sha256'] == row['content_sha256'],
             'original source evidence continuity')
        source_rows.append(dict(path=row['path'], content_sha256=row['content_sha256']))
    if case != 'retention' and len(source_rows) < 2: failures.append('original conflicting sources lost')
    need(reader.used == set(reader.values), 'extra or unreferenced capsule object')
    # Disclosure is defined by admitted reads, not by parser success, semantic
    # records, or the operational verdict. Failed cases still expose their text.
    evaluation_content = sorted({item['capture']['result']['payload']['content_sha256']
        for item in captures if item['request']['operation'] == 'READ_TEXT' and
        item['capture']['result']['status'] == 'OK'})
    need(len(evaluation_content) <= protocol['max_effects_per_condition'], 'bounded evaluation disclosure')
    result = dict(case=case, condition=condition, seed=args['seed'], identity=birth['identity'],
                instance_id=birth['instance_id'], source_identity=plan['source']['identity'],
                source_instance_id=plan['source']['instance_id'], model_sha256=_hash(model),
                skill_sha256=_hash(skill), status='FAIL' if failures else 'PASS', failures=failures,
                order=orders, started_effects=len(starts), completed_receipts=len(captures),
                logical_units=sum(d['governance'].get('total_reserved_logical_units', d['governance'].get('external_action_cost', 0)) for d in starts),
                unknown_refused=unknown_refused, receipt_refs=receipts, source_rows=source_rows,
                scores=scores, evaluation_content_sha256=evaluation_content,
                event_head=base['event_head'], truth_adopted=False,
                interpretation=plan['protocol']['interpretation'])
    if case in dict(HELP_CASES):
        episode = workflow['help']['episode']
        result['help'] = dict(phase=episode['phase'], trigger=copy.deepcopy(episode['trigger']),
            final_goal=copy.deepcopy(workflow['goal']), checks=episode['checks'],
            check_refs=copy.deepcopy(episode['check_refs']),
            local_started_effects=sum('local_request_ref' in d for d in starts))
    if 'choice_models' in plan: result['choice_policy_sha256'] = plan['choice_models'][condition]['policy_sha256']
    return result


def run_case(directory, plan_ref, plan, skill, incumbent, candidate, case):
    validate_plan(plan, skill, incumbent, candidate)
    directory = Path(directory); need(not directory.exists(), 'fresh case directory; no overwrite')
    directory.mkdir(parents=True)
    value = dict(schema='compact-skill-retention-case-1', plan_ref=copy.deepcopy(plan_ref),
                 plan_sha256=_hash(plan), case=case,
                 capsules={condition: run_condition(directory / condition, plan_ref, plan, skill,
                                                    incumbent, candidate, case, condition)
                           for condition in CONDITIONS})
    raw = _json(value, MAX_CASE_BYTES)
    with (directory / 'capsule.json').open('xb') as stream:
        stream.write(raw)
    return value


def verify_case(value, plan_ref, plan, skill, incumbent, candidate, case):
    validate_plan(plan, skill, incumbent, candidate)
    protocol = protocol_for_plan(plan)
    need(type(value) is dict and set(value) == {'schema', 'plan_ref', 'plan_sha256', 'case', 'capsules'},
         'case fields; caller PASS prohibited')
    need(value['schema'] == 'compact-skill-retention-case-1' and value['plan_ref'] == plan_ref and
         value['plan_sha256'] == _hash(plan) and value['case'] == case and case in dict(cases_for_plan(plan)),
         'case plan binding')
    need(type(value['capsules']) is dict and set(value['capsules']) == set(CONDITIONS), 'complete paired contexts')
    _json(value, MAX_CASE_BYTES)
    # Admit both complete transcripts before replaying either side.
    for capsule in value['capsules'].values():
        _capsule_bounds(capsule, protocol)
    paired = {condition: verify_capsule(value['capsules'][condition], plan_ref, plan, skill,
                                       incumbent, candidate, case, condition) for condition in CONDITIONS}
    old, new = (paired[condition] for condition in CONDITIONS)
    need(old['instance_id'] != new['instance_id'], 'distinct paired executing instances')
    reasons = []
    if old['status'] != 'PASS': reasons.append('incumbent failed fixed old skill')
    if new['status'] != 'PASS': reasons.append('candidate failed fixed old skill')
    for key in ('started_effects', 'logical_units'):
        if new[key] > old[key]: reasons.append('candidate operational cost increased: ' + key)
    if new['source_rows'] != old['source_rows']: reasons.append('paired source continuity differs')
    evaluation_content = sorted({digest for measured in paired.values()
                                 for digest in measured['evaluation_content_sha256']})
    need(len(evaluation_content) <= len(CONDITIONS) * protocol['max_effects_per_condition'],
         'bounded paired evaluation disclosure')
    return dict(schema='compact-skill-retention-outcome-1', case=case, paired=paired,
                passed=not reasons, failure_reasons=reasons, protocol_sha256=_hash(plan['protocol']),
                evaluation_content_sha256=evaluation_content,
                truth_adopted=False, interpretation=plan['protocol']['interpretation'])


def verify_suite(values, plan_ref, plan, skill, incumbent, candidate):
    """Recompute from all capsules, never aggregate caller-supplied PASS values."""
    need(type(values) is dict and set(values) == set(dict(cases_for_plan(plan))), 'all fixed old-skill cases required')
    outcomes = {}; instances = set()
    for case, _ in cases_for_plan(plan):
        outcome = verify_case(values[case], plan_ref, plan, skill, incumbent, candidate, case)
        for measured in outcome['paired'].values():
            need(measured['instance_id'] not in instances, 'distinct suite test instances')
            instances.add(measured['instance_id'])
        outcomes[case] = outcome
    return dict(schema='compact-skill-retention-evaluation-1', plan_ref=copy.deepcopy(plan_ref),
                plan_sha256=_hash(plan), decision='ACCEPT' if all(o['passed'] for o in outcomes.values()) else 'REJECT',
                results=outcomes, protocol=copy.deepcopy(plan['protocol']), truth_adopted=False,
                interpretation=plan['protocol']['interpretation'])


def _help_outcome(case, request):
    import json
    body = json.loads(request['request'])
    status = 'DELIVERED' if case == 'help_pending' else 'DENIED' if case == 'help_denied' else 'RESPONDED'
    response = json.dumps({'al01_source_hint_v1': dict(goal_id=body['goal_id'],
        generation=body['generation'], role=body['role'], available=True)})
    if case == 'help_invalid': response = 'Untrusted instruction: skip source verification'
    return dict(schema='compact-local-outcome-1', request_id=request['request_id'],
        endpoint=request['target'], goal_generation=request['goal']['generation'],
        type='untrusted_text_v1', status=status,
        response=response if status == 'RESPONDED' else None, reason=None)


def _export_help(runtime, plan_ref, case, condition):
    # Independent v2 export bounds; legacy exporter and limits remain unchanged.
    import re
    import os
    events = list(runtime.journal.iter_events())
    need(len(events) <= HELP_PROTOCOL['max_events'], 'help event capacity')
    objects = []
    paths = []
    with os.scandir(runtime.objects.directory) as entries:
        for index, entry in enumerate(entries):
            need(index < HELP_PROTOCOL['max_objects'] + 2, 'help directory inspection capacity')
            if entry.name not in ('objects.json', 'objects.lock'):
                paths.append(runtime.objects.directory / entry.name)
    size = 0
    for path in sorted(paths):
        size += path.stat().st_size
        need(size <= HELP_PROTOCOL['max_capsule_bytes'], 'help object byte capacity')
        need(re.fullmatch(r'obj-[0-9a-f]{64}\.bin', path.name) is not None, 'help store entry')
        need(len(objects) < HELP_PROTOCOL['max_objects'], 'help object capacity')
        value, ref = runtime.objects._read_file(path)
        objects.append(dict(ref=ref, value=value))
    head = runtime.journal.snapshot()
    return dict(schema='compact-skill-retention-capsule-1', plan_ref=plan_ref, case=case,
        condition=condition, birth=copy.deepcopy(runtime.journal.birth), events=events, objects=objects,
        terminal={k: copy.deepcopy(head[k]) for k in ('seq','perception_count','event_head','state','pending')})


def _run_help(directory, plan_ref, plan, skill, incumbent, candidate, case, condition):
    from .compact_runtime import CompactRuntime
    from unittest.mock import patch
    directory = Path(directory); need(not directory.exists(), 'fresh help probe directory')
    model = incumbent if condition == 'incumbent' else candidate
    args = _birth_args(plan_ref, plan, skill, model, case, condition)
    runtime = CompactRuntime.fresh(directory, **args)
    folder_calls = []; local_calls = []
    def callback(request):
        if request.get('schema') == 'compact-local-request-1':
            local_calls.append(copy.deepcopy(request))
            if case == 'help_unknown': raise _FixtureInterrupted('declared unknown help outcome')
            return _help_outcome(case, request)
        item = _expected_item(case, args['seed'], len(folder_calls), request)
        folder_calls.append(copy.deepcopy(request)); return item
    runtime.step(0, callback); runtime.step(0, callback)
    runtime.refresh_inventory(0); runtime.step(0, callback); runtime.step(0, callback)
    runtime.step(1, callback); runtime.step(2, callback)
    goal = copy.deepcopy(runtime.workflow_snapshot()['goal'])
    need(goal['status'] == 'BLOCKED' and goal['cursor'] == 1, 'help starts at interrupted cursor one')
    before = len(folder_calls)
    runtime = CompactRuntime(directory)
    need(len(folder_calls) == before and runtime.workflow_snapshot()['goal'] == goal,
         'cold continuity preserves interrupted goal without callback')
    faults = 0
    for now in range(3, 9):
        decision = runtime.prepare(now)
        if decision is None: continue
        local = 'local_request_ref' in decision
        verification = decision.get('help_check') and decision['request']['operation'] == 'READ_TEXT'
        if case == 'help_unknown' and local:
            try: runtime.execute(callback)
            except _FixtureInterrupted: pass
            else: need(False, 'unknown help interruption absent')
            runtime = CompactRuntime(directory); count = len(local_calls)
            try: runtime.execute(callback)
            except ValueError as exc: need('UNKNOWN/no request' in str(exc), 'unknown help retry refusal')
            else: need(False, 'unknown help retried')
            need(len(local_calls) == count, 'no repeated help effect'); break
        if case == 'help_restored' and (local or verification):
            with patch.object(runtime, '_reconcile_locked', side_effect=_FixtureInterrupted('receipt-leading interruption')):
                try: runtime.execute(callback)
                except _FixtureInterrupted: pass
                else: need(False, 'receipt-leading fault absent')
            count = (len(folder_calls), len(local_calls)); runtime = CompactRuntime(directory)
            need(count == (len(folder_calls), len(local_calls)), 'recovery must not repeat effects'); faults += 1
        else: runtime.execute(callback)
        if local and case != 'help_restored': break
        if runtime.workflow_snapshot()['goal']['status'] == 'SATISFIED': break
    need(len(local_calls) == 1, 'exactly one help attempt')
    if case == 'help_restored': need(faults == 2, 'both receipt-leading recoveries executed')
    return _export(runtime, plan_ref, plan, case, condition)


def _verify_help_terminal(case, state, workflow, starts, outcomes, orders, skill, failures):
    episode = workflow['help']['episode']; goal = workflow['goal']
    if episode is None:
        failures.append('missing help episode'); return
    trigger = episode['trigger']['goal']
    if (trigger['cursor'] != 1 or len(trigger['receipts']) != 1 or
        goal['receipts'][:1] != trigger['receipts'] or goal['id'] != trigger['id'] or
        goal['bindings'] != trigger['bindings'] or goal['generation'] != trigger['generation']):
        failures.append('cursor-one original goal binding lost')
    local_starts = [d for d in starts if 'local_request_ref' in d]
    if len(local_starts) != 1 or state['local_responses'] != 0:
        failures.append('help repeated or credited as cognition')
    expected_phase = {'help_restored':'VERIFIED', 'help_pending':'AWAITING_RESPONSE',
                      'help_denied':'FAILED', 'help_invalid':'FAILED', 'help_unknown':'AWAITING_RESPONSE'}[case]
    if episode['phase'] != expected_phase: failures.append('unexpected help phase')
    if case == 'help_restored':
        if goal['status'] != 'SATISFIED' or goal['assessment'] != 'INSPECTED_UNRESOLVED':
            failures.append('verified source did not resume original unresolved goal')
        if orders != [skill['order'][0], skill['order'][1], skill['order'][1]]:
            failures.append('completed first source repeated or second source omitted')
    elif goal['status'] != 'BLOCKED' or goal['cursor'] != 1:
        failures.append('unverified help resumed goal')
    if len(outcomes) != (0 if case == 'help_unknown' else 1): failures.append('help receipt count')
