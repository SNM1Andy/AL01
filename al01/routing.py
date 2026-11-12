"""Persistence-friendly, next-tick-only local routing. No network or host actions."""
from __future__ import annotations
import copy, re
from .contracts import NEURONS, validate_args, TaskScheduler

def resolve_evidence(refs,state):
    from .core import fail
    result=[]
    if len(refs)>64: fail('BUDGET_EXCEEDED','evidence refs')
    for ref in refs:
        if not re.fullmatch(r'event_[0-9]{1,9}',ref): fail('UNRESOLVED_REF',str(ref))
        index=int(ref[6:])
        if index>=len(state['events']): fail('UNRESOLVED_REF',ref)
        event=state['events'][index]
        result.append({'ref':ref,'content_hash':event['hash'],'kind':event['kind'],'tick':event['tick']})
    return result

def validate_task(task,existing_ids):
    from .core import fail, serializable
    if not isinstance(task,dict) or set(task)!={'id','module','args','depends_on'}: fail('TYPE_ERROR','task fields')
    if not isinstance(task['id'],str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,63}',task['id']): fail('TYPE_ERROR','task ID')
    if task['id'] in existing_ids: fail('DUPLICATE_TASK',task['id'])
    if not isinstance(task['module'],str) or task['module'] not in NEURONS: fail('UNKNOWN_OPCODE',str(task['module']))
    deps=task['depends_on']
    if not isinstance(deps,list) or any(not isinstance(d,str) for d in deps) or len(set(deps))!=len(deps) or any(d not in existing_ids for d in deps): fail('DEPENDENCY_ERROR','dependencies must already exist')
    args=task['args']; spec=NEURONS[task['module']]
    if not isinstance(args,list) or len(args)!=len(spec.types): fail('TYPE_ERROR','task args')
    for value,typ in zip(args,spec.types):
        if isinstance(value,dict) and 'task_ref' in value:
            if set(value)!={'task_ref'} or value['task_ref'] not in deps: fail('DEPENDENCY_ERROR','task ref')
        else:
            from .contracts import validate_type
            validate_type(value,typ)
    return copy.deepcopy(serializable(task))

def resolve_task_args(task,results):
    """Resolve dependencies without confusing serialized EntityRefs with task refs."""
    from .core import fail
    values=[]
    for arg in task['args']:
        if isinstance(arg,dict) and set(arg)=={'task_ref'}:
            record=results[arg['task_ref']]
            if record['status']!='ok': fail('DEPENDENCY_ERROR','upstream task failed')
            arg=record['value']
        values.append(arg)
    return values

def run_ready_task(task,results,*,state=None,capabilities=('neuron',)):
    """Dependencies enter a later step; no recursive same-tick dispatch."""
    values=resolve_task_args(task,results)
    return TaskScheduler().run([{'id':task['id'],'module':task['module'],'args':values}],state=state,capabilities=capabilities)[task['id']]

def run_ready_task_with_metadata(task,results,*,state,capabilities):
    """Capture successful result/evidence at the read version, before tick commit."""
    from .contracts import execute_neuron
    return execute_neuron(task['module'],resolve_task_args(task,results),state=state,capabilities=capabilities)
