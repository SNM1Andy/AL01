"""Committed request/result shapes shared by checkpoint and sandbox executor."""
import math,re
from .core import digest,fail

def validate_request(r):
    import hashlib
    if not isinstance(r,dict) or set(r)!={'action_id','dedup_key','proposal','position','world_version','world_digest','requested_tick'}:fail('ACTION_INVALID','request fields')
    if not isinstance(r['dedup_key'],str) or not 1<=len(r['dedup_key'])<=256:fail('ACTION_INVALID','request key')
    if r['action_id']!='act_'+hashlib.sha256(r['dedup_key'].encode()).hexdigest():fail('ACTION_INVALID','request identity')
    if type(r['requested_tick']) is not int or not 1<=r['requested_tick']<=2**53 or type(r['world_version']) is not int or not 0<=r['world_version']<=2**53:fail('ACTION_INVALID','request versions')
    if not isinstance(r['world_digest'],str) or not re.fullmatch('[0-9a-f]{64}',r['world_digest']):fail('ACTION_INVALID','world digest')
    if not position(r['position']):fail('ACTION_INVALID','request position')
    p=r['proposal']
    if not isinstance(p,dict):fail('ACTION_INVALID','proposal')
    if p.get('kind')=='observe':
        if set(p)!={'kind','target'} or not isinstance(p['target'],str) or not p['target']:fail('ACTION_INVALID','observation proposal')
    elif p.get('kind')=='move':
        if set(p)!={'kind','dx','dy'} or type(p['dx']) is not int or type(p['dy']) is not int or abs(p['dx'])+abs(p['dy'])!=1:fail('ACTION_INVALID','movement proposal')
    else:fail('ACTION_INVALID','proposal kind')

def position(p):return isinstance(p,list) and len(p)==2 and all(type(v) is int and abs(v)<=2**53 for v in p)

def validate_result(request,result):
    validate_request(request);p=request['proposal']
    if p['kind']=='observe':
        if not isinstance(result,dict) or set(result)!={'target','value','position'} or result['target']!=p['target'] or not position(result['position']) or type(result['value']) not in (int,float) or not math.isfinite(result['value']):fail('ACTION_INVALID','observation result')
    else:
        expected=[request['position'][0]+p['dx'],request['position'][1]+p['dy']]
        if not position(result) or result!=expected:fail('ACTION_INVALID','movement result')

def journal_payload(request):
    return {k:request[k] for k in ('proposal','position','world_version','world_digest')}

def validate_history(state):
    """Exact committed-intent minus linked-result accounting, also without disk journal."""
    from .core import canonical
    outstanding={};seen=set();legacy=set();completed=set()
    for event in state['events']:
        p=event['payload']
        if event['kind']=='commit':
            r=p.get('action_requested')
            if r is not None:
                validate_request(r);aid=r['action_id']
                if aid in seen or outstanding or r['requested_tick']!=event['tick'] or p.get('action_ids')!=[aid] or canonical(p.get('proposals'))!=canonical([r['proposal']]) or p.get('movement') is not None:fail('ACTION_INVALID','committed request accounting')
                seen.add(aid);outstanding[aid]=r
            else:
                for aid in p.get('action_ids',[]):
                    if aid in seen:fail('ACTION_INVALID','duplicate legacy action')
                    seen.add(aid);legacy.add(aid)
        if event['kind'] in ('feedback','action_result') and 'action_id' in p:
            aid=p['action_id']
            if aid in completed:fail('ACTION_INVALID','duplicate result')
            if aid in outstanding:
                r=outstanding.pop(aid);expected='feedback' if r['proposal']['kind']=='observe' else 'action_result'
                if event['kind']!=expected:fail('ACTION_INVALID','result kind')
                if expected=='feedback':result={k:v for k,v in p.items() if k not in ('action_id','goal_state')}
                else:
                    if p.get('kind')!='movement':fail('ACTION_INVALID','movement result kind')
                    result=p.get('position')
                validate_result(r,result)
            elif aid not in legacy:fail('ACTION_INVALID','orphan result')
            completed.add(aid)
    queue=state['requested_actions']
    if not isinstance(queue,list) or len(queue)>1 or len(queue)!=len(outstanding):fail('ACTION_INVALID','outstanding request count')
    if queue:
        r=queue[0];validate_request(r)
        if canonical(outstanding.get(r['action_id']))!=canonical(r) or state['position']!=r['position']:fail('ACTION_INVALID','outstanding request/position mismatch')
