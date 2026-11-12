"""Explicit lifecycle for the single baseline exploration goal, not general planning."""
from .core import digest

def marker(state,world):
    visited={tuple(p) for p in state['visited']};x,y=state['position']
    distances=[abs(cx-x)+abs(cy-y) for cy in range(world.height) for cx in range(world.width) if (cx,cy) not in visited] if world.local else []
    return {'knowledge':digest(state['observed']),'visited':len(visited),'tasks':len(state['task_results']),
            'world_version':world.version,'frontier_distance':min(distances) if distances else 0}

def initial(state,world):
    return {'status':'ACTIVE','reason':'initial','no_progress':0,'marker':marker(state,world),'updated_tick':state['tick']}

def advance(state,world,kind,payload):
    # Inert human claims are new recorded input, not progress/failure of exploration.
    # Preserve exhausted/waiting exploration while allowing the routing outbox to drain.
    messages=payload.get('messages',[]) if isinstance(payload,dict) else []
    if (kind=='commit' and payload.get('packet',{}).get('origin_module')=='human_intake'
            and not payload.get('proposals') and payload.get('wait') is None and messages
            and all(isinstance(m,dict) and m.get('record_type')=='Claim'
                    and m.get('source')=='human_external' and m.get('status')=='UNVERIFIED'
                    and m.get('evidence_refs')==[] for m in messages)):
        if state['goal_state']['status'] in ('WAITING','COMPLETED'):state['waiting']=True
        state['goal_state']['updated_tick']=state['tick']
        return
    goal=state['goal_state'];old_status=goal['status'];old_reason=goal['reason'];old=goal['marker'];new=marker(state,world)
    progress=(new['knowledge']!=old['knowledge'] or new['visited']>old['visited'] or new['tasks']>old['tasks'] or
              new['world_version']!=old['world_version'] or new['frontier_distance']<old['frontier_distance'])
    if progress:goal['no_progress']=0
    elif kind in ('commit','feedback'):goal['no_progress']=min(3,goal['no_progress']+1)
    # Keep the best distance since the visited set/world changed; ping-pong is not new progress.
    if new['visited']==old['visited'] and new['world_version']==old['world_version']:
        new['frontier_distance']=min(old['frontier_distance'],new['frontier_distance'])
    goal['marker']=new;goal['updated_tick']=state['tick']
    goal['status']='ACTIVE';goal['reason']='progress' if progress else 'pending'
    if kind=='commit' and payload.get('wait') is not None:
        unknown=any(x not in state['observed'] for x in world.visible(state['position']))
        unexplored=world.local and len({tuple(p) for p in state['visited']})<world.width*world.height
        goal['status']='WAITING' if unknown or unexplored else 'COMPLETED'
        goal['reason']='explicit_wait' if unknown or unexplored else 'environment_exhausted'
    elif state['waiting'] and old_status in ('WAITING','COMPLETED'):
        goal['status']=old_status;goal['reason']=old_reason
    elif goal['no_progress']>=3:
        goal['status']='BLOCKED';goal['reason']='three_steps_without_goal_progress'
