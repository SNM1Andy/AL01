"""Fresh-instance controller: one governed folder executor and a local-only outbox.

All external-looking local actions stop at QUEUED_LOCAL_NOT_SENT. Responses are
explicit admitted values, never notifications or independently verified facts.
Logical time is supplied by the caller; polling does not manufacture life ticks.
"""
import copy
import os
from pathlib import Path
from .folder_agent import FolderAgentRunner
from .folder_joint import need
from .folder_runner import atomic_json
from .folder_session import _hash,_encode
from .ordered_folder_session import OrderedFolderSession
from .controller_paths import check_file
from . import system_goals as goals
from . import system_actions as actions

SCHEMA='system-coordinator-1'
MAX_EVENTS=128
MAX_BYTES=4*1024*1024


class SystemCoordinator(FolderAgentRunner):
    execution_owner=SCHEMA
    def __init__(self,root,state_directory,*,local_recipients=None,**kwargs):
        directory=Path(os.path.abspath(state_directory));path=directory/'system-controller.json'
        check_file(path)
        actor=directory/'actor.json';check_file(actor)
        if path.exists() or actor.exists():
            need(path.exists() and actor.exists(),'partial system birth or unsupported migration; no replacement files')
        self._system_initializing=not path.exists() and not actor.exists()
        super().__init__(root,state_directory,**kwargs)
        self.system_path=path
        with self._joint_lock():
            if not path.exists():
                recipients={} if local_recipients is None else local_recipients
                actions.initialize(self.actor['id'],self.actor['scope_ref'],recipients)
                data=dict(schema=SCHEMA,identity=self.actor['id'],scope_ref=self.actor['scope_ref'],
                          recipients=copy.deepcopy(recipients),birth_joint_sha256=self.joint_sha,events=[])
                self._save_system(data)
            self._system_initializing=False
            self._load_system()
            need(local_recipients is None or local_recipients==self.system['recipients'],'frozen local recipients')
            self._sync_system()

    def _save_system(self,data):
        check_file(self.system_path)
        need(len(_encode(data))<=MAX_BYTES,'system storage budget')
        atomic_json(self.system_path,{'data':data,'sha256':_hash(data)});self.session._sync(self.directory)

    def _initial(self,data):
        g=goals.add_goal(goals.initial_state(self.actor['id'],self.actor['scope_ref']),'exploration','explore',limit=29)
        return dict(goals=g,actions=actions.initialize(self.actor['id'],self.actor['scope_ref'],data['recipients']),
                    proposals=[],approved=None,clock=0)

    def _folder_candidate(self,state,cognition,features):
        request,_,_,_=self._choose(cognition,features)
        if request is None:return None,None
        op=request['operation'];target=self.actor['scope_ref'];query=''
        if op=='CHECK_ACCESS':return None,None # scripted diagnostics are not an exploration need
        if op=='READ_TEXT':
            entries=[]
            for source in cognition['sources']:
                if source['request']['operation']=='LIST_SCOPE' and source['capture']['result']['status']=='OK':entries=source['capture']['result']['payload']['entries']
            target=next((e['relative_path'] for e in entries if e['entry_ref']==request['entry_ref']),None)
            need(target is not None,'approved read requires listing path')
        elif op=='CHECK_ACCESS':query=request['intent']
        active=[g for g in state['goals']['goals'] if g['status']=='active' and (g['kind']=='explore' or (g['kind']=='inspect' and op=='READ_TEXT' and g['target']==target))]
        if not active:return None,None
        g=sorted(active,key=lambda g:(g['kind']!='inspect',g['id']))[0]
        candidate=dict(goal_id=g['id'],action_id=request['request_id'],operation=op,cost=1,gain=.5,urgency=0,
                       source_sha256=_hash(state['goals']['sources']),intent={'target':target,'query':query})
        return candidate,request

    def _candidates(self,state,cognition,features):
        candidate,request=self._folder_candidate(state,cognition,features)
        candidates=[] if candidate is None else [candidate]
        active={g['id'] for g in state['goals']['goals'] if g['status']=='active'}
        for q in state['proposals']:
            if q['request_id'] in state['actions']['requests'] or q['goal_id'] not in active:continue
            if q['deadline']<=state['clock'] or q['source_version']!=len(cognition['sources']):continue
            candidates.append(dict(goal_id=q['goal_id'],action_id=q['action_id'],operation=q['action'],cost=q['logical_cost'],gain=1.,urgency=.1,
                source_sha256=_hash(state['goals']['sources']),intent={'target':q['recipient'] or 'local_wait','query':q['query']}))
        return candidates,request

    def _replay_system(self,data):
        need(type(data) is dict and set(data)=={'schema','identity','scope_ref','recipients','birth_joint_sha256','events'},'system fields')
        need(data['schema']==SCHEMA and data['identity']==self.actor['id'] and data['scope_ref']==self.actor['scope_ref'],'system identity')
        events=data['events'];need(type(events) is list and len(events)<=MAX_EVENTS,'system event budget')
        counts={0}
        for e in events:
            need(type(e) is dict and set(e)=={'index','source_count','source_joint_sha256','kind','payload','previous','sha256'},'event fields')
            need(type(e['source_count']) is int and 0<=e['source_count']<=len(self.bindings),'event source count');counts.add(e['source_count'])
        historical=self._historical_states(counts)
        need(data['birth_joint_sha256']==historical[0][2],'system birth joint binding')
        state=self._initial(data);previous='0'*64;lastcount=0
        for i,e in enumerate(events):
            body={k:v for k,v in e.items() if k!='sha256'}
            need(e['index']==i and e['previous']==previous and e['sha256']==_hash(body),'system event chain')
            count=e['source_count'];need(count>=lastcount,'system source regression')
            cognition,features,joint_sha=historical[count];need(e['source_joint_sha256']==joint_sha,'system source joint binding')
            kind=e['kind'];p=e['payload']
            if count>lastcount:
                need(kind=='SYNC' and count==lastcount+1 and state['approved'] is not None,'only approved single capture may advance source')
                need(cognition['sources'][-1]['request']==state['approved']['request'],'capture must match sole approved request')
            state['goals']=goals.apply_cognition(state['goals'],cognition)
            if kind=='SYNC':
                need(p=={},'sync payload')
                if state['approved'] is not None:
                    rid=state['approved']['request']['request_id']
                    if any(s['request']['request_id']==rid for s in cognition['sources']):state['approved']=None
            elif kind=='ADD_GOAL':
                need(set(p)=={'id','kind','target','limit'},'add goal payload')
                state['goals']=goals.add_goal(state['goals'],p['id'],p['kind'],p['target'],p['limit'])
            elif kind=='TRANSITION':
                need(state['approved'] is None,'cannot alter goals with committed executor intent pending')
                need(set(p)=={'id','status','reason'},'transition payload')
                state['goals']=goals.transition(state['goals'],p['id'],p['status'],p['reason'],joint_sha)
            elif kind=='PROPOSE_LOCAL':
                q=actions.validate_request(p)
                need(state['approved'] is None,'executor intent pending')
                actions.submit(state['actions'],q,now=state['clock'],approved_action_id=q['action_id']) # validation only, not an outbox write
                state['goals']=goals.add_goal(state['goals'],q['goal_id'],'receive_reply' if q['action']!='WAIT' else 'resolve_query',q['request_id'],1)
                need(q['source_sha256']==joint_sha and q['source_version']==count,'local proposal source')
                need(q['identity']==self.actor['id'] and q['scope_ref']==self.actor['scope_ref'],'local proposal actor')
                need(len(state['proposals'])<32 and not any(x['request_id']==q['request_id'] for x in state['proposals']),'proposal capacity/id')
                need(any(g['id']==q['goal_id'] and g['target']==q['request_id'] for g in state['goals']['goals']),'local goal/request binding')
                state['proposals'].append(q)
            elif kind=='APPROVE':
                need(state['approved'] is None,'single executor')
                candidates,request=self._candidates(state,cognition,features)
                g,allocation=goals.allocate(state['goals'],candidates,budget=2)
                need(p==allocation and allocation['selected'] is not None,'exact governance approval replay')
                state['goals']=g;selected=allocation['selected']
                if request is not None and selected['action_id']==request['request_id']:
                    state['approved']={'request':request,'source_joint_sha256':joint_sha,'allocation':allocation}
                else:
                    q=next((q for q in state['proposals'] if q['action_id']==selected['action_id']),None)
                    need(q is not None and q['intent_key']==selected['intent_key'],'typed local approval binding')
                    # A delayed proposal must still refer to this exact snapshot.
                    need(q['source_sha256']==joint_sha and q['source_version']==count,'stale local proposal')
                    state['actions']=actions.submit(state['actions'],q,now=state['clock'],approved_action_id=selected['action_id'])
                    state['goals']=goals.transition(state['goals'],q['goal_id'],'waiting','queued_local_not_sent',joint_sha)
            elif kind=='POLL':
                need(type(p) is dict and set(p)=={'now'} and type(p['now']) is int and p['now']>=state['clock'],'monotonic logical time')
                state['actions']=actions.poll(state['actions'],now=p['now']);state['clock']=p['now']
            elif kind=='RESPONSE':
                need(type(p) is dict and set(p)=={'response','now'} and type(p['now']) is int and p['now']>=state['clock'],'response time')
                state['actions']=actions.respond(state['actions'],p['response'],now=p['now']);state['clock']=p['now']
            else:need(False,'unknown system event')
            state['goals']=goals.apply_actions(state['goals'],state['actions'])
            for proposal in state['proposals']:
                if proposal['request_id'] in state['actions']['requests']:continue
                goal=next(g for g in state['goals']['goals'] if g['id']==proposal['goal_id'])
                if goal['status'] not in {'active','waiting'}:continue
                reason=('proposal_deadline_elapsed' if proposal['deadline']<=state['clock'] else
                        'proposal_source_stale' if proposal['source_version']!=count else None)
                if reason:state['goals']=goals.transition(state['goals'],goal['id'],'blocked',reason,joint_sha)
            previous=e['sha256'];lastcount=count
        return state

    def _load_system(self):
        data,_=OrderedFolderSession._read(self.system_path,MAX_BYTES)
        self.system_state=self._replay_system(data);self.system=data

    def _append_system(self,kind,payload):
        data=copy.deepcopy(self.system);events=data['events'];need(len(events)<MAX_EVENTS,'system event capacity')
        event=dict(index=len(events),source_count=len(self.bindings),source_joint_sha256=self.joint_sha,kind=kind,payload=copy.deepcopy(payload),previous=events[-1]['sha256'] if events else '0'*64)
        event['sha256']=_hash(event);events.append(event)
        state=self._replay_system(data)
        try:self._save_system(data)
        except Exception:
            self._load_system();raise
        self.system=data;self.system_state=state

    def _sync_system(self):
        self._catch_up_locked();self._load_policy();self._load_system()
        previous=self.system['events'][-1]['source_count'] if self.system['events'] else 0
        if previous<len(self.bindings):self._append_system('SYNC',{})

    def _plan(self):
        approved=self.system_state['approved'];need(approved is not None,'no governance approval')
        need(approved['source_joint_sha256']==self.joint_sha,'approved source current')
        exact=FolderAgentRunner._plan(self)
        need(exact==approved['request'],'folder executor must match approved request')
        return exact

    def _step_locked(self):
        self._sync_system()
        if self.system_state['approved'] is None:
            candidates,request=self._candidates(self.system_state,self.cognition,self.solver.manifest()['features'])
            _,allocation=goals.allocate(self.system_state['goals'],candidates,budget=2)
            if allocation['selected'] is None:
                candidate,_=self._folder_candidate(self.system_state,self.cognition,self.solver.manifest()['features'])
                if candidate is None:
                    g=next((g for g in self.system_state['goals']['goals'] if g['id']=='exploration' and g['status']=='active'),None)
                    if g is not None:self._append_system('TRANSITION',dict(id='exploration',status='waiting',reason='no_eligible_unread_observation'))
                return False
            selected=allocation['selected']
            if request is not None and selected['action_id']==request['request_id']:
                need(FolderAgentRunner._plan(self)==request,'prepared policy request matches allocation')
            self._append_system('APPROVE',allocation)
            if self.system_state['approved'] is None:return True # durable local outbox, no send
        result=super()._step_locked();self._sync_system();return result

    def add_goal(self,goal_id,kind,target=None,limit=1):
        with self._joint_lock():
            self._sync_system();self._append_system('ADD_GOAL',dict(id=goal_id,kind=kind,target=target,limit=limit))

    def transition_goal(self,goal_id,status,reason):
        with self._joint_lock():
            self._sync_system();self._append_system('TRANSITION',dict(id=goal_id,status=status,reason=reason))

    def request_local(self,action,*,goal_id,recipient=None,query,deadline):
        with self._joint_lock():
            self._sync_system();s=self.system_state
            rid='local_'+_hash([self.actor['id'],goal_id,action,recipient,query,self.joint_sha,len(s['proposals'])])[:32]
            q=actions.make_request(self.actor['id'],self.actor['scope_ref'],action,rid,self.joint_sha,goal_id=goal_id,source_version=len(self.bindings),deadline=deadline,recipient=recipient,query=query)
            need(s['clock']<deadline<=s['clock']+1024,'logical deadline')
            # A goal names a particular requested delivery, not query truth.
            self._append_system('PROPOSE_LOCAL',q)
            return copy.deepcopy(q)

    def poll(self,now):
        with self._joint_lock():
            self._sync_system();need(type(now) is int and now>=self.system_state['clock'],'logical time cannot regress')
            updated=actions.poll(self.system_state['actions'],now=now)
            # Persist the supplied logical watermark separately from life progress.
            # Repeating the same watermark is byte-identical; this is not wall time.
            if updated!=self.system_state['actions'] or now!=self.system_state['clock']:self._append_system('POLL',{'now':now})
            return actions.eligibility(self.system_state['actions'],now=now)

    def admit_response(self,response,*,now):
        with self._joint_lock():
            self._sync_system();self._append_system('RESPONSE',dict(response=response,now=now))

    def report(self):
        with self._joint_lock():
            self._load_joint();self._load_policy();self._load_system();result=super()._report_locked()
            result.update(system_schema=SCHEMA,system=copy.deepcopy(self.system_state),
                system_event_sha256=self.system['events'][-1]['sha256'] if self.system['events'] else None,
                local_transport='NONE: queued controller values are not sent or delivered',teacher_enabled=False,
                system_scope='operational receipt goals and one governed executor; not full blueprint baseline')
            return result
