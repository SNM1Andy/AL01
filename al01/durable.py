"""Checkpointed sandbox world loop with durable dispatch receipts.

Only local simulation adapters. No OS/network actions. Every effective tick has
one checkpoint. A saved action receipt may precede that checkpoint; replay uses
its same action ID and recorded result, never invokes that completed action again.
"""
import copy,os
from contextlib import contextmanager
from pathlib import Path
from .core import Individual,digest,fail,CLError
from .dispatch import DispatchJournal

class WorldDispatcher:
    def __init__(self,world,journal):
        self.world=world;self.journal=journal
    def prepare_request(self,request):
        from .actions import validate_request,journal_payload
        validate_request(request)
        return self.journal.request(request['dedup_key'],'sandbox_'+request['proposal']['kind'],journal_payload(request))
    def execute_request(self,request,capabilities):
        from .actions import validate_request,validate_result,journal_payload
        validate_request(request);p=request['proposal'];operation='sandbox_'+p['kind']
        try:
            self.journal=DispatchJournal(self.journal.path)
            record=self.journal.records.get(request['action_id'])
            if record is None or record['operation']!=operation or record['payload']!=journal_payload(request):fail('DISPATCH_CONFLICT','prepared receipt missing or inconsistent')
            if record['status']=='REQUESTED':
                cap='observe' if p['kind']=='observe' else 'move'
                if cap not in capabilities:fail('DISPATCH_CAPABILITY_DENIED',cap)
                if request['world_version']!=self.world.version or request['world_digest']!=digest(self.world.objects):fail('DISPATCH_CONFLICT','requested world changed')
            record=self.journal.dispatch(request['action_id'],self._adapter)
        except CLError as exc:
            if exc.code=='BUDGET_EXCEEDED' or exc.code.startswith('DISPATCH_'):raise
            fail('DISPATCH_CONFLICT',exc.code)
        if record['status']!='SUCCEEDED':fail('DISPATCH_UNKNOWN','action result requires verified reconciliation; no replay')
        validate_result(request,record['result'])
        return copy.deepcopy(record['result'])
    def _adapter(self,operation,payload,action_id):
        p=payload['proposal']
        if operation=='sandbox_observe':return self.world.observe(p['target'])
        if operation=='sandbox_move':return self.world.movement(payload['position'],p['dx'],p['dy'])
        fail('CAPABILITY_DENIED','sandbox world only')

class DurableRunner:
    def __init__(self,directory,individual=None):
        self.directory=Path(directory).resolve();self.directory.mkdir(parents=True,exist_ok=True)
        self.checkpoint=self.directory/'checkpoint.json'
        self.journal_path=self.directory/'actions.json'
        self.lock_path=self.directory/'runner.lock'
        with self._exclusive():
            if individual is not None:
                if self.checkpoint.exists() or self.journal_path.exists():fail('STATE_CONFLICT','new run requires empty destination')
                if individual.state['requested_actions']:fail('STATE_CONFLICT','cannot import pending actions without their original journal')
                individual.save(self.checkpoint)
            elif not self.checkpoint.exists():fail('CHECKPOINT_INVALID','no durable run checkpoint')
            self.individual=Individual.load(self.checkpoint)
            self._check_association()
    @contextmanager
    def _exclusive(self):
        try:fd=os.open(self.lock_path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        except FileExistsError:fail('DISPATCH_BUSY','runner lock exists; diagnose before manual recovery')
        try:
            os.write(fd,str(os.getpid()).encode());yield
        finally:
            os.close(fd);self.lock_path.unlink()
    def _check_association(self):
        from .actions import validate_request,validate_result,journal_payload
        records=DispatchJournal(self.journal_path).records;linked=set();requests={};completed=set()
        queued={r['action_id']:r for r in self.individual.state['requested_actions']}
        for event in self.individual.state['events']:
            payload=event['payload']
            if event['kind']=='commit':
                request=payload.get('action_requested')
                if request is not None:
                    validate_request(request);action_id=request['action_id']
                    if payload.get('action_ids')!=[action_id] or payload.get('proposals')!=[request['proposal']] or request['requested_tick']!=event['tick'] or payload.get('movement') is not None:fail('STATE_CONFLICT','committed request differs')
                    if action_id in linked:fail('STATE_CONFLICT','duplicate requested action')
                    r=records.get(action_id)
                    if r is None or r['operation']!='sandbox_'+request['proposal']['kind'] or r['payload']!=journal_payload(request):fail('STATE_CONFLICT','request journal differs')
                    linked.add(action_id);requests[action_id]=request
                else:
                    # Historical v0.1-v0.11 inline commits remain readable, never rewritten.
                    for action_id in payload.get('action_ids',[]):
                        r=records.get(action_id)
                        if action_id in linked or r is None or r['status']!='SUCCEEDED' or payload['proposals']!=[r['payload']['proposal']]:fail('STATE_CONFLICT','legacy action receipt missing or inconsistent')
                        if r['operation']=='sandbox_move' and payload['movement']!=r['result']:fail('STATE_CONFLICT','legacy movement differs')
                        linked.add(action_id)
            if event['kind'] in ('feedback','action_result') and 'action_id' in payload:
                aid=payload['action_id'];r=records.get(aid)
                if aid not in linked or aid in completed or r is None or r['status']!='SUCCEEDED':fail('STATE_CONFLICT','result receipt missing or duplicate')
                result=payload['position'] if event['kind']=='action_result' else {k:v for k,v in payload.items() if k not in ('action_id','goal_state')}
                if r['result']!=result:fail('STATE_CONFLICT','result differs from receipt')
                if aid in requests:
                    validate_result(requests[aid],result)
                    expected_kind='feedback' if requests[aid]['proposal']['kind']=='observe' else 'action_result'
                    if event['kind']!=expected_kind:fail('STATE_CONFLICT','result event kind')
                completed.add(aid)
        if set(queued)!=set(requests)-completed:fail('STATE_CONFLICT','outstanding request mismatch')
        for aid,request in queued.items():
            if request!=requests[aid]:fail('STATE_CONFLICT','queued request differs from commit')
        action_id=self.individual.state['pending_action_id']
        if action_id is not None:
            r=records.get(action_id)
            if action_id not in linked or r is None or r['status']!='SUCCEEDED' or r['result']!=self.individual.state['pending'][0]:fail('STATE_CONFLICT','legacy pending feedback differs')
    def step(self):
        with self._exclusive():
            # Never overwrite newer checkpoints through an old runner instance.
            self.individual=Individual.load(self.checkpoint);self._check_association()
            self.individual.action_dispatcher=WorldDispatcher(self.individual.world,DispatchJournal(self.journal_path))
            try:
                changed=self.individual.step()
                if changed:
                    if self.individual.state['requested_actions']:
                        self.individual.action_dispatcher.prepare_request(self.individual.state['requested_actions'][0])
                    self.individual.save(self.checkpoint)
                    # Only a successfully saved cognitive request authorizes dispatch.
                    if self.individual.state['requested_actions']:
                        self.individual.action_dispatcher.execute_request(self.individual.state['requested_actions'][0],self.individual.vm.capabilities)
                return changed
            except Exception:
                # Durable journal may be ahead; it is not rolled back with memory.
                self.individual=Individual.load(self.checkpoint)
                raise
    def resume_goal(self):
        with self._exclusive():
            self.individual=Individual.load(self.checkpoint);self._check_association()
            self.individual.resume_goal();self.individual.save(self.checkpoint)
    def run(self,steps):
        if type(steps) is not int or not 0<=steps<=10000:fail('BUDGET_EXCEEDED','durable steps 0..10000')
        count=0
        for _ in range(steps):
            if not self.step():break
            count+=1
        return count

if __name__=='__main__':
    import argparse,json
    from .core import World
    p=argparse.ArgumentParser(description='Durable offline sandbox loop, no external actions')
    p.add_argument('--directory',required=True,type=Path)
    p.add_argument('--steps',type=int,default=20)
    p.add_argument('--new',action='store_true')
    p.add_argument('--objects',type=int,default=20)
    p.add_argument('--local',action='store_true')
    p.add_argument('--resume-goal',action='store_true',help='Explicitly reset no-progress stop; unknown actions remain blocked')
    a=p.parse_args()
    runner=DurableRunner(a.directory,Individual(World(a.objects,local=a.local)) if a.new else None)
    if a.resume_goal:runner.resume_goal()
    n=runner.run(a.steps)
    print(json.dumps({'effective_steps':n,'tick':runner.individual.state['tick'],
        'closed_loops':runner.individual.state['closed_loops'],'external_actions':0}))
