"""Explicit offline migration. Never edits source or invents historical events."""
import copy,hashlib,json
from pathlib import Path
from .core import Individual,World,CVM,Compiler,VERSION,CLError,canonical,digest,fail
from .contracts import validate_snapshot
from .limits import MAX_CHECKPOINT_BYTES

SUPPORTED={f'CL-0.1-foundation-{n}' for n in range(1,14)}

def migrate_checkpoint(source,destination):
    source=Path(source); destination=Path(destination)
    if source.resolve()==destination.resolve(): fail('MIGRATION_INVALID','source must remain unchanged')
    if source.stat().st_size>MAX_CHECKPOINT_BYTES: fail('MIGRATION_INVALID','source size')
    raw=source.read_bytes()
    try:
        wrapper=json.loads(raw); old=wrapper['data']; source_version=old['cl_version']
        if source_version not in SUPPORTED: fail('MIGRATION_INVALID','unsupported source version')
        if old['schema']!=1 or digest(old)!=wrapper['sha256']: fail('MIGRATION_INVALID','source integrity')
        previous='0'*64
        for event in old['state']['events']:
            if event['previous']!=previous or digest({k:v for k,v in event.items() if k!='hash'})!=event['hash']: fail('MIGRATION_INVALID','source event chain')
            previous=event['hash']
        data=copy.deepcopy(old);state=data['state'];world=data['world']
        # Only missing fields are introduced. Existing values are never silently corrected.
        for key,value in {'requested_actions':[],'pending_action_id':None,'inbox':[],'packet_status':{},'outbox':[],'delivered':[],'tasks':[],'task_results':{},'position':[0,0],'visited':[[0,0]]}.items():
            state.setdefault(key,copy.deepcopy(value))
        world.setdefault('local',False)
        world.setdefault('width',max([v['position'][0]+1 for v in world['objects'].values()]+[1]))
        world.setdefault('height',max([v['position'][1]+1 for v in world['objects'].values()]+[1]))
        state['event_bytes']=sum(len(canonical(e).encode('utf8')) for e in state['events'])
        state['message_bytes']=sum(len(canonical(e).encode('utf8')) for e in state['outbox']+state['delivered'])
        data['cl_version']=VERSION
        data['migrations']=[{'source_version':source_version,'target_version':VERSION,
                            'source_sha256':hashlib.sha256(raw).hexdigest(),'source_event_head':previous,'tick':state['tick']}]
        if 'goal_state' not in state:
            from .goals import initial
            migration_world=World(0);migration_world.objects=world['objects'];migration_world.version=world['version']
            migration_world.local=world['local'];migration_world.width=world['width'];migration_world.height=world['height']
            state['goal_state']=initial(state,migration_world)
            state['goal_state']['reason']='migration_baseline'
        validate_snapshot(data)
        individual=Individual(World(0));individual.state=state;individual.vm=CVM(data['capabilities'])
        individual.world.objects=world['objects'];individual.world.version=world['version'];individual.world.local=world['local']
        individual.world.width=world['width'];individual.world.height=world['height'];individual.migrations=data['migrations']
        # Preserve pending source/IDs; recompile only queued programs under the current contract.
        # A pending program that cannot validate is a migration blocker, not silently discarded.
        handles=individual.handles()
        for packet in state['inbox']:
            caps=set().union(*(handles[h] for h in packet['capability_refs'])) if packet['capability_refs'] else set()
            packet['typed_body']['ir']=Compiler(individual.candidates(),caps,snapshot=state).compile(packet['typed_body']['source'])
            individual.vm.compile_packet(packet,state,individual.candidates(),handles,queued=True)
        individual.save(destination)
        return individual
    except CLError: raise
    except (KeyError,TypeError,ValueError,OverflowError,UnicodeError,RecursionError) as exc:
        fail('MIGRATION_INVALID',str(exc))

if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description='Explicit AL-01 v0.1-v0.11 to v0.12 checkpoint migration')
    parser.add_argument('source',type=Path);parser.add_argument('destination',type=Path)
    args=parser.parse_args();i=migrate_checkpoint(args.source,args.destination)
    print(json.dumps({'status':'migrated','tick':i.state['tick'],'destination':str(args.destination),'source_unchanged':True}))
