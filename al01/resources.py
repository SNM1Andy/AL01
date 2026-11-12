"""Read-only, exact capacity accounting for the bounded durable architecture.

Headroom is not a promise that another action fits: message/result sizes vary.
No quotas are raised, histories discarded, or interrupted actions retried.
"""
import json
from pathlib import Path
from .core import Individual,fail
from .durable import DurableRunner
from .dispatch import DispatchJournal,MAX_ACTIONS,MAX_JOURNAL_BYTES
from .limits import MAX_EVENTS,MAX_LOG_BYTES,MAX_MESSAGE_BYTES,MAX_CHECKPOINT_BYTES,MAX_MESSAGES

def inspect_run(directory):
    directory=Path(directory)
    if not directory.is_dir() or not (directory/'checkpoint.json').is_file():
        fail('CHECKPOINT_INVALID','resource inspection requires an existing run checkpoint')
    # Temporary runtime lock provides a consistent read; no history/state is changed.
    runner=DurableRunner(directory)
    with runner._exclusive():
        runner.individual=Individual.load(runner.checkpoint);runner._check_association()
        s=runner.individual.state;j=DispatchJournal(runner.journal_path)
        def capacity(used,limit):return {'used':used,'limit':limit,'remaining':max(0,limit-used)}
        return {'schema':'al01-resource-status-1','individual_id':s['id'],'tick':s['tick'],
                'goal_status':s['goal_state']['status'],
                'capacity':{'event_count':capacity(len(s['events']),MAX_EVENTS),
                            'event_bytes':capacity(s['event_bytes'],MAX_LOG_BYTES),
                            'message_count':capacity(len(s['outbox'])+len(s['delivered']),MAX_MESSAGES),
                            'packet_history_count':capacity(len(s['packet_status']),10000),
                            'task_history_count':capacity(len(s['tasks'])+len(s['task_results']),10000),
                            'requested_action_count':capacity(len(s['requested_actions']),1),
                            'inbox_count':capacity(len(s['inbox']),32),
                            'outbox_count':capacity(len(s['outbox']),64),
                            'pending_task_count':capacity(len(s['tasks']),32),
                            'message_bytes':capacity(s['message_bytes'],MAX_MESSAGE_BYTES),
                            'checkpoint_bytes':capacity(runner.checkpoint.stat().st_size,MAX_CHECKPOINT_BYTES),
                            'action_count':capacity(len(j.records),MAX_ACTIONS),
                            'action_journal_bytes':capacity(runner.journal_path.stat().st_size if runner.journal_path.exists() else 0,MAX_JOURNAL_BYTES)},
                'unknown_actions':sum(v['status']=='UNKNOWN' for v in j.records.values()),
                'automatic_history_pruning':False,'next_action_fit_guaranteed':False,
                'resume_requires':['checkpoint.json','actions.json when action receipts exist'],
                'segmented_copy_replaces_runtime_history':False}

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(description='Read-only accounting; does not increase budgets')
    p.add_argument('run_directory',type=Path);a=p.parse_args();print(json.dumps(inspect_run(a.run_directory),indent=2))
