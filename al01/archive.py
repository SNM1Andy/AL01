"""Verified complete-run archives. No history pruning or budget reset.

Archives contain data only, never code. Restore is restricted to a new directory.
This is backup/relocation, not log compaction or an infinite-run mechanism.
"""
import hashlib,json,os,tempfile,zipfile
from pathlib import Path
from .core import Individual,canonical,digest,fail
from .durable import DurableRunner
from .dispatch import DispatchJournal

FILES={'checkpoint.json','actions.json','events.jsonl','manifest.json'}
MAX_TOTAL=48*1024*1024
MAX_FILE={'checkpoint.json':32*1024*1024,'actions.json':1024*1024,'events.jsonl':12*1024*1024,'manifest.json':65536}

def sha(raw):return hashlib.sha256(raw).hexdigest()

def archive_run(directory,destination):
    destination=Path(destination)
    if destination.resolve().is_relative_to(Path(directory).resolve()):fail('ARCHIVE_CONFLICT','archive must be outside the live run directory')
    if destination.exists():fail('ARCHIVE_CONFLICT','destination already exists')
    runner=DurableRunner(directory)
    with runner._exclusive():
        runner.individual=Individual.load(runner.checkpoint);runner._check_association()
        checkpoint=runner.checkpoint.read_bytes()
        if runner.journal_path.exists():actions=runner.journal_path.read_bytes()
        else:
            data={'schema':'dispatch-0.1','records':{}}
            actions=canonical({'data':data,'sha256':digest(data)}).encode()
        events=''.join(canonical(e)+'\n' for e in runner.individual.state['events']).encode()
        blobs={'checkpoint.json':checkpoint,'actions.json':actions,'events.jsonl':events}
        manifest={'schema':'al01-run-archive-1','cl_version':runner.individual.snapshot()['cl_version'],
                  'individual_id':runner.individual.state['id'],'tick':runner.individual.state['tick'],
                  'files':{k:{'bytes':len(v),'sha256':sha(v)} for k,v in blobs.items()},
                  'history_pruned':False,'budgets_reset':False}
        blobs['manifest.json']=canonical(manifest).encode()
    if any(len(v)>MAX_FILE[k] for k,v in blobs.items()):fail('BUDGET_EXCEEDED','archive file size')
    destination.parent.mkdir(parents=True,exist_ok=True)
    # Exclusive creation never overwrites a prior archive. Incomplete new output
    # is retained on write failure and will fail validation; never claim it complete.
    try:
        with destination.open('xb') as stream:
            with zipfile.ZipFile(stream,'w',zipfile.ZIP_DEFLATED) as z:
                for name,data in blobs.items():z.writestr(name,data)
            stream.flush();os.fsync(stream.fileno())
    except FileExistsError:fail('ARCHIVE_CONFLICT','destination already exists')
    return manifest

def read_archive(path):
    try:
        if Path(path).stat().st_size>MAX_TOTAL:fail('ARCHIVE_INVALID','compressed size')
        with zipfile.ZipFile(path) as z:
            entries=z.infolist()
            if len(entries)!=4 or {i.filename for i in entries}!=FILES:fail('ARCHIVE_INVALID','member names/count')
            if any(i.flag_bits&1 or i.file_size>MAX_FILE[i.filename] for i in entries):fail('ARCHIVE_INVALID','encrypted/oversize member')
            if sum(i.file_size for i in entries)>MAX_TOTAL:fail('ARCHIVE_INVALID','expanded size')
            blobs={i.filename:z.read(i) for i in entries}
        m=json.loads(blobs['manifest.json'])
        if not isinstance(m,dict) or set(m)!={'schema','cl_version','individual_id','tick','files','history_pruned','budgets_reset'} or m['schema']!='al01-run-archive-1':fail('ARCHIVE_INVALID','manifest schema')
        if m['history_pruned'] is not False or m['budgets_reset'] is not False or set(m['files'])!=FILES-{'manifest.json'}:fail('ARCHIVE_INVALID','manifest scope')
        for name,meta in m['files'].items():
            if meta!={'bytes':len(blobs[name]),'sha256':sha(blobs[name])}:fail('ARCHIVE_INVALID','member integrity')
        # Validate all cross-file relationships before touching the destination.
        with tempfile.TemporaryDirectory() as temp:
            d=Path(temp)
            for name in ('checkpoint.json','actions.json'):(d/name).write_bytes(blobs[name])
            r=DurableRunner(d)
            s=r.individual.state
            if m['cl_version']!=r.individual.snapshot()['cl_version'] or m['individual_id']!=s['id'] or type(m['tick']) is not int or m['tick']!=s['tick']:fail('ARCHIVE_INVALID','manifest identity/tick')
            expected=''.join(canonical(e)+'\n' for e in s['events']).encode()
            if blobs['events.jsonl']!=expected:fail('ARCHIVE_INVALID','event export differs')
        return blobs,m
    except (OSError,ValueError,TypeError,KeyError,zipfile.BadZipFile,UnicodeError,RuntimeError,RecursionError,OverflowError,AttributeError) as exc:
        fail('ARCHIVE_INVALID',str(exc))

def restore_archive(path,destination):
    blobs,manifest=read_archive(path)
    destination=Path(destination)
    if destination.exists():fail('ARCHIVE_CONFLICT','restore requires new directory')
    destination.parent.mkdir(parents=True,exist_ok=True)
    try:destination.mkdir()
    except FileExistsError:fail('ARCHIVE_CONFLICT','destination already exists')
    # Publish the journal before making a checkpoint visible. Otherwise a
    # partially restored old checkpoint could repeat an already completed action.
    def sync_directory():
        if os.name=='posix':
            fd=os.open(destination,os.O_RDONLY|getattr(os,'O_DIRECTORY',0))
            try:os.fsync(fd)
            finally:os.close(fd)
    with (destination/'actions.json').open('xb') as f:
        f.write(blobs['actions.json']);f.flush();os.fsync(f.fileno())
    sync_directory()
    temporary=destination/'.checkpoint.restore'
    with temporary.open('xb') as f:
        f.write(blobs['checkpoint.json']);f.flush();os.fsync(f.fileno())
    os.replace(temporary,destination/'checkpoint.json')
    sync_directory()
    return manifest

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(description='Verified full-run backup/restore; no compaction')
    p.add_argument('operation',choices=('create','verify','restore'));p.add_argument('source',type=Path);p.add_argument('destination',type=Path,nargs='?')
    a=p.parse_args()
    if a.operation!='verify' and a.destination is None:p.error('destination required')
    result=archive_run(a.source,a.destination) if a.operation=='create' else read_archive(a.source)[1] if a.operation=='verify' else restore_archive(a.source,a.destination)
    print(json.dumps(result,indent=2))
