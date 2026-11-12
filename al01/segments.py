"""Bounded immutable event segments with an atomic evidence manifest.

The authoritative runtime checkpoint retains full history. This sidecar preserves
absolute event references and verifies/catches up a checkpoint prefix; it does not
prune memory, reset budgets, or reconstruct cognitive state from events alone.
"""
import hashlib,json,os,re,tempfile
from contextlib import contextmanager
from pathlib import Path
from .core import Individual,canonical,digest,fail
from .durable import DurableRunner
from .limits import MAX_EVENTS,MAX_LOG_BYTES

MAX_STORE_BYTES=32*1024*1024
MAX_SEGMENT_BYTES=2*1024*1024
MAX_SEGMENTS=4096
MAX_FILES=8192
CHUNK_EVENTS=128

class EventSegments:
    def __init__(self,directory):
        self.directory=Path(directory).resolve()
        self.directory.mkdir(parents=True,exist_ok=True)
        self.segment_dir=self.directory/'segments';self.manifest_path=self.directory/'manifest.json';self.lock_path=self.directory/'store.lock'
        for p in self.directory.iterdir():
            if p.name not in ('segments','manifest.json','store.lock') or p.is_symlink():fail('SEGMENT_INVALID','unexpected store entry')
        self.segment_dir.mkdir(exist_ok=True)
    @contextmanager
    def locked(self):
        try:fd=os.open(self.lock_path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        except FileExistsError:fail('SEGMENT_BUSY','store lock exists; diagnose before manual recovery')
        try:yield
        finally:os.close(fd);self.lock_path.unlink()
    def _inventory(self):
        count=0;size=0
        for p in self.segment_dir.iterdir():
            if p.is_symlink() or not p.is_file() or not re.fullmatch(r'(?:\.pending-)?[0-9]{5}-[0-9]{5}-[0-9a-f]{64}\.jsonl',p.name):fail('SEGMENT_INVALID','unexpected segment entry')
            count+=1;size+=p.stat().st_size
            if count>MAX_FILES or size>MAX_STORE_BYTES:fail('BUDGET_EXCEEDED','segment store capacity')
        return count,size
    def _read(self):
        self._inventory()
        if not self.manifest_path.exists():return None,[]
        if self.manifest_path.stat().st_size>1024*1024:fail('SEGMENT_INVALID','manifest size')
        try:
            wrapper=json.loads(self.manifest_path.read_text(encoding='utf-8'));m=wrapper['data']
            if set(wrapper)!={'data','sha256'} or wrapper['sha256']!=digest(m):fail('SEGMENT_INVALID','manifest integrity')
            if not isinstance(m,dict) or set(m)!={'schema','individual_id','event_count','event_head','segments'} or m['schema']!='al01-event-segments-1':fail('SEGMENT_INVALID','manifest schema')
            if not isinstance(m['individual_id'],str) or not 1<=len(m['individual_id'])<=256 or type(m['event_count']) is not int or not 0<=m['event_count']<=MAX_EVENTS:fail('SEGMENT_INVALID','identity/count')
            if not isinstance(m['segments'],list) or len(m['segments'])>MAX_SEGMENTS:fail('SEGMENT_INVALID','segment count')
            events=[];previous='0'*64;total=0
            for entry in m['segments']:
                if not isinstance(entry,dict) or set(entry)!={'file','start','end','bytes','sha256'}:fail('SEGMENT_INVALID','segment fields')
                if not all(type(entry[k]) is int for k in ('start','end','bytes')) or entry['start']!=len(events) or not entry['start']<entry['end']<=MAX_EVENTS or entry['end']-entry['start']>CHUNK_EVENTS or not 0<entry['bytes']<=MAX_SEGMENT_BYTES:fail('SEGMENT_INVALID','segment range')
                if not isinstance(entry['sha256'],str) or not re.fullmatch('[0-9a-f]{64}',entry['sha256']):fail('SEGMENT_INVALID','segment hash')
                expected=f"{entry['start']:05d}-{entry['end']:05d}-{entry['sha256']}.jsonl"
                if entry['file']!=expected:fail('SEGMENT_INVALID','segment path')
                p=self.segment_dir/expected
                if p.is_symlink() or p.stat().st_size!=entry['bytes']:fail('SEGMENT_INVALID','segment size')
                raw=p.read_bytes()
                if hashlib.sha256(raw).hexdigest()!=entry['sha256']:fail('SEGMENT_INVALID','segment integrity')
                lines=raw.splitlines()
                if len(lines)!=entry['end']-entry['start']:fail('SEGMENT_INVALID','segment line count')
                for line in lines:
                    e=json.loads(line);total+=len(line)
                    if total>MAX_LOG_BYTES:fail('BUDGET_EXCEEDED','event evidence bytes')
                    if (not isinstance(e,dict) or set(e)!={'index','tick','kind','payload','previous','hash'} or type(e['index']) is not int or e['index']!=len(events)
                        or type(e['tick']) is not int or e['tick']!=len(events)+1 or e['previous']!=previous or e['hash']!=digest({k:v for k,v in e.items() if k!='hash'})):
                        fail('SEGMENT_INVALID','event chain')
                    previous=e['hash'];events.append(e)
            if len(events)!=m['event_count'] or previous!=m['event_head']:fail('SEGMENT_INVALID','manifest head/count')
            return m,events
        except (OSError,KeyError,TypeError,ValueError,UnicodeError,RecursionError,OverflowError):fail('SEGMENT_INVALID','malformed or missing evidence')
    def read_events(self):
        with self.locked():return self._read()[1]
    def _sync_directory(self,path):
        if os.name=='posix':
            fd=os.open(path,os.O_RDONLY|getattr(os,'O_DIRECTORY',0))
            try:os.fsync(fd)
            finally:os.close(fd)
    def _publish(self,manifest):
        raw=canonical({'data':manifest,'sha256':digest(manifest)}).encode()
        if len(raw)>1024*1024:fail('BUDGET_EXCEEDED','segment manifest bytes')
        fd,tmp=tempfile.mkstemp(prefix='manifest-',dir=self.directory)
        try:
            with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
            os.replace(tmp,self.manifest_path);self._sync_directory(self.directory)
        finally:
            if os.path.exists(tmp):os.unlink(tmp)
    def _validate_state(self,state):
        try:
            if not isinstance(state,dict) or not isinstance(state.get('id'),str) or not 1<=len(state['id'])<=256:fail('SEGMENT_INVALID','state identity')
            events=state.get('events')
            if not isinstance(events,list) or len(events)>MAX_EVENTS or type(state.get('tick')) is not int or state['tick']!=len(events):fail('SEGMENT_INVALID','state event count')
            previous='0'*64;size=0
            for index,e in enumerate(events):
                if (not isinstance(e,dict) or set(e)!={'index','tick','kind','payload','previous','hash'} or type(e['index']) is not int or e['index']!=index or type(e['tick']) is not int or e['tick']!=index+1
                    or e['previous']!=previous or e['hash']!=digest({k:v for k,v in e.items() if k!='hash'})):fail('SEGMENT_INVALID','state event chain')
                size+=len(canonical(e).encode())
                if size>MAX_LOG_BYTES:fail('BUDGET_EXCEEDED','state evidence bytes')
                previous=e['hash']
        except (ValueError,TypeError,KeyError,UnicodeError,RecursionError):fail('SEGMENT_INVALID','malformed state evidence')
    def sync_state(self,state):
        """Internal validated state only. Public sync_run loads/validates checkpoint."""
        self._validate_state(state)
        with self.locked():
            manifest,existing=self._read();events=state['events']
            if manifest is not None and manifest['individual_id']!=state['id']:fail('STATE_CONFLICT','segment owner differs')
            if events[:len(existing)]!=existing:fail('STATE_CONFLICT','checkpoint is behind or diverges from evidence')
            segments=[] if manifest is None else list(manifest['segments'])
            count,stored=self._inventory();index=len(existing)
            while index<len(events):
                start=index;parts=[];size=0
                while index<len(events) and len(parts)<CHUNK_EVENTS:
                    line=(canonical(events[index])+'\n').encode()
                    if len(line)>MAX_SEGMENT_BYTES:fail('BUDGET_EXCEEDED','single event segment size')
                    if parts and size+len(line)>MAX_SEGMENT_BYTES:break
                    parts.append(line);size+=len(line);index+=1
                raw=b''.join(parts);h=hashlib.sha256(raw).hexdigest();name=f'{start:05d}-{index:05d}-{h}.jsonl';p=self.segment_dir/name
                if len(segments)>=MAX_SEGMENTS:fail('BUDGET_EXCEEDED','segment manifest capacity')
                if p.exists():
                    if p.is_symlink() or p.read_bytes()!=raw:fail('SEGMENT_INVALID','existing immutable segment differs')
                    # Windows fsync requires a writable handle; no truncation or content write.
                    with p.open('r+b' if os.name=='nt' else 'rb') as f:os.fsync(f.fileno())
                else:
                    temporary=p.with_name('.pending-'+p.name)
                    if temporary.is_symlink():fail('SEGMENT_INVALID','pending segment symlink')
                    old_size=temporary.stat().st_size if temporary.exists() else 0
                    extra=0 if temporary.exists() else 1
                    if count+extra+1>MAX_FILES or stored-old_size+2*size>MAX_STORE_BYTES:fail('BUDGET_EXCEEDED','segment store capacity')
                    with temporary.open('wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
                    # Exclusive hard-link publication cannot replace an existing final segment.
                    os.link(temporary,p)
                    temporary.unlink()
                    count+=extra;stored+=size-old_size
                segments.append({'file':name,'start':start,'end':index,'bytes':size,'sha256':h})
            self._sync_directory(self.segment_dir)
            result={'schema':'al01-event-segments-1','individual_id':state['id'],'event_count':len(events),
                    'event_head':events[-1]['hash'] if events else '0'*64,'segments':segments}
            self._publish(result)
            return result

def sync_run(run_directory,store_directory):
    if Path(store_directory).resolve().is_relative_to(Path(run_directory).resolve()):fail('SEGMENT_INVALID','store must be outside runtime directory')
    runner=DurableRunner(run_directory)
    with runner._exclusive():
        runner.individual=Individual.load(runner.checkpoint);runner._check_association()
        return EventSegments(store_directory).sync_state(runner.individual.state)

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(description='Bounded immutable evidence sidecar; no runtime pruning')
    p.add_argument('operation',choices=('sync','verify'));p.add_argument('source',type=Path);p.add_argument('destination',type=Path,nargs='?');a=p.parse_args()
    if a.operation=='sync':
        if a.destination is None:p.error('destination required')
        result=sync_run(a.source,a.destination)
    else:result={'verified_events':len(EventSegments(a.source).read_events())}
    print(json.dumps(result,indent=2))
