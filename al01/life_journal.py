"""Bounded active transition journal with immutable evidence/checkpoint segments.

Generic storage prototype, not cognitive maturity or a runtime migration. Caller
supplies trusted deterministic reducer code; persisted data is never executed.
"""
import copy,json,os,tempfile,uuid
from pathlib import Path
from contextlib import contextmanager
from .folder_session import _hash,_encode
from .controller_paths import check_file,check_directory

SCHEMA='life-journal-1'
SEGMENT_RECORDS=64
MAX_RECORD_BYTES=4096
MAX_STATE_BYTES=65536
MAX_PENDING=16
MAX_SEGMENTS=1024
MAX_FILE_BYTES=512*1024
ZERO='0'*64
MAX_ROOT_ENTRIES=4
MAX_SEAL_LEADING_FILES=1
MAX_STAGING_FILES=0 # interrupted staging requires diagnosis, never automatic GC


def need(ok,msg):
    if not ok:raise ValueError('LIFE_JOURNAL_INVALID: '+msg)


def bounded(value,limit):
    raw=_encode(value);need(len(raw)<=limit,'JSON byte budget');return raw


def bounded_entries(directory,limit):
    # Path.iterdir may eagerly call listdir. Scan at most one overflow name,
    # close the directory handle on success, rejection or partial consumption.
    with os.scandir(directory) as entries:
        for index,entry in enumerate(entries):
            need(index<limit,'directory entry budget exceeded')
            yield directory/entry.name


class LifeJournal:
    def __init__(self,directory,*,reducer,reducer_id,identity=None,initial_state=None):
        self.directory=Path(os.path.abspath(directory));check_directory(self.directory)
        self.reducer=reducer;self.reducer_id=reducer_id
        need(type(reducer_id) is str and 0<len(reducer_id)<=128,'reducer identity')
        self.birth_path=self.directory/'birth.json';self.head_path=self.directory/'head.json'
        self.segments=self.directory/'segments';self.lock_path=self.directory/'journal.lock'
        if self.directory.exists():
            count=0
            for p in bounded_entries(self.directory,MAX_ROOT_ENTRIES):
                count+=1
                need(p.name in {'birth.json','head.json','segments','journal.lock'},'unexpected root file or staging remnant')
            if count:need(self.birth_path.exists() and self.head_path.exists() and self.segments.is_dir(),'incomplete birth; no reset')
        self.directory.mkdir(parents=True,exist_ok=True);check_directory(self.segments);self.segments.mkdir(exist_ok=True)
        with self.locked():
            if not self.birth_path.exists():
                need(type(identity) is str and 0<len(identity)<=128 and initial_state is not None,'explicit fresh identity/state required')
                bounded(initial_state,MAX_STATE_BYTES)
                birth=dict(schema=SCHEMA,identity=identity,instance_id=uuid.uuid4().hex,storage_location=str(self.directory),reducer_id=reducer_id,initial_state=copy.deepcopy(initial_state))
                self._write(self.birth_path,birth,immutable=True)
                head=dict(schema=SCHEMA,birth_sha256=_hash(birth),seq=0,perception_count=0,event_head=ZERO,
                          segment_count=0,segment_head=ZERO,state=copy.deepcopy(initial_state),pending={},active=[])
                self._write(self.head_path,head)
            self.birth=self._read(self.birth_path)
            need(set(self.birth)=={'schema','identity','instance_id','storage_location','reducer_id','initial_state'} and self.birth['schema']==SCHEMA,'birth fields')
            need(self.birth['storage_location']==str(self.directory),'relocated/copied instance requires explicit migration or fork protocol')
            need(type(self.birth['identity']) is str and 0<len(self.birth['identity'])<=128,'birth identity')
            need(type(self.birth['instance_id']) is str and len(self.birth['instance_id'])==32 and all(c in '0123456789abcdef' for c in self.birth['instance_id']),'instance identity')
            bounded(self.birth['initial_state'],MAX_STATE_BYTES)
            need(self.birth['reducer_id']==reducer_id and (identity is None or identity==self.birth['identity']),'identity/reducer mismatch')
            need(initial_state is None or initial_state==self.birth['initial_state'],'initial state mismatch')
            self.identity=self.birth['identity'];self._recover()

    @contextmanager
    def locked(self):
        check_directory(self.directory);check_file(self.lock_path)
        fd=os.open(self.lock_path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        try:yield
        finally:os.close(fd);self.lock_path.unlink()

    @staticmethod
    def _sync(path):
        if os.name=='posix':
            fd=os.open(path,os.O_RDONLY|getattr(os,'O_DIRECTORY',0))
            try:os.fsync(fd)
            finally:os.close(fd)

    def _write(self,path,data,immutable=False):
        check_file(path);raw=bounded({'data':data,'sha256':_hash(data)},MAX_FILE_BYTES)
        if immutable and path.exists():
            need(self._read(path)==data,'immutable segment conflict');return
        fd,name=tempfile.mkstemp(prefix='.journal-',dir=path.parent)
        try:
            with os.fdopen(fd,'wb') as f:
                need(f.write(raw)==len(raw),'short write');f.flush();os.fsync(f.fileno())
            if immutable:
                # Exclusive publication refuses an existing immutable name.
                os.link(name,path);os.unlink(name)
            else:os.replace(name,path)
            self._sync(path.parent)
        finally:
            if os.path.exists(name):os.unlink(name)

    @staticmethod
    def _read(path):
        check_file(path);need(path.exists() and path.stat().st_size<=MAX_FILE_BYTES,'missing/oversize file')
        wrapper=json.loads(path.read_bytes())
        need(type(wrapper) is dict and set(wrapper)=={'data','sha256'} and _hash(wrapper['data'])==wrapper['sha256'],'file integrity')
        return wrapper['data']

    def _segment_path(self,index):return self.segments/f'{index:06d}.json'

    def _zero(self):return dict(seq=0,perception_count=0,event_head=ZERO,state=copy.deepcopy(self.birth['initial_state']),pending={})

    def _apply(self,base,event):
        need(type(event) is dict and set(event)=={'schema','identity','instance_id','seq','perception_count','kind','payload','effect','previous','sha256'},'event fields')
        bounded(event,MAX_RECORD_BYTES)
        body={k:v for k,v in event.items() if k!='sha256'}
        need(event['schema']==SCHEMA and event['identity']==self.identity and event['instance_id']==self.birth['instance_id'] and type(event['seq']) is int and event['seq']==base['seq']+1,'global transition sequence')
        need(event['previous']==base['event_head'] and event['sha256']==_hash(body),'event chain')
        need(type(event['perception_count']) is int and base['perception_count']<=event['perception_count']<=base['perception_count']+1,'perception clock')
        need(type(event['kind']) is str and 0<len(event['kind'])<=64,'transition kind')
        out=copy.deepcopy(base);effect=event['effect']
        if effect is not None:
            need(type(effect) is dict and set(effect)=={'phase','id'},'effect fields')
            phase=effect['phase'];eid=effect['id']
            if phase=='REQUEST':
                need(eid==self.identity+':'+str(event['seq']) and len(out['pending'])<MAX_PENDING,'new sequenced effect/capacity')
                out['pending'][eid]=dict(status='REQUESTED',request_seq=event['seq'],request_sha256=event['sha256'],request=copy.deepcopy(event['payload']))
            else:
                need(eid in out['pending'],'effect requires retained pending request')
                if phase=='START':
                    need(out['pending'][eid]['status']=='REQUESTED','unknown effects cannot be restarted')
                    out['pending'][eid]['status']='UNKNOWN'
                elif phase=='RECEIPT':
                    need(out['pending'][eid]['status']=='UNKNOWN','receipt requires started effect')
                    del out['pending'][eid]
                else:need(False,'effect phase')
        result=self.reducer(copy.deepcopy(base['state']),copy.deepcopy(event))
        bounded(result,MAX_STATE_BYTES)
        out.update(seq=event['seq'],perception_count=event['perception_count'],event_head=event['sha256'],state=result)
        return out

    def _segment(self,index,parent,base,events):
        return dict(schema=SCHEMA,identity=self.identity,birth_sha256=_hash(self.birth),index=index,
                    previous_segment=parent,start=base['seq']-len(events)+1,end=base['seq'],events=copy.deepcopy(events),anchor=copy.deepcopy(base))

    def _validate_segment(self,segment,index,parent,base):
        need(type(segment) is dict and set(segment)=={'schema','identity','birth_sha256','index','previous_segment','start','end','events','anchor'},'segment fields')
        need(segment['schema']==SCHEMA and segment['identity']==self.identity and segment['birth_sha256']==_hash(self.birth)
             and segment['index']==index and segment['previous_segment']==parent,'segment identity/chain')
        events=segment['events'];need(type(events) is list and len(events)==SEGMENT_RECORDS,'sealed segment length')
        need(segment['start']==base['seq']+1 and segment['end']==base['seq']+SEGMENT_RECORDS,'segment range')
        for event in events:base=self._apply(base,event)
        need(base==segment['anchor'],'checkpoint anchor replay');return base

    def _recover(self):
        head=self._read(self.head_path)
        need(type(head) is dict and set(head)=={'schema','birth_sha256','seq','perception_count','event_head','segment_count','segment_head','state','pending','active'},'head fields')
        need(head['schema']==SCHEMA and head['birth_sha256']==_hash(self.birth),'head birth binding')
        n=head['segment_count'];need(type(n) is int and 0<=n<=MAX_SEGMENTS,'segment capacity')
        check_directory(self.segments)
        count=0
        for p in bounded_entries(self.segments,min(n+MAX_SEAL_LEADING_FILES,MAX_SEGMENTS+MAX_SEAL_LEADING_FILES)):
            check_file(p);need(p.name.endswith('.json') and len(p.name)==11 and p.name[:6].isdigit(),'unexpected archive file')
            need(0<=int(p.name[:6])<=n,'unknown archive suffix');count+=1;need(count<=MAX_SEGMENTS+1,'archive file budget')
        base=self._zero();parent=ZERO
        for index in range(n):
            segment=self._read(self._segment_path(index));base=self._validate_segment(segment,index,parent,base);parent=_hash(segment)
        need(parent==head['segment_head'],'archive head anchor')
        active=head['active'];need(type(active) is list and len(active)<=SEGMENT_RECORDS,'bounded active records')
        for event in active:base=self._apply(base,event)
        need(all(head[k]==base[k] for k in base),'active checkpoint replay')
        # A sealed file may lead rebasing only if it is exactly the already
        # committed full active tail. It never invents an uncommitted transition.
        extra=self._segment_path(n)
        if extra.exists():
            need(len(active)==SEGMENT_RECORDS,'orphan archive')
            need(self._read(extra)==self._segment(n,parent,base,active),'orphan archive differs from committed tail')
        self.head=head;self.head_sha=_hash(head)

    def snapshot(self):return copy.deepcopy(self.head)

    def append(self,kind,payload,*,perception_count=None,effect=None):
        with self.locked():
            need(_hash(self._read(self.head_path))==self.head_sha,'stale writer; reopen instead of fork')
            try:
                if len(self.head['active'])==SEGMENT_RECORDS:
                    need(self.head['segment_count']<MAX_SEGMENTS,'archive capacity reached')
                    base={k:copy.deepcopy(self.head[k]) for k in ('seq','perception_count','event_head','state','pending')}
                    segment=self._segment(self.head['segment_count'],self.head['segment_head'],base,self.head['active'])
                    self._write(self._segment_path(self.head['segment_count']),segment,immutable=True)
                    rebased=copy.deepcopy(self.head);rebased.update(segment_count=self.head['segment_count']+1,segment_head=_hash(segment),active=[])
                    self._write(self.head_path,rebased);self.head=rebased;self.head_sha=_hash(rebased)
                seq=self.head['seq']+1
                if effect is not None:
                    need(type(effect) is dict,'effect descriptor')
                    effect=copy.deepcopy(effect)
                    if effect.get('phase')=='REQUEST':
                        need(set(effect)=={'phase'},'request effect ID is assigned from global sequence')
                        effect['id']=self.identity+':'+str(seq)
                event=dict(schema=SCHEMA,identity=self.identity,instance_id=self.birth['instance_id'],seq=seq,
                           perception_count=self.head['perception_count'] if perception_count is None else perception_count,
                           kind=kind,payload=copy.deepcopy(payload),effect=effect,previous=self.head['event_head'])
                event['sha256']=_hash(event)
                base={k:copy.deepcopy(self.head[k]) for k in ('seq','perception_count','event_head','state','pending')}
                base=self._apply(base,event);new=copy.deepcopy(self.head);new.update(base);new['active'].append(event)
                self._write(self.head_path,new);self.head=new;self.head_sha=_hash(new)
                return copy.deepcopy(event)
            except Exception:
                self._recover();raise

    def iter_events(self):
        """Streaming verified history under one lock; at most one segment resident."""
        with self.locked():
            need(_hash(self._read(self.head_path))==self.head_sha,'stale reader; reopen')
            self._recover() # validate complete archive endpoint before yielding evidence
            base=self._zero();parent=ZERO
            for index in range(self.head['segment_count']):
                segment=self._read(self._segment_path(index));base=self._validate_segment(segment,index,parent,base);parent=_hash(segment)
                for event in segment['events']:yield copy.deepcopy(event)
            need(parent==self.head['segment_head'],'archive endpoint')
            for event in self.head['active']:
                base=self._apply(base,event);yield copy.deepcopy(event)
            need(all(base[k]==self.head[k] for k in base),'stream checkpoint endpoint')

    def lookup(self,seq):
        need(type(seq) is int and 1<=seq<=self.head['seq'],'global evidence reference')
        found=None
        # Finish validation through the trusted endpoint before returning a hit.
        for event in self.iter_events():
            if event['seq']==seq:found=event
        need(found is not None,'missing evidence');return found
