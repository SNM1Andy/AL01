"""Pure single-capture CGL continuation, not reward or environmental geometry.

Latest source is a bounded validated summary; full receipts remain in the
runtime's immutable evidence store. Standalone validation proves consistency,
not source authenticity or historical ancestry. The surrounding runtime owns
global receipt deduplication, first_seen provenance
and checkpoint ancestry. This module validates and advances one bounded state;
it never substitutes a truncated historical prefix for the old FolderEventDrive.
"""
import copy,hashlib,math
from dataclasses import asdict
import numpy as np
from .instinct import ReferenceCGL,EventDrive,DriveEvent,Geometry,Parameters,VERSION as SOLVER_VERSION
from .folder_contracts import validate_request,validate_result,STATUSES,REASONS
from .folder_session import _hash,_encode

SCHEMA='compact-folder-field-2'
DRIVE_VERSION='compact-single-capture-drive-2'
MAX_CHECKPOINT_BYTES=256*1024
DURATION=.05
MAX_PERCEPTIONS=20000 # five solver steps each, existing100000-step ceiling


def need(ok,message):
    if not ok:raise ValueError('COMPACT_FIELD_INVALID: '+message)


def matrix(value):
    need(type(value) is list and len(value)==64 and all(type(row) is list and len(row)==64 for row in value),'64x64 matrix')
    need(all(type(x) in (int,float) and math.isfinite(x) for row in value for x in row),'finite numeric matrix')
    return np.asarray(value,dtype=np.float64)


def validate_item(item):
    need(type(item) is dict and set(item)=={'request','capture','receipt_sha256'},'capture item')
    q=item['request'];c=item['capture'];validate_request(q)
    need(type(c) is dict and set(c)=={'schema','capture_origin','result'} and c['schema']=='folder-session-1','capture envelope')
    need(c['capture_origin'] in ('SYNTHETIC_TEST_CAPTURE','APPLICATION_BROKER_CAPTURE'),'capture origin')
    validate_result(c['result'],q);need(item['receipt_sha256']==_hash(c),'receipt hash')
    return copy.deepcopy(item)


def validate_summary(source):
    need(type(source) is dict and set(source)=={'request','request_sha256','receipt_sha256','capture_origin','status','reason','content_sha256','utf8_bytes','entry_count'},'bounded source summary fields')
    validate_request(source['request']);need(source['request_sha256']==_hash(source['request']),'request hash')
    for key in ('request_sha256','receipt_sha256'):
        h=source[key];need(type(h) is str and len(h)==64 and all(x in '0123456789abcdef' for x in h),'source hash')
    need(source['capture_origin'] in ('SYNTHETIC_TEST_CAPTURE','APPLICATION_BROKER_CAPTURE'),'source origin')
    need(type(source['status']) is str and source['status'] in STATUSES,'source status')
    need(source['reason'] is None if source['status']=='OK' else type(source['reason']) is str and source['reason'] in REASONS[source['status']],'source reason')
    need(type(source['utf8_bytes']) is int and 0<=source['utf8_bytes']<=65536 and type(source['entry_count']) is int and 0<=source['entry_count']<=32,'bounded source counts')
    isread=source['status']=='OK' and source['request']['operation']=='READ_TEXT'
    if isread:
        h=source['content_sha256'];need(type(h) is str and len(h)==64 and all(x in '0123456789abcdef' for x in h),'content hash')
    else:need(source['content_sha256'] is None and source['utf8_bytes']==0,'nonread content absent')
    if not(source['status']=='OK' and source['request']['operation']=='LIST_SCOPE'):need(source['entry_count']==0,'nonlist entries absent')
    return copy.deepcopy(source)


def summarize(item):
    item=validate_item(item);q=item['request'];r=item['capture']['result'];payload=r['payload']
    isread=r['status']=='OK' and q['operation']=='READ_TEXT'
    return validate_summary(dict(request=q,request_sha256=_hash(q),receipt_sha256=item['receipt_sha256'],capture_origin=item['capture']['capture_origin'],
        status=r['status'],reason=r['reason'],content_sha256=payload['content_sha256'] if isread else None,
        utf8_bytes=payload['utf8_bytes'] if isread else 0,entry_count=len(payload['entries']) if r['status']=='OK' and q['operation']=='LIST_SCOPE' else 0))


class CaptureDrive(EventDrive):
    version=DRIVE_VERSION
    def __init__(self,item=None,*,start=0.,perception_index=0,first_seen=False,_summary=False):
        need(type(start) in (int,float) and math.isfinite(start) and start>=0,'interval start')
        need(type(perception_index) is int and 0<=perception_index<=MAX_PERCEPTIONS and type(first_seen) is bool,'drive index/novelty')
        self.start=float(start);self.end=float(start)+DURATION if item is not None else float(start)
        self.source=None if item is None else validate_summary(item) if _summary else summarize(item)
        self.perception_index=perception_index;self.first_seen=first_seen
        need((item is None)==(perception_index==0),'birth/capture drive')
        self.tick_duration=DURATION;self.records=();self.source_head='0'*64;self.signal='zero_birth'
        if self.source is not None:
            source=self.source;q=source['request'];amp=0.;self.signal='neutral_status_no_reward'
            if source['status']=='OK' and q['operation']=='READ_TEXT':
                amp=.125*min(1.,source['utf8_bytes']/65536)+(.125 if first_seen else 0.)
                self.signal='read_byte_fraction_and_declared_first_seen'
            elif source['status']=='OK' and q['operation']=='LIST_SCOPE':
                amp=.1*source['entry_count']/32;self.signal='listed_entry_fraction'
            key=q['scope_ref']+':'+(q['entry_ref'] or 'scope');cell=hashlib.sha256(('compact-folder-cell-1:'+key).encode()).digest()
            self.source_head=source['receipt_sha256']
            self.records=(DriveEvent(self.start,self.end,perception_index,self.source_head,cell[0]%64,cell[1]%64,amp),)
        self.digest=_hash(self._data())
    def _data(self):return dict(version=self.version,start=self.start,end=self.end,perception_index=self.perception_index,first_seen=self.first_seen,
        source_head=self.source_head,source_summary_sha256=None if self.source is None else _hash(self.source),signal=self.signal,records=[asdict(r) for r in self.records],encoding='opaque_hash_cells_not_physical_positions',meaning='capture_statistics_not_reward_or_emotion')
    def manifest(self):return dict(self._data(),sha256=self.digest)
    def at(self,time):
        need(type(time) in (int,float) and self.start-1e-12<=time<=self.end+1e-12,'drive outside committed interval')
        return super().at(time)
    def boundaries(self,start,end):
        need(self.start-1e-12<=start<=end<=self.end+1e-12,'integration outside committed interval')
        return super().boundaries(start,end)


def pack(solver,*,index,parent,source,first_seen):
    real=solver.field.real.tolist();imag=solver.field.imag.tolist()
    result=dict(schema=SCHEMA,perception_index=index,parent_checkpoint_sha256=parent,
        geometry=solver.geometry.values.tolist(),manifest=solver.manifest(),real=real,imag=imag,
        field_sha256=_hash({'real':real,'imag':imag}),source=copy.deepcopy(source),first_seen=first_seen,drive=solver.drive.manifest())
    need(len(_encode(result))<=MAX_CHECKPOINT_BYTES,'field checkpoint byte budget')
    return result


def initial_field(seed=17):
    solver=ReferenceCGL(Geometry(np.zeros((64,64))),seed=seed,dt=.01,drive=CaptureDrive())
    return pack(solver,index=0,parent=None,source=None,first_seen=False)


def restore(checkpoint):
    c=checkpoint
    need(len(_encode(c))<=MAX_CHECKPOINT_BYTES,'field checkpoint byte budget')
    need(type(c) is dict and set(c)=={'schema','perception_index','parent_checkpoint_sha256','geometry','manifest','real','imag','field_sha256','source','first_seen','drive'} and c['schema']==SCHEMA,'checkpoint fields')
    index=c['perception_index'];need(type(index) is int and 0<=index<=MAX_PERCEPTIONS,'perception index')
    need(type(c['first_seen']) is bool,'novelty Boolean')
    parent=c['parent_checkpoint_sha256'];need((parent is None)==(index==0),'parent presence')
    if parent is not None:need(type(parent) is str and len(parent)==64 and all(x in '0123456789abcdef' for x in parent),'parent hash')
    geometry=matrix(c['geometry']);need(np.count_nonzero(geometry)==0,'fixed zero geometry profile')
    real=matrix(c['real']);imag=matrix(c['imag']);need(c['field_sha256']==_hash({'real':c['real'],'imag':c['imag']}),'field content hash')
    m=c['manifest'];need(type(m) is dict,'solver manifest')
    need(m.get('solver')==SOLVER_VERSION and m.get('numpy')==np.__version__ and m.get('parameters')==asdict(Parameters()) and m.get('method')=='rk4' and type(m.get('dt')) is float and m['dt']==.01 and m.get('amplitude_limit')==10.,'solver/config profile')
    need(type(m.get('steps')) is int and m['steps']==5*index and type(m.get('time')) in (int,float) and math.isfinite(m['time']) and abs(m['time']-index*DURATION)<1e-8,'global field clock')
    need(m.get('stopped') is False and m.get('stop_reason') is None,'stopped field cannot resume')
    if index==0:
        need(c['source'] is None and c['first_seen'] is False and m['time']==0.,'birth source')
        drive=CaptureDrive()
    else:
        source=validate_summary(c['source']);d=c['drive'];need(type(d) is dict and type(d.get('start')) in (int,float),'drive interval')
        drive=CaptureDrive(source,start=d['start'],perception_index=index,first_seen=c['first_seen'],_summary=True)
        need(abs(drive.end-m['time'])<1e-10 and abs(drive.start-(index-1)*DURATION)<1e-8,'latest causal interval')
    need(_encode(drive.manifest())==_encode(c['drive']),'drive/source replay')
    solver=ReferenceCGL(Geometry(geometry),seed=m.get('seed'),dt=.01,drive=drive,initial=real+1j*imag)
    if index==0:
        born=ReferenceCGL(Geometry(geometry),seed=m.get('seed'),dt=.01,drive=CaptureDrive())
        need(np.array_equal(solver.field,born.field),'birth field must match declared initialization')
    solver.time=m['time'];solver.steps=m['steps']
    need(_encode(solver.manifest())==_encode(m),'exact restored manifest')
    return solver


def validate_field(checkpoint):
    restore(checkpoint);return copy.deepcopy(checkpoint)


def advance_field(checkpoint,item,*,perception_index,first_seen):
    solver=restore(checkpoint);source=summarize(item)
    need(type(perception_index) is int and perception_index==checkpoint['perception_index']+1 and perception_index<=MAX_PERCEPTIONS,'next perception index required')
    need(type(first_seen) is bool,'explicit first_seen Boolean')
    if checkpoint['source'] is not None:
        need(source['request']['scope_ref']==checkpoint['source']['request']['scope_ref'],'source scope cannot change')
        need(source['capture_origin']==checkpoint['source']['capture_origin'],'capture origin cannot silently change')
        need(source['request']['request_id']!=checkpoint['source']['request']['request_id'],'duplicate latest capture request')
    solver.drive=CaptureDrive(source,start=solver.time,perception_index=perception_index,first_seen=first_seen,_summary=True)
    for _ in range(5):solver.step()
    out=pack(solver,index=perception_index,parent=_hash(checkpoint),source=source,first_seen=first_seen)
    validate_field(out);return out
