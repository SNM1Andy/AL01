"""New, isolated CGL reference solver; NOT a port of an existing engine.

Blueprint v1.1 pp18–19 operational definitions. No cognitive integration,
learned semantics, strategy-effect claim, or subjective-experience claim.
"""
from dataclasses import dataclass, asdict
import hashlib
import json
import math
import numpy as np

VERSION='cgl-reference-1'
SHAPE=(64,64)
FEATURES=('energy','mean_amplitude','max_amplitude','min_amplitude','variance','spatial_gradient_energy')
MAX_STEPS=100000


class InstinctError(ValueError):pass


def _finite(value,name,low=-100.,high=100.):
    try:
        if type(value) not in (int,float) or not low<=value<=high or not math.isfinite(value):
            raise InstinctError(name)
    except OverflowError as exc:raise InstinctError(name) from exc
    return float(value)


def _array(value,complex_ok=False):
    try:
        source=np.asarray(value)
        if source.shape!=SHAPE or source.dtype.kind not in ('i','u','f','c') or (not complex_ok and np.iscomplexobj(source)):
            raise InstinctError('field shape/dtype')
        out=np.array(source,dtype=np.complex128 if complex_ok else np.float64,copy=True)
        if not np.isfinite(out).all():raise InstinctError('nonfinite field')
        return out
    except (TypeError,ValueError,OverflowError) as exc:raise InstinctError('invalid field') from exc


@dataclass(frozen=True)
class Parameters:
    mu:float=.1
    omega:float=.2
    D:float=.15
    B:float=.1
    g:float=1.
    c:float=.2
    lambda_:float=.15
    kappa:float=.1
    E_target:float=.25
    eta:float=.1
    def __post_init__(self):
        for name,value in asdict(self).items():_finite(value,name,0 if name in ('D','g','kappa','E_target') else -100)


class Geometry:
    """Centered, max-absolute normalized static geometry, identified by bytes hash."""
    version='center-maxabs-1'
    def __init__(self,values):
        raw=_array(values)
        if np.max(np.abs(raw))>1e6:raise InstinctError('geometry input magnitude')
        raw-=np.mean(raw);peak=float(np.max(np.abs(raw)))
        if peak:raw/=peak
        self._values=raw;self._values.flags.writeable=False
        self.digest=hashlib.sha256(self.version.encode()+raw.astype('<f8').tobytes()).hexdigest()
    @property
    def values(self):return self._values.copy()


def laplacian(field):
    """Periodic five-point Laplacian, dx=dy=1."""
    return sum(np.roll(field,offset,axis) for axis in (0,1) for offset in (-1,1))-4*field


def features(field):
    field=_array(field,True)
    try:
        with np.errstate(over='raise',invalid='raise'):
            a=np.abs(field)
            values=(np.mean(a*a),np.mean(a),np.max(a),np.min(a),np.mean((a-np.mean(a))**2),
                    np.mean(abs(np.roll(field,-1,0)-field)**2+abs(np.roll(field,-1,1)-field)**2))
        if not all(np.isfinite(v) for v in values):raise InstinctError('nonfinite features')
    except FloatingPointError as exc:raise InstinctError('feature overflow') from exc
    return dict(zip(FEATURES,map(float,values)))


@dataclass(frozen=True)
class DriveEvent:
    start:float
    end:float
    index:int
    content_hash:str
    x:int
    y:int
    amplitude:float


class EventDrive:
    """Verified actual-event encoding. Zero drive is permitted for numerical tests.

    Supply a complete hashed event history. Event tick * tick_duration defines
    simulation time; each encoded event acts for one tick_duration. Feedback
    uses tanh(value); actual movement results use amplitude 1. Positions map
    periodically to one grid cell. This engineering encoding is versioned,
    not inferred psychological meaning or proof of external authenticity.
    """
    version='actual-event-cell-1'
    def __init__(self,events=(),*,tick_duration=.05):
        from .core import canonical,digest,serializable
        from .limits import MAX_EVENTS,MAX_LOG_BYTES
        tick_duration=_finite(tick_duration,'tick duration',1e-6,100)
        if not isinstance(events,(list,tuple)) or len(events)>MAX_EVENTS:raise InstinctError('event count')
        previous='0'*64;size=0;records=[]
        try:
            for index,original in enumerate(events):
                event=serializable(original);size+=len(canonical(event).encode('utf8'))
                if size>MAX_LOG_BYTES:raise InstinctError('event bytes')
                if (not isinstance(event,dict) or set(event)!={'index','tick','kind','payload','previous','hash'}
                        or type(event['index']) is not int or event['index']!=index
                        or type(event['tick']) is not int or event['tick']!=index+1
                        or event['previous']!=previous or event['hash']!=digest({k:v for k,v in event.items() if k!='hash'})
                        or not isinstance(event['payload'],dict)):raise InstinctError('event chain')
                previous=event['hash'];p=event['payload'];kind=event['kind']
                if kind not in ('feedback','action_result'):continue
                if kind=='action_result' and p.get('kind')!='movement':raise InstinctError('movement event kind')
                position=p.get('position')
                if not isinstance(position,list) or len(position)!=2 or any(type(x) is not int or abs(x)>2**53 for x in position):
                    raise InstinctError('event position')
                amplitude=math.tanh(_finite(p.get('value'),'feedback value',-1e100,1e100)) if kind=='feedback' else 1.
                records.append(DriveEvent((index+1)*tick_duration,(index+2)*tick_duration,index,previous,
                                          position[0]%64,position[1]%64,amplitude))
        except (KeyError,TypeError,ValueError,OverflowError,UnicodeError) as exc:raise InstinctError('invalid event drive') from exc
        self.records=tuple(records)
        self.tick_duration=tick_duration
        self.source_head=previous
        self.digest=hashlib.sha256(json.dumps({'version':self.version,'tick_duration':tick_duration,'head':previous,
            'records':[asdict(r) for r in records]},sort_keys=True,separators=(',',':')).encode()).hexdigest()
    def at(self,time):
        time=_finite(time,'drive time',0,1e9);out=np.zeros(SHAPE,dtype=np.complex128)
        for r in self.records:
            if r.start<=time<r.end:out[r.y,r.x]+=r.amplitude
        return out
    def boundaries(self,start,end):
        return sorted({t for r in self.records for t in (r.start,r.end) if start<t<end})


class ReferenceCGL:
    def __init__(self,geometry,*,seed=0,parameters=None,method='rk4',dt=.01,amplitude_limit=10.,drive=None,initial=None):
        if not isinstance(geometry,Geometry):raise InstinctError('versioned geometry required')
        if type(seed) is not int or not 0<=seed<2**64:raise InstinctError('seed')
        if method not in ('euler','rk4'):raise InstinctError('method')
        self.dt=_finite(dt,'dt',1e-8,.05);self.amplitude_limit=_finite(amplitude_limit,'amplitude limit',1e-6,100)
        self.parameters=parameters if parameters is not None else Parameters()
        if not isinstance(self.parameters,Parameters):raise InstinctError('parameters')
        if drive is not None and not isinstance(drive,EventDrive):raise InstinctError('verified event drive required')
        self.drive=drive if drive is not None else EventDrive()
        self.geometry=geometry;self._geometry=geometry.values
        self.seed=seed;self.method=method;self.time=0.;self.steps=0;self.stopped=False;self.stop_reason=None
        rng=np.random.default_rng(seed)
        self._field=_array(initial,True) if initial is not None else .1+.01*(rng.standard_normal(SHAPE)+1j*rng.standard_normal(SHAPE))
        self._check(self._field)
    @property
    def field(self):return self._field.copy()
    def _check(self,field):
        if not np.isfinite(field).all() or np.max(np.abs(field))>self.amplitude_limit:
            raise InstinctError('nonfinite/amplitude fail-stop')
    def _rhs(self,field,drive):
        self._check(field);p=self.parameters;a2=np.abs(field)**2;energy=np.mean(a2)
        return ((p.mu+1j*p.omega)*field+(p.D+1j*p.B)*laplacian(field)
                -(p.g+1j*p.c)*a2*field+p.lambda_*self._geometry*field
                +p.kappa*(p.E_target-energy)*field+p.eta*drive)
    def step(self,dt=None):
        if self.stopped:raise InstinctError('solver stopped: '+self.stop_reason)
        h=self.dt if dt is None else _finite(dt,'step dt',1e-12,self.dt)
        try:
            if self.steps>=MAX_STEPS:raise InstinctError('step budget')
            end=self.time+h
            if end>1e6 or end<=self.time:raise InstinctError('time budget/resolution')
            points=[self.time]+self.drive.boundaries(self.time,end)+[end]
            trial=self._field.copy()
            with np.errstate(over='raise',invalid='raise',divide='raise'):
                for left,right in zip(points,points[1:]):
                    d=right-left;u=self.drive.at((left+right)/2);k1=self._rhs(trial,u)
                    if self.method=='euler':trial=trial+d*k1
                    else:
                        k2=self._rhs(trial+d*k1/2,u);k3=self._rhs(trial+d*k2/2,u);k4=self._rhs(trial+d*k3,u)
                        trial=trial+d*(k1+2*k2+2*k3+k4)/6
                    self._check(trial)
        except (InstinctError,FloatingPointError) as exc:
            self.stopped=True;self.stop_reason=str(exc);raise InstinctError(self.stop_reason) from exc
        self._field=trial;self.time=end;self.steps+=1
        return features(self._field)
    def run(self,horizon):
        horizon=_finite(horizon,'horizon',0,1000)
        if math.ceil(horizon/self.dt)>MAX_STEPS-self.steps:raise InstinctError('run step budget')
        target=self.time+horizon
        while target-self.time>max(1e-12,abs(target)*1e-14):self.step(min(self.dt,target-self.time))
        return features(self._field)
    def manifest(self):
        return {'solver':VERSION,'numpy':np.__version__,'shape':list(SHAPE),'dx':1,'dy':1,'boundary':'periodic',
                'method':self.method,'dt':self.dt,'seed':self.seed,'parameters':asdict(self.parameters),
                'geometry_version':self.geometry.version,'geometry_hash':self.geometry.digest,
                'drive_version':self.drive.version,'drive_hash':self.drive.digest,'source_event_head':self.drive.source_head,
                'time':self.time,'steps':self.steps,'amplitude_limit':self.amplitude_limit,'stopped':self.stopped,
                'stop_reason':self.stop_reason,'features':features(self._field)}


@dataclass(frozen=True)
class FrozenBias:
    """Frozen calibration and bounded proposed biases, NOT a policy integration."""
    calibration_id:str
    mean:tuple
    scale:tuple
    weights:tuple
    offset:tuple=(0.,0.,0.)
    beta:float=.1
    def __post_init__(self):
        if not isinstance(self.calibration_id,str) or not 1<=len(self.calibration_id)<=256:raise InstinctError('calibration provenance')
        if type(self.mean) is not tuple or type(self.scale) is not tuple or len(self.mean)!=6 or len(self.scale)!=6:
            raise InstinctError('calibration shape')
        for v in self.mean:_finite(v,'calibration mean',-1e6,1e6)
        for v in self.scale:_finite(v,'calibration scale',1e-6,1e6)
        if type(self.weights) is not tuple or len(self.weights)!=3 or any(type(r) is not tuple or len(r)!=6 for r in self.weights):raise InstinctError('weights shape')
        for row in self.weights:
            for v in row:_finite(v,'weight',-10,10)
        if type(self.offset) is not tuple or len(self.offset)!=3:raise InstinctError('offset shape')
        for v in self.offset:_finite(v,'offset',-10,10)
        _finite(self.beta,'beta',0,.25)
    @classmethod
    def calibrate(cls,rows,*,calibration_id,weights,beta=.1):
        if not isinstance(rows,(list,tuple)) or not 2<=len(rows)<=4096:raise InstinctError('calibration rows')
        matrix=np.array([_feature_vector(row) for row in rows])
        return cls(calibration_id,tuple(map(float,matrix.mean(0))),tuple(map(float,np.maximum(matrix.std(0),1e-6))),weights,beta=beta)
    def biases(self,values):
        z=np.clip((np.array(_feature_vector(values))-self.mean)/self.scale,-3,3)
        b=np.tanh(np.array(self.weights)@z+self.offset)
        return dict(zip(('exploration','persistence','switching'),map(float,b)))
    def score_delta(self,values,channel):
        if channel not in ('exploration','persistence','switching'):raise InstinctError('bias channel')
        return self.beta*self.biases(values)[channel]


def _feature_vector(row):
    if not isinstance(row,dict) or set(row)!=set(FEATURES):raise InstinctError('six features required')
    return [_finite(row[key],key,0,1e6) for key in FEATURES]
