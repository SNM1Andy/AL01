"""Bounded metadata policy wired to actual broker requests, not learned semantics.

Calibration is frozen from a separate zero-drive reference run. The decision
journal is durable BEFORE request submission or sensing. Historical decisions
replay against exact receipt/cognition/field prefixes; scores never grant file
access. Only acknowledged application checks apply, not OS confinement.
"""
import copy
import os
from pathlib import Path
from dataclasses import asdict
import numpy as np
from .folder_joint import FolderJointRunner,need
from .folder_runner import atomic_json
from .folder_session import _hash,_encode
from .ordered_folder_session import OrderedFolderSession
from .folder_cognition import replay_captures
from .folder_field import FolderEventDrive,TICK_DURATION
from .folder_policy import select_candidate,validate_policy,MAX_BYTES,MAX_READS
from .instinct import ReferenceCGL,Geometry,FrozenBias
from .folder_cl import prepare_decision,validate_decision
from .controller_paths import check_file

SCHEMA='folder-policy-agent-1'
CALIBRATION_SEED=991
MAX_AGENT_BYTES=4*1024*1024


def independent_calibration():
    solver=ReferenceCGL(Geometry(np.zeros((64,64))),seed=CALIBRATION_SEED,dt=.01)
    rows=[]
    for _ in range(16):solver.step();rows.append(solver.manifest()['features'])
    cal=FrozenBias.calibrate(rows,calibration_id='folder-independent-zero-drive-991-16',
        weights=((0.,)*6,(-.5,)*6,(.5,)*6),beta=.1)
    return cal,{'seed':CALIBRATION_SEED,'drive':'ZERO_NO_CAPTURE_INPUT','steps':16,'dt':.01,
                'rows_sha256':_hash(rows),'profile':'fixed zero geometry; independent initialization-only calibration'}


def decode_calibration(raw):
    need(type(raw) is dict and set(raw)=={'calibration_id','mean','scale','weights','offset','beta'},'calibration fields')
    return FrozenBias(raw['calibration_id'],tuple(raw['mean']),tuple(raw['scale']),tuple(tuple(x) for x in raw['weights']),tuple(raw['offset']),raw['beta'])


class FolderAgentRunner(FolderJointRunner):
    def __init__(self,*args,u0_enabled=None,field_enabled=None,**kwargs):
        for flag in (u0_enabled,field_enabled):need(flag is None or type(flag) is bool,'policy switch Boolean')
        need(kwargs.get('field_seed')!=CALIBRATION_SEED,'live field seed must differ from calibration seed')
        directory=kwargs.get('state_directory',args[1] if len(args)>1 else None)
        if directory is not None:
            birth=Path(os.path.abspath(directory))/'folder-joint-origin.json';check_file(birth)
            if birth.exists():
                origin,_=OrderedFolderSession._read(birth,65536)
                need(origin.get('field_seed')!=CALIBRATION_SEED,'existing field seed equals calibration seed; original files unchanged')
        super().__init__(*args,**kwargs)
        self.config_path=self.directory/'folder-agent-config.json';self.decisions_path=self.directory/'folder-agent-decisions.json'
        with self._joint_lock():
            check_file(self.config_path);check_file(self.decisions_path)
            need(self.field_seed!=CALIBRATION_SEED,'live field seed must differ from calibration seed')
            marker=self.actor.get('folder_agent_config_sha256')
            if marker is not None:
                need(self.config_path.exists() and self.decisions_path.exists(),'policy state missing; no implicit reset')
            elif self.config_path.exists() or self.decisions_path.exists():
                need(False,'policy initialization incomplete; manual diagnosis, no inferred adoption')
            else:
                rows=self.session.list_requests()
                need(not rows and self.actor.get('dispatch_started') is None,'existing fixed-planner requests require explicit migration; fresh policy boundary only')
                calibration,provenance=independent_calibration()
                config={'schema':SCHEMA,'identity':self.actor['id'],'scope_ref':self.actor['scope_ref'],
                    'field_seed':self.field_seed,'calibration':asdict(calibration),'calibration_provenance':provenance,
                    'u0_enabled':True if u0_enabled is None else u0_enabled,'field_enabled':True if field_enabled is None else field_enabled,
                    'baseline_requests':[{'request_id':r['request_id'],'request_sha256':r['request_sha256'],'receipt_sha256':r['receipt_sha256']} for r in rows]}
                sha=_hash(config);self._write(self.config_path,config)
                self._write(self.decisions_path,{'schema':SCHEMA,'identity':self.actor['id'],'scope_ref':self.actor['scope_ref'],'config_sha256':sha,'decisions':[]})
                self.actor['folder_agent_config_sha256']=sha;atomic_json(self.actor_path,self.actor);self.session._sync(self.directory)
            self._load_policy()
            need(u0_enabled is None or u0_enabled==self.config['u0_enabled'],'frozen U0 switch')
            need(field_enabled is None or field_enabled==self.config['field_enabled'],'frozen field switch')

    def _write(self,path,data):
        check_file(path)
        need(len(_encode(data))<=MAX_AGENT_BYTES,'policy storage budget')
        atomic_json(path,{'data':data,'sha256':_hash(data)});self.session._sync(self.directory)

    @staticmethod
    def _check_bytes(cognition):
        known={};used=0;reads=0
        for source in cognition['sources']:
            q=source['request'];r=source['capture']['result']
            if q['operation']=='LIST_SCOPE':
                known={e['entry_ref']:e for e in r['payload']['entries']} if r['status']=='OK' else {}
            if q['operation']=='READ_TEXT':
                reads+=1
                if r['status']=='OK':
                    size=r['payload']['utf8_bytes'];entry=known.get(q['entry_ref'])
                    need(entry is not None and entry['kind']=='file' and size<=entry['byte_size'],'actual read exceeded listed reservation; stop')
                    used+=size
        need(used<=MAX_BYTES and reads<=MAX_READS,'actual cumulative read budget exceeded; stop')

    def _historical_states(self,counts):
        # Fresh-policy sessions permit one outstanding request, hence one capture
        # per committed joint generation. Rebuild the entire checkpoint chain,
        # including full numerical arrays and receipt bindings, not merely the
        # features used by selection. This verifies consistency, not host trust.
        solver=self._fresh_field(self.field_seed);states={};sources=self.cognition['sources']
        rows=self.session.list_capture_order();parent=None
        replay=copy.copy(self)
        for count in range(len(sources)+1):
            cognition=replay_captures(self.actor['id'],self.actor['scope_ref'],sources[:count])
            if count:
                solver.drive=FolderEventDrive(cognition['events'],committed_tick=count)
                solver.run((count+1)*TICK_DURATION-solver.time)
            replay.cognition=cognition;replay.solver=solver
            replay.bindings=[self._binding(row) for row in rows[:count]]
            replay.joint_sha=parent
            digest=_hash(replay._payload())
            if count in counts:
                states[count]=(cognition,solver.manifest()['features'],digest)
            parent=digest
        need(parent==self.joint_sha,'joint checkpoint exact causal lineage replay')
        return states

    def _choose(self,cognition,features):
        self._check_bytes(cognition);sources=cognition['sources']
        listings=[s for s in sources if s['request']['operation']=='LIST_SCOPE']
        if not listings:return self._request('LIST_SCOPE'),None,'INITIAL_LIST',None
        if listings[-1]['capture']['result']['status']!='OK':return None,None,'LIST_FAILED',None
        trace=select_candidate(cognition,features,self.calibration,u0_enabled=self.config['u0_enabled'],field_enabled=self.config['field_enabled'])
        if trace['selected'] is not None:
            prepared=prepare_decision(cognition,trace['selected'])
            return validate_decision(prepared,cognition),trace,'POLICY_READ',prepared
        checked={s['request'].get('intent') for s in sources if s['request']['operation']=='CHECK_ACCESS'}
        for intent in ('export_state','leave_scope'):
            if intent not in checked:return self._request('CHECK_ACCESS',intent=intent),trace,'POLICY_BOUNDARY_CHECK',None
        return None,trace,'FINITE_COMPLETE',None

    def _load_policy(self):
        check_file(self.config_path);check_file(self.decisions_path)
        config,sha=OrderedFolderSession._read(self.config_path,MAX_AGENT_BYTES)
        need(type(config) is dict and set(config)=={'schema','identity','scope_ref','field_seed','calibration','calibration_provenance','u0_enabled','field_enabled','baseline_requests'},'policy config fields')
        need(config['schema']==SCHEMA and config['identity']==self.actor['id'] and config['scope_ref']==self.actor['scope_ref']
             and config['field_seed']==self.field_seed and sha==self.actor.get('folder_agent_config_sha256'),'policy actor/config binding')
        need(type(config['u0_enabled']) is bool and type(config['field_enabled']) is bool,'policy flags')
        calibration,provenance=independent_calibration()
        need(_encode(asdict(calibration))==_encode(config['calibration']) and provenance==config['calibration_provenance'],'independent frozen calibration replay')
        self.config=config;self.config_sha=sha;self.calibration=calibration
        self._check_bytes(self.cognition)
        data,_=OrderedFolderSession._read(self.decisions_path,MAX_AGENT_BYTES)
        need(type(data) is dict and set(data)=={'schema','identity','scope_ref','config_sha256','decisions'} and data['schema']==SCHEMA
             and data['identity']==self.actor['id'] and data['scope_ref']==self.actor['scope_ref'] and data['config_sha256']==sha,'decision journal binding')
        decisions=data['decisions'];need(type(decisions) is list and len(decisions)<=32,'decision count')
        rows=self.session.list_requests();baseline=config['baseline_requests']
        need(type(baseline) is list and len(baseline)<=32 and len(baseline)<=len(rows),'baseline count')
        for old,row in zip(baseline,rows):
            need(old=={k:row[k] for k in ('request_id','request_sha256','receipt_sha256')} and row['status']=='CONSUMED','baseline request binding')
        counts=set()
        for i,d in enumerate(decisions):
            need(type(d) is dict and set(d)=={'index','source_count','source_cognition_sha256','field_features','kind','request','policy_trace','source_joint_sha256','source_origins','prepared_decision'} and d['index']==i,'decision fields')
            need(type(d['source_count']) is int and 0<=d['source_count']<=len(self.cognition['sources']),'decision source count');counts.add(d['source_count'])
            need(type(d['source_joint_sha256']) is str and len(d['source_joint_sha256'])==64 and all(c in '0123456789abcdef' for c in d['source_joint_sha256']),'decision joint digest')
        historical=self._historical_states(counts);capture_rows=self.session.list_capture_order();seen=set()
        for i,d in enumerate(decisions):
            cognition,features,source_joint_sha=historical[d['source_count']]
            need(d['source_joint_sha256']==source_joint_sha,'decision historical joint binding')
            request,trace,kind,prepared=self._choose(cognition,features)
            need(request is not None and request==d['request'] and kind==d['kind'] and _encode(trace)==_encode(d['policy_trace'])
                 and _encode(prepared)==_encode(d['prepared_decision']) and d['source_cognition_sha256']==_hash(cognition) and d['field_features']==features,'decision exact causal replay')
            need(d['source_origins']==sorted({s['capture']['capture_origin'] for s in cognition['sources']}),'decision origin labels')
            rid=request['request_id'];need(rid not in seen,'duplicate decision request');seen.add(rid)
            index=len(baseline)+i
            if index<len(rows):
                row=rows[index];need(row['request']==request,'decision/submitted request order')
                prior=sum(r['receipt_seq']<row['submit_seq'] for r in capture_rows)
                need(prior==d['source_count'],'decision before-request capture prefix')
                if row['status']=='REQUESTED':
                    need(i==len(decisions)-1 and d['source_joint_sha256']==self.joint_sha,'pending source joint binding')
            else:need(i==len(decisions)-1 and index==len(rows) and d['source_count']==len(self.bindings) and d['source_joint_sha256']==self.joint_sha,'only last planned request may be unsubmitted')
        need(len(rows)<=len(baseline)+len(decisions),'untraced submitted request')
        self.agent_data=data

    def _plan(self):
        self._load_policy();decisions=self.agent_data['decisions'];rows=self.session.list_requests();byid={r['request_id']:r for r in rows}
        if decisions:
            last=decisions[-1];row=byid.get(last['request']['request_id'])
            if row is None or row['status']=='REQUESTED':return copy.deepcopy(last['request'])
        need(not any(r['status']=='REQUESTED' for r in rows),'pending request gates selection')
        request,trace,kind,prepared=self._choose(self.cognition,self.solver.manifest()['features'])
        if request is None:return None
        need(len(decisions)<32,'decision budget')
        decision={'index':len(decisions),'source_count':len(self.bindings),'source_cognition_sha256':_hash(self.cognition),
            'field_features':self.solver.manifest()['features'],'kind':kind,'request':request,'policy_trace':trace,
            'prepared_decision':prepared,'source_joint_sha256':self.joint_sha,'source_origins':sorted({s['capture']['capture_origin'] for s in self.cognition['sources']})}
        data=copy.deepcopy(self.agent_data);data['decisions'].append(decision)
        self._write(self.decisions_path,data) # Must persist before submit/dispatch.
        self.agent_data=data;return copy.deepcopy(request)

    def _step_locked(self):
        self._catch_up_locked();self._load_policy()
        progressed=super()._step_locked()
        self._check_bytes(self.cognition);self._load_policy()
        return progressed

    def report(self):
        with self._joint_lock():
            report=super()._report_locked();self._load_policy()
            report.update(policy_schema=SCHEMA,policy_config_sha256=self.config_sha,
                joint_scope='receipt-derived cognition/U0 and field feed the bounded request policy; no full baseline claim',
                calibration=copy.deepcopy(self.config['calibration_provenance']),
                decisions=copy.deepcopy(self.agent_data['decisions']),
                policy_scope='persisted U0/CGL metadata ranking selects actual requests; no performance/generalization claim')
        return report
