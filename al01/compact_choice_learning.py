"""Finite receipt-trained active sensing. Inputs never confer authority."""
import copy
import json
from .folder_session import _hash, _encode
from .folder_contracts import validate_relative_path
from .compact_learning import _reference

PROFILE = 'finite-active-sensing-1'
MAX_BYTES = 65536
MAX_COMPLETION_BYTES = 8192
MAX_EXPOSURES = 256
MAX_TRIALS = 64
USE_TRIALS = 16
TRAIN_TRIALS = 16
EVAL_STAGES = 32

def need(ok, reason):
    if not ok: raise ValueError('COMPACT_CHOICE_INVALID: '+reason)

def digest(value):
    return type(value) is str and len(value)==64 and all(c in '0123456789abcdef' for c in value)

def uniform_policy():
    return dict(schema='compact-choice-policy-1',version=0,parent_sha256=None,counts=[[[0,0] for _ in range(2)] for _ in range(2)],training=[],authority='NONE')

def validate_policy(p):
    need(type(p) is dict and set(p)==set(uniform_policy()) and p['schema']=='compact-choice-policy-1' and p['authority']=='NONE','policy schema/authority')
    need(type(p['version']) is int and p['version'] in (0,1),'policy version')
    need(type(p['counts']) is list and len(p['counts'])==2,'context counts')
    for row in p['counts']:
        need(type(row) is list and len(row)==2,'source counts')
        for cell in row:
            need(type(cell) is list and len(cell)==2 and all(type(n) is int for n in cell) and 0<=cell[0]<=cell[1]<=TRAIN_TRIALS,'bounded success/attempt counts')
    need(type(p['training']) is list and len(p['training'])<=TRAIN_TRIALS,'bounded training lineage')
    for ref in p['training']:_reference(ref,'compact_capture')
    need(len({_hash(ref) for ref in p['training']})==len(p['training']),'distinct actual training captures')
    need(len({(r['identity'],r['instance_id']) for r in p['training']})<=1,'one training history namespace')
    if p['version']==0:need(p==uniform_policy(),'exact uniform policy')
    else:need(p['parent_sha256']==_hash(uniform_policy()) and len(p['training'])==TRAIN_TRIALS and sum(c[1] for r in p['counts'] for c in r)==TRAIN_TRIALS,'complete empirical evidence')
    need(len(_encode(p))<=16384,'policy bytes')
    return copy.deepcopy(p)

def context(policy):
    validate_policy(policy)
    return dict(schema='compact-choice-context-1',policy=copy.deepcopy(policy),policy_sha256=_hash(policy))

def validate_context(c):
    need(type(c) is dict and set(c)=={'schema','policy','policy_sha256'} and c['schema']=='compact-choice-context-1','context schema')
    validate_policy(c['policy']);need(c['policy_sha256']==_hash(c['policy']),'context policy hash')
    return copy.deepcopy(c)

def validate_profile(p,goals):
    need(type(p) is dict and set(p) in ({'schema','descriptor_path','condition'},{'schema','descriptor_path','condition','source_adoption'}) and p['schema']==PROFILE,'explicit choice profile')
    if 'source_adoption' in p:need(p['source_adoption'] is True,'explicit source adoption birth opt-in')
    need(p['descriptor_path'] in [g['path'] for g in goals],'monitored descriptor path')
    need(p['condition'] in ('candidate','no_learning'),'registered choice condition')
    return copy.deepcopy(p)

def initial():
    return dict(schema='compact-choice-state-1',policy=uniform_policy(),candidate=None,training=[],evaluation=[],exposed=[],nonces=[],current=None,last=None,trials=0,invalid=None)

def bounded(s):
    need(type(s) is dict and set(s) in (set(initial()),set(initial())|{'adoption_ref','use_count'}) and s['schema']=='compact-choice-state-1','choice root schema')
    if 'adoption_ref' in s:
        _reference(s['adoption_ref'],'compact_choice_adoption')
        need(type(s['use_count']) is int and 0<=s['use_count']<=USE_TRIALS,'finite later use')
    validate_policy(s['policy'])
    need(s['invalid'] is None or s['invalid'] in ('INVALID_DESCRIPTOR','REUSED_EVIDENCE'),'invalid evidence marker')
    if s['candidate'] is not None:validate_policy(s['candidate'])
    need(all(type(s[k]) is list for k in ('training','evaluation','exposed','nonces')) and type(s['trials']) is int,'typed root arrays/counter')
    need(len(s['training'])<=TRAIN_TRIALS and len(s['evaluation'])<=EVAL_STAGES and len(s['exposed'])<=MAX_EXPOSURES and all(digest(d) for d in s['exposed']) and len(set(s['exposed']))==len(s['exposed']) and len(s['nonces'])<=MAX_TRIALS and all(type(n) is str and 1<=len(n)<=96 for n in s['nonces']) and len(set(s['nonces']))==len(s['nonces']) and 0<=s['trials']<=MAX_TRIALS,'finite choice budgets')
    for row in s['training']+s['evaluation']:
        need(type(row) is dict and set(row)=={'context','source_class','success','capture_ref','descriptor_ref','policy_sha256','role','fresh','task_id','stage'},'exact observation row')
        need(all(type(row[k]) is int and row[k] in (0,1) for k in ('context','source_class')) and all(type(row[k]) is bool for k in ('success','fresh')) and digest(row['policy_sha256']) and row['role'] in ('train','validation','test'),'typed observation')
        for key in ('capture_ref','descriptor_ref'):_reference(row[key],'compact_capture')
        need(type(row['task_id']) is str and len(row['task_id'])<=32 and type(row['stage']) is int and row['stage'] in (0,1),'fixed task/stage binding')
        need(all(row['capture_ref'][k]==row['descriptor_ref'][k] for k in ('identity','instance_id')),'receipt/descriptor namespace')
    need(all(r['role']=='train' for r in s['training']) and all(r['role'] in ('validation','test') for r in s['evaluation']),'permanent training/evaluation roles')
    t=s['current']
    if t is not None:
        need(type(t) is dict and set(t)=={'role','opened','deadline','phase','descriptor','descriptor_ref','selection'},'current trial schema')
        need(t['role'] in ('train','validation','test','use') and (t['role']!='use' or 'adoption_ref' in s) and all(type(t[k]) is int and 0<=t[k]<2**53 for k in ('opened','deadline')) and t['deadline']==t['opened']+4 and t['phase'] in ('DESCRIPTOR','EVIDENCE'),'current trial timing/phase')
        if t['phase']=='DESCRIPTOR':need(t['descriptor'] is None and t['descriptor_ref'] is None and t['selection'] is None,'unobserved descriptor')
        else:
            _reference(t['descriptor_ref'],'compact_capture')
            need(type(t['descriptor']) is dict and type(t['selection']) is dict and set(t['selection'])=={'source_class','path','estimates','policy_sha256'},'observed selection')
            d=t['descriptor'];paths=[o['path'] for o in d.get('options',[]) if type(o) is dict and 'path' in o]
            descriptor(json.dumps(d),paths,paths)
            candidates=[s['policy']]+([] if s['candidate'] is None else [s['candidate']])
            matching=[p for p in candidates if _hash(p)==t['selection']['policy_sha256']]
            if 'adoption_ref' in s and s['policy']['version']==1 and s['policy']==s['candidate']:
                matching=list({_hash(p):p for p in matching}.values())
            need(len(matching)==1 and type(t['selection']['source_class']) is int and t['selection']==choose(matching[0],d,len(s['training']) if t['role']=='train' else None),'recomputed actual selection')
    need(len(_encode(s))<=MAX_BYTES,'bounded external choice root')
    return copy.deepcopy(s)

def descriptor(text,goals,visible):
    need(len(text.encode())<=4096,'descriptor size')
    d=json.loads(text)
    need(type(d) is dict and set(d)=={'schema','nonce','context','required_type','options'} and d['schema']=='compact-choice-descriptor-1','strict observed descriptor')
    need(type(d['nonce']) is str and 1<=len(d['nonce'])<=96,'trial nonce')
    need(type(d['context']) is int and d['context'] in (0,1) and d['required_type']=='sample_v1','typed context/task')
    need(type(d['options']) is list and len(d['options'])==2,'two options')
    for o in d['options']:
        need(type(o) is dict and set(o)=={'path','source_class'},'option data only')
        validate_relative_path(o['path'])
        need(type(o['source_class']) is int and o['source_class'] in (0,1),'observed source class')
        need(o['path'] in goals and o['path'] in visible,'existing monitored visible source')
    need(len({o['path'].casefold() for o in d['options']})==2 and {o['source_class'] for o in d['options']}=={0,1},'distinct sources/classes')
    return d

def choose(policy,d,training_index=None):
    validate_policy(policy)
    estimates=[(cell[0]+1)/(cell[1]+2) for cell in policy['counts'][d['context']]]
    selected=training_index%2 if training_index is not None else max(range(2),key=lambda arm:(estimates[arm],-arm))
    return dict(source_class=selected,path=next(o['path'] for o in d['options'] if o['source_class']==selected),estimates=estimates,policy_sha256=_hash(policy))

def control(s,kind,role,now):
    s=bounded(s)
    need(type(now) is int and 0<=now<2**53-4,'logical control time')
    if kind=='CHOICE_BEGIN':
        need(role in ('train','validation','test','use') and s['current'] is None and s['trials']<MAX_TRIALS and s['invalid'] is None,'idle bounded valid trial')
        if role=='train':need(s['candidate'] is None and len(s['training'])<TRAIN_TRIALS,'training before freeze')
        elif role=='use':need('adoption_ref' in s and s['candidate'] is not None and len(s['evaluation'])==EVAL_STAGES and s['use_count']<USE_TRIALS,'finite incumbent use after evaluation')
        else:
            need(s['candidate'] is not None and len(s['evaluation'])<EVAL_STAGES,'evaluation after freeze')
            need(role==('validation' if len(s['evaluation'])<16 else 'test'),'fixed validation then test schedule')
        s['current']=dict(role=role,opened=now,deadline=now+4,phase='DESCRIPTOR',descriptor=None,descriptor_ref=None,selection=None)
        s['trials']+=1
    elif kind=='CHOICE_FREEZE':
        need(role is None and s['current'] is None and s['candidate'] is None and len(s['training'])==TRAIN_TRIALS and s['invalid'] is None,'complete valid idle training freeze')
        p=uniform_policy();p['version']=1;p['parent_sha256']=_hash(s['policy'])
        for r in s['training']:
            cell=p['counts'][r['context']][r['source_class']];cell[0]+=int(r['success']);cell[1]+=1;p['training'].append(r['capture_ref'])
        need(all(c[1]==4 for row in p['counts'] for c in row),'balanced four observations per cell')
        s['candidate']=validate_policy(p)
    else:need(False,'unknown choice control')
    return bounded(s)

def capture(s,profile,item,ref,path,now,goals,visible,decision):
    s=bounded(s);result=item['capture']['result'];digest=None
    need(type(now) is int and 0<=now<2**53,'logical capture time')
    if item['request']['operation']=='READ_TEXT' and result['status']=='OK':digest=result['payload']['content_sha256']
    fresh=digest is None or digest not in s['exposed']
    if digest is not None and fresh:s['exposed'].append(digest)
    t=s['current']
    if t is None or decision.get('choice') is None:return bounded(s)
    need(t['opened']<=now<t['deadline'],'trial deadline')
    if t['phase']=='DESCRIPTOR':
        try:
            need(path==profile['descriptor_path'] and result['status']=='OK' and fresh,'fresh descriptor receipt')
            d=descriptor(result['payload']['text'],goals,visible)
            need(d['nonce'] not in s['nonces'] and all(o['path']!=profile['descriptor_path'] for o in d['options']),'fresh nonce/source separation')
            s['nonces'].append(d['nonce'])
            p=s['policy'] if t['role'] in ('train','use') or profile['condition']=='no_learning' else s['candidate']
            t.update(phase='EVIDENCE',descriptor=d,descriptor_ref=ref,selection=choose(p,d,len(s['training']) if t['role']=='train' else None))
        except (ValueError,TypeError,KeyError):
            s['last']=dict(status='INVALID_DESCRIPTOR',capture_ref=ref,role=t['role']);s['current']=None;s['invalid']='INVALID_DESCRIPTOR'
    else:
        need(t['phase']=='EVIDENCE' and path==t['selection']['path'],'only committed chosen source')
        success=False
        if result['status']=='OK' and fresh:
            try:
                body=json.loads(result['payload']['text']);d=t['descriptor']
                success=type(body) is dict and type(body.get('context')) is int and body==dict(schema='compact-choice-evidence-1',nonce=d['nonce'],context=d['context'],type=d['required_type'])
            except (ValueError,TypeError):pass
        count=len(s['training']) if t['role']=='train' else s['use_count'] if t['role']=='use' else len(s['evaluation'])%16
        row=dict(context=t['descriptor']['context'],source_class=t['selection']['source_class'],success=success,capture_ref=ref,descriptor_ref=t['descriptor_ref'],policy_sha256=t['selection']['policy_sha256'],role=t['role'],fresh=fresh,task_id=t['role']+'-'+str(count//2),stage=count%2)
        if not fresh:s['invalid']='REUSED_EVIDENCE'
        if t['role']=='use':s['use_count']+=1
        else:s['training' if t['role']=='train' else 'evaluation'].append(row)
        s['last']=dict(status='SUCCESS' if success else 'FAILURE',**row);s['current']=None
    return bounded(s)
