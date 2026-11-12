"""One finite receipt-driven extension; no retraining or new authority."""
import copy
import json
from . import compact_choice_learning as choice
from .folder_session import _encode, _hash

MAX_BYTES = 32768
MAX_COMPLETION_BYTES = 8192

def need(ok, why):
    if not ok: raise ValueError('COMPACT_CONTINUATION_INVALID: '+why)

def bounded(s):
    fields={'schema','source','base_ref','policy_sha256','base_trials','base_uses','limit','used','cumulative_trials','cumulative_uses','outcome_review','threshold','failures','veto','status','reason','current','last','nonces','review'}
    need(type(s) is dict and set(s)==fields and s['schema']=='compact-receipt-continuation-1','closed schema')
    for key in ('base_trials','base_uses','limit','used','cumulative_trials','cumulative_uses','threshold','failures'):
        need(type(s[key]) is int,'integer counters')
    need(0<=s['base_trials']<=choice.MAX_TRIALS and 0<=s['base_uses']<=choice.USE_TRIALS,'legacy counters')
    need(1<=s['limit']<=16 and 0<=s['used']<=s['limit'] and 0<=s['failures']<=s['used'] and s['threshold']==2,'finite grant')
    need(s['cumulative_trials']==s['base_trials']+s['used'] and s['cumulative_uses']==s['base_uses']+s['used'],'cumulative admitted usage')
    need(type(s['nonces']) is list and len(s['nonces'])==s['used'] and all(type(n) is str and 1<=len(n)<=96 for n in s['nonces']) and len(set(s['nonces']))==s['used'],'consumed descriptors')
    need(s['status'] in ('WAITING','CONTINUE','POLICY_REVIEW_REQUIRED','EXHAUSTED'),'state')
    need(s['reason'] in ('NO_ELIGIBLE_DESCRIPTOR','OBSERVED_ELIGIBLE_DESCRIPTOR','COMMITMENT_EXPIRED','SUCCESS','FAILURE','BLOCKED','REUSED_EVIDENCE'),'reason')
    need(type(s['veto']) is bool and type(s['outcome_review']) is bool,'fixed switches')
    need(choice.digest(s['policy_sha256']) and type(s['source']) is dict,'source/policy')
    choice._reference(s['base_ref'],'compact_choice_state')
    need(s['source']['choice_state_ref']==s['base_ref'] and s['source']['choice_policy_sha256']==s['policy_sha256'],'migration anchor')
    def receipt(ref):
        choice._reference(ref,'compact_capture')
        need(all(ref[k]==s['base_ref'][k] for k in ('identity','instance_id')),'same receipt namespace')
    t=s['current']
    need((t is not None)==(s['status']=='CONTINUE'),'current/status')
    if t is not None:
        need(type(t) is dict and set(t)=={'descriptor','descriptor_ref','selection','opened','deadline'},'commitment fields')
        need(type(t['opened']) is int and type(t['deadline']) is int and 0<=t['opened']<2**53-4 and t['deadline']==t['opened']+4,'bounded deadline')
        receipt(t['descriptor_ref']);d=t['descriptor']
        paths=[o['path'] for o in d['options']];choice.descriptor(json.dumps(d),paths,paths)
        need(d['nonce']==s['nonces'][-1],'current consumed nonce')
        sel=t['selection'];need(type(sel) is dict and set(sel)=={'source_class','path','estimates','policy_sha256'},'selection fields')
        need(sel['policy_sha256']==s['policy_sha256'] and type(sel['source_class']) is int and sel['source_class'] in (0,1),'bound selected policy')
        need(any(o['path']==sel['path'] and o['source_class']==sel['source_class'] for o in d['options']),'descriptor selected option')
        need(type(sel['estimates']) is list and len(sel['estimates'])==2 and all(type(v) in (int,float) and 0<=v<=1 for v in sel['estimates']),'bounded estimates')
    last=s['last']
    if last is not None:
        need(type(last) is dict,'outcome fields')
        if last.get('status')=='EXPIRED':
            need(set(last)=={'status','descriptor_ref','policy_sha256'},'expiration fields')
        else:
            need(set(last)=={'status','success','capture_ref','descriptor_ref','policy_sha256','role'} and last['role']=='use','receipt outcome fields')
            need(last['status'] in ('SUCCESS','FAILURE','BLOCKED','REUSED_EVIDENCE'),'receipt outcome')
            expected=True if last['status']=='SUCCESS' else False if last['status']=='FAILURE' else None
            need(last['success'] is expected,'non-evidence is not false')
            receipt(last['capture_ref'])
        receipt(last['descriptor_ref']);need(last['policy_sha256']==s['policy_sha256'],'outcome policy')
    if s['status']=='POLICY_REVIEW_REQUIRED':
        need(s['outcome_review'] and s['failures']>=2 and s['review']==last and last['status']=='FAILURE','receipt-derived review')
    else:need(s['review'] is None,'no unearned review')
    if s['status']=='EXHAUSTED':need(s['used']==s['limit'],'exact exhaustion')
    if s['status']=='WAITING':need(s['used']<s['limit'],'remaining allowance')
    need(len(_encode(s))<=MAX_BYTES,'bounded ledger')
    return copy.deepcopy(s)

def grant(root, source, base_ref, limit, outcome_review):
    choice.bounded(root)
    need(type(limit) is int and 1<=limit<=16 and type(outcome_review) is bool,'explicit finite grant')
    need(root['current'] is None and root['invalid'] is None,'idle valid incumbent')
    return bounded(dict(schema='compact-receipt-continuation-1',source=source,base_ref=base_ref,
        policy_sha256=_hash(root['policy']),base_trials=root['trials'],base_uses=root.get('use_count',0),
        limit=limit,used=0,cumulative_trials=root['trials'],cumulative_uses=root.get('use_count',0),outcome_review=outcome_review,
        threshold=2,failures=0,veto=False,status='WAITING',reason='NO_ELIGIBLE_DESCRIPTOR',
        current=None,last=None,nonces=[],review=None))

def expire(s, now):
    s=bounded(s);t=s['current']
    need(t is not None and now>=t['deadline'],'expired commitment')
    s['last']=dict(status='EXPIRED',descriptor_ref=t['descriptor_ref'],policy_sha256=s['policy_sha256'])
    s['current']=None;s['status']='EXHAUSTED' if s['used']==s['limit'] else 'WAITING';s['reason']='COMMITMENT_EXPIRED'
    return bounded(s)

def capture(s, root, profile, item, ref, path, now, goals, visible, decision):
    s=bounded(s)
    need(_hash(root['policy'])==s['policy_sha256'],'bound incumbent')
    result=item['capture']['result'];ok=item['request']['operation']=='READ_TEXT' and result['status']=='OK'
    digest=result['payload']['content_sha256'] if ok else None
    fresh=ok and digest not in root['exposed']
    t=s['current']
    if decision.get('continuation') is not None:
        need(t is not None and not s['veto'] and path==t['selection']['path'],'committed dispatch')
        status='BLOCKED';success=None
        if ok:
            success=False
            if fresh:
                try:
                    d=t['descriptor'];body=json.loads(result['payload']['text'])
                    success=type(body) is dict and type(body.get('context')) is int and body==dict(schema='compact-choice-evidence-1',nonce=d['nonce'],context=d['context'],type=d['required_type'])
                except (ValueError,TypeError):pass
            status=('SUCCESS' if success else 'FAILURE') if fresh else 'REUSED_EVIDENCE'
            if not fresh:success=None
        s['last']=dict(status=status,success=success,capture_ref=ref,descriptor_ref=t['descriptor_ref'],policy_sha256=s['policy_sha256'],role='use')
        if status=='FAILURE':s['failures']+=1
        elif status=='SUCCESS':s['failures']=0
        s['current']=None;s['status']='EXHAUSTED' if s['used']==s['limit'] else 'WAITING';s['reason']=status
        if s['outcome_review'] and s['failures']>=s['threshold']:
            s['status']='POLICY_REVIEW_REQUIRED';s['review']=copy.deepcopy(s['last'])
        return bounded(s)
    if t is not None or s['status']!='WAITING' or s['veto'] or not fresh or path!=profile['descriptor_path']:return s
    try:
        d=choice.descriptor(result['payload']['text'],goals,visible)
        need(d['nonce'] not in root['nonces']+s['nonces'] and all(o['path']!=profile['descriptor_path'] for o in d['options']),'new distinct descriptor')
        need(now<2**53-4,'deadline range')
    except (ValueError,TypeError,KeyError):return s
    s['used']+=1;s['cumulative_trials']+=1;s['cumulative_uses']+=1;s['nonces'].append(d['nonce'])
    s['current']=dict(descriptor=d,descriptor_ref=ref,selection=choice.choose(root['policy'],d),opened=now,deadline=now+4)
    s['status']='CONTINUE';s['reason']='OBSERVED_ELIGIBLE_DESCRIPTOR'
    return bounded(s)
