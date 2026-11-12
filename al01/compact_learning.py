"""Bounded measured-width lifecycle. No text truth, rewards or teacher authority."""
import copy
import math
from .folder_learning import PROTOCOL
from .folder_session import _hash, _encode
from .folder_contracts import validate_relative_path

ROLES = ('train', 'heldout_mechanism', 'heldout_composition', 'retention')
MODEL_SCHEMA = 'compact-empirical-width-model-1'
MAX_MODEL_BYTES = 16384
CUMULATIVE_SCHEMA = 'compact-empirical-width-model-2'
CUMULATIVE_PROTOCOL = dict(PROTOCOL, fit_mode='CUMULATIVE_PARENT_PLUS_NEW', maximum_cohorts=3, maximum_cell_count=8*65535)

def need(ok, reason):
    if not ok: raise ValueError('COMPACT_LEARNING_INVALID: '+reason)

def _reference(ref,kind):
    need(type(ref) is dict and set(ref)=={'schema','identity','instance_id','kind','encoding','payload_bytes','sha256','byte_size'},'typed evidence reference')
    need(ref['schema']=='life-object-1' and ref['kind']==kind and ref['encoding']=='json','evidence reference kind')
    need(type(ref['identity']) is str and 1<=len(ref['identity'])<=128,'reference identity')
    for key,size in (('instance_id',32),('sha256',64)):
        need(type(ref[key]) is str and len(ref[key])==size and all(c in '0123456789abcdef' for c in ref[key]),'reference digest')
    need(type(ref['payload_bytes']) is int and type(ref['byte_size']) is int and 0<ref['payload_bytes']<ref['byte_size']<=8*1024*1024+1025,'reference size')

def uniform_model():
    return dict(schema=MODEL_SCHEMA, kind='UNIFORM_BASELINE', version=0, counts=[[0]*4 for _ in range(4)],
                parent_ref=None, training=[], protocol_sha256=_hash(dict(PROTOCOL)), authority='NONE')

def validate_model(model):
    need(type(model) is dict, 'model object')
    cumulative=model.get('schema')==CUMULATIVE_SCHEMA
    need(set(model)==set(uniform_model())|({'fit_mode'} if cumulative else set()), 'model fields')
    protocol=CUMULATIVE_PROTOCOL if cumulative else dict(PROTOCOL)
    need(model['schema'] in (MODEL_SCHEMA,CUMULATIVE_SCHEMA) and model['authority']=='NONE' and model['protocol_sha256']==_hash(protocol), 'model authority/protocol')
    if cumulative:need(model['fit_mode']=='CUMULATIVE_PARENT_PLUS_NEW' and type(model['version']) is int and 1<=model['version']<=3,'finite cumulative model version')
    need(type(model['version']) is int and 0<=model['version']<=2**53-1, 'model version')
    rows=model['counts']
    need(type(rows) is list and len(rows)==4 and all(type(r) is list and len(r)==4 for r in rows), 'count shape')
    need(all(type(n) is int and 0<=n<=8*65535 for r in rows for n in r), 'count bounds')
    need(type(model['training']) is list and len(model['training'])<=8, 'training bounds')
    if model['version']==0: need(model==uniform_model(), 'exact uniform parent')
    else:
        need(model['kind']=='FROZEN_EMPIRICAL_WIDTH' and type(model['parent_ref']) is dict and model['training'], 'empirical parent/evidence')
        _reference(model['parent_ref'],'compact_model')
        need(sum(map(sum,rows))>0, 'measured transitions')
        for record in model['training']:
            need(type(record) is dict and set(record)=={'path','capture_ref','content_sha256'}, 'training record')
            validate_relative_path(record['path'])
            _reference(record['capture_ref'],'compact_capture')
            need(all(record['capture_ref'][key]==model['parent_ref'][key] for key in ('identity','instance_id')),'training/parent namespace')
            need(type(record['capture_ref']) is dict and type(record['content_sha256']) is str and len(record['content_sha256'])==64
                 and all(c in '0123456789abcdef' for c in record['content_sha256']), 'training identity')
    need(len(_encode(model))<=MAX_MODEL_BYTES, 'model byte budget')
    return copy.deepcopy(model)

def distributions(model, *, unigram=False):
    validate_model(model)
    rows=model['counts']
    if unigram: rows=[[sum(r[i] for r in rows) for i in range(4)]]*4
    return [[(n+1)/(sum(row)+4) for n in row] for row in rows]

def measured(item):
    from .compact_observer import validate_capture
    validate_capture(item)
    need(item['request']['operation']=='READ_TEXT' and item['capture']['result']['status']=='OK', 'successful text required')
    text=item['capture']['result']['payload']['text']
    need(len(text)>=2, 'insufficient transitions')
    return [len(c.encode('utf8'))-1 for c in text]

def fit(records, resolve, parent, parent_ref):
    need(type(records) is list and 1<=len(records)<=8 and callable(resolve),'bounded training records')
    need(all(type(r) in (tuple,list) and len(r)==2 and type(r[0]) is str and type(r[1]) is dict for r in records),'training record inputs')
    need(len(_encode(records))<=16384,'training reference byte budget')
    _reference(parent_ref,'compact_model')
    for path,ref in records:
        validate_relative_path(path);_reference(ref,'compact_capture')
        need(all(ref[key]==parent_ref[key] for key in ('identity','instance_id')),'training reference namespace')
    need(len({p.casefold() for p,_ in records})==len(records),'distinct training paths')
    validate_model(parent)
    model=uniform_model();model.update(kind='FROZEN_EMPIRICAL_WIDTH',version=parent['version']+1,parent_ref=copy.deepcopy(parent_ref))
    seen=set()
    for path,ref in records:
        item=resolve(ref); widths=measured(item);content=item['capture']['result']['payload']['content_sha256']
        need(content not in seen, 'duplicate training content');seen.add(content)
        for a,b in zip(widths,widths[1:]):model['counts'][a][b]+=1
        model['training'].append(dict(path=path,capture_ref=copy.deepcopy(ref),content_sha256=content))
    return validate_model(model)

def metric(model, items, *, unigram=False):
    need(type(items) is list and 1<=len(items)<=8 and type(unigram) is bool,'bounded metric inputs')
    need(len(_encode(items))<=8*400000,'metric byte budget')
    probs=distributions(model,unigram=unigram);loss=0.;n=0
    for item in items:
        widths=measured(item)
        for a,b in zip(widths,widths[1:]):loss-=math.log(probs[a][b]);n+=1
    need(n>=PROTOCOL['minimum_transitions_per_split'], 'minimum transitions')
    return dict(transitions=n,mean_log_loss=loss/n)

def evaluate(candidate, incumbent, partitions):
    """Recompute all outcomes; missing/invalid cohorts produce retained rejection."""
    validate_model(candidate);validate_model(incumbent)
    need(type(partitions) is dict and set(partitions)==set(ROLES[1:]),'evaluation cohorts')
    need(all(type(items) is list and 1<=len(items)<=8 for items in partitions.values()),'bounded evaluation cohorts')
    need(sum(map(len,partitions.values()))<=8 and len(_encode(partitions))<=8*400000,'evaluation total budget')
    reasons=[];metrics={};seen={r['content_sha256'] for r in candidate['training']}
    for role in ROLES[1:]:
        try:
            for item in partitions[role]:
                measured(item);content=item['capture']['result']['payload']['content_sha256']
                need(content not in seen, 'cross-cohort/training content overlap');seen.add(content)
            metrics[role]=dict(candidate=metric(candidate,partitions[role]),incumbent=metric(incumbent,partitions[role]),
                               unigram=metric(candidate,partitions[role],unigram=True))
        except ValueError as exc: reasons.append(role+': '+str(exc))
    for role in ROLES[1:3]:
        if role not in metrics:continue
        m=metrics[role];new=m['candidate']['mean_log_loss']
        for label,gain,minimum in [('incumbent',m['incumbent']['mean_log_loss']-new,PROTOCOL['minimum_new_gain_over_incumbent']),
             ('uniform',math.log(4)-new,PROTOCOL['minimum_new_gain_over_uniform']),
             ('learned_unigram',m['unigram']['mean_log_loss']-new,PROTOCOL['minimum_new_gain_over_learned_unigram'])]:
            if gain<minimum:reasons.append(role+': insufficient gain over '+label)
    if 'retention' in metrics:
        m=metrics['retention']
        if m['candidate']['mean_log_loss']-m['incumbent']['mean_log_loss']>PROTOCOL['maximum_old_loss_increase']:reasons.append('retention degradation')
    return dict(schema='compact-study-evaluation-1',protocol=dict(PROTOCOL),metrics=metrics,failure_reasons=reasons,
                decision='REJECT' if reasons else 'ACCEPT',candidate_sha256=_hash(candidate),incumbent_sha256=_hash(incumbent))

def validate_plan(plan, state):
    need(type(plan) is dict and set(plan)==set(ROLES), 'four explicit cohorts')
    need(state['inventory']['valid'], 'current successful LIST required')
    visible={e['relative_path'] for e in state['inventory']['entries'] if e['kind']=='file'}
    monitored={g['path'] for g in state['goals']};assigned=set()
    for role in ROLES:
        paths=plan[role];need(type(paths) is list and 1<=len(paths)<=5, 'bounded nonempty cohort')
        for path in paths:
            validate_relative_path(path)
            need(path in visible and path in monitored and path.casefold() not in assigned and path not in state['latest_by_path'], 'distinct LIST-bound unread monitored target')
            assigned.add(path.casefold())
    need(len(assigned)<=8, 'eight total targets')
    return copy.deepcopy(plan)


def fit_cumulative(records,resolve,parent,parent_ref):
    """Versioned finite update; parent identity/history is verified by caller.

    New training references only; older measured evidence remains in immutable
    parent lineage, not relabeled as newly observed data.
    """
    validate_model(parent)
    need(parent==uniform_model() or parent['schema']==CUMULATIVE_SCHEMA,'cumulative lineage begins at uniform or verified cumulative parent')
    need(parent['version']<3,'three cumulative model generations')
    model=fit(records,resolve,parent,parent_ref)
    model.update(schema=CUMULATIVE_SCHEMA,fit_mode='CUMULATIVE_PARENT_PLUS_NEW',protocol_sha256=_hash(CUMULATIVE_PROTOCOL))
    model['counts']=[[parent['counts'][i][j]+model['counts'][i][j] for j in range(4)] for i in range(4)]
    return validate_model(model)
