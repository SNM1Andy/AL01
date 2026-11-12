"""All-block initial AL-01 embryo; functional mechanisms and locked gates labeled.

This is a fresh reference individual, not migration of the existing cloud user.
One atomic joint bundle includes cognition, CGL, development, control and audits.
"""
import copy,json,uuid,math,re
from pathlib import Path
from .core import Individual,World,canonical,digest,fail
from .coupled_runtime import CoupledRuntime,require
from .coupling import visible_candidates,rank_candidates,execute_observation
from .instinct import EventDrive
from .durable import WorldDispatcher
from .dispatch import DispatchJournal
from .contracts import execute_neuron
from .neural_control import evaluate_u0,validate_u0,candidate_offset as neural_offset
from .semantic_memory import initial_semantic_memory,apply_observation,validate_semantic_replay,query_entity
from .skills import extract_candidate,validate_skill,select_skill_action
from .governance import initial_state as initial_governance,schedule,arbitrate
from .development import (initial_development,validate_development,apply_feedback,apply_interaction,
                          score_adjustment,emotion_adjustment,relationship_value)
from .control_embryos import initial_control,update_control,candidate_offset,governance_allocate,teacher_status,teacher_request

SCHEMA='al01-integrated-alpha-3'

class IntegratedLife(CoupledRuntime):
    def __init__(self,*args,**kwargs):
        self.modules={'scheduler':initial_governance(),'compute_budget':4,'skills':{'catalog':{},'active':None,'completed':[],'aborted':[]},'semantic_memory':initial_semantic_memory(),'development':initial_development(),'control':initial_control(),'score_audit':[],
            'human_statements':[],'social_ledger':{'id':'sandbox_interactions','version':0,'tick':0,'events':[]},
            'governance':[],'paused':False,'control_audit':[],'switches':{'experience':True,'emotion':True,'ucm':True,'relation':True}}
        self.generation=0;self.parent_bundle=None
        super().__init__(*args,**kwargs)
    def _payload(self):
        core=super()._payload()
        core['versions']=dict(core['versions'],schema='joint-sandbox-multi-1')
        # v1's strict birth-parent rule applies to its unchanged cognitive subdocument.
        if self.individual.state['tick']==0:core['parent_joint_sha256']=None
        return {'schema':SCHEMA,'generation':self.generation,'parent_bundle':self.sha,
                'core_joint':core,'modules':copy.deepcopy(self.modules)}
    @staticmethod
    def _decode(data):
        require(isinstance(data,dict) and set(data)=={'schema','generation','parent_bundle','core_joint','modules'} and data['schema']==SCHEMA,'integrated schema')
        require(type(data['generation']) is int and 0<=data['generation']<=1024,'generation')
        require(data['parent_bundle'] is None or isinstance(data['parent_bundle'],str) and re.fullmatch('[0-9a-f]{64}',data['parent_bundle']),'bundle parent')
        individual,solver,calibration=CoupledRuntime._decode(data['core_joint'],multi_action=True)
        m=data['modules'];require(isinstance(m,dict) and set(m)=={'scheduler','compute_budget','skills','semantic_memory','development','control','score_audit','human_statements','social_ledger','governance','paused','control_audit','switches'},'module fields')
        require(data['generation']==individual.state['tick']+len(m['control_audit']),'generation/control lineage')
        require((data['parent_bundle'] is None)==(data['generation']==0),'bundle parent presence')
        skills=m['skills'];require(type(skills) is dict and set(skills)=={'catalog','active','completed','aborted'},'skill state')
        require(type(skills['catalog']) is dict and len(skills['catalog'])<=8,'skill catalog budget')
        for sid,skill in skills['catalog'].items():
            require(sid==skill['id'],'catalog ID');validate_skill(skill,individual.state)
        active=skills['active']
        if active is not None:
            require(type(active) is dict and set(active)=={'id','cursor','start_tick','action_ids'},'active skill shape')
            require(active['id'] in skills['catalog'] and skills['catalog'][active['id']]['status']=='TESTED_PASS','active tested skill')
            require(type(active['cursor']) is int and active['cursor'] in (0,1) and type(active['start_tick']) is int and 0<=active['start_tick']<individual.state['tick'],'skill cursor')
            require(type(active['action_ids']) is list and len(active['action_ids'])==active['cursor']+int(data['core_joint']['phase']=='REQUESTED'),'skill action count')
            require(active['start_tick']+2*active['cursor']+int(data['core_joint']['phase']=='REQUESTED')==individual.state['tick'],'skill active clock')
            for i,aid in enumerate(active['action_ids']):
                event=individual.state['events'][active['start_tick']+2*i]
                require(event['kind']=='commit' and event['payload']['action_requested']['action_id']==aid,'active skill committed identity')
                proposal=event['payload']['action_requested']['proposal'];pattern=skills['catalog'][active['id']]['pattern']
                if i==0:require(proposal==pattern[0],'active skill movement binding')
                else:
                    require(proposal['kind']=='observe','active skill observation binding')
                    request=event['payload']['action_requested'];visible=individual.world.visible(request['position'])
                    expected=[request['position'][j]+pattern[1]['offset'][j] for j in range(2)]
                    require(proposal['target'] in visible and visible[proposal['target']]['position']==expected,'active relative observation binding')

        require(type(skills['aborted']) is list and len(skills['aborted'])<=32,'skill abort budget')
        for record in skills['aborted']:
            require(type(record) is dict and set(record)=={'id','start_tick','end_tick','action_ids','reason'} and record['id'] in skills['catalog'],'skill abort fields')
            require(type(record['start_tick']) is int and record['end_tick']==record['start_tick']+2 and record['end_tick']<=individual.state['tick'] and record['reason']=='OBSERVATION_PRECONDITION_NOT_MET','skill abort clock/reason')
            move=individual.state['events'][record['start_tick']]['payload']['action_requested']
            require(move['proposal']==skills['catalog'][record['id']]['pattern'][0] and record['action_ids']==[move['action_id']],'aborted movement identity')
            position=individual.state['events'][record['end_tick']-1]['payload']['position'];offset=skills['catalog'][record['id']]['pattern'][1]['offset']
            expected=[position[i]+offset[i] for i in range(2)];observed={e['payload']['target'] for e in individual.state['events'][:record['end_tick']] if e['kind']=='feedback'}
            require(not any(name not in observed and item['position']==expected for name,item in individual.world.visible(position).items()),'abort requires missing eligible observation')

        require(type(skills['completed']) is list and len(skills['completed'])<=32,'skill completion budget')
        for record in skills['completed']:
            require(type(record) is dict and set(record)=={'id','start_tick','end_tick','action_ids','outcome'} and record['id'] in skills['catalog'],'skill completion fields')
            require(type(record['start_tick']) is int and record['end_tick']==record['start_tick']+4 and record['end_tick']<=individual.state['tick'],'skill completion clock')
            require(record['action_ids']==[individual.state['events'][i]['payload']['action_requested']['action_id'] for i in (record['start_tick'],record['start_tick']+2)],'skill completed action binding')
            final=individual.state['events'][record['end_tick']-1]
            require(final['kind']=='feedback' and record['outcome']==('positive_feedback' if final['payload']['value']>0 else 'nonpositive_feedback'),'skill completion outcome')
            pattern=skills['catalog'][record['id']]['pattern'];start=record['start_tick'];move=individual.state['events'][start]['payload']['action_requested']
            require(move['proposal']==pattern[0],'completed skill movement pattern')
            position=individual.state['events'][start+1]['payload']['position']
            require(final['payload']['position']==[position[i]+pattern[1]['offset'][i] for i in range(2)],'completed relative observation pattern')
        intervals=sorted([(r['start_tick'],r['end_tick'],r['action_ids']) for r in skills['completed']+skills['aborted']]+([(active['start_tick'],individual.state['tick'],active['action_ids'])] if active is not None else []))
        seen_actions=set();last_end=-1
        for start,end,ids in intervals:
            require(start>=last_end and not(seen_actions&set(ids)) and len(set(ids))==len(ids),'nonoverlapping unique skill executions')
            last_end=end;seen_actions.update(ids)


        validate_semantic_replay(m['semantic_memory'],individual.state)
        validate_development(m['development']);require(type(m['paused']) is bool,'pause flag')
        require(set(m['switches'])=={'experience','emotion','ucm','relation'} and all(type(v) is bool for v in m['switches'].values()),'module switches')
        # Pure control API validates persisted state without committing an update.
        candidate_offset(m['control'],{'tokens':[],'novelty':0.})
        for key,limit in [('score_audit',64),('human_statements',64),('governance',64),('control_audit',256)]:
            require(isinstance(m[key],list) and len(m[key])<=limit,'module budget '+key)
        ledger=m['social_ledger'];require(set(ledger)=={'id','version','tick','events'} and ledger['id']=='sandbox_interactions','social ledger')
        from .neuron_queries import _verified_events
        _verified_events(ledger,('read','neuron'))
        statement_ids=set()
        for record in m['human_statements']:
            require(isinstance(record,dict) and set(record)=={'id','text','source','status','core_tick'} and record['source']=='human_external' and record['status']=='UNVERIFIED','human statement')
            require(isinstance(record['text'],str) and 0<len(record['text'].strip()) and len(record['text'])<=1024,'statement bounds')
            record['text'].encode('utf8')
            require(isinstance(record['id'],str) and re.fullmatch('human_[0-9a-f]{32}',record['id']) and record['id'] not in statement_ids,'statement ID')
            require(type(record['core_tick']) is int and 0<=record['core_tick']<=individual.state['tick'],'statement tick');statement_ids.add(record['id'])
        # Replay bounded operator controls to check derived flags and intake linkage.
        compute_budget=4
        paused=False;switches={'experience':True,'emotion':True,'ucm':True,'relation':True}
        statements=[];social_payloads=[];adopted_skills=set();adoption_ticks={};last_tick=0;last_time=0.
        for entry in m['control_audit']:
            require(type(entry) is dict and set(entry)=={'kind','payload','core_tick','field_time'},'control audit fields')
            tick=entry['core_tick'];time=entry['field_time'];payload=entry['payload'];kind=entry['kind']
            require(type(tick) is int and last_tick<=tick<=individual.state['tick'],'control tick order')
            require(type(time) in (int,float) and math.isfinite(time) and last_time<=time<=solver.time+1e-12,'control clock order')
            expected=0. if tick<2 else ((tick-(tick%2))+1)*.05
            require(abs(time-expected)<1e-10,'control fixed clock binding')
            require(type(payload) is dict,'control payload');last_tick=tick;last_time=time
            if kind in ('pause','resume'):
                require(payload=={},'pause payload');paused=kind=='pause'
            elif kind=='switches':
                require(set(payload)==set(switches) and all(type(v) is bool for v in payload.values()),'switch audit')
                switches=payload
            elif kind=='statement':
                require(set(payload)=={'id','text'},'statement audit')
                statements.append(dict(payload,source='human_external',status='UNVERIFIED',core_tick=tick))
            elif kind=='compute_budget':
                require(set(payload)=={'budget'} and type(payload['budget']) is int and 2<=payload['budget']<=4,'compute budget audit');compute_budget=payload['budget']
            elif kind=='adopt_skill':
                require(set(payload)=={'skill'} and payload['skill']['status']=='TESTED_PASS','skill adoption audit')
                validate_skill(payload['skill'],individual.state)
                require(payload['skill']['id'] in skills['catalog'] and skills['catalog'][payload['skill']['id']]==payload['skill'],'adopted skill binding')
                require(payload['skill']['source']['events'][-1]['tick']<=tick,'adoption after discovery');adopted_skills.add(payload['skill']['id']);adoption_ticks.setdefault(payload['skill']['id'],tick)
            elif kind=='sandbox_interaction':
                require(set(payload)=={'sandbox','interaction_id','object_id','outcome'} and payload['sandbox'] is True,'sandbox audit')
                require(payload['outcome'] in ('cooperate','decline','harm'),'sandbox outcome')
                require(type(payload['interaction_id']) is str and re.fullmatch('sandbox_[0-9a-f]{32}',payload['interaction_id']),'sandbox ID')
                historical_position=[0,0]
                for event in individual.state['events'][:tick]:
                    if event['kind']=='action_result':historical_position=event['payload']['position']
                require(payload['object_id'] in individual.world.visible(historical_position),'sandbox historical object')
                social_payloads.append(payload)
            else:require(False,'unknown control kind')
        require(adopted_skills=={sid for sid,skill in skills['catalog'].items() if skill['status']=='TESTED_PASS'},'tested catalog requires adoption history')
        for execution in skills['completed']+skills['aborted']+([skills['active']] if skills['active'] is not None else []):
            require(execution['id'] in adoption_ticks and execution['start_tick']>=adoption_ticks[execution['id']],'skill execution after tested adoption')
        require(type(m['compute_budget']) is int and m['compute_budget']==compute_budget,'compute budget replay')
        require(paused==m['paused'] and switches==m['switches'],'control replay state')
        require(statements==m['human_statements'],'statement audit linkage')
        require(social_payloads==[e['payload'] for e in ledger['events']],'social audit linkage')
        require(len({p['interaction_id'] for p in social_payloads})==len(social_payloads),'duplicate social IDs')
        require(m['control']['steps']==individual.state['closed_loops'],'controller/feedback count')
        for ref in m['development']['processed']:
            source=individual.state if ref['ledger']==individual.state['id'] else ledger if ref['ledger']==ledger['id'] else None
            require(source is not None and ref['index']<len(source['events']) and source['events'][ref['index']]['hash']==ref['hash'],'development source binding')
        feedback_refs={e['index'] for e in individual.state['events'] if e['kind']=='feedback'}
        processed={r['index'] for r in m['development']['processed'] if r['ledger']==individual.state['id']}
        social_processed={r['index'] for r in m['development']['processed'] if r['ledger']==ledger['id']}
        require(social_processed==set(range(len(ledger['events']))),'development must consume every social fixture')
        require(feedback_refs==processed,'development must reflect every committed feedback exactly once')
        require(len(m['score_audit'])==len(data['core_joint']['decision_traces']) and len(m['governance'])==len(m['score_audit']),'decision module audit count')
        scheduler_state=initial_governance()
        for audit,trace,gov in zip(m['score_audit'],data['core_joint']['decision_traces'],m['governance']):
            require(set(audit)=={'action_id','adjustments','development_version'} and audit['action_id']==trace['action_id'],'score audit binding')
            require(type(audit['development_version']) is int and 0<=audit['development_version']<=m['development']['version'],'score development version')
            require(type(gov) is dict and set(gov)=={'allocation','compare','recall','neural','neural_allocation','service','arbitration'} and gov['allocation']==gov['neural_allocation'],'governance allocation')
            from .contracts import NEURONS,validate_type
            for name in ('compare','recall'):
                result=gov[name];spec=NEURONS[name]
                require(type(result) is dict and result.get('module')==name and result.get('module_id')==spec.module_id and result.get('module_version')==spec.module_version,'governance neuron identity')
                require(result.get('read_state_version')==trace['source_state_version'] and result.get('proposals')==[],'governance source/authority')
                validate_type(result.get('result'),spec.returns)
            require(len(trace['candidate_inputs'])<=3,'four-neuron decision budget')
            neural=gov['neural'];source_tick=trace['source_tick']
            source={'id':individual.state['id'],'version':source_tick,'tick':source_tick,'events':individual.state['events'][:source_tick]}
            require(neural['source']['tick']==source_tick and neural['source']['state_version']==source_tick,'neural decision source')
            service=gov['service'];require(type(service) is dict and set(service)=={'jobs','budget','result'},'service fields')
            historical_budget=4
            for control in m['control_audit']:
                if control['core_tick']<=source_tick and control['kind']=='compute_budget':historical_budget=control['payload']['budget']
            require(type(service['budget']) is int and service['budget']==historical_budget,'service historical budget')
            jobs=service['jobs'];require(type(jobs) is list and 2<=len(jobs)<=4,'service job profile')
            require(jobs[0]=={'id':'service_recall','module':'recall','cost':1,'gain':1.},'service recall rule')
            for job in jobs[1:]:
                require(type(job) is dict and set(job)=={'id','module','cost','gain','action_id'} and type(job['action_id']) is str,'service compare shape')
                require(job=={'id':'service_'+job['action_id'],'module':'compare','cost':1,'gain':.5,'action_id':job['action_id']},'service compare rule')

            scheduled=schedule(scheduler_state,service['jobs'],source,budget=service['budget'],module_caps={'recall':1,'compare':3},minimum_service=1)
            require(service['result']==scheduled,'scheduler causal replay');scheduler_state=scheduled['state']
            require(set(scheduled['eligible_action_ids'])=={c['entity_ref'] for c in trace['candidate_inputs']},'service action eligibility')
            proposals=[{'id':'rank_'+c['entity_ref'],'module':'compare','action_id':c['entity_ref'],'score':1.-i/max(1,len(trace['ranked'])),'evidence_refs':[]} for i,c in enumerate(trace['ranked'])]
            decision=arbitrate(proposals,source,eligible_action_ids=scheduled['eligible_action_ids'])
            require(gov['arbitration']==decision and decision['status']=='APPROVED' and decision['action_id']==trace['selected'],'single approved decision')

            neural_inputs=neural['candidate_inputs']
            require(len(neural_inputs)==len(trace['candidate_inputs']),'neural candidate count')
            source_position=[0,0]
            for event in source['events']:
                if event['kind']=='action_result':source_position=event['payload']['position']
            source_visible=individual.world.visible(source_position)
            movement_offsets={'move_east':(1,0),'move_west':(-1,0),'move_south':(0,1),'move_north':(0,-1)}
            require(all(j['action_id'] in source_visible or j['action_id'] in movement_offsets for j in jobs[1:]),'service public actions')
            for ni,ci,row in zip(neural_inputs,trace['candidate_inputs'],audit['adjustments']):
                require(ni['entity_ref']==ci['entity_ref'] and ni['baseline_score']==row['base'],'neural baseline binding')
                name=ni['entity_ref']
                if name in movement_offsets:
                    dx,dy=movement_offsets[name];expected_position=[source_position[0]+dx,source_position[1]+dy]
                    require(0<=expected_position[0]<individual.world.width and 0<=expected_position[1]<individual.world.height,'neural move bounds')
                else:
                    require(name in source_visible,'neural target visible at source');expected_position=source_visible[name]['position']
                require(ni['position']==expected_position,'neural public position binding')

                for key in ('novelty','persistence','switching'):require(ni[key]==ci[key],'neural candidate coefficients')
            validate_u0(neural,source,neural_inputs,capabilities=individual.vm.capabilities)
            first_compare=next(r['compare_contract'] for r in neural['ranked'] if r['entity_ref']==neural_inputs[0]['entity_ref'])
            require(gov['compare']==first_compare and gov['recall']==neural['recall_contract'],'reused neuron outputs')
            neural_jobs=[{'id':'neural_recall','module':'recall','cost':1,'gain':1.}]+[{'id':'neural_compare_'+str(i),'module':'compare','cost':1,'gain':.5} for i in range(len(neural_inputs))]
            require(gov['neural_allocation']==governance_allocate(neural_jobs,budget=4),'neural governance binding')

            require(len(audit['adjustments'])==len(trace['candidate_inputs']),'score candidate count')
            for row,candidate in zip(audit['adjustments'],trace['candidate_inputs']):
                require(set(row)=={'entity_ref','base','experience','emotion','ucm','relation'} and row['entity_ref']==candidate['entity_ref'],'score components')
                for key,bound in [('base',1.),('experience',.1),('emotion',.05),('ucm',.03),('relation',.01)]:
                    require(type(row[key]) in (int,float) and math.isfinite(row[key]) and abs(row[key])<=bound+1e-12,'score component bounds')
                require(abs(sum(row[k] for k in ('base','experience','emotion','ucm','relation'))-candidate['baseline_score'])<1e-12,'score decomposition')
        require(m['scheduler']==scheduler_state,'persistent scheduler replay')
        return individual,solver,calibration
    def _load(self):
        require(self.path.is_file() and self.path.stat().st_size<=24*1024*1024,'integrated checkpoint size')
        try:
            wrapper=json.loads(self.path.read_text(encoding='utf-8'));require(set(wrapper)=={'data','sha256'} and digest(wrapper['data'])==wrapper['sha256'],'integrated integrity')
            data=wrapper['data'];self.individual,self.solver,self.calibration=self._decode(data);c=data['core_joint']
            self.phase=c['phase'];self.enabled=c['enabled'];self.previous_target=c['previous_target'];self.traces=c['decision_traces'];self.sha=wrapper['sha256']
            self.modules=copy.deepcopy(data['modules']);self.generation=data['generation'];self.parent_bundle=data['parent_bundle']
        except (KeyError,ValueError,TypeError,UnicodeError,OverflowError,RecursionError) as exc:fail('JOINT_INVALID',str(exc))
    def step(self):
        with self._exclusive():
            self._load();self._association()
            if self.modules['paused']:return False
            try:
                dispatcher=WorldDispatcher(self.individual.world,DispatchJournal(self.journal_path))
                if self.phase=='READY':
                    if len(self.traces)>=64 or self.individual.state['lifecycle']!='ACTIVE' or self.individual.state['goal_state']['status']=='BLOCKED':return False
                    from .multi_action_policy import action_candidates,execute_action
                    choice=action_candidates(self.individual,self.previous_target)
                    skill_selection=None;skill_state=self.modules['skills'];active=skill_state['active']
                    if active is not None:
                        skill_selection=select_skill_action(skill_state['catalog'][active['id']],self.individual,active['cursor'])
                        require(skill_selection is not None,'active skill precondition no longer holds')
                    else:
                        disabled={r['id'] for r in skill_state['completed'] if r['outcome']=='nonpositive_feedback'}|{r['id'] for r in skill_state['aborted']}
                        for skill in skill_state['catalog'].values():
                            if skill['id'] in disabled:continue
                            skill_selection=select_skill_action(skill,self.individual,0)
                            if skill_selection is not None:break
                    if skill_selection is not None:
                        sid=skill_selection['candidate_id'];action=skill_selection['action']
                        choice={'candidates':[{'entity_ref':sid,'baseline_score':.02,'novelty':1.,'persistence':0.,'switching':0.}],
                                'actions':{sid:action},'positions':{sid:skill_selection['position']}}
                    candidates=choice['candidates']
                    if not candidates:return False
                    # One recall plus at most three registered comparisons per decision.
                    if len(candidates)>3:candidates=[candidates[i] for i in (0,(len(candidates)-1)//2,len(candidates)-1)]
                    service_jobs=[{'id':'service_recall','module':'recall','cost':1,'gain':1.}]+[{'id':'service_'+c['entity_ref'],'module':'compare','cost':1,'gain':.5,'action_id':c['entity_ref']} for c in candidates]
                    service_result=schedule(self.modules['scheduler'],service_jobs,self.individual.state,budget=self.modules['compute_budget'],module_caps={'recall':1,'compare':3},minimum_service=1)
                    require(any(j['id']=='service_recall' for j in service_result['allocations']),'governance recall allocation')
                    candidates=[c for c in candidates if c['entity_ref'] in service_result['eligible_action_ids']]
                    require(bool(candidates),'governance candidate allocation')
                    self.modules['scheduler']=service_result['state']
                    service={'jobs':service_jobs,'budget':self.modules['compute_budget'],'result':service_result}
                    dev=self.modules['development'];adjustments=[]
                    neural_jobs=[{'id':'neural_recall','module':'recall','cost':1,'gain':1.}]+[{'id':'neural_compare_'+str(i),'module':'compare','cost':1,'gain':.5} for i in range(len(candidates))]
                    neural_allocation=governance_allocate(neural_jobs,budget=4)
                    require(len(neural_allocation['allocations'])==len(neural_jobs),'neural allocation before execution')
                    neural_inputs=[dict(c,position=choice['positions'][c['entity_ref']]) for c in candidates]
                    neural=evaluate_u0(self.individual.state,neural_inputs,capabilities=self.individual.vm.capabilities,enabled=self.modules['switches']['ucm'])
                    allocation=neural_allocation;recalled=neural['recall_contract'];neuron=next(r['compare_contract'] for r in neural['ranked'] if r['entity_ref']==candidates[0]['entity_ref'])

                    for c in candidates:
                        target=c['entity_ref'];flags=self.modules['switches'];is_observation=choice['actions'][target]['kind']=='observe'
                        q=score_adjustment(dev,choice['positions'][target]) if flags['experience'] and is_observation else 0.
                        emotion=emotion_adjustment(dev,novelty=c['novelty'],switching=c['switching']) if flags['emotion'] else 0.
                        u=.5*(candidate_offset(self.modules['control'],{'tokens':[target],'novelty':c['novelty']})+neural_offset(neural,target)) if flags['ucm'] else 0.
                        relation=.02*(relationship_value(dev,target)-.5) if flags['relation'] and is_observation else 0.
                        adjustments.append({'entity_ref':target,'base':c['baseline_score'],'experience':q,'emotion':emotion,'ucm':u,'relation':relation})
                        c['baseline_score']+=q+emotion+u+relation
                    trace=rank_candidates(candidates,self.solver.manifest()['features'],self.calibration,enabled=self.enabled)
                    trace.update(candidate_inputs=copy.deepcopy(candidates),source_state_version=self.individual.state['version'],source_tick=self.individual.state['tick'],
                        source_event_head=self.individual.state['events'][-1]['hash'] if self.individual.state['events'] else '0'*64,field_time=self.solver.time)
                    proposals=[{'id':'rank_'+c['entity_ref'],'module':'compare','action_id':c['entity_ref'],'score':1.-i/max(1,len(trace['ranked'])),'evidence_refs':[]} for i,c in enumerate(trace['ranked'])]
                    arbitration=arbitrate(proposals,self.individual.state,eligible_action_ids=service_result['eligible_action_ids'])
                    require(arbitration['status']=='APPROVED' and arbitration['action_id']==trace['selected'],'approved sole executor selection')
                    trace['action_id']=execute_action(self.individual,trace['selected'],choice['actions'])
                    if skill_selection is not None:
                        if skill_state['active'] is None:skill_state['active']={'id':skill_selection['skill_id'],'cursor':0,'start_tick':trace['source_tick'],'action_ids':[]}
                        skill_state['active']['action_ids'].append(trace['action_id'])

                    self.traces.append(trace);self.phase='REQUESTED';self.modules['score_audit'].append({'action_id':trace['action_id'],'adjustments':adjustments,'development_version':dev['version']})
                    self.modules['governance'].append({'allocation':allocation,'compare':neuron,'recall':recalled,'neural':neural,'neural_allocation':neural_allocation,'service':service,'arbitration':arbitration})
                    dispatcher.prepare_request(self.individual.state['requested_actions'][0])
                else:
                    self.individual.action_dispatcher=dispatcher;require(self.individual.step(),'feedback expected')
                    event=self.individual.state['events'][-1]
                    if event['kind']=='feedback':
                        self.previous_target=event['payload']['target']
                        self.modules['development']=apply_feedback(self.modules['development'],self.individual.state,event['index'])
                        self.modules['semantic_memory']=apply_observation(self.modules['semantic_memory'],self.individual.state,event['index'])
                        p=event['payload'];memory=self.modules['governance'][-1]['neural']['focus'] if self.modules['switches']['ucm'] else list(self.modules['development']['content']['semantic'])[-8:]
                        self.modules['control']=update_control(self.modules['control'],{'tokens':[p['target']],'salience':min(1.,abs(p['value'])),'novelty':1.},memory)
                    else:require(event['kind']=='action_result' and event['payload']['kind']=='movement','known action result')
                    skill_state=self.modules['skills'];active=skill_state['active']
                    if active is not None:
                        require(active['action_ids'][-1]==event['payload']['action_id'],'active skill feedback association')
                        if active['cursor']==0:
                            next_selection=select_skill_action(skill_state['catalog'][active['id']],self.individual,1)
                            if next_selection is None:
                                require(len(skill_state['aborted'])<32,'skill abort capacity')
                                skill_state['aborted'].append({'id':active['id'],'start_tick':active['start_tick'],'end_tick':self.individual.state['tick'],'action_ids':active['action_ids'],'reason':'OBSERVATION_PRECONDITION_NOT_MET'})
                                skill_state['active']=None
                            else:active['cursor']=1
                        else:
                            require(len(skill_state['completed'])<32,'skill completion capacity')
                            skill_state['completed'].append({'id':active['id'],'start_tick':active['start_tick'],'end_tick':self.individual.state['tick'],'action_ids':active['action_ids'],'outcome':'positive_feedback' if event['payload']['value']>0 else 'nonpositive_feedback'})
                            skill_state['active']=None
                    candidate=extract_candidate(self.individual.state)
                    if candidate is not None and not any(s['pattern']==candidate['pattern'] for s in skill_state['catalog'].values()):
                        require(len(skill_state['catalog'])<8,'skill candidate capacity');skill_state['catalog'][candidate['id']]=candidate

                    self.solver.drive=EventDrive(self.individual.state['events'],tick_duration=.05)
                    self.solver.run((self.individual.state['tick']+1)*.05-self.solver.time);self.phase='READY'
                self.generation+=1;self._save();return True
            except Exception:self._load();raise
    def _control(self,kind,payload):
        with self._exclusive():
            self._load();self._association()
            require(len(self.modules['control_audit'])<256,'control audit budget')
            try:
                if kind=='pause':self.modules['paused']=True
                elif kind=='resume':self.modules['paused']=False
                elif kind=='statement':
                    text=payload['text'];require(isinstance(text,str) and 0<len(text.strip()) and len(text)<=1024,'text bounds');text.encode('utf8')
                    require(len(self.modules['human_statements'])<64,'statement budget')
                    self.modules['human_statements'].append({'id':payload['id'],'text':text,'source':'human_external','status':'UNVERIFIED','core_tick':self.individual.state['tick']})
                elif kind=='compute_budget':
                    require(set(payload)=={'budget'} and type(payload['budget']) is int and 2<=payload['budget']<=4,'compute budget');self.modules['compute_budget']=payload['budget']
                elif kind=='adopt_skill':
                    require(set(payload)=={'skill'},'adopt skill fields');skill=payload['skill'];validate_skill(skill,self.individual.state)
                    require(skill['status']=='TESTED_PASS' and skill['id'] in self.modules['skills']['catalog'],'only tested discovered skill adoption')
                    prior=self.modules['skills']['catalog'][skill['id']]
                    require(prior['status']=='CANDIDATE_UNVALIDATED' or prior==skill,'no replacement of tested procedure')
                    self.modules['skills']['catalog'][skill['id']]=copy.deepcopy(skill)
                elif kind=='sandbox_interaction':
                    require(payload['object_id'] in self.individual.world.visible(self.individual.state['position']),'visible sandbox interaction object')
                    ledger=self.modules['social_ledger'];i=len(ledger['events']);event={'index':i,'tick':i+1,'kind':'sandbox_interaction','payload':payload,
                        'previous':ledger['events'][-1]['hash'] if i else '0'*64};event['hash']=digest(event)
                    ledger['events'].append(event);ledger['tick']=i+1;ledger['version']=i+1
                    self.modules['development']=apply_interaction(self.modules['development'],ledger,i)
                elif kind=='switches':
                    require(set(payload)==set(self.modules['switches']) and all(type(v) is bool for v in payload.values()),'switch fields')
                    self.modules['switches']=copy.deepcopy(payload)
                else:fail('UNKNOWN_OPCODE',kind)
                self.modules['control_audit'].append({'kind':kind,'payload':copy.deepcopy(payload),'core_tick':self.individual.state['tick'],'field_time':self.solver.time})
                self.generation+=1;self._save()
            except Exception:self._load();raise
    def pause(self):self._control('pause',{})
    def resume(self):self._control('resume',{})
    def statement(self,text):
        receipt='human_'+uuid.uuid4().hex
        try:self._control('statement',{'id':receipt,'text':text})
        except OSError:fail('INTAKE_UNCERTAIN','receipt_id='+receipt+'; inspect statement list before retry')
        return {'receipt_id':receipt,'status':'recorded_UNVERIFIED','understanding_or_learning_claimed':False}
    def sandbox_interaction(self,object_id,outcome):
        self._control('sandbox_interaction',{'sandbox':True,'interaction_id':'sandbox_'+uuid.uuid4().hex,'object_id':object_id,'outcome':outcome})
    def set_compute_budget(self,budget):self._control('compute_budget',{'budget':budget})
    def adopt_skill(self,skill):self._control('adopt_skill',{'skill':skill})
    def semantic_query(self,entity_id,world_context=None):
        with self._exclusive():
            self._load();self._association()
            return query_entity(self.modules['semantic_memory'],entity_id,world_context=world_context)
    def status(self):
        with self._exclusive():
            self._load();self._association()
            return {'schema':SCHEMA,'tick':self.individual.state['tick'],'phase':self.phase,'paused':self.modules['paused'],'closed_loops':self.individual.state['closed_loops'],
            'field_time':self.solver.time,'field_features':self.solver.manifest()['features'],'development':copy.deepcopy(self.modules['development']),
            'scheduler':copy.deepcopy(self.modules['scheduler']),'compute_budget':self.modules['compute_budget'],'skills':copy.deepcopy(self.modules['skills']),'semantic_memory':copy.deepcopy(self.modules['semantic_memory']),'ucm':copy.deepcopy(self.modules['control']),'teacher':teacher_status(self.individual.state,self.modules['development']),
            'human_statements':copy.deepcopy(self.modules['human_statements']),'governance_last':copy.deepcopy(self.modules['governance'][-1:]),
            'functional_scope':'all-block initial embryo; no learning-transfer, emotion-benefit, social-intelligence or consciousness claim'}
