"""Local terminal research interface. No web server, teacher, or dialogue model."""
import json,re,uuid,os,tempfile
from pathlib import Path
from .core import Compiler,Individual,World,CLError,fail,canonical,digest
from .durable import DurableRunner
from .observatory import observation_data


def human_packet(individual,text,packet_id=None):
    if not isinstance(text,str) or not text.strip() or len(text)>1024:
        fail('TYPE_ERROR','human statement must be 1..1024 characters')
    try:text.encode('utf8')
    except UnicodeError:fail('TYPE_ERROR','human statement UTF-8')
    packet_id=packet_id or 'human_'+uuid.uuid4().hex
    if not isinstance(packet_id,str) or not re.fullmatch(r'human_[a-z0-9_]{1,64}',packet_id):
        fail('TYPE_ERROR','human packet identity')
    # User text only occupies a quoted literal, never source/IR/authority fields.
    source='EMIT(ASSERT('+json.dumps(text,ensure_ascii=True)+',"human_external",[]));'
    return {'schema_version':'CL_PACKET-0.1','packet_id':packet_id,'individual_id':individual.state['id'],
        'logical_tick':individual.state['tick'],'origin_module':'human_intake','kind':'OBSERVATION',
        'typed_body':{'source':source,'ir':Compiler(individual.candidates(),('emit',)).compile(source)},
        'context_refs':[],'provenance':'human_external_unverified','uncertainty':'UNKNOWN',
        'capability_refs':['cap_emit'],'trace_id':packet_id}


class ResearchSession:
    def __init__(self,runner):self.runner=runner
    def _refresh(self):
        # Observer reads the durable checkpoint; it cannot write cognitive state.
        self.runner.individual=Individual.load(self.runner.checkpoint)
        self.runner._check_association()
        return self.runner.individual
    def status(self):
        i=self._refresh();s=i.state
        return {'interface':'research_console','tick':s['tick'],'closed_loops':s['closed_loops'],
            'position':s['position'],'observed_count':len(s['observed']),'goal':s['goal_state']['status'],
            'inbox_count':len(s['inbox']),'teacher_enabled':False,'teacher_gate':{
                'offline_1000_ticks_evidence':'verified in baseline experiments, not inferred from this session',
                'held_out_learning_improvement':False,'reason':'no validated held-out learning improvement'},
            'meaning':'recorded simulation state, not a self-report of consciousness'}
    def say(self,text,packet_id=None):
        with self.runner._exclusive():
            i=self._refresh();packet=human_packet(i,text,packet_id)
            if i.state['lifecycle']!='ACTIVE' or i.state['goal_state']['status']=='BLOCKED':
                fail('INTAKE_BLOCKED','individual is paused or goal-blocked; no statement admitted')
            if packet['packet_id'] in i.state['packet_status']:fail('DUPLICATE_PACKET',packet['packet_id'])
            if any(p['origin_module']=='human_intake' for p in i.state['inbox']):
                fail('BUDGET_EXCEEDED','only one pending human statement')
            last=next((e['tick'] for e in reversed(i.state['events']) if e['kind']=='commit'
                and e['payload'].get('packet',{}).get('origin_module')=='human_intake'),None)
            if last is not None and i.state['tick']-last<8 and i.state['goal_state']['status'] not in ('COMPLETED','WAITING'):
                fail('BUDGET_EXCEEDED','allow eight cognitive ticks between human statements')
            try:
                i.enqueue_packet(packet);i.save(self.runner.checkpoint)
            except OSError:
                try:
                    self._refresh();status=self.runner.individual.state['packet_status'].get(packet['packet_id'],'not_found')
                except Exception:status='unknown'
                fail('INTAKE_UNCERTAIN',f"receipt_id={packet['packet_id']}; durable_status={status}; inspect before retry")
        return {'receipt_id':packet['packet_id'],'status':'queued','teacher_enabled':False,
                'meaning':'unverified statement queued; not understood, believed, or acted upon'}
    def events(self,limit=10):
        if type(limit) is not int or not 0<=limit<=20:fail('BUDGET_EXCEEDED','event display limit 0..20')
        i=self._refresh()
        events=observation_data(i)['events']
        return events[-limit:] if limit else []
    def _operator_record(self,record,reserve=1):
        """Bounded sidecar audit, not a fabricated cognitive event."""
        path=self.runner.checkpoint.parent/'operator.json'
        with self.runner._exclusive():
            if path.exists() and path.stat().st_size>1048576:fail('BUDGET_EXCEEDED','operator log size')
            records=json.loads(path.read_text()) if path.exists() else []
            if not isinstance(records,list) or len(records)+reserve>2048:fail('BUDGET_EXCEEDED','operator log records')
            previous='0'*64
            for old in records:
                if not isinstance(old,dict) or old.get('previous')!=previous or old.get('hash')!=digest({k:v for k,v in old.items() if k!='hash'}):
                    fail('STATE_CONFLICT','operator audit chain')
                previous=old['hash']
            entry=dict(record,previous=previous);entry['hash']=digest(entry);records.append(entry)
            raw=canonical(records)
            if len(raw.encode('utf8'))+(reserve-1)*1024>1048576:fail('BUDGET_EXCEEDED','operator log bytes')
            fd,name=tempfile.mkstemp(prefix='.operator-',dir=path.parent)
            try:
                with os.fdopen(fd,'w',encoding='utf8') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
                os.replace(name,path)
                if os.name!='nt':
                    directory_fd=os.open(path.parent,os.O_RDONLY)
                    try:os.fsync(directory_fd)
                    finally:os.close(directory_fd)
            finally:
                if os.path.exists(name):os.unlink(name)
    def advance(self,count):
        if type(count) is not int or not 1<=count<=100:fail('BUDGET_EXCEEDED','run requires 1..100 steps')
        operation='control_'+uuid.uuid4().hex;start=self.status()['tick']
        self._operator_record({'operation_id':operation,'phase':'requested','control':'advance','steps':count,'tick':start},reserve=2)
        try:
            done=self.runner.run(count);status=self.status()
            self._operator_record({'operation_id':operation,'phase':'completed','effective_steps':done,'tick':status['tick']})
        except Exception as exc:
            fail('CONTROL_INCOMPLETE',f'operation_id={operation}; inspect checkpoint/audit before retry; {type(exc).__name__}')
        return {'operator_control':'advance autonomous simulation','operation_id':operation,'effective_steps':done,'status':status}
    def command(self,line):
        if not isinstance(line,str) or len(line)>2048:fail('TYPE_ERROR','console command size')
        verb,_,rest=line.strip().partition(' ')
        if verb in ('status','状态') and not rest:return self.status()
        if verb in ('events','记录'):
            if rest and not re.fullmatch('[0-9]{1,2}',rest):fail('TYPE_ERROR','event count')
            return self.events(int(rest) if rest else 10)
        if verb in ('receipt','回执'):
            if not re.fullmatch(r'human_[a-z0-9_]{1,64}',rest):fail('TYPE_ERROR','receipt ID')
            return {'receipt_id':rest,'status':self._refresh().state['packet_status'].get(rest,'not_found')}
        if verb in ('say','说'):return self.say(rest)
        if verb in ('run','推进'):
            if not re.fullmatch('[0-9]{1,3}',rest) or not 1<=int(rest)<=100:fail('BUDGET_EXCEEDED','run requires 1..100 steps')
            return self.advance(int(rest))
        if verb in ('teach','teacher','教师'):fail('TEACHER_LOCKED','held-out learning gate unmet; teacher not implemented')
        if verb in ('help','帮助') and not rest:
            return {'commands':['status / 状态','events 10 / 记录 10','say text / 说 文本','receipt ID / 回执 ID','run 10 / 推进 10','quit / 退出'],
                    'warning':'say records a claim only. run is a researcher control, not an individual decision. No dialogue or teacher.'}
        fail('UNKNOWN_OPCODE','unsupported research-console command')


def main():
    import argparse
    p=argparse.ArgumentParser(description='AL-01 local research console, not a dialogue companion')
    p.add_argument('--directory',required=True,type=Path);p.add_argument('--new',action='store_true')
    p.add_argument('--objects',type=int,default=20);p.add_argument('--local',action='store_true');a=p.parse_args()
    if not 0<=a.objects<=1000:p.error('objects must be 0..1000')
    session=ResearchSession(DurableRunner(a.directory,Individual(World(a.objects,local=a.local)) if a.new else None))
    print('AL-01 研究交互台：状态/记录是只读，推进是研究者控制，说只保存未验证陈述。教师关闭。输入帮助。')
    while True:
        try:line=input('> ')
        except (EOFError,KeyboardInterrupt):break
        if line.strip() in ('quit','退出'):break
        try:print(json.dumps(session.command(line),ensure_ascii=False,indent=2))
        except CLError as exc:print(json.dumps({'error':exc.code,'detail':str(exc)},ensure_ascii=False))
        except OSError as exc:
            print(json.dumps({'error':'IO_ERROR','detail':str(exc),'instruction':'Inspect checkpoint and receipt ID before retrying an uncertain write.'}));break

if __name__=='__main__':main()
