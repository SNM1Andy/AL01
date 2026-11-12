"""AL-01 P0/P1 research foundation. Standard library, offline, bounded CL."""
from __future__ import annotations
import copy, hashlib, hmac, json, math, os, re, secrets, tempfile
from dataclasses import dataclass
from pathlib import Path

VERSION = 'CL-0.1-foundation-14'
from .limits import MAX_LOG_BYTES, MAX_MESSAGE_BYTES, MAX_CHECKPOINT_BYTES, MAX_EVENTS, MAX_OBJECTS, MAX_MESSAGES
from .routing import resolve_evidence, validate_task, run_ready_task
from .contracts import NEURONS, REGISTRY_VERSION, TaskScheduler, validate_packet, validate_snapshot
class CLError(Exception):
    def __init__(self, code, detail=''):
        self.code = code
        super().__init__(f'{code}: {detail}')

def fail(code, detail=''):
    raise CLError(code, detail)

def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)

def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()

TOKEN = re.compile(r'\s+|#[^\n]*|"(?:[^"\\]|\\.)*"|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|[$@][a-z_][a-z0-9_]*|[A-Za-z_][A-Za-z_0-9]*|[=;,(){}\[\]]')

class Parser:
    def __init__(self, text):
        if len(text) > 65536: fail('BUDGET_EXCEEDED', 'source length')
        self.tokens = []
        pos = 0
        while pos < len(text):
            m = TOKEN.match(text, pos)
            if not m: fail('LEX_ERROR', str(pos))
            token = m.group()
            pos = m.end()
            if not token.isspace() and not token.startswith('#'): self.tokens.append(token)
        self.tokens.append('<EOF>')
        self.i = 0
    def peek(self): return self.tokens[self.i]
    def pop(self, expected=None):
        t = self.peek()
        if expected is not None and t != expected: fail('PARSE_ERROR', f'expected {expected}, got {t}')
        if t == '<EOF>': fail('PARSE_ERROR', 'unexpected end')
        self.i += 1
        return t
    def block(self, end='<EOF>', depth=0):
        if depth > 8: fail('BUDGET_EXCEEDED', 'block depth')
        out = []
        while self.peek() != end:
            t = self.peek()
            if t == 'LET':
                self.pop(); name = self.pop()
                if not re.fullmatch(r'\$[a-z_][a-z0-9_]*', name): fail('PARSE_ERROR', 'variable')
                self.pop('='); value = self.expr(); self.pop(';')
                out.append({'kind':'let', 'name':name, 'value':value})
            elif t == 'IF':
                self.pop(); cond = self.expr(); self.pop('{')
                yes = self.block('}', depth+1); self.pop('}'); self.pop('ELSE'); self.pop('{')
                no = self.block('}', depth+1); self.pop('}')
                out.append({'kind':'if','cond':cond,'yes':yes,'no':no})
            elif t == 'RETURN':
                self.pop()
                # Blueprint example uses RETURN(expr); grammar also permits RETURN expr;.
                paren = self.peek() == '('
                if paren: self.pop()
                value = self.expr()
                if paren: self.pop(')')
                self.pop(';'); out.append({'kind':'return','value':value})
            else:
                value = self.expr(); self.pop(';'); out.append({'kind':'expr','value':value})
        return out
    def expr(self, depth=0):
        if depth > 8: fail('BUDGET_EXCEEDED', 'expression depth')
        t = self.pop()
        if t == '[':
            values = []
            if self.peek() != ']':
                while True:
                    values.append(self.expr(depth+1))
                    if self.peek() != ',': break
                    self.pop(',')
            self.pop(']'); return {'kind':'list','items':values}
        if t.startswith('"'):
            try: value = json.loads(t)
            except ValueError: fail('LEX_ERROR', 'string escape')
            if any(0xD800<=ord(c)<=0xDFFF for c in value): fail('LEX_ERROR','unpaired surrogate')
            return {'kind':'literal','value':value,'type':'Text'}
        if re.fullmatch(r'-?[0-9].*', t):
            try:
                value = float(t) if any(x in t for x in '.eE') else int(t)
                if abs(value) > 2**53 or not math.isfinite(value): fail('LEX_ERROR', 'numeric range')
            except (ValueError, OverflowError): fail('LEX_ERROR','numeric range')
            return {'kind':'literal','value':value,'type':'Float' if isinstance(value,float) else 'Int'}
        if t in ('TRUE','FALSE','UNKNOWN'):
            return {'kind':'literal','value':{'TRUE':True,'FALSE':False,'UNKNOWN':None}[t], 'type':'Unknown' if t=='UNKNOWN' else 'Bool'}
        if t.startswith('$'): return {'kind':'var','name':t}
        if t.startswith('@'): return {'kind':'ref','name':t[1:]}
        if not re.fullmatch(r'[A-Z][A-Z0-9_]*', t): fail('PARSE_ERROR', t)
        self.pop('('); args = []; names = []
        if self.peek() != ')':
            while True:
                name = None
                if self.i+1 < len(self.tokens) and self.tokens[self.i+1] == '=':
                    name = self.pop()
                    if not re.fullmatch(r'[a-z_][a-z0-9_]*', name): fail('PARSE_ERROR', 'argument name')
                    self.pop('=')
                names.append(name); args.append(self.expr(depth+1))
                if self.peek() != ',': break
                self.pop(',')
        self.pop(')')
        if any(n is None for n in names) and any(n is not None for n in names): fail('PARSE_ERROR','mixed arguments')
        if names and names[0] is not None and len(set(names)) != len(names): fail('PARSE_ERROR','duplicate argument')
        return {'kind':'call','op':t,'args':args,'names':names}

# P0 dictionary deliberately small. Unimplemented vocabulary fails closed.
SIGNATURES = {
    'UNRESOLVED':(['mention'], ['Text'], 'UnresolvedRef'),
    'SOME':(['value'], ['Any'], 'Option<T>'),
    'NONE':(['type'], ['Text'], 'Option<T>'),
    'OK':(['value'], ['Any'], 'Result<T>'),
    'ERR':(['code','type'], ['Text','Text'], 'Result<T>'),
    'IS_SOME':(['option'], ['Any'], 'Bool'),
    'IS_OK':(['result'], ['Any'], 'Bool'),
    'UNWRAP_OR':(['value','fallback'], ['Any','Any'], 'T'),
    'SELF_STATE':([], [], 'Request<StateRef>'),
    'EVENT_REF':(['index'], ['Int'], 'EventRef'),
    'STATE_REF':([], [], 'StateRef'),
    'UNKNOWN_OBJECTS':([], [], 'Request<List<EntityRef>>'),
    'QUERY':(['request'], ['Request<T>'], 'T'),
    'LEN':(['items'], ['List<T>'], 'Int'),
    'GT':(['left','right'], ['Int','Int'], 'Bool'),
    'FIRST':(['items'], ['List<T>'], 'T'),
    'OBSERVE':(['target'], ['EntityRef'], 'Action'),
    'MOVE':(['dx','dy'], ['Int','Int'], 'Action'),
    'ASK_LOCAL':(['target','request'], ['EntityRef','Text'], 'Action'),
    'REQUEST_HELP':(['target','request'], ['EntityRef','Text'], 'Action'),
    'INTERACT':(['target','request'], ['EntityRef','Text'], 'Action'),
    'WAIT':(['reason'], ['Text'], 'WaitRequest'),
    'PROPOSE':(['action'], ['Action'], 'ProposalRef'),
    'EMIT':(['value'], ['Any'], 'MessageRef'),
    'RECALL':(['query','limit'], ['Text','Int'], 'List<Record>'),
    'ASSERT':(['proposition','source','evidence'], ['Text','Text','List<Text>'], 'Claim'),
    'BELIEF':(['proposition','source','evidence'], ['Text','Text','List<Text>'], 'Belief'),
    'HYP':(['proposition','source','evidence'], ['Text','Text','List<Text>'], 'Hypothesis'),
}
CAPS = {'QUERY':'read', 'OBSERVE':'observe', 'EMIT':'emit', 'RECALL':'read', 'CALL':'neuron', 'MOVE':'move', 'EVENT_REF':'read', 'STATE_REF':'read'}

# Local actions are proposals only; these capabilities are never implicit grants.
LOCAL_ACTION_OPS = {'ASK_LOCAL':'ask_local', 'REQUEST_HELP':'request_help', 'INTERACT':'interact'}
LOCAL_ACTION_REQUEST_MAX_BYTES = 1024
CAPS.update(LOCAL_ACTION_OPS)

def validate_local_action_request(request):
    if type(request) is not str: fail('TYPE_ERROR','local action request must be Text')
    try: size = len(request.encode('utf8'))
    except UnicodeEncodeError: fail('ACTION_INVALID','local action request must be UTF-8')
    if not 1 <= size <= LOCAL_ACTION_REQUEST_MAX_BYTES:
        fail('ACTION_INVALID','local action request must be 1..1024 UTF-8 bytes')

class Compiler:
    def __init__(self, refs=(), capabilities=('read','observe','emit','neuron','move'), *, snapshot=None):
        self.refs = set(refs); self.caps = frozenset(capabilities); self.count = 0; self.seq = 0
        self.snapshot=snapshot;self.reference_scope=None;self.reference_count=0
    def expr(self, node, env):
        self.count += 1
        if self.count > 256: fail('BUDGET_EXCEEDED','IR operations')
        k = node['kind']; out = copy.deepcopy(node)
        if k == 'literal': return out, node['type']
        if k == 'var':
            if node['name'] not in env: fail('TYPE_ERROR','unbound variable')
            out['name'], typ = env[node['name']]; out['type']=typ; return out, typ
        if k == 'ref':
            if node['name'] not in self.refs: fail('UNRESOLVED_REF',node['name'])
            out['type']='EntityRef'; return out,'EntityRef'
        if k == 'list':
            pairs=[self.expr(v, env) for v in node['items']]
            types={t for _,t in pairs}
            if len(types)>1: fail('TYPE_ERROR','heterogeneous list')
            typ='List<'+(next(iter(types)) if types else 'Empty')+'>'
            out['items']=[v for v,_ in pairs]; out['type']=typ; return out,typ
        op=node['op']
        if op=='CALL':
            if not node['names'] or node['names'][0] is None or 'module' not in node['names']: fail('TYPE_ERROR','CALL needs named module literal')
            module=node['args'][node['names'].index('module')]
            if module.get('kind')!='literal' or module.get('type')!='Text': fail('TYPE_ERROR','module must be fixed text')
            spec=NEURONS.get(module['value'])
            if spec is None: fail('UNKNOWN_OPCODE',module['value'])
            if set(spec.required_capabilities)-self.caps: fail('CAPABILITY_DENIED','neuron requirements')
            signature=(['module']+list(spec.parameters), ['Text']+list(spec.types), spec.returns)
        elif op not in SIGNATURES: fail('UNKNOWN_OPCODE',op)
        else: signature=SIGNATURES[op]
        cap=CAPS.get(op)
        if cap and cap not in self.caps: fail('CAPABILITY_DENIED',cap)
        names, expected, result = signature
        args=node['args']
        if len(args)!=len(expected): fail('TYPE_ERROR','argument count')
        if node['names'] and node['names'][0] is not None:
            if set(node['names'])!=set(names): fail('TYPE_ERROR','argument names')
            args=[args[node['names'].index(n)] for n in names]
        pairs=[self.expr(v, env) for v in args]
        if op in ('ASSERT','BELIEF','HYP') and 'read' not in self.caps:
            evidence=args[2]
            if evidence.get('kind')!='list' or evidence.get('items')!=[]:
                fail('CAPABILITY_DENIED','evidence resolution requires read')
        from .sum_types import SUM_OPS,infer_sum_type
        if op in SUM_OPS:
            typ=infer_sum_type(op,pairs)
            out.update(args=[v for v,_ in pairs],names=names,type=typ)
            return out,typ
        generic=None
        for (_,actual), want in zip(pairs,expected):
            if want=='Any': continue
            if '<T>' in want:
                prefix=want.split('<')[0]+'<'
                if not actual.startswith(prefix) or not actual.endswith('>'): fail('TYPE_ERROR',f'{want}, got {actual}')
                generic=actual[len(prefix):-1]
            elif actual!=want and not (want.startswith('List<') and actual=='List<Empty>'): fail('TYPE_ERROR',f'{want}, got {actual}')
        if op in LOCAL_ACTION_OPS and pairs[1][0].get('kind')=='literal':
            validate_local_action_request(pairs[1][0]['value'])
        typ=generic if result=='T' else result
        out.update(args=[v for v,_ in pairs],names=names,type=typ)
        if op in ('EVENT_REF','STATE_REF'):
            self.reference_count+=1
            if self.reference_count>64: fail('BUDGET_EXCEEDED','explicit reference bindings')
            if self.snapshot is None: fail('UNRESOLVED_REF','explicit reference requires snapshot context')
            from .references import ReferenceScope,reference_data
            if self.reference_scope is None:self.reference_scope=ReferenceScope(self.snapshot,self.caps)
            if op=='EVENT_REF':
                if args[0].get('kind')!='literal' or pairs[0][1]!='Int': fail('TYPE_ERROR','event index must be a fixed Int literal')
                ref=self.reference_scope.event(args[0]['value'])
            else: ref=self.reference_scope.current()
            out['binding']=reference_data(ref)
        return out,typ
    def block(self, nodes, env):
        out=[]
        for n in nodes:
            k=n['kind']
            if k=='if':
                cond,t=self.expr(n['cond'],env)
                if t!='Bool': fail('TYPE_ERROR','IF requires Bool')
                out.append(dict(kind=k,cond=cond,yes=self.block(n['yes'],env.copy()),no=self.block(n['no'],env.copy())))
            else:
                expr,t=self.expr(n['value'],env); item=dict(kind=k,value=expr)
                if k=='let':
                    if n['name'] in env: fail('TYPE_ERROR','duplicate binding')
                    name=f'$v{self.seq}'; self.seq+=1; env[n['name']]=(name,t); item['name']=name
                out.append(item)
        return out
    def compile(self,text):
        self.count=0; self.seq=0;self.reference_scope=None;self.reference_count=0
        return {'version':VERSION,'body':self.block(Parser(text).block(),{}),'operation_count':self.count,'registry_version':REGISTRY_VERSION}

@dataclass(frozen=True)
class Ref:
    name: str
@dataclass
class Transaction:
    state_version: int
    world_version: int
    ir: dict
    proposals: list
    messages: list
    wait_reason: str | None
    neuron_calls: list
    seal: str = ''

def serializable(value):
    # Count expanded nodes, not unique Python identities: shared lists can expand
    # exponentially during JSON encoding even with a tiny source/IR budget.
    budget={'nodes':100000,'text_bytes':1048576}
    def convert(v,depth=0):
        budget['nodes']-=1
        if budget['nodes']<0 or depth>32: fail('BUDGET_EXCEEDED','expanded value size/depth')
        if isinstance(v,Ref): return convert({'entity_ref':v.name},depth+1)
        from .references import EventRef,StateRef,reference_data
        if isinstance(v,(EventRef,StateRef)): return convert(reference_data(v),depth+1)
        if isinstance(v,str):
            budget['text_bytes']-=len(v.encode('utf8'))
            if budget['text_bytes']<0: fail('BUDGET_EXCEEDED','value text bytes')
            return v
        if isinstance(v,list): return [convert(x,depth+1) for x in v]
        if isinstance(v,dict): return {k:convert(x,depth+1) for k,x in v.items()}
        return v
    return convert(value)

def transaction_payload(tx):
    return serializable({k:v for k,v in vars(tx).items() if k!='seal'})

class CVM:
    def __init__(self, capabilities=('read','observe','emit','neuron','move')):
        self.capabilities=frozenset(capabilities)
        self._seal_key=secrets.token_bytes(32)
    def verify(self,tx):
        expected=hmac.new(self._seal_key,canonical(transaction_payload(tx)).encode(),'sha256').hexdigest()
        if not hmac.compare_digest(expected,tx.seal): fail('TRANSACTION_INVALID','modified or unissued transaction')
    def compile_packet(self,packet,state,visible,handles,queued=False):
        body=validate_packet(packet,state,handles,allow_past=queued)
        caps=set().union(*(handles[h] for h in packet['capability_refs'])) if packet['capability_refs'] else set()
        if caps-set(self.capabilities): fail('CAPABILITY_DENIED','grant exceeds runtime authority')
        expected=Compiler(visible,caps,snapshot=state).compile(body['source'])
        if body['ir']!=expected: fail('PACKET_INVALID','IR does not match validated source')
        return body['source']
    def prepare_packet(self,packet,state,visible,handles,queued=False):
        source=self.compile_packet(packet,state,visible,handles,queued=queued)
        return self.prepare(source,state,visible)

    def prepare(self, source, state, visible):
        ir=Compiler(visible,self.capabilities,snapshot=state).compile(source)
        tx=Transaction(state['version'],state['world_version'],ir,[],[],None,[])
        recall_scan_remaining=1048576
        reference_scope=None
        def references():
            nonlocal reference_scope
            from .references import ReferenceScope
            if reference_scope is None:reference_scope=ReferenceScope(state,self.capabilities)
            return reference_scope
        def evaluate(n, env):
            nonlocal recall_scan_remaining
            k=n['kind']
            if k=='literal': return n['value']
            if k=='var': return env[n['name']]
            if k=='ref': return Ref(n['name'])
            if k=='list': return [evaluate(x,env) for x in n['items']]
            op=n['op']
            from .sum_types import SUM_OPS,variant,evaluate_sum
            if op=='UNWRAP_OR':
                present,value=variant(evaluate(n['args'][0],env),n['args'][0]['type'])
                return value if present else evaluate(n['args'][1],env)
            a=[evaluate(x,env) for x in n['args']]
            if op=='UNRESOLVED':
                from .sum_types import validate_runtime_value
                value={'unresolved_ref':{'mention':a[0]}}
                validate_runtime_value(value,'UnresolvedRef')
                return value
            if op in SUM_OPS: return evaluate_sum(op,a,n['type'],n['args'][0]['type'])
            if op in ('EVENT_REF','STATE_REF'):return references().resolve(n['binding'])
            if op=='CALL':
                if len(tx.neuron_calls)>=4: fail('BUDGET_EXCEEDED','neuron calls')
                from .contracts import execute_neuron
                metadata=execute_neuron(a[0],a[1:],state=state,capabilities=self.capabilities)
                result=metadata['result']
                tx.neuron_calls.append({'module':a[0],'inputs':serializable(a[1:]),'result':result,'registry':REGISTRY_VERSION,'contract':metadata})
                return result
            if op=='RECALL':
                if not 0<=a[1]<=64: fail('BUDGET_EXCEEDED','recall limit 0..64')
                if a[1]==0: return []
                found=[]
                for event in reversed(state['events']):
                    text=canonical(event); recall_scan_remaining-=len(text.encode('utf8'))
                    if recall_scan_remaining<0: fail('BUDGET_EXCEEDED','recall scan bytes')
                    if a[0] in text:
                        found.append(copy.deepcopy(event))
                        if len(found)==a[1]: break
                return found
            if op in ('ASSERT','BELIEF','HYP'):
                if a[2] and 'read' not in self.capabilities: fail('CAPABILITY_DENIED','evidence resolution requires read')
                return {'record_type':n['type'],'proposition':a[0],'source':a[1],'evidence_refs':a[2],'resolved_evidence':resolve_evidence(a[2],state),'status':'UNVERIFIED','logical_tick':state['tick']}
            if op=='SELF_STATE': return ('read','self')
            if op=='UNKNOWN_OBJECTS': return ('read','unknown')
            if op=='QUERY':
                if a[0][1]=='self': return references().current()
                return [Ref(x) for x in visible if x not in state['observed']][:32]
            if op=='LEN': return len(a[0])
            if op=='GT': return a[0]>a[1]
            if op=='FIRST':
                if not a[0]: fail('EMPTY_LIST','FIRST')
                return a[0][0]
            if op=='MOVE':
                if abs(a[0])+abs(a[1])!=1: fail('ACTION_INVALID','unit cardinal movement only')
                return {'kind':'move','dx':a[0],'dy':a[1]}
            if op=='OBSERVE': return {'kind':'observe','target':a[0].name}
            if op in LOCAL_ACTION_OPS:
                if not isinstance(a[0],Ref) or a[0].name not in visible:
                    fail('UNRESOLVED_REF','local action target outside visible references')
                validate_local_action_request(a[1])
                return {'kind':LOCAL_ACTION_OPS[op],'target':a[0].name,'request':a[1]}
            if op=='WAIT': tx.wait_reason=a[0]; return {'wait':a[0]}
            if op=='PROPOSE':
                if len(tx.proposals)>=1: fail('BUDGET_EXCEEDED','one action per tick')
                tx.proposals.append(a[0]); return {'proposal':len(tx.proposals)-1}
            if op=='EMIT':
                if len(tx.messages)>=64: fail('BUDGET_EXCEEDED','messages')
                tx.messages.append(a[0]); return {'message':len(tx.messages)-1}
            fail('UNKNOWN_OPCODE',op)
        def run(nodes,env):
            for n in nodes:
                if n['kind']=='if':
                    returned=run(n['yes'] if evaluate(n['cond'],env) else n['no'],env.copy())
                    if returned: return True
                else:
                    value=evaluate(n['value'],env)
                    if n['kind']=='let': env[n['name']]=value
                    if n['kind']=='return': return True
            return False
        run(ir['body'],{})
        tx.seal=hmac.new(self._seal_key,canonical(transaction_payload(tx)).encode(),'sha256').hexdigest()
        return tx

EXPLORER = '''LET $s = QUERY(SELF_STATE());
LET $u = QUERY(UNKNOWN_OBJECTS());
IF GT(LEN($u), 0) { LET $target = FIRST($u); PROPOSE(OBSERVE(target=$target)); }
ELSE { RETURN(WAIT(reason="no_candidate")); }'''

CONTROLLER = EXPLORER.replace('GT(LEN($u), 0)', 'CALL(module="greater", left=LEN($u), right=0)')

class World:
    def __init__(self, count=600, hazard=False, local=False):
        if type(count) is not int or not 0<=count<=MAX_OBJECTS or type(local) is not bool or type(hazard) is not bool: fail('BUDGET_EXCEEDED','world configuration')
        self.version=0; self.local=local; self.width=max(1,min(20,count)); self.height=max(1,math.ceil(count/self.width))
        self.objects={f'object_{i:04d}':{'position':[i%self.width,i//self.width], 'value':-1 if hazard and i==0 else 1} for i in range(count)}
    def visible(self,position=None):
        position=position or [0,0]
        return {k:{'position':v['position']} for k,v in self.objects.items() if not self.local or sum(abs(a-b) for a,b in zip(position,v['position']))<=1}
    def movement(self,position,dx,dy):
        target=[position[0]+dx,position[1]+dy]
        if abs(dx)+abs(dy)!=1 or not (0<=target[0]<self.width and 0<=target[1]<self.height): fail('ACTION_INVALID','movement out of bounds')
        return target
    def observe(self,target):
        if target not in self.objects: fail('UNRESOLVED_REF',target)
        return {'target':target, 'value':self.objects[target]['value'], 'position':self.objects[target]['position']}

def atomic_state(method):
    """Rollback host state on failure; immutable old events are shallow-copied."""
    def wrapped(self,*args,**kwargs):
        before={k:(list(v) if k=='events' else copy.deepcopy(v)) for k,v in self.state.items()}
        try: return method(self,*args,**kwargs)
        except Exception:
            self.state=before
            raise
    return wrapped

class Individual:
    def __init__(self, world=None, individual_id='al01-research-001'):
        self.world=world or World(); self.vm=CVM(); self.migrations=[]; self.action_dispatcher=None
        self.state={'id':individual_id,'version':0,'world_version':self.world.version,'tick':0,
                    'observed':{},'pending':[], 'pending_action_id':None, 'requested_actions':[], 'goal':'observe_unknown', 'events':[], 'avoid_positions':[],
                    'lifecycle':'ACTIVE','waiting':False, 'closed_loops':0,
                    'position':[0,0],'visited':[[0,0]],'event_bytes':0,'message_bytes':0,
                    'inbox':[], 'packet_status':{}, 'outbox':[], 'delivered':[], 'tasks':[], 'task_results':{}}
        from .goals import initial
        self.state['goal_state']=initial(self.state,self.world)
    def _event(self,kind,payload):
        from .goals import advance
        advance(self.state,self.world,kind,payload)
        payload=copy.deepcopy(payload);payload['goal_state']=copy.deepcopy(self.state['goal_state'])
        previous=self.state['events'][-1]['hash'] if self.state['events'] else '0'*64
        event={'index':len(self.state['events']),'tick':self.state['tick'],'kind':kind,'payload':copy.deepcopy(payload),'previous':previous}
        event['hash']=digest(event)
        size=len(canonical(event).encode('utf8'))
        if len(self.state['events'])>=MAX_EVENTS or self.state['event_bytes']+size>MAX_LOG_BYTES: fail('BUDGET_EXCEEDED','capacity reached; this version cannot prune history')
        self.state['events'].append(event); self.state['event_bytes']+=size
    @atomic_state
    def commit(self,tx,context=None):
        self.vm.verify(tx)
        if tx.state_version!=self.state['version'] or tx.world_version!=self.world.version: fail('STATE_CONFLICT')
        for proposal in tx.proposals:
            capability='observe' if proposal.get('kind')=='observe' else 'move'
            if capability not in self.vm.capabilities: fail('CAPABILITY_DENIED',capability)
        # Validate and commit intent only. The executor runs in a later phase.
        request=None;action_ids=[];movement=None
        if tx.proposals:
            if self.state['requested_actions'] or self.state['pending']:fail('BUDGET_EXCEEDED','one outstanding action')
            p=tx.proposals[0]
            if p['kind']=='observe':
                if p['target'] not in self.world.visible(self.state['position']):fail('UNRESOLVED_REF','target outside local perception')
            elif p['kind']=='move':
                x,y=self.state['position'];nx=x+p['dx'];ny=y+p['dy']
                if abs(p['dx'])+abs(p['dy'])!=1 or not (0<=nx<self.world.width and 0<=ny<self.world.height):fail('ACTION_INVALID','movement bounds')
            else:fail('UNKNOWN_OPCODE',p['kind'])
            key=f"{self.state['id']}:{self.state['version']}:0"
            action_id='act_'+hashlib.sha256(key.encode()).hexdigest()
            request={'action_id':action_id,'dedup_key':key,'proposal':copy.deepcopy(p),'position':copy.deepcopy(self.state['position']),
                     'world_version':self.world.version,'world_digest':digest(self.world.objects),'requested_tick':self.state['tick']+1}
            action_ids=[action_id]
        if len(self.state['outbox'])+len(tx.messages)>64: fail('BUDGET_EXCEEDED','outbox')
        self.state['version']+=1; self.state['tick']+=1
        for index,message in enumerate(tx.messages):
            entry={'message_id':f"msg_{self.state['version']}_{index}",'emitted_tick':self.state['tick'],'value':serializable(message)}
            size=len(canonical(entry).encode('utf8'))
            if len(self.state['outbox'])+len(self.state['delivered'])>=MAX_MESSAGES or self.state['message_bytes']+size>MAX_MESSAGE_BYTES: fail('BUDGET_EXCEEDED','message history limit')
            self.state['outbox'].append(entry); self.state['message_bytes']+=size
        if request is not None:self.state['requested_actions'].append(request)
        self.state['waiting']=request is None and tx.wait_reason is not None
        payload={'ir':tx.ir,'proposals':tx.proposals,'wait':tx.wait_reason,'messages':serializable(tx.messages),'neuron_calls':tx.neuron_calls,
                 'movement':None,'action_ids':action_ids,'action_requested':request}
        if context:
            if set(context)-{'candidates','packet'}: fail('TYPE_ERROR','trace context')
            payload.update(copy.deepcopy(context))
        self._event('commit',payload)
    def handles(self):
        return {'cap_'+cap:{cap} for cap in self.vm.capabilities}
    @atomic_state
    def enqueue_packet(self,packet):
        # Admission validates but never executes. Duplicate IDs stay rejected after restart.
        validate_packet(packet,self.state,self.handles())
        packet_id=packet['packet_id']
        if packet_id in self.state['packet_status']: fail('DUPLICATE_PACKET',packet_id)
        if len(self.state['inbox'])>=32 or len(self.state['packet_status'])>=10000: fail('BUDGET_EXCEEDED','packet queue/history')
        self.vm.compile_packet(packet,self.state,self.candidates(),self.handles())
        self.state['inbox'].append(copy.deepcopy(packet)); self.state['packet_status'][packet_id]='queued'
        self.state['version']+=1; self.state['waiting']=False
    @atomic_state
    def enqueue_task(self,task):
        if 'neuron' not in self.vm.capabilities: fail('CAPABILITY_DENIED','neuron')
        if len(self.state['tasks'])>=32 or len(self.state['task_results'])+len(self.state['tasks'])>=10000: fail('BUDGET_EXCEEDED','task queue/history')
        ids=set(self.state['task_results'])|{t['id'] for t in self.state['tasks']}
        task=validate_task(task,ids)
        self.state['tasks'].append(task); self.state['version']+=1; self.state['waiting']=False
    def _route_step(self):
        if self.state['inbox']:
            packet=self.state['inbox'][0]
            try:
                tx=self.vm.prepare_packet(packet,self.state,self.candidates(),self.handles(),queued=True)
                context={'packet':{k:copy.deepcopy(packet[k]) for k in ('packet_id','origin_module','trace_id','provenance','logical_tick')}}
                self.commit(tx,context)
                self.state['packet_status'][packet['packet_id']]='committed'
            except CLError as exc:
                if exc.code=='BUDGET_EXCEEDED' or exc.code.startswith('DISPATCH_'): raise
                self.state['version']+=1; self.state['tick']+=1
                self.state['packet_status'][packet['packet_id']]='rejected:'+exc.code
                self._event('packet_rejected',{'packet_id':packet['packet_id'],'error':exc.code})
            self.state['inbox'].pop(0)
            return True
        if self.state['outbox']:
            message=self.state['outbox'].pop(0)
            self.state['version']+=1; self.state['tick']+=1
            self.state['delivered'].append(message)
            self._event('message_delivered',message)
            return True
        if self.state['tasks']:
            ready=next((t for t in self.state['tasks'] if all(d in self.state['task_results'] for d in t['depends_on'])),None)
            if ready is None: fail('DEPENDENCY_ERROR','no ready task')
            metadata=None
            try:
                if 'neuron' not in self.vm.capabilities: fail('CAPABILITY_DENIED','neuron')
                from .routing import run_ready_task_with_metadata
                metadata=run_ready_task_with_metadata(ready,self.state['task_results'],state=self.state,capabilities=self.vm.capabilities)
                result={'status':'ok','value':metadata['result']}
            except CLError as exc: result={'status':'error','code':exc.code}
            self.state['version']+=1; self.state['tick']+=1
            self.state['task_results'][ready['id']]=result
            self.state['tasks'].remove(ready)
            self._event('neuron_result',{'task':ready,'result':result,'registry':REGISTRY_VERSION,'contract':metadata})
            return True
        return False
    def candidates(self):
        """Full ranked visible reference namespace, including observed objects."""
        visible=self.world.visible(self.state['position'])
        def rank(name):
            pos=visible[name]['position']
            # A single observed adverse outcome changes the next candidate ordering.
            # This is a transparent baseline heuristic, not a learned neural model.
            near=any(abs(pos[0]-p[0])+abs(pos[1]-p[1])<=2 for p in self.state['avoid_positions'])
            return (near,name)
        return sorted(visible,key=rank)
    def action_candidates(self,visible=None):
        """Bounded unknown-action shortlist; never truncate the reference namespace."""
        if visible is None: visible=self.candidates()
        return [name for name in visible if name not in self.state['observed']][:32]
    def _process_requested(self):
        request=self.state['requested_actions'][0];p=request['proposal']
        if self.state['position']!=request['position']:fail('STATE_CONFLICT','requested position changed')
        if self.action_dispatcher is not None:
            result=self.action_dispatcher.execute_request(request,self.vm.capabilities)
        else:
            cap='observe' if p['kind']=='observe' else 'move'
            if cap not in self.vm.capabilities:fail('CAPABILITY_DENIED',cap)
            if request['world_version']!=self.world.version or request['world_digest']!=digest(self.world.objects):fail('STATE_CONFLICT','requested world changed')
            result=self.world.observe(p['target']) if p['kind']=='observe' else self.world.movement(request['position'],p['dx'],p['dy'])
        from .actions import validate_result
        validate_result(request,result)
        if self.state['position']!=request['position']:fail('STATE_CONFLICT','requested position changed')
        self.state['version']+=1;self.state['tick']+=1;self.state['waiting']=False
        self.state['requested_actions'].pop(0)
        if p['kind']=='observe':
            self.state['observed'][result['target']]=copy.deepcopy(result)
            if result['value']<0:self.state['avoid_positions'].append(result['position'])
            self.state['closed_loops']+=1
            self._event('feedback',dict(copy.deepcopy(result),action_id=request['action_id']))
        else:
            self.state['position']=copy.deepcopy(result)
            if result not in self.state['visited']:self.state['visited'].append(copy.deepcopy(result))
            self._event('action_result',{'action_id':request['action_id'],'kind':'movement','position':copy.deepcopy(result)})
        return True
    def resume_goal(self):
        from .goals import initial
        self.state['goal_state']=initial(self.state,self.world)
        self.state['goal_state']['reason']='explicit_resume'
        self.state['version']+=1;self.state['waiting']=False
    @atomic_state
    def step(self):
        if self.state['lifecycle']!='ACTIVE': return False
        if self.state['goal_state']['status']=='BLOCKED' and not self.state['pending'] and not self.state['requested_actions']:
            if self.world.version==self.state['goal_state']['marker']['world_version']: return False
            self.resume_goal()
        if self.state['requested_actions']:return self._process_requested()
        if self.state['pending']:
            event=self.state['pending'].pop(0)
            self.state['observed'][event['target']]=event
            if event['value']<0: self.state['avoid_positions'].append(event['position'])
            self.state['version']+=1; self.state['tick']+=1; self.state['closed_loops']+=1
            self.state['waiting']=False
            payload=copy.deepcopy(event)
            if self.state['pending_action_id'] is not None: payload['action_id']=self.state['pending_action_id']
            self.state['pending_action_id']=None
            self._event('feedback',payload)
            return True
        if self._route_step(): return True
        if self.state['waiting']:
            # No busy loop counts toward cognition; wake only when world changes.
            if self.world.version==self.state['world_version']: return False
            self.state['waiting']=False
        self.state['world_version']=self.world.version
        visible=self.candidates()
        candidates=self.action_candidates(visible)
        source=CONTROLLER if 'neuron' in self.vm.capabilities else EXPLORER
        if self.world.local and not candidates:
            visited={tuple(p) for p in self.state['visited']}; x,y=self.state['position']
            frontier=[(abs(cx-x)+abs(cy-y),cx,cy) for cy in range(self.world.height) for cx in range(self.world.width) if (cx,cy) not in visited]
            if frontier:
                _,cx,cy=min(frontier)
                dx=(cx>x)-(cx<x); dy=0 if dx else (cy>y)-(cy<y)
                source=f'PROPOSE(MOVE(dx={dx},dy={dy}));'
        tx=self.vm.prepare(source,self.state,visible)
        self.commit(tx,{'candidates':candidates})
        return True
    def run(self,steps=1000):
        effective=0
        for _ in range(steps):
            if not self.step(): break
            effective+=1
        return effective
    def snapshot(self):
        return {'schema':1,'cl_version':VERSION,'migrations':copy.deepcopy(self.migrations),'state':copy.deepcopy(self.state),
                'world':{'version':self.world.version,'objects':copy.deepcopy(self.world.objects),'local':self.world.local,'width':self.world.width,'height':self.world.height},
                'capabilities':sorted(self.vm.capabilities)}
    def save(self,path):
        path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
        data=self.snapshot(); validate_snapshot(data)
        wrapper={'data':data,'sha256':digest(data)}
        encoded=canonical(wrapper)
        if len(encoded.encode('utf8'))>MAX_CHECKPOINT_BYTES: fail('BUDGET_EXCEEDED','checkpoint 32 MiB limit')
        fd,tmp=tempfile.mkstemp(prefix=path.name+'.',dir=path.parent)
        try:
            with os.fdopen(fd,'w',encoding='utf8') as f:
                f.write(encoded); f.flush(); os.fsync(f.fileno())
            os.replace(tmp,path)
            if os.name=='posix':
                directory_fd=os.open(path.parent,os.O_RDONLY|getattr(os,'O_DIRECTORY',0))
                try: os.fsync(directory_fd)
                finally: os.close(directory_fd)
        finally:
            if os.path.exists(tmp): os.unlink(tmp)
    @classmethod
    def load(cls,path):
        try:
            if Path(path).stat().st_size>MAX_CHECKPOINT_BYTES: fail('CHECKPOINT_INVALID','file size')
            wrapper=json.loads(Path(path).read_text(encoding='utf8')); data=wrapper['data']
            validate_snapshot(data)
            if digest(data)!=wrapper['sha256']: fail('CHECKPOINT_INVALID','hash')
            if data['schema']!=1 or data['cl_version']!=VERSION: fail('CHECKPOINT_INVALID','version')
            if set(data['capabilities'])-{'read','observe','emit','neuron','move'}: fail('CHECKPOINT_INVALID','capabilities')
            previous='0'*64
            for i,event in enumerate(data['state']['events']):
                if event['index']!=i or event['previous']!=previous or digest({k:v for k,v in event.items() if k!='hash'})!=event['hash']:
                    fail('CHECKPOINT_INVALID','event chain')
                previous=event['hash']
            obj=cls(World(0)); obj.world.objects=data['world']['objects']; obj.world.version=data['world']['version']
            obj.world.local=data['world']['local']; obj.world.width=data['world']['width']; obj.world.height=data['world']['height']
            obj.state=data['state']; obj.vm=CVM(data['capabilities']); obj.migrations=data['migrations']; return obj
        except (KeyError, ValueError, TypeError, RecursionError, OverflowError, UnicodeError) as exc: fail('CHECKPOINT_INVALID',str(exc))
