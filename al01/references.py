"""Explicit immutable bindings into one individual's visible snapshot namespace.

No reference grants authority or dereferences state fields. Entity names continue
to use the existing @ namespace. Event/state constructors bind identity, never
pending feedback or arbitrary historical state objects.
"""
from dataclasses import dataclass
import re

from .limits import MAX_EVENTS,MAX_LOG_BYTES


@dataclass(frozen=True)
class EventRef:
    individual_id: str
    index: int
    tick: int
    content_hash: str


@dataclass(frozen=True)
class StateRef:
    individual_id: str
    version: int
    world_version: int
    tick: int
    event_head: str


def _integer(value): return type(value) is int and 0<=value<=2**53
def _hash(value): return isinstance(value,str) and re.fullmatch(r'[0-9a-f]{64}',value) is not None


def reference_from_data(value,expected_type=None):
    """Validate shape only; use ReferenceScope.resolve for authoritative binding."""
    from .core import fail
    if isinstance(value,EventRef): ref=value
    elif isinstance(value,StateRef): ref=value
    elif isinstance(value,dict) and set(value)=={'event_ref'}:
        fields=value['event_ref']
        if not isinstance(fields,dict) or set(fields)!={'individual_id','index','tick','content_hash'}: fail('TYPE_ERROR','EventRef fields')
        ref=EventRef(**fields)
    elif isinstance(value,dict) and set(value)=={'state_ref'}:
        fields=value['state_ref']
        if not isinstance(fields,dict) or set(fields)!={'individual_id','version','world_version','tick','event_head'}: fail('TYPE_ERROR','StateRef fields')
        ref=StateRef(**fields)
    else: fail('TYPE_ERROR','expected bound reference')
    name='EventRef' if isinstance(ref,EventRef) else 'StateRef'
    if expected_type is not None and expected_type!=name: fail('TYPE_ERROR','reference kind mismatch')
    if not isinstance(ref.individual_id,str) or not 1<=len(ref.individual_id)<=256: fail('TYPE_ERROR','reference individual')
    try: ref.individual_id.encode('utf8')
    except UnicodeError: fail('TYPE_ERROR','reference UTF-8')
    if isinstance(ref,EventRef):
        if not _integer(ref.index) or ref.index>=MAX_EVENTS or not _integer(ref.tick) or ref.tick!=ref.index+1 or not _hash(ref.content_hash): fail('TYPE_ERROR','EventRef binding')
    else:
        if (not all(_integer(v) for v in (ref.version,ref.world_version,ref.tick)) or ref.version<ref.tick
                or ref.tick>MAX_EVENTS or not _hash(ref.event_head) or (ref.tick==0 and ref.event_head!='0'*64)):
            fail('TYPE_ERROR','StateRef binding')
    return ref


def reference_data(value):
    ref=reference_from_data(value)
    return {'event_ref' if isinstance(ref,EventRef) else 'state_ref':dict(vars(ref))}


def _current(state):
    from .core import fail
    if not isinstance(state,dict) or not isinstance(state.get('events'),list): fail('TYPE_ERROR','reference snapshot')
    events=state['events']
    if len(events)>MAX_EVENTS: fail('BUDGET_EXCEEDED','reference snapshot event count')
    if type(state.get('tick')) is not int or state['tick']!=len(events): fail('STATE_CONFLICT','reference snapshot count')
    head='0'*64
    if events:
        last=events[-1]
        if (not isinstance(last,dict) or type(last.get('index')) is not int or last['index']!=len(events)-1
                or type(last.get('tick')) is not int or last['tick']!=len(events) or not _hash(last.get('hash'))):
            fail('STATE_CONFLICT','reference snapshot head')
        head=last['hash']
    return reference_from_data(StateRef(state.get('id'),state.get('version'),state.get('world_version'),state['tick'],head))


class ReferenceScope:
    """One bounded read scope; event chain validation is cached within that scope."""
    def __init__(self,state,capabilities=('read',)):
        from .core import fail
        if not isinstance(capabilities,(tuple,list,set,frozenset)) or 'read' not in capabilities: fail('CAPABILITY_DENIED','read')
        self.state=state;self.stamp=_current(state);self.checked=False;self.event_bindings=();self.selected_bytes=0

    def _unchanged(self):
        from .core import fail
        if _current(self.state)!=self.stamp: fail('STATE_CONFLICT','reference source snapshot changed')

    def current(self):
        self._unchanged()
        return self.stamp

    def _check_chain(self):
        from .core import canonical,digest,fail,serializable
        self._unchanged()
        if self.checked:return
        previous='0'*64;size=0;bindings=[]
        for index,original in enumerate(self.state['events']):
            try:
                event=serializable(original)
                if not isinstance(event,dict) or set(event)!={'index','tick','kind','payload','previous','hash'}: fail('STATE_CONFLICT','reference event fields')
                size+=len(canonical(event).encode('utf8'))
                if size>MAX_LOG_BYTES: fail('BUDGET_EXCEEDED','reference history scan bytes')
                if (type(event['index']) is not int or event['index']!=index or type(event['tick']) is not int or event['tick']!=index+1
                        or event['previous']!=previous or not _hash(event['hash'])
                        or digest({k:v for k,v in event.items() if k!='hash'})!=event['hash']):
                    fail('STATE_CONFLICT','reference event chain')
                previous=event['hash']
                bindings.append(EventRef(self.stamp.individual_id,index,event['tick'],event['hash']))
            except (TypeError,ValueError,KeyError,UnicodeError,OverflowError,RecursionError):
                fail('STATE_CONFLICT','malformed reference history')
        self.event_bindings=tuple(bindings);self.checked=True

    def event(self,index):
        from .core import fail,serializable,canonical,digest
        if not _integer(index) or index>=len(self.state['events']): fail('UNRESOLVED_REF','event index')
        self._check_chain()
        ref=self.event_bindings[index]
        # Full-chain caching must not allow a selected record to change beneath
        # its immutable binding, even if trusted host code forgot a version bump.
        try:
            event=serializable(self.state['events'][index])
            self.selected_bytes+=len(canonical(event).encode('utf8'))
            if self.selected_bytes>MAX_LOG_BYTES: fail('BUDGET_EXCEEDED','selected reference read bytes')
            if (not isinstance(event,dict) or set(event)!={'index','tick','kind','payload','previous','hash'}
                    or event['hash']!=ref.content_hash or digest({k:v for k,v in event.items() if k!='hash'})!=ref.content_hash):
                fail('STATE_CONFLICT','selected reference event changed')
        except (TypeError,ValueError,KeyError,UnicodeError,OverflowError,RecursionError):
            fail('STATE_CONFLICT','malformed selected reference event')
        return ref

    def resolve(self,value):
        from .core import fail
        ref=reference_from_data(value)
        if ref.individual_id!=self.stamp.individual_id: fail('STATE_CONFLICT','reference belongs to another individual')
        actual=self.event(ref.index) if isinstance(ref,EventRef) else self.current()
        if ref!=actual: fail('STATE_CONFLICT','stale or altered reference binding')
        return ref
