"""Bounded CL Option/Result subset: explicit tags, no coercion or unchecked unwrap."""
import math, re

SUM_OPS=frozenset({'SOME','NONE','OK','ERR','IS_SOME','IS_OK','UNWRAP_OR'})
PRIMITIVES=frozenset({'Bool','Int','Float','Text','Unknown','EntityRef','EventRef','StateRef','UnresolvedRef','Claim','Belief','Hypothesis'})
GENERICS=frozenset({'List','Option','Result'})


def value_type(name):
    """Validate a canonical type name without evaluation or arbitrary identifiers."""
    from .core import fail
    if not isinstance(name,str): fail('TYPE_ERROR','type name must be text')
    if len(name)>256: fail('BUDGET_EXCEEDED','type name length')
    def parse(text,depth=0,parent=None):
        if depth>8: fail('BUDGET_EXCEEDED','type nesting')
        if text in PRIMITIVES: return
        if text=='Empty' and parent=='List': return
        start=text.find('<')
        if start<0 or text[:start] not in GENERICS or not text.endswith('>'):
            fail('TYPE_ERROR','unsupported value type')
        parse(text[start+1:-1],depth+1,text[:start])
    parse(name)
    return name


def wrapped_inner(name,family=None):
    from .core import fail
    value_type(name)
    families=(family,) if family else ('Option','Result')
    if not any(name.startswith(f+'<') for f in families): fail('TYPE_ERROR','expected '+(' or '.join(families)))
    return name[name.index('<')+1:-1]


def infer_sum_type(op,pairs):
    """Called after ordinary named-argument ordering and arity checks."""
    from .core import fail
    if op in ('SOME','OK'):
        inner=value_type(pairs[0][1])
        return value_type(('Option' if op=='SOME' else 'Result')+'<'+inner+'>')
    if op in ('NONE','ERR'):
        node,typ=pairs[-1]
        if node.get('kind')!='literal' or typ!='Text': fail('TYPE_ERROR','type name must be a fixed text literal')
        inner=value_type(node['value'])
        if op=='ERR' and pairs[0][1]!='Text': fail('TYPE_ERROR','ERR code requires Text')
        return value_type(('Option' if op=='NONE' else 'Result')+'<'+inner+'>')
    if op in ('IS_SOME','IS_OK'):
        wrapped_inner(pairs[0][1],'Option' if op=='IS_SOME' else 'Result')
        return 'Bool'
    if op=='UNWRAP_OR':
        inner=wrapped_inner(pairs[0][1]); fallback=pairs[1][1]
        # An empty list inhabits a declared list type without coercing elements.
        if inner!=fallback and not (inner.startswith('List<') and fallback=='List<Empty>'):
            fail('TYPE_ERROR','fallback must match wrapped value type')
        return inner
    fail('UNKNOWN_OPCODE',op)


def _variant_header(value,typ):
    from .core import fail
    allowed=('SOME','NONE') if typ.startswith('Option<') else ('OK','ERR')
    if not isinstance(value,dict) or value.get('type')!=typ or value.get('tag') not in allowed:
        fail('TYPE_ERROR','invalid sum value')
    tag=value['tag']; present=tag in ('SOME','OK')
    expected={'tag','type','value'} if present else ({'tag','type','code'} if tag=='ERR' else {'tag','type'})
    if set(value)!=expected: fail('TYPE_ERROR','invalid sum fields')
    if tag=='ERR' and not isinstance(value['code'],str): fail('TYPE_ERROR','invalid error code')
    return present,value.get('value')


def validate_runtime_value(value,typ):
    """Validate runtime or JSON-roundtripped typed data under an expansion budget.

    Serialized EntityRef values are checked for shape, not granted authority or
    converted into live references. Reference resolution remains the VM's job.
    """
    from .core import fail,Ref
    value_type(typ)
    budget={'nodes':100000,'text_bytes':1048576}
    def text(value):
        if not isinstance(value,str): fail('TYPE_ERROR','expected Text')
        try: budget['text_bytes']-=len(value.encode('utf8'))
        except UnicodeError: fail('TYPE_ERROR','invalid UTF-8 text')
        if budget['text_bytes']<0: fail('BUDGET_EXCEEDED','sum value text bytes')
    def check(value,typ,depth=0):
        budget['nodes']-=1
        if budget['nodes']<0 or depth>32: fail('BUDGET_EXCEEDED','sum value size/depth')
        if typ.startswith('List<'):
            if not isinstance(value,list): fail('TYPE_ERROR','expected List')
            inner=typ[5:-1]
            if inner=='Empty' and value: fail('TYPE_ERROR','nonempty List<Empty>')
            for item in value: check(item,inner,depth+1)
            return
        if typ.startswith(('Option<','Result<')):
            present,payload=_variant_header(value,typ)
            if present: check(payload,typ[typ.index('<')+1:-1],depth+1)
            elif value['tag']=='ERR': check(value['code'],'Text',depth+1)
            return
        if typ=='Text': text(value); return
        if typ in ('EventRef','StateRef'):
            from .references import reference_from_data
            ref=reference_from_data(value,typ);text(ref.individual_id)
            return
        if typ=='UnresolvedRef':
            if (not isinstance(value,dict) or set(value)!={'unresolved_ref'} or
                not isinstance(value['unresolved_ref'],dict) or set(value['unresolved_ref'])!={'mention'}):
                fail('TYPE_ERROR','invalid UnresolvedRef')
            mention=value['unresolved_ref']['mention'];text(mention)
            if not mention.strip() or len(mention)>256: fail('TYPE_ERROR','unresolved mention length')
            return
        if typ in ('Claim','Belief','Hypothesis'):
            fields={'record_type','proposition','source','evidence_refs','resolved_evidence','status','logical_tick'}
            if (not isinstance(value,dict) or set(value)!=fields or value['record_type']!=typ
                    or value['status']!='UNVERIFIED' or type(value['logical_tick']) is not int
                    or not 0<=value['logical_tick']<=2**53): fail('TYPE_ERROR','invalid epistemic record')
            text(value['proposition']);text(value['source'])
            refs=value['evidence_refs'];resolved=value['resolved_evidence']
            if not isinstance(refs,list) or not isinstance(resolved,list) or len(refs)!=len(resolved) or len(refs)>64:
                fail('TYPE_ERROR','epistemic evidence shape')
            for ref,record in zip(refs,resolved):
                text(ref)
                if (not re.fullmatch(r'event_[0-9]{1,9}',ref) or not isinstance(record,dict)
                        or set(record)!={'ref','content_hash','kind','tick'} or record['ref']!=ref
                        or type(record['tick']) is not int or record['tick']!=int(ref[6:])+1
                        or record['tick']>value['logical_tick']): fail('TYPE_ERROR','epistemic evidence binding')
                text(record['content_hash']);text(record['kind'])
                if not re.fullmatch(r'[0-9a-f]{64}',record['content_hash']) or not record['kind']:
                    fail('TYPE_ERROR','epistemic evidence metadata')
            return
        if typ=='EntityRef':
            if isinstance(value,Ref): name=value.name
            elif isinstance(value,dict) and set(value)=={'entity_ref'}: name=value['entity_ref']
            else: fail('TYPE_ERROR','expected EntityRef')
            text(name)
            if not name: fail('TYPE_ERROR','empty EntityRef')
            return
        valid=((typ=='Bool' and type(value) is bool) or
               (typ=='Int' and type(value) is int and abs(value)<=2**53) or
               (typ=='Float' and type(value) is float and math.isfinite(value)) or
               (typ=='Unknown' and value is None))
        if not valid: fail('TYPE_ERROR','invalid '+typ+' value')
    check(value,typ)


def variant(value,typ):
    """Return presence and payload only after validating the entire typed value."""
    wrapped_inner(typ)
    validate_runtime_value(value,typ)
    return _variant_header(value,typ)


def evaluate_sum(op,args,typ,input_type=None):
    from .core import fail
    if op in ('IS_SOME','IS_OK'): return variant(args[0],input_type)[0]
    if op in ('SOME','OK'): value={'tag':op,'type':typ,'value':args[0]}
    elif op=='NONE': value={'tag':'NONE','type':typ}
    elif op=='ERR': value={'tag':'ERR','type':typ,'code':args[0]}
    else: fail('UNKNOWN_OPCODE',op)
    validate_runtime_value(value,typ)
    return value
