
def quads(n=1, *, graph=None, anon=False, nested=False):
    from pyoxigraph import Quad, Triple
    from pyoxigraph import BlankNode, NamedNode
    for i in range(n):
        if anon:
            _ = Triple(BlankNode(), NamedNode(f'p:{i}'), BlankNode())
        else:
            _ = Triple(NamedNode(f's:{i}'), NamedNode(f'p:{i}'), NamedNode(f'o:{i}'))
        if nested:
            _ = Triple(_, NamedNode('m:'), _)
        yield Quad(*_,  NamedNode(f'g:{graph}') if graph else None)


def json():
    return {}

def jsonloader():
    from json2rdf import j2r

def mapper(db):
    ...


def xtest():
    from engine.triples import (ConstructQuery,
                                 Rules, PyRule, noeffect_pyfunc, 
                                 Engine,
                                 Triples,
                                 OxiGraph)
    import pyoxigraph as g

    def _data(db: OxiGraph) -> Triples:
        _ = b'<http://example.com> <http://example.com/p> "1" .\n'
        _ = g.parse(_, 'text/turtle')
        _ = Triples(_)
        return _
    Data = PyRule(_data)
    _ = """
    construct {
        <http://example.com/a> <http://www.w3.org/2000/01/rdf-schelabel> "something" .}
    where {  }
    """
    Construct = ConstructQuery(_)
    rules = Rules([noeffect_pyfunc, Data, Construct])
    engine = Engine(rules, OxiGraph(g.Store()) )
    res = engine()
    _ = res.db
    _ = list(_)
    return _


def xtest_deanon():
    from engine.triples import deanon, flatten
    from pyoxigraph import Triple, NamedNode, BlankNode
    # test bn deanoned stay stay the same.
    x = BlankNode('x')
    p = NamedNode('p:')
    y = BlankNode('y')
    t = Triple(x, p, y)
    m = NamedNode('m:')
    i = [
        Triple(t, m , t),
        t 
        ]
    o = deanon(i)
    o = list(o)
    for n in flatten(o):
        assert(not isinstance(n, BlankNode) )
    assert(isinstance(o[0].subject, Triple) )
    assert(isinstance(o[0].object, Triple)  )
    assert(o[0].subject.subject     == o[1].subject  )
    assert(o[0].subject.object      == o[1].object  )
    assert(o[0].object.subject      == o[1].subject  )
    assert(o[0].object.object       == o[1].object  )
    assert(o[0].subject.predicate   == o[1].predicate  )
    assert(o[0].object.predicate    == o[1].predicate  )
    # test nn unchanged
    s = NamedNode('s:')
    o = NamedNode('o:')
    i = [
        Triple(s, p, o)
        ]
    do = deanon(i)
    do = list(do)
    assert(do[0].subject     == s)
    assert(do[0].predicate   == p)
    assert(do[0].object      == o)




def insert(method, n=int(1e6)): # performance
    from pyoxigraph import Store
    s = Store()
    q = frozenset(quads(n=n))
    from time import time
    t0 = time()
    if method == 'objects': 
        s.bulk_extend(q)
        print(time()-t0) # ~1.2s
    else:
        assert(method == 'serialized')
        from pyoxigraph import serialize, RdfFormat
        _ = serialize(q, format=RdfFormat.N_QUADS)
        #_ = map(str, q)
        #_ = '.\n'.join(_)
        #if _: _ = _+'.\n'
        print(time()-t0)
        s.bulk_load(_,   format=RdfFormat.N_QUADS)
        print(time()-t0)
        # ~4s for ntriples and ttl

def conversion(): # performance
    i = quads(n=int(1e5))
    i = frozenset(i)
    from rdflib import Dataset
    from rdf_engine.conversions import node, og2rl
    from time import time
    t0 = time()
    #ds = og2rl(i)
    #print(time()-t0) # ~2.2 sec
    #_ = map(lambda q: tuple(map(node.og2rl, q), ), i)
    #_ = frozenset(_)  # ~.7sec
    # from typing import DefaultDict
    # ds = DefaultDict(list)
    # for q in frozenset(i):
    #     q = tuple(map(node.og2rl, q),)
    #     ds[q[-1]].append(q[:3])
    # print(time()-t0)  # ~.9
    # return ds
    ds = Dataset()
    ds.addN()
    #for q in _: ds.add(q) # ~2.4
    #print(time()-t0)

    return _
